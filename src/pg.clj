(ns pg
  (:require
   [system]
   [clojure.string :as str]
   [clojure.java.io :as io]
   [next.jdbc :as jdbc]
   [next.jdbc.result-set :as rs]
   [dsql.pg]
   [cheshire.core]
   [pg.migrations])
  (:refer-clojure :exclude [load])
  (:import (java.sql Connection DriverManager)
           (java.io BufferedInputStream BufferedReader FileInputStream FileNotFoundException InputStream InputStreamReader)
           (java.util.zip GZIPInputStream)
           (org.postgresql Driver PGConnection PGProperty)
           [java.sql PreparedStatement ResultSet]
           (org.postgresql.copy CopyManager CopyIn)
           (com.zaxxer.hikari HikariConfig HikariDataSource)
           [org.postgresql.jdbc PgArray]
           [org.postgresql.util PGobject]))

(defn json-parse [s]
  (cheshire.core/parse-string s keyword))

(set! *warn-on-reflection* true)

(defn coerce [r]
  (->> r
       (reduce (fn [acc [k v]]
                 (assoc acc (keyword (name k))
                        (cond (instance? PGobject v)
                              (case (.getType ^PGobject v)
                                "json" (cheshire.core/parse-string (.getValue ^PGobject v) keyword)
                                "jsonb" (cheshire.core/parse-string (.getValue ^PGobject v) keyword)
                                (.getValue ^PGobject v))
                              (instance? java.math.BigDecimal v) (double v)
                              (instance? PgArray v) (vec (.getArray ^PgArray v))
                              :else v))) {})))


(defn readonly-connection [ctx]
  (get-in @ctx [:ctx :pg-readonly]))

(defn execute* [conn q]
  (->> (jdbc/execute! conn q {:builder-fn rs/as-unqualified-maps})
       (mapv coerce)))

(defn datasource [ctx]
  (or (system/get-system-state ctx [:datasource])
      (throw (Exception. "No datasource in pg module. pg module probably not initialized"))))

(defn connection [ctx]
  (let [^HikariDataSource datasource (datasource ctx)]
    (.getConnection datasource)))

(defn with-connection [ctx f]
  (with-open [^Connection conn (datasource ctx)]
    (f conn)))

(defn transaction [ctx]
  (get ctx ::transaction))

(defn rollback [ctx]
  (.rollback ^Connection (transaction ctx)))

(defmacro with-transaction [ctx & body]
  `(next.jdbc/transact (datasource ~ctx)
                       (fn [tx#]
                         (let [~ctx (assoc ~ctx :pg/transaction tx#)] ;; TODO resolve kw namespace dynamicly
                           (doseq [[k# v#] (:pg/transaction-params ~ctx)]
                             (pg/execute! ~ctx {:sql (format "set local %s to %s" (name k#) v#)}))
                           ~@body))))

(defn format-dsql [dql]
  (dsql.pg/format dql))

(defn jdbc-execute! [conn {sql :sql dql :dsql :as opts}]
  (assert (or sql dql) ":sql or :dsql should be provided")
  (let [sql (cond (vector? sql) sql
                  (string? sql) [sql]
                  dql (format-dsql dql))
        start (System/nanoTime)]
    (try
      (let [res (->> (jdbc/execute! conn sql)
                     (mapv coerce))]
        (println ::executed sql {:duration (/ (- (System/nanoTime) start) 1000000.0)})
        res)
      (catch Exception e
        (println ::error sql {:duration (/ (- (System/nanoTime) start) 1000000.0)})
        (throw e)))))

;; TODO: hooks before sql and after to instrument - use open telemetry
(defn execute! [ctx {sql :sql dql :dsql}]
  (assert (or sql dql) ":sql or :dsql should be provided")
  (let [sql (cond (vector? sql) sql
                  (string? sql) [sql]
                  dql (format-dsql dql))
        start (System/nanoTime)
        conn (or (transaction ctx) (datasource ctx))]
    (try
      (let [res (->> (jdbc/execute! conn sql)
                     (mapv coerce))]
        (system/info ctx ::executed sql {:duration (/ (- (System/nanoTime) start) 1000000.0)})
        res)
      (catch Exception e
        (system/info ctx ::error (.getMessage e) {:sql sql :duration (/ (- (System/nanoTime) start) 1000000.0)})
        (let [msg (.getMessage e)]
          (if-let [[_ pos] (and (str/includes? msg "Position:") (re-find #"Position: (\d+)" msg))]
            (let [sql (first sql)
                  pos (Integer/parseInt pos)]
              (throw (Exception. (str (.getMessage e)
                                      "\n"
                                      (subs sql (min (abs (- pos 10)) 0) pos)
                                      "<*>"
                                      (subs sql pos (min (+ pos 10) (count sql)))))))
            (throw e)))))))

(defn q [q ctx & params]
  (letfn [(format-sql [q params]
            (->> params
                 (remove nil?)
                 (cons (-> (str/join " " q)
                           (str/replace #"\(\s" "(")
                           (str/replace #"\s\)" ")")
                           (str/replace #"\s\," ",")))
                 (into [])))
          (sanitize [q]
            (->> (flatten q)
                 (remove nil?)
                 (remove #(str/starts-with? % "--"))
                 (map str)))]
    (pg/execute! ctx {:sql (-> q sanitize (format-sql params))})))

(defn array-of [ctx type array]
  (with-connection ctx (fn [c] (.createArrayOf ^Connection c type (into-array String array)))))

(defn truncate! [ctx t]
  (->> (jdbc/execute! (datasource ctx) [(format "truncate \"%s\"" t)])
       (mapv coerce)))

;; TODO: fix for safe execute
(defn safe-execute! [ctx q]
  (->> (jdbc/execute! (datasource ctx) q)
       (mapv coerce)))


;; Make sure the driver is loaded
(Class/forName "org.postgresql.Driver")

(defn get-connection [config]
  (let [jdbc-url (str "jdbc:postgresql://" (:host config) ":" (:port config) "/" (:database config))]
    (DriverManager/getConnection jdbc-url (:user config) (:password config))))

;; TODO: add params
;; (def defaults
;;   {:auto-commit        true
;;    :read-only          false
;;    :connection-timeout 30000
;;    :validation-timeout 5000
;;    :idle-timeout       600000
;;    :max-lifetime       1800000
;;    :minimum-idle       10
;;    :maximum-pool-size  10})

(defn get-pool [conn]
  (let [^HikariConfig config (HikariConfig.)]
    (doto config
      (.setJdbcUrl (str "jdbc:postgresql://" (:host conn) ":" (:port conn) "/" (:database conn)))
      (.setUsername (:user conn))
      (.setPassword (:password conn))
      (.setMaximumPoolSize (:max-pool-size conn))
      (.setMinimumIdle (:min-idle conn))
      (.setIdleTimeout (:idle-timeout conn))
      (.setConnectionTimeout (:connection-timeout conn))
      (.addDataSourceProperty "cachePrepStmts" "true")
      (.addDataSourceProperty "stringtype" "unspecified")
      (.addDataSourceProperty "prepStmtCacheSize" "250")
      (.addDataSourceProperty "prepStmtCacheSqlLimit" "2048"))
    (HikariDataSource. config)))


(defn fetch [ctx sql-vector fetch-size field on-row]
  (let [fld (name field)]
    (system/info ctx ::fetch (first sql-vector))
    (with-open [^Connection c (connection ctx)
                ^PreparedStatement ps (jdbc/prepare c sql-vector)]
      (.setFetchSize ps fetch-size)
      (let [^ResultSet rs  (.executeQuery ps)]
        (loop [i 0]
          (if (.next rs)
            (do
              (on-row (.getString rs ^String fld) i)
              (recur (inc i)))
            i))))))

(defn copy-ndjson-stream [ctx table ^InputStream stream & [jsonb-column]]
  (with-open [^Connection c (connection ctx)]
    (let [copy-manager (CopyManager. (.unwrap ^Connection c PGConnection))
          copy-sql (str "COPY " table " (" (or jsonb-column "resource") " ) FROM STDIN csv quote e'\\x01' delimiter e'\\t'")]
      (.copyIn copy-manager copy-sql stream))))

(defn copy-ndjson-file [ctx file-path]
  (with-open [gzip-stream (-> file-path io/input-stream GZIPInputStream. InputStreamReader. BufferedReader.)]
    (copy-ndjson-stream ctx "_resource" gzip-stream)))

(def ^bytes NEW_LINE (.getBytes "\n"))
(def ^bytes TAB (.getBytes "\t"))
(def ^bytes X01 (.getBytes "\u0001"))

(defn load [ctx sql cb]
  (with-open [^Connection c (connection ctx)]
    (let [^CopyManager cm (CopyManager. (.unwrap ^Connection c PGConnection))
          ^CopyIn ci (.copyIn cm sql)
          write-column   (fn wr [^String s]
                           (let [^bytes bt (.getBytes s)]
                             (.writeToCopy ci bt 0 (count bt))))
          write-new-line (fn wr [] (.writeToCopy ci NEW_LINE 0 1))
          write-sep      (fn wr [] (.writeToCopy ci X01 0 1))]
      (try
        (cb write-column write-sep write-new-line)
        (catch Exception e (println :ERROR e))
        (finally
          (.endCopy  ci))))))

(defn open-copy-manager  [context sql]
  (let [^Connection c (connection context)
        ^CopyManager cm (CopyManager. (.unwrap ^Connection c PGConnection))]
    (system/info context ::open-copy-manager sql)
    {:copy  (.copyIn cm sql)
     :sql sql
     :connection c}))

(defn close-copy-manger [context {ci :copy c :connection sql :sql}]
  (system/info context ::close-copy-manager sql)
  (try (.endCopy ^CopyIn ci)  (catch Exception e (system/error context ::close-copy-manager (.getMessage e))))
  (try (.close ^Connection c) (catch Exception e (system/error context ::close-copy-manager (.getMessage e)))))

(import '[com.fasterxml.jackson.databind ObjectMapper])
(def mapper (ObjectMapper.))

(defn escaped-string [x]
  (let [es (.writeValueAsString ^ObjectMapper mapper x)] (subs es 1 (dec (count es)))))

(defn remove-x01-symbol [^String s]
  (-> (str/replace s #"\x00" "")
      (str/replace #"\\u0000" "")))

(defn copy-write-column [{^CopyIn ci :copy} ^String s]
  (let [^bytes bt (.getBytes ^String (remove-x01-symbol (escaped-string s)))]
    (.writeToCopy ci bt 0 (count bt))))

(defn copy-write-json-column [{^CopyIn ci :copy} val]
  (let [^bytes bt (.getBytes ^String (remove-x01-symbol (cheshire.core/generate-string val)))]
    (.writeToCopy ci bt 0 (count bt))))

(defn alpha-num? [s]
  (some? (re-matches #"^[a-zA-Z][a-zA-Z0-9]*$" s)))

(defn- to-array-list [arr]
  (->> arr
       (mapv (fn [x]
               (let [x (if (keyword? x) (name x) x)]
                 (if (string? x)
                   (if (alpha-num? x) x (str "\"" (remove-x01-symbol (escaped-string x)) "\""))
                   (if (number? x)
                     x
                     (assert false (pr-str x)))))))
       (str/join ",")))

(defn- to-array-litteral [arr]
  (str "{" (to-array-list arr) "}"))

(defn copy-write-array-column [{^CopyIn ci :copy} val]
  (let [^bytes bt (.getBytes ^String (to-array-litteral val))]
    (.writeToCopy ci bt 0 (count bt))))

(defn copy-write-new-line [{^CopyIn ci :copy}]
  (.writeToCopy ci NEW_LINE 0 1))

(defn copy-write-tab [{^CopyIn ci :copy}]
  (.writeToCopy ci TAB 0 1))

(defn copy-write-x01 [{^CopyIn ci :copy}]
  (.writeToCopy ci X01 0 1))


(defn copy [ctx sql cb]
  (with-open [^Connection c (connection ctx)]
    (let [^CopyManager cm (CopyManager. (.unwrap ^Connection c PGConnection))
          ^CopyIn ci (.copyIn cm sql)
          write (fn wr [^String s]
                  (let [^bytes bt (.getBytes s)]
                    (.writeToCopy ci bt 0 (count bt))
                    (.writeToCopy ci NEW_LINE 0 1)))]
      (try
        (cb write)
        (finally
          (.endCopy  ci))))))

(defn copy-ndjson [ctx table cb]
  (with-open [^Connection c (connection ctx)]
    (let [^CopyManager cm (CopyManager. (.unwrap ^Connection c PGConnection))
          copy-sql (str "COPY " table " (" (or "resource") " ) FROM STDIN csv quote e'\\x01' delimiter e'\\t'")
          ^CopyIn ci (.copyIn cm copy-sql)
          write (fn wr [res]
                  (let [^bytes bt (.getBytes (cheshire.core/generate-string res))]
                    (.writeToCopy ci bt 0 (count bt))
                    (.writeToCopy ci NEW_LINE 0 1)))]
      (try (cb write)
           (finally (.endCopy  ci))))))


(defn generate-migration [name]
  (pg.migrations/generate-migration name))

(defn migrate-prepare [context]
  (execute! context {:sql "create table if not exists public._migrations (id text primary key, file text not null, ts timestamp default  CURRENT_TIMESTAMP)"}))

(defn migrate-up
  "Apply migrations.
  Without file, apply all migrations that weren't applied yet.
  When file is provided, apply only the migration with that file."
  [context & [file]]
  (let [migrations (cond->> (pg.migrations/read-migrations)
                     file (filterv (fn [x] (= file (:file x)))))
        migrations-done (->> (execute! context {:sql "select * from public._migrations"})
                             (reduce (fn [acc {file :file :as m}]
                                       (assoc acc file m)) {}))]
    (doseq [m migrations]
      (when-not (get migrations-done (:file m))
        (system/info context ::migration-up (:file m))
        (try
          (doseq [sql (:up m)]
            (pg/execute! context {:sql sql}))
          (pg/execute! context {:sql ["insert into public._migrations (id, file) values (?, ?)" (:id m) (:file m)]})
          (catch Exception e
            (doseq [sql (:down m)]
              (try (pg/execute! context {:sql sql}) (catch Exception _e)))
            (throw e)))))))

(defn migrate-down
  "Roll back migrations.
  Without file, roll back the last migration.
  When file is provided, roll back the migration with that file.
  If file is :all, roll back all migrations."
  [context & [file]]
  (let [migrations (->> (pg.migrations/read-migrations)
                        (reduce (fn [acc {file :file :as m}]
                                  (assoc acc file m)) {}))
        rollback-migrations (execute! context {:dsql {:select :* :from :_migrations
                                                      :where (when (and file (not= :all file)) [:= :file file])
                                                      :order-by [:pg/desc :ts]
                                                      :limit (when-not (= :all file) 1)}})]
    (doseq [m rollback-migrations]
      (when-let [md (get migrations (:file m))]
        (system/info context ::migration-down (:file m))
        (doseq [sql (:down md)]
          (try (pg/execute! context {:sql sql}) (catch Exception _e)))
        (pg/execute! context {:sql ["delete from _migrations where file = ?" (:file m)]})))))

#_(defmacro load-data [ctx writers & rest]
    (println :? writers)
    (let [cns (->> writers
                   (partition 2)
                   (mapcat (fn [[b _]] [(symbol (str b "-connection")) (list 'connection '_ctx)]))
                   (into []))
          wrts (->> writers
                    (partition 2)
                    (mapcat (fn [[b opts]]
                              (let [in-nm (symbol (str b "-in"))]
                                [in-nm (list 'make-copy-in (symbol (str b "-connection")) opts)
                                 b (list 'make-writer in-nm opts)])))
                    (into []))]
      `(let [_ctx ~ctx]
         (with-open ~cns
           (let ~wrts
             ~@rest)))))


;; (defn start-2 [ctx config]
;;   (if-let [c config]
;;     (let [tmp-conn (get-connection c)
;;           res (jdbc/execute! tmp-conn ["SELECT * FROM pg_database WHERE datname = ?" (:database config)])]
;;       (when-not (seq res)
;;         (println :ensure-db (jdbc/execute! tmp-conn [(str "create database " (:database config))])))
;;       (get-connection (assoc c :database (:database config))))
;;     (get-connection config)))

#_(defn start [system & [opts]]
    (system/start-service
     system
     (let [connection (or opts (default-connection))
           _ (system/info system ::connecting (:database connection) (dissoc connection :password))
           db (get-pool connection)]
       (jdbc/execute! db ["select 1"])
       (system/info system ::connected (:database connection) (dissoc connection :password))
       {:datasource db :connection/info connection})))

#_(defn stop [system]
    (system/stop-service
     system
     (when-let [^HikariDataSource conn (system/get-system-state system [:datasoruce])]
       (.close conn))))

(system/defmanifest
  {:description "postgresql service"
   :deps []
   :config
   {:port      {:type "integer" :default 5432 :validator pos-int?}
    :host      {:type "string"  :required true}
    :user      {:type "string"  :required true}
    :database  {:type "string"  :required true}
    :password  {:type "string"  :sensitive true :required true}
    :max-pool-size {:type "integer" :default 30 :validator pos-int?}
    :min-idle {:type "integer" :default 5 :validator pos-int?}
    :idle-timeout {:type "integer" :default 300000 :validator pos-int?}
    :connection-timeout {:type "integer" :default 20000 :validator pos-int?}}})

(system/defstart
  [context config]
  (let [connection config
        _ (system/info context ::connecting (:database connection) (dissoc connection :password))
        db (get-pool connection)]
    (jdbc/execute! db ["select 1"])
    (system/info context ::connected (:database connection) (dissoc connection :password))
    {:datasource db :connection/info connection}))

(system/defstop
  [context state]
  (when-let [^HikariDataSource conn (:datasource state)]
    (system/info context ::close "close connections")
    (.close conn)))


(comment

  (require '[pg.docker :as pgd])

  (pgd/delete-pg "test-pg")

  (def pg-config (pgd/ensure-pg "test-pg"))

  (def conn (pg/get-connection pg-config))
  (jdbc-execute! conn {:sql "select 1"})
  (.close conn)

  (def sys (system/start-system {:services ["pg"] :pg pg-config}))
  (execute! sys {:sql ["select 1"]})
  (system/stop-system sys))
