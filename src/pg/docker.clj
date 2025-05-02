(ns pg.docker
  (:require [clojure.java.shell :as shell]
            [clojure.string :as str]
            [pg]))


(defn free-port []
  (with-open [socket (java.net.ServerSocket. 0)]
    (.getLocalPort socket)))

(defn cli-opts [config]
  (->> config
       (mapcat (fn [[k v]]
                 (let [nm (if (= 1 (count (name k)))
                            (str "-" (name k))
                            (str "--" (name k)))]
                   (cond
                     (string? v) [nm v]
                     (map? v) (->> v (mapcat (fn [[k v]] [nm (str (name k) "=" v)])))
                     (vector? v) [nm (str/join ":" v)]
                     (boolean? v) [nm]
                     :else [nm v]))))))

(comment
  (cli-opts
   {:name "test-pg"
    :e {:POSTGRES_DB "pg"
        :POSTGRES_USER "user"
        :POSTGRES_PASSWORD "pwd"}
    :v ["postgres-data" "/var/lib/postgresql/data"]
    :p [5432 5432]
    :d "postgres"}))


(defn docker [& args]
  (let [cmds (->> args (mapcat (fn [x] (cond (vector? x) x (map? x) (cli-opts x) :else [x]))))
        _ (println cmds)
        result (apply shell/sh "docker" cmds)]
    (if (zero? (:exit result))
      {:success true
       :output (str/split (str/trim (:out result)) #"\n")}
      {:success false
       :error (str/split (str/trim (:err result)) #"\n")
       :exit-code (:exit result)})))

(defn wait-connection [pg-config & [i]]
  (let [i (or i 0)]
    (if (> i 10)
      (throw (Exception. "could not connect"))
      (try
        (with-open [conn (pg/get-connection pg-config)]
          (pg/jdbc-execute! conn {:sql "select 1"}))
        (catch Exception e
          (println (.getMessage e))
          (Thread/sleep 1000)
          (wait-connection pg-config (inc i)))))))

(defn create-pg [pg-name db user password port]
  (docker "run" {:name pg-name
                 :shm-size "1g"
                 :e {:POSTGRES_DB db
                     :POSTGRES_USER user
                     :POSTGRES_PASSWORD password}
                 :v [(str "./.data/" pg-name) "/var/lib/postgresql/data"]
                 :p [port 5432]
                 :d "postgres"}))

(defn delete-pg [pg-name]
  (docker "rm" "--force" pg-name))

(defn get-port [pg-name]
  (if-let [port (-> (docker ["port" pg-name]) :output first (str/split #":") (last))]
    (Integer/parseInt port)
    (throw (Exception. " could not get port"))))

(defn ensure-pg
  "returns postgresql config"
  [pg-name & opts]
  (let [port (free-port)
        user "postgres"
        db   "postgres"
        password pg-name
        port (if (:success  (docker (into ["start" pg-name] opts)))
               (get-port pg-name)
               (do (create-pg pg-name db user password port)
                   port))
        config {:user user
                :password password
                :host "localhost"
                :database db
                :port port}]
    (wait-connection config)
    config))


(comment
  (require '[system])

  (docker "ps")

  (free-port)

  (ensure-pg "test-pg")

  (docker "rm" "--force" "test-pg")

  (ensure-pg "test-pg")

  (def pg-config (ensure-pg "test-pg"))

  (def conn (pg/get-connection pg-config))
  (pg/jdbc-execute! conn {:sql "select 1"})
  (.close conn)


  (def sys (system/start-system {:services ["pg"] :pg pg}))

  (pg/execute! sys {:sql ["select 1"]})

  (system/stop-system sys)



  )

