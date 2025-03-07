(ns pg.repo
  (:require [system]
            [cheshire.core]
            [pg]
            [pg.repo.table]
            [clojure.string :as str])
  (:refer-clojure :exclude [read load]))


(system/defmanifest
  {:description "basic CRUD"
   :deps ["pg"]
   :define-event {::on-update {}
                  ::on-create {}
                  ::on-delete {}
                  ::on-new-repo {}
                  }})

(defn primary-key-dsql [table-name table-schema]
  {:select {:column :kcu.column_name}
   :from {:tc :information_schema.table_constraints
          :kcu :information_schema.key_column_usage}
   :where {:join [:= :tc.constraint_name :kcu.constraint_name]
           :tbl  [:= :tc.table_name [:pg/param table-name]]
           :sch  [:= :tc.table_schema [:pg/param table-schema]]}})

(defn- columns-dsql [table-name table-schema]
  {:select {:name :column_name
            :default :column_default
            :is_nullable :is_nullable
            :type :udt_name
            :position :ordinal_position}
   :from :information_schema.columns
   :where [:and
           [:= :table_name [:pg/param table-name]]
           [:= :table_schema [:pg/param table-schema]]]})

(defn clear-table-definitions-cache [context]
  (system/clear-context-cache context [:tables]))

;;TODO: check that table exists
;;TODO: add schemas suport
(defn get-table-definition [context table-name]
  (system/get-context-cache
   context [:tables table-name]
   (fn []
     (let [[table-schema table-name*] (if (str/includes? (name table-name) ".")
                                       (str/split (name table-name) #"\." 2)
                                       ["public" table-name])]
       (when-not (first (pg/execute! context {:sql ["SELECT FROM information_schema.tables WHERE table_name = ? and table_schema = ?"
                                                    table-name* table-schema]}))
         (throw (Exception. (str "Table " (pr-str table-name) " does not exists"))))
       {:table table-name
        :primary-key (->>
                      (pg/execute! context {:dsql (primary-key-dsql table-name* table-schema)})
                      (mapv (fn [x] (keyword (:column x))))
                      (into []))
        :columns (->> (pg/execute! context {:dsql (columns-dsql table-name* table-schema)})
                      (reduce (fn [acc col]
                                (assoc acc (keyword (:name col))
                                       (cond-> {:position (:position col) :type (:type col)}
                                         (= "NO" (:is_nullable col)) (assoc :required true)
                                         (:default col) (assoc :default (:default col)))))
                              {}))}))))

(defn valid-table-defintion? [table-def]
  (pg.repo.table/assert table-def))

(defn column-definition [col-def]
  (into []
        (concat
         [(:type col-def)]
         (when (:required col-def)
           ["not null"])
         (when (:default col-def)
           [:DEFAULT (:default col-def)]))))

(defn table-dsql [table-def]
  (cond->
      {:ql/type :pg/create-table
       :table-name (:table table-def)
       :if-not-exists true
       :columns (->> (:columns table-def)
                     (reduce (fn [cols [col-name col-def]]
                               (assoc cols col-name (column-definition col-def))) {}))}
    (:primary-key table-def)
    (assoc-in [:constraint :primary-key] (:primary-key table-def))))

(defn drop-repo [context {table :table}]
  (pg/execute! context {:dsql {:ql/type :pg/drop-table :table-name table :if-exists true}}))

(defn create-indexes [context table-def]
  (let [tbl (keyword (:table table-def))]
    (doseq [[col-name col] (:columns table-def)]
      (when (:index col)
        (let [dql {:ql/type :pg/index
                   :index (keyword (str (name tbl) "_" (name col-name) "_idx"))
                   :if-not-exists true
                   :concurrently true
                   :on tbl
                   :expr [col-name]}]
          (pg/execute! context {:dsql dql}))))))

;;TODO: check that it already exists
(defn register-repo [context table-def]
  (pg.repo.table/assert table-def)
  (pg/execute! context {:dsql (table-dsql table-def)})
  (create-indexes context table-def))

(defn process-resource [resource]
  (when resource
    (if-let [res  (:resource resource)]
      (merge (dissoc resource :resource) res)
      resource)))

(defn build-insert-columns [table-def resource]
  (let [columns (:columns table-def)
        resource? (:resource columns)
        cols (keys (dissoc columns :resource))]
    (-> (->> (dissoc columns :resource)
             (sort-by :position)
             (reduce (fn [acc [col-name col-def]]
                       (if-let [v (get resource col-name)]
                         (cond
                           (= "jsonb" (:type col-def))
                           (assoc acc col-name [:pg/param (cheshire.core/generate-string v)])
                           :else
                           (if (vector? v)
                             (assoc acc col-name [:pg/array-param :text v])
                             (assoc acc col-name [:pg/param v])))
                         acc))
                     {}))
        (cond->
            (and resource? (= (:type resource?) "jsonb"))
            (assoc :resource [:pg/param (cheshire.core/generate-string (apply dissoc resource cols))])))))

;; TODO: basic validation by table schema
(defn build-insert [table-def resource]
  {:ql/type :pg/insert
   :into (:table table-def)
   :value (build-insert-columns table-def resource)
   :returning :*})

(defn insert [context {table :table resource :resource}]
  (let [table-def (get-table-definition context table)]
    (->> (pg/execute! context {:dsql (build-insert table-def resource)})
        (mapv process-resource)
        first)))

(defn build-update-columns [table-def resource]
  (let [columns (:columns table-def)
        resource? (:resource columns)
        update-map (cond-> {}
                     (and resource? (= "jsonb" (:type resource?)))
                     (assoc :resource :EXCLUDED.resource))]
    (->> columns
         (sort-by :position)
         (reduce (fn [acc [col-name _col-def]]
                   (if-not (nil? (get resource col-name))
                     (assoc acc
                            (keyword (format "\"%s\"" (name col-name)))
                            (keyword (str "EXCLUDED." (format "\"%s\"" (name col-name)))))
                     acc)
                   ) update-map))))

;; TODO: if no resource column skip resource logic
;; TODO: fail on extra columns
(defn build-upsert [table-def resource]
  {:ql/type :pg/insert
   :into (:table table-def)
   :value (build-insert-columns table-def resource)
   :on-conflict {:on (:primary-key table-def)
                 :do {:set (build-update-columns table-def resource)}}
   :returning :*})

;;TODO: add events
;;TODO: test for rextra columns
(defn upsert [context {table :table resource :resource}]
  (let [table-def (get-table-definition context table)]
    (->> (pg/execute! context {:dsql (build-upsert table-def resource)})
         (mapv process-resource)
         first)))

(defn match-to-where [match]
  (->> match
       (reduce (fn [acc [k v]]
                 (if (vector? v)
                   (assoc acc k [:in k [:pg/params-list v]])
                   (assoc acc k [:= k [:pg/param v]])))
               {})))

(defn resource-expression [table-def]
  (let [cols (:columns table-def)
        obj (->> (dissoc cols :resource)
                 (reduce (fn [acc [k v]]
                           (assoc acc k k))
                         {:ql/type :jsonb/obj}))]
    (if (:resource cols)
      [:|| :resource obj]
      obj)))

;;TODO merge where and match
(defn select [context {table :table sel :select  where :where match :match order-by :order-by limit :limit}]
  (assert table "table is required")
  (let [where (or where (match-to-where match))]
    (-> (->> (pg/execute! context {:dsql {:select :* :from (keyword table) :where where :order-by order-by :limit limit}})
             (mapv process-resource))
        (cond->> sel (mapv (fn [x] (select-keys x sel)))))))

;;TODO: reduce
(defn fetch [context {table :table where :where match :match order-by :order-by limit :limit fetch-size :fetch-size} f]
  (let [table-def (get-table-definition context table)
        where (or where (match-to-where match))
        dsql {:select {:resource (resource-expression table-def)}
              :from (keyword table)
              :where where :order-by order-by :limit limit}]
    (pg/fetch context (pg/format-dsql dsql) (or fetch-size 1000) :resource
              (fn [r i] (f (pg/json-parse r))))))

(defn read [context {table :table match :match :as opts}]
  (-> (select context opts)
      first))

(defn delete-dsql [table where match]
  {:ql/type :pg/delete
   :from (keyword table)
   :where (or where (match-to-where match))
   :returning :*})

(defn delete [context {table :table where :where match :match}]
  (assert (and table (seq (or where match))))
  (->> (pg/execute! context {:dsql (delete-dsql table where match)})
       (mapv process-resource)))

;; TODO: fix sql injection
(defn truncate [context {table :table}]
  (pg/execute! context {:sql (str "TRUNCATE " table)}))

(defn load [context {table :table} f]
  (let [table-def (get-table-definition context table)
        columns (keys (:columns table-def))
        columns-resource (keys (dissoc (:columns table-def) :resource))
        sql (str "COPY " (:table table-def) "( " (->> columns (mapv name) (str/join ",")) " )  FROM STDIN csv quote e'\\x01' delimiter e'\\t'" )]
    (system/info context ::copy sql)
    (pg/load context sql
             (fn [w wt wnl]
               (let [wr (fn [res]
                          (loop [[c & cs] columns]
                            (if c
                              (do
                                (if (= :resource c)
                                  (w (cheshire.core/generate-string (apply dissoc res columns-resource)))
                                  (when-let [v (get res c)]
                                    (w (str v))))
                                  (when (seq cs) (wt))
                                  (recur cs))
                              (wnl))))]
                 (f wr))))
    {}))

(defn open-loader [context {table :table}]
 (let [table-def (get-table-definition context table)
        columns (keys (:columns table-def))
        columns-resource (keys (dissoc (:columns table-def) :resource))
        sql (str "COPY " (:table table-def) "( " (->> columns (mapv name) (str/join ",")) " )  FROM STDIN csv quote e'\\x01' delimiter e'\\t'" )]
    (system/info context ::open-copy-manager sql)
    {:copy-manager (pg/open-copy-manager context sql)
     :sql sql
     :columns columns
     :columns-resource columns-resource}))

(defn close-loader [{cm :copy-manager}]
  (pg/close-copy-manger cm))

(defn load-resource [{columns :columns columns-resource :columns-resource cm :copy-manager} res]
  (loop [[c & cs] columns]
    (if c
      (do
        (if (= :resource c)
          (pg/copy-write-column cm (cheshire.core/generate-string (apply dissoc res columns-resource)))
          (when-let [v (get res c)]
            (pg/copy-write-column cm (str v))))
        (when (seq cs) (pg/copy-write-tab cm))
        (recur cs))
      (pg/copy-write-new-line cm))))

