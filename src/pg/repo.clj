(ns pg.repo
  (:require [system]
            [cheshire.core]
            [pg]
            [pg.repo.table]))


(system/defmanifest
  {:description "basic CRUD"
   :define-event {::on-update {}
                  ::on-create {}
                  ::on-delete {}
                  ::on-new-repo {}
                  }})

(defn primary-key-dsql [table-name]
  {:select {:column :kcu.column_name}
   :from {:tc :information_schema.table_constraints
          :kcu :information_schema.key_column_usage}
   :where {:join [:= :tc.constraint_name :kcu.constraint_name]
           :tbl  [:= :tc.table_name [:pg/param table-name]]}})

(defn- columns-dsql [table-name]
  {:select {:name :column_name
            :default :column_default
            :is_nullable :is_nullable
            :type :udt_name
            :position :ordinal_position}
   :from :information_schema.columns
   :where [:= :table_name [:pg/param table-name]]})

(defn get-table-definition [context table-name]
  {:table table-name
   :primary-key (->>
                 (pg/execute! context {:dsql (primary-key-dsql table-name)})
                 (mapv (fn [x] (keyword (:column x))))
                 (into []))
   :columns (->> (pg/execute! context {:dsql (columns-dsql table-name)})
                 (reduce (fn [acc col]
                           (assoc acc (keyword (:name col))
                                  (cond-> {:position (:position col) :type (:type col)}
                                    (= "NO" (:is_nullable col)) (assoc :required true)
                                    (:default col) (assoc :default (:default col)))))
                         {}))})


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
       :constraint    {:primary-key [:id :partition]}
       :columns (->> (:columns table-def)
                     (reduce (fn [cols [col-name col-def]]
                               (assoc cols col-name (column-definition col-def))) {}))}
    (:primary-key table-def)
    (assoc-in [:constraint :primary-key] (:primary-key table-def))))

;;TODO: check that it already exists
(defn register-repo [context table-def]
  (pg.repo.table/assert table-def)
  (pg/execute! context {:dsql (table-dsql table-def)}))

(defn build-insert-columns [table-def resource cols]
  (-> (->> (dissoc (:columns table-def) :resource)
           (sort-by :position)
           (reduce (fn [acc [col-name _col-def]]
                     (if-let [v (get resource col-name)]
                       (assoc acc col-name [:pg/param v])
                       acc)
                     ) {}))
      (assoc :resource [:pg/param (cheshire.core/generate-string (apply dissoc resource cols))])))

;; TODO: basic validation by table schema
(defn build-insert [table-def resource]
  (let [cols (into [] (disj (into #{} (keys (:columns table-def))) :resource))]
    {:ql/type :pg/insert
     :into (:table table-def)
     :value (build-insert-columns table-def resource cols)
     :returning :*}))

(defn insert [context {table :table resource :resource}]
  (let [table-def (get-table-definition context table)]
    (-> (pg/execute! context {:dsql (build-insert table-def resource)})
        first)))

(defn build-update-columns [table-def resource]
  (->> (:columns table-def)
       (sort-by :position)
       (reduce (fn [acc [col-name _col-def]]
                 (if-not (nil? (get resource col-name))
                   (assoc acc col-name (keyword (str "EXCLUDED." (name col-name))))
                   acc)
                 ) {:resource :EXCLUDED.resource})))

;; TODO: if no resource column skip resource logic
;; TODO: fail on extra columns
(defn build-upsert [table-def resource]
  (let [cols (into [] (disj (into #{} (keys (:columns table-def))) :resource))]
    {:ql/type :pg/insert
     :into (:table table-def)
     :value (build-insert-columns table-def resource cols)
     :on-conflict {:on (:primary-key table-def)
                   :do {:set (build-update-columns table-def resource)}}
     :returning :*}))

;;TODO: add events
(defn upsert [context {table :table resource :resource}]
  (let [table-def (get-table-definition context table)]
    (-> (pg/execute! context {:dsql (build-upsert table-def resource)})
        first)))

(defn delete [context {table :table where :where match :match}]
  )

(defn read [context {table :table match :match where :where}]
  )

(defn load [context {table :table} f]
  )

(defn select [context {table :table where :where}]
  (pg/execute! context {:dsql {:select :* :from (keyword table) :where where}}))


;; TODO cache table defs
