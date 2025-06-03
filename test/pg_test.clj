(ns pg-test
  (:require [system]
            [pg]
            [pg.repo]
            [cheshire.core]
            [matcho.core :as matcho]
            [clojure.test :refer [deftest is]]))

(deftest test-pg

  (def cfg {:host "localhost" :port  5401 :database "context_pg" :user "admin" :password "admin"})

  (def context (system/start-system {:services ["pg"] :pg cfg}))

  (matcho/match
   (pg/execute! context {:sql "select 1"})
   [{:?column? 1}])


  (pg/execute! context {:sql "drop table if  exists test"})
  (pg/execute! context {:sql "create table if not exists test  (resource jsonb)"})

  (dotimes [i 20]
    (pg/copy context "copy test (resource) FROM STDIN csv quote e'\\x01' delimiter e'\\t'"
             (fn [w]
               (doseq [i (range 100)] (w (cheshire.core/generate-string {:a i}))))))

  (matcho/match
      (pg/execute! context {:sql ["select count(*) from test"]})
    [{:count 2000}])


  (def cnt (atom 0))
  (pg/fetch context ["select resource from test"] 100 "resource" (fn [x i] (swap! cnt inc)))

  (matcho/match @cnt 2000)

  (do
    (pg/execute! context {:sql "drop table if exists _migrations"})
    (pg/execute! context {:sql "drop table if exists patient"})
    (pg/execute! context {:sql "drop table if exists \"users\""})
    (pg/execute! context {:sql "drop table if exists \"user_sessions\""}))

  (pg/migrate-prepare context)

  (pg/migrate-up context "init")

  (matcho/match
      (pg.repo/get-table-definition context "patient")
    {:table "patient",
     :primary-key [:id],
     :columns
     {:id {:position 1, :type "int4", :required true},
      :birthdate {:position 4, :type "timestamptz"},
      :given {:position 2, :type "text"},
      :family {:position 3, :type "text"},
      :gender {:position 5, :type "text"}}})

  (matcho/match
      (pg/execute! context {:sql "select * from _migrations"})
    [{:id "init", :file "20250216211605_init",}])

  (pg/migrate-up context "add_pt_index")
  (matcho/match
      (pg/execute! context {:sql "select * from _migrations"})
    [{:id "init", :file "20250216211605_init",}
     {:id "add_pt_index", :file "20250216213534_add_pt_index",}])

  (is (thrown? Exception (pg/migrate-up context "with_error")))

  (pg/migrate-down context)

  (pg.migrations/read-migrations)

  (matcho/match
      (pg/execute! context {:sql "select * from _migrations"})
    [{:id "init", :file "20250216211605_init",}
     nil?])

  (pg/migrate-down context)

  (matcho/match (pg/execute! context {:sql "select * from _migrations"}) [nil?])

  (is (thrown? Exception (pg/migrate-up context)))

  (matcho/match
      (pg.repo/get-table-definition context "user_sessions")
    {:table "user_sessions",
     :primary-key [:id],
     :columns
     {:id {:position 1, :type "uuid", :required true},
      :user {:position 2, :type "int8"},
      :created_at {:position 3, :type "timestamptz"},
      :updated_at {:position 4, :type "timestamptz"}}})

  (matcho/match
      (pg.repo/get-table-definition context "patient")
    {:table "patient",
     :primary-key [:id],
     :columns
     {:id {:position 1, :type "int4", :required true},
      :birthdate {:position 4, :type "timestamptz"},
      :given {:position 2, :type "text"},
      :family {:position 3, :type "text"},
      :gender {:position 5, :type "text"}}})


  (pg/execute! context {:sql "select * from patient"})

  (matcho/match
      (pg.repo/upsert context {:table "patient" :resource {:birthdate "1980-05-03" :given "Nikolai" :gender "male"}})
    {:id int?
     :given "Nikolai",
     :birthdate inst?
     :gender "male"})


  (system/stop-system context)

  ;; context

  ;; (execute! context ["truncate test"])
  ;; (execute! context ["select count(*) from test"])
  ;; (execute! context ["truncate test"])
  ;; (copy-ndjson context "test" (fn [w] (doseq [i (range 100)] (w {:i i}))))

  )

(deftest test-copy-functions

  (def cfg {:host "localhost" :port  5401 :database "context_pg" :user "admin" :password "admin"})

  (def copy-context (system/start-system {:services ["pg"] :pg cfg}))

  (pg/execute! copy-context {:sql "drop table if exists _copy_test;"})
  (pg/execute! copy-context {:sql "create table if not exists _copy_test (id int primary key, label text, tags text[], resource jsonb);"})

  (pg/execute! copy-context {:sql "select * from _copy_test"})
  (pg/execute! copy-context {:sql "truncate _copy_test"})

  (def ci (pg/open-copy-manager copy-context "copy _copy_test (id,label,tags, resource) FROM STDIN csv DELIMITER E'\\x01' QUOTE E'\\x02'"))

  (pg/copy-write-column ci "1")
  (pg/copy-write-x01 ci)
  (pg/copy-write-column ci "item-1")
  (pg/copy-write-x01 ci)
  (pg/copy-write-array-column ci ["a", "b", "c"])
  (pg/copy-write-x01 ci)
  (pg/copy-write-json-column ci {:a 1 :b 2})
  (pg/copy-write-new-line ci)

  (pg/close-copy-manger copy-context ci)

  (matcho/match
      (pg/execute! copy-context {:sql "select * from _copy_test"})
      [{:id 1, :label "item-1" :tags ["a", "b", "c"] :resource {:a 1 :b 2}}])


  (system/stop-system copy-context)

  )

(deftest test-transactions
  (def cfg {:host "localhost" :port 5401 :database "context_pg" :user "admin" :password "admin"})
  (def context (system/start-system {:services ["pg"] :pg cfg}))

  (pg/execute! context {:sql "drop table if exists transaction_test"})
  (pg/execute! context {:sql "create table if not exists transaction_test (id int primary key, value text)"})

  ;; Test successful transaction
  (pg/with-transaction context
    (pg/execute! context {:sql ["insert into transaction_test (id, value) values (1, 'test1')"]})
    (pg/execute! context {:sql ["insert into transaction_test (id, value) values (2, 'test2')"]})
    (is (some? (:pg/transaction context))))
  
  (is (nil? (:pg/transaction context)))

  (matcho/match
   (pg/execute! context {:sql "select * from transaction_test"})
    [{:id 1, :value "test1"}
     {:id 2, :value "test2"}])

  ;; Test rollback transaction
  (try
    (pg/with-transaction context
      (is (some? (:pg/transaction context)))
      (pg/execute! context {:sql ["insert into transaction_test (id, value) values (3, 'test3')"]})
      (pg/execute! context {:sql ["insert into transaction_test (id, value) values (4, 'test4')"]})
      (throw (Exception. "Simulated failure")))
    (catch Exception _))

  (is (nil? (:pg/transaction context)))

  (matcho/match
   (pg/execute! context {:sql "select * from transaction_test"})
    [{:id 1, :value "test1"}
     {:id 2, :value "test2"}])

  (system/stop-system context))
