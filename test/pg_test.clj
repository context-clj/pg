(ns pg-test
  (:require [system]
            [pg]
            [pg.repo]
            [cheshire.core]
            [matcho.core :as matcho]
            [clojure.test :as t]))



(t/deftest test-pg

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

  (t/is (thrown? Exception (pg/migrate-up context "with_error")))

  (pg/migrate-down context)

  (pg.migrations/read-migrations)

  (matcho/match
      (pg/execute! context {:sql "select * from _migrations"})
    [{:id "init", :file "20250216211605_init",}
     nil?])

  (pg/migrate-down context)

  (matcho/match (pg/execute! context {:sql "select * from _migrations"}) [nil?])

  (t/is (thrown? Exception (pg/migrate-up context)))

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
