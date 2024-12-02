(ns pg-test
  (:require [system]
            [pg]
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

  (system/stop-system context)

  ;; context

  ;; (execute! context ["truncate test"])
  ;; (execute! context ["select count(*) from test"])
  ;; (execute! context ["truncate test"])
  ;; (copy-ndjson context "test" (fn [w] (doseq [i (range 100)] (w {:i i}))))

  )
