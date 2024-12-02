(ns pg.repo-test
  (:require [system]
            [pg.repo]
            [pg]
            [clojure.test :as t]
            [matcho.core :as matcho]))


;; (reset! context-atom nil)

(defonce context-atom (atom nil))

(def cfg {:host "localhost" :port  5401 :database "context_pg" :user "admin" :password "admin"})

(defn ensure-context []
  (when-not @context-atom
    (println :connect)
    (def context (system/start-system {:services ["pg" "pg.repo"] :pg cfg}))
    (reset! context-atom context)))

;; (system/stop-system context)

(t/deftest test-pg-repo

  (ensure-context)
  context

  (pg/execute! context {:sql "drop table if exists repo_test"})

  (pg/execute! context {:sql "create table if not exists repo_test (id text primary key, ts timestamptz default current_timestamp, resource jsonb)"})

  (pg.repo/get-table-definition context "repo_test")

  (pg.repo/valid-table-defintion? (pg.repo/get-table-definition context "repo_test"))

  (pg.repo/build-insert (pg.repo/get-table-definition context "repo_test") {:id "id" :name "name"})

  (matcho/match
   (pg.repo/insert context {:table "repo_test" :resource {:id "r1" :name "name"}})
   {:id "r1", :ts #(not (nil? %)) :resource {:name "name"}})

  (pg.repo/insert context {:table "repo_test" :resource {:id "r2" :name "name" :something "else"}})
  (pg.repo/upsert context {:table "repo_test" :resource {:id "r2" :name "updated" :something "else"}})
  (pg.repo/delete context {:table "repo_test" :match {:id "r2"}})

  (pg.repo/select context {:table "repo_test" :match {:id "r1"}})

  (matcho/match
   (pg.repo/select context {:table "repo_test" :where [:= :id "r1"]})
   [{:id "r1", :resource {:name "name"}}])

  (matcho/match
   (pg.repo/select context {:table "repo_test" :match {:id "r1"}})
   [{:id "r1", :resource {:name "name"}}])

  (pg.repo/table-dsql {:table "patient"
                       :primary-key [:id]
                       :columns {:id {:type "text"}
                                 :resource {:type "jsonb"}}})

  (pg.repo/register-repo
   context {:table "patient"
            :primary-key [:id]
            :columns {:id {:type "text"}
                      :resource {:type "jsonb"}}})

  (matcho/match
   (pg.repo/get-table-definition context "patient")
   {:table "patient"
    :primary-key [:id]
    :columns {:id {:type "text"}
              :resource {:type "jsonb"}}})

  (matcho/match
   (pg.repo/upsert context {:table "patient" :resource {:id "pt-1" :name "name"}})
   {:id "pt-1", :resource {:name "name"}})

  (matcho/match
   (pg.repo/upsert context {:table "patient" :resource {:id "pt-1" :name "changed"}})
   {:id "pt-1", :resource {:name "changed"}})

  (pg.repo/load
   context {:table "patient"}
   (fn [insert]
     (doseq [i (range 100)]
       (insert {:id (str "r-" i) :name (str "pt-" i)}))))


  )
