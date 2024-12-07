(ns pg.repo-test
  (:require [system]
            [pg.repo]
            [pg]
            [pg.repo.types :as tps]
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

  (t/testing "test work with existing table"
    (pg/execute! context {:sql "drop table if exists repo_test"})

    (pg/execute! context {:sql "create table if not exists repo_test (id text primary key, ts timestamptz default current_timestamp, name text)"})

    (pg.repo/get-table-definition context "repo_test")

    (pg.repo/valid-table-defintion? (pg.repo/get-table-definition context "repo_test"))

    (pg.repo/build-insert (pg.repo/get-table-definition context "repo_test") {:id "id" :name "name"})

    (matcho/match
     (pg.repo/insert context {:table "repo_test" :resource {:id "r1" :name "name"}})
     {:id "r1", :ts #(not (nil? %)) :name "name"})

    (pg.repo/insert context {:table "repo_test" :resource {:id "r2" :name "name" :something "else"}})

    (matcho/match
     (pg.repo/upsert context {:table "repo_test" :resource {:id "r2" :name "updated"}})
     {:id "r2", :ts #(not (nil? %)) :name "updated"})

    (pg.repo/select context {:table "repo_test" :match {:id "r1"}})

    (matcho/match
     (pg.repo/select context {:table "repo_test" :where [:= :id "r2"]})
     [{:id "r2", :name "updated"} nil?])

    (matcho/match
     (pg.repo/delete context {:table "repo_test" :match {:id "r2"}})
     [{:id "r2"} nil?])

    (matcho/match
     (pg.repo/select context {:table "repo_test"})
     [{:id "r1"} nil?])

    (matcho/match
     (pg.repo/select context {:table "repo_test"})
     [{:id "r1", :name "name"} nil?])

    )

  (t/testing "repo"

    (pg.repo/table-dsql
     {:table "patient"
      :primary-key [:id]
      :columns {:id       {:type tps/text}
                :resource {:type tps/jsonb}}})

    (pg.repo/register-repo
     context {:table "patient"
              :primary-key [:id]
              :defaults true
              :columns {:id {:type tps/text}
                        :resource {:type tps/jsonb}}})

    (matcho/match
     (pg.repo/get-table-definition context "patient")
     {:table "patient"
      :primary-key [:id]
      :columns {:id {:type "text"}
                :resource {:type "jsonb"}}})

    (matcho/match
     (pg.repo/upsert context {:table "patient" :resource {:id "pt-1" :name "name"}})
     {:id "pt-1", :name "name"})


    (pg.repo/upsert context {:table "patient" :resource {:id "pt-2" :name "name"}})

    (matcho/match
     (pg.repo/upsert context {:table "patient" :resource {:id "pt-1" :name "changed" :extra 1}})
     {:id "pt-1", :name "changed" :extra 1})

    (matcho/match
     (pg.repo/select context {:table "patient" :match {:id "pt-1"}})
     [{:id "pt-1", :name "changed" :extra 1}])

    (pg.repo/truncate context {:table "patient"})

    (pg.repo/load
     context {:table "patient"}
     (fn [insert]
       (doseq [i (range 10)]
         (insert {:id (str "r-" i) :name (str "pt-" i)}))))

    (matcho/match
     (pg.repo/select context {:table "patient" :order-by :id})
     [{:id "r-0", :name "pt-0"}
      {:id "r-1", :name "pt-1"}
      {:id "r-2", :name "pt-2"}
      {:id "r-3", :name "pt-3"}
      {:id "r-4", :name "pt-4"}
      {:id "r-5", :name "pt-5"}
      {:id "r-6", :name "pt-6"}
      {:id "r-7", :name "pt-7"}
      {:id "r-8", :name "pt-8"}
      {:id "r-9", :name "pt-9"}])


    (def rows (atom []))
    (pg.repo/fetch
     context {:table "patient" :order-by :id}
     (fn [row]
       (swap! rows conj row)))

    (matcho/match
     @rows
     [{:id "r-0", :name "pt-0"}
      {:id "r-1", :name "pt-1"}
      {:id "r-2", :name "pt-2"}
      {:id "r-3", :name "pt-3"}
      {:id "r-4", :name "pt-4"}
      {:id "r-5", :name "pt-5"}
      {:id "r-6", :name "pt-6"}
      {:id "r-7", :name "pt-7"}
      {:id "r-8", :name "pt-8"}
      {:id "r-9", :name "pt-9"}])


    )




  )
