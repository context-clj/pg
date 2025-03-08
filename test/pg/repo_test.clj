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
    (pg.repo/clear-table-definitions-cache context)

    (pg/execute! context {:sql "create table if not exists repo_test (id text primary key, ts timestamptz default current_timestamp, name text, obj jsonb)"})

    (pg.repo/get-table-definition context "repo_test")

    (pg.repo/valid-table-defintion? (pg.repo/get-table-definition context "repo_test"))

    (pg.repo/build-insert (pg.repo/get-table-definition context "repo_test") {:id "id" :name "name" :obj {:a 1 :b {:c 1}}})

    (matcho/match
        (pg.repo/insert context {:table "repo_test" :resource {:id "r1" :name "name" :obj {:a 1 :b {:c 1}}}})
      {:id "r1", :ts #(not (nil? %)) :name "name" :obj {:a 1 :b {:c 1}}})

    (pg.repo/insert context {:table "repo_test" :resource {:id "r2" :name "name" :something "else"}})

    (matcho/match
     (pg.repo/upsert context {:table "repo_test" :resource {:id "r2" :name "updated" :obj {:a 2}}})
     {:id "r2", :ts #(not (nil? %)) :name "updated" :obj {:a 2 :b nil?}})

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

    (pg.repo/drop-repo context {:table "patient"})


    (pg.repo/table-dsql
     {:table "patient"
      :primary-key [:id]
      :columns {:id       {:type tps/text}
                :ts       {:type tps/timestamptz :default "current_timestamp" :index true}
                :tags     {:type "text[]"}
                :jcol     {:type "jsonb"}
                :resource {:type tps/jsonb}}})


    (pg.repo/register-repo
     context
     {:table "patient"
              :primary-key [:id]
              :defaults true
              :columns {:id       {:type tps/text}
                        :ts       {:type tps/timestamptz :default "current_timestamp" :index true}
                        :tags     {:type "text[]"}
                        :jcol     {:type "jsonb"}
                        :resource {:type tps/jsonb}}})

    (matcho/match
     (pg.repo/get-table-definition context "patient")
     {:table "patient"
      :primary-key [:id]
      :columns {:id {:type "text"}
                :resource {:type "jsonb"}}})

    (matcho/match
     (pg.repo/upsert context {:table "patient" :resource {:id "pt-1" :name "name" :jcol {:a 1} :tags ["a" "b"]}})
     {:id "pt-1", :name "name" :jcol {:a 1} :tags ["a" "b"]})


    (pg.repo/upsert context {:table "patient" :resource {:id "pt-2" :name "name"}})

    (matcho/match
     (pg.repo/upsert context {:table "patient" :resource {:id "pt-1" :name "changed" :extra 1}})
     {:id "pt-1", :name "changed" :extra 1})

    (matcho/match
     (pg.repo/select context {:table "patient" :match {:id "pt-1"}})
     [{:id "pt-1", :name "changed" :extra 1}])

    (pg.repo/select context {:table "patient"})

    (pg/execute! context {:sql "select * from patient"})

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

    (pg/execute! context {:sql "truncate patient"})


    (pg/execute! context {:sql "drop table if exists just_test"})
    (pg/execute! context {:sql "create table just_test (tagsi int[], tags text[], tagsd timestamptz[])"})
    (matcho/match
        (pg.repo/get-table-definition  context "just_test")
      {:table "just_test",
       :primary-key [],
       :columns
       {:tagsi {:position 1, :type "int4[]"},
        :tags {:position 2, :type "text[]"},
        :tagsd {:position 3, :type "timestamptz[]"}}})

    (pg.repo/clear-table-definitions-cache context)
    (pg.repo/get-table-definition  context "patient")

    (pg/execute! context {:sql ["SELECT * FROM information_schema.columns WHERE table_name = ?" "patient"]})

    (def cm (pg.repo/open-loader context {:table "patient"}))

    (doseq [i (range 5)]
      (pg.repo/load-resource context cm {:id (str "rc" i) :name (str "pt-" i) :jcol {:a i} :tags [(str "t" i) (str "t" i i)]}))

    (pg.repo/close-loader context cm)


    (matcho/match
        (pg.repo/select context {:table "patient"})
      [{:id "rc0" :name "pt-0" :jcol {:a 0} :tags ["t0" "t00"]}
       {:id "rc1" :name "pt-1" :jcol {:a 1} :tags ["t1" "t11"]}
       {:id "rc2" :name "pt-2" :jcol {:a 2} :tags ["t2" "t22"]}
       {:id "rc3" :name "pt-3" :jcol {:a 3} :tags ["t3" "t33"]}
       {:id "rc4" :name "pt-4" :jcol {:a 4} :tags ["t4" "t44"]}
       nil?]))




  )
