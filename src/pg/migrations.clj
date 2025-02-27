(ns pg.migrations
  (:require [clojure.java.io :as io]
            [clojure.string :as str]))

(defn time-prefix []
  (-> (java.time.Instant/now)
      (.atZone (java.time.ZoneId/systemDefault))
      (.format (java.time.format.DateTimeFormatter/ofPattern "yyyyMMddHHmmss"))))

(defn generate-migration [name]
  (let [file (str (System/getProperty "user.dir") "/resources/migrations/" (time-prefix) "_" name ".sql")]
    (io/make-parents (io/file file))
    (spit file (str "-- " name "\n--$up\n--$\n--$down\n--$"))))

(defn append-acc [acc mode cur]
  (if (seq cur)
    (update acc mode conj (str/join "\n" cur))
    acc))

(defn parse-migration [text]
  (loop [mode :up
         cur []
         acc {:up [] :down []}
         [l & ls] (str/split text #"\n")]
    (cond
      (nil? l)
      (append-acc acc mode cur)
      (= "--$up" (str/trim l))
      (recur :up   [] (append-acc acc mode cur) ls)

      (= "--$down" (str/trim l))
      (recur :down [] (append-acc acc mode cur) ls)

      (= "--$" (str/trim l))
      (recur mode [] (append-acc acc mode cur) ls)

      :else
      (recur mode (conj cur l) acc ls)
      )))

(defn read-migrations []
  (->> (file-seq (io/file (io/resource "migrations")))
       (filter #(str/ends-with? (.getName %) ".sql"))
       (sort-by #(.getName %))
       (mapv (fn [x]
               (merge (parse-migration (slurp x))
                      {:path (.getPath x)
                       :id (second (str/split (str/replace (.getName x) #"\.sql$" "") #"_" 2))
                       :file (str/replace (.getName x) #"\.sql$" "")})))))

(comment
  (generate-migration "init")
  (generate-migration "add_pt_index")
  (generate-migration "with_error")

  (read-migrations)

  )


