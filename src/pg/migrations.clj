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

(defn process-migration-entry [entry entry-name]
  (let [file (str/replace entry-name #"\.sql$" "")]
    (merge
     (parse-migration (slurp entry))
     {:id file
      :file file})))

(defn is-sql-migration? [entry]
  (and (not (.isDirectory entry))
       (str/ends-with? (.getName entry) ".sql")))

(defn process-jar-entries [conn loader]
  (let [jar-entries (enumeration-seq (.entries (.getJarFile conn)))]
    (->> jar-entries
         (filter is-sql-migration?)
         (sort-by #(.getName %))
         (map #(process-migration-entry
                (.getResourceAsStream loader (.getName %))
                (.getName %))))))

(defn process-filesystem-entries [file]
  (->> (file-seq file)
       (filter is-sql-migration?)
       (sort-by #(.getName %))
       (map #(process-migration-entry % (.getName %)))))

(defn read-migrations []
  (let [path "migrations/"
        loader (.getContextClassLoader (Thread/currentThread))
        resources (enumeration-seq (.getResources loader path))]
    (->> resources
         (mapcat
          (fn [^java.net.URL url]
            (let [conn (.openConnection url)]
              (if (instance? java.net.JarURLConnection conn)
                (process-jar-entries conn loader)
                (process-filesystem-entries (io/file (.getPath url)))))))
         (into []))))

(comment
  (generate-migration "init")
  (generate-migration "add_pt_index")
  (generate-migration "with_error")

  (read-migrations)

  )


