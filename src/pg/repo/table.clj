(ns pg.repo.table
  (:require [clojure.spec.alpha :as s]))

(def datatypes #{"text" "timestamptz" "jsonb" "uuid"})
(s/def ::type (s/and string? #(contains? datatypes %)))
(s/def ::default string?)
(s/def ::required boolean?)
(s/def ::position int?)

;; Column specification
(s/def ::column-spec
  (s/keys :req-un [::type]
          :opt-un [::default ::position ::required]))

;; Map of column names to their specifications
(s/def ::columns
  (s/map-of keyword? ::column-spec))

;; Primary keys must be a sequence of keywords
(s/def ::primary-key
  (s/coll-of keyword? :kind vector?))

;; Table name must be a string
(s/def ::table string?)

;; Complete table schema specification
(s/def ::table-schema
  (s/keys :req-un [::table ::columns]
          :opt-un [::primary-key]))

(defn valid? [table-def]
  (s/valid? ::table-schema table-def))

(defn explain [table-def]
  (with-out-str (s/explain ::table-schema table-def)))

(defn explain-data [table-def]
  (s/explain-data ::table-schema table-def))

(defn assert [table-def]
  (when-not (valid? table-def)
    (throw (Exception. (str (explain table-def))))))

(comment
  (explain {})

  )
