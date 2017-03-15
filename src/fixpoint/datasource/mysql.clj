(ns fixpoint.datasource.mysql
  (:require [fixpoint.datasource.jdbc :as fix-jdbc]
            [camel-snake-kebab.core :as csk]
            [clojure.java.jdbc :as jdbc]))

;; ## MySQL Adjustments
;;
;; We need to do an additional query to fetch the inserted row. For this, we
;; need to know the primary key column and the generated key.

(defn- query-inserted-row
  [db
   {:keys [db/table db/primary-key]
    :or {db/primary-key :id}}
   {:keys [generated_key]}]
  (when generated_key
    (->> [(format "select * from %s where %s = ?"
                  (csk/->snake_case_string table)
                  (csk/->snake_case_string primary-key))
          generated_key]
         (jdbc/query db)
         (first))))

(defn- remove-primary-key-field
  [document]
  (dissoc document :db/primary-key))

;; ## Datasource

(defn make-datasource
  [id db-spec]
  (fix-jdbc/make-datasource
    id
    db-spec
    {:pre-fn  remove-primary-key-field
     :post-fn query-inserted-row}))
