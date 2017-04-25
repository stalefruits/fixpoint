(ns fixpoint.datasource.mysql
  "MySQL JDBC Datasource Component

   Make sure the `mysql/mysql-connector-java` dependency is available on your
   classpath."
  (:require [fixpoint.datasource.jdbc :as fix-jdbc]
            [camel-snake-kebab.core :as csk]
            [clojure.java.jdbc :as jdbc]))

;; ## MySQL Adjustments
;;
;; We need to do an additional query to fetch the inserted row. For this, we
;; need to know the primary key column and the generated key.

(defn- verify-inserted-row!
  [table-name column document-id row]
  (when-not row
    (throw
      (IllegalStateException.
        (format
          "Could not fetch inserted row using column '%s.%s' and value: %s"
          table-name
          column
          document-id)))))

(defn- query-inserted-row
  [db
   {:keys [db/table db/primary-key]
    :or {db/primary-key :id}}
   {:keys [generated_key]}]
  (when generated_key
    (let [table-name (csk/->snake_case_string table)
          column     (csk/->snake_case_string primary-key)
          [row] (->> [(format "select * from %s where %s = ? limit 1"
                              table-name
                              column)
                      generated_key]
                     (jdbc/query db))]
      (verify-inserted-row! table-name column document-id row)
      row)))

(defn- remove-primary-key-field
  [document]
  (dissoc document :db/primary-key))

;; ## Datasource

(defn make-datasource
  "Create a MySQL JDBC datasource. Documents passed to this datasource need
   to have the format described in [[fixpoint.datasource.jdbc/make-datasource]].

   ```clojure
   {:db/table :people
    :name     \"me\"
    :age      28}
   ```

   If the respective table's primary key column is not `:id`, it has to
   additionally be specified using a `:db/primary-key` value.

   ```clojure
   {:db/table       :people
    :db/primary-key :uuid
    :name           \"me\"
    :age            28}
   ```

   (The reason for this is that, for MySQL, we need to perform a `SELECT`
   statement after insertion, since all we get back is the `:generated_key`.)"
  [id db-spec]
  (fix-jdbc/make-datasource
    id
    db-spec
    {:pre-fn  remove-primary-key-field
     :post-fn query-inserted-row}))
