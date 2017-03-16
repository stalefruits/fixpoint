(ns fixpoint.datasource.postgresql
  "PostgreSQL Datasource Component

   Make sure the `org.postgresql/postgresql` dependency is available on your
   classpath."
  (:require [fixpoint.datasource.jdbc :as fix-jdbc]))

;; This is identical to the JDBC datasource, but just in case, we alias it
;; for future extensibility.

(defn make-datasource
  "Create a PostgreSQL JDBC datasource. Documents passed to this datasource need
   to have the format described in [[fixpoint.datasource.jdbc/make-datasource]].

   ```clojure
   {:db/table :people
    :name     \"me\"
    :age      28}
   ```
   "
  [id db-spec]
  (fix-jdbc/make-datasource id db-spec))
