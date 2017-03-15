(ns fixpoint.datasource.postgresql
  (:require [fixpoint.datasource.jdbc :as fix-jdbc]))

;; This is identical to the JDBC datasource, but just in case, we alias it
;; for future extensibility.

(defn make-datasource
  [id db-spec]
  (fix-jdbc/make-datasource id db-spec))
