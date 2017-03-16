(ns fixpoint.datasource.jdbc
  "Generic JDBC datasource component."
  (:require [fixpoint.core :as fix]
            [camel-snake-kebab
             [core :as csk]
             [extras :refer [transform-keys]]]
            [clojure.java.jdbc :as jdbc]))

;; ## Logic

(defn- run-with-transaction-rollback
  [{:keys [db] :as this} f]
  (jdbc/with-db-transaction [tx db]
    (jdbc/db-set-rollback-only! tx)
    (f (assoc this :db tx))))

(defn- table-for
  [{:keys [db/table] :as document}]
  (assert table
          (str "no ':db/table' key given in JDBC fixture: "
               (pr-str document)))
  (csk/->snake_case_string table))

(defn- prepare-for-insert
  [document]
  (->> (dissoc document :db/table)
       (transform-keys csk/->snake_case_string)))

(defn- read-after-insert
  [{:keys [db/tags]} result]
  (let [result' (transform-keys csk/->kebab-case-keyword result)]
    {:data result'
     :tags (vec tags)}))

(defn- insert!
  [{:keys [db pre-fn post-fn]} document]
  (let [table     (table-for document)
        document' (prepare-for-insert document)]
    (if-not (empty? document')
      (->> document'
           (pre-fn)
           (jdbc/insert! db table)
           (first)
           (post-fn db document)
           (read-after-insert document)))))

;; ## Protocol

(defprotocol JDBCDatasource
  "Protocol for JDBC datasources."
  (get-db-spec [this]
    "Retrieve the JDBC datasource's database spec.")
  (set-db-spec [this new-db-spec]
    "Set the JDBC datasource's database spec."))

;; ## Datasource

(defrecord Database [id db pre-fn post-fn]
  JDBCDatasource
  (get-db-spec [_]
    db)
  (set-db-spec [this new-db-spec]
    (assoc this :db new-db-spec))

  fix/Datasource
  (datasource-id [_]
    id)
  (start-datasource [this]
    this)
  (stop-datasource [this]
    this)
  (run-with-rollback [this f]
    (run-with-transaction-rollback this f))
  (insert-document! [this document]
    (insert! this document))
  (as-raw-datasource [_]
    db))

(defn make-datasource
  "Create a JDBC datasource with the given `id`. `overrides` can contain:

   - `:pre-fn`: to be applied to each document before insertion,
   - `:post-fn`: to be applied to the `db-spec` and each insertion result,
     transforming the data returned by a fixture.

   Both can be useful for JDBC drivers that need special handling, e.g. MySQL
   which does not return inserted data, but a map with `:generated_key` only.

   Documents passed to this datasource need to contain the `:db/table` key
   pointing at the database table they should be inserted into. Every other
   key within the document will be interpreted as a database column and its
   value."
  [id db-spec & [overrides]]
  (map->Database
    (merge
      {:id      id
       :db      db-spec
       :pre-fn  identity
       :post-fn #(do %3)}
      overrides)))
