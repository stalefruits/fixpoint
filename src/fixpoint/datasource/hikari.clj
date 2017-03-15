(ns fixpoint.datasource.hikari
  "HikariCP wrapper for any JDBC datasource."
  (:require [fixpoint.core :as fix]
            [fixpoint.datasource.jdbc :as fix-jdbc]
            [hikari-cp.core :as hikari]
            [clojure.set :as set]
            [clojure.java.jdbc :as jdbc]))

;; ## Pool

(defn- make-hikari-config
  [db-spec pool-options]
  {:pre [(map? db-spec)]}
  (-> db-spec
      (set/rename-keys
        {:connection-uri :jdbc-url
         :subname        :jdbc-url
         :host           :server-name
         :port           :port-number
         :subprotocol    :adapter})
      (merge pool-options)))

(defn- start-pool!
  [db-spec pool-options]
  (-> (make-hikari-config db-spec pool-options)
      (hikari/make-datasource)))

(defn- stop-pool!
  [pool]
  (hikari/close-datasource pool))

;; ## Datasource

(defrecord HikariDatasource [datasource pool-options db-spec pool]
  fix/Datasource
  (datasource-id [_]
    (fix/datasource-id datasource))
  (start-datasource [this]
    (let [db-spec (:db datasource)
          pool    (start-pool! db-spec pool-options)]
      (-> this
          (assoc :db-spec db-spec
                 :pool    pool)
          (assoc-in [:datasource :db] {:datasource pool})
          (update :datasource fix/start-datasource))))
  (stop-datasource [this]
    (-> this
        (update :datasource fix/stop-datasource)
        (assoc-in [:datasource :db] db-spec)
        (update :pool stop-pool!)))
  (run-with-rollback [this f]
    (->> (fn [datasource']
           (f (assoc this :datasource datasource')))
         (fix/run-with-rollback datasource)))
  (insert-document! [_ document]
    (fix/insert-document! datasource document))

  fix-jdbc/JDBCDatasource
  (as-jdbc-datasource [_]
    (fix-jdbc/as-jdbc-datasource datasource)))

(defn wrap-jdbc-datasource
  [datasource & [pool-options]]
  {:pre (satisfies? fix-jdbc/JDBCDatasource datasource)}
  (map->HikariDatasource
    {:datasource   datasource
     :pool-options pool-options}))
