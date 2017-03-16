(ns fixpoint.datasource.hikari
  "HikariCP wrapper for any [[JDBCDatasource]]."
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

;; ## Datasource Startup/Shutdown Logic

(defn- cache-db-spec
  [{:keys [jdbc-datasource] :as this}]
  (assoc this
         :cached-db-spec
         (fix-jdbc/get-db-spec jdbc-datasource)))

(defn- clear-db-spec
  [this]
  (assoc this :cached-db-spec nil))

(defn- instantiate-pool
  [{:keys [pool-options cached-db-spec] :as this}]
  (let [pool (start-pool! cached-db-spec pool-options)]
    (-> this
        (assoc :pool pool)
        (update :jdbc-datasource fix-jdbc/set-db-spec {:datasource pool}))))

(defn- cleanup-pool
  [{:keys [cached-db-spec pool] :as this}]
  (-> this
      (update :jdbc-datasource
              fix-jdbc/set-db-spec
              cached-db-spec)
      (update :pool stop-pool!)))

(defn- start-jdbc-datasource
  [this]
  (update this :jdbc-datasource fix/start-datasource))

(defn- stop-jdbc-datasource
  [this]
  (update this :jdbc-datasource fix/stop-datasource))

;; ##  Rollback Logic

(defn- run-with-jdbc-datasource-rollback
  [{:keys [jdbc-datasource] :as this} f]
  (->> (fn [datasource']
         (f (assoc this :jdbc-datasource datasource')))
       (fix/run-with-rollback jdbc-datasource)))

;; ## Component

(defrecord HikariDatasource [jdbc-datasource
                             cached-db-spec
                             pool-options
                             pool]
  fix/Datasource
  (datasource-id [_]
    (fix/datasource-id jdbc-datasource))
  (start-datasource [this]
    (-> this
        (cache-db-spec)
        (instantiate-pool)
        (start-jdbc-datasource)))
  (stop-datasource [this]
    (-> this
        (stop-jdbc-datasource)
        (cleanup-pool)
        (clear-db-spec)))
  (run-with-rollback [this f]
    (run-with-jdbc-datasource-rollback this f))
  (insert-document! [_ document]
    (fix/insert-document! jdbc-datasource document))
  (as-raw-datasource [_]
    (fix/as-raw-datasource jdbc-datasource)))

(defn wrap-jdbc-datasource
  "Wrap the given [[JDBCDatasource]] to use a Hikari connection pool."
  [jdbc-datasource & [pool-options]]
  {:pre (satisfies? fix-jdbc/JDBCDatasource jdbc-datasource)}
  (map->HikariDatasource
    {:jdbc-datasource jdbc-datasource
     :pool-options    pool-options}))
