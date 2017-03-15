(ns fixpoint.datasource.elastic
  (:require [fixpoint.core :as fix]
            [qbits.spandex :as s]
            [qbits.spandex.utils :as utils])
  (:import [java.util UUID]))

;; ## Client

(defn- start-client
 [hosts]
 (s/client
   {:hosts       hosts
    :request     {:connect-timeout 10000
                  :socket-timeout  10000}
    :http-client {:max-conn-per-route 16
                  :max-conn-total     (* (count hosts) 16)}}))

(defn- stop-client
  [client]
  (s/close! client)
  nil)

;; ## Queries

(defn- request
  [{:keys [client]} method url body & [opts]]
  (if (and client (not= client ::none))
    (->> {:url     (utils/url url)
          :method  method
          :body    body}
         (merge opts)
         (s/request client))))

(defn- return-body-on-success
  [{:keys [status body]} & [path]]
  (when (<= 200 status 204)
    (if path
      (get-in body path)
      body)))

(defn put
  [es index type id doc]
  {:pre [index type id (map? doc)]}
  (-> (request es :put [index type id] doc)
      (return-body-on-success)))

;; ## Indices

(def ^:private default-index-settings
  {:number_of_shards 6})

(defn create-index!
  [es index mapping & [settings]]
  (->> {:settings (merge default-index-settings settings)
        :mappings mapping}
       (request es :put [index])
       (return-body-on-success)))

(defn delete-index!
  [es index]
  (-> (request es :delete [index] nil)
      (return-body-on-success)))

(defn refresh-index!
  [es index]
  (-> (request es :post [index :_refresh] nil)
      (return-body-on-success)))

;; ## Helpers

(defn- random-id
  [& [prefix]]
  (str (some-> prefix name (str "-"))
       (UUID/randomUUID)))

(defn- assert-name-key
  [document key]
  (let [value (get document key)]
    (assert (or (string? value) (keyword? value))
            (str "ES fixture requires `" key "` key, and it has to be "
                 "a keyword or string: "
                 (pr-str document)))))

(defn- assert-optional-name-key
  [document key]
  (let [value (get document key)]
    (assert (or (nil? value) (string? value) (keyword? value))
            (str "ES fixture allows `" key "` key, but it has to be "
                 "a keyword or string: "
                 (pr-str document)))))

(defn assert-map-key
  [document key]
  (let [value (get document key)]
    (assert
      (map? value)
      (str "ES fixture requires `" key "` key, and it has to be a map: "
           (pr-str document)))))

;; ## Datasource
;;
;; This datasource takes two kinds of documents:
;;
;; - insert documents (w/ `:elastic/index`)
;; - index setup documents (w/ additional `:elastic/create?` and
;;  `:elastic/mapping`)

(defn- create-index-name!
  [{:keys [indices]} {:keys [elastic/index] :as document}]
  (let [index-name (random-id index)
        index-key  (name index)]
    (swap! indices
           (fn [indices]
             (assert (not (contains? indices index-key))
                     (str
                       "ES fixture contains the already used index key `"
                       index "`: " (pr-str document)))
             (assoc indices index-key index-name)))
    index-name))

(defn- lookup-index-name
  [{:keys [indices]} {:keys [elastic/index] :as document}]
  (let [index-name (get @indices (name index))]
    (assert index-name
            (str "ES fixture requires index `" index "`: " (pr-str document)))
    index-name))

(defn- handle-create-index!
  [es {:keys [elastic/mapping] :as fixture}]
  (assert-name-key fixture :elastic/index)
  (assert-map-key fixture :elastic/mapping)
  (let [index-name (create-index-name! es fixture)
        [success? result]
        (try
          [true (create-index! es index-name mapping)]
          (catch clojure.lang.ExceptionInfo ex
            [false (:body (ex-data ex))]))]
    (assert success?
            (str "creation of index failed for ES fixture: "
                 (pr-str fixture) "\n"
                 "status: " (:status result) "\n"
                 "error:  " (:error result)))
    {:data {:id index-name}}))

(defn- handle-declare-index!
  [es fixture]
  (assert-name-key fixture :elastic/index)
  (let [index-name (create-index-name! es fixture)]
    {:data {:id index-name}}))

(defn- handle-put!
  [es {:keys [elastic/index elastic/type elastic/id] :as document}]
  (assert-name-key document :elastic/index)
  (assert-name-key document :elastic/type)
  (assert-optional-name-key document :elastic/id)
  (let [document'  (dissoc document :elastic/index :elastic/type :elastic/id)
        id         (or id (random-id (str (name index) "-" (name type))))
        index-name (lookup-index-name es document)
        [success? result]
        (try
          [true (put es index-name type id document')]
          (catch clojure.lang.ExceptionInfo ex
            [false (:body (ex-data ex))]))]
    (assert success?
            (str "insertion of document failed for ES fixture: "
                 (pr-str document) "\n"
                 "status: " (:status result) "\n"
                 "error:  " (:error result)))
    (refresh-index! es index-name)
    {:data (-> document
               (update :elastic/type name)
               (assoc  :elastic/index index-name)
               (assoc  :elastic/id id))}))

(defn- rollback-indices!
  [es indices]
  (doseq [[index-key index] indices]
    (try
      (delete-index! es index)
      (catch clojure.lang.ExceptionInfo ex
        (let [{:keys [status body]} (ex-data ex)]
          (when (not= status 404)
            (println
              (format "WARN: could not deleted index (%d): %s%n%s"
                      status
                      index
                      (pr-str body))))))
      (catch Throwable t
        (println
          (format "WARN: could not delete index (unexpected error): %s%n%s"
                  index
                  (pr-str t)))))))

(defrecord ElasticDatasource [id indices hosts client]
  fix/Datasource
  (datasource-id [_]
    id)
  (start-datasource [this]
    (-> this
        (assoc :client (start-client hosts))))
  (stop-datasource [this]
    (-> this
        (update :client stop-client)))
  (run-with-rollback [this f]
    (let [old-indices (if indices (keys @indices) #{})
          indices (or indices (atom {}))]
      (try
        (f (assoc this :indices indices))
        (finally
          (rollback-indices! this (reduce disj @indices old-indices))))))
  (insert-document! [this {:keys [elastic/mapping] :as document}]
    (cond (false? mapping) (handle-declare-index! this document)
          mapping          (handle-create-index! this document)
          :else            (handle-put! this document))))

(defn make-datasource
  "Create an ElasticSearch datasource, accepting two kinds of fixture documents:

   __Index Creation__

   These are identified using the `:elastic/mapping` key, so don't forget that
   one.

   ```clojure
   (-> {:elastic/index   :people
        :elastic/mapping {:person
                          {:properties
                           {:name {:type :string}
                            :age  {:type :long}}}}}
       (fix/on-datasource :elastic))
   ```

   __Document Insertion__

   ```clojure
   (-> {:elastic/index :people
        :name          \"Me\"
        :age           27}
       (fix/as :person/me)
       (fix/on-datasource :elastic))
   ```

   The connection between indices and documents is done using the
   `:elastic/index` key."
  [id hosts]
  (map->ElasticDatasource
    {:hosts (if (string? hosts)
              [hosts]
              hosts)
     :id    id}))

;; ## Helpers

(defmacro with-elastic-client
  "Run the given body in the context of the spandex ES client belonging
   to the given datasource."
  [[sym datasource-id] & body]
  `(let [~sym (:client (fix/datasource ~datasource-id))]
     ~@body))

(defn index
  [datasource-id index-key]
  (let [{:keys [indices]} (fix/datasource datasource-id)
        index-name (some-> indices deref (get (name index-key)))]
    (assert index-name
            (str "no such index within ES fixture context: "
                 index-key))
    index-name))
