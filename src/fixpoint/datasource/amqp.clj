(ns fixpoint.datasource.amqp
  "An AMQP broker datasource compatible with AMQP 0.9.1.

   Make sure the `org.apache.qpid/qpid-broker` dependency is available on your
   classpath."
  (:require [fixpoint.datasource.file-utils :as f]
            [fixpoint.core :as fix]
            [clojure.java.io :as io])
  (:import [org.apache.qpid.server Broker BrokerOptions]))

;; ## Helper

(defmacro ^:private let-cleanup
  [[sym start-fn stop-fn & more] & body]
  (if (seq more)
    `(let-cleanup [~sym ~start-fn ~stop-fn]
       (let-cleanup [~@more]
         ~@body))
    `(let [val# ~start-fn
           ~sym val#]
       (try
         (do ~@body)
         (catch Throwable t#
           (~stop-fn val#)
           (throw t#))))))

;; ## Metadata

(def ^:private default-configuration-file
  (io/resource "fixpoint/amqp/broker-config.json"))

(defn- random-string
  []
  (str (java.util.UUID/randomUUID)))

(defn- prepare-broker-data
  [{:keys [log-level port configuration-file username password vhost]}]
  {:workdir   (f/create-temporary-directory!)
   :port      (or port 57622)
   :log-level (or log-level :error)
   :username  (or username (random-string))
   :password  (or password (random-string))
   :vhost     (or vhost "default")
   :config    (or configuration-file default-configuration-file)})

(defn- cleanup-broker-data
  [{:keys [workdir]}]
  (f/delete-directory-recursively! workdir)
  nil)

;; ## Broker Logic

(defn- prop
  [^BrokerOptions options k v]
  (.setConfigProperty options (name k) (str v)))

(defn- start-broker!
  [{:keys [config log-level port username password vhost workdir]}]
  (let [options (doto (BrokerOptions.)
                  (prop :broker.name        "fixpoint-embedded-amqp")
                  (prop :qpid.amqp_port     port)
                  (prop :qpid.http_port     (inc port))
                  (prop :qpid.work_dir      (f/path->string workdir))
                  (prop :fixpoint.log_level (-> log-level name (.toUpperCase)))
                  (prop :fixpoint.username  username)
                  (prop :fixpoint.password  password)
                  (prop :fixpoint.vhost     vhost)
                  (.setInitialConfigurationLocation (str config)))]
    (doto (Broker.)
      (.startup options))))

(defn- stop-broker!
  [^Broker broker]
  (.shutdown broker))

;; ## Datasource

(defrecord AmqpDatasource [id options metadata broker]
  fix/Datasource
  (datasource-id [this]
    id)
  (start-datasource [this]
    (let-cleanup [metadata (prepare-broker-data options) cleanup-broker-data
                  broker   (start-broker! metadata) stop-broker!]
      (assoc this
             :broker   broker
             :metadata metadata)))
  (stop-datasource [this]
    (stop-broker! broker)
    (cleanup-broker-data metadata)
    (assoc this :metadata nil, :broker nil))
  (run-with-rollback [this f]
    (f this))
  (insert-document! [this document]
    (throw
      (IllegalArgumentException.
        "AmqpDatasource does not accept any documents.")))
  (as-raw-datasource [_]
    (-> metadata
        (select-keys [:port :username :password :vhost])
        (update :vhost #(str "/" %)))))

(defn make-datasource
  "Create a datasource corresponding to an AMQP 0.9.1 broker. Options include:

   - `:port`: the port to run the broker on (default: 57622),
   - `log-level`: the broker's log level (`:debug`, `:info`, `:warn`, `:error`),
   - `:username`: the username for authentication (default: random),
   - `:password`: the password for authentication (default: random),
   - `:vhost`: the name of the default vhost to create (default: \"default\").

   This datasource doesn't accept any documents, it just sets up an AMQP broker
   and exposes the port, vhost and credentials using [[raw-datasource]]."
  ([id]
   (make-datasource id {}))
  ([id options]
   (map->AmqpDatasource
     {:id      id
      :options options})))
