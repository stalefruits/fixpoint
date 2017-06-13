(ns fixpoint.datasource.amqp-test
  (:require [clojure.test :refer :all]
            [kithara.rabbitmq
             [channel :as rch]
             [connection :as rc]
             [exchange :as re]
             [queue :as rq]
             [publish :refer [publish]]]
            [fixpoint.datasource.amqp :as amqp]
            [fixpoint.core :as fix]))

;; ## Test Datasource

(def test-amqp
  (amqp/make-datasource :amqp {:log-level :error}))

;; ## Fixtures

(use-fixtures
  :once
  (fix/use-datasources test-amqp))

;; ## Helpers

(defmacro with-connection
  [[sym options] & body]
  `(let [connection# (rc/open ~options)
         ~sym connection#]
     (try
       (do ~@body)
       (finally
         (rc/close connection#)))))

(defn- setup-amqp!
  [connection]
  (let [ch (is (rch/open connection))
        ex (is (re/declare ch "test-exchange" :topic))
        qa (is (rq/declare ch "test-queue-a"))
        qb (is (rq/declare ch "test-queue-b"))]
    (rq/bind qa {:exchange "test-exchange", :routing-keys ["a"]})
    (rq/bind qb {:exchange "test-exchange", :routing-keys ["b" "a"]})
    {:channel ch :qa qa :qb qb}))

(defn- publish-message!
  [channel routing-key]
  (->> {:exchange "test-exchange"
        :routing-key routing-key
        :body        (.getBytes routing-key "UTF-8")}
       (publish channel))
  true)

(defn- get-messages!
  [q]
  (->> #(rq/get q {:auto-ack? true, :as :string})
       (repeatedly 2)
       (mapv (juxt :routing-key :body))))

;; ## Tests

(deftest t-amqp
  (with-connection [connection (is (fix/raw-datasource :amqp))]
    (let [{:keys [channel qa qb]} (setup-amqp! connection)]
      (is (publish-message! channel "a"))
      (is (publish-message! channel "b"))
      (is (publish-message! channel "c"))
      (is (= [["a" "a"] [nil nil]] (get-messages! qa)))
      (is (= [["a" "a"] ["b" "b"]] (get-messages! qb))))))
