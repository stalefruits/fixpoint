(ns fixpoint.core-test
  (:require [clojure.test :refer :all]
            [fixpoint.core :as fix]))

;; ## Datasource

(defrecord DummySource [data state]
  fix/Datasource
  (datasource-id [_]
    :db)
  (start-datasource [this]
    (swap! state conj :started)
    (assoc this :data (atom {})))
  (stop-datasource [this]
    (swap! state conj :stopped)
    (dissoc this :data))
  (run-with-rollback [this f]
    (let [tx (atom @data)]
      (try
        (f (assoc this :data tx))
        (finally
          (swap! state conj :rolled-back)))))
  (insert-document! [_ {:keys [id] :as row}]
    (let [generic-id (gensym)
          row'       (assoc row :id generic-id)]
      (swap! data assoc generic-id row')
      {:data row'})))

(defn- dummy-datasource
  [& [state]]
  (map->DummySource
    {:state (or state (atom []))}))

;; ## Test Data

(def ^:private test-docs
  [:doc/a :doc/b :doc/c])

(def ^:private test-fixtures
  (->> test-docs
       (map-indexed #(hash-map :index %1 :fixpoint/id %2))
       (map #(assoc % :fixpoint/datasource :db))))

;; ## Tests

(deftest t-with-datasource
  (let [state (atom [])]
    (fix/with-datasource [ds (dummy-datasource state)]
      (testing "datasource was started."
        (is (instance? clojure.lang.Atom (:data ds)))
        (is (= @state [:started])))
      (testing "datasource is registered in scope."
        (is (identical? (fix/datasource :db) ds))))
    (is (= @state [:started :stopped]))))

(deftest t-with-data
  (fix/with-datasource [_ (dummy-datasource)]
    (fix/with-data test-fixtures
      (is (every? (comp symbol? fix/id) test-docs))
      (is (every? symbol? (fix/ids test-docs)))
      (is (= [0 1 2] (map #(fix/property % :index) test-docs)))
      (is (= [0 1 2] (fix/properties test-docs :index))))))

(deftest t-with-rollback
  (fix/with-datasource [ds (dummy-datasource)]
    (is (empty? @(:data ds)))
    (fix/with-rollback [tx ds]
      (is (empty? @(:data tx)))
      (fix/with-data test-fixtures
        (is (every? (comp symbol? fix/id) test-docs))
        (is (every? symbol? (fix/ids test-docs)))
        (is (= [0 1 2] (map #(fix/property % :index) test-docs)))
        (is (= [0 1 2] (fix/properties test-docs :index))))
      (is (= 3 (count @(:data tx)))))
    (is (empty? @(:data ds)))))

(deftest t-references
  (fix/with-datasource [_ (dummy-datasource)]
    (fix/with-data (->> [(-> {:name "me"}
                             (fix/as :doc/me))
                         (-> {:name        "you"
                              :friend-name [:doc/me :name]
                              :friend-id   :doc/me
                              :first-char  [:doc/me :name first str]}
                             (fix/as :doc/you))
                         (-> {:name "someone"}
                             (fix/as :other/them))]
                        (map #(fix/on-datasource % :db)))
      (is (= {:doc/me "me", :doc/you "you"}
             (fix/by-namespace :doc :name)))
      (is (= {:other/them "someone"}
             (fix/by-namespace :other :name)))
      (is (= "me"
             (fix/property :doc/you :friend-name)))
      (is (= "prefixed-me"
             (fix/property :doc/you :friend-name #(str "prefixed-" %))))
      (is (= "m"
             (fix/property :doc/you :first-char)))
      (is (= (fix/id :doc/me)
             (fix/property :doc/you :friend-id))))))

(deftest t-use-datasources
  (let [state      (atom [])
        datasource (dummy-datasource state)
        fixture-fn (fix/use-datasources datasource)]
    (->> (fn []
           (let [ds (fix/datasource :db)]
             (testing "datasource was started."
               (is (instance? clojure.lang.Atom (:data ds)))
               (is (= @state [:started])))))
         (fixture-fn))
    (testing "datasource was rolled back."
      (is (= @state [:started :rolled-back :stopped])))))

(deftest t-use-data
  (let [datasource (dummy-datasource)
        fixture-fn (compose-fixtures
                     (fix/use-datasources datasource)
                     (fix/use-data test-fixtures))]
    (->> (fn []
           (is (every? (comp symbol? fix/id) test-docs))
           (is (every? symbol? (fix/ids test-docs)))
           (is (= [0 1 2] (map #(fix/property % :index) test-docs)))
           (is (= [0 1 2] (fix/properties test-docs :index))))
         (fixture-fn))))
