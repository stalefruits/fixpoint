(ns ^:elastic fixpoint.datasource.elastic-test
  (:require [clojure.test :refer :all]
            [qbits.spandex :as s]
            [fixpoint.datasource.elastic :as elastic]
            [fixpoint.core :as fix]))

;; ## Test Datasource

(def test-es
  (elastic/make-datasource
    :es
    (or (System/getenv "FIXPOINT_ELASTIC_HOST")
        "http://docker:9200")))

;; ## Fixtures

(def ^:private +indices+
  (->> [{:elastic/index :people
         :elastic/mapping
         {:person
          {:properties
           {:id   {:type :long}
            :name {:type :string}
            :age  {:type :long}}}}}
        {:elastic/index :posts
         :elastic/mapping
         {:post
          {:properties
           {:id        {:type :long}
            :text      {:type :string}
            :author-id {:type :string, :index :not_analyzed}}}}}
        {:elastic/index :facets
         :elastic/mapping false}]
       (map #(fix/on-datasource % :es))))

(defn- person
  [reference name age]
  (-> {:elastic/index :people
       :elastic/type  :person
       :id            (rand-int 100000)
       :name          name
       :age           age}
      (fix/as reference)
      (fix/on-datasource :es)))

(defn- post
  [reference person-reference text]
  (-> {:elastic/index :posts
       :elastic/type  :post
       :id            (rand-int 100000)
       :text          text
       :author-id     person-reference}
      (fix/as reference)
      (fix/on-datasource :es)))

(def +fixtures+
  [(person :person/me     "me"        27)
   (person :person/you    "you"       29)
   (post   :post/happy    :person/me  "Awesome.")
   (post   :post/meh      :person/you "Meh.")
   (post   :post/question [:post/happy :author-id] "Do you really think so?")])

(use-fixtures
  :once
  (fix/use-datasources test-es)
  (fix/use-data [+indices+ +fixtures+]))

;; ## Tests

(deftest t-elastic
  (testing "insertion data."
    (let [person (is (fix/property :person/me))
          post   (is (fix/property :post/happy))]
      (is (integer? (:id person)))
      (is (integer? (:id post)))))

  (testing "references."
    (are [post person] (= (fix/id person) (fix/property post :author-id))
         :post/happy    :person/me
         :post/meh      :person/you
         :post/question :person/me))

  (testing "datasource access."
    (elastic/with-elastic-client [es :es]
      (let [index-name (is (elastic/index :es :people))
            url (str "/" index-name "/person/_search")
            response (->> {:url    url
                           :method :post
                           :body   {:query {:match_all {}}}}
                          (s/request es))
            ids (->> (get-in response [:body :hits :hits])
                     (map :_id)
                     (set))]
        (is (= (set (fix/properties [:person/me :person/you] :elastic/id))
               ids)))))

  (testing "index declaration only."
    (let [index-name (is (elastic/index :es :facets))
          url (str "/" index-name "/_mapping")
          response (elastic/with-elastic-client [es :es]
                     (->> {:url               url
                           :method            :get
                           :exception-handler (comp ex-data s/decode-exception)}
                          (s/request es)))]
      (is (= 404 (:status response))))))
