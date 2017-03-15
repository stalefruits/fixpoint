(ns ^:postgresql fixpoint.datasource.hikari-test
  (:require [clojure.test :refer :all]
            [clojure.java.jdbc :as jdbc]
            [fixpoint.datasource
             [hikari :as hikari]
             [jdbc :refer [with-jdbc-datasource]]
             [postgresql :as pg]]
            [fixpoint.core :as fix]))

;; ## Test Datasource

(def test-db
  (-> (pg/make-datasource
        :test-db
        {:connection-uri (or (System/getenv "FIXPOINT_POSTGRESQL_URI")
                             "jdbc:postgresql://localhost:5432/test")})
      (hikari/wrap-jdbc-datasource)))

;; ## Fixtures

(defn- person
  [reference name age]
  (-> {:db/table :people
       :name     name
       :age      age
       :active   true}
      (fix/as reference)
      (fix/on-datasource :test-db)))

(defn- post
  [reference person-reference text]
  (-> {:db/table  :posts
       :text      text
       :author-id person-reference}
      (fix/as reference)
      (fix/on-datasource :test-db)))

(def +fixtures+
  [(person :person/me     "me"        27)
   (person :person/you    "you"       29)
   (post   :post/happy    :person/me  "Awesome.")
   (post   :post/meh      :person/you "Meh.")
   (post   :post/question [:post/happy :author-id] "Do you really think so?")])

(defn- use-postgresql-setup
  []
  (fn [f]
    (with-jdbc-datasource [db :test-db]
      (->> (str "create table people ("
                "  id         SERIAL PRIMARY KEY,"
                "  name       VARCHAR NOT NULL,"
                "  age        INT NOT NULL,"
                "  active     BOOLEAN NOT NULL DEFAULT TRUE,"
                "  created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP"
                ")")
           (jdbc/execute! db))
      (->> (str "create table posts ("
                "  id         SERIAL PRIMARY KEY,"
                "  author_id  INTEGER NOT NULL REFERENCES people (id),"
                "  text       VARCHAR NOT NULL,"
                "  created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP"
                ")")
           (jdbc/execute! db))
      (f))))

(use-fixtures
  :once
  (fix/use-datasources test-db)
  (use-postgresql-setup)
  (fix/use-data +fixtures+))

;; ## Tests

(deftest t-hikari
  (testing "insertion data."
    (let [person (is (fix/property :person/me))
          post   (is (fix/property :post/happy))]
      (is (integer? (:id person)))
      (is (integer? (:id post)))
      (is (:created-at person))
      (is (:created-at post))))

  (testing "references."
    (are [post person] (= (fix/id person) (fix/property post :author-id))
         :post/happy    :person/me
         :post/meh      :person/you
         :post/question :person/me))

  (testing "datasource access."
    (with-jdbc-datasource [db :test-db]
      (let [ids (->> ["select id from people order by name asc"]
                     (jdbc/query db)
                     (map :id))]
        (is (= (fix/ids [:person/me :person/you]) ids))))))
