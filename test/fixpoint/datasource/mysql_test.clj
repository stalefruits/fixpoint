(ns ^:mysql fixpoint.datasource.mysql-test
  (:require [clojure.test :refer :all]
            [clojure.java.jdbc :as jdbc]
            [fixpoint.datasource.mysql :as mysql]
            [fixpoint.core :as fix]))

;; ## Test Datasource

(def test-db
  (mysql/make-datasource
    :test-db
    {:connection-uri (or (System/getenv "FIXPOINT_MYSQL_URI")
                         "jdbc:mysql://localhost:3306/test")}))

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

(defn- use-mysql-setup
  []
  (fn [f]
    (let [db (fix/raw-datasource :test-db)]
      (try
        (->> (str "create table people ("
                  "  id         INT AUTO_INCREMENT PRIMARY KEY,"
                  "  name       VARCHAR(255) NOT NULL,"
                  "  age        INT NOT NULL,"
                  "  active     TINYINT NOT NULL DEFAULT TRUE,"
                  "  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP"
                  ")")
             (jdbc/execute! db))
        (->> (str "create table posts ("
                  "  id         INT AUTO_INCREMENT PRIMARY KEY,"
                  "  author_id  INT NOT NULL REFERENCES people (id),"
                  "  text       VARCHAR(255) NOT NULL,"
                  "  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP"
                  ")")
             (jdbc/execute! db))
        (f)
        (finally
          (jdbc/execute! db "drop table if exists posts")
          (jdbc/execute! db "drop table if exists people"))))))

(use-fixtures
  :once
  (fix/use-datasources test-db)
  (use-mysql-setup)
  (fix/use-data +fixtures+))

;; ## Tests

(deftest t-mysql
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
    (let [db (fix/raw-datasource :test-db)
          ids (->> ["select id from people order by name asc"]
                   (jdbc/query db)
                   (map :id))]
      (is (= (fix/ids [:person/me :person/you]) ids)))))
