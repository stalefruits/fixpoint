# fixpoint [![Build Status](https://travis-ci.org/stylefruits/fixpoint.svg?branch=master)](https://travis-ci.org/stylefruits/fixpoint)

__fixpoint__ is a library offering a simple and powerful way of setting up
test datastores and data.

Ready-to-use components for [PostgreSQL][postgres], [MySQL][mysql] and
[ElasticSearch][elastic] are already included.

[postgres]: https://www.postgresql.org/
[mysql]: https://www.mysql.com/
[elastic]: https://www.elastic.co/products/elasticsearch

## Usage

```clojure
(require '[fixpoint.core :as fix]
         '[fixpoint.datasource.postgresql :as pg])
```

Set up a datasource and give it an ID to be used later, e.g. to set up a
PostgreSQL database and call it `:test-db`:


```clojure
(def test-db
  (pg/make-datasource
    :test-db
    {:connection-uri "jdbc:postgresql://..."}))
```

Set up test fixture functions. Use `fix/as` to specify a name for a specific
fixture document that can be used in other fixture documents to refer to it.
Use `fix/on-datasource` to specify which datasource the fixture should
get inserted in, e.g.:

```clojure
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
```

Note that you can also return a seq (or nested ones) of fixtures, allowing
you to cover multiple datapoints within one fixture function. Check out
instantiation of those fixtures:

```clojure
(def +fixtures+
  [(person :person/me  "me" 27)
   (person :person/you "you" 29)
   (post   :post/happy :person/me  "Awesome.")
   (post   :post/meh   :person/you "Meh.")])
```

Note the cross-references between entities, using namespaced keywords. They,
by default, resolve to the `:id` field of the respective inserted data. You can
also do a lookup within, e.g. to create a post with the same author as another:

```clojure
(post :post/question [:post/happy :author-id] "Do you really think so?")
```

Of course, so far nothing has happened since we haven't brought our datasource
and fixtures together. Let's start up the datasource, ensuring rollback after
we're done, insert our fixtures and check out the inserted data.

```clojure
(fix/with-rollback-datasource [_ test-db]
  (fix/with-data +fixtures+
    (vector
      (fix/property :person/me)
      (fix/property :person/you :created-at)
      (fix/property :post/happy :author-id)
      (fix/id :post/meh))))
;; => [{:id 3,
;;      :name "me",
;;      :age 27,
;;      :active true,
;;      :created-at #inst "2017-03-10T11:13:06.452505000-00:00"},
;;     #inst "2017-03-10T11:13:06.452505000-00:00"
;;     3
;;     4]
```

Cool, eh?

## Integration with Clojure Tests

fixpoint functionality can be used as part of `clojure.test` fixtures. Note that
the datasource fixture has to be applied before the data fixture:

```clojure
(require '[clojure.test :refer :all]
         '[clojure.java.jdbc :as jdbc]
         '[fixpoint.datasource.jdbc :refer [with-jdbc-datasource]])

(use-fixtures
  :once
  (fix/use-datasources db)
  (fix/use-data +fixtures+))

(deftest t-people-query
  (with-jdbc-datasource [db :db]
    (is (= #{"me" "you" "someone"}
           (->> (jdbc/query db ["select name from people"])
                (map :name)
                (set))))))
```

This, as you might have noticed, should fail:

```clojure
(run-tests)
;; FAIL in (t-people-query) (b88f62883cbaa1a3f26472a814829fe3c5933107-init.clj:3)
;; expected: (= #{"someone" "you" "me"} (->> (db/query db ["select name from people"]) (map :name) (set)))
;;  actual: (not (= #{"someone" "you" "me"} #{"you" "me"}))
;;
;; Ran 1 tests containing 1 assertions.
;; 1 failures, 0 errors.
;; => {:test 1, :pass 0, :fail 1, :error 0, :type :summary}
```

## License

Copyright &copy; 2017 stylefruits GmbH

This project is licensed under the [Apache License 2.0][license].

[license]: http://www.apache.org/licenses/LICENSE-2.0.html
