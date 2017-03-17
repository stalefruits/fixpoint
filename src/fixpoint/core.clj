(ns fixpoint.core
  "Fixture Functions and Protocols"
  (:refer-clojure :exclude [ref])
  (:require [clojure.set :as set]
            [clojure.walk :as walk]))

;; ## Protocols

(defprotocol Datasource
  "Protocol for fixture-capable test datasources."
  (datasource-id [this]
    "Return an ID for this datasource.")
  (start-datasource [this]
    "Start the datasource.")
  (stop-datasource [this]
    "Stop the datasource.")
  (run-with-rollback [this f]
    "Run `(f datasource)`, where `datasource` is a transacted version of the
     current datasource. Ensure that changes made via `datasource` are rolled
     back afterwards.")
  (insert-document! [this document]
    "Insert the given `data` into the current datasource. Return a map of
     `:id` (the reference ID),  `:data` (the actual data inserted) and
     optionally `:tags` (a seq of tags for entity filtering).

     Reference IDs __must__ be namespaced keywords.")
  (as-raw-datasource [this]
    "Retrieve the underlying raw datasource, e.g. a `clojure.java.jdbc` database
     spec, or some raw connection object.

     This should fail on non-started datasources."))

(defprotocol Fixture
  "Protocol for datasource fixtures."
  (fixture-documents [this]
    "Generate a seq of fixture document maps, each one belonging to a single
     datasource identified by the `:fixpoint/datasource` key."))

(extend-protocol Fixture
  clojure.lang.Sequential
  (fixture-documents [sq]
    (mapcat fixture-documents sq))

  clojure.lang.IPersistentMap
  (fixture-documents [m]
    [m])

  nil
  (fixture-documents [_]
    []))

;; ## Datasource Registry

(def ^:private ^:dynamic *datasources*
  {})

(defn- run-with-datasource
  [datasource f]
  (let [id (datasource-id datasource)]
    (binding [*datasources* (assoc *datasources* id datasource)]
      (f datasource))))

(defn maybe-datasource
  "Get the [[Datasource]] registered with the given ID within the current scope,
   or `nil` if there is none."
  [id]
  (get *datasources* id))

(defn datasource
  "Get the [[Datasource]] registered under the given ID within the current scope.
   Throws an `AssertionError` if there is none."
  [id]
  (let [ds (maybe-datasource id)]
    (assert ds (format "no datasource registered as: %s" (pr-str id)))
    ds))

(defn raw-datasource
  "Get the raw datasource value for the [[Datasource]] registered under the
   given ID within the current scope.

   See [[datasource]] and [[as-raw-datasource]]."
  [id]
  (-> (datasource id)
      (as-raw-datasource)))

;; ## Rollback

(defn ^:no-doc with-rollback*
  "See [[with-rollback]]."
  [ds f]
  (let [id (datasource-id ds)
        ds (datasource id)]
    (->> (fn [ds']
           (binding [*datasources* (assoc *datasources* id ds')]
             (f ds')))
         (run-with-rollback ds))))

(defmacro with-rollback
  "Run the given body within a 'transacted' version of the given datasource,
   rolling back after the run has finished.

   ```clojure
   (with-datasource [ds (pg/make-datasource ...)]
     (with-rollback [tx ds]
       (let [db (as-jdbc-datasource tx)]
         (jdbc/execute! db [\"INSERT INTO ...\" ...]))))
   ```
   "
  [[sym datasource] & body]
  `(with-rollback* ~datasource
     (fn [~sym] ~@body)))

;; ## Startup/Shutdown

(defn ^:no-doc with-datasource*
  "See [[with-datasource]]."
  [datasource f]
  (let [started-datasource (start-datasource datasource)]
    (try
      (run-with-datasource started-datasource f)
      (finally
        (stop-datasource started-datasource)))))

(defmacro with-datasource
  "Start `datasource` and bind it to `sym`, then run `body` in its scope.

   ```clojure
   (with-datasource [ds (pg/make-datasource ...)]
     ...)
   ```
   "
  [[sym datasource] & body]
  `(with-datasource* ~datasource
     (fn [~sym] ~@body)))

(defmacro with-rollback-datasource
  "Start a 'transacted' version of `datasource`, rolling back any changed made
   after the run has finished.

   ```clojure
   (with-rollback-datasource [ds (pg/make-datasource ...)]
     (let [db (as-jdbc-datasource ds)]
       (jdbc/execute! db [\"INSERT INTO ...\" ...])))
   ```

   This is a convenience function combining [[with-datasource]] and
   [[with-rollback]]."
  [[sym datasource] & body]
  `(with-datasource [ds# ~datasource]
     (with-rollback [~sym ds#]
       ~@body)))

;; ## References

(defn- reference?
  [value]
  (if (sequential? value)
    (recur (first value))
    (and (keyword? value)
         (namespace value))))

(defn- parse-reference
  [value]
  (cond (vector? value)
        (let [[value' & path'] value]
          (when-let [[document-id path] (parse-reference value')]
            [document-id (concat path path')]))

        (reference? value)
        [value []]))

;; ## Fixture Insertion

(defn- lookup-reference
  [entities document-id transformations]
  (assert
    (contains? entities document-id)
    (str "no such document available within the current fixture scope: "
         (pr-str document-id)))
  (if (every? keyword? transformations)
    (let [result (get-in entities (cons document-id transformations) ::none)]
      (assert (not= result ::none)
              (format "document '%s' does not contain property '%s': %s"
                      (pr-str document-id)
                      (pr-str (vec transformations))
                      (pr-str (get entities document-id))))
      result)
    (let [document (get entities document-id)]
      (reduce
        (fn [value transformation]
          (transformation value))
        document
        transformations))))

(defn- resolve-references*
  [entities value]
  (cond (reference? value)
        (if-let [[document-id path] (parse-reference value)]
          (->> (if (empty? path) [:id] path)
               (lookup-reference entities document-id))
          value)

        (sequential? value)
        (cond-> (map #(resolve-references* entities %) value)
          (vector? value) (vec))

        (map? value)
        (->> (for [[k v] value]
               [k (resolve-references* entities v)])
             (into {}))

        :else value))

(defn- resolve-references
  [entities document]
  (->> (dissoc document :fixpoint/datasource :fixpoint/id)
       (resolve-references* entities)))

(defn- throw-document-exception!
  [entities document ^Throwable throwable]
  (throw
    (ex-info
      (format "insertion failed for document: %s%n(%s)"
              (pr-str document)
              (.getMessage throwable))
      {:document document
       :resolved-document (resolve-references entities document)}
      throwable)))

(defn- assert-id-reference
  [{:keys [fixpoint/id] :as document}]
  (when id
    (assert (reference? id)
            (str
              "':fixpoint/id' needs to be a namespaced keyword: "
              (pr-str document)))))

(defn- assert-insertion-result
  [document result]
  (when result
    (assert (:data result)
            (format
              (str "insertion result needs ':data' key.%n"
                   "document: %s%n"
                   "result:   %s")
              (pr-str document)
              (pr-str result)))))

(defn- update-entities
  [entities {:keys [fixpoint/id] :as document} result]
  (or (when id
        (when-let [{:keys [data tags]} result]
          (assert (not (contains? entities id))
                  (format
                    (str "duplicate document ':id': %s%n"
                         "document: %s%n"
                         "result:   %s")
                    (pr-str id)
                    (pr-str document)
                    (pr-str result)))
          (reduce
            (fn [entities tag]
              (update-in entities [::index tag] (fnil conj #{}) id))
            (assoc entities id data)
            tags)))
      entities))

(defn- insert-document-and-update-entities!
  [entities datasource-id document]
  (try
    (assert-id-reference document)
    (let [ds        (datasource datasource-id)
          document' (resolve-references entities document)
          result    (insert-document! ds document')]
      (assert-insertion-result document result)
      (update-entities entities document result))
    (catch Throwable t
      (throw-document-exception! entities document t))))

(defn- insert-fixtures!
  [entities fixtures]
  (->> fixtures
       (mapcat fixture-documents)
       (reduce
         (fn [entities document]
           (let [datasource-id (:fixpoint/datasource document)]
             (assert datasource-id
                     (format "document is missing ':fixpoint/datasource': %s"
                             (pr-str document)))
             (insert-document-and-update-entities!
               entities
               datasource-id
               document)))
         entities)))

;; ## Fixture Access

(def ^:private ^:dynamic *entities*
  {})

(defn ^:no-doc with-data*
  "See [[with-data]]."
  [fixtures f]
  (binding [*entities* (insert-fixtures! *entities* fixtures)]
    (f)))

(defmacro with-data
  "Given a [[Fixture]] (or a seq of them), run them against their respective
   datasources, then execute `body`.

   ```clojure
   (defn person
     [name]
     (-> {:db/table :people
          :name     name}
         (on-datasource :db)))

   (with-datasource [ds (pg/make-datasource :db ...)]
     (with-data [(person \"me\") (person \"you\")]
       ...))
   ```

   This has to be wrapped by [[with-datasource]] or [[with-rollback-datasource]]
   since otherwise there is nothing to insert into."
  [fixtures & body]
  `(with-data* ~fixtures
     (fn [] ~@body)))

(defn property
  "Look up a single fixture document's property using a reference attached to a
   fixture using [[as]].

   ```clojure
   (defn person
     [reference name]
     (-> {:db/table :people
          :name     name}
         (as reference)
         (on-datasource :db)))

   (with-datasource [ds (pg/make-datasource :db ...)]
     (with-data [(person :person/me  \"me\")
                 (person :person/you \"you\")]
       (println (property :person/me))
       (println (property :person/you :id))))
   ```

   `transformations` can be given to apply a sequence of functions, in order,
   to the fixture map. Has to be used within a [[with-data]] block."
  [document-id & transformations]
  (lookup-reference *entities* document-id transformations))

(defn properties
  "See [[property]]. Performs a lookup in multiple fixture documents, returning
   values in an order corresponding to `document-ids`."
  [document-ids & transformations]
  (map #(apply property % transformations) document-ids))

(defn match
  "Look up a fixture document's property for every entity that matches all
   of the given tags."
  [tags & transformations]
  (if-let [index-matches (seq (keep #(get-in *entities* [::index %]) tags))]
    (->> index-matches
         (reduce set/intersection)
         (map #(apply property % transformations)))))

(defn id
  "Retrieve the `:id` [[property]] for the given document."
  [document-id]
  (property document-id :id))

(defn ids
  "Retrieve the `:id` [[property]] for each of the given documents."
  [document-ids]
  (properties document-ids :id))

(defn by-namespace
  "Retrieve a [[property]] for each entity whose reference (attached using
   [[as]]) has the given namespace.

   ```clojure
   (with-datasource [ds (pg/make-datasource :db ...)]
     (with-data [(person :person/me  \"me\")
                 (person :person/you \"you\")
                 (post   :post/happy :person/me \"yay!\")]
       (by-namespace :person :id)))
   ;; => {:person/me 1, :person/you 2}
   ```

   Returns a map associating the reference (see [[as]]) with the queried
   property."
  [nspace & path]
  (let [n (name nspace)]
    (->> (for [[document-id value] *entities*
               :when (= (namespace document-id) n)]
           [document-id (lookup-reference *entities* document-id path)])
         (into {}))))

;; ## Clojure Test Integration

(defn use-datasources
  "A clojure.test fixture that will wrap test runs with startup/shutdown
   of the given datasources. After the tests have run, a rollback will
   be initiated to cleanup the database."
  [& datasources]
  (fn [f]
    (let [f' (reduce
               (fn [f datasource]
                 (fn []
                   (with-rollback-datasource [_ datasource]
                     (f))))
               f datasources)]
      (f'))))

(defn use-data
  "A clojure.test fixture that will insert the given fixtures into
   their respective datasources.

   Needs to be applied after [[use-datasources]]."
  [& fixtures]
  (fn [f]
    (with-data* fixtures f)))

;; ## Helper

(defn on-datasource
  "Declare the given fixture's target datasource, corresponding to one
   to-be-instantiated within [[with-datasource]] or
   [[with-rollback-datasource]].

   ```clojure
   (defn person
     [reference name]
     (-> {:db/table :people
          :name     name}
         (as reference)
         (on-datasource :db)))

   (with-datasource [ds (pg/make-datasource :db ...)]
     (with-data [(person :person/me  \"me\") ...]
       ...))
   ```

   The result can be passed to [[with-data]] to be run against the actual
   datasource."
  [data datasource-id]
  {:pre [(map? data)]}
  (assoc data :fixpoint/datasource datasource-id))

(defn as
  "Declare the given fixture's reference ID, which can be used from within other
   fixtures.

   ```clojure
   (defn person
     [reference name]
     (-> {:db/table :people
          :name     name}
         (as reference)
         (on-datasource :db)))

   (defn post
     [reference author-reference text]
     (-> {:db/table :posts
          :author-id author-reference
          :text      text}
         (as reference)
         (on-datasource :db)))
   ```

   A simple set of fixtures could be:

   ```clojure
   [(person :person/me \"me\")
    (post   :post/happy :person/me \"yay!\")]
   ```

   You can also reference specific fields of other documents using a vector
   notation, e.g. to declare a post with the same author as `:post/happy`:

   ```clojure
   (post :post/again [:post/happy :author-id] \"still yay!\")
   ```

   Note that every reference ID can be used _exactly once_. "
  [data document-id]
  {:pre [(map? data)
         (reference? document-id)]}
  (assoc data :fixpoint/id document-id))
