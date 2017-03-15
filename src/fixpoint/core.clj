(ns fixpoint.core
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

     Reference IDs __must__ be namespaced keywords."))

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
  [id]
  (get *datasources* id))

(defn datasource
  "Get the datasource registered under the given ID within the current scope."
  [id]
  (let [ds (maybe-datasource id)]
    (assert ds (format "no datasource registered as: %s" (pr-str id)))
    ds))

;; ## Rollback

(defn ^:no-doc with-rollback*
  [ds f]
  (let [id (datasource-id ds)
        ds (datasource id)]
    (->> (fn [ds']
           (binding [*datasources* (assoc *datasources* id ds')]
             (f ds')))
         (run-with-rollback ds))))

(defmacro with-rollback
  "Run the given body within a 'transacted' version of the given datasource,
   rolling back after the run has finished."
  [[sym datasource] & body]
  `(with-rollback* ~datasource
     (fn [~sym] ~@body)))

;; ## Startup/Shutdown

(defn ^:no-doc with-datasource*
  "Run the given function, passing a started `datasource` to it."
  [datasource f]
  (let [started-datasource (start-datasource datasource)]
    (try
      (run-with-datasource started-datasource f)
      (finally
        (stop-datasource started-datasource)))))

(defmacro with-datasource
  "Start `datasource` and bind it to `sym`, then run `body` in its scope."
  [[sym datasource] & body]
  `(with-datasource* ~datasource
     (fn [~sym] ~@body)))

(defmacro with-rollback-datasource
  "Start a 'transacted' version of `datasource`, rolling back any changed made
   after the run has finished."
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
  [entities document-id path]
  (assert
    (contains? entities document-id)
    (str "no such document available within the current fixture scope: "
         (pr-str document-id)))
  (let [result (get-in entities (cons document-id path) ::none)]
    (assert (not= result ::none)
            (format "document '%s' does not contain property '%s': %s"
                    (pr-str document-id)
                    (pr-str (vec path))
                    (pr-str (get entities document-id))))
    result))

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
  "Run the given function after inserting the given fixtures into their
   respective datasources."
  [fixtures f]
  (binding [*entities* (insert-fixtures! *entities* fixtures)]
    (f)))

(defmacro with-data
  "Run the given function after inserting the given fixtures into their
   respective datasources."
  [fixtures & body]
  `(with-data* ~fixtures
     (fn [] ~@body)))

(defn property
  "Look up a single fixture document's property."
  [document-id & path]
  (lookup-reference *entities* document-id path))

(defn properties
  "Look up the same property in multiple fixture documents."
  [document-ids & path]
  (map #(apply property % path) document-ids))

(defn match
  "Look up a fixture document's property for every entity that matches all
   of the given tags."
  [tags & path]
  (if-let [index-matches (seq (keep #(get-in *entities* [::index %]) tags))]
    (->> index-matches
         (reduce set/intersection)
         (map #(apply property % path)))))

(defn id
  "Retrieve the `:id` property of the given document."
  [document-id]
  (property document-id :id))

(defn ids
  "Retrieve the `:id` property of the given documents."
  [document-ids]
  (properties document-ids :id))

(defn by-namespace
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
  [data datasource-id]
  {:pre [(map? data)]}
  (assoc data :fixpoint/datasource datasource-id))

(defn as
  [data fixture-id]
  {:pre [(map? data)
         (reference? fixture-id)]}
  (assoc data :fixpoint/id fixture-id))
