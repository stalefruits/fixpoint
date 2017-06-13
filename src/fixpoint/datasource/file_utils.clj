(ns fixpoint.datasource.file-utils
  (:import [java.nio.file Files Path SimpleFileVisitor FileVisitResult]
           [java.nio.file.attribute FileAttribute]))

;; ## Directory Handling

(let [empty-array (into-array FileAttribute [])]
  (defn create-temporary-directory!
    ^Path []
    (Files/createTempDirectory "fixpoint-embedded-amqp" empty-array)))

(let [delete-visitor (proxy [SimpleFileVisitor] []
                       (visitFile [file _]
                         (Files/delete file)
                         FileVisitResult/CONTINUE)
                       (postVisitDirectory [directory exception]
                         (when-not exception
                           (Files/delete directory))
                         FileVisitResult/CONTINUE))]
  (defn delete-directory-recursively!
    [^Path directory]
    (Files/walkFileTree directory delete-visitor)))

(defn path->string
  [^Path path]
  (-> path
      (.toAbsolutePath)
      (str)))
