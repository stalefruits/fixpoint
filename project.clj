(defproject stylefruits/fixpoint "0.1.3-SNAPSHOT"
  :description "Simple & Powerful Test Fixtures/Datasources for Clojure"
  :url "https://github.com/stylefruits/fixpoint"
  :license {:name "Apache License 2.0"
            :url "http://www.apache.org/licenses/LICENSE-2.0.html"
            :author "stylefruits GmbH"
            :year 2017
            :key "apache-2.0"}
  :dependencies [[org.clojure/clojure "1.8.0" :scope "provided"]
                 [org.clojure/java.jdbc "0.6.1" :scope "provided"]
                 [camel-snake-kebab "0.4.0"]]
  :profiles
  {:dev {:dependencies
         [[hikari-cp "1.7.5"]
          [org.postgresql/postgresql "9.4.1212"]
          [mysql/mysql-connector-java "5.1.41"]
          [cc.qbits/spandex "0.3.4"
           :exclusions [org.clojure/clojure]]
          [org.apache.qpid/qpid-broker "6.1.3"
           :exclusions [org.webjars.bower/dstore
                        org.slf4j/slf4j-api]]
          [kithara "0.1.8"]]}
   :codox {:dependencies [[org.clojure/tools.reader "1.0.0"]
                          [codox-theme-rdash "0.1.2"]]
           :plugins [[lein-codox "0.10.3"]]
           :codox {:project {:name "fixpoint"}
                   :metadata {:doc/format :markdown}
                   :themes [:rdash]
                   :source-paths ["src"]
                   :output-path "docs"

                   :source-uri "https://github.com/stylefruits/fixpoint/blob/master/{filepath}#L{line}"
                   :namespaces [fixpoint.core #"^fixpoint\.datasource\..*"]}}}
  :test-selectors {:default    #(not-any? % [:elastic :mysql :postgresql])
                   :elastic    :elastic
                   :mysql      :mysql
                   :postgresql :postgresql}
  :aliases {"codox" ["with-profile" "codox,dev" "codox"]}
  :pedantic? :abort)
