(defproject stylefruits/fixpoint "0.1.1-SNAPSHOT"
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
           :exclusions [org.clojure/clojure]]]}}
  :test-selectors {:default    #(not-any? % [:elastic :mysql :postgresql])
                   :elastic    :elastic
                   :mysql      :mysql
                   :postgresql :postgresql}
  :pedantic? :abort)
