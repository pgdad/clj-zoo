(defproject clj-zoo "1.0.11"
  :description "FIXME: write description"
  :dependencies [[org.clojure/clojure "1.4.0"]
                 [org.clojars.pgdad/zookeeper-clj "0.9.3"]
                 [com.netflix.curator/curator-recipes "1.1.13"]
                 [com.netflix.curator/curator-framework "1.1.13"]]
  :repl-init clj-zoo.serverSession
  :aot [clj-zoo.session clj-zoo.serverSession clj-zoo.watchFor]
  :warn-on-reflection true
  :plugins [[lein-swank "1.4.4"]]
  :jar-exclusions [#"project.clj"]
  :omit-source true)
