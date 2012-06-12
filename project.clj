(defproject clj-zoo "1.0.11"
  :description "FIXME: write description"
  :dependencies [[org.clojure/clojure "1.3.0"] [org.clojars.pgdad/zookeeper-clj "0.9.3"]]
  :repl-init clj-zoo.serverSession
  :aot [clj-zoo.session clj-zoo.serverSession clj-zoo.watchFor]
  :warn-on-reflection true
  :jar-exclusions [#"project.clj"]
  :omit-source true)
