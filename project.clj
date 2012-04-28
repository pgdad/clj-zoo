(defproject clj-zoo "1.0.7"
  :description "FIXME: write description"
  :dependencies [[org.clojure/clojure "1.3.0"] [zookeeper-clj "0.9.2"]]
  :repl-init clj-zoo.serverSession
  :aot [clj-zoo.session clj-zoo.serverSession clj-zoo.watchFor])
