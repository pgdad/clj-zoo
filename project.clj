(defproject clj-zoo "1.0.0-SNAPSHOT"
  :description "FIXME: write description"
  :dependencies [[org.clojure/clojure "1.3.0"] [zookeeper-clj "0.9.2"]]
  :repl-init clj-zoo.core
  :aot [clj-zoo.util clj-zoo.server clj-zoo.client clj-zoo.core
	clj-zoo.servicewatcher])
