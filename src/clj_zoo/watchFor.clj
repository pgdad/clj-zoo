(ns clj-zoo.watchFor
  (:require [org.clojars.pgdad.zookeeper :as zk])
  )

(defn watcher-fn
  [connection f event]
  (let [node (:path event)]
    (if node
      (zk/exists connection node :watcher (partial watcher-fn connection f)))))

(defn watch
  "given a connection and a node watch for the creation and/or deletion of the node"
  [connection node f]
  (zk/exists connection node :watcher (partial watcher-fn connection f)))
