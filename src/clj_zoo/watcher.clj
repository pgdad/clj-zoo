(ns clj-zoo.watcher
  (:import (com.netflix.curator.framework.api CuratorWatcher)
           (org.apache.zookeeper WatchedEvent)))

(defn watcher
  [f]
  (proxy [com.netflix.curator.framework.api.CuratorWatcher] []
    (process [event]
      (f event))))
