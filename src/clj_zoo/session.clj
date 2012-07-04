(ns clj-zoo.session
  (:import (com.netflix.curator.framework CuratorFrameworkFactory
                                          CuratorFramework)
           (com.netflix.curator.retry RetryNTimes))
  (:require [org.clojars.pgdad.zookeeper :as zk]))

(defn login
  [keepers]
  (let [rp (RetryNTimes. 1000 100)
        f-work (CuratorFrameworkFactory/newClient keepers rp)]
    (.start f-work)
    (ref {:client (-> f-work .getZookeeperClient .getZooKeeper)
          :fWork f-work})))

(defn logout
  [session]
  (let [f-work (:fWork @session)]
    (.close f-work)))