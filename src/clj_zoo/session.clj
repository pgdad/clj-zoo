(ns clj-zoo.session
  (:import (com.netflix.curator.framework CuratorFrameworkFactory
                                          CuratorFramework)
           (com.netflix.curator.retry RetryNTimes))
  (:require [org.clojars.pgdad.zookeeper :as zk]))

(declare ensure-root-exists)

(defn login
  [keepers]
  (ensure-root-exists keepers)
  (let [rp (RetryNTimes. 1000 100)
        f-work (CuratorFrameworkFactory/newClient keepers rp)]
    (.start f-work)
    (ref {:client (-> f-work .getZookeeperClient .getZooKeeper)
          :fWork f-work})))

(defn logout
  [session]
  (let [f-work (:fWork @session)]
    (.close f-work)))

(defn create-non-existing-node
  [fWork node]
  (if-not (-> fWork .checkExists (.forPath node))
    (-> fWork .create .creatingParentsIfNeeded (.forPath node))))

(defn ensure-root-exists
  [keepers]
  (let [keepers-parts (clojure.string/split keepers #"/")
        host-part (first keepers-parts)
        chroot-part (second keepers-parts)]
    (if chroot-part
      (let [z-session (login host-part)
            fWork (:fWork @z-session)]
        (create-non-existing-node fWork (str "/" chroot-part))
        (logout z-session)))))
