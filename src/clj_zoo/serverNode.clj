(ns clj-zoo.serverNode
  (:import (com.netflix.curator.framework CuratorFramework)
           (org.apache.zookeeper CreateMode)))

(defmacro server-region-base
  [region]
  `(str "/servers/" ~region))

(defmacro server-node-pattern [region]
  `(str (server-region-base ~region) "/instance-"))

(defn- current-load
  []
  (. (java.lang.management.ManagementFactory/getOperatingSystemMXBean)
     getSystemLoadAverage))

(defn- load-range
  [l]
  (if (> l 1.00)
    (+ 1 (quot l 1))
    (/ (+ 1 (quot (* 10 l) 1)) 10)))

(defn- gen-instance-data-bytes
  [c-load host]
  (.getBytes (str "1\n" c-load "\n" host "\n") "UTF-8"))

(defn- load-updater
  [fWork host server-node prev-load-range]
  (loop [prev-range prev-load-range]
    (Thread/sleep 5000)
    (let [c-load (current-load)
          c-range (load-range c-load)]
      (if (and (-> fWork .getZookeeperClient .isConnected)
               (-> fWork .checkExists (.forPath server-node))
               (not (== c-range prev-range)))
        (let [data-bytes (gen-instance-data-bytes c-load host)]
          (-> fWork .setData (.forPath server-node data-bytes))))
      (recur c-range))))

(defn my-host*
  []
  (.. java.net.InetAddress getLocalHost getHostName))

(defn- create-node
  [fWork mode node data]
  (-> fWork .create (.withMode mode) .creatingParentsIfNeeded (.forPath node data)))

(defn create
  [fWork region]
  (let [node (server-node-pattern region)
        c-load (current-load)
        host (my-host*)
        data-bytes (gen-instance-data-bytes c-load host)
        created (create-node fWork
                             CreateMode/EPHEMERAL_SEQUENTIAL
                             node data-bytes)]
    (.start (Thread.
             (fn [] (load-updater fWork host created 0.0))))
    created))
