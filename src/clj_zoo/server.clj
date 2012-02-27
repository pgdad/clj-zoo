(ns clj-zoo.server
  (:require [zookeeper :as zk] [clojure.zip :as zipper]
            [clojure.string] [clojure.set])
  (:import [java.lang.Thread] [java.lang.management.ManagementFactory])
  (:use [clj-zoo.util])
  (:gen-class))

(defmacro service-region-base
  [app env region]
  `(str (app-in-env ~app ~env) "/services/" ~region))

(defmacro service-node-parent
  [service-base service-name
   service-version-major service-version-minor service-version-micro]
  `(str ~service-base "/"
        ~service-name "/"
        ~service-version-major "/"
        ~service-version-minor "/"
        ~service-version-micro))

(defmacro service-node-pattern
  [service-base service-name service-version-major
   service-version-minor service-version-micro]
  `(str (service-node-parent ~service-base
                             ~service-name
                             ~service-version-major
                             ~service-version-minor
                             ~service-version-micro)
        "/instance-"))
(defmacro server-region-base
  [app env region]
  `(str (app-in-env ~app ~env) "/servers/" ~region))

(defmacro server-node-pattern
  [app env region]
  `(str (server-region-base ~app ~env ~region) "/instance-"))


(defn- current-load
  []
  (. (java.lang.management.ManagementFactory/getOperatingSystemMXBean)
     getSystemLoadAverage))

(defn- load-range
  [l]
  (if (> l 1.00)
    (+ 1 (quot l 1))
    (/ (+ 1 (quot (* 10 l) 1)) 10)))

(defn- load-updater
  [client server-node prev-load-range]
  (loop [prev-range prev-load-range]
    (let [alive (.. client getState isAlive)
          node-exists (zk/exists client server-node)]
      (Thread/sleep 5000)
      (let [c-load (current-load)
            c-range (load-range c-load)]
        (if (and (.. client getState isAlive)
                 (zk/exists client server-node)
                 (not (== c-range prev-range)))
          (do
            (let [data-version (:version (zk/exists client server-node))]
              (zk/set-data
               client server-node
               (.getBytes (str "1\n" c-load "\n") "UTF-8") data-version))))
        (if (not (.. client getState isAlive)) (println "QUIT") (recur c-range))))))

(defn server-login
  [keepers env app region]
  (let [client (zk/connect keepers)
        app-base (app-in-env app env)
        region-base (str app-base "/" region)
        service-base (service-region-base app env region)
        server-base (server-region-base app env region)
        server-node (server-node-pattern app env region)
        session (hash-map :client client :service-base service-base
                          :server-node server-node
                          :services (hash-set))]
    (zk/create-all client service-base :persistent? true)
    (zk/create-all client server-base :persistent? true)
    (let [full-server-path (zk/create client server-node :sequential? true)]
      (.start (Thread. (fn [] (load-updater client full-server-path 0.0))))
      (ref (assoc session :server-node full-server-path)))))

(defn- add-service-to-session
  [session service-node]
  (let [services (:services session)]
    (assoc session :services (merge services service-node))))

(defn- drop-services-from-session
  [session]
  (assoc session :services (hash-set)))

(defn register-service
  [session service-name
   service-version-major service-version-minor service-version-micro
   service-url]
  (let [service-base (:service-base @session)
        client (:client @session)
        service-node (zk/create-all
                      client
                      (service-node-pattern service-base
                                            service-name
                                            service-version-major
                                            service-version-minor
                                            service-version-micro)
                      :sequential? true)
        server-node (:server-node @session)
        service-data-version (:version (zk/exists client service-node))]
    (zk/set-data client service-node
                 (.getBytes (str "1\n" server-node "\n" service-url "\n") "UTF-8") service-data-version)
    (dosync
     (alter session add-service-to-session service-node))))

(defn unregister-services
  [session]
  (let [services (:services @session)
        client (:client @session)]
    (doseq [service services] (zk/delete client service))
    (dosync
     (alter session drop-services-from-session))))

