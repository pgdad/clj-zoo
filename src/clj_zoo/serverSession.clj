(ns clj-zoo.serverSession
  (:require [clj-zoo.session :as session]
            [clj-zoo.watchFor :as wf]
            [org.clojars.pgdad.zookeeper :as zk])
  (:import (org.apache.zookeeper ZooKeeper))
  (:gen-class :constructors {[String String] []}
              :state state
              :init init
              :implements [clojure.lang.IDeref]
              :methods [[registerService [String int int int String] String]
                        [unregisterService [String] void]
                        [unregisterAllServices [] void]
                        [logout [] void]]
              ))

(def ^:const slash-re #"/")

(defmacro service-region-base
  [region]
  `(str "/services/" ~region))

(defmacro passivated-region-base
  [region]
  `(str "/passiveservices/" ~region))

(def create-passive-base "/createpassive")

(def request-passivation-base "/requestpassivation")

(def request-activation-base "/requestactivation")

(defmacro request-passivation-node
  [region active-node]
  `(let [service-base# (service-region-base ~region)
         service-part# (clojure.string/replace-first ~active-node
                                                     "/services" "")]
     (str request-passivation-base service-part#)))

(defn my-passivation-request-node
  [active-node]
  (let [node-parts (clojure.string/split active-node slash-re)
        region (nth node-parts 2)]
    (request-passivation-node region active-node)))

(defn request-passivation
  [connection node]
  (when (zk/exists connection node)
    (zk/create-all connection (my-passivation-request-node node)
                   :persistent? true)))

(defmacro request-activation-node
  [region passive-node]
  `(let [service-base# (service-region-base ~region)
         service-part# (clojure.string/replace-first ~passive-node
                                                     "/passiveservices" "")]
     (str request-activation-base service-part#)))

(defn my-activation-request-node
  [passive-node]
  (let [node-parts (clojure.string/split passive-node slash-re)
        region (nth node-parts 2)]
    (request-activation-node region passive-node)))

(defn request-activation
  [connection node]
  (when (zk/exists connection node)
    (zk/create-all connection (my-activation-request-node node)
                   :persistent? true)))

(defmacro passive-service-node-pattern
  [region name major minor micro]
  `(str (passivated-region-base ~region)
	"/" ~name  "/" ~major "/" ~minor "/" ~micro "/instance-"))

(defmacro service-node-pattern [region name major minor micro]
  `(str (service-region-base ~region)
        "/" ~name "/" ~major "/" ~minor "/" ~micro "/instance-"))

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
  [^ZooKeeper client host server-node prev-load-range]
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
            (let [data-version (:version (zk/exists client server-node))
                  data-bytes (gen-instance-data-bytes c-load host)]
              (zk/set-data client server-node data-bytes data-version))))
        (if (not (.. client getState isAlive)) (println "QUIT") (recur c-range))))))

(defn- my-host*
  []
  (.. java.net.InetAddress getLocalHost getHostName))

(defn login
  [keepers region]
  (let [c-session (session/login keepers)
        client (:client @c-session)
        host (my-host*)
        server-node (server-node-pattern region)
	c-load (current-load)
	data-bytes (gen-instance-data-bytes c-load host)
        instance (zk/create-all client server-node
			:sequential? true :data data-bytes)]
    (.start (Thread. (fn [] (load-updater client host instance 0.0))))
    (ref {:fWork (:fWork @c-session)
          :instance instance
          :region region
          :client client
          :services {}})
    ))

(defn logout
  [session]
  (session/logout session))

(defn -logout
  [this]
  (logout (.state this)))

(defn- add-service-to-session
  [session service-def]
  (let [services (:services session)]
    (assoc session :services (merge services service-def))))

(defn- rm-service-from-session
  [session service-node]
  (let [services (:services session)
        new-services (dissoc services service-node)]
    (assoc session :services new-services)))

(defn- create-passivated?
  [session service]
  (let [fWork (:fWork @session)
        client (:client @session)
        host (my-host*)
        region (:region @session)
        passivate-service-node (str create-passive-base "/" service)
        passivate-region-service-node (str create-passive-base "/_region/" region "/" service)
        passivate-host-service-node (str create-passive-base "/_host/" host "/" service)
        passivate-region-node (str create-passive-base "/_region/_all")
        passivate-host-node (str create-passive-base "/_host/" host "/_all")]
     (or (zk/exists client passivate-service-node)
         (zk/exists client passivate-region-service-node)
         (zk/exists client passivate-host-service-node)
         (zk/exists client passivate-region-node)
         (zk/exists client passivate-host-node))
  ))

(defn- create-service-node
  "create either passive or active node"
  [session passivated? data-bytes name major minor micro]
  (let [node-name (if passivated?
                    (passive-service-node-pattern
                     (:region @session)
                     name
                     major
                     minor
                     micro)
                    (service-node-pattern
                     (:region @session)
                     name
                     major
                     minor
                     micro))
	client (:client (:curator @session))]
    (zk/create-all client node-name :sequential? true :data data-bytes)))

(declare watch-for-passivate)

(defn- activate
  [session name major minor micro url passive-node event]
  (if (= (:event-type event) :NodeCreated)
    (let [created-node (:path event)
          fWork (:fWork @session)
          client (:client @session)
          passive-data (:data (zk/data client passive-node))
          node (create-service-node session false passive-data
                                    name major minor micro)]
      (zk/delete client passive-node)
      (watch-for-passivate session name major minor micro url node)
      (zk/delete client created-node)))
  )

(defn- watch-for-activate
  [session name major minor micro url passive-node]
  (let [client (:client (:curator @session))
	passive-exists (zk/exists client passive-node)]
    (if passive-exists 
      (let [activate-node (request-activation-node
                           (:region @session)
                           passive-node)]
        (zk/exists client activate-node
                   :watcher (partial activate session name major
                                     minor micro url
                                     passive-node))))))

(defn- passivate
  [session name major minor micro url active-node event]
  (if (= (:event-type event) :NodeCreated)
    (let [created-node (:path event)
          client (:client (:curator @session))
          active-data (:data (zk/data client active-node))
          node (create-service-node session true active-data
                                    name major minor micro)]
      (zk/delete client active-node)
      (watch-for-activate session name major minor micro url node)
      (zk/delete client created-node))
    )
  )


(defn- watch-for-passivate
  [session name major minor micro url active-node]
  (let [fWork (:fWork @session)
        client @session
	active-exists (zk/exists client active-node)]
    (if active-exists 
      (let [passivate-node (request-passivation-node
                            (:region @session)
                            active-node)]
        (zk/exists client passivate-node
                   :watcher (partial passivate session name major
                                     minor micro url
                                     active-node))))))

(defn registerService
  "returns the original zookeeper node created for this service
   The node can change over time (is passivated/activate) but is unique
   for the duration of the session. The original node can be used to
   unregister the service."
  [session serviceName major minor micro url]
  (let [fWork (:fWork @session)
        client (:client @session)
	cre-passivated (create-passivated? session serviceName)
        server-node (:instance @session)
        service-node (create-service-node
                      session
                      cre-passivated
                      (.getBytes (str "1\n" server-node "\n" url "\n") "UTF-8")
                      serviceName
                      major
                      minor
                      micro)]

    (dosync
     (alter session add-service-to-session
            {service-node {:url url
                           :passive cre-passivated
                           :name serviceName
                           :current-node service-node
                           :major major
                           :minor minor
                           :micro micro}}))
    (if cre-passivated
      ;; start watching for activate request
      (watch-for-activate session serviceName major minor micro url service-node)
      ;; else start watching for passivate requests
      (watch-for-passivate session serviceName major minor micro url service-node))
    
    service-node
    ))

(defn -registerService [this serviceName major minor micro url]
  (registerService (.state this) serviceName major minor micro url))

(defn unregisterService
  [session service-node]
  (let [client (:client @session)
        service (:services @session)
        current-node (:current-node (service service-node))]
    (zk/delete client service-node)
    (dosync
     (alter session rm-service-from-session service-node))))

(defn -unregisterService [this service-node]
  (unregisterService (.state this) service-node))

(defn unregisterAllServices
  [session]
  (doseq [service (keys (:services @session))]
    (unregisterService session service)))

(defn -unregisterAllServices [this]
  (unregisterAllServices (.state this)))

(defn -init
  [keepers region] [[] (login keepers region)])
