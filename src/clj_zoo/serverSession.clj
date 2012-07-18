(ns clj-zoo.serverSession
  (:require [clj-zoo.session :as session]
            [clj-zoo.util :as util]
            [clj-zoo.watchFor :as wf]
            [clj-zoo.serverNode :as serverNode])
  (:import (java.util LinkedHashMap)
           (org.apache.zookeeper ZooKeeper CreateMode)
           (com.netflix.curator.framework CuratorFramework)
           (com.netflix.curator.framework.api CuratorWatcher)
           (com.netflix.curator.x.discovery ServiceDiscoveryBuilder
                                            ServiceInstanceBuilder
                                            ServiceInstance
                                            UriSpec)
           (com.netflix.curator.x.discovery.details JsonInstanceSerializer))
  (:gen-class :constructors {[String String] []}
              :state state
              :init init
              :implements [clojure.lang.IDeref]
              :methods [[registerService [String int int int String] String]
                        [unregisterService [String] void]
                        [unregisterAllServices [] void]
                        [logout [] void]]
              ))

(defn- ->keyword
  [key]
  (if (keyword? key)
    key
    (keyword (if (.startsWith key ":") (.substring key 1) key))))

(defn- ->clj
 [o]
 (let [entries (.entrySet o)]
   (reduce (fn [m [^String k v]]
             (assoc m (->keyword k) v))
           {} entries)))

(defn service->payload-map
  [service]
  (let [result (->clj (.getPayload service))]
    (assoc result :id (.getId service))))

(def create-passive-base "/createpassive")

(def request-passivation-base "/requestpassivation")

(def request-activation-base "/requestactivation")

(defn login
  [keepers region]
  (let [c-session (session/login keepers)
        server-node (serverNode/create (:fWork @c-session)
                                       region)]
    (ref {:fWork (:fWork @c-session)
          :instance server-node
          :region region
          :client (:client @c-session)
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
        host (serverNode/my-host*)
        region (:region @session)
        passivate-service-node (str create-passive-base "/" service)
        passivate-region-service-node (str create-passive-base "/_region/" region "/" service)
        passivate-host-service-node (str create-passive-base "/_host/" host "/" service)
        passivate-region-node (str create-passive-base "/_region/_all")
        passivate-host-node (str create-passive-base "/_host/" host "/_all")]
     (or (util/exists? fWork passivate-service-node)
         (util/exists? fWork passivate-region-service-node)
         (util/exists? fWork passivate-host-service-node)
         (util/exists? fWork passivate-region-node)
         (util/exists? fWork passivate-host-node))
  ))

(defn- build-service-discovery
  ([fWork path]
     (let [sdb (ServiceDiscoveryBuilder/builder (class (LinkedHashMap.)))
           sd (-> sdb (.client fWork) (.basePath path)
                  (.serializer (JsonInstanceSerializer.
                                (class (LinkedHashMap.))))
                  .build)]
       (.start sd)
       sd))
  ([fWork basePath region]
     (build-service-discovery fWork (str basePath region)))
  )

(def sd-builder (memoize build-service-discovery))

(defn passivated-discovery
  [fWork region]
  (sd-builder fWork "/passivatedservices/" region))

(defn activated-discovery
  [fWork region]
  (sd-builder fWork "/services/" region))

(defn- create-instance-0
  "create either passive or active node"
  [ssl? region server name major minor micro port uri url]
  (let [u-spec (UriSpec. (str "{scheme}://{address}:{port}" uri))
        si (-> (ServiceInstance/builder)
               (.name name)
               (.uriSpec u-spec)
               (.payload {:major major :minor minor
                          :micro micro :server server
                          :region region :url url}))
        si-with-port (if ssl? (.sslPort si port) (.port si port))]
    (.build si-with-port)))

(declare watch-for-passivate)
(declare watch-for-passivate-0)
(declare watch-for-activate-0)

(defn- register-instance
  [discovery instance]
  (-> discovery (.registerService instance)))

(defn- unregister-instance
  [discovery instance]
  (-> discovery (.unregisterService instance)))

(defn- activate-0
  [fWork instance]
  (let [region (-> instance service->payload-map :region)
        active-discovery (activated-discovery fWork region)
        passive-discovery (passivated-discovery fWork region)]
    (unregister-instance passive-discovery instance)
    (register-instance active-discovery instance)
    (watch-for-passivate-0 fWork instance)))

(defn- passivate-0
  [fWork instance]
  (let [region (-> instance service->payload-map :region)
        active-discovery (activated-discovery fWork region)
        passive-discovery (passivated-discovery fWork region)]
    (unregister-instance active-discovery instance)
    (register-instance passive-discovery instance)
    (watch-for-activate-0 fWork instance)))

(defn- activateWatcher
  [fWork instance]
  (proxy [com.netflix.curator.framework.api.CuratorWatcher] []
    (process [event]
      (do
        (println (str "ACTIVATE WATCHER: " event))
        (activate-0 fWork instance)))))

(defn- passivateWatcher
  [fWork instance]
  (proxy [com.netflix.curator.framework.api.CuratorWatcher] []
    (process [event]
      (do
        (println (str "PASSIVATE WATCHER: " event))
        (passivate-0 fWork instance)))))

(defn- watch-for-activate-0
  [fWork instance]
  (let [region (-> instance service->payload-map :region)
        name (.getName instance)
        id (.getId instance)
        path (str "/requestactivation/" region "/" name "/" id)]
    (-> fWork .checkExists (.usingWatcher
                            (activateWatcher fWork instance)) (.forPath path))))

(defn- watch-for-passivate-0
  [fWork instance]
  (let [region (-> instance service->payload-map :region)
        name (.getName instance)
        id (.getId instance)
        path (str "/requestpassivation/" region "/" name "/" id)]
    (-> fWork .checkExists (.usingWatcher
                            (passivateWatcher fWork instance)) (.forPath path))))

(defn- passivationRequestPath
  [region name id]
  (str "/requestpassivation/" region "/" name "/" id))

(defn- activationRequestPath
  [region name id]
  (str "/requestactivation?" region "/" name "/" id))

(defn- deletePassivationRequest
  [fWork region name id]
  (util/delete-path fWork (passivationRequestPath region name id)))

(defn requestPassivation
  [fWork region name id]
  (-> fWork .create .creatingParentsIfNeeded
      (.forPath (passivationRequestPath region name id))))

(defn- deleteActivationRequest
  [fWork region name id]
  (util/delete-path fWork (activationRequestPath region name id)))

(defn requestActivation
  [fWork region name id]
  (-> fWork .create .creatingParentsIfNeeded
      (.forPath (str "/requestactivation/" region "/" name "/" id))))

(defn registerService
  "returns the original zookeeper node created for this service
   The node can change over time (is passivated/activate) but is unique
   for the duration of the session. The original node can be used to
   unregister the service."
  [session ssl? serviceName major minor micro port uri url]
  (let [fWork (:fWork @session)
        region (:region @session)
        client (:client @session)
	cre-passivated (create-passivated? session serviceName)
        server-node (:instance @session)
        discovery (if cre-passivated
                    (passivated-discovery fWork region)
                    (activated-discovery fWork region))
        instance (create-instance-0 ssl?
                                    (:region @session)
                                    server-node
                                    serviceName
                                    major
                                    minor
                                    micro
                                    port
                                    uri
                                    url)]

    (register-instance discovery instance)
    (dosync
     (alter session add-service-to-session
            {(str serviceName "/" (.getId instance))
             {:uri uri
              :passive cre-passivated
              :instance instance
              :name serviceName
              :id (.getId instance)
              :major major
              :minor minor
              :micro micro}}))
    (if cre-passivated
      ;; start watching for activate request
      (watch-for-activate-0 fWork instance)
      ;; else start watching for passivate requests
      (watch-for-passivate-0 fWork instance))
    instance
    ))

(defn -registerService [this serviceName major minor micro url]
  (registerService (.state this) serviceName major minor micro url))

(defn unregisterService
  [session service-node]
  (let [fWork (:fWork @session)
        service (:services @session)
        current-node (:current-node (service service-node))]
    (util/delete-path fWork service-node)
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
