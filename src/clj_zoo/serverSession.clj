(ns clj-zoo.serverSession
  (:require [clj-zoo.session :as session]
            [clj-zoo.watchFor :as wf]
            [clj-zoo.serverNode :as serverNode]
            [org.clojars.pgdad.zookeeper :as zk])
  (:import (org.apache.zookeeper ZooKeeper CreateMode)
           (com.netflix.curator.framework CuratorFramework)
           (com.netflix.curator.x.discovery ServiceDiscoveryBuilder
                                            ServiceInstanceBuilder
                                            ServiceInstance
                                            UriSpec))
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

(defn- service-full-path
  [base region name id]
  (str base "/" region "/" name "/" id))

(defmacro passive-service-node-pattern
  [region name major minor micro]
  `(str (passivated-region-base ~region)
	"/" ~name  "/" ~major "/" ~minor "/" ~micro "/instance-"))

(defmacro service-node-pattern [region name major minor micro]
  `(str (service-region-base ~region)
        "/" ~name "/" ~major "/" ~minor "/" ~micro "/instance-"))

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
     (or (zk/exists client passivate-service-node)
         (zk/exists client passivate-region-service-node)
         (zk/exists client passivate-host-service-node)
         (zk/exists client passivate-region-node)
         (zk/exists client passivate-host-node))
  ))

(defn- build-service-discovery
  [fWork basePath region]
  (let [sdb (ServiceDiscoveryBuilder/builder (class {}))
        sd (-> sdb (.client fWork) (.basePath (str basePath region))
               .build)]
    (.start sd)
    sd))

(def sd-builder (memoize build-service-discovery))

(defn- passivated-discovery
  [fWork region]
  (sd-builder fWork "/passivatedservices/" region))

(defn- activated-discovery
  [fWork region]
  (sd-builder fWork "/services/" region))

(defn- create-instance-0
  "create either passive or active node"
  [ssl? region server name major minor micro port uri]
  (let [u-spec (UriSpec. (str "{scheme}://{address}:{port}" uri))
        si (-> (ServiceInstance/builder)
               (.name name)
               (.uriSpec u-spec)
               (.payload {:major major :minor minor
                          :micro micro :server server
                          :region region}))
        si-with-port (if ssl? (.sslPort si port) (.port si port))]
    (.build si-with-port)))

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
	client (:client @session)]
    (zk/create-all client node-name :sequential? true :data data-bytes)))

(declare watch-for-passivate)
(declare watch-for-passivate-0)

(defn- register-instance
  [discovery instance]
  (-> discovery (.registerService instance)))

(defn- unregister-instance
  [discovery instance]
  (-> discovery (.unregisterService instance)))

(defn- activate-0
  [fWork active-discovery passive-discovery instance]
  (unregister-instance passive-discovery instance)
  (register-instance active-discovery instance)
  (watch-for-passivate-0 fWork active-discovery passive-discovery instance))

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

(defn- watch-for-activate-0
  [session instance])

(defn- watch-for-activate
  [session name major minor micro url passive-node]
  (let [client (:client @session)
	passive-exists (zk/exists client passive-node)]
    (if passive-exists 
      (let [activate-node (request-activation-node
                           (:region @session)
                           passive-node)]
        (zk/exists client activate-node
                   :watcher (partial activate session name major
                                     minor micro url
                                     passive-node))))))

(defn- create-active-0
  [session instance])

(defn- passivate-0
  [session discovery instance]
  (-> discovery (.unregisterService instance))
  (create-active-0 session instance)
  (watch-for-activate-0 session instance))

(defn- passivate
  [session name major minor micro url active-node event]
  (if (= (:event-type event) :NodeCreated)
    (let [created-node (:path event)
          client (:client @session)
          active-data (:data (zk/data client active-node))
          node (create-service-node session true active-data
                                    name major minor micro)]
      (zk/delete client active-node)
      (watch-for-activate session name major minor micro url node)
      (zk/delete client created-node))
    )
  )

(defn- passivate-watcher-0
  [event]
  ;; if event is node created ...
  )

(defn- watch-for-passivate-0
  [fWork active-discovery passive-discovery instance]
  (let [name (.getName instance)
        id (.getName instance)
        region (-> instance .getPayload :region)
        active-exists (-> fWork .checkExists
                          (.forPath (service-full-path "/services/"
                                                       region name id)))]
    (if active-exists
      (let [passivate-node (service-full-path "/passivatedservices/"
                                              region name id)]
        (-> fWork .checkExists (.usingWatcher passivate-watcher-0)
            (.forPath passivate-node))))))

(defn- watch-for-passivate
  [session name major minor micro url active-node]
  (let [fWork (:fWork @session)
        client (:client @session)
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
  [session ssl? serviceName major minor micro port uri]
  (let [fWork (:fWork @session)
        client (:client @session)
	cre-passivated (create-passivated? session serviceName)
        server-node (:instance @session)
        instance (create-instance-0 ssl?
                                    (:region @session)
                                    server-node
                                    serviceName
                                    major
                                    minor
                                    micro
                                    port
                                    uri)]

    (register-instance (activated-discovery fWork (:region @session)) instance)
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
    #_(if cre-passivated
      ;; start watching for activate request
      (watch-for-activate session serviceName major minor micro uri service-node)
      ;; else start watching for passivate requests
      (watch-for-passivate session serviceName major minor micro uri service-node))
    
    instance
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
