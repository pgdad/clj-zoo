(ns clj-zoo.serverSession
  (:require [clj-zoo.session :as session]
            [clj-zoo.watchFor :as wf] [zookeeper :as zk])
  (:gen-class :constructors {[String String String String] [],
                             [String String String String String] []}
              :state state
              :init -init
              ))

(defmacro app-in-env
  [app env]
  `(str "/" ~env "/" ~app))

(defmacro service-region-base
  [app env region]
  `(str (app-in-env ~app ~env) "/services/" ~region))

(defmacro passivated-region-base
  [app env region]
  `(str (app-in-env ~app ~env) "/passiveservices/" ~region))

(defmacro create-passive-base
  [app env]
  `(str (app-in-env ~app ~env) "/createpassive"))

(defmacro request-passivation-region-base
  [app env region]
  `(str (app-in-env ~app ~env) "/requestpassivation"))

(defmacro request-activation-region-base
  [app env region]
  `(str (app-in-env ~app ~env) "/requestactivation"))

(defmacro request-passivation-node
  [app env region active-node]
  `(let [service-base# (service-region-base ~app ~env ~region)
         service-part# (clojure.string/replace-first ~active-node
                                                     (str (app-in-env ~app ~env) "/services") "")]
     (str (request-passivation-region-base ~app ~env ~region)
          service-part#)))

(defn my-passivation-request-node
  [active-node]
  (let [node-parts (clojure.string/split active-node (re-pattern "/"))
        env (nth node-parts 1)
        app (nth node-parts 2)
        region (nth node-parts 4)]
    (request-passivation-node app env region active-node)))

(defmacro request-activation-node
  [app env region passive-node]
  `(let [service-base# (service-region-base ~app ~env ~region)
         service-part# (clojure.string/replace-first ~passive-node
                                                     (str (app-in-env ~app ~env) "/passiveservices") "")]
     (str (request-activation-region-base ~app ~env ~region)
          service-part#)))

(defn my-activation-request-node
  [passive-node]
  (let [node-parts (clojure.string/split passive-node (re-pattern "/"))
        env (nth node-parts 1)
        app (nth node-parts 2)
        region (nth node-parts 4)]
    (request-activation-node app env region passive-node)))

(defmacro passive-service-node-pattern
  [app env region name major minor micro]
  `(str (passivated-region-base ~app ~env ~region)
	"/" ~name  "/" ~major "/" ~minor "/" ~micro "/instance-"))

(defmacro passivate-request-base
  [app env region])

(defmacro service-node-pattern [app env region name major minor micro]
  `(str (service-region-base ~app ~env ~region)
        "/" ~name "/" ~major "/" ~minor "/" ~micro "/instance-"))

(defmacro server-region-base
  [app env region]
  `(str (app-in-env ~app ~env) "/servers/" ~region))

(defmacro server-node-pattern [app env region]
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

(defn- gen-instance-data-bytes
  [c-load host]
  (.getBytes (str "1\n" c-load "\n" host "\n") "UTF-8"))

(defn- load-updater
  [client host server-node prev-load-range]
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

(defn login
  [keepers env app region]
  (let [client (session/login keepers)
        host (.. java.net.InetAddress getLocalHost getHostName)
        server-node (server-node-pattern app env region)
	c-load (current-load)
	data-bytes (gen-instance-data-bytes c-load host)
        instance (zk/create-all client server-node
			:sequential? true :data data-bytes)]
    (.start (Thread. (fn [] (load-updater client host instance 0.0))))
    (ref {:env env
          :instance instance
          :region region
          :client client
          :services {}
          :app app})
    ))

(defn logout
  [session]
  (zk/close (:client @session)))

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
  (let [client (:client @session)
        app (:app @session)
        env (:env @session)
        passivate-node (str (create-passive-base app env) "/" service) ]
    (zk/exists client passivate-node))
  )

(defn- create-service-node
  "create either passive or active node"
  [session passivated? data-bytes name major minor micro]
  (let [node-name (if passivated?
                    (passive-service-node-pattern
                     (:app @session)
                     (:env @session)
                     (:region @session)
                     name
                     major
                     minor
                     micro)
                    (service-node-pattern
                     (:app @session)
                     (:env @session)
                     (:region @session)
                     name
                     major
                     minor
                     micro))
	client (:client @session)]
    (zk/create-all client node-name :sequential? true :data data-bytes)))

(declare watch-for-passivate)

(defn- activate
  [session name major minor micro url passive-node event]
  (if (= (:event-type event) :NodeCreated)
    (let [created-node (:path event)
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
  (let [client (:client @session)
	passive-exists (zk/exists client passive-node)]
    (if passive-exists 
      (let [activate-node (request-activation-node
                           (:app @session)
                           (:env @session)
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
          client (:client @session)
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
  (let [client (:client @session)
	active-exists (zk/exists client active-node)]
    (if active-exists 
      (let [passivate-node (request-passivation-node
                            (:app @session)
                            (:env @session)
                            (:region @session)
                            active-node)]
        (zk/exists client passivate-node
                   :watcher (partial passivate session name major
                                     minor micro url
                                     active-node))))))

(defn registerService
  [session serviceName major minor micro url]
  (let [client (:client @session)
	cre-passivated (create-passivated? session serviceName)
        node-name (service-node-pattern (:app @session)
                                        (:env @session)
                                        (:region @session)
                                        serviceName
                                        major
                                        minor
                                        micro)
        service-node (create-service-node
                      session
                      cre-passivated
                      serviceName
                      major
                      minor
                      micro)
        server-node (:instance @session)
        service-data-version (:version (zk/exists client service-node))]
    (zk/set-data client service-node
                 (.getBytes (str "1\n" server-node "\n" url "\n") "UTF-8") service-data-version)
    (dosync
     (alter session add-service-to-session
            {service-node {:url url :passive cre-passivated}}))
    (if cre-passivated
      ;; start watching for activate request
      (watch-for-activate session serviceName major minor micro url service-node)
      ;; else start watching for passivate requests
      (watch-for-passivate session serviceName major minor micro url service-node))
    
    ))

(defn unregisterService
  [session service-node]
  (let [client (:client @session)]
    (zk/delete client service-node)
    (dosync
     (alter session rm-service-from-session service-node))))

(defn -init
  [keepers env app region] [[] (login keepers env app region)])
