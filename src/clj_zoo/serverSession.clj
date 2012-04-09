(ns clj-zoo.serverSession
  (:require [clj-zoo.session :as session]
	[clj-zoo.watchFor :as wf] [zookeeper :as zk])
  (:gen-class :constructors {[String String String String] [],
                             [String String String String String] []}
              :state state
              :init -init
             ))

(defmacro app-in-env
  [serviceroot app env]
  `(str ~serviceroot "/" ~env "/" ~app))

(defmacro service-region-base
  [serviceroot app env region]
  `(str (app-in-env ~serviceroot ~app ~env) "/services/" ~region))

(defmacro passivated-region-base
  [serviceroot app env region]
  `(str (app-in-env ~serviceroot ~app ~env) "/passiveservices/" ~region))

(defmacro create-passive-base
  [serviceroot app env]
  `(str (app-in-env ~serviceroot ~app ~env) "/createpassive"))

(defmacro request-passivation-region-base
  [serviceroot app env region]
  `(str (app-in-env ~serviceroot ~app ~env) "/requestpassivation"))

(defmacro request-activation-region-base
  [serviceroot app env region]
  `(str (app-in-env ~serviceroot ~app ~env) "/requestactivation"))

(defmacro request-passivation-node
  [serviceroot app env region active-node]
  `(let [service-base# (service-region-base ~serviceroot ~app ~env ~region)
	service-part# (clojure.string/replace-first ~active-node
			(str (app-in-env ~serviceroot ~app ~env) "/services") "")]
	 (str (request-passivation-region-base ~serviceroot ~app ~env ~region)
		service-part#)))


(defmacro request-activation-node
  [serviceroot app env region passive-node]
  `(let [service-base# (service-region-base ~serviceroot ~app ~env ~region)
	service-part# (clojure.string/replace-first ~passive-node
			(str (app-in-env ~serviceroot ~app ~env) "/passiveservices") "")]
	 (str (request-activation-region-base ~serviceroot ~app ~env ~region)
		service-part#)))

(defmacro passive-service-node-pattern
  [serviceroot app env region name major minor micro]
  `(str (passivated-region-base ~serviceroot ~app ~env ~region)
	"/" ~name  "/" ~major "/" ~minor "/" ~micro "/instance-"))

(defmacro passivate-request-base
  [serviceroot app env region])

(defmacro service-node-pattern [serviceroot app env region name major minor micro]
  `(str (service-region-base ~serviceroot ~app ~env ~region)
        "/" ~name "/" ~major "/" ~minor "/" ~micro "/instance-"))

(defmacro server-region-base
  [serviceroot app env region]
  `(str (app-in-env ~serviceroot ~app ~env) "/servers/" ~region))

(defmacro server-node-pattern [serviceroot app env region]
  `(str (server-region-base ~serviceroot ~app ~env ~region) "/instance-"))

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
            (let [data-version (:version (zk/exists client server-node))
                  data-bytes (.getBytes (str "1\n" c-load "\n") "UTF-8")]
              (zk/set-data client server-node data-bytes data-version))))
        (if (not (.. client getState isAlive)) (println "QUIT") (recur c-range))))))

(defn login
  ([keepers env app region] (login keepers env app region "/services"))
  ([keepers env app region serviceroot]
     (let [client (session/login keepers)
           server-node (server-node-pattern serviceroot app env region)
           instance (zk/create-all client server-node :sequential? true)]
       (.start (Thread. (fn [] (load-updater client instance 0.0))))
       (ref {:serviceroot serviceroot
             :env env
             :instance instance
             :region region
             :client client
             :services {}
             :app app})
       )))

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
        serviceroot (:serviceroot @session)
        app (:app @session)
        env (:env @session)
        passivate-node (str (create-passive-base serviceroot app env) "/" service) ]
    (zk/exists client passivate-node))
  )

(defn- create-service-node
  "create either passive or active node"
  [session passivated? name major minor micro]
  (let [node-name (if passivated?
			(passive-service-node-pattern
				(:serviceroot @session)
				(:app @session)
				(:env @session)
				(:region @session)
				name
				major
				minor
				micro)
			(service-node-pattern
				(:serviceroot @session)
				(:app @session)
				(:env @session)
				(:region @session)
				name
				major
				minor
				micro))
	client (:client @session)]
	(zk/create-all client node-name :sequential? true)))
	
(defn- activate
  [session name major minor micro url passive-node]
  (println "CALLED -activate")
  )

(defn -watch-for-activate
  [session name major minor micro url passive-node event]
  (println (str "WATCH FOR ACTIVATE: " event))
  )

(defn- passivate
  [session name major minor micro url active-node event]
  (println (str "CALLED passivate: " event))
  (if (= (:event-type event) :NodeCreated)
	(let [created-node (:path event)
		other (println "IN PASS LET")
		client (:client @session)
		other2 (println "IN PASS LET")
		active-data (zk/data client active-node)
		other3 (println "IN PASS LET")
		node (create-service-node session true
			name major minor micro)
		other4 (println "IN PASS LET")
		data-ver (:version (zk/exists client node))]
		(println (str "IN PASS LET post dataver: " data-ver))
		(println (str "-created-node: " created-node))
		(try
		(zk/set-data client node active-data data-ver)
		(catch Exception e (println (str "EX in PASS" e))))
		(println (str "PASSIAVATE CREATED NODE: " node))
		(zk/delete client active-node)
		(println (str "PASSIVATE DELETED NODE: " active-node))))
  )


(defn- watch-for-passivate
  [session name major minor micro url active-node]
  (println "WATCH FOR PASSIVATE")
  (let [client (:client @session)
	active-exists (zk/exists client active-node)]
	(if active-exists 
		(let [passivate-node (request-passivation-node
					(:serviceroot @session)
					(:app @session)
					(:env @session)
					(:region @session)
					active-node)]
			(println (str "-- WAITFOR passivate node: " passivate-node))
			(zk/exists client passivate-node
				:watcher (partial passivate session name major
						minor micro url
						active-node))))))

(defn registerService
  [session serviceName major minor micro url]
  (let [client (:client @session)
	cre-passivated (create-passivated? session serviceName)
        node-name (service-node-pattern (:serviceroot @session)
                                        (:app @session)
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
	true
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
  ([keepers env app region serviceroot] [[] (login keepers env app region serviceroot)])
  ([keepers env app region] [[]] (-init keepers env app region "/services")))
