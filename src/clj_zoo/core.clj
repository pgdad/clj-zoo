(ns clj-zoo.core
  (:require [zookeeper :as zk] [clojure.zip :as zipper]
	[clojure.string] [clojure.set])
  (:import [java.lang.Thread] [java.lang.management.ManagementFactory])
  (:gen-class))

                                        ; session is map with keys:
                                        ; client, representing zk client
                                        ; envs, list of regexp used to match env part of the path
                                        ; regions, list of regexp used to match region part of the path
; 

(def ^:dynamic *service-root* "/services")

(defmacro dbg[x] `(let [x# ~x] (println "dbg:" '~x "=" x#) x#))

(defmacro app-in-env
  "/*service-root*/<env>/<app>"
  [app env]
  `(str *service-root* "/" ~env "/" ~app))

(defmacro client-instance-base
  "<app-in-env>/clients"
  [app env]
  `(str (app-in-env ~app ~env) "/clients"))

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

(defmacro client-service-lu-base
  [app-base region service-name service-version]
  `(str ~app-base "/services/" ~region "/" ~service-name "/" ~service-version))
    
(defn- current-load
  []
  (. (java.lang.management.ManagementFactory/getOperatingSystemMXBean) getSystemLoadAverage))

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
            (zk/set-data client server-node (.getBytes (str "1\n" c-load "\n") "UTF-8") data-version))))
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

(defn- full-children
  [client node]
  (let [children (zk/children client node)]
    (if children
      (map (fn [child] (str (if (= node "/") "" node) "/" child)) children)
      nil)))

(defn- r-z
  [client node-list]
  (loop [nodes node-list
         accum '()]
    (if (or (not (first nodes)) (not (zk/exists client (first nodes))))
      accum
      (let [r (rest nodes)
            node (first nodes)
            children (full-children client node)
            n-r (if children (flatten (cons children r)) r)
            n-accum (cons node accum)]
        (recur n-r n-accum)))))

(defn logout
  [session]
  (zk/close (:client @session)))


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


(defn client-login
  [keepers env app]
  (let [client (zk/connect keepers)
        app-base (app-in-env app env)
        session (hash-map :client client :app-base app-base :serv-to-load (hash-map))]
    (zk/create-all client (client-instance-base app env) :persistent? true)
    (zk/create client (str (client-instance-base app env) "/instance-")
               :sequential? true)
    (ref session)))

(defn- add-serv-to-load
  [session-ref server-path load]
  (dosync
        (let [session @session-ref s-t-l-map (:serv-to-load session)]
	(assoc session :serv-to-load (assoc s-t-l-map server-path load)))))

(defn- get-load
  [session server-path]
  (let [client (:client session)
	data (zk/data client server-path)
	data-s (String. (:data data) "UTF-8")
	instance (second (clojure.string/split-lines data-s))
	inst-data (zk/data client instance)
	load (second (clojure.string/split-lines (String. (:data inst-data) "UTF-8")))]
	(read-string load)))

(defn- get-server-instance
  [session service-path]
  (let [client (:client session)
	data (zk/data client service-path)
	instance (second (clojure.string/split-lines
		(String. (:data data) "UTF-8")))]
	instance))

(defn- get-service-load
  [session service-path]
  (let [instance (get-server-instance session service-path)]
	(get-load session instance)))

(defn my-union
  [& args]
  (let [cnt (count args)]
	(if (= 0 cnt)
		(hash-set)
		(if (= cnt 1)
			(hash-set (first args))
			(clojure.set/union
				 (first args) (hash-set (second args)))))))

(defn- regions
  [session]
  (let [client (:client session)
        app-base (:app-base session)]
    (zk/children client (str app-base "/services"))))

(defn- matching-entry
   [pattern entry]
   (re-matches pattern entry))

(defn- matching-entries
  [pattern-str entries]
  (filter (fn [e] (matching-entry  (re-pattern pattern-str) e)) entries))

(defn- lookup-matching-regions
   [session region-regexps]
   (let [regs (regions session)]
     (if regs
       (map (fn [pattern] (matching-entries pattern regs) ) region-regexps)
       '())))

(defn- my-children
  [client my-node]
  (if (zk/exists client my-node)
    (let [my-offspring (zk/children client my-node)]
      (if (and my-offspring (not (= '() my-offspring)))
        (map (fn [child] (str my-node "/" child)) my-offspring)
        nil))
    nil))

(defn- our-children
  [client nodes]
  (filter (fn [x] (if x true false)) (flatten (map (fn [parent] (my-children client parent)) nodes))))

(defn- lookup-matching-services-in-region
  [session region service-name service-version-major]
  (let [client (:client session)
        app-base (:app-base session)
        service-base-major (str (client-service-lu-base app-base
                                                        region
                                                        service-name
                                                        service-version-major))
        services-minor (my-children client service-base-major)]
    (if services-minor
      (let [services-micro (our-children client services-minor)]
        (if services-micro
          (our-children client services-micro)
          `()))
      `())))

(defn- lookup-matching-services-in-regions
  [session regions service-name service-version-major]
  (zipmap regions (map (fn [region]
                         (let [ lu (lookup-matching-services-in-region
                                    session
                                    region
                                    service-name
                                    service-version-major)]
                           lu))
                       regions)))


(defn get-servers
  "for a list of services, return set of servers"
  [session services]
  (distinct (map (fn [service]
		(get-server-instance session service))
		services)))

(defn- get-services
  [session regions service-name service-version-major]
  (map (fn [regs]
		(lookup-matching-services-in-regions
			session
			regs
			service-name
			service-version-major))
	regions))

(defn lookup-service
  "look up a set of service providers for one service"
  [session-ref region-regexps-list service-name service-version-major]
  (let [session @session-ref
	regions (lookup-matching-regions session region-regexps-list)
        ;; services is a 'region -> services-list' map
	services (get-services session regions service-name service-version-major)
	server-instances (hash-set)]
	
	(println "+++++++++++++++++++++++")
	(println (count services))
	(doseq [regional services]
		(println "-----------------------")
		(println regional)
		(doseq [key (keys regional)]
			(println "      -----------------")
			(println key)
			(println (regional key))))
	(println "=======================")
	(println services)

))
   
;; (def ss (slogins `("DC1" "DC2")))
;; (def c (client-login "localhost" "PROD" "RSI"))
;; (doseq [s ss] (doseq [minor `(0 1 2)] (register-service s "mySpecialService" "1" minor "5" "http://localhost/mySpecialService")))
;; (lookup-matching-services-in-regions @c `("DC1") "mySpecialService" "1")
;; (lookup-service @c `("DC.*" ".*1$" ".*2$") "mySpecialService" "1")
;;  (r-z (:client @c) '("/services"))



(defn- slogins
  [regions]
  (map (fn [region] (server-login "localhost" "PROD" "RSI" region)) regions))
