(ns clj-zoo.client
  (:require [zookeeper :as zk] [clojure.zip :as zipper]
            [clojure.string] [clojure.set])
  (:use [clj-zoo.util])
  (:gen-class))

(defmacro client-instance-base
  "<app-in-env>/clients"
  [app env]
  `(str (app-in-env ~app ~env) "/clients"))

(defmacro client-service-lu-base
  [app-base region service-name service-version]
  `(str ~app-base "/services/" ~region "/" ~service-name "/" ~service-version))

(defn client-login
  [keepers env app region-regexps]
  (let [client (zk/connect keepers)
        app-base (app-in-env app env)
        session (hash-map :client client
                          :app-base app-base
                          :region-regexps region-regexps)]
    (zk/create-all client (client-instance-base app env) :persistent? true)
    (zk/create client (str (client-instance-base app env) "/instance-")
               :sequential? true)
    (ref session)))

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

(defn my-union
  [& args]
  (let [cnt (count args)]
    (if (= 0 cnt)
      (hash-set)
      (if (= cnt 1)
        (first args)
        (clojure.set/union
         (first args) (hash-set (second args)))))))

(defn get-server
  [session service-path]
  (let [client (:client session)
	data (zk/data client service-path)
	instance (second (clojure.string/split-lines
                          (String. (:data data) "UTF-8")))]
    instance))

(defn get-load
  [session server-path]
  (let [client (:client session)
	data (zk/data client server-path)
	data-s (String. (:data data) "UTF-8")
	load (second (clojure.string/split-lines data-s))]
    (read-string load)))

(defn get-service-load
  [session service-path]
  (let [instance (get-server session service-path)]
    (get-load session instance)))

(defn get-servers
  "for a list of services, return set of servers"
  [session services]
  (distinct (map (fn [service]
                   (get-server session service))
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

(defn- regional-services
  "( <map 'region' -> 'services' ) => ( 'services from all regions' )"
  [services-map]
  (flatten (map (fn [key] (services-map key)) (keys services-map))))

(defn- combined-regional-services
  " SEQ( <map 'region' -> 'services' ) -> (lots of services' )"
  [all-services]
  (distinct (flatten (map (fn [regional] (regional-services regional)) all-services))))

;; (defn- services-for-all-regions
;;   "( maps of region -> services )"
;;   [reg-of-servs-maps]

(defn lookup-service
  "look up a set of service providers for one service"
  [session-ref service-name service-version-major]
  (let [session @session-ref
	region-regexps (:region-regexps session)
	regions (lookup-matching-regions session region-regexps)
        ;; services is a 'region -> services-list' map
	services (get-services session regions service-name service-version-major)
	server-instances (hash-set)
        combined-services (combined-regional-services services)
        service-loads (map (fn [service-path]
                             (get-service-load session service-path))
                           combined-services)
	]
    
    (zipmap combined-services service-loads)
  ))
