(ns clj-zoo.servicewatcher
  (:require [zookeeper :as zk] [clojure.zip :as zipper]
            [clojure.string] [clojure.set])
  (:use [clj-zoo.util] [clj-zoo.client])
  (:gen-class))


(defn new
  [session]
  (ref (hash-map :session session
	:server-to-services (hash-map) :server-to-load (hash-map))))

(defn add-load-mapping
  [service-watcher server load]
  (dosync
	(let [watcher @service-watcher
		load-map (:server-to-load watcher)]
		(alter service-watcher assoc
			:server-to-load (assoc load-map server load)))))

(defn add-service-mapping
  [service-watcher server service]
  (dosync
	(let [watcher @service-watcher
		service-map (:server-to-services watcher)]
		(alter service-watcher assoc
			:server-to-services (assoc service-map server service)))))

(defn add-service
  [service-watcher service & server-n-load ] 
  (let [session (:session @service-watcher)
	cnt (count server-n-load)]
	(println (str "ADDING SERVICE: " cnt " SESSION: " session))
	(if (= 0 cnt)
		(add-service service-watcher service
			(get-server @session service))
		(if (= 1 cnt)
			(add-service service-watcher service
				(first server-n-load)
				(get-load @session (first server-n-load)))
			(dosync 
				(add-load-mapping service-watcher
					(first server-n-load)
					(second server-n-load))
				(add-service-mapping service-watcher
					(first server-n-load)
					service))))))
