(ns clj-zoo.serviceNode
  (:require [clj-zoo.serverNode :as serverNode]
            [org.clojars.pgdad.zookeeper :as zk])
  (:import (com.netflix.curator.framework CuratorFramework)))

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


