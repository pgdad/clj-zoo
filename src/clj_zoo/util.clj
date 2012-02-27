(ns clj-zoo.util
  (:require [zookeeper :as zk] [clojure.zip :as zipper]
            [clojure.string] [clojure.set])
  (:import [java.lang.Thread] [java.lang.management.ManagementFactory])
  (:gen-class))

(def ^:dynamic *service-root* "/services")

(defmacro dbg[x] `(let [x# ~x] (println "dbg:" '~x "=" x#) x#))

(defmacro app-in-env
  "/*service-root*/<env>/<app>"
  [app env]
  `(str *service-root* "/" ~env "/" ~app))

(defn logout
  [session]
  (zk/close (:client @session)))

(defn- full-children
  [client node]
  (let [children (zk/children client node)]
    (if children
      (map (fn [child] (str (if (= node "/") "" node) "/" child)) children)
      nil)))

(defn r-z
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


