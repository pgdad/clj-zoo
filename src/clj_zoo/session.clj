(ns clj-zoo.session
  (:require [org.clojars.pgdad.zookeeper :as zk]))

(defn login
  [keepers]
  (zk/connect keepers))

(defn logout
  [session]
  (zk/close session))