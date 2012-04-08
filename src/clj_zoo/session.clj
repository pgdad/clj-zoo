(ns clj-zoo.session
  (:require [zookeeper :as zk]))

(defn login
  [keepers]
  (zk/connect keepers))

(defn logout
  [session]
  (zk/close session))