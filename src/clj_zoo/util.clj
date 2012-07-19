(ns clj-zoo.util
  (:import (com.netflix.curator.framework CuratorFrameworkFactory
                                          CuratorFramework)
           (com.netflix.curator.retry RetryNTimes))
  (:require [org.clojars.pgdad.zookeeper :as zk]))

(defn create-non-existing-path
  [fWork path]
  (if-not (-> fWork .checkExists (.forPath path))
    (-> fWork .create .creatingParentsIfNeeded (.forPath path))))

(defn exists?
  [fWork path]
  (-> fWork .checkExists (.forPath path)))

(defn delete-path
  [fWork path]
  (when (exists? fWork path) (-> fWork .delete (.forPath path))))

(defn service-node-path
  [fWork passive region name id]
  (str (if passive "/passivatedservices/" "/services/") region "/" name "/" id))