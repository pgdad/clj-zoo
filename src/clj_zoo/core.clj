(ns clj-zoo.core
  (:require [zookeeper :as zk] [clojure.zip :as zipper]
            [clojure.string] [clojure.set])
  (:import [java.lang.Thread] [java.lang.management.ManagementFactory])
  (:use [clj-zoo.util] [clj-zoo.server])
  (:gen-class))

                                        ; session is map with keys:
                                        ; client, representing zk client
                                        ; envs, list of regexp used to match env part of the path
                                        ; regions, list of regexp used to match region part of the path
                                        ; 







;; (def ss (slogins `("DC1" "DC2")))
;; (def c (client-login "localhost" "PROD" "RSI"))
;; (doseq [s ss] (doseq [minor `(0 1 2)] (register-service s "mySpecialService" "1" minor "5" "http://localhost/mySpecialService")))
;; (lookup-matching-services-in-regions @c `("DC1") "mySpecialService" "1")
;; (lookup-service c `("DC.*" ".*1$" ".*2$") "mySpecialService" "1")
;;  (r-z (:client @c) '("/services"))



(defn- slogins
  [regions]
  (map (fn [region] (server-login "localhost" "PROD" "RSI" region)) regions))
