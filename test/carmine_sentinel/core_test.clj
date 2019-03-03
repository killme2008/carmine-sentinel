(ns carmine-sentinel.core-test
  (:require [clojure.test :refer :all]
            [taoensso.carmine :as car]
            [carmine-sentinel.core :refer :all]))


;;; NOTE:
;;; add the following to `redis.conf`:
;;; requirepass foobar

;;; NOTE:
;;; SENTINEL CONF `PORT` SHOULD BE DIFFERENT FOR EACH SENTINEL
;;; MAKE SURE TO REMOVE `myid` OR HARDCODE DIFFERENT IDS FOR EACH SENTINEL
;;; NOTE:
;;; Use different files for each sentinel

;;; > cat sentinel.conf
;;; port 5000
;;; daemonize no
;;; pidfile "/var/run/redis-sentinel.pid"
;;; logfile ""
;;; dir "/tmp"
;;; sentinel deny-scripts-reconfig yes
;;; sentinel monitor mymaster 127.0.0.1 6379 2
;;; protected-mode no
;;; requirepass "foobar"


(def token "foobar")
(def host "127.0.0.1")
(def conn {:pool {}
           :spec {:password token}
           :sentinel-group :group1
           :master-name "mymaster"})

(set-sentinel-groups!
 {:group1
  {:specs [{:host host :port 5000 :password token}
           {:host host :port 5001 :password token}
           {:host host :port 5002 :password token}]}})


(deftest resolve-master-spec
  (testing "Try to resolve the master's spec using the sentinels' specs"
    (is (=
         (let [server-conn     {:pool {},
                                :spec {:password "foobar"},
                                :sentinel-group :group1,
                                :master-name "mymaster"}
               specs           [{:host "127.0.0.1", :port 5002, :password "foobar"}
                                {:host "127.0.0.1", :port 5001, :password "foobar"}
                                {:host "127.0.0.1", :port 5000, :password "foobar"}]
               sentinel-group :group1
               master-name    "mymaster"]
           (@#'carmine-sentinel.core/try-resolve-master-spec
            server-conn specs sentinel-group master-name))
         [{:password "foobar", :host "127.0.0.1", :port 6379} ()])
        )))

(deftest subscribing-all-sentinels
  (testing ""
    (is (=
         (let [sentinel-group :group1
               master-name "mymaster"
               server-conn conn]
           (@#'carmine-sentinel.core/subscribe-all-sentinels
            sentinel-group
            master-name))
         [{:password "foobar", :port 5002, :host "127.0.0.1"}
          {:password "foobar", :port 5001, :host "127.0.0.1"}
          {:password "foobar", :port 5000, :host "127.0.0.1"}]))))

(deftest asking-sentinel-master
  (testing ""
    (is (= (let [sentinel-group :group1
                 master-name "mymaster"
                 server-conn conn]
             (@#'carmine-sentinel.core/ask-sentinel-master sentinel-group
              master-name
              server-conn))
           {:password "foobar", :host "127.0.0.1", :port 6379}))))

(deftest sentinel-redis-spec
  (testing "Trying to get redis spec by sentinel-group and master name"
    (is (= {:password "foobar", :host "127.0.0.1", :port 6379}
           (let [server-conn conn]
             (get-sentinel-redis-spec (:sentinel-group server-conn)
                                      (:master-name server-conn)
                                      server-conn))))))

(try
  (defmacro test-wcar* [& body] `(wcar conn ~@body))
  (catch Exception e
    (println "WARNING: caught exception while defining wcar*,"
             "can occur when re-running tests in the same repl."
             "Please verify and check if it isn't the cause of your tests' failure:"
             e)))


(deftest ping
  (testing "Checking if ping works."
    (is (= "PONG" (test-wcar* (car/ping))))))

