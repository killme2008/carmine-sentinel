(defproject net.fnil/carmine-sentinel "1.0.0"
  :description "A Clojure library designed to connect redis by sentinel, make carmine to support sentinel."
  :url "https://github.com/killme2008/carmine-sentinel"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.10.0"]
                 [com.taoensso/carmine "2.19.1"]
                 [cheshire "5.8.1"]
                 [clj-http "3.9.1"]]
  :plugins [[codox "0.6.8"]]
  :warn-on-reflection true)
