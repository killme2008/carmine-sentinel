# carmine-sentinel

A Clojure library designed to connect redis by [sentinel](redis.io/topics/sentinel), make [carmine](https://github.com/ptaoussanis/carmine) to support [sentinel](redis.io/topics/sentinel)。

## Usage

```clojure
[net.fnil/carmine-sentinel "0.2.0"]
```

**Carmine-sentinel require carmine version must be `2.14.0`right now.**

First, require carmine and carmine-sentinel:

```clojure
(ns my-app
  (:require [taoensso.carmine :as car]
            [carmine-sentinel.core :as cs :refer [set-sentinel-groups!]]))
```

The only difference compares with carmine is that we will use `carmine-sentinel.core/wcar` to replace `taoensso.carmine/wcar` and add a new function `set-sentinel-groups!`.

Second, configure sentinel groups:

```clojure
(set-sentinel-groups!
  {:group1
   {:specs [{:host "127.0.0.1" :port 5000} {:host "127.0.0.1" :port 5001} {:host "127.0.0.1" :port 5002}]
    :pool  {<opts>} }})
```

There is only one group named `:group1` above, and it has three sentinel instances (port from 5000 to 5002 at 127.0.0.1). Optional, you can set the pool option values and add more sentinel groups.

You can use `add-sentinel-groups!` and `remove-sentinel-group!` to manage the configuration all the time.

Next, we can define the `wcar*`:

```clojure
(def server1-conn {:pool {<opts>} :spec {} :sentinel-group :group1 :master-name "mymaster"})
(defmacro wcar* [& body] `(cs/wcar server1-conn ~@body))
```

The spec in `server1-conn` is empty, and there are two new options in server1-conn:

* `:sentinel-group` Which sentinel instances group to resolve master addr.Here is `:group1`.
* `:master-name` Master name configured in that sentinel group.Here is `mymaster`.

The `spec` in server1-conn will be merged to resolved master spec at runtime.So you can set `:password`,`:timeout-ms` etc. other options in it.

Also, you can define many `wcar*`-like macros to use other sentinel group and master name.

At last, you can use `wcar*` as the same in carmine.

```clojure
(wcar* (car/set "key" 1))
(wcar* (car/get "key"))
```

If you want to bypass sentinel and connect to redis server directly such as doing testing on your local machine, you can ignore `sentinel-group` and `master-name`, just provide redis server connection spec you want connect to directly like this:

```clojure
(def server1-conn {:pool {<opts>} :spec {:host "127.0.0.1" :port 6379}})
(defmacro wcar* [& body] `(cs/wcar server1-conn ~@body))
```

## Pub/Sub

Please use `carmine-sentinel.core/with-new-pubsub-listener` to replace `taoensso.carmine/with-new-pubsub-listener` and provide `master-name`, `sentinel-group` to take advantage of sentinel cluster like this:

```clojure
(def server1-conn {:sentinel-group :group1 :master-name "mymaster"})

;;Pub/Sub
(def listener
  (with-new-pubsub-listener server1-conn
    {"foobar" (fn f1 [msg] (println "Channel match: " msg))
     "foo*"   (fn f2 [msg] (println "Pattern match: " msg))}
   (car/subscribe  "foobar" "foobaz")
   (car/psubscribe "foo*")))
```

`carmine-sentinel.core/with-new-pubsub-listener` also support bypass sentinel and connect to redis server directly. You just need to provide the redis server spec you want connect to while ignore `sentinel-group` and `master-name`:

```clojure
(def server1-conn {:spec {:host "127.0.0.1" :port 6379}})

;;Pub/Sub
(def listener
  (with-new-pubsub-listener server1-conn
    {"foobar" (fn f1 [msg] (println "Channel match: " msg))
     "foo*"   (fn f2 [msg] (println "Pattern match: " msg))}
   (car/subscribe  "foobar" "foobaz")
   (car/psubscribe "foo*")))
```

## MessageQueue and Lock

You have to invoke `update-conn-spec` before using other APIs in carmine:

```clojure
(def server1-conn {:pool {<opts>} :spec {} :sentinel-group :group1 :master-name "mymaster"})


;;Message queue
(def my-worker
  (car-mq/worker (cs/update-conn-spec server1-conn) "my-queue"
   {:handler (fn [{:keys [message attempt]}]
               (println "Received" message)
               {:status :success})}))


;;;Lock
(locks/with-lock (cs/update-conn-spec server1-conn) "my-lock"
  1000 ; Time to hold lock
  500  ; Time to wait (block) for lock acquisition
  (println "This was printed under lock!"))
```

## Reading From Slaves

If you want to read data from slave, you can set `prefer-slave?` to be true:

```clojure
(def slave-conn {:pool {<opts>} :spec {}
                 :sentinel-group :group1 :master-name "mymaster"
                 :prefer-slave? true})

(defmacro wcars* [& body] `(cs/wcar slave-conn ~@body))

(wcars* (car/set "key" 1)) ;; ExceptionInfo READONLY You can't write against a read only slave
```

If you have many slaves for one master, the default balancer is `first` function, but you can custom it by `slaves-balancer`,
for example, using random strategy:

```clojure
(def slave-conn {:pool {<opts>} :spec {}
                 :sentinel-group :group1
                 :master-name "mymaster"
                 :prefer-slave? true
                 :slaves-balancer rand-nth)
```

## Listen on carmine-sentinel events

You can register a listener to listen carmine-sentinel events such as `error`, `get-master-addr-by-name`
and `+switch-master` etc. :

```clojure
(cs/register-listener! (fn [e] (println "Event " e " happens")))
```

## Failover

At startup, carmine-sentinel will connect the first sentinel instance to resolve the master address, if if fails, carmine-sentinel will try the next sentinel until find a resolved master address or throw an exception.The resolved addr will be cached in memory.

And Carmine-sentinel subcribes `+switch-master` channel in sentinel.When the master redis instance is down, sentinel will publish a `+switch-master` message, while carmine-sentinel receives this message, it will clean the last cached result and try to connect the new redis master at once.

At last, carmine-sentinel will refresh the sentinel instance list by the response from command `SENTINEL sentinels [name]`.

## API docs

* [Carmine-sentinel APIs](http://fnil.net/docs/carmine_sentinel/)

## License

Copyright © 2016 killme2008

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
