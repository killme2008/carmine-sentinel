(ns carmine-sentinel.core
  (:require [taoensso.carmine :as car]
            [taoensso.carmine.commands :as cmds])
  (:import (java.io EOFException)))

;; {Sentinel group -> master-name -> spec}
(defonce ^:private sentinel-resolved-specs (atom nil))
;; {Sentinel group -> specs}
(defonce ^:private sentinel-groups (volatile! nil))
;; Sentinel event listeners
(defonce ^:private sentinel-listeners (atom nil))
;; Carmine-sentinel event listeners
(defonce ^:private event-listeners (volatile! []))
;; Locks for resolving spec
(defonce ^:private locks (atom nil))

(defn- get-lock [sg mn]
  (if-let [lock (get @locks (str sg "/" mn))]
    lock
    (let [lock (Object.)
          curr @locks]
      (if (compare-and-set! locks curr (assoc curr (str sg "/" mn) lock))
        lock
        (recur sg mn)))))

(defmacro sync-on [sg mn & body]
  `(locking (get-lock ~sg ~mn)
     ~@body))

;;define commands for sentinel
(cmds/defcommand "SENTINEL get-master-addr-by-name"
  {:fn-name         "sentinel-get-master-addr-by-name"
   :fn-params-fixed [name]
   :fn-params-more  nil
   :req-args-fixed  ["SENTINEL" "get-master-addr-by-name" name]
   :cluster-key-idx 2
   :fn-docstring    "get master address by master name. complexity O(1)"})

(cmds/defcommand "SENTINEL slaves"
  {:fn-name         "sentinel-slaves"
   :fn-params-fixed [name]
   :fn-params-more  nil
   :req-args-fixed  ["SENTINEL" "slaves" name]
   :cluster-key-idx 2
   :fn-docstring    "get slaves address by master name. complexity O(1)"})

(cmds/defcommand "SENTINEL sentinels"
  {:fn-name         "sentinel-sentinels"
   :fn-params-fixed [name]
   :fn-params-more  nil
   :req-args-fixed  ["SENTINEL" "sentinels" name]
   :cluster-key-idx 2
   :fn-docstring    "get sentinel instances by mater name. complexity O(1)"})

(defn- master-role? [spec]
  (= "master"
     (first (car/wcar {:spec spec}
                      (car/role)))))

(defn- make-sure-master-role
  "Make sure the spec is a master role."
  [spec]
  (when-not (master-role? spec)
    (throw (IllegalStateException.
            (format "Spec %s is not master role." spec)))))

(defn- dissoc-in
  [m [k & ks :as keys]]
  (if ks
    (if-let [nextmap (get m k)]
      (let [newmap (dissoc-in nextmap ks)]
        (if (seq newmap)
          (assoc m k newmap)
          (dissoc m k)))
      m)
    (dissoc m k)))

(defmacro silently [& body]
  `(try ~@body (catch Exception _#)))

(defn notify-event-listeners [event]
  (doseq [listener @event-listeners]
    (silently (listener event))))

(defn- handle-switch-master [sg msg]
  (when (= "message" (first msg))
    (let [[master-name old-ip old-port new-ip new-port]
          (clojure.string/split (-> msg nnext first)  #" ")]
      (when master-name
        ;;remove last resolved spec
        (swap! sentinel-resolved-specs dissoc-in [sg master-name])
        (notify-event-listeners {:event "+switch-master"
                                 :old {:host old-ip
                                       :port (Integer/valueOf ^String old-port)}
                                 :new {:host new-ip
                                       :port (Integer/valueOf ^String new-port)}})))))

(defrecord SentinelListener [internal-pubsub-listener stopped-mark]
  java.io.Closeable
  (close [_]
    (reset! stopped-mark true)
    (some->> @internal-pubsub-listener (car/close-listener))))

(defn- subscribe-switch-master! [sg spec]
  (if-let [^SentinelListener sentinel-listener (get @sentinel-listeners spec)]
    (deref (.internal-pubsub-listener sentinel-listener))
    (do
      (let [stop? (atom false)
            listener (atom nil)]
        (swap! sentinel-listeners assoc spec (SentinelListener. listener stop?))
        (future
          (while (not @stop?)
            (silently
              ;; It's unusual to use timeout in redis pub/sub but due to Carmine does not
              ;; support ping/pong test for a connection waiting for an event publishing
              ;; from redis, we do need this to maintain liveness in case redis server
              ;; crash unintentionally. Ref. https://github.com/antirez/redis/issues/420
              (let [spec-with-timeout (update spec :timeout-ms #(or % 10000))
                    f (->> (car/with-new-pubsub-listener spec-with-timeout
                             {"+switch-master" (partial handle-switch-master sg)}
                             (car/subscribe "+switch-master"))
                           (reset! listener)
                           :future)]
                (when (not @stop?)
                  (deref f))))
            (silently (Thread/sleep 1000)))))
      (recur sg spec))))

(defn- unsubscribe-switch-master! [sentinel-spec]
  (silently
    (when-let [^SentinelListener sentinel-listener (get @sentinel-listeners sentinel-spec)]
      (.close sentinel-listener)
      (swap! sentinel-listeners dissoc sentinel-spec)
      true)))

(defn- pick-specs-from-sentinel-raw-states [raw-states]
  (map
   (fn [{:strs [ip port]}]
     {:host ip, :port (Integer/valueOf port)})
   (map (partial apply hash-map) raw-states)))

(defn- subscribe-all-sentinels [sentinel-group master-name]
  (when-let [old-sentinel-specs (not-empty (get-in @sentinel-groups [sentinel-group :specs]))]
    (let [valid-specs
          (set ;; remove duplicate sentinel spec
           (flatten
            (mapv
             (fn [spec]
               (try
                 (conj
                  (map (fn [new-raw-spec] (merge spec new-raw-spec))
                       (pick-specs-from-sentinel-raw-states
                        (car/wcar {:spec spec} (sentinel-sentinels master-name))))
                  spec)
                 (catch Exception _ [])))
             old-sentinel-specs)))
          invalid-specs (remove valid-specs old-sentinel-specs)
          ;; still keeping the invalid specs but append them to tail then
          ;; convert spec list to vector to take advantage of their order later
          all-specs (vec (concat valid-specs invalid-specs))]

      (doseq [spec valid-specs]
        (subscribe-switch-master! sentinel-group spec))

      (vswap! sentinel-groups assoc-in [sentinel-group :specs] all-specs)

      (not-empty all-specs))))

(defn- try-resolve-master-spec [server-conn specs sentinel-group master-name]
  (let [sentinel-spec (first specs)]
    (try
      (when-let [[master slaves]
                 (car/wcar {:spec sentinel-spec} :as-pipeline
                           (sentinel-get-master-addr-by-name master-name)
                           (sentinel-slaves master-name))]
        (let [master-spec (merge (:spec server-conn)
                                 {:host (first master)
                                  :port (Integer/valueOf ^String (second master))})
              slaves (pick-specs-from-sentinel-raw-states slaves)]
          (make-sure-master-role master-spec)
          (swap! sentinel-resolved-specs assoc-in [sentinel-group master-name]
                 {:master master-spec
                  :slaves slaves})
          (make-sure-master-role master-spec)
          (notify-event-listeners {:event "get-master-addr-by-name"
                                   :sentinel-group sentinel-group
                                   :master-name master-name
                                   :master master
                                   :slaves slaves})
          [master-spec slaves]))
      (catch Exception e
        (swap! sentinel-resolved-specs dissoc-in [sentinel-group master-name])
        (notify-event-listeners
         {:event "error"
          :sentinel-group sentinel-group
          :master-name master-name
          :sentinel-spec sentinel-spec
          :exception e})
        ;;Close the listener
        (unsubscribe-switch-master! sentinel-spec)
        nil))))

(defn- choose-spec [mn master slaves prefer-slave? slaves-balancer]
  (when (= :error master)
    (throw (IllegalStateException.
            (str "Specs not found by master name: " mn))))
  (if (and prefer-slave? (seq slaves))
    (slaves-balancer slaves)
    master))

(defn- ask-sentinel-master [sentinel-group master-name
                            {:keys [prefer-slave? slaves-balancer]
                             :as server-conn}]
  (if-let [all-specs (subscribe-all-sentinels sentinel-group master-name)]
    (loop [specs all-specs
           tried-specs []]
      (if (seq specs)
        (if-let [[master-spec slaves]
                 (try-resolve-master-spec server-conn specs sentinel-group master-name)]
          (do
            ;;Move the sentinel instance to the first position of sentinel list
            ;;to speedup next time resolving.
            (vswap! sentinel-groups assoc-in [sentinel-group :specs]
                    (vec (concat specs tried-specs)))
            (choose-spec master-name master-spec slaves prefer-slave? slaves-balancer))
          ;;Try next sentinel
          (recur (next specs)
                 (conj tried-specs (first specs))))
        ;;Tried all sentinel instancs, we don't get any valid specs
        ;;Set a :error mark for this situation.
        (do
          (swap! sentinel-resolved-specs assoc-in [sentinel-group master-name]
                 {:master :error
                  :slaves :error})
          (notify-event-listeners {:event "get-master-addr-by-name"
                                   :sentinel-group sentinel-group
                                   :master-name master-name
                                   :master :error
                                   :slaves :error})
          (throw (IllegalStateException.
                  (str "Specs not found by master name: " master-name))))))
    (throw (IllegalStateException.
            (str "Missing specs for sentinel group: " sentinel-group)))))

;;APIs
(defn remove-invalid-resolved-master-specs!
  "Iterate all the resolved master specs and remove any invalid
   master spec found by checking role on redis.
   Please call this periodically to keep safe."
  []
  (doseq [[group-id resolved-specs] @sentinel-resolved-specs]
    (doseq [[master-name master-specs] resolved-specs]
      (try
        (when-not (master-role? (:master master-specs))
          (swap! sentinel-resolved-specs dissoc-in [group-id master-name]))
        (catch EOFException _
          (swap! sentinel-resolved-specs dissoc-in [group-id master-name]))))))

(defn register-listener!
  "Register listener for switching master.
  The listener will be called with an event:
    {:event \"+switch-master\"
     :old {:host old-master-ip
           :port old-master-port
     :new {:host new-master-ip
           :port new-master-port}}}
  "
  [listener]
  (vswap! event-listeners conj listener))

(defn unregister-listener!
  "Remove the listener for switching master."
  [listener]
  (vswap! event-listeners remove (partial = listener)))

(defn get-sentinel-redis-spec
  "Get redis spec by sentinel-group and master name.
   If it is not resolved, it will query from sentinel and
   cache the result in memory.
   Recommend to call this function at your app startup  to reduce
   resolving cost."
  [sentinel-group master-name
   {:keys [prefer-slave? slaves-balancer]
    :or {prefer-slave? false
         slaves-balancer first}
    :as server-conn}]
  {:pre [(not (nil? sentinel-group))
         (not (empty? master-name))]}
  (if-let [resolved-spec (get-in @sentinel-resolved-specs [sentinel-group master-name])]
    (if-let [s (choose-spec master-name
                            (:master resolved-spec)
                            (:slaves resolved-spec)
                            prefer-slave?
                            slaves-balancer)]
      s
      (throw (IllegalStateException.
              (str "Spec not found: " sentinel-group "/" master-name ", " server-conn))))
    ;;Synchronized on [sentinel-group master-name] lock
    (sync-on sentinel-group master-name
             ;;Double checking
             (if (nil? (get-in @sentinel-resolved-specs [sentinel-group master-name]))
               (ask-sentinel-master sentinel-group master-name server-conn)
               (get-sentinel-redis-spec sentinel-group master-name server-conn)))))

(defn set-sentinel-groups!
  "Configure sentinel groups, it will replace current conf:
   {:group-name {:specs [{ :host host
                          :port port
                          :password password
                          :timeout-ms timeout-ms },
                         ...other sentinel instances...]
                 :pool {<opts>}}}
  The conf is a map of sentinel group to connection spec."
  [conf]
  (doseq [[_ group-conf] @sentinel-groups]
    (doseq [spec (:specs group-conf)]
      (unsubscribe-switch-master! spec)))
  (vreset! sentinel-groups conf)
  (reset! sentinel-resolved-specs nil))

(defn add-sentinel-groups!
  "Add sentinel groups,it will be merged into current conf:
   {:group-name {:specs  [{ :host host
                          :port port
                          :password password
                          :timeout-ms timeout-ms },
                          ...other sentinel instances...]
                 :pool {<opts>}}}
  The conf is a map of sentinel group to connection spec."
  [conf]
  (vswap! sentinel-groups merge conf))

(defn remove-sentinel-group!
  "Remove a sentinel group configuration by name."
  [group-name]
  (doseq [sentinel-spec (get-in @sentinel-groups [group-name :specs])]
    (unsubscribe-switch-master! sentinel-spec))
  (vswap! sentinel-groups dissoc group-name)
  (swap! sentinel-resolved-specs dissoc group-name))

(defn remove-last-resolved-spec!
  "Remove last resolved master spec by sentinel group and master name."
  [sg master-name]
  (swap! sentinel-resolved-specs dissoc-in [sg master-name]))

(defn update-conn-spec
  "Cast a carmine-sentinel conn to carmine raw conn spec.
   It will resolve master from sentinel first time,then cache the result in
   memory for reusing."
  [server-conn]
  (if (and (:sentinel-group server-conn) (:master-name server-conn))
    (update server-conn
            :spec
            merge
            (get-sentinel-redis-spec (:sentinel-group server-conn)
                                     (:master-name server-conn)
                                     server-conn))
    server-conn))

(defmacro wcar
  "It's the same as taoensso.carmine/wcar, but supports
      :master-name \"mymaster\"
      :sentinel-group :default
   in conn for redis sentinel cluster.
  "
  {:arglists '([conn :as-pipeline & body] [conn & body])}
  [conn & sigs]
  `(car/wcar
    (update-conn-spec ~conn)
    ~@sigs))

(defmacro with-new-pubsub-listener
  "It's the same as taoensso.carmine/with-new-pubsub-listener,
   but supports
    :master-name \"mymaster\"
    :sentinel-group :default
   in conn for redis sentinel cluster.

   Please note that you can only pass connection spec like
   hostname and port to taoensso.carmine/with-new-pubsub-listener
   like:

   (taoensso.carmine/with-new-pubsub-listener
     {:host \"127.0.0.1\" :port 6379}
     {... channel and handler stuff ... }
     ... publish and subscribe stuff ... )

   but for with-new-pubsub-listener in carmine-sentinel, you need
   to wrap connection spec with another layer along with master-name
   and sentinel-group to take advantage of sentinel cluster like:

   (carmine-sentinel/with-new-pubsub-listener
     {:spec {:host \"127.0.0.1\" :port 6379}
      :master-name \"mymaster\"
      :sentinel-group :default}
     {... channel and handler stuff ... }
     ... publish and subscribe stuff ... )
  "
  [conn-spec & others]
  `(car/with-new-pubsub-listener
     (:spec (update-conn-spec ~conn-spec))
     ~@others))

(defn sentinel-group-status
  "Get the status of all the registered sentinel groups and resolved redis cluster specs.

   For example, firstly we set sentinel groups:

   (set-sentinel-groups!
     {:group1 {:specs [{:host \"127.0.0.1\" :port 5000}
                       {:host \"127.0.0.1\" :port 5001}
                       {:host \"127.0.0.1\" :port 5002}]}})

   Then do something to trigger the resolving of the redis cluster specs.

   (let [server1-conn {:pool {} :spec {} :sentinel-group :group1 :master-name \"mymaster\"}]
     (wcar server1-conn
       (car/set \"a\" 100)))

   At last we execute (sentinel-group-status), then got things like:

   {:group1 {:redis-clusters [{:master-name \"mymaster\",
                               :master-spec {:host \"127.0.0.1\", :port 6379},
                               :slave-specs ({:host \"127.0.0.1\", :port 6380})}],
             :sentinels [{:host \"127.0.0.1\", :port 5000, :with-active-sentinel-listener? true}
                         {:host \"127.0.0.1\", :port 5001, :with-active-sentinel-listener? true}
                         {:host \"127.0.0.1\", :port 5002, :with-active-sentinel-listener? true}]}}"
  []
  (reduce (fn [cur [group-name sentinel-specs]]
            (assoc cur
              group-name
              {:redis-clusters (->> (get @sentinel-resolved-specs group-name)
                                    (mapv (fn [[master-name specs]]
                                            {:master-name master-name
                                             :master-spec (:master specs)
                                             :slave-specs (:slaves specs)})))
               :sentinels      (->> (:specs sentinel-specs)
                                    (map #(let [^SentinelListener listener (get @sentinel-listeners %)]
                                            (assoc %
                                              :with-active-sentinel-listener?
                                              (and (some? listener)
                                                   (not @(.stopped-mark listener)))))))}))
          {}
          @sentinel-groups))

(comment
  (set-sentinel-groups!
   {:group1
    {:specs [{:host "127.0.0.1" :port 5000} {:host "127.0.0.1" :port 5001} {:host "127.0.0.1" :port 5002}]}})
  (let [server1-conn {:pool {} :spec {} :sentinel-group :group1 :master-name "mymaster"}]
    (println
     (wcar server1-conn
           (car/set "a" 100)
           (car/get "a")))))

(comment

  (do ;; reset environment
    (reset! sentinel-resolved-specs nil)
    (vreset! sentinel-groups nil)
    (reset! sentinel-listeners nil)
    (vreset! event-listeners [])
    (reset! locks nil)

    (let [token "foobar"
          host "127.0.0.1"]

      (def server1-conn
        {:pool {}
         :spec {:password token}
         :sentinel-group :group1
         :master-name "mymaster"})

      (set-sentinel-groups!
       {:group1
        {:specs [{:host host :port 5000 :password token}
                 {:host host :port 5001 :password token}
                 {:host host :port 5002 :password token}]}})))

  (defmacro wcar* [& body] `(wcar server1-conn ~@body))

  (wcar* (car/ping))

  (wcar* (car/set "key" 1))

  (wcar* (car/get "key"))

  )
