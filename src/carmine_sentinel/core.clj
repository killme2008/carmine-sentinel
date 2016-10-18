(ns carmine-sentinel.core
  (:require [taoensso.carmine :as car]
            [taoensso.carmine.commands :as cmds]
            [taoensso.carmine.locks :as locks]))

;; {Sentinel group -> master-name -> spec}
(defonce ^:private sentinel-resolved-specs (atom nil))
;; {Sentinel group -> specs}
(defonce ^:private sentinel-groups (atom nil))
;; Sentinel event listeners
(defonce ^:private sentinel-listeners (atom nil))
;; Carmine-sentinel event listeners
(defonce ^:private event-listeners (atom []))
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
  {
   :summary "get master address by master name.",
   :complexity "O(1)",
   :arguments [{:name "name",
                :type "string"}]})

(cmds/defcommand "SENTINEL slaves"
  {
   :summary "get slaves address by master name.",
   :complexity "O(1)",
   :arguments [{:name "name",
                :type "string"}]})


(cmds/defcommand "SENTINEL sentinels"
  {
   :summary "get sentinel instances by mater name.",
   :complexity "O(1)",
   :arguments [{:name "name",
                :type "string"}]})

(defn- make-sure-role
  "Make sure the spec is a master role."
  [spec]
  (when-not (=
             "master"
             (first (car/wcar {:spec spec}
                              (car/role))))
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

(defn notify-event-listeners [event]
  (doseq [listener @event-listeners]
    (try
      (listener event)
      (catch Exception _))))

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

(defn- subscribe-switch-master! [sg spec]
  (if-let [listener (get @sentinel-listeners spec)]
    (deref listener)
    (do
      (swap! sentinel-listeners assoc spec
             (delay
              (car/with-new-pubsub-listener (dissoc spec :timeout-ms)
                {"+switch-master" (partial handle-switch-master sg)}
                (car/subscribe "+switch-master"))))
      (recur sg spec))))

(defn- try-resolve-master-spec [specs sg master-name]
  (let [sentinel-spec (first specs)]
    (try
      (when-let [[master slaves sinstances]
                 (car/wcar {:spec sentinel-spec} :as-pipeline
                           (sentinel-get-master-addr-by-name master-name)
                           (sentinel-slaves master-name)
                           (sentinel-sentinels master-name))]
        (subscribe-switch-master! sg sentinel-spec)
        (let [master-spec {:host (first master)
                           :port (Integer/valueOf ^String (second master))}
              slaves (->> slaves
                          (map (partial apply hash-map))
                          (map (fn [{:strs [ip port]}]
                                 {:host ip
                                  :port (Integer/valueOf ^String port)})))
              ;;Server returned sentinel specs
              rs-specs (->> sinstances
                            (map (partial apply hash-map))
                            (map (fn [{:strs [ip port]}]
                                   {:host ip
                                    :port (Integer/valueOf ^String port)})))]
          (make-sure-role master-spec)
          (swap! sentinel-resolved-specs assoc-in [sg master-name]
                 {:master master-spec
                  :slaves slaves})
          (notify-event-listeners {:event "get-master-addr-by-name"
                                   :sentinel-group sg
                                   :master-name master-name
                                   :master master
                                   :slaves slaves})
          [master-spec slaves rs-specs]))
      (catch Exception e
        (notify-event-listeners
         {:event "error"
          :sentinel-group sg
          :master-name master-name
          :sentinel-spec sentinel-spec
          :exception e})
        ;;Close the listener
        (try
          (when-let [listener (get @sentinel-listeners sentinel-spec)]
            (car/close-listener @listener)
            (swap! sentinel-listeners dissoc sentinel-spec))
          (catch Exception _))
        nil))))

(defn- choose-spec [mn master slaves prefer-slave? slaves-balancer]
  (when (= :error master)
    (throw (IllegalStateException.
            (str "Specs not found by master name: " mn))))
  (if prefer-slave?
    (slaves-balancer slaves)
    master))

(defn- ask-sentinel-master [sg master-name
                            {:keys [prefer-slave? slaves-balancer]}]
  (if-let [conn (get @sentinel-groups sg)]
    (loop [specs (-> conn :specs)
           tried-specs []]
      (if (seq specs)
        (if-let [[ms sls rs-specs] (try-resolve-master-spec specs sg master-name)]
          (do
            ;;Move the sentinel instance to the first position of sentinel list
            ;;to speedup next time resolving.
            (swap! sentinel-groups assoc-in [sg :specs]
                   (vec
                    (concat specs tried-specs
                            ;;adds server returned new sentinel specs to tail.
                            (remove (apply hash-set (:specs conn))
                                    rs-specs))))
            (choose-spec master-name ms sls prefer-slave? slaves-balancer))
          ;;Try next sentinel
          (recur (next specs)
                 (conj tried-specs (first specs))))
        ;;Tried all sentinel instancs, we don't get any valid specs
        ;;Set a :error mark for this situation.
        (do
          (swap! sentinel-resolved-specs assoc-in [sg master-name]
                 {:master :error
                  :slaves :error})
          (notify-event-listeners {:event "get-master-addr-by-name"
                                   :sentinel-group sg
                                   :master-name master-name
                                   :master :error
                                   :slaves :error})
          (throw (IllegalStateException.
                  (str "Specs not found by master name: " master-name))))))
    (throw (IllegalStateException.
            (str "Missing specs for sentinel group: " sg)))))

;;APIs
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
  (swap! event-listeners conj listener))

(defn unregister-listener!
  "Remove the listener for switching master."
  [listener]
  (swap! event-listeners remove (partial = listener)))

(defn get-sentinel-redis-spec
  "Get redis spec by sentinel-group and master name.
   If it is not resolved, it will query from sentinel and
   cache the result in memory.
   Recommend to call this function at your app startup  to reduce
   resolving cost."
  [sg master-name {:keys [prefer-slave? slaves-balancer]
                   :or {prefer-slave? false
                        slaves-balancer first}
                   :as opts}]
  (when (nil? sg)
    (throw (IllegalStateException. "Missing sentinel-group.")))
  (when (empty? master-name)
    (throw (IllegalStateException. "Missing master-name.")))
  (if-let [ret (get-in @sentinel-resolved-specs [sg master-name])]
    (if-let [s (choose-spec master-name
                            (:master ret)
                            (:slaves ret)
                            prefer-slave?
                            slaves-balancer)]
      s
      (throw (IllegalStateException. (str "Spec not found: "
                                          sg
                                          "/"
                                          master-name
                                          ", "
                                          opts))))
    ;;Synchronized on [sg master-name] lock
    (sync-on sg master-name
             ;;Double checking
             (if (nil? (get-in @sentinel-resolved-specs [sg master-name]))
               (ask-sentinel-master sg master-name opts)
               (get-sentinel-redis-spec sg master-name opts)))))

(defn set-sentinel-groups!
  "Configure sentinel groups, it will replace current conf:
   {:group-name {:specs  [{ :host host
                          :port port
                          :password password
                          :timeout-ms timeout-ms },
                         ...other sentinel instances...]
                 :pool {<opts>}}}
  The conf is a map of sentinel group to connection spec."
  [conf]
  (reset! sentinel-groups conf))

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
  (swap! sentinel-groups merge conf))

(defn remove-sentinel-group!
  "Remove a sentinel group configuration by name."
  [group-name]
  (swap! sentinel-groups dissoc group-name))

(defn remove-last-resolved-spec!
  "Remove last resolved master spec by sentinel group and master name."
  [sg master-name]
  (swap! sentinel-resolved-specs dissoc-in [sg master-name]))

(defn update-conn-spec
  "Cast a carmine-sentinel conn to carmine raw conn spec.
   It will resolve master from sentinel first time,then cache the result in
   memory for reusing."
  [conn]
  (update conn
          :spec
          merge
          (get-sentinel-redis-spec (:sentinel-group conn)
                                   (:master-name conn)
                                   conn)))

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

(comment
  (set-sentinel-groups!
   {:group1
    {:specs [{:host "127.0.0.1" :port 5000} {:host "127.0.0.1" :port 5001} {:host "127.0.0.1" :port 5002}]}})
  (let [server1-conn {:pool {} :spec {} :sentinel-group :group1 :master-name "mymaster"}]
    (println
     (wcar server1-conn
           (car/set "a" 100)
           (car/get "a")))))
