(ns carmine-sentinel.core
  (:require [taoensso.carmine :as car]
            [taoensso.carmine.commands :as cmds]
            [taoensso.carmine.locks :as locks]))

;; sentinel group -> master-name -> spec
(defonce ^:private sentinel-masters (atom nil))
;; sentinel group -> specs
(defonce ^:private sentinel-groups (atom nil))
;; sentinel listeners
(defonce ^:private sentinel-listeners (atom nil))
;; sentinel listeners
(defonce ^:private event-listeners (atom []))

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
        (swap! sentinel-masters dissoc-in [sg master-name])
        (notify-event-listeners {:event "+switch-master"
                                 :old {:host old-ip
                                       :port (Integer/valueOf old-port)}
                                 :new {:host new-ip
                                       :port (Integer/valueOf new-port)}})))))

(defn- subscribe-switch-master! [sg spec]
  (if-let [listener (get @sentinel-listeners spec)]
    (deref listener)
    (do
      (swap! sentinel-listeners assoc spec
             (delay
              (car/with-new-pubsub-listener spec
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
          (swap! sentinel-masters assoc-in [sg master-name] {:master master-spec
                                                             :slaves slaves})

          [master-spec slaves rs-specs]))
      (catch Exception _
        ;;Close the listener
        (try
          (when-let [listener (get @sentinel-listeners sentinel-spec)]
            (car/close-listener @listener)
            (swap! sentinel-listeners dissoc sentinel-spec))
          (catch Exception _))
        nil))))

(defn- choose-spec [master slaves prefer-slave? slaves-balancer]
  (if prefer-slave?
    (slaves-balancer slaves)
    master))

(defn- ask-sentinel-master [sg master-name {:keys [prefer-slave? slaves-balancer]}]
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
            (choose-spec ms sls prefer-slave? slaves-balancer))
          ;;Try next sentinel
          (recur (next specs)
                 (conj tried-specs (first specs))))
        (throw (IllegalStateException. (str "Master spec not found by name: " master-name)))))
    (throw (IllegalStateException. (str "Missing specs for sentinel group: " sg)))))

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

(defn get-sentinel-master-spec
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
  (if-let [ret (get-in @sentinel-masters [sg master-name])]
    (if-let [s (choose-spec (:master ret)
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
    (ask-sentinel-master sg master-name opts)))

(defn set-sentinel-groups!
  "Configure sentinel groups:
   { :default {:specs  [{ :host host
                          :port port
                          :password password
                          :timeout-ms timeout-ms },
                        ......]
               :pool {<opts>}}}
  It's a list of sentinel instance process spec:
  ."
  [conf]
  (reset! sentinel-groups conf))

(defn remove-last-resolved-spec!
  "Remove last resolved master spec by sentinel group and master name."
  [sg master-name]
  (swap! sentinel-masters dissoc-in [sg master-name]))

(defn update-conn-spec
  "Cast a carmine-sentinel conn to carmine raw conn spec.
   It will resolve master from sentinel first time,then cache the result in
   memory for reusing."
  [conn]
  (update conn
          :spec
          merge
          (get-sentinel-master-spec (:sentinel-group conn)
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
