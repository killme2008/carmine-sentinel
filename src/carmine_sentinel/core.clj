(ns carmine-sentinel.core
  (:require [taoensso.carmine :as car]
            [taoensso.carmine.commands :as cmds]))

;; sentinel group -> master-name -> spec
(defonce ^:private sentinel-masters (atom nil))
;; sentinel group -> specs
(defonce ^:private sentinel-groups (atom nil))
;; sentinel listeners
(defonce ^:private sentinel-listeners (atom nil))
;; sentinel listeners
(defonce ^:private event-listeners (atom []))

;;define command sentinel-get-master-addr-by-name
(cmds/defcommand "SENTINEL get-master-addr-by-name"
  {
   :summary "get master address by master name.",
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
        (notify-event-listeners {:event :switch-master
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
      (when-let [master (car/wcar {:spec sentinel-spec}
                                  (sentinel-get-master-addr-by-name master-name))]
        (subscribe-switch-master! sg sentinel-spec)
        (let [master-spec {:host (first master)
                           :port (Integer/valueOf ^String (second master))}]
          (make-sure-role master-spec)
          (swap! sentinel-masters assoc-in [sg master-name] master-spec)
          master-spec))
      (catch Exception _
        ;;Close the listener
        (try
          (when-let [listener (get @sentinel-listeners sentinel-spec)]
            (car/close-listener @listener)
            (swap! sentinel-listeners dissoc sentinel-spec))
          (catch Exception _))
        nil))))

(defn- ask-sentinel-master [sg master-name]
  (if-let [conn (get @sentinel-groups sg)]
    (loop [specs (-> conn :specs)]
      (if (seq specs)
        (if-let [ms (try-resolve-master-spec specs sg master-name)]
          ms
          ;;Try next sentinel
          (recur (next specs)))
        (throw (IllegalStateException. (str "Master spec not found by name: " master-name)))))
    (throw (IllegalStateException. (str "Missing specs for sentinel group: " sg)))))

;;APIs
(defn register-listener! [listener]
  (swap! event-listeners conj listener))

(defn unregister-listener! [listener]
  (swap! event-listeners remove (partial = listener)))

(defn get-sentinel-master-spec
  "Get redis spec by sentinel-group and master name.
   If it is not resolved, it will query from sentinel and
   cache the result in memory."
  [sg master-name]
  (when (nil? sg)
    (throw (IllegalStateException. "Missing sentinel-group.")))
  (when (empty? master-name)
    (throw (IllegalStateException. "Missing master-name.")))
  (if-let [spec (get-in @sentinel-masters [sg master-name])]
    spec
    (ask-sentinel-master sg master-name)))

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

(defmacro wcar
  "It's the same as taoensso.carmine/wcar, but supports
      :master-name \"mymaster\"
      :sentinel-group :default
   in conn for redis sentinel cluster.
  "
  {:arglists '([conn :as-pipeline & body] [conn & body])}
  [conn & sigs]
  `(car/wcar
    (update ~conn
            :spec
            merge
            (->> ~conn
                 :master-name
                 (get-sentinel-master-spec (:sentinel-group ~conn))))
    ~@sigs))
