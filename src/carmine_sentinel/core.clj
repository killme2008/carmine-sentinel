(ns carmine-sentinel.core
  (:require [taoensso.carmine :as car]
            [taoensso.carmine.commands :as cmds]))

;; sentinel group -> master-name -> spec
(defonce sentinel-masters (atom nil))
;; sentinel group -> specs
(defonce sentinel-groups (atom nil))

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

(defn- ask-sentinel-master [sg master-name]
  (if-let [conn (get @sentinel-groups sg)]
    (if-let [master (car/wcar {:spec (-> conn :specs rand-nth)}
                              (sentinel-get-master-addr-by-name master-name))]
      (let [spec {:host (first master)
                  :port (Integer/valueOf ^String (second master))}]
        (make-sure-role spec)
        (swap! sentinel-masters assoc-in [sg master-name] spec)
        spec)
      (throw (IllegalStateException. (str "Master addr not found by name: " master-name))))
    (throw (IllegalStateException. (str "Missing specs for sentinel group: " sg)))))

(defn get-sentinel-master-spec [sg master-name]
  (when (nil? sg)
    (throw (IllegalStateException. "Missing sentinel-group.")))
  (when (empty? master-name)
    (throw (IllegalStateException. "Missing master-name.")))
  (if-let [spec (get-in @sentinel-masters [sg master-name])]
    spec
    (ask-sentinel-master sg master-name)))

(defn set-sentinel-groups!
  "Configure sentinel specs:
   {:specs  [{ :host host
               :port port
               :password password
               :timeout-ms timeout-ms },
              ......]
    :pool {<opts>}}
  It's a list of sentinel instance process spec:
  ."
  [conf]
  (reset! sentinel-groups conf))

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
