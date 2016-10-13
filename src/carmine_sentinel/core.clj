(ns carmine-sentinel.core
  (:require [taoensso.carmine :as car]
            [taoensso.carmine.commands :as cmds]))

(defonce sentinel-masters (atom nil))

(defonce sentinels-conn (atom nil))

;;define command sentinel-get-master-addr-by-name
(cmds/defcommand "SENTINEL get-master-addr-by-name"
  {
   :summary "get master address by master name.",
   :complexity "O(1)",
   :arguments [{:name "name",
                :type "string"}]})

(defn- ask-sentinel-master [master-name]
  (if-let [conn @sentinels-conn]
    (if-let [master (car/wcar {:spec (-> conn :specs rand-nth)}
                              (sentinel-get-master-addr-by-name master-name))]
      (let [spec {:host (first master)
                  :port (Integer/valueOf ^String (second master))}]
        (swap! sentinels-conn assoc master-name spec)
        spec)
      (throw (IllegalStateException. (str "Master addr not found by name: " master-name))))
    (throw (IllegalStateException. "Please calling `set-sentinel-conn!` at first."))))

(defn get-sentinel-master-spec [master-name]
  (when (empty? master-name)
    (throw (IllegalArgumentException. "Missing master-name.")))
  (if-let [spec (get @sentinel-masters master-name)]
    spec
    (ask-sentinel-master master-name)))

(defn set-sentinel-conn!
  "Configure sentinel specs:
   {:specs  [{ :host host
               :port port
               :password password
               :timeout-ms timeout-ms },
              ......]
    :pool {<opts>}}
  It's a list of sentinel instance process spec:
  ."
  [conn]
  (if (empty? (:specs conn))
    (throw (IllegalArgumentException. "Empty sentinel specs."))
    (reset! sentinels-conn conn)))

(defmacro wcar
  "It's the same as taoensso.carmine/wcar, but supports
      :sentinel-specs [{:host host
                        :port port
                        :password password
                        :timeout-ms timeout-ms}
                        ...
                      ]
   in conn.
  "
  {:arglists '([conn :as-pipeline & body] [conn & body])}
  [conn & sigs]
  `(car/wcar
    (update ~conn
            :spec
            merge
            (-> ~conn
                :master-name
                (get-sentinel-master-spec)))
    ~@sigs))
