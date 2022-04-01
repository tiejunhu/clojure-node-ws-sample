(ns server

  (:require
   [cljs.core.async :as async :refer (<!)]
   [hiccups.runtime]

   ;; sente
   [taoensso.sente :as sente]
   [taoensso.timbre :as timbre :refer-macros (debugf infof)]
   [taoensso.sente.server-adapters.macchiato :as sente-macchiato]
   ;; bidi
   [bidi.bidi :as bidi]
   ;; macchiato
   [macchiato.server :as m-server]
   [macchiato.middleware.defaults :as m-defaults]
   [macchiato.auth.backends.session :as m-session]
   [macchiato.auth.middleware :as m-auth]
   [macchiato.middleware.resource :as m-resource]
   [macchiato.middleware.anti-forgery :as csrf]
   [macchiato.util.response :as m-resp])
  (:require-macros
   [hiccups.core :as hiccups]
   [cljs.core.async.macros :as asyncm :refer (go-loop)]))

(defn not-found [ring-req]
  (-> (hiccups/html
       [:html
        [:body
         [:h2 (:uri ring-req) " was not found"]]])
      (m-resp/not-found)
      (m-resp/content-type "text/html")))

(defn landing-pg-handler
  "index页面处理，使用hiccups语法生成html在页面中嵌入csrf token"
  []
  (debugf "Landing page handler")
  (let [af-token csrf/*anti-forgery-token*]
    (-> [:html
         [:body
          [:div
           {:id  "__anti-forgery-token"
            :data-csrf-token af-token}]
          [:h1 "Sente reference example"]
          [:p "An Ajax/WebSocket" [:strong " (random choice!)"] " has been configured for this example"]
          [:hr]
          [:p [:strong "Step 1: "] " try hitting the buttons:"]
          [:button#btn1 {:type "button"} "chsk-send! (w/o reply)"]
          [:button#btn2 {:type "button"} "chsk-send! (with reply)"]

          [:p [:strong "Step 2: "] " observe std-out (for server output) and below (for client output):"]
          [:textarea#output {:style "width: 100%; height: 200px;"}]

          [:hr]
          [:h2 "Step 3: try login with a user-id"]
          [:p  "The server can use this id to send events to *you* specifically."]
          [:p
           [:input#input-login {:type :text :placeholder "User-id"}]
           [:button#btn-login {:type "button"} "Secure login!"]]

          [:hr]
          [:h2 "Step 4: want to re-randomize Ajax/WebSocket connection type?"]
          [:p "Hit your browser's reload/refresh button"]
          [:script {:src "main.js"}]    ; Include our cljs target
          ]]
        (hiccups/html)
        (m-resp/ok)
        (m-resp/content-type "text/html"))))

(defn login-handler
  "Here's where you'll add your server-side login/auth procedure (Friend, etc.).
  In our simplified example we'll just always successfully authenticate the user
  with whatever user-id they provided in the auth request."
  [ring-req]
  (let [{:keys [session params]} ring-req
        {:keys [user-id]}        params]
    (debugf "Login request: %s" params)
    {:status 200 :session (assoc session :uid user-id)}))

(let [packer :edn ; Default packer, a good choice in most cases
      {:keys [ch-recv send-fn ajax-post-fn ajax-get-or-ws-handshake-fn connected-uids]}
      (sente-macchiato/make-macchiato-channel-socket-server! {:packer packer})]
  (def ajax-post                ajax-post-fn)
  (def ajax-get-or-ws-handshake ajax-get-or-ws-handshake-fn)
  (def ch-chsk                  ch-recv) ; ChannelSocket's receive channel
  (def chsk-send!               send-fn) ; ChannelSocket's send API fn
  (def connected-uids           connected-uids) ; Watchable, read-only atom
  )

(defn wrap-macchiato-res [handler]
  (fn [req res]
    (res (handler req))))

(defn routes []
  ["/" {""      {:get (wrap-macchiato-res landing-pg-handler)}
        "chsk"  {:get  ajax-get-or-ws-handshake
                 :post ajax-post
                 :ws   ajax-get-or-ws-handshake}
        "login" {:post (wrap-macchiato-res login-handler)}}])

(defn router [req res raise]
  (debugf "Request: %s" (select-keys req [:request-method :websocket? :uri :params :session]))
  (if-let [{:keys [handler route-params]}
           (bidi/match-route* (routes) (:uri req) req)]
    (handler (assoc req :route-params route-params) res raise)
    (res (not-found req))))

(def main-ring-handler
  (-> router
      (m-auth/wrap-authentication (m-session/session-backend))
      (m-resource/wrap-resource "resources/public")
      (m-defaults/wrap-defaults m-defaults/site-defaults)))

(defn start-selected-web-server! [ring-handler port]
  (infof "Starting Macchiato...")
  (let [options {:handler     ring-handler
                 :port        port
                 :websockets? true
                 :on-success  #(infof "Macchiato started on port %s" port)}
        server  (m-server/start options)]
    (m-server/start-ws server ring-handler)
    {:port    port
     :stop-fn #(.end server)}))

(defonce broadcast-enabled?_ (atom true))

(defn test-fast-server>user-pushes
  "Quickly pushes 100 events to all connected users. Note that this'll be
  fast+reliable even over Ajax!"
  []
  (doseq [uid (:any @connected-uids)]
    (doseq [i (range 100)]
      (chsk-send! uid [:fast-push/is-fast (str "hello " i "!!")]))))

(defn start-example-broadcaster!
  "As an example of server>user async pushes, setup a loop to broadcast an
  event to all connected users every 10 seconds"
  []
  (let [broadcast!
        (fn [i]
          (let [uids (:any @connected-uids)]
            (debugf "Broadcasting server>user: %s uids" (count uids))
            (doseq [uid uids]
              (chsk-send! uid
                          [:some/broadcast
                           {:what-is-this "An async broadcast pushed from server"
                            :how-often "Every 10 seconds"
                            :to-whom uid
                            :i i}]))))]

    (go-loop [i 0]
      (<! (async/timeout 10000))
      (when @broadcast-enabled?_ (broadcast! i))
      (recur (inc i)))))

(defmulti -event-msg-handler
  "Multimethod to handle Sente `event-msg`s"
  :id ; Dispatch on event-id
  )

(defn event-msg-handler
  "Wraps `-event-msg-handler` with logging, error catching, etc."
  [{:as ev-msg}]
  (-event-msg-handler ev-msg) ; Handle event-msgs on a single thread
  )

(defmethod -event-msg-handler
  :default ; Default/fallback case (no other matching handler)
  [{:keys [event ?reply-fn]}]
  (debugf "Unhandled event: %s" event)
  (when ?reply-fn
    (?reply-fn {:umatched-event-as-echoed-from-server event})))

(defmethod -event-msg-handler :example/test-rapid-push
  [] (test-fast-server>user-pushes))

(defmethod -event-msg-handler :example/toggle-broadcast
  [{:keys [?reply-fn]}]
  (let [loop-enabled? (swap! broadcast-enabled?_ not)]
    (?reply-fn loop-enabled?)))

(defonce router_ (atom nil))

(defn  stop-router! [] (when-let [stop-fn @router_] (stop-fn)))

(defn start-router! []
  (stop-router!)
  (reset! router_
          (sente/start-server-chsk-router! ch-chsk event-msg-handler)))

(defonce web-server_ (atom nil)) ; {:server _ :port _ :stop-fn (fn [])}

(defn stop-web-server! [] (when-let [m @web-server_] ((:stop-fn m))))

(defn start-web-server! [& [port]]
  (stop-web-server!)
  (let [{:keys [port] :as server-map}
        (start-selected-web-server! (var main-ring-handler) (or port 4000))
        uri (str "http://localhost:" port "/")]
    (infof "Web server is running at `%s`" uri)
    (reset! web-server_ server-map)))

(defn start! []
  (start-router!)
  (start-web-server!)
  (start-example-broadcaster!))

(defn main []
  (start!))
