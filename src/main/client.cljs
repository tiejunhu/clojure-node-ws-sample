(ns client
  (:require
   [clojure.string  :as str]
   [taoensso.encore :as encore :refer ()]
   [taoensso.timbre :as timbre]
   [taoensso.sente  :as sente]))

;; (timbre/set-level! :trace) ; Uncomment for more logging

;;;; Util for logging output to on-screen console

(def output-el (.getElementById js/document "output"))
(defn ->output! [fmt & args]
  (let [msg (apply encore/format fmt args)]
    (timbre/debug msg)
    (aset output-el "value" (str "• " (.-value output-el) "\n" msg))
    (aset output-el "scrollTop" (.-scrollHeight output-el))))

(->output! "ClojureScript appears to have loaded correctly.")

(def ?csrf-token
  (when-let [el (.getElementById js/document "__anti-forgery-token")]
    (.getAttribute el "data-csrf-token")))

;;;; Define our Sente channel socket (chsk) client

(let [;; For this example, select a random protocol:
      rand-chsk-type :auto
      _              (->output! "Randomly selected chsk type: %s" rand-chsk-type)

      ;; Serializtion format, must use same val for client + server:
      packer         :edn ; Default packer, a good choice in most cases
      ;; (sente-transit/get-flexi-packer :edn) ; Experimental, needs Transit dep

      {:keys [chsk ch-recv send-fn]}
      (sente/make-channel-socket-client!
       "/chsk" ; Must match server Ring routing URL
       ?csrf-token
       {:type   rand-chsk-type
        :packer packer})]

  (def chsk       chsk)
  (def ch-chsk    ch-recv) ; ChannelSocket's receive channel
  (def chsk-send! send-fn) ; ChannelSocket's send API fn
  )

;;;; Sente event handlers

(defmulti -event-msg-handler
  "Multimethod to handle Sente `event-msg`s"
  :id ; Dispatch on event-id
  )

(defn event-msg-handler
  "Wraps `-event-msg-handler` with logging, error catching, etc."
  [{:as ev-msg}]
  (-event-msg-handler ev-msg))

(defmethod -event-msg-handler
  :default ; Default/fallback case (no other matching handler)
  [{:keys [event]}]
  (->output! "Unhandled event: %s" event))

(defmethod -event-msg-handler :chsk/state
  [{:keys [?data]}]
  (if (= ?data {:first-open? true})
    (->output! "Channel socket successfully established!")
    (->output! "Channel socket state change: %s" ?data)))

(defmethod -event-msg-handler :chsk/recv
  [{:keys [?data]}]
  (->output! "Push event from server: %s" ?data))

(defmethod -event-msg-handler :chsk/handshake
  [{:keys [?data]}]
  (->output! "Handshake: %s" ?data))

(defonce router_ (atom nil))
(defn  stop-router! [] (when-let [stop-f @router_] (stop-f)))
(defn start-router! []
  (stop-router!)
  (reset! router_
          (sente/start-client-chsk-router!
           ch-chsk event-msg-handler)))

;;;; UI events

(when-let [target-el (.getElementById js/document "btn1")]
  (.addEventListener target-el "click"
                     (fn []
                       (->output! "Button 1 was clicked (won't receive any reply from server)")
                       (chsk-send! [:example/button1 {:had-a-callback? "nope"}]))))

(when-let [target-el (.getElementById js/document "btn2")]
  (.addEventListener target-el "click"
                     (fn []
                       (->output! "Button 2 was clicked (will receive reply from server)")
                       (chsk-send! [:example/button2 {:had-a-callback? "indeed"}] 5000
                                   (fn [cb-reply] (->output! "Callback reply: %s" cb-reply))))))

(defn btn-login-click []
  (let [user-id (.-value (.getElementById js/document "input-login"))]
    (if (str/blank? user-id)
      (js/alert "Please enter a user-id first")
      (do
        (->output! "Logging in with user-id %s" user-id)

            ;;; Use any login procedure you'd like. Here we'll trigger an Ajax
            ;;; POST request that resets our server-side session. Then we ask
            ;;; our channel socket to reconnect, thereby picking up the new
            ;;; session.

        (sente/ajax-lite "/login"
                         {:method :post
                          :headers {:x-csrf-token ?csrf-token}
                          :params {:user-id    (str user-id)}}
                         (fn [ajax-resp]
                           (->output! "Ajax login response: %s" ajax-resp)
                           (let [login-successful? true ; Your logic here
                                 ]
                             (if-not login-successful?
                               (->output! "Login failed")
                               (do
                                 (->output! "Login successful")
                                 (sente/chsk-reconnect! chsk))))))))))

(when-let [target-el (.getElementById js/document "btn-login")]
  (.addEventListener target-el "click" btn-login-click))

(defn start! [] (start-router!))

(defonce _start-once (start!))
