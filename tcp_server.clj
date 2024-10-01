(ns proxy-concurrency.tcp-server
  (:refer-clojure :exclude [read send])
  (:require [clojure.string :as str])
  (:import (java.net InetSocketAddress StandardSocketOptions)
           [java.nio ByteBuffer]
           (java.nio.channels SelectionKey Selector ServerSocketChannel SocketChannel)))

(defn start-system
  [system-map]
  (try
    (doseq [{start-fn :start} (vals system-map)]
      (start-fn system-map))
    system-map
    (catch Exception e
      (println "Error starting system:" (.getMessage e))
      (doseq [{stop-fn :stop} (vals system-map)]
        (stop-fn system-map))
      (throw e))))

(def empty-line? (comp boolean #{[10] [13 10]}))

(defn parse-headers [headers]
  (let [lines (str/split (String. headers) #"(\r\n|\n|\r)")]
    (zipmap (map (fn [line] (keyword (str/lower-case (str/trim (first (str/split line #":" 2)))))) lines)
            (map (fn [line] (str/trim (second (str/split line #":" 2)))) lines))))

(defn reassemble-headers [parsed-headers]
  (.getBytes (str (str/join "\r\n" (map (fn [[k v]] (str (name k) ": " v)) parsed-headers)) "\r\n\r\n")))

(defn read-byte-line
  [byte-seq]
  (reduce (fn [[acc rem] byte]
            (let [res [(conj acc byte) (rest rem)]] (if (= byte 10) (reduced res) res))) [[] byte-seq] byte-seq))

(defn read-byte-lines-until [byte-seq pred]
  (loop [rem-byte-seq byte-seq
         acc          []]
    (if (or (empty? rem-byte-seq) (pred (peek acc)))
      [acc rem-byte-seq]
      (let [[line rem] (read-byte-line rem-byte-seq)]
        (recur rem (conj acc line))))))

(defn decompose-http-message [chunk]
  (let [[start-line rem-lines] (read-byte-line chunk)
        [headers body] (read-byte-lines-until rem-lines empty-line?)]
    [(byte-array start-line) (byte-array (mapcat identity headers)) (byte-array body)]))

(defn get-selector [{:keys [selector]}] @selector)

(def selector-comp
  (fn []
    (let [selector (atom nil)]
      {:selector selector
       :start    (fn [_]
                   (reset! selector (Selector/open)))
       :stop     (fn [_] (when-let [^Selector selector @selector] (.close selector)))})))

(def server-channel-comp
  (fn []
    (let [port       8001
          queue-size 50
          channel    (atom nil)]
      {:channel channel
       :start   (fn [{selector-comp :selector}]
                  (reset! channel
                          (let [server-channel (ServerSocketChannel/open)]
                            (.setOption server-channel StandardSocketOptions/SO_REUSEADDR true)
                            (.bind server-channel (InetSocketAddress. port) queue-size)
                            (.configureBlocking server-channel false)
                            (.register server-channel (get-selector selector-comp) SelectionKey/OP_ACCEPT {:op :accept})
                            server-channel)))
       :stop    (fn [_]
                  (when-let [server-channel @channel]
                    (.close server-channel)))})))

;TODO implement way to free connections after a set timeout
(def upstream-channel-comp
  (fn []
    {:free-channels   (ref #{})
     :busy-channels   (ref #{})
     :max-connections 2
     :start           (fn [_])
     :stop            (fn [_])}))

(defn new-connection [{:keys [max-connections free-channels busy-channels]}]
  (let [host    "127.0.0.1"
        port    9000]
    (dosync
     (if-let [channel (first @free-channels)]
       (do (alter free-channels disj channel)
           (alter busy-channels conj channel)
           channel)
       (if (< (count @busy-channels) max-connections)
         (let [channel (SocketChannel/open)]
           (alter busy-channels conj channel)
           (.connect channel (InetSocketAddress. host port))
           (.configureBlocking channel false)
           channel)
         (throw (ex-info "All connections busy" {})))))))

(defn free-connection [^SocketChannel channel {:keys [free-channels busy-channels]}]
  (dosync
   (if (.isOpen channel)
     (do (alter busy-channels disj channel)
         (alter free-channels conj channel))
     (alter busy-channels disj channel))))

(def system
  (fn []
    {:selector         (selector-comp)
     :server-channel   (server-channel-comp)
     :upstream-channel (upstream-channel-comp)}))

(defn get-address [^SocketChannel channel]
  (.toString (.getRemoteAddress channel)))

(defn accept [selector-comp upstream-channel-comp {^ServerSocketChannel server-channel :channel}]
  (when-let [downstream-channel (.accept server-channel)]
    (try
      (do (println "New connection from" (get-address downstream-channel))
          (.configureBlocking downstream-channel false)
          (.register downstream-channel (get-selector selector-comp) SelectionKey/OP_READ {:op               :read-downstream
                                                                                           :upstream-channel (new-connection upstream-channel-comp)})
          downstream-channel)
      (catch Exception e (throw (ex-info (.getMessage e) {:downstream-channel downstream-channel}))))))

(defn select [selector-comp]
  (let [^Selector selector (get-selector selector-comp)
        _                  (.select selector)
        selected-keys      (filter (fn [^SelectionKey selected-key] (pos? (.interestOps selected-key))) (.selectedKeys selector))]
    (doseq [^SelectionKey selected-key selected-keys]
      (when (not= (.interestOps selected-key) SelectionKey/OP_ACCEPT)
        (.interestOps selected-key 0)))
    (map (fn [^SelectionKey selected-key]
           {:attachment (.attachment selected-key)
            :channel    (.channel selected-key)}) selected-keys)))

(defn log-bytes [data diagram]
  (println (str diagram (alength data) "B"))
  data)

(defn ^:private clear-and-return [^ByteBuffer buffer outcome]
  (.clear buffer)
  outcome)

(defn read
  [^SocketChannel channel]
  (let [buffer     (ByteBuffer/allocate 1024)
        bytes-read (.read channel buffer)]
    (clear-and-return
     buffer
     (cond (pos? bytes-read)
           (let [dst (byte-array bytes-read)]
             (.flip buffer)
             (.get buffer dst)
             {:outcome :success :result dst})
           (neg? bytes-read)
           (do (println "Closing downstream socket")
               (.close channel)
               {:outcome :closed})
           :else {:outcome :empty-read}))))

(defn read-downstream
  [selector-comp upstream-channel-comp {downstream-channel :channel attachment :attachment}]
  (try
    (let [{:keys [outcome result]} (read downstream-channel)]
      (case outcome
        :success (do (log-bytes result "-> *    ")
                     (.register (:upstream-channel attachment)
                                (get-selector selector-comp)
                                SelectionKey/OP_WRITE
                                {:op                 :write-upstream
                                 :message-chunk      (byte-array (concat (:message-chunk attachment []) result))
                                 :parsed-message     (:parsed-message attachment)
                                 :remaining-length   (:remaining-length attachment)
                                 :downstream-channel downstream-channel}))
        :closed (free-connection (:upstream-channel attachment) upstream-channel-comp)
        :empty-read (.register downstream-channel (get-selector selector-comp) SelectionKey/OP_READ attachment)))
    (catch Exception e (throw (ex-info (.getMessage e) {:downstream-channel downstream-channel
                                                        :upstream-channel   (:upstream-channel attachment)})))))

(defn read-upstream
  [selector-comp upstream-channel-comp {upstream-channel :channel attachment :attachment}]
  (try
    (let [{:keys [outcome result]} (read upstream-channel)]
      (case outcome
        :success (do (log-bytes result "   * <- ")
                     (.register (:downstream-channel attachment)
                                (get-selector selector-comp)
                                SelectionKey/OP_WRITE
                                {:op               :write-downstream
                                 :message-chunk    (byte-array (concat (:message-chunk attachment []) result))
                                 :parsed-message   (:parsed-message attachment)
                                 :parsed-request   (:parsed-request attachment)
                                 :remaining-length (:remaining-length attachment)
                                 :upstream-channel upstream-channel}))
        :closed (free-connection upstream-channel upstream-channel-comp)
        :empty-read (.register upstream-channel (get-selector selector-comp) SelectionKey/OP_READ attachment)))
    (catch Exception e (throw (ex-info (.getMessage e) {:downstream-channel (:downstream-channel attachment)
                                                        :upstream-channel   upstream-channel})))))

(defn parse
  [selector-comp {:keys [attachment channel]}]
  (let [parsed-message     (:parsed-message attachment)
        message-chunk      (:message-chunk attachment)
        downstream-channel (:downstream-channel attachment)
        selector           (get-selector selector-comp)]
    (cond parsed-message parsed-message
          (empty-line? (peek (first (read-byte-lines-until message-chunk empty-line?))))
          (let [[start-line headers body] (decompose-http-message message-chunk)
                parsed-headers (parse-headers headers)]
            {:start-line     start-line
             :parsed-headers parsed-headers
             :body           body})
          :else (.register downstream-channel selector SelectionKey/OP_READ {:op               :read-downstream
                                                                             :message-chunk    message-chunk
                                                                             :upstream-channel channel}))))

(defn build-upstream-request
  [{:keys [parsed-headers start-line body]} {:keys [attachment]}]
  (if (:remaining-length attachment)
    {:message-chunk    (:message-chunk attachment)
     :remaining-length (:remaining-length attachment)}
    (let [upstream-request-headers (reassemble-headers (dissoc parsed-headers :connection))
          content-length           (Integer/parseInt (get parsed-headers :content-length "0"))]
      {:message-chunk    (byte-array (concat start-line upstream-request-headers body))
       :remaining-length (+ content-length (alength start-line) (alength upstream-request-headers))})))

(defn build-downstream-response
  [{:keys [parsed-headers start-line body]} {{{{:keys [connection]} :parsed-headers} :parsed-request :as attachment} :attachment}]
  (if (:remaining-length attachment)
    {:message-chunk    (:message-chunk attachment)
     :remaining-length (:remaining-length attachment)}
    (let [downstream-response-headers (reassemble-headers (cond-> parsed-headers (some? connection) (assoc :connection connection)))
          content-length           (Integer/parseInt (get parsed-headers :content-length "0"))]
      {:message-chunk    (byte-array (concat start-line downstream-response-headers body))
       :remaining-length (+ content-length (alength start-line) (alength downstream-response-headers))})))

(defn write
  [^SocketChannel channel ^"[B" message-chunk]
  (let [buffer (ByteBuffer/allocate (alength message-chunk))]
    (.put buffer message-chunk)
    (while (not= (.position buffer) 0)
      (.flip buffer)
      (.write channel buffer)
      (.compact buffer))
    (.clear buffer)))

(defn write-upstream
  [{:keys [remaining-length message-chunk]} selector-comp parsed-message {^SocketChannel channel :channel attachment :attachment}]
  (try
    (let [new-remaining-length (- remaining-length (alength message-chunk))]
      (write channel message-chunk)
      (log-bytes message-chunk "   * -> ")
      (if (pos? new-remaining-length)
        (.register (:downstream-channel attachment) (get-selector selector-comp) SelectionKey/OP_READ {:op               :read-downstream
                                                                                                       :parsed-message   parsed-message
                                                                                                       :remaining-length new-remaining-length
                                                                                                       :upstream-channel channel})
        (.register channel (get-selector selector-comp) SelectionKey/OP_READ {:op                 :read-upstream
                                                                              :parsed-request     parsed-message
                                                                              :downstream-channel (:downstream-channel attachment)})))
    (catch Exception e (throw (ex-info (.getMessage e) {:downstream-channel (:downstream-channel attachment)
                                                        :upstream-channel   channel})))))

(def ^:private close? (comp boolean #{"close"} :connection :parsed-headers :parsed-request))

(defn write-downstream
  [{:keys [remaining-length message-chunk]} selector-comp parsed-message {channel :channel attachment :attachment}]
  (try
    (let [new-remaining-length (- remaining-length (alength message-chunk))]
      (write channel message-chunk)
      (log-bytes message-chunk "<- *    ")
      (cond (pos? new-remaining-length) (.register (:upstream-channel attachment)
                                                   (get-selector selector-comp) SelectionKey/OP_READ
                                                   {:op                 :read-upstream
                                                    :parsed-message     parsed-message
                                                    :parsed-request     (:parsed-request attachment)
                                                    :remaining-length   new-remaining-length
                                                    :downstream-channel channel})
            (close? attachment) (.close channel)
            :else
            (.register channel (get-selector selector-comp) SelectionKey/OP_READ {:op               :read-downstream
                                                                                  :upstream-channel (:upstream-channel attachment)})))
    (catch Exception e (throw (ex-info (.getMessage e) {:downstream-channel channel
                                                        :upstream-channel   (:upstream-channel attachment)})))))

(defn keep-server-running [system handler]
  (let [result (try (handler system)
                    (catch Exception _ ::restart))]
    (when (= ::restart result)
      (recur system handler))))

(defn concurrency-proxy
  [system]
  (while true
    (let [selector-comp         (:selector system)
          upstream-channel-comp (:upstream-channel system)]
      (try
        (doseq [{:keys [attachment] :as selected-key} (select selector-comp)]
          (case (:op attachment)
            :read-upstream (read-upstream selector-comp upstream-channel-comp selected-key)
            :read-downstream (read-downstream selector-comp upstream-channel-comp selected-key)
            :write-upstream (let [parsed-message (parse selector-comp selected-key)]
                              (when (map? parsed-message)
                                (-> (build-upstream-request parsed-message selected-key)
                                    (write-upstream selector-comp parsed-message selected-key))))
            :write-downstream (let [parsed-message (parse selector-comp selected-key)]
                                (when (map? parsed-message)
                                  (-> (build-downstream-response parsed-message selected-key)
                                      (write-downstream selector-comp parsed-message selected-key))))
            :accept (accept selector-comp upstream-channel-comp selected-key)
            (throw (Exception. "Unknown channel op"))))
        (catch Exception e
          (println (ex-data e))
          (when-let [downstream-channel (:downstream-channel (ex-data e))]
            (.close downstream-channel))
          (when-let [upstream-channel (:upstream-channel (ex-data e))]
            (.close upstream-channel)
            (free-connection upstream-channel upstream-channel-comp))
          (println e)
          (println "Error:" (.getMessage e))
          (throw e))))))

(comment
 (future
  (-> (system)
      start-system
      (keep-server-running #'concurrency-proxy))))
