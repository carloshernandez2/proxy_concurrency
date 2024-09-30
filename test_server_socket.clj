(ns proxy-concurrency.test-server-socket
  (:import [java.net InetSocketAddress StandardSocketOptions]
           [java.nio ByteBuffer]
           [java.nio.channels Selector ServerSocketChannel SocketChannel SelectionKey]))

(defn test-proxy
  []
  (let [server-socket-channel (ServerSocketChannel/open)
        selector (Selector/open)
        _ (.setOption server-socket-channel StandardSocketOptions/SO_REUSEADDR true)
        _ (.bind server-socket-channel (InetSocketAddress. 8001) 50)
        _ (.configureBlocking server-socket-channel false)
        _ (.register server-socket-channel selector SelectionKey/OP_ACCEPT {:op :accept})]
    (loop [_ (.select selector)]
      (loop [selected-keys (.selectedKeys selector)]
        (let [^SelectionKey selected-key (first selected-keys)
              {:keys [op] :as attachment} (.attachment selected-key)]
          (cond (= op :accept)
                (let [^ServerSocketChannel server-socket-channel (.channel selected-key)]
                  (when-let [downstream-socket-channel (.accept server-socket-channel)]
                    (.configureBlocking downstream-socket-channel false)
                    (.register downstream-socket-channel selector SelectionKey/OP_READ {:op :read})))
                (= op :read)
                (let [^SocketChannel downstream-socket-channel (.channel selected-key)
                      buffer (ByteBuffer/allocate 2)
                      bytes-read (.read downstream-socket-channel buffer)]
                  (cond (pos? bytes-read)
                        (let [dst (byte-array bytes-read)]
                          (def dst (String. dst))
                          (.flip buffer)
                          (.get buffer dst)
                          (.register downstream-socket-channel selector SelectionKey/OP_WRITE {:op :write :data dst})
                          (.clear buffer))
                        (neg? bytes-read)
                        (do
                          (println "Closing downstream socket")
                          (.close downstream-socket-channel))))
                (= op :write)
                (let [^SocketChannel downstream-socket-channel (.channel selected-key)
                      ^"[B" data (:data attachment)]
                  (let [buffer (ByteBuffer/allocate (alength data))]
                    (.put buffer data)
                    (while (not= (.position buffer) 0)
                      (.flip buffer)
                      (.write downstream-socket-channel buffer)
                      (.compact buffer))
                    (.clear buffer)
                    (.register downstream-socket-channel selector SelectionKey/OP_READ {:op :read})))
                :else (throw (Exception. "Unknown channel type")))
          (when-let [rem-keys (not-empty (rest selected-keys))] (recur rem-keys))))
      (recur (.select selector)))))

(comment
 (future (test-proxy)))
