(ns w08r-flink.processor
  (:require [w08r-flink.kafka :as k])
  (:import
   (w08r_flink ts_extractor sliding_total tumbling_count)
   (org.apache.flink.streaming.api.datastream DataStreamSource KeyedStream)
   (java.time Duration)
   (org.apache.flink.api.common.eventtime WatermarkStrategy)
   (org.apache.flink.streaming.api.environment StreamExecutionEnvironment)
   (org.apache.flink.streaming.api.windowing.assigners SlidingEventTimeWindows
                                                       TumblingProcessingTimeWindows)

   (org.apache.flink.table.api.bridge.java StreamTableEnvironment)
   (org.apache.flink.streaming.api.windowing.windows TimeWindow)
   (org.apache.flink.table.api Expressions Tumble)
   (org.apache.flink.api.common.functions MapFunction)
   (org.apache.flink.types Row)
   (org.apache.flink.api.java.typeutils TypeExtractor ResultTypeQueryable)
   (org.apache.flink.streaming.util.serialization
    JSONKeyValueDeserializationSchema))

  (:gen-class))

(defn exp [coll]
  (into-array org.apache.flink.table.expressions.Expression coll))

(defn $
  [arg] (Expressions/$ arg))

(defn -main [& _args]
  (let [e (StreamExecutionEnvironment/getExecutionEnvironment)
        t ^StreamTableEnvironment (StreamTableEnvironment/create e)
        kvd (new JSONKeyValueDeserializationSchema false)
        ks (k/source kvd)

        counts (k/sink "counts")

        total (k/sink "total")

        in ^DataStreamSource (-> (.fromSource
                e ks
                (WatermarkStrategy/forBoundedOutOfOrderness
                 (Duration/ofSeconds 20))
                "clicks")
               (.map (k/->click-to-row)))]

    (-> in
        (.keyBy (k/->keyer))
        (.window
         (TumblingProcessingTimeWindows/of
          (Duration/ofSeconds 5)))
        (.process (new tumbling_count))
        (.sinkTo counts))

    (-> in
        (.windowAll
         (SlidingEventTimeWindows/of
          (Duration/ofSeconds 10) (Duration/ofSeconds 5)))
        (.process (new sliding_total))
        (.sinkTo total))

    (let [input-table (.fromDataStream
                       t in
                       (exp [(.rowtime ($ "f1")) ($ "f0")]))]
      (.createTemporaryView t "Urls" input-table)
      (-> (.from t "Urls")
          (.window (->
                    (Tumble/over (-> (Expressions/lit 60)
                                     (.seconds)))
                    (.on ($ "f1"))
                    (.as "w")))
          (.groupBy (exp [($ "w") ($ "f0")]))
          (.select (exp [($ "f0")
                         (.start ($ "w"))
                         (.end ($ "w"))
                         (.rowtime ($ "w"))
                         (.count ($ "f0"))]))
          (as-> rt (.toDataStream t rt))
          (as-> rs (do
                     (.createTemporaryView t "UrlWindowCount" rs)
                     (.print rs)))))

    (let [count-table (.from t "UrlWindowCount")]
      (-> (.from t "UrlWindowCount")
          (.select (exp [(.count ($ "f0"))]))
          (as-> rt (.toDataStream t rt))
          (as-> rs (.print rs))))

    (.execute e "Kafka streamer")))
