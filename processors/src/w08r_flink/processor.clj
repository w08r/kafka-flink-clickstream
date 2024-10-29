(ns w08r-flink.processor
  (:require [w08r-flink.kafka :as k])
  (:import
   (w08r_flink sliding_total tumbling_count)
   (java.time Duration)
   (org.apache.flink.api.common.eventtime WatermarkStrategy)
   (org.apache.flink.streaming.api.environment StreamExecutionEnvironment)
   (org.apache.flink.streaming.api.windowing.assigners SlidingEventTimeWindows
                                                       TumblingProcessingTimeWindows)
   (org.apache.flink.streaming.api.windowing.windows TimeWindow)
   (org.apache.flink.streaming.util.serialization
    JSONKeyValueDeserializationSchema))

  (:gen-class))

(defn -main [& _args]
  (let [e (StreamExecutionEnvironment/getExecutionEnvironment)
        kvd (new JSONKeyValueDeserializationSchema false)
        ks (k/source kvd)

        counts (k/sink "counts")

        total (k/sink "total")

        in (-> (.fromSource
                e ks
                (WatermarkStrategy/forBoundedOutOfOrderness
                 (Duration/ofSeconds 20))
                "clicks")
               (.map (k/->click-to-string)))]

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

    (.execute e "Kafka streamer")))
