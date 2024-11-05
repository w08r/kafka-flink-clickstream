(ns w08r-flink.processor
  (:require [w08r-flink.kafka :as k])
  (:import
   (w08r_flink sliding_total tumbling_count Keyer)
   (org.apache.flink.streaming.api.datastream DataStreamSource)
   (java.time Duration)
   (org.apache.flink.configuration Configuration CheckpointingOptions)
   (org.apache.flink.api.common.eventtime WatermarkStrategy)
   (org.apache.flink.streaming.api.environment StreamExecutionEnvironment)
   (org.apache.flink.streaming.api.windowing.assigners SlidingEventTimeWindows
                                                       TumblingProcessingTimeWindows)

   (org.apache.flink.table.api.bridge.java StreamTableEnvironment)
   (org.apache.flink.table.api Expressions Tumble)
   (org.apache.flink.streaming.util.serialization
    JSONKeyValueDeserializationSchema))

  (:gen-class))

(defn exp [coll]
  (into-array org.apache.flink.table.expressions.Expression coll))

(defn $
  [arg] (Expressions/$ arg))

(defn setup-checkpointing [^StreamExecutionEnvironment e]
  (.enableCheckpointing e 2000)
  (let [cfg (new Configuration)]
    (.set cfg CheckpointingOptions/CHECKPOINT_STORAGE "filesystem")
    (.set cfg CheckpointingOptions/CHECKPOINTS_DIRECTORY "file:///checkpoints")
    (.configure e cfg)))

(defn -main [& _args]
  (let [e (StreamExecutionEnvironment/getExecutionEnvironment)
        t ^StreamTableEnvironment (StreamTableEnvironment/create e)
        kvd (new JSONKeyValueDeserializationSchema false)

        ks (k/source kvd)
        counts (k/sink "counts")
        total (k/sink "total")

        ;; Setup the main click stream, read from kafka.  The
        ;; `forBoundedOutOfOrderness` is recommended for kafka in the
        ;; case that data is partitioned, but within a partition the
        ;; events are in strictly chronological order
        in ^DataStreamSource (-> (.fromSource
                                  e ks
                                  (WatermarkStrategy/forBoundedOutOfOrderness
                                   (Duration/ofSeconds 20))
                                  "clicks")
                                 (.map (k/->click-to-row)))]

    (setup-checkpointing e)

    ;; set up the grouped counts output stream
    (-> in
        (.keyBy (new Keyer))
        (.window
         (TumblingProcessingTimeWindows/of
          (Duration/ofSeconds 5)))
        (.process (new tumbling_count))
        (.sinkTo counts))

    ;; set up the non-grouped total output stream
    (-> in
        (.windowAll
         (SlidingEventTimeWindows/of
          (Duration/ofSeconds 10) (Duration/ofSeconds 5)))
        (.process (new sliding_total))
        (.sinkTo total))

    ;; load the static url lookup table from csv
    (.executeSql t "CREATE TEMPORARY TABLE url_lookup (
                        url VARCHAR(50)
                    )
                    WITH (
                        'connector'='filesystem',
                        'path'='/urls.csv',
                        'format'='csv');")

    ;; Create an initial table from the main click input
    ;; stream; join that to the statuc urls table which provides
    ;; a lookup of urls we want to emit on.
    ;; Create a 60s tumble window and group by url over that, count the
    ;; total for the window.  Emit window stats and count.
    (let [input-table (.fromDataStream
                       t in
                       (exp [(.rowtime ($ "f1")) ($ "f0")]))
          lookup (.from t "url_lookup")]
      (.createTemporaryView t "Urls" input-table)
      (-> (.from t "Urls")
          ;; join to static table for url filtering
          (.join lookup)
          (.where (.isEqual ($ "url") ($ "f0")))
          ;; create tumbling window, w, over 60s
          (.window (->
                    (Tumble/over (-> (Expressions/lit 60)
                                     (.seconds)))
                    (.on ($ "f1"))
                    (.as "w")))
          ;; group by the window, w, and the url column, f0
          (.groupBy (exp [($ "w") ($ "f0")]))
          ;; pull out the window stats for the group, and its count
          (.select (exp [($ "f0")
                         (.start ($ "w"))
                         (.end ($ "w"))
                         (.rowtime ($ "w"))
                         (.count ($ "f0"))]))
          ;; rewrite the results as a new datastream
          (as-> rt (.toDataStream t rt))
          ;; write the stream to stdout
          (as-> rs (.print rs))))

    (.execute e "Kafka streamer")))
