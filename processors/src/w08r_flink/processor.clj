(ns w08r-flink.processor
  (:import (com.fasterxml.jackson.databind.node ObjectNode)
           (w08r_flink sliding_total tumbling_count)
           (java.time Duration)
           (org.apache.flink.api.java.functions KeySelector)
           (org.apache.flink.api.common.eventtime WatermarkStrategy)
           (org.apache.flink.api.common.serialization DeserializationSchema SimpleStringSchema)
           (org.apache.flink.api.common.typeinfo TypeInformation)
           (org.apache.flink.api.java.typeutils TypeExtractor ResultTypeQueryable)
           (org.apache.flink.connector.kafka.sink KafkaRecordSerializationSchema KafkaSink)
           (org.apache.flink.connector.kafka.source KafkaSource)
           (org.apache.flink.connector.kafka.source.enumerator.initializer OffsetsInitializer)
           (org.apache.flink.connector.kafka.source.reader.deserializer KafkaRecordDeserializationSchema)
           (org.apache.flink.streaming.api.datastream DataStream)
           (org.apache.flink.streaming.api.environment StreamExecutionEnvironment)
           (org.apache.flink.streaming.api.functions.windowing ProcessAllWindowFunction ProcessWindowFunction)
           (org.apache.flink.streaming.api.windowing.assigners SlidingEventTimeWindows TumblingProcessingTimeWindows)
           (org.apache.flink.streaming.api.windowing.windows TimeWindow)
           (org.apache.flink.streaming.connectors.kafka KafkaDeserializationSchema)
           (org.apache.flink.streaming.util.serialization JSONKeyValueDeserializationSchema)
           (org.apache.flink.table.api Table)
           (org.apache.flink.table.api.bridge.java StreamTableEnvironment)
           (org.apache.flink.util Collector)
           (org.apache.flink.api.common.functions MapFunction)
           (org.apache.kafka.clients.consumer ConsumerRecord)
           (org.apache.kafka.clients.producer ProducerRecord))

  (:gen-class))

(deftype kafka-click-to-string []
  MapFunction (map [this o]
               (-> o (.get "value") (.get "url") (.asText)))
  ResultTypeQueryable (getProducedType [this]
                        (TypeExtractor/getForClass String)))

(deftype keyer []
  KeySelector (getKey [this in] in)
  ResultTypeQueryable (getProducedType [this]
                        (TypeExtractor/getForClass String)))

(defn -main [& _args]
  (let [e (StreamExecutionEnvironment/getExecutionEnvironment)
        kvd (new JSONKeyValueDeserializationSchema false)
        ks (-> (KafkaSource/builder)
               (.setBootstrapServers "kafka:9092")
               (.setTopics ["clicks"])
               (.setGroupId "flink")
               (.setStartingOffsets (OffsetsInitializer/earliest))
               (.setDeserializer (reify KafkaRecordDeserializationSchema
                                   (open [this c]
                                     (.open kvd c))
                                   (getProducedType [this]
                                     (TypeExtractor/getForClass ObjectNode))
                                   (deserialize [this, r o]
                                     (.deserialize kvd r o))))
               (.build))

        counts (-> (KafkaSink/builder)
                   (.setBootstrapServers "kafka:9092")
                   (.setRecordSerializer (reify KafkaRecordSerializationSchema
                                           (serialize [this, e, c, t]
                                             (new ProducerRecord
                                                  "counts",
                                                  (-> (java.util.UUID/randomUUID) (.toString) (.getBytes))
                                                  (.getBytes e)))))
                   (.build))

        total (-> (KafkaSink/builder)
                  (.setBootstrapServers "kafka:9092")
                  (.setRecordSerializer (reify KafkaRecordSerializationSchema
                                          (serialize [this, e, c, t]
                                            (new ProducerRecord
                                                 "total",
                                                 (-> (java.util.UUID/randomUUID) (.toString) (.getBytes))
                                                 (.getBytes e)))))
                  (.build))

        in (-> (.fromSource e ks (WatermarkStrategy/forBoundedOutOfOrderness (Duration/ofSeconds 20)) "clicks")
               (.map (new kafka-click-to-string)))]

    (-> in
        (.keyBy (new keyer))
        (.window (TumblingProcessingTimeWindows/of (Duration/ofSeconds 5)))
        (.process (new tumbling_count))
        (.sinkTo counts))

    (-> in
        (.windowAll (SlidingEventTimeWindows/of (Duration/ofSeconds 10) (Duration/ofSeconds 5)))
        (.process (new sliding_total))
        (.sinkTo total))

    (.execute e "Kafka streamer")))
