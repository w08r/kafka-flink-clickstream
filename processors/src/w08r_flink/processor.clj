(ns w08r-flink.processor
  (:import (com.fasterxml.jackson.databind.node ObjectNode)
           (w08r_flink sliding_total tumbling_count)
           (java.time Duration)
           (org.apache.flink.api.java.functions KeySelector)
           (org.apache.flink.api.common.eventtime WatermarkStrategy)
           (org.apache.flink.api.java.typeutils TypeExtractor ResultTypeQueryable)
           (org.apache.flink.connector.kafka.sink KafkaRecordSerializationSchema KafkaSink)
           (org.apache.flink.connector.kafka.source KafkaSource)
           (org.apache.flink.connector.kafka.source.enumerator.initializer OffsetsInitializer)
           (org.apache.flink.connector.kafka.source.reader.deserializer KafkaRecordDeserializationSchema)
           (org.apache.flink.streaming.api.environment StreamExecutionEnvironment)
           (org.apache.flink.streaming.api.windowing.assigners SlidingEventTimeWindows TumblingProcessingTimeWindows)
           (org.apache.flink.streaming.api.windowing.windows TimeWindow)
           (org.apache.flink.streaming.util.serialization JSONKeyValueDeserializationSchema)
           (org.apache.flink.api.common.functions MapFunction)
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

(defn kafka-deser [topic]
  (reify KafkaRecordSerializationSchema
    (serialize [this, e, c, t]
      (new ProducerRecord
           topic,
           (-> (java.util.UUID/randomUUID) (.toString) (.getBytes))
           (.getBytes e)))))

(defn kafka-sink [topic]
  (-> (KafkaSink/builder)
      (.setBootstrapServers "kafka:9092")
      (.setRecordSerializer (kafka-deser topic))
      (.build)))

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

        counts (kafka-sink "counts")

        total (kafka-sink "total")

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
