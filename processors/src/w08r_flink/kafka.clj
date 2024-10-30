(ns w08r-flink.kafka
  (:import (com.fasterxml.jackson.databind.node ObjectNode)
           (org.apache.flink.connector.kafka.sink KafkaRecordSerializationSchema KafkaSink)
           (org.apache.flink.connector.kafka.source KafkaSource)
           (org.apache.flink.connector.kafka.source.enumerator.initializer OffsetsInitializer)
           (org.apache.flink.api.java.typeutils TypeExtractor ResultTypeQueryable)
           (org.apache.flink.api.java.tuple Tuple2)
           (org.apache.flink.streaming.util.serialization JSONKeyValueDeserializationSchema)
           (org.apache.flink.api.common.functions MapFunction)
           (org.apache.flink.connector.kafka.source.reader.deserializer KafkaRecordDeserializationSchema)
           (org.apache.flink.types Row RowKind)
           (org.apache.kafka.clients.producer ProducerRecord)))

(set! *warn-on-reflection* true)

(deftype click-to-row []
  MapFunction (map [this o]
                (let [on ^ObjectNode o]
                  (Tuple2/of
                   (-> on (.get "value") (.get "url") (.asText))
                   (-> on (.get "value") (.get "time") (.asLong)))))
  ResultTypeQueryable (getProducedType [this]
                        (TypeExtractor/getForObject (Tuple2/of "" 0))))

(defn serialiser [^String topic]
  (reify KafkaRecordSerializationSchema
    (serialize ^ProducerRecord [this, e, c, t]
      (let [es ^String e]
        (new ProducerRecord
             topic,
             (-> (java.util.UUID/randomUUID) (.toString) (.getBytes))
             (.getBytes es))))))

(defn sink [^String topic]
  (-> (KafkaSink/builder)
      (.setBootstrapServers "kafka:9092")
      (.setRecordSerializer (serialiser topic))
      (.build)))

(defn source [^JSONKeyValueDeserializationSchema kvd]
  (-> (KafkaSource/builder)
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
      (.build)))
