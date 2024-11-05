(ns w08r-flink.kafka
  (:import (com.fasterxml.jackson.databind.node ObjectNode)
           (com.fasterxml.jackson.databind ObjectMapper)
           (org.apache.flink.connector.kafka.sink KafkaRecordSerializationSchema KafkaSink)
           (org.apache.flink.connector.kafka.source KafkaSource)
           (org.apache.flink.connector.kafka.source.enumerator.initializer OffsetsInitializer)
           (org.apache.flink.api.java.typeutils TypeExtractor ResultTypeQueryable)
           (org.apache.flink.api.java.tuple Tuple2)
           (org.apache.flink.streaming.util.serialization JSONKeyValueDeserializationSchema)
           (org.apache.flink.api.common.functions MapFunction)
           (org.apache.flink.connector.kafka.source.reader.deserializer
            KafkaRecordDeserializationSchema)
           (org.apache.kafka.clients.producer ProducerRecord)))

(set! *warn-on-reflection* true)

;; Convert the click data, which has already been deserialised into a
;; jackson objectnode, into a map.  This class implements the
;; MapFunction interface so can be used in a process call.
(deftype click-to-row
         []
  MapFunction (map [_this o]
                (let [on ^ObjectNode o]
                  (Tuple2/of
                   (-> on (.get "value") (.get "url") (.asText))
                   (-> on (.get "value") (.get "time") (.asLong)))))

  ResultTypeQueryable (getProducedType [_this]
                        (TypeExtractor/getForObject (Tuple2/of "" 0))))

(defn serialiser
  "Serialiser for given kafka topic.  If there is a key in the incoming
  map, use it for partition selection, otherwise fall back on a random
  uuid."
  [^String topic]
  (reify KafkaRecordSerializationSchema
    (serialize ^ProducerRecord [_this, e, _c, _t]
      (let [om (new ObjectMapper)
            ^String key (if (contains? e :key)
                          (:key e)
                          (-> (java.util.UUID/randomUUID) (.toString)))]
        (new ProducerRecord
             topic,
             (.getBytes key)
             (.writeValueAsBytes om e))))))

(defn sink
  "Create a kafka sink for given topic."
  [^String topic]
  (-> (KafkaSink/builder)
      (.setBootstrapServers "kafka:9092")
      (.setRecordSerializer (serialiser topic))
      (.build)))

(defn source
  "Create a kafka source.  Use the passed in serde to parse json
  data."
  [^JSONKeyValueDeserializationSchema kvd]
  (-> (KafkaSource/builder)
      (.setBootstrapServers "kafka:9092")
      (.setTopics ["clicks"])
      (.setGroupId "flink")
      (.setStartingOffsets (OffsetsInitializer/earliest))
      (.setDeserializer (reify KafkaRecordDeserializationSchema
                          (open [_this c]
                            (.open kvd c))
                          (getProducedType [_this]
                            (TypeExtractor/getForClass ObjectNode))
                          (deserialize [_this, r o]
                            (.deserialize kvd r o))))
      (.build)))
