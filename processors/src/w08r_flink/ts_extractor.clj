(ns w08r-flink.ts-extractor
  (:import
   (java.lang Iterable)
   (org.apache.flink.util Collector))
  (:gen-class
   :extends org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
   :main false
   :prefix "tse-"))

(defn tse-extractTimestamp [this, e]
  (.toEpochMilli (.getField 2 e)))
