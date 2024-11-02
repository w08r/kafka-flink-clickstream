(ns w08r-flink.tumbling-count
  (:import
   (java.util HashMap)
   (java.lang Iterable)
   (org.apache.flink.util Collector))
  (:gen-class
   :implements [org.apache.flink.api.java.typeutils.ResultTypeQueryable]
   :extends org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction
   :main false
   :prefix "tf-"))

(set! *warn-on-reflection* true)

(defn tf-getProducedType [this]
  (org.apache.flink.api.java.typeutils.TypeExtractor/getForClass HashMap))

(defn tf-process [this k c ^Iterable i ^Collector o]
  (let [h (new HashMap 2)]
    (.put h :key k)
    (.put h :count (count i))
    (.collect o h)))
