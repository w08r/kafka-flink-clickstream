(ns w08r-flink.tumbling-count
  (:import
   (java.lang Iterable)
   (org.apache.flink.util Collector))
  (:gen-class
   :implements [org.apache.flink.api.java.typeutils.ResultTypeQueryable]
   :extends org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction
   :main false
   :prefix "tf-"))

(set! *warn-on-reflection* true)

(defn tf-getProducedType [this]
  (org.apache.flink.api.java.typeutils.TypeExtractor/getForClass String))

(defn tf-process [this k c ^Iterable i ^Collector o]
  (.collect o (str k ":" (count i))))
