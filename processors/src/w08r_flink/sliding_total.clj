(ns w08r-flink.sliding-total
  (:import
   (java.lang Iterable)
   (org.apache.flink.util Collector))
  (:gen-class
   :implements [org.apache.flink.api.java.typeutils.ResultTypeQueryable]
   :extends org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction
   :main false
   :prefix "st-"))

(set! *warn-on-reflection* true)

(defn st-getProducedType [this]
  (org.apache.flink.api.java.typeutils.TypeExtractor/getForClass String))

(defn st-process [this c ^Iterable i ^Collector o]
  (.collect o (str "total:" (count i))))
