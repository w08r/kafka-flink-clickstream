(ns w08r-flink.sliding-total
  (:gen-class
   :implements [org.apache.flink.api.java.typeutils.ResultTypeQueryable]
   :extends org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction
   :init init
   :state state
   :main false
   :prefix "st-"))

(defn st-init [])

(defn st-getProducedType [this]
  (org.apache.flink.api.java.typeutils.TypeExtractor/getForClass String))

(defn st-process [this c i o]
  (.collect o (str "total:" (count i))))
