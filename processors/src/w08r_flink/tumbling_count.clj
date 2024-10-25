(ns w08r-flink.tumbling-count
  (:gen-class
   :implements [org.apache.flink.api.java.typeutils.ResultTypeQueryable]
   :extends org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction
   :init init
   :state state
   :main false
   :prefix "tf-"))

(defn tf-init [])

(defn tf-getProducedType [this]
  (org.apache.flink.api.java.typeutils.TypeExtractor/getForClass String))

(defn tf-process [this k c i o]
  (.collect o (str k ":" (count i))))
