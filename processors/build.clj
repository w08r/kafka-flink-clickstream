(ns build
  (:require [clojure.tools.build.api :as b]))

(def lib 'w08r/processor)
(def version "0.0.0")
(def class-dir "target/classes")
(def uber-file (format "target/%s-%s-standalone.jar" (name lib) version))

;; delay to defer side effects (artifact downloads)
(def basis (delay (b/create-basis {:project "deps.edn" :aliases [:extra]})))
(def uber-basis (delay (b/create-basis {:project "deps.edn"})))

(defn clean [_]
  (b/delete {:path "target"}))

(defn uber [_]
  (clean nil)
  (b/copy-dir {:src-dirs ["src" "resources"]
               :target-dir class-dir})
  (b/compile-clj {:basis @basis
                  :ns-compile '[w08r-flink.sliding-total
                                w08r-flink.tumbling-count
                                w08r-flink.processor
                                w08r-flink.kafka]
                  :class-dir class-dir})
  (b/uber {:class-dir class-dir
           :uber-file uber-file
           :basis @uber-basis
           :main 'w08r-flink.processor}))
