(ns w08r-flink.processor-test
  (:require
   [w08r-flink.sliding-total :as st])
  (:import
   (org.apache.flink.util Collector))
  (:use clojure.test))

(deftest TestSlidingTotal
  (testing "Adds everything up"
    (let [out (atom {})
          col (reify Collector
                (collect [this h]
                  (swap! out (fn [in] (merge in h)))))]
      (st/st-process nil nil [] col)
      (is (= 1 (count @out)))
      (is (= {:total 0} @out)))))
