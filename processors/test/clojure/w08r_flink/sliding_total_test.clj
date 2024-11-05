(ns w08r-flink.sliding-total-test
  (:require
   [w08r-flink.sliding-total :as st]
   [clojure.test :refer [deftest is testing]])
  (:import
   (org.apache.flink.util Collector)))

(deftest TestSlidingTotal
  (testing "Adds everything up"
    (let [out (atom {})
          col (reify Collector
                (collect [_this h]
                  (swap! out (fn [in] (merge in h)))))]
      (st/st-process nil nil [] col)
      (is (= 1 (count @out)))
      (is (= {:total 0} @out)))))
