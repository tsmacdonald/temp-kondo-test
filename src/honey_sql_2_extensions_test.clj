(ns honey-sql-2-extensions-test
  (:require
   [clojure.test :as t]
   [metabase.util.honey-sql-2-extensions :as h2x]))

(t/deftest ^:parallel sanity-test
  (t/testing "I can use the thing without complaint"
    (t/is (= (h2x/with-database-type-info 1 "int")
             (h2x/with-database-type-info 1 "INT")))))
