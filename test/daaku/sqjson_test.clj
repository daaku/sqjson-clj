(ns daaku.sqjson-test
  (:refer-clojure :exclude [replace count])
  (:require [clojure.string :as str]
            [clojure.test :refer [deftest is]]
            [daaku.sqjson :as sqjson]
            [next.jdbc :as jdbc]))

(def yoda {:name "yoda" :movie :star-wars :age 900})
(def leia {:name "leia" :movie :star-wars :age 60})

(defn- make-test-db []
  (let [ds (jdbc/get-connection (jdbc/get-datasource "jdbc:sqlite::memory:"))]
    (run! #(next.jdbc/execute! ds [%]) (:migrations sqjson/*opts*))
    ds))

(defn- where-sql [sql]
  (-> sql
      (str/replace "c1" (sqjson/encode-path "c1"))
      (str/replace "c2" (sqjson/encode-path "c2"))))

(defn- where-test [in out]
  (is (= [(where-sql (first out)) (rest out)] (sqjson/encode-where in))))

(deftest where-unexpected
  (is (thrown? RuntimeException #"unexpected where"
               (sqjson/encode-where #{:a}))))

(deftest where-map
  (where-test {:c1 1} ["c1=?" 1]))

(deftest where-map-and
  (where-test {:c1 1 :c2 2} ["(c1=? and c2=?)" 1 2]))

(deftest where-seq
  (where-test [:> :c1 1] ["c1>?" 1]))

(deftest where-seq-and
  (where-test [:and [:= :c1 1] [:> :c2 2]]
              ["(c1=? and c2>?)" 1 2]))

(deftest where-seq-or
  (where-test [:or [:= :c1 1] [:> :c2 2]]
              ["(c1=? or c2>?)" 1 2]))

(deftest unique-id-index
  (let [db (make-test-db)
        doc (sqjson/insert db yoda)]
    (is (thrown-with-msg? org.sqlite.SQLiteException #"SQLITE_CONSTRAINT_UNIQUE"
                          (sqjson/insert db doc)))))

(deftest json-mapper
  (let [db (make-test-db)
        doc {:id 1
             :keyword :keyword
             :set #{:a :b}
             :vec [1 2]
             :offset-date-time (java.time.OffsetDateTime/now)
             :local-date-time (java.time.LocalDateTime/now)}]
    (is (= doc (sqjson/insert db doc)))))

(deftest insert-get-delete-get
  (let [db (make-test-db)
        {:keys [id] :as doc} (sqjson/insert db yoda)]
    (is (some? (:id doc))
        "expect an id")
    (is (= doc (sqjson/get db {:id id}))
        "get the doc by id")
    (is (= doc (sqjson/get db {:age 900}))
        "get the doc by age")
    (is (= doc (sqjson/delete db {:id id}))
        "delete should work and return the doc")
    (is (nil? (sqjson/get db {:id id}))
        "get should now return nil")
    (is (nil? (sqjson/delete db {:id id}))
        "second delete should return nil")))

(deftest patch
  (let [db (make-test-db)
        {:keys [id] :as doc} (sqjson/insert db yoda)
        with-gender (assoc doc :gender :male)
        with-gender-age (assoc with-gender :age 950)]
    (is (= doc (sqjson/get db {:id id}))
        "start with the original doc")
    (is (= with-gender (sqjson/patch db {:id id} {:gender :male}))
        "patch includes gender")
    (is (= with-gender (sqjson/get db {:id id}))
        "get includes gender")
    (is (= with-gender-age (sqjson/patch db {:id id} {:age 950}))
        "patch includes gender-age")
    (is (= with-gender-age (sqjson/get db {:id id}))
        "get includes gender-age")))

(deftest patch-all
  (let [db (make-test-db)]
    (is (= 0 (sqjson/patch-all db {:movie :star-wars} {:release 1977})))
    (sqjson/insert db yoda)
    (sqjson/insert db leia)
    (is (= 2 (sqjson/patch-all db {:movie :star-wars} {:release 1977})))
    (is (= [1977 1977] (map :release (sqjson/select db {:movie :star-wars}))))))

(deftest replace
  (let [db (make-test-db)
        {:keys [id] :as doc} (sqjson/insert db yoda)
        leia-with-id (assoc leia :id id)]
    (is (= doc (sqjson/get db {:id id}))
        "start with the original doc")
    (is (= leia-with-id (sqjson/replace db {:id id} leia-with-id))
        "replace returns new doc")
    (is (= leia-with-id (sqjson/get db {:id id}))
        "get returns new doc")))

(deftest upsert
  (let [db (make-test-db)
        {:keys [id] :as doc} (sqjson/upsert db yoda)
        leia-with-id (assoc leia :id id)]
    (is (= doc (sqjson/get db {:id id}))
        "start with the original doc")
    (is (= leia-with-id (sqjson/upsert db leia-with-id))
        "upsert returns new doc")
    (is (= leia-with-id (sqjson/get db {:id id}))
        "get returns new doc")))

(deftest count
  (let [db (make-test-db)]
    (is (= 0 (sqjson/count db {:movie :star-wars})))
    (sqjson/insert db yoda)
    (sqjson/insert db leia)
    (is (= 2 (sqjson/count db {:movie :star-wars})))))

(deftest select
  (let [db (make-test-db)
        yoda (sqjson/insert db yoda)
        leia (sqjson/insert db leia)]
    (is (= #{yoda leia} (set (sqjson/select db {:movie :star-wars}))))))

(deftest select-empty
  (is (empty? (sqjson/select (make-test-db) {}))))

(deftest delete-all
  (let [db (make-test-db)]
    (is (= 0 (sqjson/delete-all db {})))
    (sqjson/insert db yoda)
    (sqjson/insert db leia)
    (is (= 2 (sqjson/delete-all db {})))))
