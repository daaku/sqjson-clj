(ns daaku.sqjson-test
  (:require [clojure.test :refer [deftest is]]
            [daaku.sqjson :as sqjson]
            [next.jdbc :as jdbc]))

(def yoda {:name "yoda" :age 900})
(def leia {:name "leia" :age 60})

(defn- make-test-db []
  (let [ds (jdbc/get-connection (jdbc/get-datasource "jdbc:sqlite::memory:"))]
    (run! #(next.jdbc/execute! ds [%]) (:migrations sqjson/opts))
    ds))

(def where
  [[{:c1 1} ["c1 = ?" 1]]
   [{:c1 1 :c2 2} ["c1 = ? and c2 = ?" 1 2]]
   [[[:= :c1 1] [:> :c2 2]]
    "c1 = ? and c2 > ?" 1 2]
   [[:or [:= :c1 1] [:> :c2 2]]
    "c1 = ? and c2 > ?" 1 2]])

(deftest unique-id-index
  (let [db (make-test-db)
        doc (sqjson/insert db yoda)]
    (is (thrown-with-msg? org.sqlite.SQLiteException #"SQLITE_CONSTRAINT_UNIQUE"
                          (sqjson/insert db doc)))))

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
