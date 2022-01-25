(ns daaku.sqjson
  "Treat SQLite as a JSON DB."

  (:refer-clojure :exclude [get replace])
  (:require [clojure.string :as str]
            [jsonista.core :as j]
            [jsonista.tagged :as jt]
            [next.jdbc :as jdbc])
  (:import (clojure.lang Keyword PersistentHashSet)))

(def ^:private mapper
  (j/object-mapper
   {:encode-key-fn true
    :decode-key-fn true
    :modules [(jt/module
               {:handlers {Keyword {:tag "!kw"
                                    :encode jt/encode-keyword
                                    :decode keyword}
                           PersistentHashSet {:tag "!set"
                                              :encode jt/encode-collection
                                              :decode set}
                           java.time.OffsetDateTime {:tag "!odt"
                                                     :encode jt/encode-str
                                                     :decode #(java.time.OffsetDateTime/parse %)}
                           java.time.LocalDateTime {:tag "!ldt"
                                                    :encode jt/encode-str
                                                    :decode #(java.time.LocalDateTime/parse %)}}})]}))

(defn make-options [{:keys [mapper table]
                     :or {mapper mapper table "doc"}}]
  {:kw (keyword table "data")
   :migrations [(str "create table if not exists " table "(data text)")
                (str "create unique index if not exists " table "_id on " table "(json_extract(data, '$.id'))")]
   :insert (str "insert into " table "(data) values(?) returning data")
   :select (str "select data from " table " where ")
   :delete-one (str "delete from " table " where rowid in (select rowid from " table " where ")
   :patch-one (str "update " table " set data = json_patch(data, ?) where rowid in (select rowid from " table " where ")
   :replace-one (str "update " table " set data = ? where rowid in (select rowid from " table " where ")
   :upsert (str "insert into " table "(data) values(?) on conflict(json_extract(data, '$.id')) do update set data = excluded.data returning data")
   :count (str "select count(*) as count from " table " where ")
   :mapper mapper})

(def ^:dynamic *opts* (make-options {}))

(defn encode-doc [doc]
  (j/write-value-as-string doc (:mapper *opts*)))

(defn decode-doc [s]
  (j/read-value s (:mapper *opts*)))

(defn encode-path [p]
  (str "json_extract(data, '$." (name p) "')"))

(defn encode-where-map [where]
  (let [[sql params]
        (reduce (fn [[sql params] [p v]]
                  [(conj! sql (str (encode-path p) "=?"))
                   (conj! params (if (or (string? v) (number? v))
                                   v
                                   (encode-doc v)))])
                [(transient []) (transient [])]
                where)]
    [(str/join " and " (persistent! sql)) (persistent! params)]))

(defn encode-where [where]
  (encode-where-map where))

(defn- add-id [doc]
  (if (contains? doc :id)
    doc
    (assoc doc :id (.toString (java.util.UUID/randomUUID)))))

(defn insert [ds doc]
  (->
   (jdbc/execute-one! ds [(:insert *opts*) (-> doc add-id encode-doc)])
   ((:kw *opts*))
   decode-doc))

(defn get [ds where]
  (let [[sql params] (encode-where where)]
    (some-> (jdbc/execute-one! ds (concat [(str (:select *opts*) sql " limit 1")] params))
            ((:kw *opts*))
            decode-doc)))

(defn delete [ds where]
  (let [[sql params] (encode-where where)]
    (some-> (jdbc/execute-one! ds (concat [(str (:delete-one *opts*) sql " limit 1) returning data")]
                                          params))
            ((:kw *opts*))
            decode-doc)))

(defn patch [ds where patch]
  (let [[sql params] (encode-where where)]
    (some-> (jdbc/execute-one! ds (concat [(str (:patch-one *opts*) sql " limit 1) returning data")]
                                          [(encode-doc patch)] params))
            ((:kw *opts*))
            decode-doc)))

(defn replace [ds where doc]
  (let [[sql params] (encode-where where)]
    (some-> (jdbc/execute-one! ds (concat [(str (:replace-one *opts*) sql " limit 1) returning data")]
                                          [(encode-doc doc)] params))
            ((:kw *opts*))
            decode-doc)))

(defn upsert [ds doc]
  (->
   (jdbc/execute-one! ds [(:upsert *opts*) (-> doc add-id encode-doc)])
   ((:kw *opts*))
   decode-doc))

(defn count [ds where]
  (let [[sql params] (encode-where where)]
    (-> (jdbc/execute-one! ds (concat [(str (:count *opts*) sql)] params))
        :count)))
