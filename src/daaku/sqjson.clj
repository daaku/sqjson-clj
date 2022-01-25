(ns daaku.sqjson
  "Treat SQLite as a JSON DB."

  (:refer-clojure :exclude [get replace count])
  (:require [clojure.string :as str]
            [jsonista.core :as j]
            [jsonista.tagged :as jt]
            [next.jdbc :as jdbc]
            [next.jdbc.result-set :as rs])
  (:import (clojure.lang Keyword PersistentHashSet)))

(def ^:private jdbc-opts {:builder-fn rs/as-arrays})

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
  {:migrations [(str "create table if not exists " table "(data text)")
                (str "create unique index if not exists " table "_id on " table "(json_extract(data, '$.id'))")]
   :insert (str "insert into " table "(data) values(?) returning data")
   :select (str "select data from " table " where ")
   :delete-one (str "delete from " table " where rowid in (select rowid from " table " where ")
   :delete-all (str "delete from " table " where ")
   :patch-one (str "update " table " set data = json_patch(data, ?) where rowid in (select rowid from " table " where ")
   :patch-all (str "update " table " set data = json_patch(data, ?) where ")
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
  (if (empty? where)
    ["true"]
    (encode-where-map where)))

(defn- add-id [doc]
  (if (contains? doc :id)
    doc
    (assoc doc :id (.toString (java.util.UUID/randomUUID)))))

(defmacro ^:private one-doc [ds params]
  `(some-> (jdbc/execute-one! ~ds ~params jdbc-opts)
           first
           decode-doc))

(defmacro ^:private one-where [ds sql-key sql-suffix where params-prefix]
  `(let [[sql# params#] (encode-where ~where)]
     (one-doc ~ds (concat [(str (~sql-key *opts*) sql# ~sql-suffix)] ~params-prefix params#))))

(defmacro ^:private one-val [ds sql-key where params-prefix kw]
  `(let [[sql# params#] (encode-where ~where)]
     (-> (jdbc/execute-one! ~ds (concat [(str (~sql-key *opts*) sql#)] ~params-prefix params#))
         ~kw)))

(defn insert [ds doc]
  (one-doc ds [(:insert *opts*) (-> doc add-id encode-doc)]))

(defn get [ds where]
  (one-where ds :select " limit 1" where nil))

(defn delete [ds where]
  (one-where ds :delete-one " limit 1) returning data" where nil))

(defn delete-all [ds where]
  (one-val ds :delete-all where nil :next.jdbc/update-count))

(defn patch [ds where patch]
  (one-where ds :patch-one " limit 1) returning data" where [(encode-doc patch)]))

(defn patch-all [ds where patch]
  (one-val ds :patch-all where [(encode-doc patch)] :next.jdbc/update-count))

(defn replace [ds where doc]
  (one-where ds :replace-one " limit 1) returning data" where [(encode-doc doc)]))

(defn upsert [ds doc]
  (one-doc ds [(:upsert *opts*) (-> doc add-id encode-doc)]))

(defn count [ds where]
  (one-val ds :count where nil :count))

(defn select [ds where]
  (let [[sql params] (encode-where where)]
    (map (comp decode-doc first)
         (rest (jdbc/execute! ds (concat [(str (:select *opts*) sql)] params) jdbc-opts)))))
