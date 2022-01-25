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

(defn encode-sql-param [v]
  (if (or (string? v) (number? v))
    v
    (encode-doc v)))

(defn encode-path [p]
  (str "json_extract(data, '$." (name p) "')"))

(defn- join-sql [op parts]
  (if (= 1 (clojure.core/count parts))
    (first parts)
    (str "(" (str/join (str " " (name op) " ") parts) ")")))

(declare encode-where)

(defn encode-where-map [where]
  (let [[sql params]
        (reduce (fn [[sql params] [p v]]
                  [(conj! sql (str (encode-path p) "=?"))
                   (conj! params (encode-sql-param v))])
                [(transient []) (transient [])]
                where)]
    [(join-sql :and (persistent! sql)) (persistent! params)]))

(defn- encode-where-seq-join [op va]
  (let [[sql params]
        (reduce (fn [[sql params] where]
                  (let [[sql' params'] (encode-where where)]
                    [(conj! sql sql')
                     (conj! params params')]))
                [(transient []) (transient [])]
                va)]
    [(join-sql op (persistent! sql)) (apply concat (persistent! params))]))

(defn- where-bin-op [op]
  (case op
    := "="
    :> ">"
    :>= ">="
    :< "<"
    :<= "<="
    :<> "<>"
    :like " like "
    :not-like " not like "))

(defn encode-where-seq [[op & va :as where]]
  (cond (contains? #{:= :> :>= :< :<= :<> :like :not-like} op)
        (let [[p v] va]
          [(str (encode-path p) (where-bin-op op) "?") [(encode-sql-param v)]])

        (contains? #{:or :and} op)
        (encode-where-seq-join op (remove nil? va))

        :else
        (throw (ex-info "unexpected where" {:where where}))))

(defn encode-where [where]
  (cond (empty? where)
        ["true"]

        (map? where)
        (encode-where-map where)

        (sequential? where)
        (encode-where-seq where)

        :else
        (throw (ex-info "unexpected where" {:where where}))))

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
