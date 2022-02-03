(ns daaku.sqjson
  "Treat SQLite as a JSON DB."

  (:refer-clojure :exclude [get replace count])
  (:require [clojure.string :as str]
            [jsonista.core :as j]
            [jsonista.tagged :as jt]
            [next.jdbc :as jdbc]
            [next.jdbc.result-set :as rs])
  (:import [clojure.lang Keyword PersistentHashSet]
           [daaku.ulid ULID]))

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
                           java.time.OffsetDateTime
                           {:tag "!odt"
                            :encode jt/encode-str
                            :decode #(java.time.OffsetDateTime/parse %)}
                           java.time.LocalDateTime
                           {:tag "!ldt"
                            :encode jt/encode-str
                            :decode #(java.time.LocalDateTime/parse %)}}})]}))

(defn make-options [{:keys [mapper table]
                     :or {mapper mapper table "doc"}}]
  {:migrations [(str "create table if not exists " table "(data text)")
                (str "create unique index if not exists "
                     table "_id on " table "(json_extract(data, '$.id'))")]
   :insert (str "insert into " table "(data) values(?) returning data")
   :select (str "select data from " table " where ")
   :delete-one (str "delete from " table " where rowid in (select rowid from "
                    table " where ")
   :delete-all (str "delete from " table " where ")
   :patch-one (str "update " table " set data = json_patch(data, ?) "
                   "where rowid in (select rowid from " table " where ")
   :patch-all (str "update " table " set data = json_patch(data, ?) where ")
   :replace-one (str "update " table
                     " set data = ? where rowid in (select rowid from "
                     table " where ")
   :upsert (str "insert into " table "(data) values(?) "
                "on conflict(json_extract(data, '$.id')) do update "
                "set data = excluded.data returning data")
   :count (str "select count(*) from " table " where ")
   :mapper mapper})

(def ^:dynamic *opts* (make-options {}))

(defn encode-doc [doc]
  (j/write-value-as-string doc (:mapper *opts*)))

(defn decode-doc [s]
  (j/read-value s (:mapper *opts*)))

(defn encode-sql-param [v]
  (cond
    (nil? v)     "null"
    (number? v)  (str v)
    (boolean? v) (str v)
    (string? v)  (str \' (str/replace v "'" "''") \')
    :else        (encode-sql-param (encode-doc v))))

(defn encode-path [p]
  (str "json_extract(data, '$." (name p) "')"))

(defn- join-sql [op parts]
  (if (= 1 (clojure.core/count parts))
    (first parts)
    (str "(" (str/join (str " " (name op) " ") parts) ")")))

(declare encode-where)

(defn- encode-where-map [where]
  ; FIXME is null / is not null
  (let [sql
        (reduce (fn [sql [p v]]
                  (conj! sql (str (encode-path p) "="
                                  (encode-sql-param v))))
                (transient [])
                where)]
    (join-sql :and (persistent! sql))))

(defn- encode-where-seq-join [op va]
  (let [sql
        (reduce (fn [sql where]
                  (conj! sql (encode-where where)))
                (transient [])
                va)]
    (join-sql op (persistent! sql))))

(defn- op-str [op]
  (case op
    := "="
    :> ">"
    :>= ">="
    :< "<"
    :<= "<="
    :<> "<>"
    :like " like "
    :not-like " not like "
    :in " in "
    :not-in " not in "
    :asc " asc"
    :desc " desc"
    :nulls-first " nulls first"))

(defn- encode-where-seq [[op & va :as where]]
  (cond (contains? #{:= :> :>= :< :<= :<> :like :not-like} op)
        (let [[p v] va]
          (cond (and (nil? v) (= op :=))
                (str (encode-path p) " is null")

                (and (nil? v) (= op :<>))
                (str (encode-path p) " is not null")

                :else
                (str (encode-path p) (op-str op) (encode-sql-param v))))

        (contains? #{:or :and} op)
        (encode-where-seq-join op (remove nil? va))

        (contains? #{:in :not-in} op)
        (let [[p v] va]
          (str (encode-path p) (op-str op) "("
               (str/join "," (map encode-sql-param v)) ")"))

        :else
        (throw (ex-info "unexpected where" {:where where}))))

(defn encode-where [where]
  (cond (empty? where)
        "true"

        (map? where)
        (encode-where-map where)

        (sequential? where)
        (encode-where-seq where)

        :else
        (throw (ex-info "unexpected where" {:where where}))))

(defn encode-query [where {:keys [order-by limit offset]}]
  (let [where-sql (encode-where where)]
    [(str/join " "
               (remove nil?
                       [where-sql
                        (when order-by
                          (str "order by "
                               (str/join ", " (map #(if (keyword? %)
                                                      (encode-path %)
                                                      (let [[p op] %]
                                                        (str (encode-path p)
                                                             (op-str op))))
                                                   order-by))))
                        (when limit "limit ?")
                        (when offset "offset ?")]))
     (remove nil? [limit offset])]))

(defn- add-id [doc]
  (if (contains? doc :id)
    doc
    (assoc doc :id (ULID/gen))))

(defmacro ^:private one-doc [ds params]
  `(some-> (jdbc/execute-one! ~ds ~params jdbc-opts)
           first
           decode-doc))

(defmacro ^:private one-where [ds sql-key sql-suffix where params]
  `(let [sql# (encode-where ~where)]
     (one-doc ~ds (concat [(str (~sql-key *opts*) sql# ~sql-suffix)] ~params))))

(defmacro ^:private one-val [ds sql-key where params]
  `(let [sql# (encode-where ~where)]
     (-> (jdbc/execute-one! ~ds (concat [(str (~sql-key *opts*) sql#)] ~params)
                            jdbc-opts))))

(defn insert [ds doc]
  (one-doc ds [(:insert *opts*) (-> doc add-id encode-doc)]))

(defn get [ds where]
  (one-where ds :select " limit 1" where nil))

(defn delete [ds where]
  (one-where ds :delete-one " limit 1) returning data" where nil))

(defn delete-all [ds where]
  (:next.jdbc/update-count (one-val ds :delete-all where nil)))

(defn patch [ds where patch]
  (one-where ds :patch-one " limit 1) returning data" where
             [(encode-doc patch)]))

(defn patch-all [ds where patch]
  (:next.jdbc/update-count (one-val ds :patch-all where [(encode-doc patch)])))

(defn replace [ds where doc]
  (one-where ds :replace-one " limit 1) returning data" where
             [(encode-doc doc)]))

(defn upsert [ds doc]
  (one-doc ds [(:upsert *opts*) (-> doc add-id encode-doc)]))

(defn count [ds where]
  (first (one-val ds :count where nil)))

(defn select [ds where & opts]
  (let [[sql params] (encode-query where opts)]
    (map (comp decode-doc first)
         (rest (jdbc/execute! ds (concat [(str (:select *opts*) sql)] params)
                              jdbc-opts)))))
