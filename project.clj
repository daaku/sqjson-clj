(defproject org.clojars.daaku/sqjson "0.2.0"
  :description "Treat SQLite as a JSON DB."
  :url "https://github.com/daaku/sqjson-clj"
  :scm {:name "git" :url "https://github.com/daaku/sqjson-clj"}
  :license {:name "MIT License"}
  :dependencies [[com.github.seancorfield/next.jdbc "1.2.761"]
                 [metosin/jsonista "0.3.5"]
                 [org.clojure/clojure "1.10.3" :scope "provided"]
                 [org.xerial/sqlite-jdbc "3.36.0.3"]])
