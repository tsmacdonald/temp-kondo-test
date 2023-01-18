(ns honey-sql-2-extensions
  "Honey SQL 2 extensions. Used for the application database. For QP/drivers stuff,
  see [[metabase.util.honeysql-extensions]], which at the time of this writing still uses Honey SQL 1."
  (:refer-clojure
   :exclude
   [+ - / * abs mod inc dec cast concat format second])
  (:require
   [clojure.string :as str]
   [honey.sql :as sql]
   [metabase.util :as u]
   [malli :as mu]
   [metabase.util.malli.schema :as ms])
  (:import
   (java.util Locale)))

(defn- english-upper-case
  "Use this function when you need to upper-case an identifier or table name. Similar to `clojure.string/upper-case`
  but always converts the string to upper-case characters in the English locale. Using `clojure.string/upper-case` for
  table names, like we are using below in the `:h2` `honeysql.format` function can cause issues when the user has
  changed the locale to a language that has different upper-case characters. Turkish is one example, where `i` gets
  converted to `İ`. This causes the `SETTING` table to become the `SETTİNG` table, which doesn't exist."
  [^CharSequence s]
  (-> s str (.toUpperCase Locale/ENGLISH)))

(sql/register-dialect!
 :h2
 (update (sql/get-dialect :ansi) :quote (fn [quote]
                                          (comp english-upper-case quote))))

;; register the `extract` function with HoneySQL
;; (hsql/format (sql/call :extract :a :b)) -> "extract(a from b)"
(defn- format-extract [_fn [unit expr]]
  (let [[sql & args] (sql/format-expr expr)]
    (into [(str "extract(" (name unit) " from " sql ")")]
          args)))

(sql/register-fn! ::extract format-extract)

;; register the function `distinct-count` with HoneySQL
(defn- format-distinct-count
  "(sql/format-expr [::h2x/distinct-count :x])
   =>
   count(distinct x)"
  [_fn [expr]]
  (let [[sql & args] (sql/format-expr expr)]
    (into [(str "count(distinct " sql ")")]
          args)))

(sql/register-fn! ::distinct-count format-distinct-count)

;; register the function `percentile` with HoneySQL
;; (hsql/format (sql/call :percentile-cont :a 0.9)) -> "percentile_cont(0.9) within group (order by a)"
(defn- format-percentile-cont [_fn [expr p]]
  {:pre [(number? p)]}
  (let [[sql & args] (sql/format-expr expr)]
    (into [(str "PERCENTILE_CONT(" p ") within group (order by " sql ")")]
          args)))

(sql/register-fn! ::percentile-cont format-percentile-cont)

;; HoneySQL 0.7.0+ parameterizes numbers to fix issues with NaN and infinity -- see
;; https://github.com/jkk/honeysql/pull/122. However, this broke some of Metabase's behavior, specifically queries
;; with calculated columns with numeric literals -- some SQL databases can't recognize that a calculated field in a
;; SELECT clause and a GROUP BY clause is the same thing if the calculation involves parameters. Go ahead an use the
;; old behavior so we can keep our HoneySQL dependency up to date.
#_(extend-protocol honeysql.format/ToSql
    Number
    (to-sql [x] (str x)))

(def IdentifierType
  "Malli schema for valid Identifier types."
  [:enum
   :database
   :schema
   :constraint
   :index
   ;; Suppose we have a query like:
   ;; SELECT my_field f FROM my_table t
   ;; then:
   :table                               ; is `my_table`
   :table-alias                         ; is `t`
   :field                               ; is `my_field`
   :field-alias])  ; is `f`

(defn- identifier? [x]
  (and (vector? x)
       (= (first x) ::identifier)))

(defn- format-identifier [_fn [_identifier-type components]]
  ;; `:aliased` `true` => don't split dots in the middle of components
  [(str/join \. (map (fn [component]
                       (sql/format-entity component {:aliased true}))
                     components))])

(sql/register-fn! ::identifier format-identifier)

(mu/defn identifier
  "Define an identifer of type with `components`. Prefer this to using keywords for identifiers, as those do not
  properly handle identifiers with slashes in them.

  `identifier-type` represents the type of identifier in question, which is important context for some drivers, such
  as BigQuery (which needs to qualify Tables identifiers with their dataset name.)

  This function automatically unnests any Identifiers passed as arguments, removes nils, and converts all args to
  strings."
  [identifier-type :- IdentifierType & components]
  [::identifier
   identifier-type
   (vec (for [component components
              component (if (identifier? component)
                          (last component)
                          [component])
              :when     (some? component)]
          (u/qualified-name component)))])

;;; Single-quoted string literal

(defn- escape-and-quote-literal [s]
  (as-> s <>
    (str/replace <> #"(?<![\\'])'(?![\\'])"  "''")
    (str \' <> \')))

(sql/register-fn!
 ::literal
 (fn [_fn [s]]
   [(escape-and-quote-literal s)]))

(defn literal
  "Wrap keyword or string `s` in single quotes and a HoneySQL `raw` form.

  We'll try to escape single quotes in the literal, unless they're already escaped (either as `''` or as `\\`, but
  this won't handle wacky cases like three single quotes in a row.

  DON'T USE `LITERAL` FOR THINGS THAT MIGHT BE WACKY (USER INPUT). Only use it for things that are hardcoded."
  [s]
  [::literal (u/qualified-name s)])

(defn- format-at-time-zone [_fn [expr zone]]
  (let [[expr-sql & expr-args] (sql/format-expr expr)
        [zone-sql & zone-args] (sql/format-expr (literal zone))]
    (into [(clojure.core/format "(%s AT TIME ZONE %s)"
                                expr-sql
                                zone-sql)]
          cat
          [expr-args zone-args])))

(sql/register-fn!
 ::at-time-zone
 format-at-time-zone)

(defn at-time-zone
  "Create a Honey SQL form that returns `expr` at time `zone`. Does not add type info! Add appropriate DB type info
  yourself to the result."
  [expr zone]
  [::at-time-zone expr zone])

(defn- format-typed [_fn [expr _type-info]]
  (sql/format-expr expr))

(sql/register-fn! ::typed format-typed)

(def ^:private NormalizedTypeInfo
  [:map
   [:metabase.util.honeysql-extensions/database-type
    {:optional true}
    [:and
     ms/NonBlankString
     [:fn
      {:error/message "lowercased string"}
      (fn [s]
        (= s (u/lower-case-en s)))]]]])

(mu/defn ^:private normalize-type-info :- NormalizedTypeInfo
  [type-info]
  (cond-> type-info
    (:metabase.util.honeysql-extensions/database-type type-info)
    (update :metabase.util.honeysql-extensions/database-type (comp u/lower-case-en name))))

(defn type-info->db-type
  "For a given type-info, returns the `database-type`."
  [type-info]
  {:added "0.39.0"}
  (:metabase.util.honeysql-extensions/database-type type-info))

(mu/defn with-database-type-info
  {:style/indent [:form]}
  [honeysql-form db-type :- [:maybe ms/KeywordOrString]]
  (if (some? db-type)
    (with-type-info honeysql-form {:metabase.util.honeysql-extensions/database-type db-type})
    (unwrap-typed-honeysql-form honeysql-form)))
