# Databricks notebook source
# MAGIC %md
# MAGIC # 📘 14-CS2 · PySpark & Python Cheat Sheet for the Data Engineer Associate exam (Runnable)
# MAGIC Every Python / PySpark API the exam expects you to read and write — **one runnable cell per idea**, each starting with:
# MAGIC
# MAGIC ```
# MAGIC # WHAT : what the code does
# MAGIC # EXAM : what the exam likes to ask about it (traps, defaults, differences)
# MAGIC ```
# MAGIC
# MAGIC **How to use it** · *Run all* once (≈ 5–8 min on serverless; the streaming cells take the longest) — every cell works on
# MAGIC Free Edition — then keep it as your reference. Things that only run inside a pipeline or a job are 📖 **reference** blocks.
# MAGIC Tables are created in **`skillwave.playground`** with the prefix `py_`; the last cell drops them.
# MAGIC
# MAGIC | # | Topic | # | Topic |
# MAGIC |---|---|---|---|
# MAGIC | A | Notebook basics: magics, dbutils, widgets, task values | H | Joins, set operations |
# MAGIC | B | SparkSession, SQL from Python, catalog | I | UDFs |
# MAGIC | C | Reading data (files, tables, schemas, options) | J | Writing: modes, saveAsTable, clustering |
# MAGIC | D | Columns & rows: select, withColumn, filter, sort | K | Delta Lake Python API (DeltaTable, time travel, CDF) |
# MAGIC | E | Functions: strings, dates, NULLs, conditions | L | Structured Streaming & Auto Loader |
# MAGIC | F | Complex types: JSON, structs, arrays | M | Performance: explain, partitions, broadcast, caching |
# MAGIC | G | Aggregations, windows, deduplication | N | 📖 Declarative pipelines (Python) · SDK · clean-up |

# COMMAND ----------

# MAGIC %run ./_14_prepare

# COMMAND ----------

# DBTITLE 1,0. Setup - imports, foundation and the playground schema
# WHAT : the 3 imports you'll see in every PySpark notebook + our working schema
# EXAM : `spark` (SparkSession) and `dbutils` already exist in Databricks notebooks - no need to create them
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window

require_foundation(verbose=False)
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{CAT}`.playground COMMENT 'Cheat-sheet experiments (safe to drop)'")
spark.sql(f"USE CATALOG `{CAT}`")
spark.sql("USE SCHEMA playground")
spark.sql("CREATE VOLUME IF NOT EXISTS playground.files COMMENT 'scratch files'")
PG = f"{CAT}.playground"                                     # fully-qualified prefix for py_ tables
print("working in", PG, "· landing files in", LANDING)

# COMMAND ----------

# MAGIC %md
# MAGIC ## A · Notebook basics
# MAGIC 📖 **Magic commands** (first line of a cell): `%python` `%sql` `%scala` `%r` change the cell language · `%md` markdown ·
# MAGIC `%run ./other_notebook` runs another notebook **inline** (its variables/functions become yours) · `%pip install x` installs
# MAGIC a library for the notebook session (restarts Python) · `%sh` shell on the driver (classic compute) · `%fs ls /Volumes/...`
# MAGIC = `dbutils.fs.ls`.

# COMMAND ----------

# DBTITLE 1,A1. dbutils.fs - list, read, write, copy, remove files
# WHAT : file-system utilities for volumes (and legacy DBFS)
# EXAM : dbutils.fs.ls returns FileInfo(path, name, size, modificationTime); put(path, text, overwrite=True); rm(path, recurse=True)
files = dbutils.fs.ls(f"{LANDING}/courses")
print([(f.name, f.size) for f in files])
print(dbutils.fs.head(f"{LANDING}/courses/courses_2026.csv", 120))
dbutils.fs.put(f"/Volumes/{CAT}/playground/files/hello.txt", "hello from dbutils", True)
dbutils.fs.cp(f"/Volumes/{CAT}/playground/files/hello.txt", f"/Volumes/{CAT}/playground/files/copy/hello.txt")
dbutils.fs.rm(f"/Volumes/{CAT}/playground/files/copy", True)
# dbutils.help() / dbutils.fs.help() print the full API

# COMMAND ----------

# DBTITLE 1,A2. Widgets - notebook parameters
# WHAT : input boxes at the top of the notebook; in a JOB, task/job parameters with the same name fill them
# EXAM : dbutils.widgets.text / dropdown / combobox / multiselect, .get(name) returns a STRING; in SQL use :name
dbutils.widgets.text("country", "Saudi Arabia", "Country")
dbutils.widgets.dropdown("level", "Beginner", ["Beginner", "Intermediate", "Advanced"], "Level")
country = dbutils.widgets.get("country")
print(f"country = {country!r} · level = {dbutils.widgets.get('level')!r} · type = {type(country).__name__}")

# COMMAND ----------

# DBTITLE 1,A3. Task values - pass small values between job tasks
# WHAT : set in one task, read in a downstream task (or in an If/else condition: {{tasks.<task>.values.<key>}})
# EXAM : debugValue is returned when the notebook runs interactively (outside a job); values must be JSON-serialisable, small
try:
    dbutils.jobs.taskValues.set(key="bad_rows", value=3)
    print("read back:", dbutils.jobs.taskValues.get(taskKey="quality_check", key="bad_rows", default=0, debugValue=42))
except Exception as e:
    print("task values not available here:", _first_line(e))

# COMMAND ----------

# MAGIC %md
# MAGIC 📖 **More dbutils** — `dbutils.notebook.run("./child", 600, {"p": "1"})` runs another notebook as a separate job (returns
# MAGIC its `dbutils.notebook.exit(value)`) — unlike `%run`, variables are **not** shared · `dbutils.secrets.get(scope="kv",
# MAGIC key="db-password")` reads a secret (shown as `[REDACTED]`) · `dbutils.widgets.removeAll()`.
# MAGIC
# MAGIC ## B · SparkSession, SQL from Python, catalog

# COMMAND ----------

# DBTITLE 1,B1. spark.sql() - and named parameters
# WHAT : run any SQL from Python; returns a DataFrame (lazy for queries)
# EXAM : args={...} with :name markers is the safe way to pass values (no string concatenation -> no SQL injection)
df = spark.sql(f"SELECT student_id, plan, interests FROM json.`{LANDING}/students` "
               "WHERE plan = :plan AND size(interests) >= :n LIMIT 5",
               args={"plan": "pro", "n": 3})
display(df)

# COMMAND ----------

# DBTITLE 1,B2. Temp views - share a DataFrame with SQL
# WHAT : createOrReplaceTempView registers the DataFrame for SQL in THIS session
# EXAM : temp views disappear with the session; createOrReplaceGlobalTempView -> global_temp schema (not on serverless)
students = spark.read.json(f"{LANDING}/students")
students.createOrReplaceTempView("v_students")
display(spark.sql("SELECT plan, count(*) AS n FROM v_students GROUP BY plan"))

# COMMAND ----------

# DBTITLE 1,B3. spark.table() and the catalog API
# WHAT : read a table as a DataFrame; inspect what exists
# EXAM : spark.table("cat.schema.t") == spark.read.table(...); tableExists avoids try/except
print(spark.catalog.tableExists(f"{CAT}.silver.students"), spark.catalog.currentCatalog(), spark.catalog.currentDatabase())
print([t.name for t in spark.catalog.listTables(PG)][:10])

# COMMAND ----------

# DBTITLE 1,B4. Spark configuration
# WHAT : read/set session configs
# EXAM : spark.sql.shuffle.partitions (default 200 classic; 'auto' on serverless + AQE), autoBroadcastJoinThreshold (10 MB classic);
#        serverless allows only a few settable configs
print(spark.conf.get("spark.sql.shuffle.partitions"))
spark.conf.set("spark.sql.shuffle.partitions", "auto")

# COMMAND ----------

# MAGIC %md
# MAGIC ## C · Reading data

# COMMAND ----------

# DBTITLE 1,C1. CSV with options and an explicit schema (DDL string)
# WHAT : spark.read.format("csv").option(...).schema(...).load(path)
# EXAM : header, sep/delimiter, inferSchema (extra pass over the data!) - a schema is faster and safer;
#        mode: PERMISSIVE (default, bad -> NULL / _corrupt_record) | DROPMALFORMED | FAILFAST
courses = (spark.read.format("csv")
           .option("header", "true")
           .option("sep", ",")
           .option("mode", "PERMISSIVE")
           .schema("course_id STRING, title STRING, category STRING, level STRING, instructor_id STRING, "
                   "list_price DOUBLE, duration_hours INT, published_on DATE")
           .load(f"{LANDING}/courses/"))
courses.printSchema()
print(courses.count(), "rows")

# COMMAND ----------

# DBTITLE 1,C2. JSON with a StructType schema
# WHAT : the programmatic schema type (StructType of StructFields)
# EXAM : JSON is self-describing (schema inferred if you don't give one); multiLine for one JSON document per file
student_schema = T.StructType([
    T.StructField("student_id", T.StringType(), nullable=False),
    T.StructField("email", T.StringType()),
    T.StructField("profile", T.StringType()),
    T.StructField("plan", T.StringType()),
    T.StructField("interests", T.ArrayType(T.StringType())),
    T.StructField("signup_ts", T.TimestampType()),
])
students = spark.read.schema(student_schema).json(f"{LANDING}/students")
print(students.dtypes)

# COMMAND ----------

# DBTITLE 1,C3. Other formats + file metadata
# WHAT : parquet / text / binaryFile readers and the hidden _metadata column
# EXAM : _metadata.file_path replaces input_file_name on Unity Catalog; text -> one column "value"; wholetext per file
syllabus = (spark.read.format("text").option("wholetext", "true").load(f"{LANDING}/syllabus")
            .select(F.col("_metadata.file_name").alias("file"), F.length("value").alias("chars")))
display(syllabus.limit(3))
blobs = spark.read.format("binaryFile").load(f"{LANDING}/syllabus").select("path", "length")
print(blobs.count(), "binary files")

# COMMAND ----------

# DBTITLE 1,C4. Build a DataFrame from Python data
# WHAT : spark.createDataFrame(rows, schema) - handy for tests and lookup tables
# EXAM : the schema can be a DDL string ("a STRING, b INT") or a StructType
lookup = spark.createDataFrame([("WELCOME10", 0.10), ("SUMMER20", 0.20), ("VIP50", 0.50)],
                               "coupon STRING, discount DOUBLE")
display(lookup)

# COMMAND ----------

# MAGIC %md
# MAGIC ## D · Columns and rows

# COMMAND ----------

# DBTITLE 1,D1. select, col, alias, expr, selectExpr
# WHAT : choose / compute columns; 4 equivalent ways to reference a column: "name", F.col("name"), df.name, df["name"]
# EXAM : DataFrames are IMMUTABLE - every transformation returns a NEW DataFrame (assign it!)
df = courses.select("course_id", F.col("title"), courses.level,
                    (F.col("list_price") * 1.15).alias("price_with_vat"),
                    F.expr("upper(category) AS category_uc"))
df2 = courses.selectExpr("course_id", "list_price * 1.15 AS price_with_vat")
display(df.limit(3))

# COMMAND ----------

# DBTITLE 1,D2. withColumn, withColumns, withColumnRenamed, drop, cast
# WHAT : add/replace columns, rename, remove, change type
# EXAM : withColumn with an EXISTING name replaces that column; cast("int") / cast(T.IntegerType())
clean = (courses
         .withColumn("category", F.coalesce("category", F.lit("Uncategorized")))
         .withColumns({"price_int": F.col("list_price").cast("int"), "is_free": F.col("list_price") == 0})
         .withColumnRenamed("duration_hours", "hours")
         .drop("published_on"))
print(clean.columns)

# COMMAND ----------

# DBTITLE 1,D3. filter / where, orderBy / sort, limit, distinct
# WHAT : row operations; conditions with & | ~ and parentheses (not and/or/not)
# EXAM : filter("SQL string") also works; isin([...]), between(a, b), like/rlike, isNull/isNotNull
cheap_ai = (clean.where((F.col("category") == "AI") & (F.col("list_price") < 100))
                 .orderBy(F.col("list_price").desc(), "title")
                 .limit(5))
display(cheap_ai)
print(clean.filter("level IN ('Beginner', 'Intermediate')").select("level").distinct().count(), "levels")

# COMMAND ----------

# DBTITLE 1,D4. Inspect: show, display, printSchema, columns, dtypes, count, describe, summary
# WHAT : look at data and metadata
# EXAM : count() is an ACTION (runs a job); describe() = count/mean/stddev/min/max; summary() adds percentiles
courses.show(3, truncate=False)
print(courses.columns, courses.count())
display(courses.select("list_price", "duration_hours").summary())

# COMMAND ----------

# MAGIC %md
# MAGIC ## E · Functions: strings, dates, NULLs, conditions

# COMMAND ----------

# DBTITLE 1,E1. Strings
# WHAT : lower/upper/trim, concat_ws, split + getItem, regexp_replace/regexp_extract, substring, length
# EXAM : split(...).getItem(0) is 0-based; concat_ws ignores NULLs, concat returns NULL if any input is NULL
s = students.select(
    "student_id",
    F.lower(F.trim("email")).alias("email"),
    F.split(F.lower(F.trim("email")), "@").getItem(1).alias("domain"),
    F.regexp_extract("student_id", "S(0*)(.+)", 2).alias("number"),
    F.get_json_object("profile", "$.country").alias("country"))
display(s.limit(5))

# COMMAND ----------

# DBTITLE 1,E2. Dates and timestamps
# WHAT : convert and compute with dates
# EXAM : from_unixtime(epoch_seconds) / to_timestamp / to_date / date_format / date_trunc / datediff / date_add
d = students.select(
    "signup_ts",
    F.to_date("signup_ts").alias("signup_date"),
    F.date_format("signup_ts", "yyyy-MM").alias("month"),
    F.datediff(F.current_date(), F.col("signup_ts")).alias("days_since_signup"),
    F.date_trunc("week", "signup_ts").alias("week_start"),
    F.unix_timestamp("signup_ts").alias("epoch"),
    F.from_unixtime(F.unix_timestamp("signup_ts")).alias("back_to_string"))
display(d.limit(3))

# COMMAND ----------

# DBTITLE 1,E3. NULL handling - fillna / na.fill, dropna / na.drop, coalesce, isNull
# WHAT : replace or remove missing values
# EXAM : fillna({"col": value}) per column; dropna(how="any"|"all", subset=[...]); fillna only fills matching types
print("missing e-mails:", students.where(F.col("email").isNull()).count())
filled = students.fillna({"email": "unknown@skillwave.academy", "plan": "free"})
no_email_dropped = students.dropna(subset=["email"])
print(filled.where("email IS NULL").count(), no_email_dropped.count())

# COMMAND ----------

# DBTITLE 1,E4. Conditions - when / otherwise
# WHAT : CASE WHEN in the DataFrame API
# EXAM : without .otherwise() unmatched rows get NULL
tiers = courses.withColumn("tier", F.when(F.col("list_price") < 50, "budget")
                                    .when(F.col("list_price") < 120, "standard")
                                    .otherwise("premium"))
display(tiers.groupBy("tier").count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## F · Complex types: JSON strings, structs, arrays

# COMMAND ----------

# DBTITLE 1,F1. JSON string -> struct with from_json (+ schema_of_json)
# WHAT : parse once, then use dot notation; "struct.*" expands all fields
# EXAM : from_json needs a schema (DDL, StructType, or schema_of_json(sample)); get_json_object for one field
profile_schema = "first_name STRING, last_name STRING, country STRING, city STRING, birth_year INT"
parsed = (students.withColumn("p", F.from_json("profile", profile_schema))
                  .select("student_id", "p.*", F.col("p.country").alias("country_again")))
display(parsed.limit(3))
print(spark.range(1).select(F.schema_of_json(F.lit('{"a": 1, "b": ["x"]}'))).first()[0])

# COMMAND ----------

# DBTITLE 1,F2. explode / explode_outer / posexplode, size, array_contains
# WHAT : one row per array element (explode) and array helpers
# EXAM : explode drops rows with NULL/empty arrays; explode_outer keeps them (NULL element)
exploded = students.select("student_id", F.explode("interests").alias("interest"))
print("rows after explode:", exploded.count(), "· students:", students.count())
display(students.select("student_id", F.size("interests").alias("n"),
                        F.array_contains("interests", "ai").alias("likes_ai")).limit(3))

# COMMAND ----------

# DBTITLE 1,F3. collect_list / collect_set, array_distinct, flatten, struct, to_json
# WHAT : the reverse of explode: aggregate rows into arrays; build structs and JSON
# EXAM : collect_set removes duplicates, collect_list keeps them (order not guaranteed)
by_country = (parsed.join(students.select("student_id", "interests"), "student_id")
              .groupBy("country")
              .agg(F.collect_set("city").alias("cities"),
                   F.array_distinct(F.flatten(F.collect_list("interests"))).alias("all_interests"),
                   F.to_json(F.struct(F.count("*").alias("n"), F.max("birth_year").alias("youngest_birth_year"))).alias("stats")))
display(by_country)

# COMMAND ----------

# DBTITLE 1,F4. Higher-order functions in Python
# WHAT : F.transform / F.filter / F.exists / F.aggregate take a Python lambda on Columns
# EXAM : same as SQL TRANSFORM/FILTER/EXISTS/AGGREGATE - no explode needed
display(students.select("interests",
                        F.transform("interests", lambda x: F.upper(x)).alias("upper"),
                        F.filter("interests", lambda x: x != "sql").alias("no_sql"),
                        F.exists("interests", lambda x: x == "ai").alias("likes_ai")).limit(3))

# COMMAND ----------

# MAGIC %md
# MAGIC ## G · Aggregations, windows, deduplication

# COMMAND ----------

# DBTITLE 1,G1. groupBy + agg (many aggregates at once)
# WHAT : group and aggregate; alias every result
# EXAM : countDistinct (exact) vs approx_count_distinct (fast, ~5% error); agg after groupBy; avg == mean
enr = spark.read.json(f"{ENROLL_STAGING}/enrollments_2026-09-01.json")
summary = (enr.groupBy("channel")
              .agg(F.count("*").alias("enrollments"),
                   F.round(F.sum("price_paid"), 2).alias("revenue"),
                   F.round(F.avg("price_paid"), 2).alias("avg_price"),
                   F.countDistinct("student_id").alias("students"),
                   F.approx_count_distinct("student_id").alias("students_approx"),
                   F.sum(F.when(F.col("coupon").isNotNull(), 1).otherwise(0)).alias("with_coupon"))
              .orderBy(F.desc("revenue")))
display(summary)

# COMMAND ----------

# DBTITLE 1,G2. pivot, rollup
# WHAT : pivot = values of a column become columns; rollup = subtotals
# EXAM : giving the pivot values explicitly avoids an extra job to find them
display(enr.groupBy("status").pivot("channel", ["web", "mobile", "partner"]).count())
display(enr.rollup("channel", "status").count().orderBy("channel", "status"))

# COMMAND ----------

# DBTITLE 1,G3. Window functions
# WHAT : Window.partitionBy(...).orderBy(...) + row_number / rank / dense_rank / lag / lead / sum over
# EXAM : rowsBetween(Window.unboundedPreceding, Window.currentRow) = running total
w = Window.partitionBy("course_id").orderBy("enrolled_at")
ranked = (enr.withColumn("n_in_course", F.row_number().over(w))
             .withColumn("prev_price", F.lag("price_paid").over(w))
             .withColumn("running_revenue",
                         F.sum("price_paid").over(w.rowsBetween(Window.unboundedPreceding, Window.currentRow)))
             .withColumn("price_rank", F.dense_rank().over(Window.partitionBy("course_id").orderBy(F.desc("price_paid")))))
display(ranked.where("course_id = 'C101'").select("enrolled_at", "price_paid", "n_in_course", "prev_price",
                                                  "running_revenue", "price_rank").limit(5))

# COMMAND ----------

# DBTITLE 1,G4. Deduplication - distinct, dropDuplicates(subset), latest per key
# WHAT : three levels of dedup
# EXAM : distinct() = all columns; dropDuplicates(["id"]) keeps ONE ARBITRARY row per id; for "the latest" use row_number
print("rows:", enr.count(), "· distinct:", enr.distinct().count(), "· unique ids:", enr.dropDuplicates(["enrollment_id"]).count())
updates = spark.read.json(f"{LANDING}/student-updates")
latest = (updates.withColumn("rn", F.row_number().over(Window.partitionBy("student_id").orderBy(F.desc("updated_ts"))))
                 .where("rn = 1").drop("rn"))
print("latest version per student:", latest.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## H · Joins and set operations

# COMMAND ----------

# DBTITLE 1,H1. All join types
# WHAT : df.join(other, on, how) with how = inner | left | right | full | left_semi | left_anti | cross
# EXAM : on="key" (same name, single column in result) vs a condition (keeps both columns);
#        left_anti = rows WITHOUT a match (orphans), left_semi = rows WITH a match (no right columns)
#        WATCH the fan-out: C120 is twice in the CSV, so inner (154) > 150 enrollments - dedup dimensions first!
c = courses.select("course_id", "title", "category")
for how in ("inner", "left", "right", "full", "left_semi", "left_anti"):
    print(f"{how:<10}", enr.join(c, on="course_id", how=how).count())
print("cross     ", c.crossJoin(lookup).count(), "= 21 x 3")

# COMMAND ----------

# DBTITLE 1,H2. Join on different column names / several keys, and broadcast
# WHAT : condition joins; F.broadcast(small_df) forces a broadcast hash join (no shuffle of the big side)
# EXAM : auto-broadcast below spark.sql.autoBroadcastJoinThreshold; avoid ambiguous columns after condition joins
lk = lookup.withColumnRenamed("coupon", "code")
priced = enr.join(F.broadcast(lk), enr.coupon == lk.code, "left").drop("code")
multi = enr.alias("a").join(enr.alias("b"),
                            (F.col("a.student_id") == F.col("b.student_id")) & (F.col("a.course_id") != F.col("b.course_id")))
print(priced.where("discount IS NOT NULL").count(), "enrollments with a known coupon ·", multi.count(), "pairs")

# COMMAND ----------

# DBTITLE 1,H3. union / unionByName / intersect / subtract / exceptAll
# WHAT : combine DataFrames
# EXAM : union() matches by POSITION and keeps duplicates (= UNION ALL); unionByName matches by NAME
#        (allowMissingColumns=True fills NULLs); subtract = EXCEPT DISTINCT; exceptAll keeps duplicates
day1 = spark.read.json(f"{ENROLL_STAGING}/enrollments_2026-09-01.json")
day4 = spark.read.json(f"{ENROLL_STAGING}/enrollments_2026-09-04.json")      # has the extra column referrer
both = day1.unionByName(day4, allowMissingColumns=True)
print(both.count(), "rows ·", "referrer" in both.columns)
ids1, ids_all = day1.select("student_id"), both.select("student_id")
print("intersect:", ids1.intersect(ids_all).count(), "· subtract:", ids_all.subtract(ids1).count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## I · UDFs

# COMMAND ----------

# DBTITLE 1,I1. Python UDF, pandas UDF, and registering for SQL
# WHAT : custom Python logic on columns
# EXAM : Python UDFs are SLOW (rows serialized to Python, no Catalyst optimization) - prefer built-in functions or
#        SQL UDFs; pandas_udf (vectorized, Arrow) is much faster than a row-at-a-time udf
@F.udf(returnType="string")
def initials(first, last):
    return (first or "?")[0] + (last or "?")[0]


import pandas as pd


@F.pandas_udf("double")
def with_vat(price: pd.Series) -> pd.Series:
    return price * 1.15


spark.udf.register("sql_initials", initials)                 # now usable in SQL too
display(parsed.select("first_name", "last_name", initials("first_name", "last_name").alias("ini")).limit(3))
display(courses.select("title", with_vat("list_price").alias("with_vat")).limit(3))
display(spark.sql("SELECT sql_initials('Amira', 'Haddad') AS ini"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## J · Writing data

# COMMAND ----------

# DBTITLE 1,J1. saveAsTable with save modes
# WHAT : write a DataFrame as a managed Delta table
# EXAM : modes: errorifexists (DEFAULT) | append | overwrite | ignore; overwrite replaces data (schema change needs
#        .option("overwriteSchema", "true")); append with new columns needs .option("mergeSchema", "true")
clean.write.mode("overwrite").saveAsTable(f"{PG}.py_courses")
clean.limit(2).write.mode("append").saveAsTable(f"{PG}.py_courses")
clean.write.mode("ignore").saveAsTable(f"{PG}.py_courses")                    # table exists -> silently does nothing
try:
    clean.write.saveAsTable(f"{PG}.py_courses")                               # default mode = error if exists
except Exception as e:
    print("❌ expected:", _first_line(e)[:100])
print(spark.table(f"{PG}.py_courses").count(), "rows (21 + 2 + 0)")

# COMMAND ----------

# DBTITLE 1,J2. Schema evolution on write - mergeSchema / overwriteSchema
# WHAT : allow new columns (mergeSchema) or a completely new schema (overwriteSchema with overwrite)
# EXAM : without mergeSchema an append with an extra column FAILS (schema enforcement)
extra = clean.limit(1).withColumn("language", F.lit("en"))
try:
    extra.write.mode("append").saveAsTable(f"{PG}.py_courses")
except Exception as e:
    print("❌ schema enforcement:", _first_line(e)[:110])
extra.write.mode("append").option("mergeSchema", "true").saveAsTable(f"{PG}.py_courses")
print(spark.table(f"{PG}.py_courses").columns[-1])

# COMMAND ----------

# DBTITLE 1,J3. insertInto, writeTo (DataFrameWriterV2) with liquid clustering, partitionBy
# WHAT : insertInto = INSERT by POSITION into an existing table; writeTo().clusterBy() creates a liquid-clustered table
# EXAM : partitionBy("col") creates Hive-style partitions (only for big tables + low-cardinality columns)
(spark.table(f"{PG}.py_courses").limit(1).write.insertInto(f"{PG}.py_courses"))
try:
    enr.writeTo(f"{PG}.py_enrollments").using("delta").clusterBy("course_id").createOrReplace()
except Exception as e:                                                 # older runtimes: plain table + ALTER
    print("writeTo/clusterBy not available:", _first_line(e)[:90])
    enr.write.mode("overwrite").saveAsTable(f"{PG}.py_enrollments")
    spark.sql(f"ALTER TABLE {PG}.py_enrollments CLUSTER BY (course_id)")
print(spark.sql(f"DESCRIBE DETAIL {PG}.py_enrollments").select("clusteringColumns").first()[0])

# COMMAND ----------

# MAGIC %md
# MAGIC ## K · Delta Lake Python API

# COMMAND ----------

# DBTITLE 1,K1. DeltaTable - history, detail, update, delete
# WHAT : programmatic access to a Delta table's operations
# EXAM : DeltaTable.forName(spark, "cat.schema.t") or forPath(spark, path); history() returns a DataFrame
from delta.tables import DeltaTable

dt = DeltaTable.forName(spark, f"{PG}.py_courses")
dt.update(condition="category = 'AI'", set={"list_price": "list_price * 0.9"})
dt.delete("course_id = 'C120' AND hours > 100")                       # deletes nothing - just syntax
display(dt.history().select("version", "operation", "operationMetrics").limit(5))

# COMMAND ----------

# DBTITLE 1,K2. MERGE in Python - upsert and insert-only
# WHAT : merge(source, condition).whenMatchedUpdate[All]().whenNotMatchedInsert[All]().execute()
# EXAM : same semantics as SQL MERGE; dedup the source first (one source row per target row)
src = spark.createDataFrame([("C101", "Lakehouse Fundamentals (2nd ed.)", 59.0), ("C130", "Python Jobs in Practice", 69.0)],
                            "course_id STRING, title STRING, list_price DOUBLE")
(dt.alias("t").merge(src.alias("s"), "t.course_id = s.course_id")
   .whenMatchedUpdate(set={"title": "s.title", "list_price": "s.list_price"})
   .whenNotMatchedInsert(values={"course_id": "s.course_id", "title": "s.title", "list_price": "s.list_price"})
   .execute())
display(spark.table(f"{PG}.py_courses").where("course_id IN ('C101', 'C130')"))

# COMMAND ----------

# DBTITLE 1,K3. Time travel and restore in Python
# WHAT : read an old version with versionAsOf / timestampAsOf; roll back with restoreToVersion
# EXAM : spark.read.option("versionAsOf", 0).table(...) == SELECT ... VERSION AS OF 0
v0 = spark.read.option("versionAsOf", 0).table(f"{PG}.py_courses")
print("version 0:", v0.count(), "rows · now:", spark.table(f"{PG}.py_courses").count())
last = dt.history().agg(F.max("version")).first()[0]
dt.restoreToVersion(last - 1)                                          # undo the MERGE
print("after restore:", spark.table(f"{PG}.py_courses").where("course_id = 'C130'").count(), "row(s) for C130")

# COMMAND ----------

# DBTITLE 1,K4. Change Data Feed - read the row-level changes
# WHAT : with delta.enableChangeDataFeed = true, Delta records inserts/updates/deletes (_change_type, _commit_version)
# EXAM : read with .option("readChangeFeed", "true").option("startingVersion", n); SQL: table_changes('t', n)
spark.sql(f"ALTER TABLE {PG}.py_courses SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")
start = dt.history().agg(F.max("version")).first()[0] + 1
dt.update(condition="course_id = 'C101'", set={"list_price": "49.0"})
changes = (spark.read.option("readChangeFeed", "true").option("startingVersion", start)
           .table(f"{PG}.py_courses").select("course_id", "list_price", "_change_type", "_commit_version"))
display(changes)

# COMMAND ----------

# DBTITLE 1,K5. OPTIMIZE and VACUUM from Python
# WHAT : compaction (and clustering / Z-order) and cleanup of old files
# EXAM : optimize().executeZOrderBy("col") for non-clustered tables; vacuum() default retention 7 days
display(dt.optimize().executeCompaction())
print("vacuum (dry run in SQL):", spark.sql(f"VACUUM {PG}.py_courses DRY RUN").count(), "files would be deleted")

# COMMAND ----------

# MAGIC %md
# MAGIC ## L · Structured Streaming and Auto Loader

# COMMAND ----------

# DBTITLE 1,L1. Auto Loader - incremental file ingestion
# WHAT : readStream.format("cloudFiles") discovers only NEW files; checkpoint + schemaLocation make it restartable
# EXAM : cloudFiles.format, cloudFiles.schemaLocation (required for inference/evolution), cloudFiles.inferColumnTypes,
#        cloudFiles.schemaHints, cloudFiles.schemaEvolutionMode (addNewColumns default | rescue | failOnNewColumns | none),
#        _rescued_data column; trigger(availableNow=True) = process everything new, then stop
cp = f"/Volumes/{CAT}/playground/files/_checkpoints"
q = (spark.readStream.format("cloudFiles")
     .option("cloudFiles.format", "json")
     .option("cloudFiles.schemaLocation", f"{cp}/py_bronze_enrollments/_schema")
     .option("cloudFiles.inferColumnTypes", "true")
     .option("cloudFiles.schemaHints", "price_paid DECIMAL(10,2)")
     .option("cloudFiles.schemaEvolutionMode", "rescue")              # never stop: unknown fields -> _rescued_data
     .load(f"{ENROLL_STAGING}")
     .withColumn("source_file", F.col("_metadata.file_name"))
     .writeStream
     .option("checkpointLocation", f"{cp}/py_bronze_enrollments")
     .trigger(availableNow=True)
     .toTable(f"{PG}.py_bronze_enrollments"))
q.awaitTermination()
print(spark.table(f"{PG}.py_bronze_enrollments").count(), "rows ·", q.lastProgress["numInputRows"] if q.lastProgress else 0,
      "in the last micro-batch")

# COMMAND ----------

# DBTITLE 1,L2. Stream from a Delta table + streaming query handle
# WHAT : spark.readStream.table() reads a Delta table incrementally (new appends only)
# EXAM : outputMode append (default) | complete (whole result each time, aggregations) | update;
#        one checkpoint per query (never share); query.status / lastProgress / stop(); spark.streams.active
q2 = (spark.readStream.table(f"{PG}.py_bronze_enrollments")
      .where("price_paid >= 0")
      .select("enrollment_id", "course_id", "price_paid", "source_file")
      .writeStream
      .outputMode("append")
      .option("checkpointLocation", f"{cp}/py_silver_enrollments")
      .trigger(availableNow=True)
      .toTable(f"{PG}.py_silver_enrollments"))
q2.awaitTermination()
print("active streams:", len(spark.streams.active), "· silver rows:", spark.table(f"{PG}.py_silver_enrollments").count())

# COMMAND ----------

# DBTITLE 1,L3. Event-time windows + watermark (stateful aggregation)
# WHAT : count per 1-hour window of event time; the watermark bounds state and decides when late data is dropped
# EXAM : withWatermark("ts", "10 minutes") + groupBy(F.window("ts", "1 hour")); append mode emits a window only after
#        the watermark passes its end; complete mode rewrites the whole result
q3 = (spark.readStream.table(f"{PG}.py_bronze_enrollments")
      .withColumn("ts", F.col("enrolled_at").cast("timestamp"))
      .withWatermark("ts", "10 minutes")
      .groupBy(F.window("ts", "1 hour"), "channel")
      .agg(F.count("*").alias("enrollments"))
      .writeStream
      .outputMode("complete")
      .option("checkpointLocation", f"{cp}/py_hourly")
      .trigger(availableNow=True)
      .toTable(f"{PG}.py_hourly_enrollments"))
q3.awaitTermination()
display(spark.table(f"{PG}.py_hourly_enrollments").orderBy("window").limit(5))

# COMMAND ----------

# DBTITLE 1,L4. foreachBatch - run batch code (e.g. MERGE) on every micro-batch
# WHAT : the way to upsert from a stream (a Delta sink itself only appends / completes)
# EXAM : function(batch_df, batch_id); use fully-qualified names and batch_df.sparkSession inside
spark.sql(f"CREATE TABLE IF NOT EXISTS {PG}.py_latest_by_course (course_id STRING, last_price DECIMAL(10,2))")


def upsert_latest(batch_df, batch_id):
    latest_rows = (batch_df.groupBy("course_id").agg(F.max("price_paid").alias("last_price")))
    (DeltaTable.forName(batch_df.sparkSession, f"{PG}.py_latest_by_course").alias("t")
        .merge(latest_rows.alias("s"), "t.course_id = s.course_id")
        .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())


q4 = (spark.readStream.table(f"{PG}.py_silver_enrollments")
      .writeStream.foreachBatch(upsert_latest)
      .option("checkpointLocation", f"{cp}/py_latest_by_course")
      .trigger(availableNow=True).start())
q4.awaitTermination()
print(spark.table(f"{PG}.py_latest_by_course").count(), "courses upserted")

# COMMAND ----------

# MAGIC %md
# MAGIC 📖 **Triggers** — *(default)* micro-batch as soon as the previous one ends · `processingTime="1 minute"` fixed interval ·
# MAGIC `availableNow=True` all available data in several batches, then stop · `once=True` (deprecated) one batch then stop ·
# MAGIC continuous (experimental). Serverless supports only **availableNow** (and `once`). Exactly-once = replayable source +
# MAGIC checkpoint (write-ahead log) + idempotent sink. Kafka source: `spark.readStream.format("kafka").option("kafka.bootstrap.servers", …).option("subscribe", "topic")`.
# MAGIC
# MAGIC ## M · Performance

# COMMAND ----------

# DBTITLE 1,M1. explain() - read the plan
# WHAT : the logical/physical plan Spark will run (lazy evaluation: nothing runs until an ACTION)
# EXAM : transformations (select, filter, join...) are lazy; actions (count, collect, show, write) trigger jobs;
#        narrow (filter, select) vs wide (groupBy, join, distinct -> shuffle = "Exchange" in the plan)
joined = enr.join(F.broadcast(c), "course_id").groupBy("category").agg(F.sum("price_paid"))
joined.explain(mode="formatted")

# COMMAND ----------

# DBTITLE 1,M2. Partitions - repartition vs coalesce
# WHAT : repartition(n[, cols]) = full shuffle to n partitions (can increase); coalesce(n) = merge partitions, no shuffle
# EXAM : coalesce only DECREASES; repartition("col") hash-partitions by a column (helps joins/writes by that key)
r = enr.repartition(8, "course_id")
print("after repartition:", r.select(F.spark_partition_id().alias("p")).distinct().count(), "non-empty partitions")
print("after coalesce(2):", r.coalesce(2).select(F.spark_partition_id().alias("p")).distinct().count())

# COMMAND ----------

# DBTITLE 1,M3. Caching (classic compute) - persist / unpersist
# WHAT : keep a DataFrame in memory/disk for reuse across actions
# EXAM : cache() = persist(MEMORY_AND_DISK) for DataFrames; lazy (filled by the next action); unpersist() frees it;
#        NOT supported on serverless compute (the disk cache works automatically there)
try:
    enr.cache()
    enr.count()
    enr.unpersist()
    print("cached and released")
except Exception as e:
    print("caching not supported here (serverless):", _first_line(e)[:80])

# COMMAND ----------

# MAGIC %md
# MAGIC 📖 **Diagnosing slow jobs** (exam domain 6) — **skew**: one task much longer than the others in a stage (fix: AQE skew
# MAGIC join `spark.sql.adaptive.skewJoin.enabled`, salting, better keys) · **spill**: data written to disk because partitions don't
# MAGIC fit memory (fix: more memory/smaller partitions) · **shuffle**: big Exchange stages (fix: broadcast small tables, filter
# MAGIC early, cluster data) · **small files**: OPTIMIZE / auto compaction / liquid clustering · Look in the **Spark UI** (classic)
# MAGIC or the **query profile** (serverless/SQL warehouses) and the job **run history**.
# MAGIC
# MAGIC ## N · 📖 Declarative pipelines in Python · Databricks SDK
# MAGIC
# MAGIC ```python
# MAGIC from pyspark import pipelines as dp                          # Lakeflow Spark Declarative Pipelines (formerly `import dlt`)
# MAGIC from pyspark.sql import functions as F
# MAGIC
# MAGIC @dp.table(comment="raw orders")                              # streaming DataFrame -> STREAMING TABLE
# MAGIC def bronze_orders():
# MAGIC     return (spark.readStream.format("cloudFiles")
# MAGIC             .option("cloudFiles.format", "json")
# MAGIC             .load(spark.conf.get("landing_path") + "/orders"))      # pipeline configuration value
# MAGIC
# MAGIC @dp.table
# MAGIC @dp.expect_or_fail("valid_id", "order_id IS NOT NULL")       # fail the update
# MAGIC @dp.expect_or_drop("valid_price", "price >= 0")             # drop the row
# MAGIC @dp.expect("has_email", "email IS NOT NULL")                 # warn only (counted in metrics)
# MAGIC def silver_orders():
# MAGIC     return spark.readStream.table("bronze_orders")
# MAGIC
# MAGIC @dp.materialized_view                                        # batch DataFrame -> MATERIALIZED VIEW
# MAGIC def gold_daily():
# MAGIC     return spark.read.table("silver_orders").groupBy(F.to_date("order_ts").alias("day")).agg(F.sum("price"))
# MAGIC
# MAGIC @dp.temporary_view                                           # only visible inside the pipeline
# MAGIC def v_valid():
# MAGIC     return spark.read.table("silver_orders").where("price > 0")
# MAGIC
# MAGIC dp.create_streaming_table("customers")                       # AUTO CDC (formerly apply_changes)
# MAGIC dp.create_auto_cdc_flow(target="customers", source="customers_cdc_feed", keys=["customer_id"],
# MAGIC                         sequence_by="event_ts", apply_as_deletes=F.expr("operation = 'DELETE'"),
# MAGIC                         except_column_list=["operation", "event_ts"], stored_as_scd_type=2)
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,N1. Databricks SDK - the REST API from Python
# WHAT : WorkspaceClient() authenticates automatically inside a notebook; every UI action is an API call
# EXAM : jobs/pipelines can be managed as code (SDK, REST, CLI, Declarative Automation Bundles)
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
print("me:", w.current_user.me().user_name)
for j in list(w.jobs.list(limit=5))[:5]:
    print("job:", j.job_id, j.settings.name if j.settings else "")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧹 Clean-up

# COMMAND ----------

# DBTITLE 1,Stop streams, drop the py_ tables (optional)
stop_all_streams()
DROP_PY_TABLES = False                # set True to remove what this notebook created
if DROP_PY_TABLES:
    for t in spark.catalog.listTables(PG):
        if t.name.startswith("py_"):
            spark.sql(f"DROP TABLE IF EXISTS {PG}.{t.name}")
    dbutils.fs.rm(f"/Volumes/{CAT}/playground/files/_checkpoints", True)
    dbutils.widgets.removeAll()
    print("🧹 py_ tables and checkpoints removed")
else:
    print("Kept the py_ tables - set DROP_PY_TABLES = True and re-run to remove them.")
