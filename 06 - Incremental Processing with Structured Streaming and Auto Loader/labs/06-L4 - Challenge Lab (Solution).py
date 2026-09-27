# Databricks notebook source
# MAGIC %md
# MAGIC # 🏆 Lab 06-L4 · Challenge Lab — Streaming & Auto Loader — ✅ SOLUTION
# MAGIC > Try the challenge yourself first! This notebook contains the reference answers and runs end-to-end.
# MAGIC
# MAGIC **Time:** ~50 min · **Compute:** Serverless · **Story:** build ShopWave's first **incremental order pipeline**:
# MAGIC files → bronze (Auto Loader) → silver (clean, deduplicated) → gold (daily revenue). Every step must be **re-runnable**
# MAGIC and process **only new data**.
# MAGIC
# MAGIC Write the code yourself. Replace every `None` / `# TODO`, then run each **✅ Check**.
# MAGIC Use `trigger(availableNow=True)` (serverless!) — the helper `run_stream(writer, "table")` does that for you — and a
# MAGIC **separate checkpoint per stream**: `checkpoint("ch_bronze")`, `checkpoint("ch_silver")`, `checkpoint("ch_gold")`.
# MAGIC
# MAGIC | Task | Skill |
# MAGIC |---|---|
# MAGIC | 1 | Auto Loader → bronze table with file metadata |
# MAGIC | 2 | Incremental re-runs |
# MAGIC | 3 | Bronze → silver: cast, filter and **deduplicate** a stream |
# MAGIC | 4 | Silver → gold: streaming aggregation with the right **output mode** |
# MAGIC | 5–8 | 🧠 Streaming concepts |

# COMMAND ----------

# MAGIC %run ../../Includes/_setup

# COMMAND ----------

# MAGIC %run ./_06_prepare

# COMMAND ----------

# DBTITLE 1,Setup for the challenge (run me - don't edit) - resets the orders feed and your tables
stop_all_streams()
for _t in ("lab06_ch_bronze", "lab06_ch_silver", "lab06_ch_gold"):
    spark.sql(f"DROP TABLE IF EXISTS {_t}")
for _c in ("ch_bronze", "ch_silver", "ch_gold"):
    if path_exists(checkpoint(_c)):
        dbutils.fs.rm(checkpoint(_c), True)
reset_orders_landing()                                   # orders-landing/ holds 01.json only (121 rows)
ORDERS_LANDING = f"{dataset_path}/orders-landing"
print("ORDERS_LANDING =", ORDERS_LANDING)

answer_task5 = answer_task6 = answer_task7 = answer_task8 = None
_bronze_1 = _bronze_3 = None


def check(label, condition):
    print(("✅ " if condition else "❌ ") + label)


def _exists(t):
    return spark.catalog.tableExists(t)


def _count(t):
    return spark.table(t).count() if _exists(t) else -1

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 1 · Bronze with Auto Loader
# MAGIC Create a stream with **Auto Loader** over `ORDERS_LANDING` (JSON) that:
# MAGIC * stores its schema in `checkpoint("ch_bronze")` and infers **real column types** (numbers, not strings),
# MAGIC * adds a column **`source_file`** (the file name),
# MAGIC * writes to **`lab06_ch_bronze`** with checkpoint `checkpoint("ch_bronze")`.
# MAGIC
# MAGIC Run it once and store the row count in `_bronze_1`.

# COMMAND ----------

# DBTITLE 1,Task 1 · SOLUTION
ch_bronze_writer = (spark.readStream.format("cloudFiles")
                         .option("cloudFiles.format", "json")
                         .option("cloudFiles.schemaLocation", checkpoint("ch_bronze"))
                         .option("cloudFiles.inferColumnTypes", "true")          # numbers instead of strings
                         .load(ORDERS_LANDING)
                         .select("*", F.col("_metadata.file_name").alias("source_file"))
                         .writeStream
                         .option("checkpointLocation", checkpoint("ch_bronze"))
                         .option("mergeSchema", "true"))
run_stream(ch_bronze_writer, "lab06_ch_bronze")
_bronze_1 = spark.table("lab06_ch_bronze").count()
print(_bronze_1)

# COMMAND ----------

# DBTITLE 1,✅ Check 1
check("121 rows after the first run", _bronze_1 == 121)
_bt = dict(spark.table("lab06_ch_bronze").dtypes) if _exists("lab06_ch_bronze") else {}
check("order_timestamp was inferred as a number (bigint)", _bt.get("order_timestamp") == "bigint")
check("source_file column exists", "source_file" in _bt)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 2 · Two more days
# MAGIC Run the next cell (files `02.json` and `03.json` arrive). Then run **the same writer** again in the Task 2 cell, e.g.
# MAGIC `run_stream(<your Task 1 writer>, "lab06_ch_bronze")` (don't recreate the table, don't touch the checkpoint), and store
# MAGIC the row count in `_bronze_3`. ⚠️ Don't re-run the Task 1 cell itself — it would overwrite `_bronze_1` and cost you Task 1.

# COMMAND ----------

# DBTITLE 1,📦 Two more files arrive (safe to re-run)
_ = land_until("orders", "03.json")

# COMMAND ----------

# DBTITLE 1,Task 2 · SOLUTION
run_stream(ch_bronze_writer, "lab06_ch_bronze")        # same writer, same checkpoint -> only 02.json and 03.json
_bronze_3 = spark.table("lab06_ch_bronze").count()
print(_bronze_3)

# COMMAND ----------

# DBTITLE 1,✅ Check 2
check("363 rows - only the new files were processed", _bronze_3 == 363)
check("3 distinct source files", _exists("lab06_ch_bronze") and
      spark.table("lab06_ch_bronze").select("source_file").distinct().count() == 3)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 3 · Silver: clean and deduplicate
# MAGIC Stream **from the table** `lab06_ch_bronze` into **`lab06_ch_silver`**:
# MAGIC * `order_ts` = `order_timestamp` converted to a TIMESTAMP (it holds Unix **seconds**),
# MAGIC * keep only orders with `quantity > 0`,
# MAGIC * remove duplicate `order_id`s **with bounded state** (hint: watermark on `order_ts`, e.g. `"1 day"`),
# MAGIC * keep the columns `order_id`, `customer_id`, `order_ts`, `quantity`, `total`.

# COMMAND ----------

# DBTITLE 1,Task 3 · SOLUTION
ch_silver_writer = (spark.readStream.table("lab06_ch_bronze")
                         .withColumn("order_ts", F.timestamp_seconds("order_timestamp"))
                         .where("quantity > 0")
                         .withWatermark("order_ts", "1 day")                     # bounds the dedup state
                         .dropDuplicatesWithinWatermark(["order_id"])
                         .select("order_id", "customer_id", "order_ts", "quantity", "total")
                         .writeStream
                         .option("checkpointLocation", checkpoint("ch_silver")))
run_stream(ch_silver_writer, "lab06_ch_silver")
# Alternative with the classic API: .dropDuplicates(["order_id", "order_ts"]) after the withWatermark.

# COMMAND ----------

# DBTITLE 1,✅ Check 3
if _exists("lab06_ch_silver"):
    _s = spark.table("lab06_ch_silver")
    _expected_silver = (spark.table("lab06_ch_bronze").where("quantity > 0").select("order_id").distinct().count())
    check("no duplicate order_id", _s.count() == _s.select("order_id").distinct().count())
    check("no cancelled orders (quantity 0)", _s.where("quantity = 0").count() == 0)
    check(f"all {_expected_silver} valid orders present", _s.count() == _expected_silver)
    check("order_ts is a timestamp", dict(_s.dtypes).get("order_ts") == "timestamp")
else:
    check("lab06_ch_silver exists", False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 4 · Gold: daily revenue
# MAGIC Stream from **`lab06_ch_silver`** into **`lab06_ch_gold`** with one row per day: `order_date` (DATE), `orders`
# MAGIC (count) and `revenue` (sum of `total`, rounded to 2 decimals). Choose an output mode that works **without** a watermark.

# COMMAND ----------

# DBTITLE 1,Task 4 · SOLUTION
ch_gold_writer = (spark.readStream.table("lab06_ch_silver")
                       .groupBy(F.to_date("order_ts").alias("order_date"))
                       .agg(F.count("*").alias("orders"), F.round(F.sum("total"), 2).alias("revenue"))
                       .writeStream
                       .outputMode("complete")                                   # aggregation without watermark
                       .option("checkpointLocation", checkpoint("ch_gold")))
run_stream(ch_gold_writer, "lab06_ch_gold")
display(spark.table("lab06_ch_gold").orderBy("order_date"))

# COMMAND ----------

# DBTITLE 1,✅ Check 4
if _exists("lab06_ch_gold") and _exists("lab06_ch_silver"):
    _g = {r["order_date"]: (r["orders"], float(r["revenue"])) for r in spark.table("lab06_ch_gold").collect()}
    _exp = {r["d"]: (r["n"], float(r["rev"])) for r in spark.table("lab06_ch_silver")
            .groupBy(F.to_date("order_ts").alias("d")).agg(F.count("*").alias("n"), F.round(F.sum("total"), 2).alias("rev")).collect()}
    check("one row per day", set(_g) == set(_exp) and len(_g) >= 3)
    check("orders and revenue match silver", all(_g[d][0] == _exp[d][0] and abs(_g[d][1] - _exp[d][1]) < 0.01 for d in _exp))
else:
    check("lab06_ch_gold exists", False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 5 · 🧠 Triggers on serverless
# MAGIC A notebook on **serverless** compute runs `df.writeStream.trigger(processingTime="5 minutes").toTable("t")`. What happens,
# MAGIC and what should the engineer use to process new data every 5 minutes?
# MAGIC
# MAGIC * **A** — It works; serverless supports every trigger
# MAGIC * **B** — It fails; use `trigger(availableNow=True)` and schedule the notebook every 5 minutes with a Lakeflow Job (or use a continuous Lakeflow pipeline)
# MAGIC * **C** — It silently falls back to `once`
# MAGIC * **D** — It fails; remove the trigger — the default trigger is supported on serverless

# COMMAND ----------

# DBTITLE 1,Task 5 · SOLUTION
answer_task5 = "B"
# Serverless supports only availableNow (and the deprecated once). The default trigger (processingTime 0) is NOT
# supported either (D). Schedule an availableNow run, or use a continuous Lakeflow pipeline for always-on streaming.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 6 · 🧠 The lost checkpoint
# MAGIC An append-mode stream from an Auto Loader source into a Delta table has run daily for a month. Someone deletes its
# MAGIC checkpoint folder (the schema location lives elsewhere). What happens on the next run?
# MAGIC
# MAGIC * **A** — Nothing: the Delta table remembers which files were loaded
# MAGIC * **B** — The stream fails until the checkpoint is restored from backup
# MAGIC * **C** — All files in the source folder are processed again → duplicates in the target table
# MAGIC * **D** — Only files that arrived after the deletion are processed

# COMMAND ----------

# DBTITLE 1,Task 6 · SOLUTION
answer_task6 = "C"
# The checkpoint is the stream's memory of processed files/offsets. Without it the stream starts from scratch and
# re-reads every file -> duplicates in an append sink. (COPY INTO tracks files in the table; Auto Loader in the checkpoint.)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 7 · 🧠 A new column in the source
# MAGIC An Auto Loader stream uses the **default** schema evolution mode. A new field appears in today's JSON files. What happens?
# MAGIC
# MAGIC * **A** — The new field is silently ignored forever
# MAGIC * **B** — The stream fails once with an `UnknownFieldException` after adding the column to the schema; restarting it continues with the new column
# MAGIC * **C** — The field is put in `_rescued_data` and the stream never fails
# MAGIC * **D** — The stream fails until someone edits the schema manually

# COMMAND ----------

# DBTITLE 1,Task 7 · SOLUTION
answer_task7 = "B"
# addNewColumns (default when the schema is inferred): add the column to the schema location, fail once, restart continues.
# C describes "rescue" mode, D "failOnNewColumns", A "none".

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 8 · 🧠 Output modes
# MAGIC A stream counts orders **per customer** (no watermark) and writes to a Delta table with `outputMode("append")`. What happens?
# MAGIC
# MAGIC * **A** — It works and appends one row per customer per trigger
# MAGIC * **B** — It works but only writes new customers
# MAGIC * **C** — It fails: aggregations without a watermark can't use append mode — use `complete` (or add a watermark / use `update` with `foreachBatch`)
# MAGIC * **D** — It fails: Delta tables don't support streaming aggregations

# COMMAND ----------

# DBTITLE 1,Task 8 · SOLUTION
answer_task8 = "C"
# Append mode needs final rows; an aggregate without a watermark is never final -> AnalysisException at start.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🏁 Final score
# MAGIC Run every **✅ Check** cell first — the score uses their results.

# COMMAND ----------

# DBTITLE 1,Score
_silver_ok = _exists("lab06_ch_silver") and (spark.table("lab06_ch_silver").count()
                                              == spark.table("lab06_ch_silver").select("order_id").distinct().count() > 0)
_final = {
    "Task 1": _bronze_1 == 121,
    "Task 2": _bronze_3 == 363,
    "Task 3": _silver_ok,
    "Task 4": _exists("lab06_ch_gold") and spark.table("lab06_ch_gold").count() >= 3,
    "Task 5": answer_task5 == "B",
    "Task 6": answer_task6 == "C",
    "Task 7": answer_task7 == "B",
    "Task 8": answer_task8 == "C",
}
for k, v in _final.items():
    print(("✅ " if v else "❌ ") + k)
print(f"\nScore: {sum(_final.values())} / {len(_final)}")
stop_all_streams()
