# Databricks notebook source
# MAGIC %md
# MAGIC # 🧪 Lab 06-L3 · Windows, Watermarks & `foreachBatch` (Guided)
# MAGIC **Time:** ~50 min · **Compute:** Serverless (Free Edition) — `trigger(availableNow=True)` everywhere
# MAGIC
# MAGIC Page clicks arrive in 4 files, each covering two hours of **event time**. The app re-sends a few clicks (duplicates), and
# MAGIC one file contains clicks that are **6–8 hours late**. You'll count clicks per 30-minute window with and without a
# MAGIC **watermark**, remove duplicates, and upsert per-user totals with **`foreachBatch` + `MERGE`**.
# MAGIC
# MAGIC | Part | You will… |
# MAGIC |---|---|
# MAGIC | 1 | Read the clicks stream and define the four queries |
# MAGIC | 2 | Deliver the 4 files one by one and run all queries after each delivery |
# MAGIC | 3 | Compare windowed counts: **complete mode, no watermark** vs **append mode + watermark** |
# MAGIC | 4 | Check **deduplication** with `dropDuplicatesWithinWatermark` |
# MAGIC | 5 | Check the **`foreachBatch` + `MERGE`** upsert |
# MAGIC | 6 | Stream-stream joins, Kafka and other things to know |
# MAGIC | 7 | ✅ Automatic checks |

# COMMAND ----------

# MAGIC %run ../../Includes/_setup

# COMMAND ----------

# MAGIC %run ./_06_prepare

# COMMAND ----------

# DBTITLE 1,Start clean (safe to re-run)
stop_all_streams()
for t in ("lab06_clicks_complete", "lab06_clicks_append", "lab06_clicks_dedup", "lab06_user_clicks"):
    spark.sql(f"DROP TABLE IF EXISTS {t}")
for c in ("clicks_complete", "clicks_append", "clicks_dedup", "user_clicks"):
    if path_exists(checkpoint(c)):
        dbutils.fs.rm(checkpoint(c), True)
reset_clicks_landing()                                  # clicks-landing/ holds clicks_01.json (10:00-11:57)
clicks_landing = f"{dataset_path}/clicks-landing"
print(dbutils.fs.head(f"{clicks_landing}/clicks_01.json", 200))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1 · One source, four queries
# MAGIC **Event time** = when the click happened (`event_time` in the data). **Processing time** = when Spark sees it. Windows and
# MAGIC watermarks work on **event time**, so late or out-of-order data lands in the right window.

# COMMAND ----------

# DBTITLE 1,The source stream (explicit schema, event_time as TIMESTAMP)
CLICK_SCHEMA = "click_id STRING, user_id STRING, page STRING, event_time TIMESTAMP"
clicks = spark.readStream.schema(CLICK_SCHEMA).json(clicks_landing)

# COMMAND ----------

# DBTITLE 1,Query A - 30-minute tumbling windows, NO watermark, complete mode
counts_complete = (clicks.groupBy(F.window("event_time", "30 minutes").alias("w"))
                         .agg(F.count("*").alias("clicks"))
                         .select(F.col("w.start").alias("window_start"), F.col("w.end").alias("window_end"), "clicks"))
writer_complete = (counts_complete.writeStream.outputMode("complete")
                   .option("checkpointLocation", checkpoint("clicks_complete")))

# COMMAND ----------

# DBTITLE 1,Query B - same windows WITH a 1-hour watermark, append mode
counts_append = (clicks.withWatermark("event_time", "1 hour")
                       .groupBy(F.window("event_time", "30 minutes").alias("w"))
                       .agg(F.count("*").alias("clicks"))
                       .select(F.col("w.start").alias("window_start"), F.col("w.end").alias("window_end"), "clicks"))
writer_append = (counts_append.writeStream.outputMode("append")
                 .option("checkpointLocation", checkpoint("clicks_append")))

# COMMAND ----------

# MAGIC %md
# MAGIC **Watermark = max event time seen − threshold.** With `withWatermark("event_time", "1 hour")`:
# MAGIC * data **older than the watermark** may be dropped (it's "too late"),
# MAGIC * a window whose end is **before** the watermark is **final**: in append mode it's written **once**, and its state is removed.

# COMMAND ----------

# DBTITLE 1,Query C - deduplicate re-sent clicks within the watermark
deduped = (clicks.withWatermark("event_time", "1 hour")
                 .dropDuplicatesWithinWatermark(["click_id"]))
writer_dedup = deduped.writeStream.option("checkpointLocation", checkpoint("clicks_dedup"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Query D — `foreachBatch`: run any batch code on each micro-batch
# MAGIC `foreachBatch(fn)` hands every micro-batch to your function as a normal DataFrame, so you can use batch-only features:
# MAGIC **`MERGE`** (upserts), writing to several tables, calling APIs. Here: keep a running total of clicks per user.

# COMMAND ----------

# DBTITLE 1,Query D - foreachBatch + MERGE upsert
user_clicks_table = f"{catalog_name}.{schema_name}.lab06_user_clicks"     # fully qualified: the batch runs in its own session
spark.sql(f"""CREATE TABLE IF NOT EXISTS {user_clicks_table}
              (user_id STRING, clicks BIGINT, last_click TIMESTAMP)""")


def upsert_user_clicks(batch_df, batch_id):
    per_user = batch_df.groupBy("user_id").agg(F.count("*").alias("clicks"), F.max("event_time").alias("last_click"))
    per_user.createOrReplaceTempView("user_clicks_batch")
    batch_df.sparkSession.sql(f"""
        MERGE INTO {user_clicks_table} AS t
        USING user_clicks_batch AS s
        ON t.user_id = s.user_id
        WHEN MATCHED THEN UPDATE SET t.clicks = t.clicks + s.clicks,
                                     t.last_click = greatest(t.last_click, s.last_click)
        WHEN NOT MATCHED THEN INSERT (user_id, clicks, last_click) VALUES (s.user_id, s.clicks, s.last_click)""")


writer_upsert = (clicks.writeStream.foreachBatch(upsert_user_clicks)
                 .option("checkpointLocation", checkpoint("user_clicks")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2 · Deliver the files one by one
# MAGIC To make event time move forward like in real life, we deliver **one file at a time** and run every query after each
# MAGIC delivery (each run = one micro-batch of new data).

# COMMAND ----------

# DBTITLE 1,4 deliveries x 4 queries
foreach_batch_ok = True
for delivery in range(1, 5):
    if delivery > 1:
        land_clicks(1)
    print(f"--- delivery {delivery} ---")
    run_stream(writer_complete, "lab06_clicks_complete")
    run_stream(writer_append, "lab06_clicks_append")
    run_stream(writer_dedup, "lab06_clicks_dedup")
    if foreach_batch_ok:
        try:
            run_stream(writer_upsert)
        except Exception as e:
            foreach_batch_ok = False
            print("🚫 foreachBatch not available on this compute:", (str(e).strip().splitlines() or [repr(e)])[0][:160])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 3 · Complete mode vs append mode + watermark

# COMMAND ----------

# DBTITLE 1,Side by side
complete = spark.table("lab06_clicks_complete").withColumnRenamed("clicks", "complete_mode")
append = spark.table("lab06_clicks_append").withColumnRenamed("clicks", "append_mode")
display(complete.join(append, ["window_start", "window_end"], "left").orderBy("window_start"))

first_window_complete = complete.orderBy("window_start").first()["complete_mode"]
first_window_append = append.orderBy("window_start").first()["append_mode"]
n_complete, n_append = complete.count(), append.count()
print(f"10:00-10:30 window: complete = {first_window_complete}, append+watermark = {first_window_append}")
print(f"windows written: complete = {n_complete}, append+watermark = {n_append}")

# COMMAND ----------

# MAGIC %md
# MAGIC Read the table carefully:
# MAGIC
# MAGIC * **10:00–10:30** — complete mode shows **14**: the 10 original clicks **plus the 4 late ones** from file 4. Append mode
# MAGIC   shows **10**: when file 4 arrived the watermark was already **14:57** (latest event time seen 15:57 − 1 hour), so the 10:1x clicks were **dropped as too late**
# MAGIC   and the finished window was never reopened.
# MAGIC * **11:30–12:00** — both show **15**: the 5 re-sent clicks arrived within the watermark, so they were counted
# MAGIC   (counting isn't deduplication — see Part 4).
# MAGIC * **The last windows** are missing in append mode: they are not final yet. They will be written when newer data moves
# MAGIC   the watermark past their end. Complete mode shows everything every time — but keeps **all** state forever and
# MAGIC   rewrites the whole table each trigger.
# MAGIC
# MAGIC | | complete mode (no watermark) | append mode + watermark | update mode + watermark |
# MAGIC |---|---|---|---|
# MAGIC | Rows written per trigger | whole result | only **finished** windows | changed windows |
# MAGIC | Late data | always counted | dropped after the watermark | dropped after the watermark |
# MAGIC | State | grows forever | cleaned up | cleaned up |
# MAGIC | Delta table sink | ✅ | ✅ | via `foreachBatch` + `MERGE` |
# MAGIC
# MAGIC Other window types: **sliding** `window("event_time", "30 minutes", "10 minutes")` (overlapping) and **session**
# MAGIC windows `session_window("event_time", "15 minutes")` (gap-based).
# MAGIC
# MAGIC ## Part 4 · Deduplication

# COMMAND ----------

# DBTITLE 1,Were the duplicates removed?
dedup = spark.table("lab06_clicks_dedup")
dedup_rows, dedup_distinct = dedup.count(), dedup.select("click_id").distinct().count()
raw_rows = spark.read.schema(CLICK_SCHEMA).json(clicks_landing).count()
print(f"raw rows delivered: {raw_rows} | after dedup: {dedup_rows} (distinct click_id: {dedup_distinct})")

# COMMAND ----------

# MAGIC %md
# MAGIC * `dropDuplicatesWithinWatermark(["click_id"])` keeps the first copy of each `click_id` and **forgets** ids once they are
# MAGIC   older than the watermark → bounded state. The 4 very late clicks are older than the watermark, so they are dropped here too.
# MAGIC * `dropDuplicates(["click_id"])` **without** a watermark also works on a stream — but remembers **every id forever**
# MAGIC   (state grows without limit). With a watermark, include the event-time column in the key: `dropDuplicates(["click_id", "event_time"])`.
# MAGIC
# MAGIC ## Part 5 · `foreachBatch` + `MERGE`

# COMMAND ----------

# DBTITLE 1,Per-user totals maintained by MERGE
user_clicks = spark.table("lab06_user_clicks")
total_upserted = user_clicks.agg(F.sum("clicks")).first()[0] if foreach_batch_ok else None
display(user_clicks.orderBy(F.desc("clicks")).limit(10))
print("sum of clicks in lab06_user_clicks:", total_upserted, "| raw rows delivered:", raw_rows)

# COMMAND ----------

# MAGIC %md
# MAGIC The totals add up to **all** delivered rows (duplicates and late clicks included — this query has no watermark or dedup).
# MAGIC
# MAGIC > ⚠️ `foreachBatch` gives **at-least-once** semantics for whatever your function does: if a micro-batch is retried, the
# MAGIC > function runs again. `t.clicks + s.clicks` would then double-count. Make the logic idempotent (e.g. MERGE on a unique key
# MAGIC > with "last value wins"), or for Delta writes use the `txnAppId` / `txnVersion` options (`batch_id` as the version).
# MAGIC
# MAGIC ## Part 6 · Other things the exam expects you to know
# MAGIC
# MAGIC | Topic | Key facts |
# MAGIC |---|---|
# MAGIC | **Stream-static join** | a static **Delta** table is joined at its latest version each micro-batch (06-L1); inner and left joins supported |
# MAGIC | **Stream-stream join** | both sides buffered in **state**; add **watermarks** (+ a time-range condition) so state can be cleaned; outer joins **require** them |
# MAGIC | **Multiple watermarks** | the global watermark is the **minimum** of the inputs (configurable to max) |
# MAGIC | **Kafka source** | `spark.readStream.format("kafka").option("kafka.bootstrap.servers", "...").option("subscribe", "orders").load()` → columns `key`, `value` (binary — cast to string / parse JSON), `topic`, `partition`, `offset`, `timestamp` |
# MAGIC | **Delta as a source** | append-only by default; `skipChangeCommits` to ignore updates/deletes; `readChangeFeed` for CDC (06-L1, Section 08) |
# MAGIC | **Exactly-once** | replayable source + checkpoint (offsets / commits) + idempotent sink |
# MAGIC
# MAGIC ## Part 7 · ✅ Automatic checks

# COMMAND ----------

# DBTITLE 1,Check your work
_checks = {
    "complete mode counted the late clicks (10:00 window = 14)": first_window_complete == 14,
    "append + watermark dropped them (10:00 window = 10)": first_window_append == 10,
    "append mode wrote fewer windows (the last ones are not final yet)": n_append < n_complete,
    "dedup: no click_id appears twice": dedup_rows == dedup_distinct,
    "dedup removed the 5 re-sent clicks (and dropped late ones)": dedup_rows <= raw_rows - 5,
}
if foreach_batch_ok:
    _checks["foreachBatch MERGE totals = all delivered rows"] = total_upserted == raw_rows
for name, ok in _checks.items():
    print(("✅ " if ok else "❌ ") + name)
if not foreach_batch_ok:
    print("➖ foreachBatch check skipped (not available on this compute)")
stop_all_streams()
print("\n🎉 Lab complete - next: 06-L4 · Challenge Lab")
