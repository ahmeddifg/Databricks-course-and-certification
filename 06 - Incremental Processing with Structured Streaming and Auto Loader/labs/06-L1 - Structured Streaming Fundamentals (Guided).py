# Databricks notebook source
# MAGIC %md
# MAGIC # 🧪 Lab 06-L1 · Structured Streaming Fundamentals (Guided)
# MAGIC **Time:** ~60 min · **Compute:** Serverless (Free Edition) — every stream uses `trigger(availableNow=True)`
# MAGIC
# MAGIC ShopWave's order system drops **one JSON file per day** into `orders-landing/`. You'll build streams that pick up only
# MAGIC the new files, look inside a **checkpoint**, run **streaming SQL**, and see which operations and output modes are allowed.
# MAGIC
# MAGIC | Part | You will… |
# MAGIC |---|---|
# MAGIC | 1 | Compare a **batch** read with a **streaming** read of the same folder |
# MAGIC | 2 | Write your first stream into a Delta table (checkpoint + `availableNow`) |
# MAGIC | 3 | Prove **incremental processing**: re-run with and without new files |
# MAGIC | 4 | Look inside the **checkpoint** and the query **progress** |
# MAGIC | 5 | Streaming **SQL** on a temp view, a **stream-static join** and **output modes** |
# MAGIC | 6 | Hit the **unsupported operations** on purpose |
# MAGIC | 7 | Read a **Delta table as a stream** (bronze → silver) and handle **updates/deletes** in the source |
# MAGIC | 8 | **Triggers** on serverless, `display()` of a stream |
# MAGIC | 9 | What happens when you **delete a checkpoint** |
# MAGIC | 10 | ✅ Automatic checks |

# COMMAND ----------

# MAGIC %run ../../Includes/_setup

# COMMAND ----------

# MAGIC %run ./_06_prepare

# COMMAND ----------

# DBTITLE 1,Start clean (safe to re-run)
stop_all_streams()
for t in ("lab06_orders_bronze", "lab06_orders_silver", "lab06_customer_revenue", "lab06_country_revenue",
          "lab06_orders_dupe_demo", "lab06_sort_try"):
    spark.sql(f"DROP TABLE IF EXISTS {t}")
for c in ("orders_bronze", "orders_silver", "customer_revenue", "customer_revenue_append_try", "country_revenue",
          "dupe_demo", "trigger_demo", "sort_try"):
    if path_exists(checkpoint(c)):
        dbutils.fs.rm(checkpoint(c), True)
reset_orders_landing()                                  # orders-landing/ holds only 01.json now
landing = f"{dataset_path}/orders-landing"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1 · Batch read vs streaming read
# MAGIC The **same** folder, read two ways. A batch read is a snapshot; a streaming read is an **unbounded table**: every new file
# MAGIC becomes new rows appended to it.

# COMMAND ----------

# DBTITLE 1,Batch: a DataFrame you can count
orders_batch = spark.read.schema(ORDER_SCHEMA).json(landing)
print("isStreaming:", orders_batch.isStreaming, "| rows now:", orders_batch.count())

# COMMAND ----------

# DBTITLE 1,Streaming: a DataFrame that describes an endless source
orders_stream = spark.readStream.schema(ORDER_SCHEMA).json(landing)
print("isStreaming:", orders_stream.isStreaming)
try:
    orders_stream.count()
except Exception as e:
    print("🚫 Expected - a stream has no end, so it can't be counted directly:\n  ",
          (str(e).strip().splitlines() or [repr(e)])[0][:160])

# COMMAND ----------

# MAGIC %md
# MAGIC > 💡 A plain **file** streaming source needs a **schema** (here the course's `ORDER_SCHEMA`) — it doesn't infer one.
# MAGIC > **Auto Loader** (06-L2) infers and evolves the schema for you.
# MAGIC
# MAGIC ## Part 2 · Your first streaming write
# MAGIC Three things every production stream needs:
# MAGIC
# MAGIC 1. a **sink** — here a Delta table (`toTable`)
# MAGIC 2. a **checkpoint location** — unique per stream; it remembers what was already processed
# MAGIC 3. a **trigger** — on serverless: `availableNow=True` (process everything new, then stop)

# COMMAND ----------

# DBTITLE 1,readStream -> transform -> writeStream
bronze_writer = (orders_stream
                 .select("*",
                         F.col("_metadata.file_name").alias("source_file"),
                         F.current_timestamp().alias("ingested_at"))
                 .writeStream
                 .format("delta")                                    # the default on Databricks, shown for clarity
                 .outputMode("append")                               # new rows are only ever added
                 .option("checkpointLocation", checkpoint("orders_bronze")))

q1 = run_stream(bronze_writer, "lab06_orders_bronze")
rows_run1 = spark.table("lab06_orders_bronze").count()
print("rows in lab06_orders_bronze:", rows_run1)

# COMMAND ----------

# MAGIC %md
# MAGIC The equivalent `writeStream` spelled out (what `run_stream()` does for you):
# MAGIC
# MAGIC ```python
# MAGIC query = (df.writeStream
# MAGIC            .option("checkpointLocation", "/Volumes/.../checkpoints/lab06/orders_bronze")
# MAGIC            .trigger(availableNow=True)          # or processingTime="1 minute" on classic compute
# MAGIC            .toTable("lab06_orders_bronze"))
# MAGIC query.awaitTermination()                        # block until the availableNow run is done
# MAGIC ```
# MAGIC
# MAGIC ## Part 3 · Incremental processing

# COMMAND ----------

# DBTITLE 1,Run again with NO new files
q2 = run_stream(bronze_writer, "lab06_orders_bronze")
rows_run2 = spark.table("lab06_orders_bronze").count()
print("rows:", rows_run2, "(unchanged - the checkpoint says 01.json is done)")

# COMMAND ----------

# DBTITLE 1,Two new files arrive -> only they are processed
land_until("orders", "03.json")        # delivers 02.json and 03.json (a re-run of this cell lands nothing)
q3 = run_stream(bronze_writer, "lab06_orders_bronze")
rows_run3 = spark.table("lab06_orders_bronze").count()
display(spark.sql("SELECT source_file, count(*) AS rows FROM lab06_orders_bronze GROUP BY source_file ORDER BY source_file"))

# COMMAND ----------

# MAGIC %md
# MAGIC This is the core promise of Structured Streaming: **each input record is processed exactly once**, even if you run the
# MAGIC query every minute, stop it, or it crashes — because of the checkpoint (next part).
# MAGIC
# MAGIC ## Part 4 · Inside the checkpoint and the progress

# COMMAND ----------

# DBTITLE 1,What's in a checkpoint folder?
show_checkpoint("orders_bronze")

# COMMAND ----------

# MAGIC %md
# MAGIC | Folder / file | Holds |
# MAGIC |---|---|
# MAGIC | `metadata` | the query's unique id |
# MAGIC | `offsets/` | one file per micro-batch: **what** the batch will read (written *before* processing — write-ahead log) |
# MAGIC | `commits/` | one file per micro-batch: marks the batch as **done** (written *after* the sink commit) |
# MAGIC | `sources/` | source bookkeeping, e.g. the list of files already seen |
# MAGIC | `state/` | state of aggregations, dedup, joins (stateful queries only) |
# MAGIC
# MAGIC On restart Spark compares `offsets` and `commits`: an uncommitted batch is **re-run** with exactly the same input, and
# MAGIC the Delta sink ignores the repeat (idempotent) → **exactly-once** end to end.

# COMMAND ----------

# DBTITLE 1,Query progress: what did the last run do?
p = progress_dict(q3.lastProgress)
print({k: p.get(k) for k in ("id", "runId", "batchId", "numInputRows", "timestamp")})
print("active streams right now:", len(spark.streams.active), "(availableNow queries stop by themselves)")

# COMMAND ----------

# MAGIC %md
# MAGIC Useful handles on a `StreamingQuery`: `query.status`, `query.lastProgress`, `query.recentProgress`,
# MAGIC `query.awaitTermination()`, `query.stop()`, and `spark.streams.active` for all running queries of the session.
# MAGIC
# MAGIC ## Part 5 · Streaming SQL, stream-static joins and output modes
# MAGIC A streaming DataFrame can be registered as a **temp view** — every SQL query on it is streaming too.

# COMMAND ----------

# DBTITLE 1,Read the bronze TABLE as a stream and expose it to SQL
spark.readStream.table("lab06_orders_bronze").createOrReplaceTempView("orders_stream_v")

revenue_stream = spark.sql("""
    SELECT customer_id, count(*) AS orders, round(sum(total), 2) AS revenue
    FROM orders_stream_v
    GROUP BY customer_id""")
print("isStreaming:", revenue_stream.isStreaming)

# COMMAND ----------

# DBTITLE 1,An aggregation in APPEND mode is rejected ...
append_rejected = False
try:
    run_stream(revenue_stream.writeStream.outputMode("append")
               .option("checkpointLocation", checkpoint("customer_revenue") + "_append_try"),
               "lab06_customer_revenue")
except Exception as e:
    append_rejected = True
    print("🚫 Expected:", (str(e).strip().splitlines() or [repr(e)])[0][:220])

# COMMAND ----------

# MAGIC %md
# MAGIC Why? In **append** mode a row, once written, is final — but a running total per customer keeps changing. Without a
# MAGIC **watermark** (06-L3) Spark can never declare a group "final".
# MAGIC (`toTable` may already have created an empty `lab06_customer_revenue` table before the query was rejected — the next
# MAGIC cell fills it.)
# MAGIC
# MAGIC | Output mode | Writes each trigger… | Typical use |
# MAGIC |---|---|---|
# MAGIC | `append` (default) | only **new** rows | raw ingestion, filters, projections; aggregations **with** a watermark |
# MAGIC | `complete` | the **whole** result table again | small aggregate tables |
# MAGIC | `update` | only rows that **changed** | aggregates into sinks that can upsert (via `foreachBatch` + `MERGE`) — the Delta table sink itself supports append & complete |

# COMMAND ----------

# DBTITLE 1,... COMPLETE mode rewrites the full aggregate each run
run_stream(revenue_stream.writeStream.outputMode("complete")
           .option("checkpointLocation", checkpoint("customer_revenue")), "lab06_customer_revenue")
display(spark.table("lab06_customer_revenue").orderBy(F.desc("revenue")).limit(5))
customers_in_bronze = spark.table("lab06_orders_bronze").select("customer_id").distinct().count()

# COMMAND ----------

# MAGIC %md
# MAGIC **Stream-static join**: enrich each streaming order with the (static) customer dimension. Only the stream drives the
# MAGIC join: new orders trigger a micro-batch, changes to the dimension alone don't. When the static side is a **Delta table**,
# MAGIC each micro-batch joins its **latest version**, so dimension updates are picked up for new orders (here we use a JSON
# MAGIC snapshot for simplicity).

# COMMAND ----------

# DBTITLE 1,Stream-static join -> revenue per country
customers = (spark.read.json(f"{dataset_path}/customers-json")
                  .select("customer_id", F.get_json_object("profile", "$.address.country").alias("country")))

country_revenue = (spark.readStream.table("lab06_orders_bronze")
                        .join(customers, "customer_id", "left")
                        .groupBy("country")
                        .agg(F.count("*").alias("orders"), F.round(F.sum("total"), 2).alias("revenue")))

run_stream(country_revenue.writeStream.outputMode("complete")
           .option("checkpointLocation", checkpoint("country_revenue")), "lab06_country_revenue")
display(spark.table("lab06_country_revenue").orderBy(F.desc("revenue")))

# COMMAND ----------

# MAGIC %md
# MAGIC > 🎯 The `C9999` orders have no customer → country `NULL` with the left join. With an **inner** stream-static join they
# MAGIC > would disappear silently.
# MAGIC
# MAGIC ## Part 6 · Operations a stream can't do
# MAGIC Some DataFrame operations need to see **all** the data — impossible for an endless source.

# COMMAND ----------

# DBTITLE 1,Sorting a non-aggregated stream
sort_rejected = False
try:
    run_stream(spark.readStream.table("lab06_orders_bronze").orderBy("total")
               .writeStream.option("checkpointLocation", checkpoint("sort_try")), "lab06_sort_try")
except Exception as e:
    sort_rejected = True
    print("🚫 Expected:", (str(e).strip().splitlines() or [repr(e)])[0][:220])
spark.sql("DROP TABLE IF EXISTS lab06_sort_try")

# COMMAND ----------

# MAGIC %md
# MAGIC | Not supported on a streaming DataFrame | Instead… |
# MAGIC |---|---|
# MAGIC | `orderBy` / `sort` (except after an aggregation in **complete** mode) | sort when you **read** the result table |
# MAGIC | `limit`, `take`, `count()`, `collect()`, `show()` as actions | write to a sink, then query the table |
# MAGIC | chained stateful aggregations **without** watermarks | add watermarks (supported since Spark 3.4) or use `foreachBatch` |
# MAGIC | outer joins stream-stream **without** watermarks | add watermarks (06-P3) |
# MAGIC
# MAGIC ## Part 7 · A Delta table as a streaming source (bronze → silver)
# MAGIC Delta tables are great streaming sources: each **new commit** is new input. The stream expects the source to be
# MAGIC **append-only**.

# COMMAND ----------

# DBTITLE 1,Silver: clean stream from the bronze table
silver_writer = (spark.readStream.table("lab06_orders_bronze")
                      .where("quantity > 0")
                      .withColumn("order_ts", F.timestamp_seconds("order_timestamp"))
                      .drop("order_timestamp")
                      .writeStream.option("checkpointLocation", checkpoint("orders_silver")))
run_stream(silver_writer, "lab06_orders_silver")
silver_rows_1 = spark.table("lab06_orders_silver").count()
print("silver rows:", silver_rows_1)

# COMMAND ----------

# DBTITLE 1,Someone DELETEs rows in bronze -> the stream refuses to continue
spark.sql("DELETE FROM lab06_orders_bronze WHERE customer_id = 'C9999'")   # a non-append commit
land_until("orders", "04.json")
run_stream(bronze_writer, "lab06_orders_bronze")                          # new file 04.json -> bronze

delete_blocked = globals().get("delete_blocked", False)       # keeps the result if you re-run this cell
try:
    run_stream(silver_writer, "lab06_orders_silver")
except Exception as e:
    delete_blocked = True
    print("🚫 Expected:", (str(e).strip().splitlines() or [repr(e)])[0][:220])

# COMMAND ----------

# DBTITLE 1,skipChangeCommits: ignore commits that update/delete, keep processing appends
silver_writer_skip = (spark.readStream.option("skipChangeCommits", "true").table("lab06_orders_bronze")
                           .where("quantity > 0")
                           .withColumn("order_ts", F.timestamp_seconds("order_timestamp"))
                           .drop("order_timestamp")
                           .writeStream.option("checkpointLocation", checkpoint("orders_silver")))
run_stream(silver_writer_skip, "lab06_orders_silver")
silver_rows_2 = spark.table("lab06_orders_silver").count()
print("silver rows now:", silver_rows_2, "(04.json added; the DELETE commit was skipped, not propagated)")

# COMMAND ----------

# MAGIC %md
# MAGIC | Source table change | Streaming read |
# MAGIC |---|---|
# MAGIC | `INSERT` / append | ✅ processed |
# MAGIC | `UPDATE`, `DELETE`, `MERGE`, overwrite | ❌ stream fails — unless `skipChangeCommits` (ignore them) |
# MAGIC | You need the changes downstream | read the **Change Data Feed** (`readChangeFeed`) — Section 08 |
# MAGIC
# MAGIC > ⚠️ `skipChangeCommits` **ignores** the change: silver still contains the deleted `C9999` rows. That is exactly what
# MAGIC > the exam wants you to notice.
# MAGIC
# MAGIC ## Part 8 · Triggers on serverless and `display()`

# COMMAND ----------

# DBTITLE 1,A processing-time trigger is rejected on serverless
processing_time_rejected = False
try:
    q = (spark.readStream.table("lab06_orders_bronze").writeStream.format("noop")
              .option("checkpointLocation", checkpoint("trigger_demo"))
              .trigger(processingTime="10 seconds").start())
    print("⚠️ Started - you are on classic compute. Stopping it again.")
    q.stop()
except Exception as e:
    processing_time_rejected = True
    print("🚫 Expected on serverless:", (str(e).strip().splitlines() or [repr(e)])[0][:200])

# COMMAND ----------

# MAGIC %md
# MAGIC | Trigger | Behaviour | Serverless |
# MAGIC |---|---|---|
# MAGIC | *(none)* | micro-batches back to back (`processingTime = 0`) — runs forever | ❌ must set a trigger |
# MAGIC | `processingTime="1 minute"` | a micro-batch every minute — runs forever | ❌ |
# MAGIC | `availableNow=True` | process **all** data available now (possibly in several micro-batches), then **stop** | ✅ recommended |
# MAGIC | `once=True` | one micro-batch, then stop — **deprecated**, use `availableNow` | ✅ (not recommended) |
# MAGIC | continuous / real-time mode | millisecond latency, special cases | ❌ |
# MAGIC
# MAGIC > 💡 `display(streaming_df)` also **starts a stream** (with a default trigger and a temporary checkpoint). It's handy on
# MAGIC > classic compute for demos; on serverless the default trigger isn't allowed — write to a table with `availableNow` and
# MAGIC > display the table instead.
# MAGIC > For "always on" streaming without classic compute use a **continuous Lakeflow pipeline** (Section 08), or schedule the
# MAGIC > `availableNow` notebook in a **Lakeflow Job** (Section 10).
# MAGIC
# MAGIC ## Part 9 · Never share or delete a checkpoint casually
# MAGIC The checkpoint **is** the memory of the stream. Watch what happens without it.

# COMMAND ----------

# DBTITLE 1,Same stream, checkpoint deleted -> everything is re-processed
spark.sql("DROP TABLE IF EXISTS lab06_orders_dupe_demo")       # start this demo from scratch (safe to re-run)
if path_exists(checkpoint("dupe_demo")):
    dbutils.fs.rm(checkpoint("dupe_demo"), True)

dupe_writer = (spark.readStream.schema(ORDER_SCHEMA).json(landing)
                    .writeStream.option("checkpointLocation", checkpoint("dupe_demo")))
run_stream(dupe_writer, "lab06_orders_dupe_demo")
dupe_first = spark.table("lab06_orders_dupe_demo").count()

dbutils.fs.rm(checkpoint("dupe_demo"), True)                   # 💥 "cleaning up"
run_stream(dupe_writer, "lab06_orders_dupe_demo")
dupe_second = spark.table("lab06_orders_dupe_demo").count()
print(f"rows after first run: {dupe_first} | after deleting the checkpoint and re-running: {dupe_second}")

# COMMAND ----------

# MAGIC %md
# MAGIC * Delete a checkpoint **only** when you intend a full reprocess — and then also truncate/replace the target.
# MAGIC * **Never** let two streams share a checkpoint folder; each writer gets its own.
# MAGIC * Changing the query in incompatible ways (e.g. a different aggregation) also needs a new checkpoint.
# MAGIC
# MAGIC ## Part 10 · ✅ Automatic checks

# COMMAND ----------

# DBTITLE 1,Check your work
_checks = {
    "first run loaded 01.json (121 rows)": rows_run1 == 121,
    "re-run without new files loaded nothing": rows_run2 == 121,
    "two new files -> 363 rows": rows_run3 == 363,
    "aggregation in append mode was rejected": append_rejected,
    "complete mode wrote one row per customer": spark.table("lab06_customer_revenue").count() == customers_in_bronze,
    "sorting a stream was rejected": sort_rejected,
    "silver has no cancelled orders": spark.table("lab06_orders_silver").where("quantity = 0").count() == 0,
    "the DELETE in the source blocked the silver stream": delete_blocked,
    "skipChangeCommits let 04.json through": silver_rows_2 > silver_rows_1,
    "deleting the checkpoint re-processed everything (duplicates)": dupe_second == 2 * dupe_first,
}
for name, ok in _checks.items():
    print(("✅ " if ok else "❌ ") + name)
print(("ℹ️ processingTime trigger rejected (serverless)" if processing_time_rejected
       else "ℹ️ processingTime trigger accepted (classic compute)"))
stop_all_streams()
print("\n🎉 Lab complete - next: 06-L2 · Auto Loader")
