# Databricks notebook source
# MAGIC %md
# MAGIC # 🧪 Lab 08-L1 · Hand-coded Medallion Pipeline with Structured Streaming (Guided)
# MAGIC **Time:** ~45 min · **Compute:** Serverless (Free Edition) — every stream uses `trigger(availableNow=True)`
# MAGIC
# MAGIC You'll build ShopWave's order pipeline **by hand**: files → **bronze** (Auto Loader) → **silver** (clean, deduplicate,
# MAGIC enrich with a stream-static join) → **gold** (daily revenue per country). Every hop is its own stream with its own
# MAGIC checkpoint. At the end you'll count the moving parts — and see why Lakeflow **Spark Declarative Pipelines** (08-L2) exist.
# MAGIC
# MAGIC | Part | You will… |
# MAGIC |---|---|
# MAGIC | 1 | Create the **customer dimension** (a static Delta table) |
# MAGIC | 2 | **Bronze** — Auto Loader with ingestion metadata columns |
# MAGIC | 3 | **Silver** — `from_unixtime`, filter, watermark dedup, **stream-static join** |
# MAGIC | 4 | **Gold** — streaming aggregation in **complete** mode |
# MAGIC | 5 | Run the whole chain again: **only new files** flow through all hops |
# MAGIC | 6 | Change the dimension and see what a stream-static join does (and doesn't do) |
# MAGIC | 7 | Count the moving parts: hand-coded vs declarative |
# MAGIC | 8 | ✅ Automatic checks |

# COMMAND ----------

# MAGIC %run ../../Includes/_setup

# COMMAND ----------

# MAGIC %run ./_08_prepare

# COMMAND ----------

# DBTITLE 1,Start clean (safe to re-run)
stop_all_streams()
for t in ("lab08_hc_customers", "lab08_hc_bronze", "lab08_hc_silver", "lab08_hc_gold"):
    spark.sql(f"DROP TABLE IF EXISTS {t}")
for c in ("hc_bronze", "hc_silver", "hc_gold"):
    if path_exists(checkpoint(c)):
        dbutils.fs.rm(checkpoint(c), True)
reset_feed("hc_orders")                 # empty lab08/hc-orders/ ...
_ = land_until("hc_orders", "01.json")  # ... and deliver the first day of orders
print("landing folder:", landing("hc_orders"), "->", landed("hc_orders"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## The plan
# MAGIC
# MAGIC | Hop | Table | Reads | Writes | Why this layer? |
# MAGIC |---|---|---|---|---|
# MAGIC | 🥉 Bronze | `lab08_hc_bronze` | JSON files (Auto Loader) | append | raw copy + **where/when** each row came from → you can always replay |
# MAGIC | 🥈 Silver | `lab08_hc_silver` | bronze **as a stream** + customers (static) | append | typed, valid, **deduplicated**, enriched — the "single source of truth" |
# MAGIC | 🥇 Gold | `lab08_hc_gold` | silver **as a stream** | **complete** | business aggregate for BI: orders & revenue per day and country |
# MAGIC
# MAGIC Each hop is a separate streaming query with its **own checkpoint**. Tables in between decouple the hops: silver can be
# MAGIC rebuilt from bronze, gold from silver — without touching the source files.
# MAGIC
# MAGIC ## Part 1 · The customer dimension (static)

# COMMAND ----------

# DBTITLE 1,A normal (batch) Delta table - the "static" side of the join
spark.sql(f"""
    CREATE OR REPLACE TABLE lab08_hc_customers AS
    SELECT customer_id,
           profile:address:country::string AS country,
           profile:address:city::string    AS city
    FROM json.`{dataset_path}/customers-json`""")
display(spark.table("lab08_hc_customers").limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2 · Bronze: Auto Loader + ingestion metadata
# MAGIC Bronze keeps the data **as it arrived** and adds columns that answer "where from, and when?" — invaluable when you debug
# MAGIC silver or need to re-process one bad file.

# COMMAND ----------

# DBTITLE 1,Bronze stream: files -> lab08_hc_bronze
bronze_writer = (spark.readStream
                      .format("cloudFiles")
                      .option("cloudFiles.format", "json")
                      .option("cloudFiles.schemaLocation", checkpoint("hc_bronze"))   # where the inferred schema lives
                      .option("cloudFiles.inferColumnTypes", "true")                  # numbers as numbers, not strings
                      .load(landing("hc_orders"))
                      .select("*",
                              F.col("_metadata.file_name").alias("source_file"),       # ingestion metadata
                              F.current_timestamp().alias("ingested_at"))
                      .writeStream
                      .option("checkpointLocation", checkpoint("hc_bronze")))

run_stream(bronze_writer, "lab08_hc_bronze", "bronze")
bronze_1 = spark.table("lab08_hc_bronze").count()
display(spark.table("lab08_hc_bronze").select("order_id", "order_timestamp", "customer_id", "quantity", "total",
                                              "source_file", "ingested_at").limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC > 💡 `order_timestamp` is a **Unix timestamp in seconds** (e.g. `1782864000`). Bronze keeps it that way — converting it is
# MAGIC > a silver job.
# MAGIC
# MAGIC ## Part 3 · Silver: clean, deduplicate, enrich
# MAGIC Rules for ShopWave silver orders:
# MAGIC
# MAGIC 1. `order_ts` = `from_unixtime(order_timestamp)` cast to **TIMESTAMP**
# MAGIC 2. drop **cancelled** orders (`quantity = 0`)
# MAGIC 3. drop **duplicate** `order_id`s — with a **watermark**, so the dedup state doesn't grow forever
# MAGIC 4. add the customer's `country` and `city` → **stream-static join** with `lab08_hc_customers`

# COMMAND ----------

# DBTITLE 1,Silver stream: lab08_hc_bronze -> lab08_hc_silver
customers_static = spark.read.table("lab08_hc_customers")         # static side: a Delta TABLE

silver_df = (spark.readStream.table("lab08_hc_bronze")             # streaming side: new bronze rows only
                  .where("quantity > 0")
                  .withColumn("order_ts", F.from_unixtime("order_timestamp").cast("timestamp"))
                  .withWatermark("order_ts", "1 day")
                  .dropDuplicatesWithinWatermark(["order_id"])
                  .join(customers_static, "customer_id", "left")
                  .select("order_id", "customer_id", "country", "city", "order_ts", "quantity", "total", "source_file"))

silver_writer = silver_df.writeStream.option("checkpointLocation", checkpoint("hc_silver"))
run_stream(silver_writer, "lab08_hc_silver", "silver")
silver_1 = spark.table("lab08_hc_silver").count()
print(f"bronze rows: {bronze_1} | silver rows: {silver_1}  (1 cancelled order dropped, 1 duplicate removed)")

# COMMAND ----------

# MAGIC %md
# MAGIC **Stream-static join rules (exam!)**
# MAGIC * Only the **stream** drives the query: a new bronze row triggers work; a change in `lab08_hc_customers` alone does not.
# MAGIC * The static side is a **Delta table**, so every micro-batch joins with its **latest version** (you'll prove it in Part 6).
# MAGIC * A **left** join keeps orders of unknown customers (`C9999`) with `country = NULL`; an **inner** join would drop them silently.
# MAGIC
# MAGIC ## Part 4 · Gold: a streaming aggregate in complete mode

# COMMAND ----------

# DBTITLE 1,Gold stream: lab08_hc_silver -> lab08_hc_gold
gold_df = (spark.readStream.table("lab08_hc_silver")
                .groupBy(F.to_date("order_ts").alias("order_date"), "country")
                .agg(F.count("*").alias("orders"), F.round(F.sum("total"), 2).alias("revenue")))

gold_writer = (gold_df.writeStream
                      .outputMode("complete")                     # rewrite the whole (small) result every run
                      .option("checkpointLocation", checkpoint("hc_gold")))
run_stream(gold_writer, "lab08_hc_gold", "gold")
display(spark.table("lab08_hc_gold").orderBy("order_date", F.desc("revenue")))

# COMMAND ----------

# MAGIC %md
# MAGIC Why **complete**? The aggregate per (day, country) changes whenever a new order for that group arrives. **Append** mode
# MAGIC would need a watermark on an event-time **window** to know when a group is final; **complete** simply rewrites the whole
# MAGIC result table on each trigger — fine for small gold tables.
# MAGIC
# MAGIC ## Part 5 · New files → one run of the whole chain
# MAGIC In production a **Lakeflow Job** would run the three streams in order (bronze → silver → gold) on a schedule. Here a small
# MAGIC function does the same.

# COMMAND ----------

# DBTITLE 1,Orchestrate the three hops
def run_medallion():
    """Run bronze, silver and gold once each (availableNow), in dependency order."""
    run_stream(bronze_writer, "lab08_hc_bronze", "bronze")
    run_stream(silver_writer, "lab08_hc_silver", "silver")
    run_stream(gold_writer, "lab08_hc_gold", "gold")


_ = land_until("hc_orders", "03.json")        # two more days arrive: 02.json and 03.json
run_medallion()
bronze_3, silver_3 = spark.table("lab08_hc_bronze").count(), spark.table("lab08_hc_silver").count()
print(f"bronze: {bronze_1} -> {bronze_3} | silver: {silver_1} -> {silver_3}")
display(spark.sql("SELECT source_file, count(*) AS rows FROM lab08_hc_silver GROUP BY source_file ORDER BY source_file"))

# COMMAND ----------

# MAGIC %md
# MAGIC Each hop's checkpoint remembered where it stopped, so only the **two new files** were read by bronze, only the **new
# MAGIC bronze rows** by silver, and gold updated its running aggregates. Run the cell again: no file is landed, every stream
# MAGIC reads **0 input rows**, and nothing changes. ✅ Idempotent.
# MAGIC
# MAGIC ## Part 6 · The dimension changes
# MAGIC Customer **C0022** moves from the United States to **Canada**. Then day 4 (`04.json`) arrives — it contains 2 new orders
# MAGIC from C0022 (C0022 already has 2 older orders in silver).

# COMMAND ----------

# DBTITLE 1,Update the dimension, land day 4, run the chain
spark.sql("UPDATE lab08_hc_customers SET country = 'Canada', city = 'Toronto' WHERE customer_id = 'C0022'")
_ = land_until("hc_orders", "04.json")
run_medallion()
display(spark.sql("""
    SELECT order_id, to_date(order_ts) AS order_date, country, city, source_file
    FROM lab08_hc_silver WHERE customer_id = 'C0022' ORDER BY order_ts"""))
c0022 = {r["country"]: r["n"] for r in spark.sql(
    "SELECT country, count(*) AS n FROM lab08_hc_silver WHERE customer_id = 'C0022' GROUP BY country").collect()}

# COMMAND ----------

# MAGIC %md
# MAGIC * The **new** orders were joined with the **latest** version of the dimension → `Canada`.
# MAGIC * The **old** silver rows still say `United States`: a stream only processes **new** input. Nothing re-joins old rows.
# MAGIC
# MAGIC Is that right? It depends on the question you ask: "where did the customer live **when** they ordered?" → correct
# MAGIC (point-in-time, like SCD2). "Revenue per **current** country?" → wrong, and you'd have to rebuild silver/gold yourself
# MAGIC (delete checkpoints + tables, re-run everything). A **materialized view** in a declarative pipeline is always
# MAGIC recomputed to match its current inputs — one of the reasons gold is usually an MV there.
# MAGIC
# MAGIC ## Part 7 · Count the moving parts
# MAGIC What did *you* have to manage for three tables?
# MAGIC
# MAGIC | You managed by hand | Where it bit you (or will in production) |
# MAGIC |---|---|
# MAGIC | 3 streaming queries + their **order** | a job with 3 dependent tasks, or a custom function like `run_medallion()` |
# MAGIC | 3 **checkpoints** + 1 **schema location** | never share, never delete by accident, new one after incompatible changes |
# MAGIC | Output modes, triggers, watermark | append vs complete, `availableNow` on serverless |
# MAGIC | Retries & restarts | Auto Loader's schema evolution **stops** the stream once → someone must restart it |
# MAGIC | Data quality | a `where()` silently drops rows — no metrics about **how many** or **why** |
# MAGIC | Recomputing gold after a dimension change | manual: drop tables, delete checkpoints, re-run |
# MAGIC | Lineage / monitoring | build it yourself (query progress, logs) |
# MAGIC
# MAGIC **Lakeflow Spark Declarative Pipelines** turn this around: you **declare** the three tables (a `STREAMING TABLE` for
# MAGIC bronze/silver, a `MATERIALIZED VIEW` for gold) and the pipeline handles ordering, checkpoints, retries, schema evolution,
# MAGIC data-quality metrics, lineage and incremental refresh. That's 08-L2.
# MAGIC
# MAGIC ## Part 8 · ✅ Automatic checks

# COMMAND ----------

# DBTITLE 1,Check your work
gold_total = spark.table("lab08_hc_gold").agg(F.sum("orders")).first()[0]
silver_now = spark.table("lab08_hc_silver").count()
_checks = {
    "bronze loaded 01.json (121 rows)": bronze_1 == 121,
    "silver: 1 cancelled + 1 duplicate removed (119)": silver_1 == 119,
    "only the new files flowed through (363 bronze / 357 silver)": (bronze_3, silver_3) == (363, 357),
    "day 4 arrived (484 bronze rows)": spark.table("lab08_hc_bronze").count() == 484,
    "silver has no duplicate order_id": silver_now == spark.table("lab08_hc_silver").select("order_id").distinct().count(),
    "silver has no cancelled orders": spark.table("lab08_hc_silver").where("quantity = 0").count() == 0,
    "order_ts is a timestamp": dict(spark.table("lab08_hc_silver").dtypes).get("order_ts") == "timestamp",
    "gold (complete mode) covers every silver order": gold_total == silver_now,
    "C0022: 2 old orders in United States, 2 new ones in Canada": c0022 == {"United States": 2, "Canada": 2},
}
for name, ok in _checks.items():
    print(("✅ " if ok else "❌ ") + name)
stop_all_streams()
print("\n🎉 Lab complete - next: 08-L2 · Your first declarative pipeline in SQL")
