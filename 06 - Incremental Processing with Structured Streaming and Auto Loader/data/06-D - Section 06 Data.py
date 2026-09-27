# Databricks notebook source
# MAGIC %md
# MAGIC # 🗂️ 06-D · Data used in Section 06
# MAGIC
# MAGIC Streaming needs data that **arrives over time**. Every source below has a *staging* folder (all files, never read by the
# MAGIC labs) and a *landing* folder that a helper fills **one file at a time**.
# MAGIC
# MAGIC | Source | Staging → landing | Files × rows | Special features | Helper | Used in |
# MAGIC |---|---|---|---|---|---|
# MAGIC | Orders | `orders-staging/` → `orders-landing/` | 10 × 121 JSON (1 duplicate each, ~1 cancelled) | one file per day, 2026-07-01 … 07-10 | `land_new_orders(n)` / `reset_orders_landing()` (from `Includes/_setup`) | 06-L1, 06-L4 |
# MAGIC | App events | `events-staging/` → `events-landing/` | 4 × 50 JSON | batch 3 adds **`coupon`**; batch 4 has 3 `amount` values as text (`"12.50 EUR"`) | `land_events(n)` / `reset_events_landing()` | 06-L2 |
# MAGIC | Clicks | `clicks-staging/` → `clicks-landing/` | 40 + 45 + 40 + 44 JSON | event time 10:00–17:57; batch 2 re-sends 5 clicks; batch 4 has 4 clicks from 10:10–10:13 (late) | `land_clicks(n)` / `reset_clicks_landing()` | 06-L3 |
# MAGIC
# MAGIC Checkpoints and schema locations live in the course volume under `…/shopwave/checkpoints/lab06/<stream>` (helper
# MAGIC `checkpoint(name)`). Tables are named `lab06_*` in `<catalog>.shopwave`.

# COMMAND ----------

# MAGIC %run ../../Includes/_setup

# COMMAND ----------

# MAGIC %run ../labs/_06_prepare

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔍 Preview the sources

# COMMAND ----------

# DBTITLE 1,App events - per batch
display(spark.read.json(f"{dataset_path}/events-staging")
             .select("*", F.col("_metadata.file_name").alias("file"))
             .groupBy("file").agg(F.count("*").alias("rows"), F.count("coupon").alias("with_coupon"))
             .orderBy("file"))

# COMMAND ----------

# DBTITLE 1,Clicks - event-time range per batch
display(spark.read.schema("click_id STRING, user_id STRING, page STRING, event_time TIMESTAMP")
             .json(f"{dataset_path}/clicks-staging")
             .select("*", F.col("_metadata.file_name").alias("file"))
             .groupBy("file").agg(F.count("*").alias("rows"), F.min("event_time").alias("first"), F.max("event_time").alias("last"))
             .orderBy("file"))

# COMMAND ----------

# DBTITLE 1,What is currently landed?
for folder in ("orders-landing", "events-landing", "clicks-landing"):
    path = f"{dataset_path}/{folder}"
    files = [f.name for f in dbutils.fs.ls(path)] if path_exists(path) else []
    print(f"{folder:<16} {files}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧹 Reset Section 06
# MAGIC Uncomment to stop all streams, drop every `lab06_*` table, delete the `lab06` checkpoints and reset the three landing folders.

# COMMAND ----------

# DBTITLE 1,Reset (optional)
# reset_lab06()
