# Databricks notebook source
# MAGIC %md
# MAGIC # 🗂️ 12-D · Data used in Section 12
# MAGIC
# MAGIC Section 12 needs data that is **big enough to show performance effects** and **skewed on purpose**. `labs/_12_prepare`
# MAGIC generates it deterministically with Spark (`range()` + hash functions) — everybody gets exactly the same rows.
# MAGIC
# MAGIC | Table | Rows | Columns | Built from |
# MAGIC |---|---|---|---|
# MAGIC | `perf_events` | **3,000,000** | `event_id`, `event_ts`, `event_date` (2026-01-01 … 2026-06-30, 181 days), `customer_id`, `product_id`, `event_type` (`view` / `cart` / `purchase`), `device` (`web` / `ios` / `android`), `amount` (purchases only) | generated, 16 partitions |
# MAGIC | `perf_customers` | 300 | `customer_id`, `first_name`, `last_name`, `country`, `city` | `customers-json` |
# MAGIC | `perf_products` | 36 | `product_id`, `title`, `brand`, `category`, `price` | `products-csv` |
# MAGIC | `ch12_job_runs` | 30 | `run_id`, `run_date` (2026-09-01 … 30), `result_state`, `setup_seconds`, `run_duration_seconds` | generated run history of *ShopWave Nightly (prod)* |
# MAGIC | `ch12_task_runs` | 120 | `run_id`, `run_date`, `task_key`, `result_state`, `execution_seconds` | the same runs, one row per task |
# MAGIC | `perf_job_log` | grows | `run_id`, `step`, `status`, `seconds`, `rows_processed`, `logged_at` | written by the 12-L1 job tasks |
# MAGIC
# MAGIC | Key figure | Value |
# MAGIC |---|---|
# MAGIC | Events of the hot customer **`C0007`** | **907,522** (30.25 %) — next biggest customer ≈ 7,200 (0.24 %) |
# MAGIC | Views · carts · purchases | 1,715,041 · 856,354 · **428,605** |
# MAGIC | Purchase revenue | 109,439,204.28 |
# MAGIC | Baseline (median, nights 1–20) extract · transform_orders · transform_clicks · publish | 119 s · 300 s · 241 s · 59 s |
# MAGIC | Regression | `transform_clicks` ≈ 2.4–2.7 × from 2026-09-24 (6 successful runs) |
# MAGIC | Failed runs | 2026-09-12 (`transform_orders` → 1 victim), 2026-09-27 (`extract` → 3 victims) = 6.7 % |
# MAGIC | Compute start-up spike | 2026-09-18: `setup_seconds` = 540, run 991 s |
# MAGIC
# MAGIC **The run-history story (`ch12_*`)**
# MAGIC
# MAGIC ```
# MAGIC night  1-11  ~540 s  ✅            night 18  991 s ✅  ← waited 540 s for compute, tasks normal
# MAGIC night 12     ❌ transform_orders failed → publish UPSTREAM_FAILED
# MAGIC night 24-30  ~830-900 s ✅  ← transform_clicks 2.6× slower (C0007 bot traffic, skewed join)
# MAGIC night 27     ❌ extract failed → 3 tasks UPSTREAM_FAILED
# MAGIC ```
# MAGIC
# MAGIC **Objects the labs create**
# MAGIC
# MAGIC | Lab | Objects |
# MAGIC |---|---|
# MAGIC | 12-L1 | job **`12-L1 ShopWave Nightly`** (4 notebook tasks running `labs/tasks/work`), rows in `perf_job_log` |
# MAGIC | 12-L2 | — (temporary views only) |
# MAGIC | 12-L3 | `perf_small`, `perf_part_date`, `perf_zorder`, `perf_liquid`, `perf_liquid_df`, `perf_auto` |
# MAGIC | 12-L4 | `perf_bot_events` (dropped at the end), notebook-scoped library `humanize` |
# MAGIC | 12-L5 | views `ch12_v_baseline`, `ch12_v_regressions`, `ch12_v_failures`, table `ch12_events` |

# COMMAND ----------

# MAGIC %run ../../Includes/_setup

# COMMAND ----------

# MAGIC %run ../labs/_12_prepare

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔍 Preview

# COMMAND ----------

# DBTITLE 1,Tables and row counts
_rows = []
for _t in ("perf_events", "perf_customers", "perf_products", "ch12_job_runs", "ch12_task_runs", "perf_job_log",
           "perf_small", "perf_part_date", "perf_zorder", "perf_liquid", "ch12_events"):
    _rows.append((_t, spark.table(_t).count() if spark.catalog.tableExists(_t) else None))
display(spark.createDataFrame(_rows, "object STRING, row_count LONG"))

# COMMAND ----------

# DBTITLE 1,The skew in one query
# MAGIC %sql
# MAGIC SELECT customer_id, count(*) AS events, round(100 * count(*) / sum(count(*)) OVER (), 2) AS pct
# MAGIC FROM perf_events GROUP BY customer_id ORDER BY events DESC LIMIT 5

# COMMAND ----------

# DBTITLE 1,Event types and revenue
# MAGIC %sql
# MAGIC SELECT event_type, count(*) AS events, sum(amount) AS revenue FROM perf_events GROUP BY event_type ORDER BY events DESC

# COMMAND ----------

# DBTITLE 1,The 30 nights of run history
# MAGIC %sql
# MAGIC SELECT j.run_date, j.result_state, j.setup_seconds, j.run_duration_seconds,
# MAGIC        max(CASE WHEN t.task_key = 'extract'          THEN t.execution_seconds END) AS extract_s,
# MAGIC        max(CASE WHEN t.task_key = 'transform_orders' THEN t.execution_seconds END) AS transform_orders_s,
# MAGIC        max(CASE WHEN t.task_key = 'transform_clicks' THEN t.execution_seconds END) AS transform_clicks_s,
# MAGIC        max(CASE WHEN t.task_key = 'publish'          THEN t.execution_seconds END) AS publish_s
# MAGIC FROM ch12_job_runs j JOIN ch12_task_runs t USING (run_id)
# MAGIC GROUP BY ALL ORDER BY j.run_date

# COMMAND ----------

# DBTITLE 1,Layout of the Section 12 tables that exist
for _t in ("perf_events", "perf_small", "perf_part_date", "perf_zorder", "perf_liquid", "ch12_events"):
    if spark.catalog.tableExists(_t):
        layout(_t)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧹 Reset Section 12
# MAGIC `reset_lab12(confirm="YES")` deletes the `12-L…` jobs and drops all `perf_*` / `ch12_*` tables (the ShopWave base dataset
# MAGIC stays). The next `%run ./_12_prepare` rebuilds the tables.

# COMMAND ----------

# DBTITLE 1,Reset (optional)
# reset_lab12(confirm="YES")
