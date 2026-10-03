# Databricks notebook source
# MAGIC %md
# MAGIC # 🏆 Lab 12-L5 · Challenge Lab — The ShopWave Performance Review
# MAGIC **Time:** ~60 min · **Compute:** Serverless notebook · **Works on Free Edition** · No step-by-step instructions — only the goal,
# MAGIC hints and automatic checks. Solution: `12-L5 - Challenge Lab (Solution)`.
# MAGIC
# MAGIC Management wants a written performance review of the production job **ShopWave Nightly (prod)** before the quarter closes.
# MAGIC You have **30 nights of run history** and the clickstream table. Answer with SQL / PySpark, fix what you find, and prove it.
# MAGIC
# MAGIC | Table | Grain | Columns |
# MAGIC |---|---|---|
# MAGIC | `ch12_job_runs` | one row per run (30) | `run_id`, `run_date` (2026-09-01 … 30), `result_state`, `setup_seconds` (compute start-up), `run_duration_seconds` |
# MAGIC | `ch12_task_runs` | one row per task run (120) | `run_id`, `run_date`, `task_key`, `result_state` (`SUCCESS`, `FAILED`, `UPSTREAM_FAILED`), `execution_seconds` |
# MAGIC | `perf_events` | 3M events | the clickstream (30 % from `C0007`) |
# MAGIC
# MAGIC The DAG of the job: `extract → (transform_orders, transform_clicks) → publish`.
# MAGIC
# MAGIC | Task | Objective | Points |
# MAGIC |---|---|---|
# MAGIC | 1 | Baseline per task — view `ch12_v_baseline` | 6.1 |
# MAGIC | 2 | Regressions — view `ch12_v_regressions` | 6.1 |
# MAGIC | 3 | Blockers and failure rate — view `ch12_v_failures` + `failure_rate_pct` | 6.2 |
# MAGIC | 4 | The day-18 spike (MCQ) | 6.1 / 6.5 |
# MAGIC | 5 | Re-layout the events table — `ch12_events` | 6.4 |
# MAGIC | 6 | Fix the skewed aggregation — salted two-stage revenue per customer | 6.3 |
# MAGIC | 7–8 | Two diagnosis questions (MCQ) | 6.4 / 6.5 |

# COMMAND ----------

# MAGIC %run ../../Includes/_setup

# COMMAND ----------

# MAGIC %run ./_12_prepare

# COMMAND ----------

# DBTITLE 1,Start clean (safe to re-run)
for _v in ("ch12_v_baseline", "ch12_v_regressions", "ch12_v_failures"):
    spark.sql(f"DROP VIEW IF EXISTS {_v}")
spark.sql("DROP TABLE IF EXISTS ch12_events")
print("ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 1 · Baseline per task
# MAGIC Create the view **`ch12_v_baseline`** with columns `task_key`, `baseline_s`: the **median** `execution_seconds` of each task
# MAGIC over the **successful** task runs of the first 20 nights (`run_date <= '2026-09-20'`).
# MAGIC
# MAGIC > 💡 `percentile(col, 0.5)` or `median(col)`. Why the median and not `avg`?

# COMMAND ----------

# DBTITLE 1,Task 1
# MAGIC %sql
# MAGIC -- TODO: CREATE OR REPLACE VIEW ch12_v_baseline AS ...
# MAGIC SELECT 'replace me' AS todo

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 2 · Regressions
# MAGIC Create the view **`ch12_v_regressions`** with columns `task_key`, `run_date`, `execution_seconds`, `baseline_s`, `ratio`:
# MAGIC every **successful** task run whose execution time is **more than 1.5 ×** its baseline (`ratio` = execution / baseline,
# MAGIC rounded to 2 decimals).

# COMMAND ----------

# DBTITLE 1,Task 2
# MAGIC %sql
# MAGIC -- TODO: CREATE OR REPLACE VIEW ch12_v_regressions AS ...
# MAGIC SELECT 'replace me' AS todo

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 3 · Blockers, victims and the failure rate
# MAGIC 1. Create the view **`ch12_v_failures`** with one row per **failed run**: `run_id`, `run_date`, `blocker` (the `task_key`
# MAGIC    that **FAILED**), `victims` (number of `UPSTREAM_FAILED` tasks in that run).
# MAGIC 2. Set the Python variable **`failure_rate_pct`** = failed runs / all runs × 100, rounded to 1 decimal (from `ch12_job_runs`).

# COMMAND ----------

# DBTITLE 1,Task 3a
# MAGIC %sql
# MAGIC -- TODO: CREATE OR REPLACE VIEW ch12_v_failures AS ...
# MAGIC SELECT 'replace me' AS todo

# COMMAND ----------

# DBTITLE 1,Task 3b
failure_rate_pct = None   # TODO

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 4 · The day-18 spike
# MAGIC On 2026-09-18 the run took **991 s** instead of ~540 s, but no task was slower than its baseline. Look at the data
# MAGIC (`ch12_job_runs`) and choose the best explanation:
# MAGIC
# MAGIC * **A.** `transform_clicks` regressed because of data skew
# MAGIC * **B.** The run waited for **compute start-up** (`setup_seconds` = 540): e.g. cloud capacity or a cold pool — not a code
# MAGIC   or data problem; check the compute event log / consider serverless or a pool
# MAGIC * **C.** The job was triggered twice and the runs were queued
# MAGIC * **D.** Predictive optimization rewrote the tables during the run

# COMMAND ----------

# DBTITLE 1,Task 4
answer_task4 = None   # "A", "B", "C" or "D"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 5 · Re-layout the events table
# MAGIC The dashboards on top of the clickstream filter by **customer** and by **date range**. Create **`ch12_events`** with all
# MAGIC rows of `perf_events`, using the layout Databricks recommends for new tables, keyed for those filters, so that a
# MAGIC single-customer filter reads **at most half** of the table's files. Use `TBLPROPERTIES ('delta.targetFileSize' = '4mb')`
# MAGIC so the small demo table has several files.
# MAGIC
# MAGIC > 💡 Which layout can't be combined with ZORDER or PARTITIONED BY? Which command clusters the data that was written before?
# MAGIC > Check yourself with `layout("ch12_events")` and `skipping_report("ch12_events", "customer_id", "C0123")`.

# COMMAND ----------

# DBTITLE 1,Task 5
# MAGIC %sql
# MAGIC -- TODO: CREATE TABLE ch12_events ... AS SELECT * FROM perf_events;
# MAGIC -- TODO: OPTIMIZE ...
# MAGIC SELECT 'replace me' AS todo

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 6 · Fix the skewed aggregation
# MAGIC `transform_clicks` regressed because `C0007` (30 % of the events) sends a third of the rows to one task. Compute the
# MAGIC number of **distinct products viewed** per customer (`event_type = 'view'`) with a **salted two-stage aggregation**:
# MAGIC
# MAGIC * `salted` — the view events with a `salt` column (16 values),
# MAGIC * `result_df` — columns `customer_id`, `distinct_products` (LONG), the same values as the direct
# MAGIC   `countDistinct("product_id")`.
# MAGIC
# MAGIC > 💡 Stage 1: group by `(customer_id, salt)` and collect the set of products. Stage 2: group by `customer_id`, merge the sets.

# COMMAND ----------

# DBTITLE 1,Task 6
salted = None      # TODO: DataFrame of the view events + a salt column
result_df = None   # TODO: customer_id, distinct_products

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 7 · Predictive optimization
# MAGIC Which statement is **true**?
# MAGIC * **A.** It runs `OPTIMIZE`, `VACUUM` and `ANALYZE` automatically on Unity Catalog **managed** tables, and can be set with
# MAGIC   `ALTER CATALOG|SCHEMA|TABLE … ENABLE | DISABLE | INHERIT PREDICTIVE OPTIMIZATION`
# MAGIC * **B.** It works on external tables only, because their files are in your own storage
# MAGIC * **C.** It replaces liquid clustering: tables with predictive optimization can't use `CLUSTER BY`
# MAGIC * **D.** It must be scheduled as a job with a cron expression
# MAGIC
# MAGIC ## Task 8 · A notebook keeps restarting
# MAGIC A notebook does `rows = spark.table("perf_events").collect()` and then loops over the rows in Python. On a bigger table the
# MAGIC message *“The spark driver has stopped unexpectedly and is restarting”* appears. Best fix?
# MAGIC * **A.** Add more workers
# MAGIC * **B.** Enable predictive optimization
# MAGIC * **C.** Increase `spark.sql.shuffle.partitions`
# MAGIC * **D.** Express the loop as a Spark transformation (filter / aggregate / join) and only collect a small result —
# MAGIC   it's a **driver** out-of-memory problem

# COMMAND ----------

# DBTITLE 1,Tasks 7-8
answer_task7 = None   # "A", "B", "C" or "D"
answer_task8 = None

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Checks

# COMMAND ----------

# DBTITLE 1,Run all checks
_score = 0


def _check(n, label, fn):
    global _score
    try:
        ok = bool(fn())
    except Exception as e:
        print(f"❌ Task {n}: {label} — {_first_line(e)}")
        return
    _score += ok
    print(("✅" if ok else "❌") + f" Task {n}: {label}")


def _t1():
    b = {r["task_key"]: float(r["baseline_s"]) for r in spark.table("ch12_v_baseline").collect()}
    return b == {"extract": 119.0, "publish": 59.0, "transform_clicks": 241.0, "transform_orders": 300.0}


def _t2():
    rows = spark.table("ch12_v_regressions").collect()
    return ({r["task_key"] for r in rows} == {"transform_clicks"} and len(rows) == 6
            and str(min(r["run_date"] for r in rows)) == "2026-09-24"
            and all(2.4 <= float(r["ratio"]) <= 2.75 for r in rows))


def _t3():
    rows = {int(r["run_id"]): (r["blocker"], int(r["victims"])) for r in spark.table("ch12_v_failures").collect()}
    return rows == {880012: ("transform_orders", 1), 880027: ("extract", 3)} and failure_rate_pct == 6.7


def _t5():
    info = layout("ch12_events", verbose=False)
    hit, total = files_scanned("ch12_events", "customer_id", "C0123")
    return (spark.table("ch12_events").count() == PERF_ROWS and set(info["clustered_by"]) == {"customer_id", "event_date"}
            and not info["partitioned_by"] and total >= 3 and hit <= total / 2)


def _t6():
    direct = (spark.table("perf_events").where("event_type = 'view'").groupBy("customer_id")
                   .agg(F.countDistinct("product_id").alias("distinct_products")))
    same = result_df.exceptAll(direct).count() == 0 and direct.exceptAll(result_df).count() == 0
    m = stage_summary(salted.repartition(16, "customer_id", "salt"), 16, "your salted shuffle")
    return "salt" in salted.columns and same and (not m["skewed"] or m["max_share"] < 15)


_check(1, "baseline medians per task", _t1)
_check(2, "the regression: transform_clicks, 6 runs from 2026-09-24, ~2.4-2.7x", _t2)
_check(3, "blockers, victims and failure rate 6.7 %", _t3)
_check(4, "the day-18 spike", lambda: answer_task4 == "B")
_check(5, "ch12_events: liquid clustered by customer_id + event_date, customer filter reads ≤ 50 % of files", _t5)
_check(6, "salted two-stage aggregation: same result, balanced tasks", _t6)
_check(7, "predictive optimization", lambda: answer_task7 == "A")
_check(8, "driver out of memory", lambda: answer_task8 == "D")
print(f"\n🏁 score: {_score}/8" + ("  — 🎉 performance review complete!" if _score == 8 else ""))
