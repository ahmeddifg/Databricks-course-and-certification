# Databricks notebook source
# MAGIC %md
# MAGIC # 🧪 Lab 08-L2 · Your First Declarative Pipeline in SQL (Guided)
# MAGIC **Time:** ~60 min · **Compute:** Serverless notebook + a **serverless pipeline** · **Works on Free Edition**
# MAGIC
# MAGIC The same ShopWave medallion as 08-L1 — but **declared** in three SQL files and run by **Lakeflow Spark Declarative
# MAGIC Pipelines (SDP)**. You'll create the pipeline in the UI, run it, feed it new files, read its **data-quality metrics**, break
# MAGIC it with a bad record, fix it, and tour its settings.
# MAGIC
# MAGIC | Part | You will… |
# MAGIC |---|---|
# MAGIC | 1 | Read the pipeline **source files** (`labs/pipelines/shopwave_sql/transformations/*.sql`) |
# MAGIC | 2 | 🖱️ **Create the pipeline** in the UI (or let a helper do it) |
# MAGIC | 3 | 🖱️ **Run** it — graph, tables, update details |
# MAGIC | 4 | Land new files and run again → **incremental** processing |
# MAGIC | 5 | **Expectations**: warn / drop metrics from the UI and the **event log** |
# MAGIC | 6 | A bad record → **FAIL UPDATE** → fix the code → rerun |
# MAGIC | 7 | **Full refresh**, lineage in the event log, pipeline datasets in Unity Catalog |
# MAGIC | 8 | 🖱️ Settings tour: triggered vs continuous, development vs production, configuration, schedule |
# MAGIC | 9 | ✅ Automatic checks |
# MAGIC
# MAGIC > 🆓 **Free Edition:** only **one active pipeline update per pipeline type** at a time — wait until a run finishes before
# MAGIC > starting another. A serverless pipeline in *standard* performance mode can take a few minutes to start.

# COMMAND ----------

# MAGIC %run ../../Includes/_setup

# COMMAND ----------

# MAGIC %run ./_08_prepare

# COMMAND ----------

# MAGIC %md
# MAGIC ### Start clean
# MAGIC ⚠️ This deletes the `08-L2 ShopWave orders (SQL)` pipeline **if you created it in an earlier attempt** (and with it its
# MAGIC tables), empties the `lab08/orders/` landing folder and delivers the first file. Skip it only if you want to continue an
# MAGIC earlier attempt.

# COMMAND ----------

# DBTITLE 1,Start clean (safe to re-run)
delete_pipeline("L2")
for t in ("sdp_orders_bronze", "sdp_customers", "sdp_products", "sdp_orders_silver", "sdp_order_items_silver",
          "sdp_daily_revenue", "sdp_country_revenue", "sdp_category_revenue"):
    for stmt in ("DROP TABLE IF EXISTS", "DROP MATERIALIZED VIEW IF EXISTS"):
        try:
            spark.sql(f"{stmt} {t}")
            break
        except Exception:
            pass
reset_feed("sdp_orders")
_ = land_until("sdp_orders", "01.json")
print("landed:", landed("sdp_orders"))
_silver_code = open(pipeline_source("shopwave_sql") + "/transformations/02_silver.sql").read()
if "EXPECT (order_id IS NOT NULL) ON VIOLATION DROP ROW" in _silver_code:
    print("⚠️ 02_silver.sql still contains your Part 6 fix from an earlier attempt. Set valid_order_id back to "
          "ON VIOLATION FAIL UPDATE before Part 6 (or discard the change in the Git dialog).")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1 · The source code: three SQL files, zero orchestration
# MAGIC A pipeline's code lives in **source files** (SQL or Python) inside the pipeline's **root folder** — by convention in a
# MAGIC `transformations/` sub-folder. The pipeline reads *all* of them, builds a **dependency graph** from the table references and
# MAGIC runs everything in the right order. File names and statement order don't matter.

# COMMAND ----------

# DBTITLE 1,Print the source files of this pipeline
_src = pipeline_source("shopwave_sql")
print("root folder:", _src, "\n")
for _f in sorted(os.listdir(f"{_src}/transformations")):
    print("=" * 30, _f, "=" * 30)
    print(open(f"{_src}/transformations/{_f}").read())

# COMMAND ----------

# MAGIC %md
# MAGIC Read them once and notice:
# MAGIC
# MAGIC | In the code | Meaning |
# MAGIC |---|---|
# MAGIC | `CREATE OR REFRESH STREAMING TABLE … FROM STREAM read_files(…)` | incremental ingestion with **Auto Loader** — no checkpoint, no schema location to manage |
# MAGIC | `CREATE OR REFRESH MATERIALIZED VIEW` (no `STREAM`) | recomputed from its sources on every update (incrementally when possible) |
# MAGIC | `FROM STREAM(sdp_orders_bronze)` | read another pipeline table **as a stream** (only new rows) — **no `LIVE.` prefix** any more |
# MAGIC | `CONSTRAINT … EXPECT (…)` + optional `ON VIOLATION DROP ROW` / `ON VIOLATION FAIL UPDATE` | **expectations** (data-quality rules) |
# MAGIC | `CREATE TEMPORARY VIEW` | a helper query inside the pipeline — never published to the catalog |
# MAGIC | `${dataset_path}` | a value from the pipeline **Configuration** — the code has no hard-coded paths |
# MAGIC
# MAGIC ## Part 2 · 🖱️ Create the pipeline
# MAGIC Run the next cell — it prints the exact values to type.

# COMMAND ----------

# DBTITLE 1,Values for the pipeline settings
_values = [
    ("Pipeline name", PIPELINES08["L2"]["name"]),
    ("Default catalog", catalog_name),
    ("Default schema", schema_name),
    ("Root folder", pipeline_source("shopwave_sql")),
    ("Source code path", pipeline_source("shopwave_sql") + "/transformations"),
    ("Configuration key", "dataset_path"),
    ("Configuration value", dataset_path),
]
for k, v in _values:
    print(f"{k:<20}: {v}")

# COMMAND ----------

# MAGIC %md
# MAGIC **Option A — in the UI (recommended the first time)** 🖱️
# MAGIC
# MAGIC 1. Left sidebar → **New** → **ETL pipeline** (or **Jobs & Pipelines** → **Create** → **ETL pipeline**).
# MAGIC 2. Name it exactly **`08-L2 ShopWave orders (SQL)`**. Under the name, pick the **default catalog** and **schema** printed
# MAGIC    above — unqualified table names in the code are published there.
# MAGIC 3. Choose **Add existing assets** (not *sample code*). As **pipeline root folder** select the `shopwave_sql` folder and as
# MAGIC    **source code path** its `transformations` folder (both printed above) → **Add**.
# MAGIC    *If you only see a blank editor:* open **Settings** → **Source code** → **Configure paths** → **Add path** → pick the
# MAGIC    `transformations` folder → **Save**.
# MAGIC 4. Open **Settings** (⚙️ in the editor header) → **Configuration** → **Add configuration**: key **`dataset_path`**, value =
# MAGIC    the volume path printed above → **Save**.
# MAGIC 5. Still in Settings, check: **Compute = Serverless** · **Pipeline mode = Triggered**. Leave the rest as it is.
# MAGIC
# MAGIC **Option B — let the notebook create the same pipeline** (REST API through the Databricks SDK). The next cell only creates
# MAGIC the pipeline if it doesn't exist yet, so it's also safe to run after Option A — it just prints the link.

# COMMAND ----------

# DBTITLE 1,Create the pipeline if it doesn't exist yet + link to it
if not find_pipeline("L2"):
    create_or_update_pipeline("L2")
pipeline_link("L2")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 3 · 🖱️ The first run
# MAGIC In the pipeline editor click **Run pipeline** (or run the next cell, which starts the same update and waits for it).
# MAGIC While it runs, look at:
# MAGIC
# MAGIC * the **pipeline graph** (right/bottom panel): 5 tables + 3 gold MVs, arrows = dependencies. The temporary view
# MAGIC   `sdp_orders_unique` is not a node you can query afterwards.
# MAGIC * the **update states**: *Initializing → Setting up tables → Running → Completed*.
# MAGIC * click a table node → rows written, and for `sdp_orders_silver` the **Data quality** tab.
# MAGIC
# MAGIC > Other buttons you'll meet in the editor: **Run file** (only the tables of the open file — fast iteration),
# MAGIC > **Dry run** (validate the code and the graph without writing any data), **Settings**, **Schedule**.

# COMMAND ----------

# DBTITLE 1,Run the pipeline (same as clicking "Run pipeline")
state_1 = run_pipeline("L2")

# COMMAND ----------

# DBTITLE 1,The pipeline's tables are normal Unity Catalog objects - query them anywhere
counts_1 = {t: table_count(t) for t in ("sdp_orders_bronze", "sdp_orders_silver", "sdp_order_items_silver",
                                        "sdp_customers", "sdp_products")}
print(counts_1)
display(spark.table("sdp_daily_revenue"))

# COMMAND ----------

# MAGIC %md
# MAGIC Expected: **121** bronze rows (the whole file), **120** in silver (the cancelled order was **dropped** by `not_cancelled`),
# MAGIC and gold shows **119** orders for 2026-07-01 because the temporary view removes the duplicate. `sdp_customers`: 300,
# MAGIC `sdp_products`: 36.
# MAGIC
# MAGIC ## Part 4 · Incremental processing
# MAGIC Two more days of orders arrive. Run the pipeline again.

# COMMAND ----------

# DBTITLE 1,Land 02.json and 03.json, then run the pipeline again
_ = land_until("sdp_orders", "03.json")
state_2 = run_pipeline("L2")
counts_2 = {t: table_count(t) for t in ("sdp_orders_bronze", "sdp_orders_silver")}
print(counts_2)
display(spark.table("sdp_daily_revenue").orderBy("order_date"))

# COMMAND ----------

# MAGIC %md
# MAGIC 🖱️ In the UI, open the latest update and click `sdp_orders_bronze`: it wrote **242** rows — just the two new files, like
# MAGIC your hand-coded stream in 08-L1, but without a checkpoint of your own. The event log records the same numbers:

# COMMAND ----------

# DBTITLE 1,Rows written per flow in the latest update (event log)
try:
    display(spark.sql("""
        WITH last_update AS (
            SELECT max_by(origin.update_id, timestamp) AS update_id
            FROM event_log(TABLE(sdp_orders_bronze)) WHERE event_type = 'flow_progress')
        SELECT e.timestamp, e.origin.flow_name AS flow_name,
               e.details:flow_progress.status::string                 AS status,
               e.details:flow_progress.metrics.num_output_rows::bigint AS output_rows
        FROM event_log(TABLE(sdp_orders_bronze)) AS e, last_update AS l
        WHERE e.event_type = 'flow_progress' AND e.origin.update_id = l.update_id
          AND e.details:flow_progress.metrics.num_output_rows IS NOT NULL
        ORDER BY e.timestamp"""))
except Exception as e:
    print("event log not readable here:", _first_line(e))

# COMMAND ----------

# MAGIC %md
# MAGIC > 💡 `event_log(TABLE(<any table of the pipeline>))` returns the event log of the pipeline that **owns** the table
# MAGIC > (only its owner may call it). `event_log('<pipeline-id>')` works too. Event types you should know: `flow_progress`
# MAGIC > (rows, data quality, backlog), `update_progress` (update states), `flow_definition` (lineage, schemas), `user_action`
# MAGIC > (who started/edited what), `create_update`.
# MAGIC
# MAGIC ## Part 5 · Expectations: warn and drop
# MAGIC `sdp_orders_silver` has three rules:
# MAGIC
# MAGIC | Expectation | Action | What it caught so far |
# MAGIC |---|---|---|
# MAGIC | `valid_order_id` | **FAIL UPDATE** | nothing (yet) |
# MAGIC | `not_cancelled` | **DROP ROW** | 1 cancelled order per file |
# MAGIC | `known_customer` | **warn** (default) | 1 order of `C9999` per file — the row is **kept**, `country` is NULL |
# MAGIC
# MAGIC 🖱️ UI: click `sdp_orders_silver` → **Data quality** tab. Code: the helper below runs the documented event-log query.

# COMMAND ----------

# DBTITLE 1,Data-quality metrics from the event log
dq_2 = expectation_metrics("sdp_orders_silver")
display(dq_2)
print(spark.sql("SELECT count(*) AS unknown_customer_rows FROM sdp_orders_silver WHERE country IS NULL").first())

# COMMAND ----------

# MAGIC %md
# MAGIC ```sql
# MAGIC -- what expectation_metrics() runs
# MAGIC SELECT e.dataset, e.name, SUM(e.passed_records) AS passed, SUM(e.failed_records) AS failed
# MAGIC FROM (SELECT explode(from_json(details:flow_progress:data_quality:expectations,
# MAGIC              'array<struct<name: string, dataset: string, passed_records: bigint, failed_records: bigint>>')) AS e
# MAGIC       FROM event_log(TABLE(sdp_orders_silver))
# MAGIC       WHERE event_type = 'flow_progress')
# MAGIC GROUP BY e.dataset, e.name
# MAGIC ```
# MAGIC
# MAGIC ## Part 6 · A broken record → FAIL UPDATE
# MAGIC The order system sends a record **without an `order_id`**. `valid_order_id` is a **fail** expectation.

# COMMAND ----------

# DBTITLE 1,Land a file with a bad order and run
_bad = {"order_id": None, "order_timestamp": 1783209600, "customer_id": "C0001", "quantity": 1, "total": 19.99,
        "items": [{"product_id": "P001", "quantity": 1, "subtotal": 19.99}]}
_write_text(f"{landing('sdp_orders')}/zz_bad_order.json", json.dumps(_bad) + "\n")
state_fail = run_pipeline("L2")

# COMMAND ----------

# MAGIC %md
# MAGIC Expected: **FAILED** ❌ — with a message naming the expectation `valid_order_id`. 🖱️ Open the failed update in the UI: the
# MAGIC graph shows which flow failed; bronze **did** ingest the file (it has no rule), silver's transaction was **rolled back**,
# MAGIC and the downstream tables were not updated.
# MAGIC
# MAGIC **Fix it.** Deciding what to do with bad data is *your* call — here: drop such rows and count them.
# MAGIC
# MAGIC 1. 🖱️ In the pipeline editor open `transformations/02_silver.sql`.
# MAGIC 2. Change the `valid_order_id` line to
# MAGIC
# MAGIC    `CONSTRAINT valid_order_id  EXPECT (order_id IS NOT NULL) ON VIOLATION DROP ROW,`
# MAGIC 3. The file saves automatically. Run the next cell (or click **Run pipeline**).
# MAGIC
# MAGIC > Git folder note: this edits a file of the course repo. Keep the change for now (08-L2's checks expect it); you can
# MAGIC > **discard** it later in the Git dialog.

# COMMAND ----------

# DBTITLE 1,Rerun after the fix
state_fixed = run_pipeline("L2")
counts_3 = {t: table_count(t) for t in ("sdp_orders_bronze", "sdp_orders_silver")}
print(counts_3)
display(expectation_metrics("sdp_orders_silver"))

# COMMAND ----------

# MAGIC %md
# MAGIC Silver's checkpoint had **not** advanced past the bad row (the failed transaction was rolled back), so the rerun
# MAGIC processed the same new bronze rows again — now with the new rule — and `valid_order_id` shows **1 failed record** (dropped).
# MAGIC No data lost, no duplicates.
# MAGIC
# MAGIC ## Part 7 · Full refresh, lineage and the datasets in Unity Catalog
# MAGIC **Refresh** (default) processes only new data. **Full refresh** truncates the streaming tables, clears their checkpoints
# MAGIC and reprocesses **all** source files still available. 🖱️ In the editor: arrow next to **Run pipeline** → **Run pipeline
# MAGIC with full table refresh** — or run the cell.

# COMMAND ----------

# DBTITLE 1,Full refresh (same result - all 4 files are still in the landing folder)
state_full = run_pipeline("L2", full_refresh=True)
counts_full = {t: table_count(t) for t in ("sdp_orders_bronze", "sdp_orders_silver")}
print(counts_full)

# COMMAND ----------

# MAGIC %md
# MAGIC ⚠️ **Exam trap:** a full refresh re-reads the **source**. If the source has lost data (Kafka retention, files cleaned up by
# MAGIC a lifecycle rule), a full refresh **loses** that data from the streaming table. Protect such tables with the table
# MAGIC property `pipelines.reset.allowed = false`.

# COMMAND ----------

# DBTITLE 1,Lineage from the event log (flow_definition events)
display(spark.sql("""
    SELECT DISTINCT details:flow_definition.output_dataset::string AS output_dataset,
                    details:flow_definition.input_datasets::string AS input_datasets,
                    details:flow_definition.flow_type::string      AS flow_type
    FROM event_log(TABLE(sdp_orders_bronze))
    WHERE details:flow_definition IS NOT NULL
    ORDER BY output_dataset"""))

# COMMAND ----------

# DBTITLE 1,What kind of objects did the pipeline publish?
display(spark.sql("SHOW TABLES LIKE 'sdp_*'"))
_types = {}
for _t in ("sdp_orders_bronze", "sdp_daily_revenue"):
    _info = {r["col_name"]: r["data_type"] for r in spark.sql(f"DESCRIBE TABLE EXTENDED {_t}").collect()}
    _types[_t] = str(_info.get("Type"))
print(_types)

# COMMAND ----------

# MAGIC %md
# MAGIC * Streaming tables and materialized views are **Unity Catalog** objects — `Type` shows `STREAMING_TABLE` /
# MAGIC   `MATERIALIZED_VIEW`. You can `SELECT` them from any notebook, SQL warehouse or dashboard.
# MAGIC * They are **managed by the pipeline**: only the pipeline writes them. Deleting the pipeline drops them (unless you
# MAGIC   choose to keep them).
# MAGIC * The temporary view `sdp_orders_unique` does **not** exist outside the pipeline.
# MAGIC
# MAGIC ## Part 8 · 🖱️ Settings tour
# MAGIC Open **Settings** in the pipeline editor and find each item:
# MAGIC
# MAGIC | Setting | Values | What it means |
# MAGIC |---|---|---|
# MAGIC | **Pipeline mode** | **Triggered** (default) / Continuous | Triggered: process what's there, then stop (schedule it with a job). Continuous: keep running, process data as it arrives |
# MAGIC | **Compute** | **Serverless** / classic | Serverless: no cluster config; classic: workers, autoscaling, Photon, **product edition** |
# MAGIC | **Product edition** (classic) | Core · Pro · **Advanced** | Core = streaming ingest · Pro = + CDC (`AUTO CDC`) · Advanced = + expectations |
# MAGIC | **Channel** | **Current** / Preview | runtime version for the pipeline — Preview to test upcoming changes |
# MAGIC | **Default catalog / schema** | yours | where unqualified names are published (you can still write `catalog.schema.table` in the code) |
# MAGIC | **Configuration** | `dataset_path = /Volumes/…` | key-value pairs: `${key}` in SQL, `spark.conf.get("key")` in Python |
# MAGIC | **Notifications** | e-mail on success / failure | |
# MAGIC | **Run as** | user or **service principal** | identity used for production runs |
# MAGIC | **Event log** | publish to a UC table | optional; by default the event log is hidden and read with `event_log()` |
# MAGIC
# MAGIC **Development vs production behaviour** — updates you start from the **editor/UI** use fast-start, debugging-friendly
# MAGIC behaviour: compute is **reused** and **retries are disabled** so you see errors immediately. Updates started by a **job**, the
# MAGIC **API** or a continuous pipeline use production behaviour: **automatic retries** and compute **restarts** on recoverable
# MAGIC errors, and compute is released as soon as the update ends. (Older UIs had an explicit **Development / Production** toggle
# MAGIC — same idea, still asked on the exam.)
# MAGIC
# MAGIC 🖱️ Finally click **Schedule** → *Add schedule*: that creates a **Lakeflow Job** with a **pipeline task** (Section 10). Cancel
# MAGIC it for now — nothing should run while you're not watching.
# MAGIC
# MAGIC ## Part 9 · ✅ Automatic checks

# COMMAND ----------

# DBTITLE 1,Check your work
try:
    _dq = {r["expectation"]: (r["passed_records"], r["failed_records"])
           for r in expectation_metrics("sdp_orders_silver").collect()}
except Exception as e:
    _dq = {}
    print("⚠️ could not read the event log:", _first_line(e))
_silver = spark.table("sdp_orders_silver")
_checks = {
    "first run succeeded": state_1 == "COMPLETED",
    "first run: 121 bronze / 120 silver rows": (counts_1.get("sdp_orders_bronze"), counts_1.get("sdp_orders_silver")) == (121, 120),
    "incremental run: 363 bronze / 360 silver": (counts_2.get("sdp_orders_bronze"), counts_2.get("sdp_orders_silver")) == (363, 360),
    "gold counts every order once (357)": spark.table("sdp_daily_revenue").agg(F.sum("orders")).first()[0] == 357,
    "not_cancelled dropped the cancelled orders": _dq.get("not_cancelled", (0, 0))[1] >= 3,
    "known_customer only warned (C9999 rows kept)": _dq.get("known_customer", (0, 0))[1] >= 3
                                                   and _silver.where("customer_id = 'C9999'").count() == 3,
    "the bad record FAILED the update": state_fail == "FAILED",
    "after the fix the update COMPLETED and the bad row was dropped": state_fixed == "COMPLETED"
                                                                        and _silver.where("order_id IS NULL").count() == 0,
    "full refresh rebuilt the same tables (364 / 360)": (counts_full.get("sdp_orders_bronze"), counts_full.get("sdp_orders_silver")) == (364, 360),
    "order lines = sum of item array sizes": spark.table("sdp_order_items_silver").count()
                                             == _silver.agg(F.sum(F.size("items"))).first()[0],
    "bronze is a streaming table, gold a materialized view": "STREAMING" in _types.get("sdp_orders_bronze", "").upper()
                                                             and "MATERIALIZED" in _types.get("sdp_daily_revenue", "").upper(),
}
for name, ok in _checks.items():
    print(("✅ " if ok else "❌ ") + name)
print("\n🎉 Lab complete - next: 08-L3 · Python pipelines, AUTO CDC and the event log")
