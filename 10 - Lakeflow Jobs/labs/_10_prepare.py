# Databricks notebook source
# MAGIC %md
# MAGIC # 🔧 _10_prepare — Section 10 data, job-task logic & Jobs API helpers
# MAGIC Included by every Section 10 lab **and by every job-task notebook** in `labs/tasks/`:
# MAGIC
# MAGIC ```
# MAGIC %run ../../Includes/_setup        (task notebooks: %run ../../../Includes/_setup)
# MAGIC %run ./_10_prepare                (task notebooks: %run ../_10_prepare)
# MAGIC ```
# MAGIC
# MAGIC It is idempotent and quick (no heavy work at include time): it creates the inbox folder and the small control tables, and
# MAGIC defines functions.
# MAGIC
# MAGIC **The story.** Every night ShopWave receives **one JSON file per day** of July orders. Section 10 orchestrates the
# MAGIC processing of those files with **Lakeflow Jobs**:
# MAGIC
# MAGIC ```
# MAGIC orders-staging/01.json … 10.json  ──land──▶  raw/lab10/inbox/  ──load──▶  jobs_bronze_orders  ──▶  jobs_silver_orders  ──▶  jobs_gold_daily
# MAGIC                                                                                     └─ quality ─▶ jobs_quarantine        └─▶ jobs_country_report
# MAGIC ```
# MAGIC
# MAGIC | Object | What it is |
# MAGIC |---|---|
# MAGIC | `raw/lab10/inbox/` (volume folder) | where the daily files arrive (`land10()` copies the next staged file) — also the **file arrival trigger** path |
# MAGIC | `jobs_bronze_orders` | raw orders + `source_file`, `batch_id` (one batch per load), `ingested_at` |
# MAGIC | `jobs_silver_orders` | deduplicated, cancelled orders removed, `order_date`, `country`, `known_customer` |
# MAGIC | `jobs_gold_daily` | one row per day: `orders`, `revenue`, `avg_order_value`, `unknown_customer_orders` |
# MAGIC | `jobs_quarantine` · `jobs_country_report` · `jobs_published` · `jobs_gold_kpis` | outputs of the other tasks |
# MAGIC | `jobs_task_log` | every task writes one line here: *which task ran, which attempt, what it did* |
# MAGIC | `jobs_control` | a tiny config table (`publish_enabled`) used to make a task fail on purpose |
# MAGIC
# MAGIC Each daily file has **121 rows**: **119 valid orders** + **3 bad rows** (1 exact duplicate, 1 cancelled order, 1 order of the
# MAGIC unknown customer `C9999`).
# MAGIC
# MAGIC **Task functions** (called by the notebooks in `labs/tasks/`): `task_land_orders`, `task_load_bronze`,
# MAGIC `task_quality_check`, `task_build_silver_gold`, `task_quarantine_report`, `task_call_partner_api`, `task_country_report`,
# MAGIC `task_publish`, `task_notify_failure`, `task_cleanup`.
# MAGIC
# MAGIC **Lab helpers:** `land10(n)`, `reset_lab10_data()`, `set_control(key, value)`, `task_log()`, `task_path(name)`,
# MAGIC `write_sql_task_file()` · Jobs API: `find_job(name)`, `job_link(name)`, `job_summary(name)`, `job_runs(name)`,
# MAGIC `run_summary(run_id)`, `run_job_now(name, params)`, `create_or_replace_job(spec)`, `pause_lab10_triggers()` ·
# MAGIC pipeline: `create_or_update_pipeline10()`, `find_pipeline10()` · `reset_lab10(confirm="YES")`.

# COMMAND ----------

# DBTITLE 1,Section 10 configuration, tables and the inbox
import json
import os
import time
import datetime as _dt
from pyspark.sql import functions as F
from pyspark.sql.window import Window

LAB10 = f"{dataset_path}/lab10"
INBOX10 = f"{LAB10}/inbox"                                  # the daily files arrive here (file arrival trigger path)
_STAGING10 = f"{dataset_path}/orders-staging"                # 01.json = 1 July ... 10.json = 10 July
DATA_TABLES10 = ("jobs_bronze_orders", "jobs_silver_orders", "jobs_gold_daily", "jobs_quarantine",
                 "jobs_country_report", "jobs_published", "jobs_gold_kpis", "jobs_task_log", "jobs_control",
                 "jobs_customers")
JOB_NAMES10 = {
    "L1": "10-L1 ShopWave Daily Load",
    "L2": "10-L2 ShopWave Control Flow",
    "L3_FILE": "10-L3 File Arrival Load",
    "L3_TABLE": "10-L3 KPIs on Silver Update",
    "L3_PIPE": "10-L3 Land and Pipeline",
    "L4": "10-L4 Finance Close",
}


def job_table(name: str) -> str:
    """Fully qualified name of a Section 10 table (use it in SQL files and job settings)."""
    return f"{catalog_name}.{schema_name}.{name}"


def _ensure_lab10():
    dbutils.fs.mkdirs(INBOX10)
    spark.sql("""CREATE TABLE IF NOT EXISTS jobs_bronze_orders (
                   order_id STRING, order_timestamp BIGINT, customer_id STRING, quantity INT, total DOUBLE,
                   items ARRAY<STRUCT<product_id: STRING, quantity: INT, subtotal: DOUBLE>>,
                   source_file STRING, batch_id INT, ingested_at TIMESTAMP)
                 COMMENT 'Section 10 bronze: raw July orders loaded by the load_bronze job task'""")
    spark.sql("""CREATE TABLE IF NOT EXISTS jobs_task_log (
                   logged_at TIMESTAMP, task STRING, attempt STRING, status STRING, message STRING)
                 COMMENT 'Section 10: one line per job-task execution'""")
    spark.sql("CREATE TABLE IF NOT EXISTS jobs_control (key STRING, value STRING) "
              "COMMENT 'Section 10: configuration read by the publish task'")
    if spark.table("jobs_control").count() == 0:
        spark.createDataFrame([("publish_enabled", "true")], "key STRING, value STRING") \
             .write.mode("append").saveAsTable("jobs_control")
    if not spark.catalog.tableExists("jobs_customers"):
        (spark.read.json(f"{dataset_path}/customers-json")
              .select("customer_id", F.get_json_object("profile", "$.address.country").alias("country"))
              .write.mode("overwrite").saveAsTable("jobs_customers"))


_ensure_lab10()

# COMMAND ----------

# DBTITLE 1,The daily files: land10() and reset
def landed10() -> list:
    """Names of the files already in the inbox."""
    return sorted(f.name for f in dbutils.fs.ls(INBOX10) if f.name.endswith(".json")) if path_exists(INBOX10) else []


def land10(n: int = 1, verbose: bool = True) -> list:
    """'New files arrive': copy the next n staged daily files (01.json = 1 July ...) into the inbox. Returns their names."""
    staged = sorted(f.name for f in dbutils.fs.ls(_STAGING10) if f.name.endswith(".json"))
    pending = [f for f in staged if f not in set(landed10())]
    todo = pending[:max(0, int(n))]
    for name in todo:
        dbutils.fs.cp(f"{_STAGING10}/{name}", f"{INBOX10}/{name}")
        if verbose:
            print(f"📥 landed {name} (orders of 2026-07-{name[:2]}) -> {INBOX10}/")
    if not pending and verbose:
        print("No more files to land - all 10 July files are in the inbox (reset_lab10_data() starts over).")
    return todo


def set_control(key: str, value: str):
    """Change a value in jobs_control (e.g. set_control('publish_enabled', 'true'))."""
    rows = {r["key"]: r["value"] for r in spark.table("jobs_control").collect()}
    rows[key] = str(value)
    spark.createDataFrame(sorted(rows.items()), "key STRING, value STRING").write.mode("overwrite") \
         .saveAsTable("jobs_control")
    print(f"⚙️ jobs_control: {key} = {value}")


def get_control(key: str, default: str = None) -> str:
    row = spark.table("jobs_control").where(F.col("key") == key).first()
    return row["value"] if row else default


def task_log(n: int = 30):
    """The latest lines of jobs_task_log (newest first): which task ran, which attempt, what happened."""
    return spark.table("jobs_task_log").orderBy(F.col("logged_at").desc()).limit(n)


def reset_lab10_data(verbose: bool = True):
    """Empty the inbox and drop/recreate the Section 10 DATA tables (your jobs are kept)."""
    if path_exists(INBOX10):
        dbutils.fs.rm(INBOX10, True)
    for t in DATA_TABLES10:
        if t != "jobs_customers":
            spark.sql(f"DROP TABLE IF EXISTS {t}")
    _ensure_lab10()
    if verbose:
        print("🧹 Section 10 data reset: empty inbox, empty jobs_* tables, publish_enabled = true")

# COMMAND ----------

# DBTITLE 1,Job-task logic (what the notebooks in labs/tasks/ run)
def _log10(task: str, status: str, message: str, attempt=None):
    print(f"[{task}] {status}: {message}")
    (spark.createDataFrame([(task, str(attempt or ""), status, message)],
                           "task STRING, attempt STRING, status STRING, message STRING")
          .select(F.current_timestamp().alias("logged_at"), "task", "attempt", "status", "message")
          .write.mode("append").saveAsTable("jobs_task_log"))


def set_task_value(key: str, value):
    """dbutils.jobs.taskValues.set() - only meaningful inside a job run; interactively it just prints."""
    try:
        dbutils.jobs.taskValues.set(key=key, value=value)
    except Exception as e:                                   # local tests / very old runtimes
        print("   (task value not set:", (str(e).strip().splitlines() or [repr(e)])[0][:120], ")")
    print(f"   📤 task value {key} = {json.dumps(value)}")


def get_task_value(task_key: str, key: str, debug_value):
    """dbutils.jobs.taskValues.get() with a debugValue for interactive runs (and a safe fallback)."""
    try:
        return dbutils.jobs.taskValues.get(taskKey=task_key, key=key, default=debug_value, debugValue=debug_value)
    except Exception:
        return debug_value


def _latest_batch10() -> int:
    return spark.table("jobs_bronze_orders").agg(F.coalesce(F.max("batch_id"), F.lit(0))).first()[0]


def task_land_orders(files="1") -> dict:
    landed = land10(int(files or 1), verbose=True)
    set_task_value("landed_files", landed)
    _log10("land_orders", "OK", f"landed {len(landed)} file(s): {', '.join(landed) or '-'}")
    return {"landed_files": landed}


def task_load_bronze() -> dict:
    """Load the inbox files that are not in bronze yet - as ONE new batch. Re-running it loads nothing twice."""
    loaded = {r[0] for r in spark.table("jobs_bronze_orders").select("source_file").distinct().collect()}
    new_files = [f for f in landed10() if f not in loaded]
    if not new_files:
        set_task_value("new_rows", 0)
        set_task_value("batch_id", 0)
        _log10("load_bronze", "OK", "no new files in the inbox - nothing to load")
        return {"new_rows": 0, "batch_id": 0, "files": []}
    batch = _latest_batch10() + 1
    df = (spark.read.schema(ORDER_SCHEMA).json([f"{INBOX10}/{f}" for f in new_files])
               .withColumn("source_file", F.col("_metadata.file_name"))
               .withColumn("batch_id", F.lit(batch))
               .withColumn("ingested_at", F.current_timestamp()))
    df.write.mode("append").saveAsTable("jobs_bronze_orders")
    rows = spark.table("jobs_bronze_orders").where(F.col("batch_id") == batch).count()
    set_task_value("new_rows", rows)
    set_task_value("batch_id", batch)
    _log10("load_bronze", "OK", f"batch {batch}: {rows} rows from {', '.join(new_files)}")
    return {"new_rows": rows, "batch_id": batch, "files": new_files}


def _batch_rows10(batch: int):
    return spark.table("jobs_bronze_orders").where(F.col("batch_id") == batch)


def _bad_rows10(batch: int):
    """The bad rows of a batch, one row per problem: duplicate copies, cancelled orders, unknown customers."""
    rows = _batch_rows10(batch)
    w = Window.partitionBy("order_id").orderBy("source_file")
    dups = (rows.withColumn("_n", F.row_number().over(w)).where("_n > 1")
                .select("order_id", F.lit("duplicate").alias("reason")))
    first = rows.dropDuplicates(["order_id"])
    cancelled = first.where("quantity = 0").select("order_id", F.lit("cancelled").alias("reason"))
    unknown = (first.join(spark.table("jobs_customers"), "customer_id", "left_anti")
                    .select("order_id", F.lit("unknown_customer").alias("reason")))
    return dups.unionByName(cancelled).unionByName(unknown)


def _resolve_batch10() -> int:
    batch = int(get_task_value("load_bronze", "batch_id", -1))
    return _latest_batch10() if batch < 0 else batch          # -1 = interactive run / no load_bronze task


def task_quality_check() -> dict:
    """Count the bad rows of the batch just loaded and publish task values for the next tasks:
    bad_rows, valid_rows and countries (top 3 countries by revenue - the For each input)."""
    batch = _resolve_batch10()
    if batch == 0:
        result = {"batch_id": 0, "bad_rows": 0, "valid_rows": 0, "countries": []}
    else:
        bad = _bad_rows10(batch)
        bad_rows = bad.count()
        valid = (_batch_rows10(batch).dropDuplicates(["order_id"]).where("quantity > 0")
                                     .join(spark.table("jobs_customers"), "customer_id", "left"))
        top = (valid.where("country IS NOT NULL").groupBy("country").agg(F.sum("total").alias("revenue"))
                    .orderBy(F.col("revenue").desc(), "country").limit(3).collect())
        result = {"batch_id": batch, "bad_rows": bad_rows, "valid_rows": valid.count(),
                  "countries": [r["country"] for r in top]}
    for k in ("bad_rows", "valid_rows", "countries"):
        set_task_value(k, result[k])
    _log10("quality_check", "OK", f"batch {result['batch_id']}: {result['bad_rows']} bad rows, "
                                  f"{result['valid_rows']} valid rows, top countries {result['countries']}")
    return result


def _quarantined_batches10() -> list:
    if not spark.catalog.tableExists("jobs_quarantine"):
        return []
    return sorted(r[0] for r in spark.table("jobs_quarantine").select("batch_id").distinct().collect())


def task_build_silver_gold() -> dict:
    """Rebuild silver (dedup, no cancelled orders, typed date, country) and the daily gold table from ALL of bronze -
    except batches that were quarantined. A full rebuild is idempotent: running it twice gives the same result."""
    held_back = _quarantined_batches10()
    silver = (spark.table("jobs_bronze_orders")
                   .where(~F.col("batch_id").isin(held_back) if held_back else F.lit(True))
                   .dropDuplicates(["order_id"])
                   .where("quantity > 0")
                   .join(spark.table("jobs_customers"), "customer_id", "left")
                   .select("order_id", "customer_id",
                           F.timestamp_seconds("order_timestamp").alias("order_ts"),
                           F.to_date(F.timestamp_seconds("order_timestamp")).alias("order_date"),
                           F.coalesce("country", F.lit("Unknown")).alias("country"),
                           F.col("country").isNotNull().alias("known_customer"),
                           "quantity", "total", "batch_id"))
    silver.write.mode("overwrite").saveAsTable("jobs_silver_orders")
    gold = (spark.table("jobs_silver_orders").groupBy("order_date")
                 .agg(F.count("*").alias("orders"),
                      F.round(F.sum("total"), 2).alias("revenue"),
                      F.round(F.avg("total"), 2).alias("avg_order_value"),
                      F.sum(F.when(~F.col("known_customer"), 1).otherwise(0)).alias("unknown_customer_orders")))
    gold.write.mode("overwrite").saveAsTable("jobs_gold_daily")
    silver_rows = spark.table("jobs_silver_orders").count()
    days = spark.table("jobs_gold_daily").count()
    set_task_value("silver_rows", silver_rows)
    set_task_value("gold_days", days)
    _log10("build_silver_gold", "OK", f"silver {silver_rows} rows, gold {days} day(s)"
                                      + (f" - quarantined batch(es) {held_back} held back" if held_back else ""))
    return {"silver_rows": silver_rows, "gold_days": days}


def task_quarantine_report() -> dict:
    """The 'too many bad rows' branch: copy the bad rows of the batch to jobs_quarantine. build_silver_gold skips
    quarantined batches, so the whole batch is held back until someone looks at it."""
    batch = _resolve_batch10()
    bad = (_bad_rows10(batch).withColumn("batch_id", F.lit(batch))
                             .withColumn("quarantined_at", F.current_timestamp()))
    bad.write.mode("append").saveAsTable("jobs_quarantine")
    n = bad.count()
    _log10("quarantine_report", "QUARANTINED", f"batch {batch}: {n} bad rows copied to jobs_quarantine - "
                                               "the batch is held back from silver/gold")
    return {"quarantined": n}


def task_call_partner_api(attempt="", fail_attempts="1") -> dict:
    """Simulates a flaky external API: fails while attempt <= fail_attempts. In a job, pass
    attempt = {{task.execution_count}} so the task knows which attempt it is."""
    if str(attempt).strip() == "":
        _log10("call_partner_api", "OK", "interactive run (no attempt number) - call succeeded")
        return {"attempt": None}
    if int(attempt) <= int(fail_attempts or 0):
        _log10("call_partner_api", "FAILED", f"attempt {attempt}: partner API timed out (simulated)", attempt)
        raise RuntimeError(f"Simulated transient error from the partner API on attempt {attempt} "
                           "- a retry will succeed")
    _log10("call_partner_api", "OK", f"attempt {attempt}: partner API call succeeded", attempt)
    return {"attempt": int(attempt)}


def task_country_report(country: str) -> dict:
    """One For-each iteration: summary of the latest batch for ONE country (append-only -> parallel-safe)."""
    batch = _resolve_batch10()
    rows = (_batch_rows10(batch).dropDuplicates(["order_id"]).where("quantity > 0")
                                .join(spark.table("jobs_customers"), "customer_id")
                                .where(F.col("country") == country))
    report = (rows.agg(F.count("*").alias("orders"), F.round(F.sum("total"), 2).alias("revenue"))
                  .select(F.lit(batch).alias("batch_id"), F.lit(country).alias("country"), "orders", "revenue",
                          F.current_timestamp().alias("reported_at")))
    report.write.mode("append").saveAsTable("jobs_country_report")
    r = report.first()
    _log10("country_report", "OK", f"batch {batch} · {country}: {r['orders']} orders, revenue {r['revenue']}")
    return {"country": country, "orders": r["orders"], "revenue": r["revenue"]}


def task_publish() -> dict:
    """Publish a snapshot of the gold KPIs - fails on purpose while jobs_control.publish_enabled = 'false'."""
    if (get_control("publish_enabled", "true") or "").lower() != "true":
        _log10("publish", "FAILED", "publish_enabled = false in jobs_control")
        raise RuntimeError("Publishing is switched off: jobs_control.publish_enabled = 'false'. Fix the configuration "
                           "(set_control('publish_enabled', 'true')) and REPAIR the run.")
    snap = (spark.table("jobs_gold_daily")
                 .agg(F.count("*").alias("days"), F.sum("orders").alias("orders"),
                      F.round(F.sum("revenue"), 2).alias("revenue"))
                 .withColumn("published_at", F.current_timestamp()))
    snap.write.mode("append").saveAsTable("jobs_published")
    r = snap.first()
    _log10("publish", "OK", f"published {r['days']} day(s), {r['orders']} orders, revenue {r['revenue']}")
    return {"days": r["days"], "orders": r["orders"], "revenue": r["revenue"]}


def task_notify_failure(reason="") -> dict:
    _log10("notify_failure", "ALERT", "an upstream task failed - on-call notified " + (f"({reason})" if reason else ""))
    return {"notified": True}


def task_cleanup() -> dict:
    _log10("cleanup", "OK", "cleanup finished (runs whatever happened upstream)")
    return {"cleanup": True}

# COMMAND ----------

# DBTITLE 1,The SQL file for the SQL task (written to your home folder)
def sql_task_file_path() -> str:
    me = spark.sql("SELECT current_user()").first()[0]
    return f"/Workspace/Users/{me}/shopwave_jobs/10_gold_kpis.sql"


def _gold_kpis_sql() -> str:
    return (f"-- Section 10 SQL task: refresh the KPI table from the daily gold table\n"
            f"CREATE OR REPLACE TABLE {job_table('jobs_gold_kpis')}\n"
            f"COMMENT 'Section 10: KPIs refreshed by the SQL task gold_kpis'\n"
            f"AS SELECT count(*)                AS days_loaded,\n"
            f"          sum(orders)             AS orders,\n"
            f"          round(sum(revenue), 2)  AS revenue,\n"
            f"          max(order_date)         AS latest_day,\n"
            f"          current_timestamp()     AS refreshed_at\n"
            f"   FROM {job_table('jobs_gold_daily')};\n\n"
            f"SELECT * FROM {job_table('jobs_gold_kpis')};\n")


def write_sql_task_file(verbose: bool = True) -> str:
    """Write the .sql file used by the SQL task. It contains YOUR catalog/schema, so it is generated into your home
    folder (Workspace > Users > you > shopwave_jobs) instead of living in the Git folder."""
    path = sql_task_file_path()
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w") as fh:
            fh.write(_gold_kpis_sql())
    except Exception as e:                                   # fallback: Workspace import API
        import base64
        print("direct write not possible, using the Workspace API:", _first_line(e))
        _api("POST", "/api/2.0/workspace/mkdirs", body={"path": path.replace("/Workspace", "", 1).rsplit("/", 1)[0]})
        _api("POST", "/api/2.0/workspace/import",
             body={"path": path.replace("/Workspace", "", 1), "format": "AUTO", "overwrite": True,
                   "content": base64.b64encode(_gold_kpis_sql().encode()).decode()})
    if verbose:
        print("📝 SQL task file:", path)
    return path

# COMMAND ----------

# DBTITLE 1,Jobs REST API helpers (through the Databricks SDK)
# Everything you click in the Jobs UI is also a REST call (Jobs API 2.2). The labs use these helpers to CHECK your jobs,
# and the solutions use them to BUILD jobs as code - exactly what "View as code" / bundles (Section 11) do.
_ws10 = None


def _ws():
    global _ws10
    if _ws10 is None:
        from databricks.sdk import WorkspaceClient
        _ws10 = WorkspaceClient()
    return _ws10


def _api(method: str, path: str, body=None, query=None):
    return _ws().api_client.do(method, path, query=query, body=body) or {}


def _first_line(e) -> str:
    return (str(e).strip().splitlines() or [repr(e)])[0][:300]


def _host() -> str:
    return _ws().config.host.rstrip("/")


def _me() -> str:
    return spark.sql("SELECT current_user()").first()[0]


def _notebook_path() -> str:
    try:
        return dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    except Exception:
        return dbutils.notebook.getContext().notebookPath        # newer runtimes


def _section_root() -> str:
    """Workspace path of the '10 - Lakeflow Jobs' folder, derived from the running notebook.
    Override: SECTION10_ROOT = '/Workspace/<path>/10 - Lakeflow Jobs' in a cell before %run."""
    if globals().get("SECTION10_ROOT"):
        return SECTION10_ROOT.rstrip("/")
    nb = _notebook_path()
    cut = max(nb.rfind("/labs/"), nb.rfind("/data/"), nb.rfind("/questions/"))
    if cut < 0:
        raise RuntimeError("Could not find the section folder from this notebook's path. Set SECTION10_ROOT = "
                           "'/Workspace/<path to>/10 - Lakeflow Jobs' in a cell before the %run cells.")
    root = nb[:cut]
    return root if root.startswith("/Workspace") else "/Workspace" + root


def task_path(name: str) -> str:
    """Workspace path of a job-task notebook, e.g. task_path('land_orders')."""
    return f"{_section_root()}/labs/tasks/{name}"


def show_task_paths():
    for n in ("land_orders", "load_bronze", "quality_check", "build_silver_gold", "quarantine_report",
              "call_partner_api", "country_report", "publish", "notify_failure", "cleanup"):
        print(f"{n:<20} {task_path(n)}")


def _paged(path: str, key: str, query=None, max_pages: int = 20) -> list:
    q, out = dict(query or {}), []
    for _ in range(max_pages):
        res = _api("GET", path, query=q)
        out += res.get(key) or []
        token = res.get("next_page_token")
        if not token:
            break
        q["page_token"] = token
    return out


def warehouse() -> dict:
    """The SQL warehouse for SQL tasks (prefers a running serverless one). Override with COURSE_WAREHOUSE."""
    whs = _api("GET", "/api/2.0/sql/warehouses").get("warehouses", [])
    if not whs:
        raise RuntimeError("No SQL warehouse visible to you - create one or ask for CAN USE on one.")
    wanted = globals().get("COURSE_WAREHOUSE")
    if wanted:
        whs = [w for w in whs if wanted in (w.get("id"), w.get("name"))] or whs
    return sorted(whs, key=lambda w: (w.get("state") != "RUNNING", not w.get("enable_serverless_compute"),
                                      w.get("name", "")))[0]


def find_job(name: str):
    """The job called `name` that YOU created (newest first), as returned by the Jobs API - or None."""
    me = _me()
    jobs = _paged("/api/2.2/jobs/list", "jobs", {"name": name, "limit": 100})
    jobs = [j for j in jobs if (j.get("settings") or {}).get("name") == name
            and j.get("creator_user_name") in (me, None)]
    if not jobs:
        return None
    jobs.sort(key=lambda j: j.get("created_time", 0), reverse=True)
    return _api("GET", "/api/2.2/jobs/get", query={"job_id": jobs[0]["job_id"]})


def job_link(name: str):
    j = find_job(name)
    if not j:
        print(f"No job called '{name}' yet.")
        return
    displayHTML(f'<a href="{_host()}/jobs/{j["job_id"]}" target="_blank">🔗 Open job “{name}”</a>')


_TASK_TYPES10 = (("notebook_task", "notebook"), ("sql_task", "sql"), ("pipeline_task", "pipeline"),
                 ("condition_task", "if/else"), ("for_each_task", "for each"), ("run_job_task", "run job"),
                 ("dashboard_task", "dashboard"), ("spark_python_task", "python script"),
                 ("python_wheel_task", "python wheel"), ("dbt_task", "dbt"), ("spark_jar_task", "jar"))


def _task_type(t: dict) -> str:
    for key, label in _TASK_TYPES10:
        if key in t:
            if key == "sql_task":
                sub = next((s for s in ("file", "query", "alert", "dashboard") if s in t[key]), "")
                return f"sql ({sub})" if sub else "sql"
            return label
    return "other"


def _task_params(t: dict) -> dict:
    if "notebook_task" in t:
        return dict(t["notebook_task"].get("base_parameters") or {})
    if "sql_task" in t:
        return dict(t["sql_task"].get("parameters") or {})
    if "run_job_task" in t:
        return dict(t["run_job_task"].get("job_parameters") or {})
    return {}


def _describe_task(t: dict) -> dict:
    d = {"type": _task_type(t),
         "depends_on": [(x["task_key"], x.get("outcome")) for x in t.get("depends_on") or []],
         "run_if": t.get("run_if", "ALL_SUCCESS"),
         "max_retries": t.get("max_retries", 0),
         "min_retry_interval_millis": t.get("min_retry_interval_millis", 0),
         "timeout_seconds": t.get("timeout_seconds", 0),
         "params": _task_params(t),
         "disabled": t.get("disabled", False),
         "serverless": not any(k in t for k in ("existing_cluster_id", "new_cluster", "job_cluster_key"))}
    if "notebook_task" in t:
        d["path"] = t["notebook_task"].get("notebook_path", "")
    if "sql_task" in t:
        d["path"] = ((t["sql_task"].get("file") or {}).get("path") or "")
        d["warehouse_id"] = t["sql_task"].get("warehouse_id")
    if "pipeline_task" in t:
        d["pipeline_id"] = t["pipeline_task"].get("pipeline_id")
    if "condition_task" in t:
        c = t["condition_task"]
        d["condition"] = (c.get("left"), c.get("op"), c.get("right"))
    if "for_each_task" in t:
        fe = t["for_each_task"]
        d["inputs"] = fe.get("inputs")
        d["concurrency"] = fe.get("concurrency", 1)
        d["nested"] = _describe_task(fe.get("task") or {})
    if "run_job_task" in t:
        d["run_job_id"] = t["run_job_task"].get("job_id")
    return d


def job_summary(name: str, verbose: bool = True):
    """What your job looks like to the Jobs API: tasks, types, dependencies, run-if, retries, parameters, triggers."""
    j = find_job(name)
    if not j:
        if verbose:
            print(f"❌ no job called '{name}' (created by you)")
        return None
    s = j.get("settings") or {}
    trig = s.get("trigger") or {}
    trigger_type = next((k for k in ("file_arrival", "table_update", "periodic", "table") if k in trig), None)
    summary = {
        "job_id": j["job_id"],
        "tasks": {t["task_key"]: _describe_task(t) for t in s.get("tasks") or []},
        "parameters": {p["name"]: p.get("default") for p in s.get("parameters") or []},
        "schedule": s.get("schedule"),
        "trigger_type": trigger_type,
        "trigger": trig.get(trigger_type) if trigger_type else None,
        "trigger_paused": trig.get("pause_status") if trig else None,
        "continuous": s.get("continuous"),
        "max_concurrent_runs": s.get("max_concurrent_runs", 1),
        "queue": (s.get("queue") or {}).get("enabled"),
        "email_on_failure": (s.get("email_notifications") or {}).get("on_failure") or [],
        "timeout_seconds": s.get("timeout_seconds", 0),
        "health_rules": (s.get("health") or {}).get("rules") or [],
        "tags": s.get("tags") or {},
        "run_as": (j.get("run_as_user_name") or ""),
    }
    if verbose:
        print(f"🧩 job '{name}' (id {summary['job_id']}) - {len(summary['tasks'])} task(s)")
        for k, t in summary["tasks"].items():
            deps = ", ".join(f"{d}" + (f" ({o})" if o else "") for d, o in t["depends_on"]) or "-"
            extra = []
            if t["run_if"] != "ALL_SUCCESS":
                extra.append(f"run if {t['run_if']}")
            if t["max_retries"]:
                extra.append(f"retries {t['max_retries']}")
            if t["params"]:
                extra.append(f"params {t['params']}")
            if "condition" in t:
                extra.append("condition " + " ".join(str(x) for x in t["condition"]))
            if "inputs" in t:
                extra.append(f"inputs {t['inputs']} · concurrency {t['concurrency']} · nested {t['nested']['type']}")
            print(f"   • {k:<20} {t['type']:<12} depends on: {deps:<36} {' · '.join(extra)}")
        print("   job parameters :", summary["parameters"] or "-")
        if summary["schedule"]:
            sch = summary["schedule"]
            print(f"   schedule       : {sch.get('quartz_cron_expression')} {sch.get('timezone_id')} "
                  f"({sch.get('pause_status')})")
        if trigger_type:
            print(f"   trigger        : {trigger_type} {summary['trigger']} ({summary['trigger_paused']})")
    return summary

# COMMAND ----------

# DBTITLE 1,Runs: list, inspect, run now, wait
def _task_state(t: dict) -> str:
    st = t.get("state") or {}
    return st.get("result_state") or st.get("life_cycle_state") or (t.get("status") or {}).get("state") or "?"


def job_runs(name: str, n: int = 10) -> list:
    """The latest runs of the job (newest first)."""
    j = find_job(name)
    if not j:
        return []
    return _api("GET", "/api/2.2/jobs/runs/list", query={"job_id": j["job_id"], "limit": n}).get("runs", [])


def run_summary(run_id: int, verbose: bool = True) -> dict:
    """State of a job run and of each task (incl. if/else outcome, attempts and repairs)."""
    r = _api("GET", "/api/2.2/jobs/runs/get", query={"run_id": run_id, "include_history": "true"})
    tasks = {}
    for t in r.get("tasks") or []:
        key = t["task_key"]
        info = {"state": _task_state(t), "attempt": t.get("attempt_number", 0), "run_id": t.get("run_id"),
                "outcome": (t.get("condition_task") or {}).get("outcome"),
                "message": (t.get("state") or {}).get("state_message", "")}
        if key not in tasks or info["attempt"] >= tasks[key]["attempt"]:
            info["attempts_seen"] = max(info["attempt"], (tasks.get(key) or {}).get("attempts_seen", 0))
            tasks[key] = info
    summary = {"run_id": run_id, "state": _task_state(r), "trigger": r.get("trigger"),
               "parameters": {p["name"]: p.get("value", p.get("default")) for p in r.get("job_parameters") or []},
               "repairs": len([h for h in r.get("repair_history") or [] if h.get("type") == "REPAIR"]),
               "start_time": r.get("start_time"), "tasks": tasks, "url": r.get("run_page_url")}
    if verbose:
        print(f"▶️ run {run_id}: {summary['state']} · trigger {summary['trigger']} · parameters {summary['parameters']}"
              + (f" · repairs {summary['repairs']}" if summary["repairs"] else ""))
        for k, t in tasks.items():
            extra = f" → outcome {t['outcome']}" if t["outcome"] else ""
            print(f"   • {k:<20} {t['state']:<16}{extra}")
    return summary


def last_run(name: str, verbose: bool = True, finished: bool = True):
    """Summary of the newest (finished) run of the job, or None."""
    for r in job_runs(name, 25):
        if not finished or (r.get("state") or {}).get("life_cycle_state") in ("TERMINATED", "INTERNAL_ERROR", "SKIPPED"):
            return run_summary(r["run_id"], verbose)
    if verbose:
        print(f"no {'finished ' if finished else ''}run of '{name}' yet")
    return None


def wait_for_run(run_id: int, timeout_min: int = 30, verbose: bool = True) -> dict:
    t0, last = time.time(), None
    while time.time() - t0 < timeout_min * 60:
        r = _api("GET", "/api/2.2/jobs/runs/get", query={"run_id": run_id})
        lc = (r.get("state") or {}).get("life_cycle_state")
        if verbose and lc != last:
            print(f"   {time.strftime('%H:%M:%S')}  {lc}")
            last = lc
        if lc in ("TERMINATED", "INTERNAL_ERROR", "SKIPPED"):
            break
        time.sleep(10)
    return run_summary(run_id, verbose)


def run_job_now(name: str, params: dict = None, wait: bool = True) -> dict:
    """Like clicking 'Run now' (or 'Run now with different parameters'). Returns the run summary."""
    j = find_job(name)
    if not j:
        raise RuntimeError(f"No job called '{name}' - create it first.")
    body = {"job_id": j["job_id"]}
    if params:
        body["job_parameters"] = {k: str(v) for k, v in params.items()}
    run_id = _api("POST", "/api/2.2/jobs/run-now", body=body)["run_id"]
    print(f"🚀 started '{name}' run {run_id}" + (f" with {params}" if params else ""))
    return wait_for_run(run_id) if wait else {"run_id": run_id}


def repair_run(run_id: int, wait: bool = True) -> dict:
    """Like clicking 'Repair run': re-run only the failed/skipped tasks (and what depends on them)."""
    rid = _api("POST", "/api/2.2/jobs/runs/repair", body={"run_id": run_id, "rerun_all_failed_tasks": True,
                                                         "rerun_dependent_tasks": True}).get("repair_id")
    print(f"🔧 repair {rid} of run {run_id} started")
    return wait_for_run(run_id) if wait else {"run_id": run_id, "repair_id": rid}


def wait_for_new_run(name: str, after_ms: int, timeout_min: int = 8, verbose: bool = True):
    """Wait until a run of the job STARTS after `after_ms` (epoch ms) - used for triggers - then wait for it to end."""
    t0 = time.time()
    while time.time() - t0 < timeout_min * 60:
        runs = [r for r in job_runs(name, 10) if (r.get("start_time") or 0) >= after_ms]
        if runs:
            run = sorted(runs, key=lambda r: r["start_time"])[0]
            print(f"⚡ '{name}' was triggered ({run.get('trigger')}) after {time.time() - t0:.0f}s - waiting for it...")
            return wait_for_run(run["run_id"], verbose=verbose)
        time.sleep(15)
    print(f"⏳ no new run of '{name}' within {timeout_min} min - is the trigger active (not paused)?")
    return None


def now_ms() -> int:
    return int(time.time() * 1000)

# COMMAND ----------

# DBTITLE 1,Jobs as code: create / replace / pause / delete
def create_or_replace_job(spec: dict) -> int:
    """Create the job described by `spec` (Jobs API settings) or overwrite the existing job with that name."""
    j = find_job(spec["name"])
    if j:
        _api("POST", "/api/2.2/jobs/reset", body={"job_id": j["job_id"], "new_settings": spec})
        print(f"🔁 job '{spec['name']}' ({j['job_id']}) replaced")
        return j["job_id"]
    job_id = _api("POST", "/api/2.2/jobs/create", body=spec)["job_id"]
    print(f"🆕 job '{spec['name']}' created ({job_id})")
    return job_id


def nb_task(key: str, notebook: str = None, depends=(), params: dict = None, **extra) -> dict:
    """A notebook task for a job spec. depends: 'task' or ('task', 'true'/'false') for If/else branches."""
    t = {"task_key": key,
         "notebook_task": {"notebook_path": task_path(notebook or key), "source": "WORKSPACE",
                           "base_parameters": dict(params or {})}}
    if depends:
        t["depends_on"] = [{"task_key": d} if isinstance(d, str) else {"task_key": d[0], "outcome": d[1]}
                           for d in depends]
    t.update(extra)
    return t


def sql_file_task(key: str, depends=(), **extra) -> dict:
    t = {"task_key": key,
         "sql_task": {"warehouse_id": warehouse()["id"], "file": {"path": sql_task_file_path(), "source": "WORKSPACE"}}}
    if depends:
        t["depends_on"] = [{"task_key": d} for d in depends]
    t.update(extra)
    return t


def l2_base_spec() -> dict:
    """10-L2 starting point: the 4-task chain + job parameters files and max_bad_rows."""
    return {"name": JOB_NAMES10["L2"], "max_concurrent_runs": 1, "queue": {"enabled": True},
            "tags": {"section": "10"},
            "parameters": [{"name": "files", "default": "1"}, {"name": "max_bad_rows", "default": "5"}],
            "tasks": [nb_task("land_orders"),
                      nb_task("load_bronze", depends=["land_orders"]),
                      nb_task("quality_check", depends=["load_bronze"]),
                      nb_task("build_silver_gold", depends=["quality_check"])]}


def l2_full_spec() -> dict:
    """10-L2 finished: If/else gate, For each, retries, run-if failure handler and cleanup (the 'Plan B' of the lab)."""
    spec = l2_base_spec()
    spec["tasks"] = [
        nb_task("land_orders"),
        nb_task("load_bronze", depends=["land_orders"]),
        nb_task("quality_check", depends=["load_bronze"]),
        {"task_key": "check_quality", "depends_on": [{"task_key": "quality_check"}],
         "condition_task": {"op": "GREATER_THAN", "left": "{{tasks.quality_check.values.bad_rows}}",
                            "right": "{{job.parameters.max_bad_rows}}"}},
        nb_task("quarantine_report", depends=[("check_quality", "true")]),
        nb_task("build_silver_gold", depends=[("check_quality", "false")]),
        {"task_key": "country_reports", "depends_on": [{"task_key": "build_silver_gold"}],
         "for_each_task": {"inputs": "{{tasks.quality_check.values.countries}}", "concurrency": 2,
                           "task": nb_task("country_reports_iteration", "country_report",
                                           params={"country": "{{input}}"})}},
        nb_task("call_partner_api", depends=["build_silver_gold"],
                params={"attempt": "{{task.execution_count}}", "fail_attempts": "1"},
                max_retries=2, min_retry_interval_millis=15000),
        nb_task("publish", depends=["call_partner_api", "country_reports"]),
        nb_task("notify_failure", depends=["publish"], run_if="AT_LEAST_ONE_FAILED",
                params={"reason": "publish = {{tasks.publish.result_state}}"}),
        nb_task("cleanup", depends=["publish"], run_if="ALL_DONE"),
    ]
    return spec


def pause_lab10_triggers():
    """Pause the schedule/trigger of every '10-L…' job you created (do this at the end of the labs)."""
    for j in _paged("/api/2.2/jobs/list", "jobs", {"limit": 100}):
        s = j.get("settings") or {}
        if not (s.get("name") or "").startswith("10-L") or j.get("creator_user_name") not in (_me(), None):
            continue
        full = _api("GET", "/api/2.2/jobs/get", query={"job_id": j["job_id"]}).get("settings") or {}
        new = {}
        for key in ("schedule", "trigger", "continuous"):
            if full.get(key) and full[key].get("pause_status") != "PAUSED":
                new[key] = dict(full[key], pause_status="PAUSED")
        if new:
            _api("POST", "/api/2.2/jobs/update", body={"job_id": j["job_id"], "new_settings": new})
            print(f"⏸️ paused {', '.join(new)} of '{s['name']}'")
    print("✅ all Section 10 schedules/triggers are paused")


def delete_lab10_jobs():
    for j in _paged("/api/2.2/jobs/list", "jobs", {"limit": 100}):
        name = (j.get("settings") or {}).get("name") or ""
        if name.startswith("10-L") and j.get("creator_user_name") in (_me(), None):
            _api("POST", "/api/2.2/jobs/delete", body={"job_id": j["job_id"]})
            print("🗑️ job", name)

# COMMAND ----------

# DBTITLE 1,Pipeline helper for the pipeline task (10-L3)
PIPELINE10 = "10-L3 ShopWave inbox pipeline"


def find_pipeline10():
    """(pipeline_id, state) of the 10-L3 pipeline you created, or None."""
    res = _api("GET", "/api/2.0/pipelines", query={"filter": f"name LIKE '{PIPELINE10}'", "max_results": 100})
    mine = [s for s in res.get("statuses", []) if s.get("name") == PIPELINE10
            and s.get("creator_user_name") in (_me(), None)]
    return (mine[0]["pipeline_id"], mine[0].get("state")) if mine else None


def create_or_update_pipeline10() -> str:
    """Create (or update) the small serverless pipeline used by the pipeline task:
    labs/pipelines/jobs_orders/transformations/orders.sql, configuration inbox_path = the lab10 inbox."""
    root = f"{_section_root()}/labs/pipelines/jobs_orders"
    spec = {"name": PIPELINE10, "catalog": catalog_name, "schema": schema_name, "serverless": True,
            "continuous": False, "development": True, "channel": "CURRENT", "root_path": root,
            "libraries": [{"glob": {"include": f"{root}/transformations/**"}}],
            "configuration": {"inbox_path": INBOX10}}
    found = find_pipeline10()
    if found:
        _api("PUT", f"/api/2.0/pipelines/{found[0]}", body=dict(spec, id=found[0]))
        print(f"🔁 pipeline '{PIPELINE10}' updated ({found[0]})")
        return found[0]
    try:
        pid = _api("POST", "/api/2.0/pipelines", body=spec)["pipeline_id"]
    except Exception as e:                                   # older workspaces: list the file instead of a glob
        print("glob libraries not accepted, retrying with a file library:", _first_line(e))
        spec["libraries"] = [{"file": {"path": f"{root}/transformations/orders.sql"}}]
        pid = _api("POST", "/api/2.0/pipelines", body=spec)["pipeline_id"]
    print(f"🆕 pipeline '{PIPELINE10}' created ({pid})")
    return pid


def delete_pipeline10():
    found = find_pipeline10()
    if found:
        _api("DELETE", f"/api/2.0/pipelines/{found[0]}")
        print(f"🗑️ pipeline '{PIPELINE10}'")

# COMMAND ----------

# DBTITLE 1,Reset Section 10
def reset_lab10(confirm: str = ""):
    """Delete your '10-L…' jobs and the 10-L3 pipeline, drop the jobs_* tables and empty the inbox."""
    if confirm != "YES":
        print("Nothing done. To really reset Section 10 call: reset_lab10(confirm='YES')")
        return
    for fn in (delete_lab10_jobs, delete_pipeline10):
        try:
            fn()
        except Exception as e:
            print("note:", _first_line(e))
    for t in DATA_TABLES10 + ("pl10_bronze_orders", "pl10_daily_revenue"):
        for stmt in ("DROP TABLE IF EXISTS", "DROP MATERIALIZED VIEW IF EXISTS"):
            try:
                spark.sql(f"{stmt} {t}")
                break
            except Exception:
                pass
    if path_exists(LAB10):
        dbutils.fs.rm(LAB10, True)
    print("✅ Section 10 reset - re-run %run ./_10_prepare to start again")


print(f"🔧 Section 10 ready - inbox {INBOX10} ({len(landed10())} file(s) landed)")
