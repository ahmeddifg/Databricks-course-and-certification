# Databricks notebook source
# MAGIC %md
# MAGIC # 🔧 _12_prepare — Section 12 data, layout / skew / plan helpers and Jobs API helpers
# MAGIC Included by every Section 12 lab **and by the job-task notebook** `labs/tasks/work`:
# MAGIC
# MAGIC ```
# MAGIC %run ../../Includes/_setup        (task notebook: %run ../../../Includes/_setup)
# MAGIC %run ./_12_prepare                (task notebook: %run ../_12_prepare)
# MAGIC ```
# MAGIC
# MAGIC Idempotent: it builds the tables only the first time (≈ 20–40 s on serverless), afterwards it only defines functions.
# MAGIC
# MAGIC **The story.** ShopWave's nightly pipeline got slower and sometimes fails. Section 12 plays the **on-call data engineer**:
# MAGIC read the run history, find the slow task, find the bottleneck inside Spark (skew, shuffle, spill), fix the table layout
# MAGIC (liquid clustering, predictive optimization) and diagnose compute / library / memory failures.
# MAGIC
# MAGIC | Object | Rows | Content |
# MAGIC |---|---|---|
# MAGIC | `perf_events` | 3,000,000 | clickstream: `event_id`, `event_ts`, `event_date` (Jan–Jun 2026), `customer_id`, `product_id`, `event_type`, `device`, `amount` — **skewed**: 30 % of all events belong to one customer, **`C0007`** (a marketplace bot) |
# MAGIC | `perf_customers` · `perf_products` | 300 · 36 | small dimension tables (from the ShopWave base data) |
# MAGIC | `ch12_job_runs` · `ch12_task_runs` | 30 · 120 | 30 nights of run history of the production job (used by the 12-L5 challenge) |
# MAGIC | `perf_job_log` | grows | one line per task execution of the 12-L1 job |
# MAGIC
# MAGIC **Helpers**
# MAGIC
# MAGIC | Area | Functions |
# MAGIC |---|---|
# MAGIC | general | `q(sql)`, `try_sql(sql)`, `check(label, condition)` |
# MAGIC | file layout (12-L3) | `layout(t)`, `table_detail(t)`, `optimize(t, extra)`, `file_ranges(t, col)`, `files_scanned(t, col, low, high=None)`, `skipping_report(t, col, low, high=None)`, `history(t)`, `last_op_metrics(t, op)`, `po_status(kind, name)`, `make_small_files(t, batches)` |
# MAGIC | Spark internals (12-L2) | `partition_rows(df, n)`, `stage_summary(df, n, title)`, `plan_text(df)`, `plan_ops(df)`, `time_it(label, df)` |
# MAGIC | Jobs (12-L1) | `find_job`, `job_link`, `create_or_replace_job`, `l1_job_spec`, `run_job_now`, `wait_for_run`, `run_history`, `history_df`, `baseline_report`, `run_errors`, `set_duration_threshold`, `job_log`, `delete_lab12_jobs` |
# MAGIC | reset | `reset_lab12(confirm="YES")` |

# COMMAND ----------

# DBTITLE 1,Section 12 configuration and general helpers
import json
import re
import time
import datetime as _dt
from pyspark.sql import functions as F

PERF_ROWS = 3_000_000
HOT_CUSTOMER = "C0007"                                   # the skewed key: ~30 % of all events
JOB_NAMES12 = {"L1": "12-L1 ShopWave Nightly"}


def _first_line(e) -> str:
    return (str(e).strip().splitlines() or [repr(e)])[0][:300]


def q(stmt: str):
    """Run one SQL statement and return the DataFrame."""
    return spark.sql(stmt)


def try_sql(stmt: str, show_result: bool = False) -> bool:
    """Run SQL; on error print the first line of the message instead of failing. Returns True if it ran."""
    try:
        df = spark.sql(stmt)
        if show_result:
            display(df)
        print("✅ ran:", " ".join(stmt.split())[:110])
        return True
    except Exception as e:
        print("⛔ error:", _first_line(e))
        return False


def check(label: str, condition) -> bool:
    print(("✅ " if condition else "❌ ") + label)
    return bool(condition)

# COMMAND ----------

# DBTITLE 1,Section 12 tables (built on the first run only)
_PERF_EVENTS_SQL12 = f"""
WITH base AS (
  SELECT id,
         pmod(hash(id, 4), 15638400)          AS secs,     -- 181 days * 86400 s
         CAST(pmod(hash(id, 5), 7) AS INT)    AS et
  FROM range(0, {PERF_ROWS}, 1, 16))
SELECT id                                                                          AS event_id,
       timestamp_seconds(1767225600 + secs)                                        AS event_ts,
       date_add(DATE'2026-01-01', CAST(secs DIV 86400 AS INT))                     AS event_date,
       CASE WHEN pmod(hash(id, 1), 100) < 30 THEN '{HOT_CUSTOMER}'
            ELSE concat('C', lpad(CAST(pmod(hash(id, 2), 300) + 1 AS STRING), 4, '0')) END AS customer_id,
       concat('P', lpad(CAST(pmod(hash(id, 3), 36) + 1 AS STRING), 3, '0'))       AS product_id,
       element_at(array('view', 'view', 'view', 'view', 'cart', 'cart', 'purchase'), et + 1) AS event_type,
       element_at(array('web', 'ios', 'android'), CAST(pmod(hash(id, 6), 3) AS INT) + 1)    AS device,
       CAST(CASE WHEN et = 6 THEN (pmod(hash(id, 7), 49000) + 1000) / 100.0 ELSE 0 END AS DECIMAL(10, 2)) AS amount
FROM base"""


def _build_perf_events():
    spark.sql(f"CREATE OR REPLACE TABLE perf_events "
              f"COMMENT 'Section 12: 3M clickstream events, 30 pct from customer {HOT_CUSTOMER}' AS {_PERF_EVENTS_SQL12}")


def _build_perf_dims():
    (spark.read.json(f"{dataset_path}/customers-json")
          .select("customer_id",
                  F.get_json_object("profile", "$.first_name").alias("first_name"),
                  F.get_json_object("profile", "$.last_name").alias("last_name"),
                  F.get_json_object("profile", "$.address.country").alias("country"),
                  F.get_json_object("profile", "$.address.city").alias("city"))
          .write.mode("overwrite").saveAsTable("perf_customers"))
    (spark.read.option("header", True).option("sep", ";").csv(f"{dataset_path}/products-csv")
          .select("product_id", "title", "brand", "category", F.col("price").cast("double").alias("price"))
          .write.mode("overwrite").saveAsTable("perf_products"))


_TASKS12 = (("extract", 120, ()), ("transform_orders", 300, ("extract",)), ("transform_clicks", 240, ("extract",)),
            ("publish", 60, ("transform_orders", "transform_clicks")))


def _history_rows12():
    """30 nights of the production job - deterministic. Story: day 12 and 27 fail, day 18 waits for compute,
    from day 24 on transform_clicks is ~2.6x slower (the C0007 bot traffic hits a skewed join)."""
    job_rows, task_rows = [], []
    for day in range(1, 31):
        run_id, run_date = 880000 + day, _dt.date(2026, 9, 1) + _dt.timedelta(days=day - 1)
        setup = 540 if day == 18 else 45 + (day * 13) % 20
        failing = {12: "transform_orders", 27: "extract"}.get(day)
        state, dur = {}, {}
        for key, base, deps in _TASKS12:
            noise = ((day * 37 + len(key) * 11) % 21) - 10                    # -10 .. +10 percent
            seconds = base * (100 + noise) // 100
            if key == "transform_clicks" and day >= 24:
                seconds = seconds * 26 // 10
            if any(state[d] != "SUCCESS" for d in deps):
                state[key], dur[key] = "UPSTREAM_FAILED", 0
            elif key == failing:
                state[key], dur[key] = "FAILED", seconds * 4 // 10
            else:
                state[key], dur[key] = "SUCCESS", seconds
            task_rows.append((run_id, run_date, key, state[key], dur[key]))
        critical = dur["extract"] + max(dur["transform_orders"], dur["transform_clicks"]) + dur["publish"]
        result = "FAILED" if failing else "SUCCESS"
        job_rows.append((run_id, run_date, result, setup, setup + critical))
    return job_rows, task_rows


def _build_run_history():
    job_rows, task_rows = _history_rows12()
    spark.createDataFrame(job_rows, "run_id BIGINT, run_date DATE, result_state STRING, setup_seconds INT, "
                                    "run_duration_seconds INT").write.mode("overwrite").saveAsTable("ch12_job_runs")
    spark.createDataFrame(task_rows, "run_id BIGINT, run_date DATE, task_key STRING, result_state STRING, "
                                     "execution_seconds INT").write.mode("overwrite").saveAsTable("ch12_task_runs")


def _build_job_log():
    spark.sql("CREATE TABLE IF NOT EXISTS perf_job_log (run_id STRING, step STRING, status STRING, "
              "seconds DOUBLE, rows_processed BIGINT, logged_at TIMESTAMP) "
              "COMMENT 'Section 12: one line per task execution of the 12-L1 job'")


def build_lab12(force: bool = False) -> list:
    built = []
    for name, fn in (("perf_events", _build_perf_events), ("perf_customers", _build_perf_dims),
                     ("ch12_job_runs", _build_run_history), ("perf_job_log", _build_job_log)):
        if force or not spark.catalog.tableExists(name):
            fn()
            built.append(name)
    return built


_built12 = build_lab12()
print("✅ Section 12 tables " + ("built: " + ", ".join(_built12) if _built12 else "ready"))

# COMMAND ----------

# DBTITLE 1,File-layout helpers (12-L3): files, data skipping, history, predictive optimization
def table_detail(t: str) -> dict:
    return spark.sql(f"DESCRIBE DETAIL {t}").first().asDict()


def layout(t: str, verbose: bool = True) -> dict:
    """numFiles, size, average file size, partition and clustering columns (from DESCRIBE DETAIL)."""
    d = table_detail(t)
    n, size = int(d.get("numFiles") or 0), int(d.get("sizeInBytes") or 0)
    info = {"table": t, "files": n, "size_mb": round(size / 1048576, 2),
            "avg_file_mb": round(size / 1048576 / max(n, 1), 3),
            "partitioned_by": list(d.get("partitionColumns") or []),
            "clustered_by": list(d.get("clusteringColumns") or [])}
    if verbose:
        extra = ""
        if info["partitioned_by"]:
            extra += f" · PARTITIONED BY {info['partitioned_by']}"
        if info["clustered_by"]:
            extra += f" · CLUSTER BY {info['clustered_by']}"
        print(f"📦 {t}: {n} files · {info['size_mb']} MB · avg {info['avg_file_mb']} MB per file{extra}")
    return info


def file_ranges(t: str, col: str):
    """One row per data file: rows, min and max of `col` - exactly the statistics Delta keeps per file for data skipping."""
    return (spark.table(t)
                 .groupBy(F.col("_metadata.file_path").alias("file_path"))
                 .agg(F.count(F.lit(1)).alias("rows"), F.min(col).alias("min_value"), F.max(col).alias("max_value"))
                 .withColumn("file", F.element_at(F.split("file_path", "/"), -1))
                 .select("file", "rows", "min_value", "max_value")
                 .orderBy("min_value", "max_value"))


def files_scanned(t: str, col: str, low, high=None) -> tuple:
    """(files Delta must open, total files) for WHERE col = low  (or col BETWEEN low AND high),
    using the min/max of each file - the same test Delta's data skipping does."""
    high = low if high is None else high
    ranges = file_ranges(t, col)
    total = ranges.count()
    hit = ranges.where((F.col("max_value") >= F.lit(low)) & (F.col("min_value") <= F.lit(high))).count()
    return hit, total


def skipping_report(t: str, col: str, low, high=None) -> tuple:
    hit, total = files_scanned(t, col, low, high)
    cond = f"{col} = '{low}'" if high is None else f"{col} BETWEEN '{low}' AND '{high}'"
    pct = 100 * (total - hit) / total if total else 0
    print(f"🔎 {t} WHERE {cond}: Delta must open {hit} of {total} files → {pct:.0f} % skipped")
    return hit, total


def history(t: str, n: int = 10):
    """DESCRIBE HISTORY with the interesting columns only."""
    return (spark.sql(f"DESCRIBE HISTORY {t} LIMIT {n}")
                 .select("version", "timestamp", "operation", "operationParameters", "operationMetrics"))


def last_op_metrics(t: str, operation: str = "OPTIMIZE") -> dict:
    """operationMetrics of the latest `operation` in the table history (numAddedFiles, numRemovedFiles, ...)."""
    rows = (spark.sql(f"DESCRIBE HISTORY {t}").where(F.col("operation") == operation)
                 .orderBy(F.desc("version")).limit(1).collect())
    return dict(rows[0]["operationMetrics"] or {}) if rows else {}


def optimize(t: str, extra: str = "", verbose: bool = True) -> dict:
    """Run OPTIMIZE t [extra] and return {'added': files written, 'removed': files rewritten} from its result."""
    t0 = time.time()
    res = spark.sql(f"OPTIMIZE {t} {extra}".strip()).first()
    m = {}
    try:
        m = res["metrics"].asDict(recursive=True)
    except Exception:
        pass
    if not m or m.get("numFilesAdded") is None:                    # fall back to the table history
        h = last_op_metrics(t, "OPTIMIZE")
        m = {"numFilesAdded": h.get("numAddedFiles", 0), "numFilesRemoved": h.get("numRemovedFiles", 0)}
    out = {"added": int(m.get("numFilesAdded") or 0), "removed": int(m.get("numFilesRemoved") or 0),
           "seconds": round(time.time() - t0, 1)}
    if verbose:
        print(f"🧹 OPTIMIZE {t} {extra}: rewrote {out['removed']} file(s) into {out['added']} ({out['seconds']} s)")
    return out


def po_status(kind: str, name: str) -> str:
    """Predictive optimization setting of a CATALOG / SCHEMA / TABLE, as shown by DESCRIBE ... EXTENDED."""
    try:
        rows = spark.sql(f"DESCRIBE {kind} EXTENDED {name}").collect()
    except Exception as e:
        return "? (" + _first_line(e) + ")"
    for r in rows:
        vals = [str(v) for v in r if v is not None]
        if vals and "predictive optimization" in vals[0].lower():
            return " ".join(vals[1:]).strip() or "(empty)"
    return "not shown by DESCRIBE"


def make_small_files(t: str, batches: int = 30, rows_per_batch: int = 20000, verbose: bool = True):
    """Simulate a chatty writer: `batches` tiny INSERTs -> (at least) one small file each."""
    spark.sql(f"CREATE OR REPLACE TABLE {t} TBLPROPERTIES ('delta.autoOptimize.autoCompact' = 'false') "
              "AS SELECT * FROM perf_events WHERE false")                  # no auto compaction: keep the small files
    for b in range(batches):
        spark.sql(f"INSERT INTO {t} SELECT * FROM perf_events "
                  f"WHERE event_id >= {b * rows_per_batch} AND event_id < {(b + 1) * rows_per_batch}")
        if verbose and (b + 1) % 10 == 0:
            print(f"   {b + 1}/{batches} small inserts done")

# COMMAND ----------

# DBTITLE 1,Spark-internals helpers (12-L2): partitions, Spark-UI-style summary metrics, plans
def partition_rows(df, n: int = None) -> list:
    """Rows in each partition of `df` (= input records of each task of the next stage). Empty partitions = 0."""
    counts = {r[0]: r[1] for r in df.groupBy(F.spark_partition_id().alias("p")).count().collect()}
    size = max(n or 0, (max(counts) + 1) if counts else 0)
    return [counts.get(i, 0) for i in range(size)]


def _pct(sorted_vals: list, p: float):
    if not sorted_vals:
        return 0
    return sorted_vals[int(round(p * (len(sorted_vals) - 1)))]


def stage_summary(df, n: int = None, title: str = "Stage") -> dict:
    """Spark-UI-style 'Summary Metrics for N completed tasks', using records per task.
    Rule of thumb from the Databricks Spark UI guide: Max more than 50 % above the 75th percentile -> skew."""
    vals = sorted(partition_rows(df, n))
    m = {"tasks": len(vals), "min": vals[0] if vals else 0, "p25": _pct(vals, .25), "median": _pct(vals, .5),
         "p75": _pct(vals, .75), "max": vals[-1] if vals else 0, "total": sum(vals)}
    m["max_share"] = round(100 * m["max"] / m["total"], 1) if m["total"] else 0
    m["skewed"] = m["max"] > 1.5 * max(m["p75"], 1)
    verdict = (f"⚠️ SKEW — the biggest task reads {m['max_share']} % of all rows "
               f"({m['max'] / max(m['p75'], 1):.1f}× the 75th percentile)") if m["skewed"] else "✅ balanced"
    cells = "".join(f"<td>{m[k]:,}</td>" for k in ("min", "p25", "median", "p75", "max"))
    displayHTML(f"""<div style="font-family:Arial;font-size:13px;max-width:760px">
      <b>{title}</b> — Summary Metrics for {m['tasks']} tasks (records read per task)
      <table style="border-collapse:collapse;margin:6px 0;width:100%">
      <tr style="background:#eef2f7"><th style="text-align:left;padding:4px 8px">Metric</th><th>Min</th><th>25th percentile</th>
      <th>Median</th><th>75th percentile</th><th>Max</th></tr>
      <tr style="text-align:center"><td style="text-align:left;padding:4px 8px">Input records</td>{cells}</tr></table>
      <span style="color:{'#c92a2a' if m['skewed'] else '#2b8a3e'};font-weight:bold">{verdict}</span></div>""")
    return m


def plan_text(df) -> str:
    """The plan of a DataFrame as text (EXPLAIN) - works on serverless too."""
    name = f"_plan12_{int(time.time() * 1000) % 10_000_000}"
    df.createOrReplaceTempView(name)
    try:
        return spark.sql(f"EXPLAIN SELECT * FROM {name}").first()[0]
    finally:
        spark.catalog.dropTempView(name)


_JOINS12 = ("BroadcastHashJoin", "SortMergeJoin", "ShuffledHashJoin", "BroadcastNestedLoopJoin", "CartesianProduct")


def plan_ops(df, verbose: bool = True) -> dict:
    """Shuffles (Exchange), broadcasts and join strategies in the physical plan."""
    phys = plan_text(df).split("== Physical Plan ==")[-1].splitlines()
    shuffles = [l.strip() for l in phys if "Exchange" in l and "Broadcast" not in l and "Source" not in l
                and "Reused" not in l]
    broadcasts = [l.strip() for l in phys if "BroadcastExchange" in l]
    joins = sorted({j for l in phys for j in _JOINS12 if j in l})
    res = {"shuffles": len(shuffles), "broadcasts": len(broadcasts), "joins": joins}
    if verbose:
        print(f"🧭 plan: {res['shuffles']} shuffle exchange(s) · {res['broadcasts']} broadcast(s) · joins: {joins or '-'}")
        for l in shuffles[:6]:
            print("   ↪", l[:140])
    return res


def time_it(label: str, df) -> float:
    """Run df completely (hash of every column, so nothing can be skipped) and return the seconds it took."""
    t0 = time.time()
    df.agg(F.max(F.xxhash64(*[F.col(c) for c in df.columns]))).collect()
    secs = round(time.time() - t0, 2)
    print(f"⏱️ {label}: {secs} s")
    return secs

# COMMAND ----------

# DBTITLE 1,Job-task logic (what labs/tasks/work runs)
def _log12(run_id: str, step: str, status: str, seconds: float, rows: int):
    spark.createDataFrame([(str(run_id), step, status, float(seconds), int(rows), _dt.datetime.now())],
                          "run_id STRING, step STRING, status STRING, seconds DOUBLE, rows_processed BIGINT, "
                          "logged_at TIMESTAMP").write.mode("append").saveAsTable("perf_job_log")


_STEP_SQL12 = {
    "extract": "SELECT count(*) FROM perf_events WHERE event_date = DATE'2026-06-30'",
    "transform_orders": ("SELECT count(*) FROM (SELECT c.country, sum(e.amount) FROM perf_events e "
                         "JOIN perf_customers c USING (customer_id) WHERE e.event_type = 'purchase' GROUP BY c.country)"),
    "transform_clicks": "SELECT count(*) FROM (SELECT product_id, count(*) FROM perf_events WHERE event_type = 'view' GROUP BY product_id)",
    "publish": "SELECT count(*) FROM perf_customers",
}


def task_work(step: str, base_seconds="10", run_id="", slow_step="none", slow_factor="1", fail_step="none") -> dict:
    """One task of the 12-L1 job: a real (small) Spark query + simulated extra work time.
    slow_step/slow_factor make one task slower; fail_step makes one task fail - to create a run history to analyse."""
    t0 = time.time()
    rows = spark.sql(_STEP_SQL12.get(step, "SELECT 1")).first()[0]
    if step == fail_step:
        _log12(run_id, step, "FAILED", time.time() - t0, rows)
        raise RuntimeError(f"[SIMULATED FAILURE] {step}: source column `order_ts` not found - "
                           "the upstream schema changed (fail_step job parameter)")
    factor = float(slow_factor or 1) if step == slow_step else 1.0
    time.sleep(max(0.0, float(base_seconds or 0) * factor - (time.time() - t0)))   # simulated processing time
    secs = round(time.time() - t0, 1)
    _log12(run_id, step, "SUCCESS", secs, rows)
    return {"step": step, "seconds": secs, "rows": rows, "slow_factor": factor}


def job_log(n: int = 30):
    return spark.sql(f"SELECT * FROM perf_job_log ORDER BY logged_at DESC LIMIT {n}")

# COMMAND ----------

# DBTITLE 1,Jobs REST API helpers (through the Databricks SDK)
_ws12 = None


def _ws():
    global _ws12
    if _ws12 is None:
        from databricks.sdk import WorkspaceClient
        _ws12 = WorkspaceClient()
    return _ws12


def _api(method: str, path: str, body=None, query=None):
    return _ws().api_client.do(method, path, query=query, body=body) or {}


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
    """Workspace path of the Section 12 folder, derived from the running notebook.
    Override: SECTION12_ROOT = '/Workspace/<path>/12 - Troubleshooting, Monitoring and Optimization' before %run."""
    if globals().get("SECTION12_ROOT"):
        return SECTION12_ROOT.rstrip("/")
    nb = _notebook_path()
    cut = max(nb.rfind("/labs/"), nb.rfind("/data/"), nb.rfind("/questions/"))
    if cut < 0:
        raise RuntimeError("Could not find the section folder from this notebook's path. Set SECTION12_ROOT = "
                           "'/Workspace/<path to>/12 - Troubleshooting, Monitoring and Optimization' before the %run cells.")
    root = nb[:cut]
    return root if root.startswith("/Workspace") else "/Workspace" + root


def task_path(name: str = "work") -> str:
    return f"{_section_root()}/labs/tasks/{name}"


def _paged(path: str, key: str, query=None, max_pages: int = 20) -> list:
    qy, out = dict(query or {}), []
    for _ in range(max_pages):
        res = _api("GET", path, query=qy)
        out += res.get(key) or []
        token = res.get("next_page_token")
        if not token:
            break
        qy["page_token"] = token
    return out


def find_job(name: str):
    """The job called `name` that YOU created (newest first) - or None."""
    me = _me()
    jobs = [j for j in _paged("/api/2.2/jobs/list", "jobs", {"name": name, "limit": 100})
            if (j.get("settings") or {}).get("name") == name and j.get("creator_user_name") in (me, None)]
    if not jobs:
        return None
    jobs.sort(key=lambda j: j.get("created_time", 0), reverse=True)
    return _api("GET", "/api/2.2/jobs/get", query={"job_id": jobs[0]["job_id"]})


def job_link(name: str):
    j = find_job(name)
    if not j:
        print(f"No job called '{name}' yet.")
        return
    displayHTML(f'<a href="{_host()}/jobs/{j["job_id"]}/runs" target="_blank">🔗 Open the runs of job “{name}”</a>')


def create_or_replace_job(spec: dict) -> int:
    j = find_job(spec["name"])
    if j:
        _api("POST", "/api/2.2/jobs/reset", body={"job_id": j["job_id"], "new_settings": spec})
        print(f"🔁 job '{spec['name']}' ({j['job_id']}) replaced")
        return j["job_id"]
    job_id = _api("POST", "/api/2.2/jobs/create", body=spec)["job_id"]
    print(f"🆕 job '{spec['name']}' created ({job_id})")
    return job_id


def nb_task(key: str, notebook: str = "work", depends=(), params: dict = None, **extra) -> dict:
    t = {"task_key": key,
         "notebook_task": {"notebook_path": task_path(notebook), "source": "WORKSPACE",
                           "base_parameters": dict(params or {})}}
    if depends:
        t["depends_on"] = [{"task_key": d} for d in depends]
    t.update(extra)
    return t


def l1_job_spec() -> dict:
    """extract -> (transform_orders, transform_clicks) -> publish, all tasks run labs/tasks/work on serverless."""
    def p(step, secs):
        return {"step": step, "base_seconds": str(secs), "run_id": "{{job.run_id}}"}
    return {"name": JOB_NAMES12["L1"], "max_concurrent_runs": 1, "queue": {"enabled": True},
            "tags": {"section": "12"},
            "parameters": [{"name": "slow_step", "default": "none"}, {"name": "slow_factor", "default": "1"},
                           {"name": "fail_step", "default": "none"},
                           {"name": "catalog", "default": catalog_name}, {"name": "schema", "default": schema_name}],
            "tasks": [nb_task("extract", params=p("extract", 10)),
                      nb_task("transform_orders", depends=["extract"], params=p("transform_orders", 25)),
                      nb_task("transform_clicks", depends=["extract"], params=p("transform_clicks", 20)),
                      nb_task("publish", depends=["transform_orders", "transform_clicks"], params=p("publish", 5))]}

# COMMAND ----------

# DBTITLE 1,Runs: start, wait, history, baseline, errors
def _task_state(t: dict) -> str:
    st = t.get("state") or {}
    return st.get("result_state") or st.get("life_cycle_state") or (t.get("status") or {}).get("state") or "?"


def job_runs(name: str, n: int = 25) -> list:
    """The latest runs of the job (newest first)."""
    j = find_job(name)
    if not j:
        return []
    return _api("GET", "/api/2.2/jobs/runs/list", query={"job_id": j["job_id"], "limit": min(n, 25)}).get("runs", [])


def wait_for_run(run_id: int, timeout_min: int = 30, verbose: bool = True) -> dict:
    t0, last = time.time(), None
    while time.time() - t0 < timeout_min * 60:
        r = _api("GET", "/api/2.2/jobs/runs/get", query={"run_id": run_id})
        lc = (r.get("state") or {}).get("life_cycle_state")
        if verbose and lc != last:
            print(f"   {time.strftime('%H:%M:%S')}  run {run_id}: {lc}")
            last = lc
        if lc in ("TERMINATED", "INTERNAL_ERROR", "SKIPPED"):
            break
        time.sleep(10)
    return r


def run_job_now(name: str, params: dict = None, wait: bool = True) -> int:
    """Like 'Run now' / 'Run now with different parameters'. Returns the run_id."""
    j = find_job(name)
    if not j:
        raise RuntimeError(f"No job called '{name}' - create it first.")
    body = {"job_id": j["job_id"]}
    if params:
        body["job_parameters"] = {k: str(v) for k, v in params.items()}
    run_id = _api("POST", "/api/2.2/jobs/run-now", body=body)["run_id"]
    print(f"🚀 '{name}' run {run_id} started" + (f" with {params}" if params else ""))
    if wait:
        wait_for_run(run_id)
    return run_id


def _ms_to_s(ms) -> float:
    return round((ms or 0) / 1000, 1)


def _run_row(det: dict) -> dict:
    st, status = det.get("state") or {}, det.get("status") or {}
    term = (status.get("termination_details") or {})
    start, end = det.get("start_time") or 0, det.get("end_time") or 0
    row = {"run_id": det.get("run_id"),
           "started": _dt.datetime.fromtimestamp(start / 1000, _dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S") if start else "",
           "state": st.get("result_state") or st.get("life_cycle_state") or status.get("state") or "?",
           "termination_code": term.get("code", ""),
           "duration_s": _ms_to_s(det.get("run_duration") or ((end - start) if end and start else 0)),
           "queue_s": _ms_to_s(det.get("queue_duration")),
           "params": ", ".join(f"{p['name']}={p.get('value', p.get('default'))}" for p in det.get("job_parameters") or []
                               if p["name"] in ("slow_step", "slow_factor", "fail_step")),
           "tasks": {}}
    for t in det.get("tasks") or []:
        ts, te = t.get("start_time") or 0, t.get("end_time") or 0
        secs = _ms_to_s(t.get("execution_duration")) or (_ms_to_s(te - ts) if ts and te else 0.0)
        row["tasks"][t["task_key"]] = {"state": _task_state(t), "seconds": secs, "setup_s": _ms_to_s(t.get("setup_duration")),
                                       "task_run_id": t.get("run_id")}
    return row


def run_history(name: str, n: int = 20) -> list:
    """Finished runs of the job, OLDEST first, with run and task durations (what the matrix view shows)."""
    out = []
    for r in reversed(job_runs(name, n)):
        det = _api("GET", "/api/2.2/jobs/runs/get", query={"run_id": r["run_id"]})
        if (det.get("state") or {}).get("life_cycle_state") in ("TERMINATED", "INTERNAL_ERROR", "SKIPPED"):
            out.append(_run_row(det))
    return out


def history_df(rows: list):
    """run_history() as a DataFrame: one row per run, one duration column per task (seconds)."""
    keys = []
    for r in rows:
        keys += [k for k in r["tasks"] if k not in keys]
    data = [(str(r["run_id"]), r["started"], r["state"], r["params"], float(r["duration_s"]))
            + tuple(float(r["tasks"].get(k, {}).get("seconds") or 0) for k in keys) for r in rows]
    schema = "run_id STRING, started STRING, state STRING, params STRING, run_s DOUBLE" + \
             "".join(f", `{k}_s` DOUBLE" for k in keys)
    return spark.createDataFrame(data, schema)


def _median(vals: list) -> float:
    v = sorted(vals)
    if not v:
        return 0.0
    mid = len(v) // 2
    return float(v[mid]) if len(v) % 2 else (v[mid - 1] + v[mid]) / 2


def baseline_report(rows: list, current_run_id=None, threshold: float = 1.5) -> dict:
    """Compare one run (default: the newest) with the MEDIAN of the earlier SUCCESSFUL runs - run and task level."""
    if not rows:
        print("no finished runs yet")
        return {}
    cur = next((r for r in rows if r["run_id"] == current_run_id), rows[-1]) if current_run_id else rows[-1]
    base = [r for r in rows if r["state"] == "SUCCESS" and r["run_id"] != cur["run_id"]
            and rows.index(r) < rows.index(cur)]
    if not base:
        print("no earlier successful runs to build a baseline from")
        return {}
    report = {"run_id": cur["run_id"], "state": cur["state"], "baseline_runs": len(base), "tasks": {}}
    lines = [("RUN (total)", _median([r["duration_s"] for r in base]), cur["duration_s"])]
    for k, t in cur["tasks"].items():
        lines.append((k, _median([r["tasks"].get(k, {}).get("seconds", 0) for r in base]), t["seconds"]))
    print(f"📈 run {cur['run_id']} ({cur['state']}) vs baseline = median of {len(base)} earlier successful run(s)")
    print(f"   {'':<18}{'baseline s':>11}{'this run s':>12}{'ratio':>8}")
    for k, b, c in lines:
        ratio = c / b if b else 0
        flag = "  🔺 REGRESSION" if b and ratio > threshold else ""
        state = cur["tasks"].get(k, {}).get("state", "")
        if state and state != "SUCCESS":
            flag = f"  ⛔ {state}"
        print(f"   {k:<18}{b:>11.1f}{c:>12.1f}{ratio:>8.2f}{flag}")
        report["tasks"][k] = {"baseline": b, "current": c, "ratio": round(ratio, 2), "flag": flag.strip()}
    finished = len(rows)
    failed = len([r for r in rows if r["state"] not in ("SUCCESS",)])
    report["failure_rate"] = round(100 * failed / finished, 1)
    print(f"   failure rate over the last {finished} finished runs: {failed}/{finished} = {report['failure_rate']} %")
    return report


def run_errors(run_id: int, verbose: bool = True) -> dict:
    """For each task of a run that did not succeed: its state and the first line of its error (runs/get-output)."""
    det = _api("GET", "/api/2.2/jobs/runs/get", query={"run_id": run_id})
    out = {}
    for t in det.get("tasks") or []:
        state = _task_state(t)
        if state == "SUCCESS":
            continue
        msg = (t.get("state") or {}).get("state_message", "")
        try:
            o = _api("GET", "/api/2.2/jobs/runs/get-output", query={"run_id": t["run_id"]})
            msg = o.get("error") or msg
        except Exception:
            pass
        out[t["task_key"]] = {"state": state, "error": (msg or "").splitlines()[0][:200] if msg else ""}
        if verbose:
            print(f"   • {t['task_key']:<18} {state:<16} {out[t['task_key']]['error']}")
    term = ((det.get("status") or {}).get("termination_details") or {})
    if verbose and term:
        print(f"   run termination: code={term.get('code')} type={term.get('type')}")
    return out


def set_duration_threshold(name: str, seconds: int, email_me: bool = False):
    """Health rule RUN_DURATION_SECONDS > seconds  (UI: Job details → Duration threshold → Warning)."""
    j = find_job(name)
    settings = {"health": {"rules": [{"metric": "RUN_DURATION_SECONDS", "op": "GREATER_THAN", "value": int(seconds)}]}}
    if email_me:
        settings["email_notifications"] = {"on_duration_warning_threshold_exceeded": [_me()]}
    _api("POST", "/api/2.2/jobs/update", body={"job_id": j["job_id"], "new_settings": settings})
    print(f"⏰ '{name}': duration warning when a run takes longer than {int(seconds)} s")


def delete_lab12_jobs():
    for j in _paged("/api/2.2/jobs/list", "jobs", {"limit": 100}):
        name = (j.get("settings") or {}).get("name") or ""
        if name.startswith("12-L") and j.get("creator_user_name") in (_me(), None):
            _api("POST", "/api/2.2/jobs/delete", body={"job_id": j["job_id"]})
            print("🗑️ job", name)

# COMMAND ----------

# DBTITLE 1,Reset Section 12
def reset_lab12(confirm: str = ""):
    """Delete the Section 12 jobs and drop the perf_* / ch12_* tables (the base dataset stays)."""
    if confirm != "YES":
        print("Nothing done. To really reset Section 12 call: reset_lab12(confirm='YES')")
        return
    try:
        delete_lab12_jobs()
    except Exception as e:
        print("jobs:", _first_line(e))
    for r in spark.sql("SHOW TABLES").collect():
        n = r["tableName"]
        if not r["isTemporary"] and n.startswith(("perf_", "ch12_")):
            spark.sql(f"DROP TABLE IF EXISTS {n}")
            print("🗑️ table", n)
    print("✅ Section 12 reset - re-run %run ./_12_prepare to rebuild the tables")


print("🔧 Section 12 helpers loaded")
