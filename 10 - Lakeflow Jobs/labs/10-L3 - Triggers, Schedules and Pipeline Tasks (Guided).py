# Databricks notebook source
# MAGIC %md
# MAGIC # 🧪 Lab 10-L3 · Triggers, Schedules and Pipeline Tasks (Guided)
# MAGIC **Time:** ~60 min · **Compute:** Serverless notebook + serverless jobs + serverless pipeline + your SQL warehouse ·
# MAGIC **Works on Free Edition**
# MAGIC
# MAGIC So far you clicked **Run now**. In production nobody clicks: jobs start on a **schedule** or when **data arrives**. In this
# MAGIC lab you configure every trigger type of the exam and orchestrate a **pipeline** from a job:
# MAGIC
# MAGIC ```
# MAGIC Part 2  10-L3 Land and Pipeline     land_orders ──▶ pipeline task (Spark Declarative Pipeline)                [manual]
# MAGIC Part 3  10-L3 File Arrival Load     load_bronze ──▶ build_silver_gold        [📥 file arrival: raw/lab10/inbox/]
# MAGIC Part 4  10-L3 KPIs on Silver Update gold_kpis (SQL file)                     [📥 table update: jobs_silver_orders]
# MAGIC Part 1  10-L1 ShopWave Daily Load   + ⏰ schedule (Quartz cron), then paused
# MAGIC ```
# MAGIC
# MAGIC | Part | You will… |
# MAGIC |---|---|
# MAGIC | 1 | 🖱️ Add a **scheduled** trigger (Quartz cron + time zone) to the 10-L1 job — and pause it |
# MAGIC | 2 | Create a small **pipeline** and 🖱️ orchestrate it with a **pipeline task** |
# MAGIC | 3 | 🖱️ A job with a **file arrival** trigger on a volume |
# MAGIC | 4 | 🖱️ A job with a **table update** trigger — two jobs chained **by data** |
# MAGIC | 5 | Monitor: trigger types of runs, **system tables** |
# MAGIC | 6 | **Pause** every trigger · ✅ checks |

# COMMAND ----------

# MAGIC %run ../../Includes/_setup

# COMMAND ----------

# MAGIC %run ./_10_prepare

# COMMAND ----------

# DBTITLE 1,Start clean (safe to re-run)
try:
    pause_lab10_triggers()                                # nothing may start by itself while we reset
except Exception as e:
    print("note:", _first_line(e))
reset_lab10_data()
SQL_FILE = write_sql_task_file()
task_build_silver_gold()                                  # creates the (empty) silver table the table-update trigger watches
L3_STARTED_MS = now_ms()
print("\nlab start (epoch ms):", L3_STARTED_MS)


def check(label, condition):
    print(("✅ " if condition else "❌ ") + label)
    return bool(condition)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1 · 🖱️ A scheduled trigger (time-based)
# MAGIC The finance team wants the daily load to run **every day at 06:00 Riyadh time**.
# MAGIC
# MAGIC 1. Open the job **`10-L1 ShopWave Daily Load`** (create it with 10-L1 first if you skipped that lab).
# MAGIC 2. **Job details** panel → **Schedules & Triggers** → **Add trigger** → **Trigger type: Scheduled**.
# MAGIC 3. Switch the schedule to **Advanced** and enter the Quartz cron expression **`0 0 6 * * ?`**, time zone
# MAGIC    **(UTC+03:00) Asia/Riyadh**. (In *Simple* mode you could only say “every 1 day” — without choosing the time.)
# MAGIC 4. **Save**. Then set **Trigger status: Paused** (or click **Pause** next to the schedule) — we don't want a 06:00 run of a
# MAGIC    lab job. A paused trigger keeps its configuration.
# MAGIC
# MAGIC > 🧠 Quartz = `seconds minutes hours day-of-month month day-of-week`. `0 0 6 * * ?` = 06:00:00 every day; `?` = “no specific
# MAGIC > value” for day-of-week. *Weekdays at 02:30* would be `0 30 2 ? * MON-FRI`.

# COMMAND ----------

# DBTITLE 1,✅ Check Part 1
_s1 = job_summary(JOB_NAMES10["L1"], verbose=False)
_sch = (_s1 or {}).get("schedule") or {}
check("the 10-L1 job has a Quartz schedule 0 0 6 * * ?",
      " ".join((_sch.get("quartz_cron_expression") or "").split()) == "0 0 6 * * ?")
check("time zone Asia/Riyadh", _sch.get("timezone_id") == "Asia/Riyadh")
check("the schedule is PAUSED", _sch.get("pause_status") == "PAUSED")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2 · A pipeline task — orchestrating Spark Declarative Pipelines
# MAGIC The pipeline (source: `labs/pipelines/jobs_orders/transformations/orders.sql`) has a **streaming table**
# MAGIC `pl10_bronze_orders` (reads new inbox files with `read_files`) and a **materialized view** `pl10_daily_revenue`. Its
# MAGIC configuration `inbox_path` points at the lab10 inbox.
# MAGIC
# MAGIC **Create the pipeline** — run the cell (or create it in the UI: *Jobs & Pipelines → Create → ETL pipeline*, root folder
# MAGIC `labs/pipelines/jobs_orders`, default catalog/schema = your course schema, configuration `inbox_path` = the path printed
# MAGIC below, serverless).

# COMMAND ----------

# DBTITLE 1,Create (or update) the pipeline
print("inbox_path =", INBOX10)
PIPELINE_ID = create_or_update_pipeline10()
displayHTML(f'<a href="{_host()}/pipelines/{PIPELINE_ID}" target="_blank">🔗 Open the pipeline “{PIPELINE10}”</a>')

# COMMAND ----------

# MAGIC %md
# MAGIC **Create the job** **`10-L3 Land and Pipeline`** in the UI:
# MAGIC
# MAGIC | # | Task name | Type | Settings | Depends on |
# MAGIC |---|---|---|---|---|
# MAGIC | 1 | `land_orders` | Notebook | `labs/tasks/land_orders` · Serverless | — |
# MAGIC | 2 | `refresh_pipeline` | **Pipeline** | Pipeline: **`10-L3 ShopWave inbox pipeline`** · *Full refresh*: **off** | `land_orders` |
# MAGIC
# MAGIC Add the job parameter **`files`** = **`1`**, then **Run now**. A pipeline task runs one **update** of the pipeline with the
# MAGIC pipeline's own (serverless) compute — the task itself has no compute setting.

# COMMAND ----------

# DBTITLE 1,After the pipeline job run
last_run(JOB_NAMES10["L3_PIPE"])
for _t in ("pl10_bronze_orders", "pl10_daily_revenue"):
    if spark.catalog.tableExists(_t):
        display(spark.table(_t).groupBy().count() if _t == "pl10_bronze_orders" else spark.table(_t).orderBy("order_date"))

# COMMAND ----------

# MAGIC %md
# MAGIC Expected: inbox `01.json` · `pl10_bronze_orders` **121** rows · `pl10_daily_revenue` 1 row: 2026-07-01, **119** orders,
# MAGIC **60,716.61**. Open the pipeline: its update was **started by the job** (the update shows the job as the cause), and it
# MAGIC ran in **production mode** behaviour (retries) because it was triggered by a job, not from the editor.
# MAGIC
# MAGIC > 🧠 **Pipeline schedule or job?** A pipeline can have its own schedule. Use a **pipeline task** when the pipeline must
# MAGIC > run **in order** with other work (land files → pipeline → SQL checks → dashboard task). Don't do both — the pipeline would
# MAGIC > run twice.
# MAGIC
# MAGIC ## Part 3 · 🖱️ A file arrival trigger (data-driven)
# MAGIC Partners drop files into the inbox at **unpredictable times**. Instead of polling every 5 minutes, start the load when a
# MAGIC **new file arrives**.
# MAGIC
# MAGIC Create the job **`10-L3 File Arrival Load`**:
# MAGIC
# MAGIC | # | Task name | Type | Path | Depends on |
# MAGIC |---|---|---|---|---|
# MAGIC | 1 | `load_bronze` | Notebook | `labs/tasks/load_bronze` · Serverless | — |
# MAGIC | 2 | `build_silver_gold` | Notebook | `labs/tasks/build_silver_gold` · Serverless | `load_bronze` |
# MAGIC
# MAGIC Then **Schedules & Triggers** → **Add trigger** → **File arrival**:
# MAGIC * **Storage location**: the inbox path printed below — **with a trailing `/`**.
# MAGIC * **Advanced** → **Wait after last change**: `60` seconds (debounce: wait until the partner has finished uploading) ·
# MAGIC   **Minimum time between triggers**: `60` seconds (cool-down).
# MAGIC * Trigger status **Active** → **Save**. (Use **Test connection** if offered: it verifies the job can read the location.)

# COMMAND ----------

# DBTITLE 1,The path for the file arrival trigger
print(INBOX10 + "/")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 4 · 🖱️ A table update trigger — chaining jobs by data
# MAGIC A second team owns the KPIs. Their job must start **whenever silver changes**, no matter who wrote it or when.
# MAGIC
# MAGIC Create the job **`10-L3 KPIs on Silver Update`**:
# MAGIC
# MAGIC | # | Task name | Type | Settings |
# MAGIC |---|---|---|---|
# MAGIC | 1 | `gold_kpis` | **SQL** → **File** | the `.sql` file from 10-L1 (path printed below) · your SQL warehouse |
# MAGIC
# MAGIC **Schedules & Triggers** → **Add trigger** → **Table update** → **Tables**: the silver table printed below · condition
# MAGIC **Any updated** (with several tables you could require **All updated**) · **Advanced** → **Wait after last change**
# MAGIC `30` seconds · status **Active** → **Save**.

# COMMAND ----------

# DBTITLE 1,Paths for Part 4
print("table to watch :", job_table("jobs_silver_orders"))
print("SQL file       :", SQL_FILE)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Now let data drive both jobs
# MAGIC The next cell **lands one file** in the inbox and then waits: first for the **file arrival** job (it should start within ~1–2
# MAGIC minutes + the 60 s debounce), then for the **table update** job (it starts after the first job rewrites silver).
# MAGIC Nobody clicks *Run now*.

# COMMAND ----------

# DBTITLE 1,Land a file and watch the chain (takes ~3-8 minutes)
_t0 = now_ms()
land10(1)                                   # 02.json - the pipeline job landed 01.json in Part 2
_runF = wait_for_new_run(JOB_NAMES10["L3_FILE"], _t0, timeout_min=10)
_runT = wait_for_new_run(JOB_NAMES10["L3_TABLE"], _t0, timeout_min=10) if _runF else None
if spark.catalog.tableExists("jobs_gold_kpis"):
    display(spark.table("jobs_gold_kpis"))

# COMMAND ----------

# MAGIC %md
# MAGIC Expected: `10-L3 File Arrival Load` ran with trigger **FILE_ARRIVAL** and loaded **both** inbox files that bronze hadn't
# MAGIC seen (`01.json` from Part 2 and `02.json`) → silver **238** rows, gold 2 days · then `10-L3 KPIs on Silver Update` ran with
# MAGIC a **table** trigger → `jobs_gold_kpis` = **238** orders, revenue **132,359.60**.
# MAGIC
# MAGIC > 🧠 The file-arrival job's task **finds** the new files itself (`load_bronze` compares the inbox with what bronze already
# MAGIC > has) — the trigger only says *“something new arrived”*. Auto Loader / streaming tables do the same with checkpoints.
# MAGIC >
# MAGIC > 🧪 **Try it:** run `land10(2)` in a new cell. Two files arrive within seconds → thanks to *wait after last change*, you
# MAGIC > get **one** run that loads both, not two.
# MAGIC
# MAGIC ## Part 5 · Monitoring: who started which run?

# COMMAND ----------

# DBTITLE 1,Runs of the Section 10 jobs and their trigger types
_rows = []
for _key, _name in JOB_NAMES10.items():
    for _r in job_runs(_name, 10):
        if (_r.get("start_time") or 0) >= L3_STARTED_MS - 3600 * 1000:
            _rows.append((_name, _r["run_id"], _r.get("trigger"),
                          (_r.get("state") or {}).get("result_state") or (_r.get("state") or {}).get("life_cycle_state"),
                          _dt.datetime.fromtimestamp(_r["start_time"] / 1000, _dt.timezone.utc).replace(tzinfo=None)))
display(spark.createDataFrame(_rows or [("-", 0, "-", "-", None)],
                              "job STRING, run_id LONG, trigger STRING, result STRING, start_time_utc TIMESTAMP")
        .orderBy(F.col("start_time_utc").desc()))

# COMMAND ----------

# MAGIC %md
# MAGIC `ONE_TIME` = Run now / API · `FILE_ARRIVAL` · `TABLE` (table update) · `PERIODIC` (schedule) · `RETRY` · `RUN_JOB_TASK`.
# MAGIC
# MAGIC The same history is in the **system tables** (`system.lakeflow.*`, Unity Catalog). They can lag a few minutes and need
# MAGIC access to the `system` catalog — the cell tells you if they are not available to you.

# COMMAND ----------

# DBTITLE 1,system.lakeflow.job_run_timeline (if you have access)
try:
    display(spark.sql("""
        SELECT j.name, t.run_id, t.trigger_type, t.result_state, t.period_start_time, t.period_end_time
        FROM system.lakeflow.job_run_timeline t
        JOIN (SELECT job_id, name FROM system.lakeflow.jobs
              QUALIFY row_number() OVER (PARTITION BY job_id ORDER BY change_time DESC) = 1) j USING (job_id)
        WHERE j.name LIKE '10-L%' AND t.period_start_time > current_timestamp() - INTERVAL 1 DAY
        ORDER BY t.period_start_time DESC
        LIMIT 20"""))
except Exception as e:
    print("system tables not available here:", _first_line(e))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 6 · Pause everything + ✅ checks
# MAGIC Active triggers keep watching. On a paid workspace a forgotten trigger costs money every time data changes — pause them.

# COMMAND ----------

# DBTITLE 1,Pause all Section 10 triggers
pause_lab10_triggers()

# COMMAND ----------

# DBTITLE 1,✅ Checks
_sp = job_summary(JOB_NAMES10["L3_PIPE"], verbose=False) or {"tasks": {}}
_pipe_tasks = [t for t in _sp["tasks"].values() if t["type"] == "pipeline"]
check("10-L3 Land and Pipeline has a pipeline task (the 10-L3 pipeline) after land_orders",
      len(_pipe_tasks) == 1 and _pipe_tasks[0].get("pipeline_id") == (find_pipeline10() or [None])[0]
      and [d for d, _ in _pipe_tasks[0]["depends_on"]] == ["land_orders"])
check("the pipeline filled pl10_bronze_orders", spark.catalog.tableExists("pl10_bronze_orders")
      and spark.table("pl10_bronze_orders").count() >= 121)
_sf = job_summary(JOB_NAMES10["L3_FILE"], verbose=False) or {}
check("10-L3 File Arrival Load: file arrival trigger on the inbox",
      _sf.get("trigger_type") == "file_arrival"
      and (_sf.get("trigger") or {}).get("url", "").rstrip("/") == INBOX10.rstrip("/"))
check("… with 'wait after last change' set",
      int((_sf.get("trigger") or {}).get("wait_after_last_change_seconds") or 0) > 0)
_st = job_summary(JOB_NAMES10["L3_TABLE"], verbose=False) or {}
_tables = [t.lower() for t in ((_st.get("trigger") or {}).get("table_names") or [])]
check("10-L3 KPIs on Silver Update: table update trigger on jobs_silver_orders",
      _st.get("trigger_type") == "table_update" and job_table("jobs_silver_orders").lower() in _tables)
check("a run started by FILE_ARRIVAL", any(r.get("trigger") == "FILE_ARRIVAL" for r in job_runs(JOB_NAMES10["L3_FILE"], 10)))
check("a run started by the table update trigger",
      any("TABLE" in str(r.get("trigger")) for r in job_runs(JOB_NAMES10["L3_TABLE"], 10)))
check("all Section 10 triggers are paused",
      all(((job_summary(n, verbose=False) or {}).get("trigger_paused") in (None, "PAUSED"))
          for n in (JOB_NAMES10["L3_FILE"], JOB_NAMES10["L3_TABLE"])))

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ What you practised
# MAGIC * **Scheduled** trigger with **Quartz cron** + time zone; **pausing** a trigger.
# MAGIC * **Pipeline task**: a job runs a Spark Declarative Pipeline update in order with other tasks (pipeline compute).
# MAGIC * **File arrival** trigger on a **UC volume** path, with *wait after last change* (debounce) and *minimum time between
# MAGIC   triggers* (cool-down).
# MAGIC * **Table update** trigger: a consumer job starts whenever an upstream table changes — jobs chained **by data**, not by
# MAGIC   guessed times.
# MAGIC * Run **trigger types** in the UI/API and in `system.lakeflow.job_run_timeline`.
# MAGIC
# MAGIC ➡️ Next: **10-L4 · Challenge Lab**
