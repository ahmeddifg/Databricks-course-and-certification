# Databricks notebook source
# MAGIC %md
# MAGIC # 🤖 Lab 14-L4 · Automate It — Pipeline, Job, Monitoring and CI/CD (Guided)
# MAGIC **Capstone part 4 of 4** · **Time:** ~70 min (mostly waiting for runs) · **Compute:** Serverless · **Works on Free Edition**
# MAGIC
# MAGIC Your lakehouse works — when **you** run the notebooks. Now SkillWave wants it to run **every morning by itself**, with a
# MAGIC quality gate, and the code under version control.
# MAGIC
# MAGIC **The daily job you'll build**
# MAGIC
# MAGIC ```
# MAGIC                     ┌──▶ ingest_bronze ──▶ build_silver ──▶ check_quality ──true──▶ quarantine_alert
# MAGIC  land_day ──────────┤    (1 retry)         (task value       (If/else:
# MAGIC  (job param days)   │                       quarantined)      quarantined > max_quarantine)
# MAGIC                     │                                          └──false──┐
# MAGIC                     └──▶ refresh_pipeline (pipeline task) ───────────────┴──▶ build_gold
# MAGIC ```
# MAGIC
# MAGIC | Part | You will… | Exam objective |
# MAGIC |---|---|---|
# MAGIC | 1 | Create and run a **Lakeflow Spark Declarative Pipeline** (SQL files, expectations) | D2 / D3 |
# MAGIC | 2 | Build the **multi-task job**: dependencies, parameters, task values, If/else, retries, pipeline task, schedule | D4 |
# MAGIC | 3 | Run it 3 times — normal day, quality gate tripped, catch-up | D4 |
# MAGIC | 4 | **Monitor**: run history, pipeline event log, system tables | D6 |
# MAGIC | 5 | **CI/CD**: Git folder, job as code, Declarative Automation Bundles | D5 |
# MAGIC | 6 | ✅ Checks + pause the schedule | |
# MAGIC
# MAGIC Each part has a **🖱️ UI route** (recommended — it's what the exam describes) and a **🅱️ Plan B** cell that does the same by
# MAGIC code, in case you get stuck.

# COMMAND ----------

# MAGIC %run ./_14_prepare

# COMMAND ----------

# DBTITLE 1,Status - needs gold from 14-L3
require_gold()
print("landed enrollment days:", landed_days())
print("silver.enrollments:", count("silver.enrollments"), "· gold.fact_enrollments:", count("gold.fact_enrollments"))
if max(landed_days()) > 4:
    print("ℹ️ More than 4 days have landed already (you ran this lab before) - your numbers will differ from the text.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1 · The declarative pipeline
# MAGIC Look at the three SQL files in **`labs/pipelines/skillwave_sdp/transformations/`** (open them in the workspace browser):
# MAGIC
# MAGIC | File | Defines | Type |
# MAGIC |---|---|---|
# MAGIC | `01_bronze.sql` | `sdp_bronze_enrollments` — `FROM STREAM read_files('${landing_path}/enrollments', format => 'json')` | streaming table |
# MAGIC | `02_silver.sql` | `sdp_silver_enrollments` — 4 **expectations** (fail / drop / drop / warn) + stream-static join to `silver.courses` | streaming table |
# MAGIC | `03_gold.sql` | `sdp_gold_daily_revenue` — revenue per day and category | materialized view |
# MAGIC
# MAGIC You only **declare** the tables; the pipeline works out the order (the DAG), creates checkpoints, retries and the
# MAGIC incremental processing.

# COMMAND ----------

# DBTITLE 1,1.1 A schema for the pipeline's tables
# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS skillwave.sdp COMMENT 'Tables published by the SkillWave declarative pipeline';

# COMMAND ----------

# DBTITLE 1,1.2 Values you'll type in the UI
print("Pipeline name        :", PIPELINE14)
print("Root folder          :", f"{_section_root()}/labs/pipelines/skillwave_sdp")
print("Default catalog      :", CAT)
print("Default schema       : sdp")
print("Configuration key    : landing_path")
print("Configuration value  :", LANDING)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🖱️ UI route — create the pipeline
# MAGIC 1. Left sidebar → **Jobs & Pipelines** → **Create** → **ETL pipeline**.
# MAGIC 2. Name it exactly **`14 SkillWave SDP pipeline`**. Default **catalog** `skillwave`, default **schema** `sdp`.
# MAGIC 3. Choose **Add existing assets** and select the **root folder** printed above (`…/labs/pipelines/skillwave_sdp`) and the
# MAGIC    source folder `transformations`.
# MAGIC 4. Open **Settings** (gear) → **Configuration** → **Add configuration**: key `landing_path`, value = the path printed above.
# MAGIC    Check that compute is **Serverless**. Save.
# MAGIC 5. Click **Run pipeline**. Watch the **graph**: bronze → silver → gold, with row counts and the expectation results.
# MAGIC
# MAGIC ### 🅱️ Plan B — the same by code (Pipelines REST API)

# COMMAND ----------

# DBTITLE 1,1.3 Plan B - create (or update) and run the pipeline by code
RUN_PLAN_B_PIPELINE = False          # set True only if the UI route didn't work for you
if RUN_PLAN_B_PIPELINE:
    create_or_update_capstone_pipeline()
    run_pipeline14()
pipeline_link()

# COMMAND ----------

# DBTITLE 1,1.4 What did the pipeline produce?
# MAGIC %sql
# MAGIC SELECT 'bronze (streaming table)' AS layer, count(*) AS rows FROM skillwave.sdp.sdp_bronze_enrollments UNION ALL
# MAGIC SELECT 'silver (streaming table)', count(*)  FROM skillwave.sdp.sdp_silver_enrollments                UNION ALL
# MAGIC SELECT 'gold (materialized view)', sum(enrollments) FROM skillwave.sdp.sdp_gold_daily_revenue;

# COMMAND ----------

# MAGIC %md
# MAGIC With days 1–4 landed: **600 · 592 · 588** — silver **dropped** 4 negative prices and 4 unknown courses (expectations with
# MAGIC `DROP ROW`); gold removed the 4 duplicate events with `DISTINCT`. (The 4 `S9999` rows passed: no rule checks students — a
# MAGIC good example of why you write the rules down.)

# COMMAND ----------

# DBTITLE 1,1.5 Expectation metrics from the event log
# MAGIC %sql
# MAGIC SELECT timestamp,
# MAGIC        details:flow_progress:data_quality:expectations AS expectations
# MAGIC FROM event_log(TABLE(skillwave.sdp.sdp_silver_enrollments))
# MAGIC WHERE event_type = 'flow_progress'
# MAGIC   AND details:flow_progress:data_quality IS NOT NULL
# MAGIC ORDER BY timestamp DESC
# MAGIC LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC Each expectation reports `passed_records` and `failed_records`; `known_coupon` (no `ON VIOLATION`) only **warns**: its
# MAGIC failures are counted but the rows stay. Only the pipeline **owner** can query `event_log(...)` (or publish the event log
# MAGIC to a table in the pipeline settings).
# MAGIC
# MAGIC ## Part 2 · The multi-task job
# MAGIC
# MAGIC ### 🖱️ UI route — build it step by step
# MAGIC First print the notebook paths you'll pick:

# COMMAND ----------

# DBTITLE 1,2.1 Task notebook paths
show_task_paths()

# COMMAND ----------

# MAGIC %md
# MAGIC 1. **Jobs & Pipelines → Create → Job**. Rename it (top left) to exactly **`14 SkillWave Daily Refresh`**.
# MAGIC 2. **Job parameters** (right panel → *Edit parameters*): `catalog` = `skillwave`, `days` = `1`, `max_quarantine` = `5`.
# MAGIC    Job parameters are pushed down to every task (notebook widgets with the same name).
# MAGIC 3. Add the tasks below (**+ Add task**). Type *Notebook*, source *Workspace*, compute **Serverless**:
# MAGIC
# MAGIC | # | Task name | Type | Notebook / setting | Depends on |
# MAGIC |---|---|---|---|---|
# MAGIC | 1 | `land_day` | Notebook | `labs/tasks/land_day` | — |
# MAGIC | 2 | `ingest_bronze` | Notebook | `labs/tasks/ingest_bronze` · **Retries: 1** (min. interval 10 s) | `land_day` |
# MAGIC | 3 | `build_silver` | Notebook | `labs/tasks/build_silver` | `ingest_bronze` |
# MAGIC | 4 | `check_quality` | **If/else condition** | `{{tasks.build_silver.values.quarantined}}` **>** `{{job.parameters.max_quarantine}}` | `build_silver` |
# MAGIC | 5 | `quarantine_alert` | Notebook | `labs/tasks/quarantine_alert` | `check_quality` → **true** |
# MAGIC | 6 | `refresh_pipeline` | **Pipeline** | `14 SkillWave SDP pipeline` | `land_day` |
# MAGIC | 7 | `build_gold` | Notebook | `labs/tasks/build_gold` | `check_quality` → **false** **and** `refresh_pipeline` |
# MAGIC
# MAGIC 4. **Schedules & Triggers → Add trigger → Scheduled**: *Advanced* → cron `0 0 6 * * ?`, time zone **Asia/Riyadh**
# MAGIC    (= every day 06:00). Then **Pause** it (top right: the schedule toggle) — you'll run it manually.
# MAGIC 5. Look at the **graph** — it must look like the picture at the top. Save.
# MAGIC
# MAGIC > 💡 `build_silver` publishes **task values** with `dbutils.jobs.taskValues.set("quarantined", n)`; the If/else task reads
# MAGIC > them with the dynamic value reference `{{tasks.build_silver.values.quarantined}}`. Numeric operators (`>`, `>=`, `<`, `<=`)
# MAGIC > compare numbers; `==` / `!=` compare strings.
# MAGIC
# MAGIC ### 🅱️ Plan B — the same job by code (Jobs API 2.2)

# COMMAND ----------

# DBTITLE 1,2.2 Plan B - create (or replace) the job by code
RUN_PLAN_B_JOB = False               # set True only if the UI route didn't work for you
if RUN_PLAN_B_JOB:
    create_or_replace_job(capstone_job_spec())
job_link()

# COMMAND ----------

# DBTITLE 1,2.3 Inspect your job (built in the UI or by code)
_js = job_summary()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 3 · Run it — three days in the life of the job
# MAGIC
# MAGIC ### Run 1 — a normal day
# MAGIC 🖱️ Click **Run now** in the job (or run the next cell). It lands **day 5**, ingests, builds silver (3 bad rows → quarantine),
# MAGIC `check_quality` = **false** (3 > 5? no), the pipeline refreshes, gold is rebuilt.

# COMMAND ----------

# DBTITLE 1,3.1 Run 1 (default parameters) - takes a few minutes
_run1 = run_job_now()

# COMMAND ----------

# DBTITLE 1,3.2 After run 1
# MAGIC %sql
# MAGIC SELECT (SELECT count(*) FROM silver.enrollments)            AS silver_enrollments,
# MAGIC        (SELECT count(*) FROM silver.enrollments_quarantine) AS quarantined,
# MAGIC        (SELECT count(*) FROM gold.fact_enrollments)         AS gold_fact,
# MAGIC        (SELECT count(*) FROM sdp.sdp_bronze_enrollments)    AS pipeline_bronze;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected: **730 · 15 · 730 · 750**.
# MAGIC
# MAGIC ### Run 2 — the quality gate trips
# MAGIC The data team is nervous today: only 2 bad rows are acceptable. 🖱️ **Run now with different parameters** →
# MAGIC `max_quarantine` = **2**. Day 6 lands with its 3 bad rows → `check_quality` = **true** → `quarantine_alert` runs and
# MAGIC `build_gold` is **Excluded** (not run, not failed — the run still succeeds).

# COMMAND ----------

# DBTITLE 1,3.3 Run 2 - max_quarantine = 2
_run2 = run_job_now(params={"max_quarantine": 2})

# COMMAND ----------

# MAGIC %md
# MAGIC Silver grew to **876**, but gold still shows **730**: the gate protected the dashboards. Open the run in the UI: the
# MAGIC If/else task shows the outcome **true**, `build_gold` is grey (*Excluded*).
# MAGIC
# MAGIC ### Run 3 — catch up
# MAGIC Someone checked the quarantine — fine. 🖱️ **Run now** with the default parameters. No new file lands (all 6 days are
# MAGIC in), `quarantined` = **0** → false → gold is rebuilt with everything.

# COMMAND ----------

# DBTITLE 1,3.4 Run 3 - defaults again
_run3 = run_job_now()

# COMMAND ----------

# DBTITLE 1,3.5 After run 3
# MAGIC %sql
# MAGIC SELECT count(*) AS gold_fact, round(sum(price_paid), 2) AS revenue
# MAGIC FROM gold.fact_enrollments;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected: **876 · 62,671.40** — and the job was **idempotent**: re-running it without new data changed nothing except
# MAGIC what was pending.
# MAGIC
# MAGIC > 🎯 **Time-based vs data-driven triggers.** This job uses a **schedule** (time-based). Alternatives: a **file arrival**
# MAGIC > trigger on `/Volumes/skillwave/landing/raw/enrollments/` (runs when a new file lands — better if files arrive at random
# MAGIC > times), a **table update** trigger (runs when a source table changes), or **continuous** (a new run as soon as one ends).
# MAGIC
# MAGIC ## Part 4 · Monitor
# MAGIC 🖱️ In the job → **Runs** tab: the **matrix view** shows every run × task with colours and durations — spot a task that is
# MAGIC getting slower day after day (performance **trend**) or that fails intermittently. Click a task run to see its output,
# MAGIC the notebook result and the error. **Repair run** re-runs only failed tasks (+ their dependents).

# COMMAND ----------

# DBTITLE 1,4.1 Run history by code
_j = find_job()
if _j:
    _runs = _api("GET", "/api/2.2/jobs/runs/list", query={"job_id": _j["job_id"], "limit": 10}).get("runs", [])
    for r in _runs:
        dur = ((r.get("end_time") or 0) - (r.get("start_time") or 0)) / 1000
        print(f"run {r['run_id']} · {(r.get('state') or {}).get('result_state', '…'):<10} · {dur:6.0f}s · "
              f"trigger {r.get('trigger')}")

# COMMAND ----------

# DBTITLE 1,4.2 System tables - the same history for ALL jobs, as SQL (may be restricted on Free Edition)
try:
    display(spark.sql(f"""
        SELECT run_id, period_start_time, period_end_time, result_state, trigger_type
        FROM system.lakeflow.job_run_timeline
        WHERE job_id = '{_j['job_id'] if _j else -1}'
        ORDER BY period_start_time DESC
        LIMIT 20"""))
except Exception as e:
    print("system.lakeflow tables not available to you:", _first_line(e))

# COMMAND ----------

# DBTITLE 1,4.3 Pipeline health - update history from the event log
# MAGIC %sql
# MAGIC SELECT timestamp, event_type, message
# MAGIC FROM event_log(TABLE(skillwave.sdp.sdp_bronze_enrollments))
# MAGIC WHERE event_type IN ('update_progress', 'create_update')
# MAGIC ORDER BY timestamp DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC | Symptom | Where to look | Typical cause / fix |
# MAGIC |---|---|---|
# MAGIC | A task gets slower every day | job **matrix view**, `system.lakeflow.job_task_run_timeline` | data growth → liquid clustering, incremental processing |
# MAGIC | One task much longer than the rest of its stage | **query profile** / Spark UI: task duration distribution | **data skew** → AQE skew join, salting, better keys |
# MAGIC | "Spill to disk" in the profile | query profile | memory pressure → bigger compute, fewer rows per partition |
# MAGIC | Big shuffle read/write | query profile | wide joins → broadcast the small side, filter earlier |
# MAGIC | Cluster won't start | compute **event log** | quota / instance unavailable / init script / policy → serverless, different instance type |
# MAGIC
# MAGIC ## Part 5 · CI/CD — Git folder, job as code, Declarative Automation Bundles
# MAGIC
# MAGIC ### 5a · 🖱️ Commit your work in the Git folder
# MAGIC This course lives in a **Git folder**. Click the **branch name** at the top of the notebook → the Git dialog:
# MAGIC 1. **Create branch** `capstone/<your-name>` (never work directly on `main`).
# MAGIC 2. See the changed files, write a commit message (*"Capstone: SkillWave lakehouse"*), **Commit & Push**.
# MAGIC 3. On GitHub/Azure DevOps open a **pull request** → review → merge to `main`. Other workspaces **Pull** to get it.
# MAGIC
# MAGIC ### 5b · The job as code
# MAGIC 🖱️ In the job → **⋮ → Edit as YAML / View as code** shows the job definition in YAML. That definition is what a bundle
# MAGIC stores in git. The same, from the API:

# COMMAND ----------

# DBTITLE 1,5.1 Your job definition (what you'd put under version control)
if _j:
    _settings = find_job()["settings"]
    print(json.dumps({k: _settings[k] for k in ("name", "parameters", "schedule", "tasks") if k in _settings},
                     indent=2)[:3000])

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5c · A Declarative Automation Bundle for this project
# MAGIC A **bundle** (formerly *Databricks Asset Bundles*) = your code **+** a `databricks.yml` describing jobs/pipelines **+**
# MAGIC targets (dev, prod) with **variables**. The **Databricks CLI** validates and deploys it — locally or from CI (GitHub Actions).
# MAGIC
# MAGIC ```yaml
# MAGIC # databricks.yml  (bundle root)
# MAGIC bundle:
# MAGIC   name: skillwave_capstone
# MAGIC
# MAGIC variables:
# MAGIC   catalog:
# MAGIC     description: Unity Catalog catalog of the environment
# MAGIC     default: skillwave_dev
# MAGIC   max_quarantine:
# MAGIC     default: "5"
# MAGIC
# MAGIC include:
# MAGIC   - resources/*.yml          # resources/skillwave_job.yml + resources/skillwave_pipeline.yml
# MAGIC
# MAGIC targets:
# MAGIC   dev:
# MAGIC     mode: development        # resources prefixed "[dev <you>]", schedules paused, deployed to your user folder
# MAGIC     default: true
# MAGIC     workspace:
# MAGIC       host: https://<your-workspace>.cloud.databricks.com
# MAGIC   prod:
# MAGIC     mode: production         # deployed once, shared root path, run as a service principal
# MAGIC     variables:
# MAGIC       catalog: skillwave
# MAGIC       max_quarantine: "2"
# MAGIC     workspace:
# MAGIC       host: https://<your-workspace>.cloud.databricks.com
# MAGIC       root_path: /Workspace/Shared/.bundle/${bundle.name}/${bundle.target}
# MAGIC     run_as:
# MAGIC       service_principal_name: <application-id-of-the-sp>
# MAGIC ```
# MAGIC
# MAGIC ```yaml
# MAGIC # resources/skillwave_job.yml  (excerpt)
# MAGIC resources:
# MAGIC   jobs:
# MAGIC     skillwave_daily_refresh:
# MAGIC       name: SkillWave Daily Refresh
# MAGIC       parameters:
# MAGIC         - name: catalog
# MAGIC           default: ${var.catalog}
# MAGIC         - name: max_quarantine
# MAGIC           default: ${var.max_quarantine}
# MAGIC       tasks:
# MAGIC         - task_key: land_day
# MAGIC           notebook_task:
# MAGIC             notebook_path: ../src/tasks/land_day.py
# MAGIC         - task_key: ingest_bronze
# MAGIC           depends_on: [{task_key: land_day}]
# MAGIC           max_retries: 1
# MAGIC           notebook_task:
# MAGIC             notebook_path: ../src/tasks/ingest_bronze.py
# MAGIC         # ... build_silver, check_quality (condition_task), quarantine_alert, refresh_pipeline, build_gold
# MAGIC         - task_key: refresh_pipeline
# MAGIC           depends_on: [{task_key: land_day}]
# MAGIC           pipeline_task:
# MAGIC             pipeline_id: ${resources.pipelines.skillwave_sdp.id}
# MAGIC ```
# MAGIC
# MAGIC | CLI command | What it does |
# MAGIC |---|---|
# MAGIC | `databricks bundle init` | create a bundle from a template |
# MAGIC | `databricks bundle validate -t dev` | check the YAML and resolve variables |
# MAGIC | `databricks bundle deploy -t dev` | upload files + create/update the jobs and pipelines of target **dev** |
# MAGIC | `databricks bundle run -t dev skillwave_daily_refresh` | run a deployed job/pipeline by its **resource key** |
# MAGIC | `databricks bundle deploy -t prod --var="max_quarantine=1"` | override a variable at deploy time |
# MAGIC | `databricks bundle destroy -t dev` | delete everything the bundle deployed to dev |
# MAGIC
# MAGIC > 🎯 Variables are referenced as `${var.name}`; values come from (highest wins) `--var` on the command line → `BUNDLE_VAR_name`
# MAGIC > environment variable → the target's `variables:` → the variable's `default`. A CI pipeline typically runs
# MAGIC > `validate` → `deploy -t staging` → tests → `deploy -t prod` on merge to `main`, authenticated as a **service principal**.
# MAGIC
# MAGIC ## Part 6 · ✅ Checks and clean-up

# COMMAND ----------

# DBTITLE 1,Checks
_ok = []
_pid = None
try:
    _pid = find_pipeline14()
    _ok.append(check("pipeline '14 SkillWave SDP pipeline' exists", _pid is not None))
    _ok.append(check("pipeline tables exist in skillwave.sdp",
                     all(table_exists(f"sdp.{t}") for t in ("sdp_bronze_enrollments", "sdp_silver_enrollments",
                                                             "sdp_gold_daily_revenue"))))
    if table_exists("sdp.sdp_silver_enrollments"):
        _ok.append(check("silver expectations dropped the negative prices",
                         spark.sql(f"SELECT count_if(price_paid < 0) FROM {fq('sdp.sdp_silver_enrollments')}").first()[0] == 0))
except Exception as e:
    print("pipeline not checked:", _first_line(e))
try:
    _js = job_summary(verbose=False)
except Exception as e:
    print("job not checked:", _first_line(e))
    _js = None
if _js:
    _t = _js["tasks"]
    _ok.append(check("job has the 7 tasks", set(_t) == {"land_day", "ingest_bronze", "build_silver", "check_quality",
                                                          "quarantine_alert", "refresh_pipeline", "build_gold"}))
    _ok.append(check("check_quality is an If/else task comparing quarantined with max_quarantine",
                     _t.get("check_quality", {}).get("type") == "if/else"
                     and "quarantined" in json.dumps(_t["check_quality"]["condition"])
                     and "max_quarantine" in json.dumps(_t["check_quality"]["condition"])))
    _ok.append(check("build_gold runs on the false branch and after the pipeline",
                     {("check_quality", "false"), ("refresh_pipeline", None)} <= set(_t.get("build_gold", {}).get("depends_on", []))))
    _ok.append(check("quarantine_alert runs on the true branch",
                     ("check_quality", "true") in _t.get("quarantine_alert", {}).get("depends_on", [])))
    _ok.append(check("ingest_bronze has a retry", _t.get("ingest_bronze", {}).get("retries", 0) >= 1))
    _ok.append(check("refresh_pipeline is a pipeline task", _t.get("refresh_pipeline", {}).get("type") == "pipeline"))
    _ok.append(check("job parameters catalog, days, max_quarantine",
                     {"catalog", "days", "max_quarantine"} <= set(_js["parameters"])))
    _sch = _js.get("schedule") or {}
    _ok.append(check("daily schedule (Asia/Riyadh) is PAUSED",
                     bool(_sch) and _sch.get("pause_status") == "PAUSED"))
else:
    _ok.append(check("job '14 SkillWave Daily Refresh' exists", False))
_ok.append(check("all 6 days processed: silver.enrollments = 876", count("silver.enrollments") == 876))
_ok.append(check("gold caught up: fact_enrollments = silver.enrollments",
                 count("gold.fact_enrollments") == count("silver.enrollments")))
print("\n🏆 Capstone complete! Try 14-L5 (challenge), then the mock exam." if all(_ok) else "\nFix the ❌ items above.")

# COMMAND ----------

# DBTITLE 1,Pause the schedule (if you un-paused it)
try:
    pause_capstone_job()
except Exception as e:
    print("note:", _first_line(e))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🧠 What you practised
# MAGIC | Concept | Where |
# MAGIC |---|---|
# MAGIC | Declarative pipeline: streaming tables, MV, `STREAM read_files`, `${config}`, expectations warn/drop/fail, event log | Part 1 |
# MAGIC | Job: DAG dependencies, job parameters, task values, If/else, retries, pipeline task, Quartz schedule + time zone | Part 2 |
# MAGIC | Run now / with different parameters, *Excluded* branch, idempotent re-runs, triggers time-based vs data-driven | Part 3 |
# MAGIC | Runs matrix, run history, `system.lakeflow`, pipeline event log, symptoms → causes | Part 4 |
# MAGIC | Git folder branch/commit/push/PR, job as YAML, bundle targets & variables, CLI validate/deploy/run/destroy | Part 5 |
# MAGIC
# MAGIC ➡️ Next: **14-L5 Challenge** (course reviews feature) · then the **cheat sheets** and the **mock exam**.
