# Databricks notebook source
# MAGIC %md
# MAGIC # 🧪 Lab 10-L2 · Control Flow, Retries and Repair Runs (Guided)
# MAGIC **Time:** ~75 min · **Compute:** Serverless notebook + serverless job compute · **Works on Free Edition**
# MAGIC
# MAGIC Night after night the load job of 10-L1 is too naïve: it publishes bad batches, it gives up on the first network hiccup and
# MAGIC when something fails everything has to be re-run. You'll turn it into a production-grade job:
# MAGIC
# MAGIC ```
# MAGIC                                                   ┌─(true)──▶ quarantine_report
# MAGIC land_orders ▶ load_bronze ▶ quality_check ▶ check_quality (If/else: bad_rows > max_bad_rows?)
# MAGIC                                                   └─(false)─▶ build_silver_gold ─┬─▶ country_reports (For each country) ─┐
# MAGIC                                                                                  └─▶ call_partner_api (retries) ──────────┴─▶ publish ─┬─▶ notify_failure (at least one failed)
# MAGIC                                                                                                                                        └─▶ cleanup (all done)
# MAGIC ```
# MAGIC
# MAGIC | Part | You will… |
# MAGIC |---|---|
# MAGIC | 1 | Create the starting job **by code** (4 tasks) and read its JSON |
# MAGIC | 2 | 🖱️ **Branch** with an If/else condition task (quality gate) |
# MAGIC | 3 | 🖱️ **Loop** with a For each task over a task value |
# MAGIC | 4 | 🖱️ Run it twice and watch **both branches** |
# MAGIC | 5 | 🖱️ **Retries** for a flaky task |
# MAGIC | 6 | 🖱️ **Run if**: a failure handler and an always-run cleanup — then a run that fails |
# MAGIC | 7 | 🖱️ Fix the cause and **Repair run** |
# MAGIC | 8 | ✅ Automatic checks |

# COMMAND ----------

# MAGIC %run ../../Includes/_setup

# COMMAND ----------

# MAGIC %run ./_10_prepare

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1 · The starting job — created as code
# MAGIC Every job is a JSON document for the **Jobs API** (`POST /api/2.2/jobs/create`). The next cell builds the 4-task chain of
# MAGIC 10-L1 plus two **job parameters** — `files` (how many daily files to land) and `max_bad_rows` (the quality threshold) — and
# MAGIC creates the job **`10-L2 ShopWave Control Flow`**. If the job already exists from an earlier attempt, it is **kept** (set
# MAGIC `REBUILD_BASE = True` to start over).

# COMMAND ----------

# DBTITLE 1,Start clean + create the base job (safe to re-run)
JOB = JOB_NAMES10["L2"]
REBUILD_BASE = False
try:
    pause_lab10_triggers()
except Exception as e:
    print("note:", _first_line(e))
reset_lab10_data()
_spec = l2_base_spec()
print(json.dumps(_spec, indent=2)[:1500], "\n...")
if find_job(JOB) and not REBUILD_BASE:
    print(f"\n'{JOB}' already exists - continuing with YOUR job (REBUILD_BASE = True to reset it)")
else:
    create_or_replace_job(_spec)
job_link(JOB)

# COMMAND ----------

# MAGIC %md
# MAGIC Open the job (link above). You'll see the same graph you built by hand in 10-L1 — **View as code** would show you this JSON
# MAGIC as YAML. From now on you work in the **UI**.
# MAGIC
# MAGIC ## Part 2 · 🖱️ Branch: a quality gate with an *If/else condition* task
# MAGIC `quality_check` publishes the **task value** `bad_rows` (duplicates + cancelled orders + unknown customers in the batch).
# MAGIC Every daily file has **3** bad rows; the threshold `max_bad_rows` is **5**.
# MAGIC
# MAGIC 1. Click the `quality_check` task in the graph → **+ Add task** → choose **If/else condition**.
# MAGIC
# MAGIC | Field | Value |
# MAGIC |---|---|
# MAGIC | **Task name** | `check_quality` |
# MAGIC | **Depends on** | `quality_check` |
# MAGIC | **Condition** | left `{{tasks.quality_check.values.bad_rows}}` · operator **`>`** · right `{{job.parameters.max_bad_rows}}` |
# MAGIC
# MAGIC    (Type `{{` in the field to get suggestions for dynamic values.) → **Create task**.
# MAGIC 2. Click **`build_silver_gold`** → **Depends on**: remove `quality_check`, add **`check_quality (false)`** → **Save task**.
# MAGIC    (Good data = condition false = build.)
# MAGIC 3. Click `check_quality` → **+ Add task** → **Notebook**: name **`quarantine_report`**, path `labs/tasks/quarantine_report`,
# MAGIC    **Depends on** **`check_quality (true)`** → **Create task**.
# MAGIC
# MAGIC > 🧠 `>` compares **numbers** — `==` and `!=` would compare **strings** (`"5.0" == "5"` is false!).

# COMMAND ----------

# DBTITLE 1,✅ Check Part 2
def check(label, condition):
    print(("✅ " if condition else "❌ ") + label)
    return bool(condition)


_s = job_summary(JOB, verbose=False) or {"tasks": {}}
_t = _s["tasks"]
_c = _t.get("check_quality", {})
check("check_quality is an If/else condition task depending on quality_check",
      _c.get("type") == "if/else" and [d for d, _ in _c.get("depends_on", [])] == ["quality_check"])
check("condition: tasks.quality_check.values.bad_rows > job.parameters.max_bad_rows",
      bool(_c) and "tasks.quality_check.values.bad_rows" in str(_c["condition"][0])
      and _c["condition"][1] == "GREATER_THAN" and "job.parameters.max_bad_rows" in str(_c["condition"][2]))
check("build_silver_gold runs on the FALSE branch",
      _t.get("build_silver_gold", {}).get("depends_on") == [("check_quality", "false")])
check("quarantine_report runs on the TRUE branch",
      _t.get("quarantine_report", {}).get("depends_on") == [("check_quality", "true")])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 3 · 🖱️ Loop: a *For each* task over a task value
# MAGIC `quality_check` also publishes **`countries`** — the top 3 countries of the batch by revenue, as a JSON array
# MAGIC (e.g. `["Japan", "Saudi Arabia", "Egypt"]`). One report per country:
# MAGIC
# MAGIC 1. Click `build_silver_gold` → **+ Add task** → choose **For each**.
# MAGIC
# MAGIC | Field | Value |
# MAGIC |---|---|
# MAGIC | **Task name** | `country_reports` |
# MAGIC | **Depends on** | `build_silver_gold` |
# MAGIC | **Inputs** | `{{tasks.quality_check.values.countries}}` |
# MAGIC | **Concurrency** | `2` (two iterations at a time — Free Edition allows 5 concurrent tasks) |
# MAGIC
# MAGIC 2. Inside the For each box, configure the **nested task** (*Add a task to loop over*): **Notebook**, path
# MAGIC    `labs/tasks/country_report`, **Parameters** → **Add**: key **`country`**, value **`{{input}}`** → **Create task**.
# MAGIC
# MAGIC > 💡 You could also type a fixed list into *Inputs*: `["Saudi Arabia", "Egypt", "United Arab Emirates"]`. A task-value
# MAGIC > reference makes the loop **data-driven** — the list changes with every batch.

# COMMAND ----------

# DBTITLE 1,✅ Check Part 3
_s = job_summary(JOB, verbose=False) or {"tasks": {}}
_f = _s["tasks"].get("country_reports", {})
check("country_reports is a For each task after build_silver_gold",
      _f.get("type") == "for each" and [d for d, _ in _f.get("depends_on", [])] == ["build_silver_gold"])
check("inputs = {{tasks.quality_check.values.countries}}", "tasks.quality_check.values.countries" in str(_f.get("inputs")))
check("concurrency 2", _f.get("concurrency") == 2)
_n = _f.get("nested", {})
check("nested task: notebook labs/tasks/country_report with country = {{input}}",
      _n.get("type") == "notebook" and _n.get("path", "").endswith("/labs/tasks/country_report")
      and "{{input}}" in str(_n.get("params", {}).get("country")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 4 · 🖱️ Two runs, two branches
# MAGIC 1. **Run now** (default `files = 1`, `max_bad_rows = 5`). Wait for it, then run the next cell.

# COMMAND ----------

# DBTITLE 1,Run A - expected: condition FALSE (3 bad rows <= 5)
_rA = last_run(JOB)
display(spark.sql("SELECT * FROM jobs_country_report ORDER BY reported_at DESC, country") if
        spark.catalog.tableExists("jobs_country_report") else spark.sql("SELECT 'no country report yet' AS info"))

# COMMAND ----------

# MAGIC %md
# MAGIC Expected: `check_quality` → outcome **false** · `build_silver_gold` ✅ · `quarantine_report` **Excluded** ·
# MAGIC `country_reports` ✅ with **3 iterations** (Japan, Saudi Arabia, Egypt — open the For each task to see each iteration).
# MAGIC
# MAGIC 2. Now a bigger batch: **Run now with different parameters** → `files = 2` → Run. Two files = **6** bad rows.

# COMMAND ----------

# DBTITLE 1,Run B - expected: condition TRUE (6 bad rows > 5)
_rB = last_run(JOB)
display(spark.sql("SELECT batch_id, reason, count(*) AS rows FROM jobs_quarantine GROUP BY ALL ORDER BY ALL")
        if spark.catalog.tableExists("jobs_quarantine") else spark.sql("SELECT 'no quarantine yet' AS info"))

# COMMAND ----------

# MAGIC %md
# MAGIC Expected: outcome **true** · `quarantine_report` ✅ (6 rows: 2 duplicates, 2 cancelled, 2 unknown customers) ·
# MAGIC `build_silver_gold` and everything after it **Excluded**. The run is still **Succeeded** — excluded tasks are not
# MAGIC failures. Batch 2 is now **held back**: `build_silver_gold` skips quarantined batches from now on.
# MAGIC
# MAGIC > 🧠 Look at the run graph: the branch that was not taken is grey (*Excluded*). An excluded task counts as **successful**
# MAGIC > for its downstream *Run if* evaluation.
# MAGIC
# MAGIC ## Part 5 · 🖱️ Retries for a flaky dependency
# MAGIC ShopWave must notify a partner system after each load — and that API often times out the first time.
# MAGIC
# MAGIC 1. Click `build_silver_gold` → **+ Add task** → **Notebook**: name **`call_partner_api`**, path
# MAGIC    `labs/tasks/call_partner_api`, **Depends on** `build_silver_gold`.
# MAGIC 2. **Parameters** → add **`attempt`** = **`{{task.execution_count}}`** and **`fail_attempts`** = **`1`** (the task fails
# MAGIC    while *attempt ≤ fail_attempts*, i.e. on its first attempt).
# MAGIC 3. **Retries** → **Add** → **2** retries, the shortest interval offered (e.g. 15 seconds or 1 minute). Leave *Retry on
# MAGIC    timeout* off. → **Create task**.
# MAGIC
# MAGIC > 💡 `{{task.execution_count}}` counts the executions of this task in the run — retries (and repairs) included — so the
# MAGIC > notebook knows it is attempt 1, 2, 3…
# MAGIC
# MAGIC ## Part 6 · 🖱️ Run if: a failure handler and an always-run cleanup
# MAGIC 1. Add **`publish`** (Notebook `labs/tasks/publish`) — **Depends on** `call_partner_api` **and** `country_reports`
# MAGIC    (click both), Run if: *All succeeded* (default).
# MAGIC 2. Add **`notify_failure`** (Notebook `labs/tasks/notify_failure`) — **Depends on** `publish` — **Run if dependencies:
# MAGIC    At least one failed** — parameter `reason` = `publish = {{tasks.publish.result_state}}`.
# MAGIC 3. Add **`cleanup`** (Notebook `labs/tasks/cleanup`) — **Depends on** `publish` — **Run if dependencies: All done**.
# MAGIC
# MAGIC Now break the publishing configuration on purpose (a typical real-life cause: a feature flag or a missing permission):

# COMMAND ----------

# DBTITLE 1,Break the configuration
set_control("publish_enabled", "false")

# COMMAND ----------

# MAGIC %md
# MAGIC 4. **Run now** (`files = 1`). Expected in the graph:
# MAGIC
# MAGIC | Task | Expected | Why |
# MAGIC |---|---|---|
# MAGIC | `check_quality` | outcome **false** | file 04 has 3 bad rows |
# MAGIC | `call_partner_api` | ✅ after **1 retry** | attempt 1 fails, attempt 2 succeeds |
# MAGIC | `country_reports` | ✅ 3 iterations | Egypt, Brazil, France |
# MAGIC | `publish` | ❌ **Failed** | `publish_enabled = false` |
# MAGIC | `notify_failure` | ✅ | *At least one failed* is true |
# MAGIC | `cleanup` | ✅ | *All done* runs whatever happened |
# MAGIC | the run | ❌ **Failed** | a task failed — the handler doesn't hide that |

# COMMAND ----------

# DBTITLE 1,Run C - the failing run
_rC = last_run(JOB)
display(task_log(12))

# COMMAND ----------

# MAGIC %md
# MAGIC In the log you see `call_partner_api` **FAILED** on attempt 1 and **OK** on attempt 2 — the retry made the transient
# MAGIC error invisible to the rest of the job. Click `call_partner_api` in the run: the task page lists **both attempts**.
# MAGIC
# MAGIC ## Part 7 · 🔧 Fix the cause and repair the run
# MAGIC Re-running everything would land **another** file and redo work that already succeeded. Instead:

# COMMAND ----------

# DBTITLE 1,Fix the configuration
set_control("publish_enabled", "true")

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Open the **failed run** (Runs tab).
# MAGIC 2. Click **Repair run** (top right). The dialog pre-selects the tasks to re-run: the failed task **`publish`** and the tasks
# MAGIC    that depend on it (`notify_failure`, `cleanup`). Successful tasks (`land_orders` … `country_reports`) are **not**
# MAGIC    re-run.
# MAGIC 3. Click **Repair run** in the dialog and wait.

# COMMAND ----------

# DBTITLE 1,After the repair
_rR = last_run(JOB)
display(spark.table("jobs_published") if spark.catalog.tableExists("jobs_published")
        else spark.sql("SELECT 'nothing published yet' AS info"))

# COMMAND ----------

# MAGIC %md
# MAGIC Expected: the **same run** is now **Succeeded**, with **1 repair** in its history · `publish` ✅ (2 days, 238 orders,
# MAGIC revenue 122,131.99 — batch 2 is still held back) · `notify_failure` **Excluded** this time · `cleanup` ✅ again · the inbox
# MAGIC still has only **4** files (`land_orders` didn't run again).
# MAGIC
# MAGIC > ⚠️ A repaired task restarts **from the beginning**. That is safe here because `publish` appends one snapshot only when it
# MAGIC > succeeds — design your tasks the same way (idempotent, or failing *before* writing).
# MAGIC
# MAGIC ## Part 8 · ✅ Automatic checks

# COMMAND ----------

# DBTITLE 1,✅ Checks
_s = job_summary(JOB, verbose=False) or {"tasks": {}}
_t = _s["tasks"]
_api_t = _t.get("call_partner_api", {})
check("call_partner_api: retries ≥ 1 and parameter attempt = {{task.execution_count}}",
      _api_t.get("max_retries", 0) >= 1 and "task.execution_count" in str(_api_t.get("params", {}).get("attempt")))
check("publish depends on call_partner_api AND country_reports",
      sorted(d for d, _ in _t.get("publish", {}).get("depends_on", [])) == ["call_partner_api", "country_reports"])
check("notify_failure: run if AT_LEAST_ONE_FAILED", _t.get("notify_failure", {}).get("run_if") == "AT_LEAST_ONE_FAILED")
check("cleanup: run if ALL_DONE", _t.get("cleanup", {}).get("run_if") == "ALL_DONE")
_runs = [run_summary(r["run_id"], verbose=False) for r in job_runs(JOB, 15)]
_outcomes = {r["tasks"].get("check_quality", {}).get("outcome") for r in _runs}
check("both branches were taken in some run (outcomes true and false)", {"true", "false"} <= _outcomes)
check("a run quarantined a batch", spark.catalog.tableExists("jobs_quarantine")
      and spark.table("jobs_quarantine").count() > 0)
check("the For each task wrote one report per country (≥ 3 rows)", spark.catalog.tableExists("jobs_country_report")
      and spark.table("jobs_country_report").count() >= 3)
_log = spark.table("jobs_task_log").where("task = 'call_partner_api'")
check("call_partner_api failed on attempt 1 and succeeded on attempt 2 (retry)",
      _log.where("status = 'FAILED' AND attempt = '1'").count() >= 1 and _log.where("status = 'OK' AND attempt = '2'").count() >= 1)
_repaired = [r for r in _runs if r["repairs"] >= 1]
check("a run was repaired and is now SUCCESS", any(r["state"] == "SUCCESS" for r in _repaired))
check("in the repaired run, notify_failure ended EXCLUDED and publish SUCCESS",
      any(r["tasks"].get("notify_failure", {}).get("state") == "EXCLUDED"
          and r["tasks"].get("publish", {}).get("state") == "SUCCESS" for r in _repaired))
check("publish_enabled is back to true", get_control("publish_enabled") == "true")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🆘 Plan B — stuck somewhere?
# MAGIC The next cell replaces your job with the finished version of this lab (`l2_full_spec()` in `_10_prepare`) so you can study
# MAGIC a working configuration. Uncomment and run it, then redo Parts 4–7 with it.

# COMMAND ----------

# DBTITLE 1,Plan B (optional)
# create_or_replace_job(l2_full_spec()); job_link(JOB)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ What you practised
# MAGIC * **If/else condition** task: numeric `>` on a **task value** vs a **job parameter**; downstream tasks on the **(true)** /
# MAGIC   **(false)** branch; the other branch is **Excluded**.
# MAGIC * **For each** task: inputs from a **task value**, nested notebook task with `{{input}}`, **concurrency**.
# MAGIC * **Retries** with `{{task.execution_count}}` — transient errors disappear, the run continues.
# MAGIC * **Run if**: *At least one failed* (failure handler) and *All done* (cleanup); a handled failure still fails the run.
# MAGIC * **Repair run**: fix the cause, re-run only failed tasks and their dependents in the **same** run.
# MAGIC
# MAGIC ➡️ Next: **10-L3 · Triggers, Schedules and Pipeline Tasks**
