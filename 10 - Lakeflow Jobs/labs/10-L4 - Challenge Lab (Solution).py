# Databricks notebook source
# MAGIC %md
# MAGIC # 🏆 Lab 10-L4 · Challenge Lab — The Finance Close Job — ✅ SOLUTION
# MAGIC > Try the challenge yourself first! This notebook builds the whole job **as code** (Jobs API 2.2 settings — the JSON
# MAGIC > behind *View as code*), runs it and waits. Building it in the UI is the actual exercise; compare your job with this spec.
# MAGIC
# MAGIC **Time:** ~60 min · **Compute:** Serverless notebook + serverless jobs + your SQL warehouse · **Story:** Finance wants
# MAGIC the nightly close to be fully automated: load two days of files, stop bad batches, report the top countries in parallel,
# MAGIC refresh the KPIs, notify the partner (with retries) and raise an alert if anything fails. It must be scheduled at 02:30
# MAGIC Riyadh time — but stay **paused** until Finance signs off.
# MAGIC
# MAGIC Build the job in the **UI** (no step list this time — 10-L1 to 10-L3 have them all), or — if you prefer — as **code** with
# MAGIC `create_or_replace_job(spec)` and the helpers `nb_task()` / `sql_file_task()` from `_10_prepare`. Then run each **✅ Check**.
# MAGIC
# MAGIC | Task | Skill |
# MAGIC |---|---|
# MAGIC | 1 | A 10-task job: notebook, **SQL file**, **If/else**, **For each** tasks, dependencies, **job parameters** |
# MAGIC | 2 | **Retries** + a **Run if** failure handler |
# MAGIC | 3 | A **scheduled** trigger (Quartz cron, time zone) — **paused** |
# MAGIC | 4 | Run it once successfully and verify the data |
# MAGIC | 5–8 | 🧠 Lakeflow Jobs concepts |

# COMMAND ----------

# MAGIC %run ../../Includes/_setup

# COMMAND ----------

# MAGIC %run ./_10_prepare

# COMMAND ----------

# DBTITLE 1,Setup for the challenge (run me - don't edit)
JOB4 = JOB_NAMES10["L4"]
try:
    pause_lab10_triggers()                     # other lab jobs must not react to the files this challenge lands
except Exception as e:
    print("note:", _first_line(e))
reset_lab10_data()
SQL_FILE = write_sql_task_file()
answer_task5 = answer_task6 = answer_task7 = answer_task8 = None


def check(label, condition):
    print(("✅ " if condition else "❌ ") + label)
    return bool(condition)


def _deps(t):
    return sorted(d for d, _ in t.get("depends_on", []))


print("Task notebooks:")
show_task_paths()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 1 · The job `10-L4 Finance Close`
# MAGIC **Job parameters:** `files` = `2` · `max_bad_rows` = `10`. **All notebook tasks on serverless.** Use exactly these task
# MAGIC names:
# MAGIC
# MAGIC | Task | Type | What / settings | Depends on |
# MAGIC |---|---|---|---|
# MAGIC | `land_orders` | Notebook | `labs/tasks/land_orders` | — |
# MAGIC | `load_bronze` | Notebook | `labs/tasks/load_bronze` | `land_orders` |
# MAGIC | `quality_check` | Notebook | `labs/tasks/quality_check` | `load_bronze` |
# MAGIC | `gate` | **If/else** | the batch's `bad_rows` task value **greater than** the job parameter `max_bad_rows` | `quality_check` |
# MAGIC | `quarantine_report` | Notebook | `labs/tasks/quarantine_report` | `gate` — when the batch is **bad** |
# MAGIC | `build_silver_gold` | Notebook | `labs/tasks/build_silver_gold` | `gate` — when the batch is **good** |
# MAGIC | `country_reports` | **For each** | over the `countries` task value of `quality_check`, **3** at a time; nested notebook `labs/tasks/country_report` with parameter `country` = the current element | `build_silver_gold` |
# MAGIC | `gold_kpis` | **SQL** (file) | the SQL file printed above, on your SQL warehouse | `build_silver_gold` |
# MAGIC
# MAGIC (`call_partner_api` and `notify_failure` come in Task 2.)

# COMMAND ----------

# DBTITLE 1,Tasks 1-3 · SOLUTION - the job as code
spec4 = {
    "name": JOB4,
    "tags": {"section": "10", "team": "finance"},
    "max_concurrent_runs": 1,
    "queue": {"enabled": True},
    "parameters": [{"name": "files", "default": "2"}, {"name": "max_bad_rows", "default": "10"}],
    "schedule": {"quartz_cron_expression": "0 30 2 * * ?", "timezone_id": "Asia/Riyadh",
                 "pause_status": "PAUSED"},                                   # Task 3
    "tasks": [
        nb_task("land_orders"),
        nb_task("load_bronze", depends=["land_orders"]),
        nb_task("quality_check", depends=["load_bronze"]),
        {"task_key": "gate", "depends_on": [{"task_key": "quality_check"}],    # If/else condition task
         "condition_task": {"op": "GREATER_THAN",
                            "left": "{{tasks.quality_check.values.bad_rows}}",
                            "right": "{{job.parameters.max_bad_rows}}"}},
        nb_task("quarantine_report", depends=[("gate", "true")]),
        nb_task("build_silver_gold", depends=[("gate", "false")]),
        {"task_key": "country_reports", "depends_on": [{"task_key": "build_silver_gold"}],
         "for_each_task": {"inputs": "{{tasks.quality_check.values.countries}}", "concurrency": 3,
                           "task": nb_task("country_report_iteration", "country_report",
                                           params={"country": "{{input}}"})}},
        sql_file_task("gold_kpis", depends=["build_silver_gold"]),
        # Task 2
        nb_task("call_partner_api", depends=["gold_kpis"],
                params={"attempt": "{{task.execution_count}}", "fail_attempts": "1"},
                max_retries=2, min_retry_interval_millis=15000),
        nb_task("notify_failure", depends=["call_partner_api", "country_reports"], run_if="AT_LEAST_ONE_FAILED",
                params={"reason": "partner = {{tasks.call_partner_api.result_state}}"}),
    ],
}
create_or_replace_job(spec4)
job_summary(JOB4)
job_link(JOB4)

# COMMAND ----------

# DBTITLE 1,✅ Check Task 1
_s = job_summary(JOB4, verbose=False)
check(f"job '{JOB4}' exists", _s is not None)
_s = _s or {"tasks": {}, "parameters": {}}
_t = _s["tasks"]
check("job parameters files = 2 and max_bad_rows = 10",
      str(_s["parameters"].get("files")) == "2" and str(_s["parameters"].get("max_bad_rows")) == "10")
check("land_orders → load_bronze → quality_check (notebook tasks, serverless)",
      all(_t.get(k, {}).get("type") == "notebook" and _t[k].get("serverless") for k in ("land_orders", "load_bronze", "quality_check"))
      and _deps(_t.get("load_bronze", {})) == ["land_orders"] and _deps(_t.get("quality_check", {})) == ["load_bronze"])
_g = _t.get("gate", {})
check("gate: If/else, bad_rows task value > job parameter max_bad_rows",
      _g.get("type") == "if/else" and _deps(_g) == ["quality_check"]
      and "tasks.quality_check.values.bad_rows" in str(_g["condition"][0]) and _g["condition"][1] == "GREATER_THAN"
      and "job.parameters.max_bad_rows" in str(_g["condition"][2]))
check("quarantine_report on (true), build_silver_gold on (false)",
      _t.get("quarantine_report", {}).get("depends_on") == [("gate", "true")]
      and _t.get("build_silver_gold", {}).get("depends_on") == [("gate", "false")])
_fe = _t.get("country_reports", {})
check("country_reports: For each over the countries task value, concurrency 3, nested country_report with {{input}}",
      _fe.get("type") == "for each" and _deps(_fe) == ["build_silver_gold"]
      and "tasks.quality_check.values.countries" in str(_fe.get("inputs")) and _fe.get("concurrency") == 3
      and _fe.get("nested", {}).get("path", "").endswith("/labs/tasks/country_report")
      and "{{input}}" in str(_fe.get("nested", {}).get("params", {}).get("country")))
check("gold_kpis: SQL file task on a warehouse, after build_silver_gold",
      _t.get("gold_kpis", {}).get("type") == "sql (file)" and _deps(_t.get("gold_kpis", {})) == ["build_silver_gold"]
      and _t["gold_kpis"].get("path", "").endswith("10_gold_kpis.sql"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 2 · Resilience
# MAGIC * `call_partner_api` (Notebook `labs/tasks/call_partner_api`) runs after `gold_kpis`. Its first attempt always fails
# MAGIC   (`fail_attempts` = `1`): give it what it needs to know **which attempt** it is, and at least **2 retries**.
# MAGIC * `notify_failure` (Notebook `labs/tasks/notify_failure`) must run **only if** `call_partner_api` **or** `country_reports`
# MAGIC   failed.

# COMMAND ----------

# DBTITLE 1,✅ Check Task 2
_s = job_summary(JOB4, verbose=False) or {"tasks": {}}
_t = _s["tasks"]
_a = _t.get("call_partner_api", {})
check("call_partner_api after gold_kpis, ≥ 2 retries, attempt = {{task.execution_count}}, fail_attempts = 1",
      _deps(_a) == ["gold_kpis"] and _a.get("max_retries", 0) >= 2
      and "task.execution_count" in str(_a.get("params", {}).get("attempt"))
      and str(_a.get("params", {}).get("fail_attempts")) == "1")
_n = _t.get("notify_failure", {})
check("notify_failure depends on call_partner_api AND country_reports, run if AT_LEAST_ONE_FAILED",
      _deps(_n) == ["call_partner_api", "country_reports"] and _n.get("run_if") == "AT_LEAST_ONE_FAILED")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 3 · The schedule
# MAGIC Every day at **02:30:00**, time zone **Asia/Riyadh**, as a **Quartz cron** expression — and **paused**.

# COMMAND ----------

# DBTITLE 1,✅ Check Task 3
_sch = ((job_summary(JOB4, verbose=False) or {}).get("schedule") or {})
_cron = " ".join((_sch.get("quartz_cron_expression") or "").split())
check("Quartz cron = 02:30:00 every day", _cron in ("0 30 2 * * ?", "0 30 2 ? * *", "0 30 2 ? * * *", "0 30 2 * * ? *"))
check("time zone Asia/Riyadh", _sch.get("timezone_id") == "Asia/Riyadh")
check("paused", _sch.get("pause_status") == "PAUSED")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 4 · Run it once
# MAGIC **Run now** (defaults: `files = 2`, `max_bad_rows = 10`). Before you look: two files have 6 bad rows — which branch do you
# MAGIC expect? Which tasks will be *Excluded*? How many attempts will `call_partner_api` need? Then run the check.

# COMMAND ----------

# DBTITLE 1,Task 4 · SOLUTION - Run now and wait (~5-10 minutes)
_run4 = run_job_now(JOB4)

# COMMAND ----------

# DBTITLE 1,✅ Check Task 4
_r4 = last_run(JOB4, verbose=True)
_tk = (_r4 or {}).get("tasks", {})
check("the run SUCCEEDED", (_r4 or {}).get("state") == "SUCCESS")
check("gate took the FALSE branch (6 bad rows ≤ 10)", _tk.get("gate", {}).get("outcome") == "false")
check("quarantine_report and notify_failure were EXCLUDED",
      _tk.get("quarantine_report", {}).get("state") == "EXCLUDED" and _tk.get("notify_failure", {}).get("state") == "EXCLUDED")
_batch = spark.table("jobs_bronze_orders").agg(F.max("batch_id")).first()[0] or 0
_exp_countries = [r["country"] for r in (
    spark.table("jobs_bronze_orders").where(F.col("batch_id") == _batch).dropDuplicates(["order_id"]).where("quantity > 0")
         .join(spark.table("jobs_customers"), "customer_id").groupBy("country").agg(F.sum("total").alias("rev"))
         .orderBy(F.col("rev").desc(), "country").limit(3).collect())]
_got_countries = sorted(r["country"] for r in spark.table("jobs_country_report").where(F.col("batch_id") == _batch).collect()) \
    if spark.catalog.tableExists("jobs_country_report") else []
check(f"one country report per top country of batch {_batch} {sorted(_exp_countries)}",
      _batch > 0 and _got_countries == sorted(_exp_countries))
_log = spark.table("jobs_task_log").where("task = 'call_partner_api'")
check("call_partner_api failed once and then succeeded",
      _log.where("status = 'FAILED'").count() >= 1 and _log.where("status = 'OK' AND attempt IN ('2', '3')").count() >= 1)
_gold_rev = spark.table("jobs_gold_daily").agg(F.round(F.sum("revenue"), 2)).first()[0] \
    if spark.catalog.tableExists("jobs_gold_daily") else None
check("jobs_gold_kpis matches the gold table", spark.catalog.tableExists("jobs_gold_kpis") and _gold_rev is not None
      and abs((spark.table("jobs_gold_kpis").first()["revenue"] or 0) - _gold_rev) < 0.01)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tasks 5–8 · 🧠 Concepts
# MAGIC Answer with `"A"`, `"B"`, `"C"` or `"D"`.
# MAGIC
# MAGIC **Task 5.** The silver table is written by another team's pipeline; its duration varies between 20 and 90 minutes. Your
# MAGIC gold job must start right after every silver update. Which trigger?
# MAGIC * A. A schedule two hours after the upstream pipeline's schedule
# MAGIC * B. A continuous trigger
# MAGIC * C. A file arrival trigger on the pipeline's storage location
# MAGIC * D. A table update trigger on the silver table
# MAGIC
# MAGIC **Task 6.** Task `release_lock` must run after `load` and `transform` — **whatever** their result. Which *Run if
# MAGIC dependencies* condition?
# MAGIC * A. All succeeded
# MAGIC * B. At least one failed
# MAGIC * C. All done
# MAGIC * D. None failed
# MAGIC
# MAGIC **Task 7.** A nightly 10-task job failed at task 7 because its notebook referenced a wrong table name. You fixed the
# MAGIC notebook. What completes the night's work fastest, without re-running the six successful tasks?
# MAGIC * A. **Repair run** on the failed run
# MAGIC * B. **Run now**
# MAGIC * C. Clone the job and run the clone
# MAGIC * D. Add 3 retries to task 7 and wait for tomorrow's run
# MAGIC
# MAGIC **Task 8.** An If/else condition task compares `{{tasks.check.values.ratio}}` **`==`** `1`. The task value is `1.0`. What
# MAGIC is the outcome?
# MAGIC * A. true — the values are equal numbers
# MAGIC * B. false — `==` compares the operands as strings (`"1.0"` vs `"1"`)
# MAGIC * C. the task fails — task values can't be used in conditions
# MAGIC * D. true only on serverless compute

# COMMAND ----------

# DBTITLE 1,Tasks 5-8
answer_task5 = "D"   # table update trigger: starts right after each update, whatever the upstream duration
answer_task6 = "C"   # All done: runs after the dependencies finish, whatever their result
answer_task7 = "A"   # Repair run: re-runs only the failed task and its dependents, with the fixed notebook
answer_task8 = "B"   # == and != compare strings; > >= < <= compare numbers

# COMMAND ----------

# DBTITLE 1,✅ Check Tasks 5-8
import hashlib
_h = lambda n, v: hashlib.sha256(f"{n}:{str(v).strip().upper()}".encode()).hexdigest()[:10]
_key = {5: "1701cf30f6", 6: "42591f949b", 7: "0d3757a0a5", 8: "1659afaa97"}
for _n, _a in ((5, answer_task5), (6, answer_task6), (7, answer_task7), (8, answer_task8)):
    check(f"Task {_n}", _a is not None and _h(_n, _a) == _key[_n])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🏁 Final score

# COMMAND ----------

# DBTITLE 1,Final score - re-runs every check quietly
_s = job_summary(JOB4, verbose=False) or {"tasks": {}, "parameters": {}}
_t = _s["tasks"]
_sch = _s.get("schedule") or {}
_r4 = last_run(JOB4, verbose=False) or {"tasks": {}}
_score = {
    "Task 1 job structure": _t.get("gate", {}).get("type") == "if/else"
                            and _t.get("country_reports", {}).get("type") == "for each"
                            and _t.get("gold_kpis", {}).get("type") == "sql (file)"
                            and _t.get("build_silver_gold", {}).get("depends_on") == [("gate", "false")],
    "Task 2 retries + run if": _t.get("call_partner_api", {}).get("max_retries", 0) >= 2
                               and _t.get("notify_failure", {}).get("run_if") == "AT_LEAST_ONE_FAILED",
    "Task 3 paused schedule": _sch.get("pause_status") == "PAUSED" and _sch.get("timezone_id") == "Asia/Riyadh",
    "Task 4 successful run": _r4.get("state") == "SUCCESS" and _r4["tasks"].get("gate", {}).get("outcome") == "false",
    "Tasks 5-8": all(a is not None and _h(n, a) == _key[n]
                     for n, a in ((5, answer_task5), (6, answer_task6), (7, answer_task7), (8, answer_task8))),
}
for k, v in _score.items():
    print(("✅ " if v else "❌ ") + k)
print(f"\nScore: {sum(_score.values())}/{len(_score)}" + ("  🏆 Section 10 complete!" if all(_score.values()) else ""))
pause_lab10_triggers()
