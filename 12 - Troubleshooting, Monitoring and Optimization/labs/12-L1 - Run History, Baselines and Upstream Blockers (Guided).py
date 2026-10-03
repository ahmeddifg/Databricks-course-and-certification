# Databricks notebook source
# MAGIC %md
# MAGIC # 🧪 Lab 12-L1 · Run History, Baselines and Upstream Blockers (Guided)
# MAGIC **Time:** ~45 min (≈ 15 min of it is waiting for job runs) · **Compute:** Serverless notebook + serverless job tasks ·
# MAGIC **Works on Free Edition**
# MAGIC
# MAGIC You are on call for **“12-L1 ShopWave Nightly”**, a 4-task job:
# MAGIC
# MAGIC ```
# MAGIC extract ──▶ transform_orders ──┐
# MAGIC         └─▶ transform_clicks ──┴──▶ publish
# MAGIC ```
# MAGIC
# MAGIC You will build a run history, then read it like an on-call engineer: find a **regression** against the **baseline**, find
# MAGIC the **upstream blocker** of a failed run, **repair** it, compute the **failure rate**, and set a **duration threshold**.
# MAGIC
# MAGIC | Part | You will… | Exam objective |
# MAGIC |---|---|---|
# MAGIC | 1 | Create the job (as code) and look at its task graph | — |
# MAGIC | 2 | Build a **baseline**: 3 normal runs (queued) | 6.1 |
# MAGIC | 3 | 🖱️ Read the **matrix view** | 6.1 |
# MAGIC | 4 | Make one task slower → find the **regression** (matrix + timeline + baseline report) | 6.1 |
# MAGIC | 5 | Make one task fail → find the **upstream blocker** in the **graph view** | 6.2 |
# MAGIC | 6 | **Repair** the failed run | 6.2 |
# MAGIC | 7 | **Failure rate** and the **Runs** tab | 6.2 |
# MAGIC | 8 | **Duration threshold** (health rule) | 6.1 / 6.2 |
# MAGIC | 9 | (optional) The same with **`system.lakeflow`** tables | 6.1 / 6.2 |
# MAGIC
# MAGIC > 🧠 The task notebook `labs/tasks/work` runs a small real Spark query and then **simulates** the rest of its processing time
# MAGIC > with a pause. That makes the durations predictable, so you can create a slowdown or a failure with a **job parameter**
# MAGIC > (`slow_step`, `slow_factor`, `fail_step`) and practise reading the evidence.

# COMMAND ----------

# MAGIC %run ../../Includes/_setup

# COMMAND ----------

# MAGIC %run ./_12_prepare

# COMMAND ----------

# DBTITLE 1,Lab settings
JOB = JOB_NAMES12["L1"]
print("job name :", JOB)
print("task notebook:", task_path("work"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1 · Create the job
# MAGIC The job is created with the Jobs API (the same JSON you see in the UI under **⋮ → View as code**). All tasks run the same
# MAGIC notebook with different **task parameters**; the three **job parameters** are pushed down to every task.

# COMMAND ----------

# DBTITLE 1,Create (or reset) the job
_job_id = create_or_replace_job(l1_job_spec())
job_link(JOB)

# COMMAND ----------

# MAGIC %md
# MAGIC 🖱️ Open the link → **Tasks** tab: you see the DAG. Click `publish` → *Depends on*: `transform_orders`, `transform_clicks`;
# MAGIC *Run if dependencies*: **All succeeded**. Look at **Job details** (right panel): *Max concurrent runs* = 1 and **Queue** = on.
# MAGIC
# MAGIC ## Part 2 · Build a baseline — three normal runs
# MAGIC A baseline needs several **comparable** runs. Start three runs **at once**: with *max concurrent runs = 1* and queueing on,
# MAGIC runs 2 and 3 wait in the **Queued** state and start one after another (≈ 5–8 min in total).

# COMMAND ----------

# DBTITLE 1,Start 3 runs and wait for the last one
_baseline_ids = [run_job_now(JOB, wait=False) for _ in range(3)]
wait_for_run(_baseline_ids[-1], timeout_min=40)
_rows = run_history(JOB)
display(history_df(_rows))

# COMMAND ----------

# DBTITLE 1,✅ Check Part 2
check("at least 3 successful runs in the history", len([r for r in _rows if r["state"] == "SUCCESS"]) >= 3)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 3 · 🖱️ Read the matrix view
# MAGIC 1. Open the job → **Runs** tab. The top part is the **matrix view**: one **column per run** (newest on the right), a **bar**
# MAGIC    on top whose height is the run **duration**, and one **row per task** with a coloured cell (green = succeeded).
# MAGIC 2. Hover a bar → start time, duration, status. Hover a task cell → that task's duration.
# MAGIC 3. Switch to the **list view** (toggle at the top): one row per run with *Start time*, *Duration*, *Status*, *Triggered by*
# MAGIC    (here *Manual*), *Parameters*.
# MAGIC 4. While runs were queued you could see them as **Queued** — waiting for a slot, not for compute.
# MAGIC
# MAGIC The table above (`history_df`) is the same information through the API: `run_s` = run duration, `<task>_s` = execution
# MAGIC time of each task.
# MAGIC
# MAGIC ## Part 4 · A regression — which task got slower?
# MAGIC Production data grew and `transform_clicks` now takes **5×** longer. Simulate it with the job parameters
# MAGIC `slow_step = transform_clicks`, `slow_factor = 5` (UI: **Run now with different parameters**).

# COMMAND ----------

# DBTITLE 1,Run with a slow transform_clicks
_slow_id = run_job_now(JOB, {"slow_step": "transform_clicks", "slow_factor": 5})
_rows = run_history(JOB)
_report = baseline_report(_rows, current_run_id=_slow_id)

# COMMAND ----------

# MAGIC %md
# MAGIC 🖱️ Now find it in the UI, the way you would without this helper:
# MAGIC 1. **Runs** tab → the newest bar is clearly taller than the baseline bars. In its column, the `transform_clicks` cell is the
# MAGIC    one whose duration jumped.
# MAGIC 2. Click the run → **Timeline** view (toggle next to *Graph* / *List*): task bars on a time axis. `transform_orders` and
# MAGIC    `transform_clicks` run in **parallel**; `publish` can only start when the **longer** one finishes — `transform_clicks` is
# MAGIC    on the **critical path**, so its slowdown is the run's slowdown.
# MAGIC 3. Click `transform_clicks` → *Parameters* show `slow_factor = 5`. In real life: compare the input size (rows), the code
# MAGIC    version (Git commit) and the compute with an earlier run, then open the query profile / Spark UI (12-L2).

# COMMAND ----------

# DBTITLE 1,✅ Check Part 4
_flag = (_report.get("tasks", {}).get("transform_clicks") or {}).get("flag", "")
check("baseline report flags transform_clicks as a regression", "REGRESSION" in _flag)
check("extract and transform_orders stayed close to their baseline (< 2x)",
      all((_report["tasks"].get(k) or {}).get("ratio", 0) < 2 for k in ("extract", "transform_orders")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 5 · A failure — find the upstream blocker
# MAGIC Someone renamed a column upstream and `transform_orders` breaks (`fail_step = transform_orders`).

# COMMAND ----------

# DBTITLE 1,Run with a failing transform_orders
_fail_id = run_job_now(JOB, {"fail_step": "transform_orders"})
_errors = run_errors(_fail_id)

# COMMAND ----------

# MAGIC %md
# MAGIC 🖱️ In the UI:
# MAGIC 1. **Runs** tab → the newest column is **red**; the `transform_orders` cell is red, the `publish` cell is grey.
# MAGIC 2. Click the run → **Graph** view: `extract` ✅, `transform_clicks` ✅, `transform_orders` ❌ **Failed**, `publish` ⏭️
# MAGIC    **Upstream failed**. The run result is **Failed**.
# MAGIC 3. Don't debug `publish` — it never ran. Click **`transform_orders`** (the first red node) → the error
# MAGIC    `[SIMULATED FAILURE] … column order_ts not found` and the notebook output.

# COMMAND ----------

# DBTITLE 1,✅ Check Part 5
_st = {k: v["state"] for k, v in _errors.items()}
check("transform_orders FAILED (the blocker)", _st.get("transform_orders") == "FAILED")
check("publish was not run: UPSTREAM_FAILED (a victim)", _st.get("publish") == "UPSTREAM_FAILED")
check("transform_clicks is not in the error list (it succeeded)", "transform_clicks" not in _st)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 6 · Repair the run
# MAGIC The upstream team fixed the column. A **repair run** re-runs only the **failed and skipped** tasks (and their dependents):
# MAGIC `transform_orders` and `publish`. `extract` and `transform_clicks` keep their successful results. Here the "fix" is to
# MAGIC repair with `fail_step = none`.
# MAGIC
# MAGIC 🖱️ UI way: open the failed run → **Repair run** (top right) → it pre-selects the failed tasks → you can change job
# MAGIC parameters → **Repair run**. The cell below does the same through the API.

# COMMAND ----------

# DBTITLE 1,Repair the failed run
_api("POST", "/api/2.2/jobs/runs/repair", body={"run_id": _fail_id, "rerun_all_failed_tasks": True,
                                               "rerun_dependent_tasks": True, "job_parameters": {"fail_step": "none"}})
_det = wait_for_run(_fail_id, timeout_min=20)
print("run state after repair:", (_det.get("state") or {}).get("result_state"))

# COMMAND ----------

# MAGIC %md
# MAGIC 🖱️ Open the run again: a **repair** selector appears at the top (*Original run*, *Repair 1*); the matrix view shows the run's
# MAGIC latest state. Only two tasks have a second attempt.

# COMMAND ----------

# DBTITLE 1,✅ Check Part 6
_det = _api("GET", "/api/2.2/jobs/runs/get", query={"run_id": _fail_id, "include_history": "true"})
check("the repaired run succeeded", (_det.get("state") or {}).get("result_state") == "SUCCESS")
check("the run has a repair in its history",
      len([h for h in _det.get("repair_history") or [] if h.get("type") == "REPAIR"]) >= 1)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 7 · Failure rate and the Runs tab

# COMMAND ----------

# DBTITLE 1,Failure rate of this job
_rows = run_history(JOB)
display(history_df(_rows))
_failed = [r for r in _rows if r["state"] != "SUCCESS"]
print(f"failure rate: {len(_failed)}/{len(_rows)} finished runs = {100 * len(_failed) / max(len(_rows), 1):.0f} %")

# COMMAND ----------

# MAGIC %md
# MAGIC > 🤔 The repaired run now shows **SUCCESS** — its original failure is no longer counted here. A failure-rate metric must
# MAGIC > decide: count *runs that needed manual repair* as failures (system tables keep the original rows per attempt) or not.
# MAGIC
# MAGIC 🖱️ **Jobs & Pipelines → Runs** (tab at the top of the jobs list):
# MAGIC 1. The **finished runs count graph** shows succeeded / failed / skipped runs per day for **all** your jobs.
# MAGIC 2. Filter **Status = Failed** — the failed attempt of today appears. Filter by **job** = `12-L1 ShopWave Nightly`.
# MAGIC 3. Look for the **error code** filter and the **top error types** — the starting point when many jobs fail at once.
# MAGIC
# MAGIC ## Part 8 · Warn early: a duration threshold
# MAGIC Set a **duration warning** at 1.5 × the baseline median: runs that are too slow are flagged (and can notify you) but keep
# MAGIC running — unlike a **timeout**, which stops the run.

# COMMAND ----------

# DBTITLE 1,Set the duration threshold from the baseline
_base = [r["duration_s"] for r in _rows if r["run_id"] in _baseline_ids and r["state"] == "SUCCESS"]
_threshold = int(1.5 * _median(_base or [120]))
set_duration_threshold(JOB, _threshold)
_s = find_job(JOB)["settings"]
check("health rule RUN_DURATION_SECONDS is set",
      any(r.get("metric") == "RUN_DURATION_SECONDS" for r in (_s.get("health") or {}).get("rules") or []))

# COMMAND ----------

# MAGIC %md
# MAGIC 🖱️ Job page → **Job details** panel → **Duration threshold**: the *Warning* value is set. In **Job notifications** you could
# MAGIC add yourself for **Duration warning**. (Optional: run the slow version again — the run shows a duration-warning marker
# MAGIC once it passes the threshold.)

# COMMAND ----------

# DBTITLE 1,(optional) Trigger a slow run to see the warning
# run_job_now(JOB, {"slow_step": "transform_clicks", "slow_factor": 5})

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 9 · (optional) The same in SQL — system tables
# MAGIC `system.lakeflow` is updated with a delay (up to about an hour), and you need access to the `system` catalog (an admin
# MAGIC grants it; on Free Edition it usually works). Come back later if it is empty.

# COMMAND ----------

# DBTITLE 1,Task durations of this job from system.lakeflow
_sql = f"""
SELECT run_id, task_key, result_state, termination_code,
       setup_duration_seconds, execution_duration_seconds, period_start_time
FROM system.lakeflow.job_task_run_timeline
WHERE job_id = '{_job_id}' AND result_state IS NOT NULL
ORDER BY period_start_time DESC, task_key"""
try:
    display(spark.sql(_sql))
except Exception as e:
    print("system.lakeflow not available:", _first_line(e))

# COMMAND ----------

# DBTITLE 1,Baseline per task (median of successful executions) and failure rate
_sql = f"""
SELECT task_key,
       count(*)                                                        AS executions,
       count_if(result_state <> 'SUCCEEDED')                           AS not_succeeded,
       percentile(execution_duration_seconds, 0.5) FILTER (WHERE result_state = 'SUCCEEDED') AS median_exec_s,
       max(execution_duration_seconds)                                 AS max_exec_s
FROM system.lakeflow.job_task_run_timeline
WHERE job_id = '{_job_id}' AND result_state IS NOT NULL
GROUP BY task_key ORDER BY task_key"""
try:
    display(spark.sql(_sql))
except Exception as e:
    print("system.lakeflow not available:", _first_line(e))

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Summary
# MAGIC | You saw | Where |
# MAGIC |---|---|
# MAGIC | The run-time **trend** and a **regression** in one task | matrix view bars + cells, timeline view, baseline = median of successful runs |
# MAGIC | A failed run's **blocker** vs its **victims** | graph view: first *Failed* task vs *Upstream failed* tasks |
# MAGIC | **Repair** = re-run failed + dependent tasks only | run page → Repair run (repair history) |
# MAGIC | **Failure rate**, error codes | Runs tab (finished-runs graph, filters) · `system.lakeflow.*_timeline` |
# MAGIC | **Duration threshold** (warning, run continues) vs **timeout** (run stopped) | Job details → Duration threshold · health rules |
# MAGIC
# MAGIC **Clean-up** (optional — the job has no schedule, it costs nothing while idle):

# COMMAND ----------

# DBTITLE 1,Clean-up (optional)
# delete_lab12_jobs()
