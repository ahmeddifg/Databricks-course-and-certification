# Databricks notebook source
# MAGIC %md
# MAGIC # 📈 12-P1 · Monitoring Jobs and Pipeline Health
# MAGIC **Section 12** · exam objectives **6.1** — *identify trends in job performance using the Lakeflow Jobs run history view to
# MAGIC compare current execution times against historical baselines* and **6.2** — *use the Lakeflow Jobs UI to monitor pipeline
# MAGIC health by interpreting job statuses, viewing DAG-based task graphs to spot upstream blockers, and tracking pipeline run times
# MAGIC and failure rates*
# MAGIC
# MAGIC | After this presentation you can… |
# MAGIC |---|
# MAGIC | Navigate the monitoring views: **Jobs & Pipelines** list, a job's **Runs** (matrix / list), a **run** (graph / timeline / list) |
# MAGIC | Read **run** and **task** statuses — including *Upstream failed*, *Excluded*, *Succeeded with failures*, *Timed out* |
# MAGIC | Compare a run with its **historical baseline** and find **which task** got slower — and **why** (queue, setup or execution) |
# MAGIC | Find the **upstream blocker** of a failed run in the task graph, and fix it with a **repair run** |
# MAGIC | Track **run times and failure rates** in the UI and with the **`system.lakeflow`** tables; warn early with **duration thresholds** |
# MAGIC
# MAGIC > ▶️ **Run all** to render the diagrams. Practice: **labs/12-L1** (a real job, its run history and a regression to find).

# COMMAND ----------

# MAGIC %run ../../Includes/_style

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · The monitoring map

# COMMAND ----------

# DBTITLE 1,Slide · From the list to the line of code
show("""
<div class="kicker">Slide 1 · Zoom in step by step: all jobs → one job → one run → one task → one query</div>
<div class="flow">
 <div class="step"><b>🗂️ Jobs &amp; Pipelines</b>every job &amp; pipeline you can see<br>creator · trigger · <b>last 5 run results</b><br>search by name / ID / tag</div><div class="arrow">➜</div>
 <div class="step" style="border:2px solid #1c7ed6"><b>📊 Job → Runs tab</b><b>matrix view</b>: one column per run<br>bar = run duration · cell = task status<br>(list view: one row per run)</div><div class="arrow">➜</div>
 <div class="step"><b>🧩 Job run page</b><b>graph</b> (DAG) · <b>timeline</b> · <b>list</b><br>parameters, compute, duration</div><div class="arrow">➜</div>
 <div class="step"><b>📄 Task run</b>notebook output, error &amp; stack trace<br>logs, task values, metrics</div><div class="arrow">➜</div>
 <div class="step"><b>🔬 Query profile / Spark UI</b>why a step is slow<br>(12-P2)</div>
</div>
<div class="grid">
 <div class="card"><h3>🏃 Jobs &amp; Pipelines → <b>Runs</b> tab</h3>runs of <b>all</b> jobs (last 60 days): <b>finished-runs count graph</b> (succeeded / failed / skipped), filters by status, job, run-as, time, <b>error code</b>; <b>top error types</b></div>
 <div class="card green"><h3>🗃️ System tables</h3><code>system.lakeflow.jobs</code>, <code>job_tasks</code>, <code>job_run_timeline</code>, <code>job_task_run_timeline</code> — SQL over <b>365 days</b> of runs, all workspaces (lag: up to ~1 h)</div>
 <div class="card orange"><h3>🔔 Push, don't pull</h3>notifications on start / success / failure, <b>duration warning</b> threshold, timeout, streaming-backlog health rules</div>
</div>
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · Reading statuses

# COMMAND ----------

# DBTITLE 1,Slide · Run and task statuses
show("""
<div class="kicker">Slide 2 · The colour of a cell tells you what happened — and where to look next</div>
<table class="tbl">
<tr><th>Status</th><th>Level</th><th>Meaning</th><th>Your next step</th></tr>
<tr><td><span class="pill green">Succeeded</span></td><td>run · task</td><td>finished without error</td><td>compare its <b>duration</b> with the baseline</td></tr>
<tr><td><span class="pill red">Failed</span></td><td>run · task</td><td>a task raised an error (after all retries)</td><td>open the <b>first red task</b> → error + stack trace</td></tr>
<tr><td><span class="pill gray">Upstream failed</span></td><td>task</td><td>not run because a task it <b>depends on</b> failed (run-if not met)</td><td>don't debug it — go <b>upstream</b> to the blocker</td></tr>
<tr><td><span class="pill gray">Excluded</span></td><td>task</td><td>skipped on purpose: the other branch of an <b>If/else</b>, or a run-if like <i>All failed</i> not met</td><td>usually fine — check the condition</td></tr>
<tr><td><span class="pill orange">Succeeded with failures</span></td><td>run</td><td>some tasks failed, but the leaf tasks that decide the result succeeded (e.g. a cleanup with <i>All done</i>)</td><td>look at the failed tasks anyway</td></tr>
<tr><td><span class="pill orange">Timed out</span></td><td>run · task</td><td>exceeded its <b>timeout</b> → stopped</td><td>was it slower than usual (trend) or stuck?</td></tr>
<tr><td><span class="pill gray">Canceled</span> / <span class="pill gray">Skipped</span></td><td>run</td><td>stopped by a user/API, or not started (e.g. max concurrent runs reached and queueing off)</td><td>check concurrency / queue settings</td></tr>
<tr><td><span class="pill">Queued</span> · <span class="pill">Pending</span> · <span class="pill">Running</span></td><td>run · task</td><td>waiting for a slot · waiting for compute · executing</td><td>long <i>Pending</i> = compute start-up (12-P4)</td></tr>
<tr><td><span class="pill gray">Disabled</span></td><td>task</td><td>task switched off in the job definition</td><td>—</td></tr>
</table>
""" + callout("exam", "<b>Upstream failed</b> is a <i>symptom</i>, not a cause. In the task graph the blocker is the <b>first failed task</b> on the path; everything "
              "downstream of it is grey. Fix it, then use <b>Repair run</b>: it re-runs only the failed and skipped tasks (and their dependents) with the current settings."))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · Trends: is this run slower than usual?

# COMMAND ----------

# DBTITLE 1,Slide · The matrix view
_runs = [("R1", 512, "ok"), ("R2", 546, "ok"), ("R3", 574, "ok"), ("R4", 543, "ok"), ("R5", 512, "ok"), ("R6", 431, "fail"),
         ("R7", 525, "ok"), ("R8", 991, "slow-setup"), ("R9", 542, "ok"), ("R10", 902, "regress"), ("R11", 855, "regress"), ("R12", 101, "fail2")]
_bars = "".join(
    f'<div style="display:inline-block;width:52px;text-align:center;vertical-align:bottom">'
    f'<div style="height:{int(d / 8)}px;background:{dict(ok="#2f9e44", fail="#e03131", fail2="#e03131").get(k, "#e8590c" if k == "regress" else "#868e96")};'
    f'border-radius:4px 4px 0 0;margin:0 6px"></div><div class="muted">{r}<br>{d}s</div></div>' for r, d, k in _runs)
show(f"""
<div class="kicker">Slide 3 · Matrix view: the bar chart is the trend, the cells are the tasks</div>
<div style="padding:8px 0;border-bottom:1px solid #d6dde8">{_bars}</div>
<table class="tbl" style="margin-top:6px">
<tr><th>Run</th><th>What the matrix shows</th><th>Diagnosis</th></tr>
<tr><td>R1–R5, R7, R9</td><td>similar bars ≈ 510–575 s</td><td>this is the <b>baseline</b> (median ≈ 540 s)</td></tr>
<tr><td>R6, R12</td><td>red bars · a red task cell + grey <i>Upstream failed</i> cells</td><td>failures — open the red cell</td></tr>
<tr><td>R8</td><td>one tall bar, <b>task cells normal</b></td><td>the time was spent <b>before</b> the tasks ran: <b>queue / compute setup</b> (e.g. capacity) — not a code problem</td></tr>
<tr><td>R10, R11</td><td>bars stay tall from now on · <b>one task cell</b> (transform_clicks) ≈ 2.6× longer</td><td>a <b>regression</b> in that task — data growth, skew, a code or layout change → Spark UI / query profile</td></tr>
</table>
""" + callout("tip", "Compare a run with the <b>median</b> (or p90) of the recent <b>successful</b> runs, not with the average — one outlier like R8 would distort an average. "
              "Then go one level down: <b>which task</b> grew, and did it grow in <b>queue</b>, <b>setup</b> (compute start) or <b>execution</b> time?"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### The baseline checklist
# MAGIC
# MAGIC | Question | Where to look |
# MAGIC |---|---|
# MAGIC | Is the **whole run** slower than its baseline? | Runs tab → matrix bar chart (hover a bar for the duration), list view *Duration* column |
# MAGIC | **Which task** got slower? | matrix cells (hover = task duration) · run page → **Timeline** view (task bars side by side) |
# MAGIC | Did it wait or work? **Queue** vs **setup** vs **execution** | run details (queue duration), task details (*setup* = compute start-up, *execution*), `job_task_run_timeline` (`setup_duration_seconds`, `execution_duration_seconds`) |
# MAGIC | Same **parameters** and **input size**? | run page → *Parameters*; task output / row counts; table history (`DESCRIBE HISTORY` *numOutputRows*) |
# MAGIC | One-off or **trend**? | several consecutive runs above the threshold (R10, R11, …) = trend; one spike = incident |
# MAGIC | What changed? | code (Git commit / bundle deployment, Section 11), data volume or **skew**, table **layout** (small files), compute type or size |
# MAGIC
# MAGIC ## 4 · Health of the DAG: upstream blockers

# COMMAND ----------

# DBTITLE 1,Slide · Graph view
show("""
<div class="kicker">Slide 4 · Run page → Graph view: follow the colours upstream</div>
<div class="flow">
 <div class="step" style="border:2px solid #2f9e44"><b>extract</b>✅ Succeeded · 2 min</div><div class="arrow">➜</div>
 <div style="display:flex;flex-direction:column;gap:6px;flex:1 1 160px">
  <div class="step" style="border:2px solid #e03131"><b>transform_orders</b>❌ Failed · <code>column order_ts not found</code></div>
  <div class="step" style="border:2px solid #2f9e44"><b>transform_clicks</b>✅ Succeeded · 4 min</div>
 </div><div class="arrow">➜</div>
 <div class="step" style="border:2px dashed #868e96"><b>publish</b>⏭️ Upstream failed<br>(run if: All succeeded)</div><div class="arrow">➜</div>
 <div class="step" style="border:2px solid #e8590c"><b>Run result</b>❌ Failed</div>
</div>
<div class="grid">
 <div class="card red"><h3>1 · Find the blocker</h3>the <b>first</b> red node on the path — here <code>transform_orders</code>. Click it → output, error, stack trace, the notebook as it ran.</div>
 <div class="card"><h3>2 · Classify the error</h3>code / schema change? data quality? permission? compute or library (12-P4)? transient (retry would have worked)?</div>
 <div class="card green"><h3>3 · Fix &amp; repair</h3>fix the cause → <b>Repair run</b> re-runs <code>transform_orders</code> and <code>publish</code> only; <code>extract</code> and <code>transform_clicks</code> keep their results</div>
</div>
""" + callout("tip", "The <b>Timeline</b> view of a run shows the task bars on a time axis: overlapping bars = parallel tasks, a long bar on the critical path = what "
              "delays the whole run. For serverless tasks it links to the <b>query profiles</b> of the task's queries."))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 · Failure rates and run times at scale — system tables
# MAGIC
# MAGIC The UI is perfect for one job. For **all** jobs, for **months**, or for a dashboard, query the **`system.lakeflow`** schema
# MAGIC (365 days of history, typically less than one hour behind):
# MAGIC
# MAGIC ```sql
# MAGIC -- Failure rate and run times per job, last 30 days (multi-task jobs: use the task timeline for durations)
# MAGIC WITH runs AS (
# MAGIC   SELECT job_id, run_id,
# MAGIC          max_by(result_state, period_end_time)                                    AS result_state,  -- long runs span several rows
# MAGIC          timestampdiff(SECOND, min(period_start_time), max(period_end_time))     AS run_seconds
# MAGIC   FROM system.lakeflow.job_run_timeline
# MAGIC   WHERE period_start_time >= current_date() - INTERVAL 30 DAYS
# MAGIC   GROUP BY job_id, run_id)
# MAGIC SELECT j.name,
# MAGIC        count(*)                                                        AS runs,
# MAGIC        round(100 * count_if(r.result_state <> 'SUCCEEDED') / count(*), 1) AS failure_rate_pct,
# MAGIC        percentile(r.run_seconds, 0.5)                                  AS p50_seconds,
# MAGIC        percentile(r.run_seconds, 0.9)                                  AS p90_seconds
# MAGIC FROM runs r
# MAGIC JOIN (SELECT job_id, max_by(name, change_time) AS name FROM system.lakeflow.jobs GROUP BY job_id) j USING (job_id)
# MAGIC WHERE r.result_state IS NOT NULL                                     -- finished runs only
# MAGIC GROUP BY j.name
# MAGIC ORDER BY failure_rate_pct DESC
# MAGIC ```
# MAGIC
# MAGIC | Table | Grain | Useful columns |
# MAGIC |---|---|---|
# MAGIC | `system.lakeflow.jobs` · `job_tasks` | one row per **version** of a job / task (SCD2) | `name`, `creator_id`, `run_as`, tags, trigger, `depends_on_keys` |
# MAGIC | `system.lakeflow.job_run_timeline` | job run (runs longer than 1 h → several rows) | `period_start_time`, `period_end_time`, `result_state`, `termination_code`, `trigger_type` |
# MAGIC | `system.lakeflow.job_task_run_timeline` | task run | `task_key`, `result_state`, `termination_code`, `setup_duration_seconds`, `execution_duration_seconds` |
# MAGIC | `system.billing.usage` | DBUs | join on `usage_metadata.job_id` → **cost per run** |
# MAGIC
# MAGIC > ⚠️ `result_state` and `termination_code` are filled on the **last** row of a run (intermediate slices have NULL). Value
# MAGIC > names differ between the Jobs API (`SUCCESS`, `FAILED`, `UPSTREAM_FAILED`) and the system tables (`SUCCEEDED`, `ERROR`,
# MAGIC > `BLOCKED`, `TIMED_OUT`, `CANCELLED`, `SKIPPED`) — check the table docs when you write the filter. For **multi-task** jobs
# MAGIC > the duration columns live in `job_task_run_timeline`.
# MAGIC
# MAGIC ## 6 · Warn before users notice

# COMMAND ----------

# DBTITLE 1,Slide · Proactive monitoring
show("""
<div class="kicker">Slide 6 · Health rules and notifications turn the baseline into an alert</div>
<div class="grid">
 <div class="card orange"><h3>⏰ Duration threshold</h3>Job details → <b>Duration threshold</b>: a <b>warning</b> when a run exceeds the expected time (health rule <code>RUN_DURATION_SECONDS &gt; n</code>) — the run <b>continues</b>. Pair it with a <b>Duration warning</b> notification.</div>
 <div class="card red"><h3>⛔ Timeout</h3>the run (or task) is <b>stopped</b> and marked <i>Timed out</i>. Use it to stop hung runs, not to detect slowness.</div>
 <div class="card"><h3>🔔 Notifications</h3>Start · Success · Failure · Duration warning · <b>Streaming backlog</b> (seconds / bytes / records / files) — e-mail, Slack, Teams, PagerDuty, webhooks; job level or task level</div>
 <div class="card green"><h3>🔁 Retries</h3>task retries absorb <b>transient</b> failures; the task shows its attempts. A task that needs retries every night is a trend too.</div>
</div>
<table class="tbl">
<tr><th>Pipelines (Lakeflow Spark Declarative Pipelines) have their own health view</th></tr>
<tr><td>Pipeline page → <b>graph</b> of flows (rows written, expectations passed / dropped / failed) · <b>update</b> history (Completed / Failed / Canceled) · <b>event log</b>
(<code>event_log(TABLE(t))</code>: <code>flow_progress</code>, <code>data_quality</code>, errors) — and a pipeline <b>task</b> inside a job shows up in the job's matrix like any other task.</td></tr>
</table>
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🕰️ Recognize on the exam
# MAGIC * *Workflows* = old name of **Lakeflow Jobs**; *Delta Live Tables / DLT* = old name of **Lakeflow Spark Declarative Pipelines**.
# MAGIC * Older material talks about the **job runs list** with a *Duration* column only; the **matrix view** is the visual run
# MAGIC   history with a bar per run and a cell per task.
# MAGIC
# MAGIC ## ✅ Key takeaways
# MAGIC 1. **Trend** = compare the current run with the **median of recent successful runs** in the **matrix view**; then find the
# MAGIC    **task** that grew and whether it grew in **queue / setup / execution**.
# MAGIC 2. **Upstream failed** tasks are victims — the **first failed task** upstream in the **graph view** is the blocker; fix it and
# MAGIC    **repair** the run.
# MAGIC 3. Statuses to know: Succeeded, Failed, **Upstream failed**, **Excluded**, **Succeeded with failures**, Timed out, Canceled, Skipped.
# MAGIC 4. **Failure rate** and **p50/p90 run times**: Runs tab (finished-runs graph, error filters) for a quick look,
# MAGIC    **`system.lakeflow.*_timeline`** tables for SQL over months and all jobs.
# MAGIC 5. **Duration threshold** = warning, run continues · **timeout** = run is stopped · notifications push both to you.
