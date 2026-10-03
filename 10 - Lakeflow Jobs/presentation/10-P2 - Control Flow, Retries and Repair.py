# Databricks notebook source
# MAGIC %md
# MAGIC # 🔀 10-P2 · Control Flow, Retries and Repair
# MAGIC **Section 10** · exam objective **4.1** — *implement control flows (retries and conditional tasks such as branching and
# MAGIC looping) using Lakeflow Jobs* (+ dependencies of 4.2)
# MAGIC
# MAGIC | After this presentation you can… |
# MAGIC |---|
# MAGIC | Pick the right **Run if dependencies** condition and predict the state of every task (incl. *Excluded*, *Upstream failed*) |
# MAGIC | Build a **branch** with an **If/else condition** task and a **loop** with a **For each** task |
# MAGIC | Make tasks resilient: **retries**, **timeouts**, **duration thresholds**, **notifications**, **concurrency & queueing** |
# MAGIC | Recover from a failed run with **Repair run** — and know when it is safe |
# MAGIC
# MAGIC > ▶️ **Run all** to render the diagrams.

# COMMAND ----------

# MAGIC %run ../../Includes/_style

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · Run if dependencies

# COMMAND ----------

# DBTITLE 1,Slide · Run if
show("""
<div class="kicker">Slide 1 · Set on the DOWNSTREAM task: when may I run, given how my dependencies ended?</div>
<h2>Six <b>Run if dependencies</b> conditions</h2>
<table class="tbl">
<tr><th>Condition</th><th>Runs when…</th><th>If not met the task is…</th><th>Typical use</th></tr>
<tr><td><b>All succeeded</b> (default)</td><td>every dependency succeeded</td><td><code>Upstream failed</code></td><td>normal ETL chain</td></tr>
<tr><td><b>At least one succeeded</b></td><td>≥ 1 dependency succeeded</td><td><code>Upstream failed</code></td><td>merge results of redundant sources</td></tr>
<tr><td><b>None failed</b></td><td>no dependency failed and ≥ 1 ran (excluded ones are OK)</td><td><code>Upstream failed</code></td><td>join after an If/else (one branch is always excluded)</td></tr>
<tr><td><b>All done</b></td><td>every dependency finished — <b>whatever</b> the result</td><td>—</td><td>cleanup, release locks, audit log</td></tr>
<tr><td><b>At least one failed</b></td><td>≥ 1 dependency failed</td><td><code>Excluded</code> (skipped)</td><td>failure handler / custom alert</td></tr>
<tr><td><b>All failed</b></td><td>every dependency failed</td><td><code>Excluded</code></td><td>last-resort fallback</td></tr>
</table>
<div class="grid">
 <div class="card"><h3>🧮 Evaluation rules</h3><ul><li><b>Excluded</b> upstream tasks count as <b>successful</b></li>
 <li><b>Upstream failed / Upstream canceled</b> count as <b>failed</b></li><li>if all dependencies are excluded, the task is excluded too</li></ul></div>
 <div class="card orange"><h3>🧾 The job's result</h3>a failure-handler task that runs (and succeeds) does <b>not</b> turn a failed run into a
 successful one — the run is still <b>Failed</b> because a task failed.</div>
</div>
""" + callout("exam", "Classic question: <i>“a task must run even if the previous tasks fail, to clean up”</i> → <b>All done</b>. "
              "<i>“send a custom alert only when something failed”</i> → <b>At least one failed</b>."))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · Branching — the *If/else condition* task

# COMMAND ----------

# DBTITLE 1,Slide · If/else
show("""
<div class="kicker">Slide 2 · A task that only evaluates a boolean expression — then the job follows the true or the false branch</div>
<div class="flow">
 <div class="step"><b>quality_check</b>sets task value <code>bad_rows</code></div><div class="arrow">➜</div>
 <div class="step" style="border:2px solid #7048e8"><b>check_quality</b> (If/else)<br><code>{{tasks.quality_check.values.bad_rows}}</code> &gt; <code>{{job.parameters.max_bad_rows}}</code></div><div class="arrow">➜</div>
 <div class="step"><b>true ➜ quarantine_report</b><br><b>false ➜ build_silver_gold</b></div>
</div>
<div class="grid">
 <div class="card"><h3>⚖️ Operands &amp; operators</h3>left / right: constants, <b>job parameters</b>, <b>task parameters</b>, <b>task values</b>, other dynamic references<br>
 operators: <code>==</code> <code>!=</code> <code>&gt;</code> <code>&gt;=</code> <code>&lt;</code> <code>&lt;=</code></div>
 <div class="card red"><h3>⚠️ String vs numeric</h3><code>==</code> and <code>!=</code> compare <b>strings</b> (<code>12.0 == 12</code> → false);
 <code>&gt; &gt;= &lt; &lt;=</code> compare <b>numbers</b> (<code>12.0 &gt;= 12</code> → true). Booleans become <code>"true"</code>/<code>"false"</code>.</div>
 <div class="card green"><h3>🔌 Wiring</h3>downstream task → <b>Depends on</b> <code>check_quality (true)</code> or <code>check_quality (false)</code>.
 The branch that is not taken is <b>Excluded</b>. Join the branches again with <b>None failed</b> / <b>All done</b>.</div>
</div>
""" + callout("legacy", "Before If/else tasks, branching meant code: a notebook calling <code>dbutils.notebook.run('a')</code> or "
              "<code>dbutils.notebook.run('b')</code> in an <code>if</code>. That hides the logic from the job graph — prefer the task."))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · Looping — the *For each* task

# COMMAND ----------

# DBTITLE 1,Slide · For each
show("""
<div class="kicker">Slide 3 · Run ONE nested task once per element of a list</div>
<h2>For each <code>country</code> in <code>{{tasks.quality_check.values.countries}}</code> → <code>country_report</code></h2>
<div class="flow">
 <div class="step"><b>Inputs</b>a JSON array: <code>["Japan","Egypt","Brazil"]</code><br>or a task value / job parameter reference</div><div class="arrow">➜</div>
 <div class="step" style="border:2px solid #0c8599"><b>For each (concurrency 2)</b>iteration 1: Japan · iteration 2: Egypt<br>then iteration 3: Brazil</div><div class="arrow">➜</div>
 <div class="step"><b>Nested task</b>notebook <code>country_report</code><br>parameter <code>country = {{input}}</code></div>
</div>
<table class="tbl">
<tr><th>Setting / rule</th><th>Value</th></tr>
<tr><td>Inputs</td><td>JSON array of strings, numbers, booleans or objects (≤ 5,000 chars typed in; ≤ 48 KB from a task value)</td></tr>
<tr><td>Reference the element</td><td><code>{{input}}</code> · for objects <code>{{input.region}}</code></td></tr>
<tr><td>Concurrency</td><td>default <b>1</b> (one iteration at a time) — raise it to run iterations in parallel</td></tr>
<tr><td>Nested task</td><td>any standard task type — <b>not</b> another For each (no nesting); use a Run Job task inside to go deeper</td></tr>
<tr><td>Result</td><td>the For each task fails if an iteration fails (after its retries); the run page shows each iteration</td></tr>
</table>
""" + callout("tip", "Parallel iterations write to the same tables at the same time → prefer <b>append-only</b> writes (or writes to disjoint "
              "partitions). The lab's <code>country_report</code> appends one row per country, so iterations never conflict."))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · Modular jobs — the *Run Job* task
# MAGIC A **Run Job** task triggers **another job** and waits for it (parent → child), passing **job parameters**. Use it to reuse a
# MAGIC standard ingestion job in several pipelines, to go beyond the 1,000-task limit, or to nest loops (For each → Run Job →
# MAGIC For each). `{{job.trigger.type}}` of the child run is `run_job_task`.
# MAGIC
# MAGIC ## 5 · Retries, timeouts, thresholds, notifications

# COMMAND ----------

# DBTITLE 1,Slide · Resilience settings
show("""
<div class="kicker">Slide 5 · Make transient failures invisible and real failures loud</div>
<div class="grid two">
 <div class="card green"><h3>🔁 Retries (task)</h3><ul>
 <li><b>number of retries</b> + <b>retry interval</b> (measured from the start of the failed attempt)</li>
 <li><b>retry on timeout</b> (optional)</li>
 <li>each retry re-runs the task <b>from the beginning</b> → idempotent tasks only</li>
 <li><code>{{task.execution_count}}</code> = attempt number (1, 2, …)</li>
 <li>serverless jobs: <b>auto-optimization</b> retries failed tasks by default; continuous jobs retry with exponential back-off</li></ul></div>
 <div class="card orange"><h3>⏱️ Duration thresholds (task or job)</h3><ul>
 <li><b>Warning</b> = expected duration → a “duration warning” notification, the run continues</li>
 <li><b>Timeout</b> = maximum duration → the run/task is stopped: <code>Timed out</code></li></ul>
 <h3>📣 Notifications (task or job)</h3>on <b>start</b> · <b>success</b> · <b>failure</b> · <b>duration warning</b> · streaming backlog;
 to email, Slack, Microsoft Teams, PagerDuty, webhooks (<b>notification destinations</b>); options to mute skipped / canceled runs</div>
</div>
<table class="tbl">
<tr><th>Job setting</th><th>Default</th><th>Meaning</th></tr>
<tr><td><b>Maximum concurrent runs</b></td><td>1</td><td>how many runs of THIS job may be active at once</td></tr>
<tr><td><b>Queue</b></td><td>on (UI jobs)</td><td>a run that can't start (concurrency limit) waits in a queue (up to 48 h) instead of being <b>skipped</b></td></tr>
<tr><td><b>Disable a task</b></td><td>—</td><td>keep the task in the definition but skip it at runtime (treated as excluded)</td></tr>
</table>
""" + callout("trap", "Retries are for <b>transient</b> errors (network, throttling, spot loss). A bug or bad data fails on every attempt — retries only "
              "delay the alert. Combine a few retries with a failure notification."))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6 · Repair run

# COMMAND ----------

# DBTITLE 1,Slide · Repair
show("""
<div class="kicker">Slide 6 · Fix the cause, then re-run only what didn't succeed</div>
<div class="flow">
 <div class="step"><b>Run fails</b>publish ❌ · notify_failure ✅ (at least one failed) · cleanup ✅ (all done)</div><div class="arrow">➜</div>
 <div class="step"><b>Fix the cause</b>code, config, data or job settings</div><div class="arrow">➜</div>
 <div class="step" style="border:2px solid #2f9e44"><b>🔧 Repair run</b>re-runs <b>failed / skipped / canceled</b> tasks <b>+ their dependents</b><br>successful tasks are NOT re-run</div><div class="arrow">➜</div>
 <div class="step"><b>Same job run</b>repair history on the run page · <code>{{job.repair_count}}</code></div>
</div>
<div class="grid">
 <div class="card"><h3>✅ Why</h3>faster and cheaper than <b>Run now</b> (which re-runs everything); keeps the original run id and history</div>
 <div class="card orange"><h3>⚙️ Settings</h3>repaired tasks run with the <b>current</b> job/task settings (fixed path, cluster…); you may change job parameters for the repair</div>
 <div class="card red"><h3>⚠️ Careful</h3>a repaired task starts <b>from scratch</b> — if it wrote half its output before failing, non-idempotent code duplicates data.
 Single-task job? Use <b>Run now</b> (repair needs a multi-task job).</div>
</div>
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7 · Troubleshooting a failed run — a routine
# MAGIC
# MAGIC 1. **Runs tab** → the failed run → **graph view**: the red task is the root cause; *Upstream failed* tasks are victims.
# MAGIC 2. Click the red task → **output** (error message, stack trace, `dbutils.notebook.exit` value), **logs**, **query profile**
# MAGIC    (serverless) or **Spark UI** (classic), **task values**.
# MAGIC 3. **Matrix view** (task × run): did it fail before? Since when? Duration trend?
# MAGIC 4. Fix → **Repair run**. If it was transient → add **retries**. If someone must know next time → **notification**.
# MAGIC 5. Fleet view: `system.lakeflow.job_task_run_timeline` (failures per task, durations) — Section 12 goes deeper.
# MAGIC
# MAGIC ## 8 · 🕰️ Legacy you may meet
# MAGIC
# MAGIC | Old | Today |
# MAGIC |---|---|
# MAGIC | `dbutils.notebook.run("child", 600, {"p": "v"})` orchestration inside a notebook (returns the child's `exit` value) | separate **tasks** + dependencies, If/else, For each, **task values** |
# MAGIC | single-task jobs, Jobs API 2.0/2.1 | multi-task jobs, Jobs API **2.2** |
# MAGIC | *Delta Live Tables pipeline* task | **Pipeline** task |
# MAGIC | `{{task_retry_count}}`, `{{run_id}}` | `{{task.execution_count}}`, `{{task.run_id}}` |
# MAGIC
# MAGIC ## ✅ Key takeaways
# MAGIC 1. **Run if dependencies**: All succeeded (default) · At least one succeeded · None failed · All done · At least one failed ·
# MAGIC    All failed. Unmet success conditions → *Upstream failed*; unmet failure conditions → *Excluded*.
# MAGIC 2. **If/else condition**: `==`/`!=` compare strings, `<`/`>`… compare numbers; wire downstream tasks to **(true)** or
# MAGIC    **(false)**; the other branch is *Excluded*.
# MAGIC 3. **For each**: JSON array or reference as **inputs**, `{{input}}` in the nested task, **concurrency** default 1, no nested For each.
# MAGIC 4. **Retries** (+ interval, retry on timeout) for transient errors; **duration warning vs timeout**; notifications on
# MAGIC    start/success/failure/duration; **max concurrent runs** default 1 + **queue**.
# MAGIC 5. **Repair run** re-runs only failed/skipped tasks and their dependents in the same run, with current settings — tasks
# MAGIC    should be idempotent.
# MAGIC
# MAGIC ➡️ Next: **10-P3 · Triggers, Schedules and Orchestration Patterns**
