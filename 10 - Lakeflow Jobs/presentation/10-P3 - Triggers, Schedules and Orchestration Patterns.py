# Databricks notebook source
# MAGIC %md
# MAGIC # ⏰ 10-P3 · Triggers, Schedules and Orchestration Patterns
# MAGIC **Section 10** · exam objectives **4.3** — *implement job schedules with an understanding of trigger types (scheduled, file
# MAGIC arrival, table update)* and **4.4** — *choose between time-based and data-driven triggers based on data availability and
# MAGIC pipeline dependencies* (+ pipeline / SQL / dashboard tasks of **4.2**)
# MAGIC
# MAGIC | After this presentation you can… |
# MAGIC |---|
# MAGIC | Configure every trigger type: **manual**, **scheduled** (simple & Quartz cron), **file arrival**, **table update**, **continuous** |
# MAGIC | Read and write **Quartz cron** expressions and avoid the time-zone / DST traps |
# MAGIC | Decide **time-based vs data-driven** for a scenario — and justify it |
# MAGIC | Orchestrate **pipelines**, **SQL**, **alerts** and **dashboards** in one job; recognise common job patterns |
# MAGIC
# MAGIC > ▶️ **Run all** to render the diagrams.

# COMMAND ----------

# MAGIC %run ../../Includes/_style

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · What starts a run?

# COMMAND ----------

# DBTITLE 1,Slide · Trigger types
show("""
<div class="kicker">Slide 1 · One job, one trigger (+ manual runs are always possible)</div>
<h2>Job details → <b>Schedules &amp; Triggers</b> → Add trigger</h2>
<table class="tbl">
<tr><th>Trigger</th><th>Starts a run when…</th><th>Kind</th><th><code>{{job.trigger.type}}</code></th></tr>
<tr><td><b>Manual</b> (Run now, API, Run Job task)</td><td>someone / something asks</td><td>on demand</td><td><code>one_time</code> · <code>run_job_task</code></td></tr>
<tr><td><b>Scheduled</b></td><td>the clock says so (simple interval or Quartz cron + time zone)</td><td>⏰ time-based</td><td><code>periodic</code></td></tr>
<tr><td><b>File arrival</b></td><td>new files land in a <b>UC volume</b> or <b>external location</b> path</td><td>📥 data-driven</td><td><code>file_arrival</code></td></tr>
<tr><td><b>Table update</b></td><td>one / all of up to 10 <b>UC tables</b> (Delta, MV, streaming table, views) get a new commit</td><td>📥 data-driven</td><td><code>table</code></td></tr>
<tr><td><b>Continuous</b></td><td>always: a new run starts as soon as the previous one ends (exactly one active run)</td><td>♾️ always on</td><td><code>continuous</code></td></tr>
</table>
""" + callout("info", "Every trigger can be <b>paused</b> (the job stays, nothing starts automatically) — the safe way to stop a production job for "
              "maintenance, and what you do at the end of each lab."))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · Scheduled triggers and Quartz cron
# MAGIC
# MAGIC | Option | What you set |
# MAGIC |---|---|
# MAGIC | **Simple** | *every N minutes / hours / days / weeks* — Databricks picks the start time |
# MAGIC | **Advanced** | a **Quartz cron** expression + a **time zone** (UI helper: *every day at 06:00*, *weekdays at 02:30* …) |
# MAGIC
# MAGIC ```text
# MAGIC Quartz cron =  seconds  minutes  hours  day-of-month  month  day-of-week  [year]
# MAGIC                0        30       2      ?             *      MON-FRI          → 02:30:00 on weekdays
# MAGIC                0        0        6      *             *      ?                → every day at 06:00:00
# MAGIC                0        0        */4    *             *      ?                → every 4 hours (00:00, 04:00, 08:00…)
# MAGIC                0        15       1      1             *      ?                → 01:15 on the 1st of every month
# MAGIC ```
# MAGIC
# MAGIC * Quartz has a **seconds** field first and uses **`?`** in *day-of-month* **or** *day-of-week* (“no specific value”). The
# MAGIC   Unix cron `0 6 * * *` (5 fields: minute hour dom month dow) is the same schedule as Quartz `0 0 6 * * ?`.
# MAGIC * Minimum interval between scheduled runs: **10 seconds**; the scheduler is not for low-latency work (runs may start a few
# MAGIC   minutes late).
# MAGIC * **Time zone**: choose UTC for strict hourly schedules — in zones with daylight saving time an hourly run can be skipped
# MAGIC   or delayed at the change.
# MAGIC * A schedule doesn't care whether new data exists — the job runs anyway (design it to do nothing harmlessly).
# MAGIC
# MAGIC ## 3 · File arrival triggers

# COMMAND ----------

# DBTITLE 1,Slide · File arrival
show("""
<div class="kicker">Slide 3 · Run when new files appear — instead of polling with a schedule</div>
<div class="flow">
 <div class="step"><b>📥 New file</b><code>/Volumes/main/shopwave/raw/lab10/inbox/04.json</code></div><div class="arrow">➜</div>
 <div class="step"><b>🔎 Lakeflow checks</b>~ every minute (best effort), recursively incl. sub-folders</div><div class="arrow">➜</div>
 <div class="step"><b>▶️ Run</b>trigger type <code>file_arrival</code> · <code>{{job.trigger.file_arrival.location}}</code></div><div class="arrow">➜</div>
 <div class="step"><b>📚 Task reads the files</b>Auto Loader / COPY INTO / your idempotent loader</div>
</div>
<div class="grid">
 <div class="card"><h3>✅ Requirements</h3><ul><li>Unity Catalog; the path is a <b>volume</b> or an <b>external location</b> (or a sub-path)</li>
 <li>run-as identity can <b>read</b> the location; you have CAN MANAGE on the job</li></ul></div>
 <div class="card orange"><h3>🎛️ Advanced options</h3><ul><li><b>Minimum time between triggers</b> (cool-down): at most one run per interval</li>
 <li><b>Wait after last change</b> (debounce): wait until files stop arriving; each new file resets the timer</li></ul></div>
 <div class="card red"><h3>⚠️ Limits &amp; behaviour</h3><ul><li>only <b>new</b> files trigger — overwriting a file with the same name does not</li>
 <li>without <b>file events</b> on the external location: ≤ 50 file-arrival jobs per workspace, ≤ 10,000 files in the path · enable file events to lift them</li>
 <li>the trigger does not hand the file list to the task — the task must find what is new (checkpoint / idempotent load)</li></ul></div>
</div>
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · Table update triggers

# COMMAND ----------

# DBTITLE 1,Slide · Table update
show("""
<div class="kicker">Slide 4 · Run when upstream TABLES change — the natural trigger between teams and pipelines</div>
<div class="flow">
 <div class="step"><b>Team A's job / pipeline</b>writes <code>jobs_silver_orders</code></div><div class="arrow">➜</div>
 <div class="step"><b>🔔 Table update trigger</b>tables: <code>main.shopwave.jobs_silver_orders</code><br>condition: <b>any</b> / <b>all</b> updated</div><div class="arrow">➜</div>
 <div class="step"><b>Team B's job</b>refreshes KPIs / dashboard</div>
</div>
<table class="tbl">
<tr><th>Topic</th><th>Detail</th></tr>
<tr><td>Monitored objects</td><td>UC managed/external <b>Delta</b> (and Iceberg) tables, <b>materialized views</b>, <b>streaming tables</b>, views &amp; metric views, shared (OpenSharing) and system tables</td></tr>
<tr><td>Several tables</td><td>up to <b>10</b> per trigger; <b>Any updated</b> or <b>All updated</b> (since the last run)</td></tr>
<tr><td>Advanced options</td><td><b>Minimum time between triggers</b> (after the previous run completes) · <b>Wait after last change</b> (each new commit resets it)</td></tr>
<tr><td>Inside the run</td><td><code>{{job.trigger.table_update.updated_tables}}</code> (JSON list), commit timestamps / versions</td></tr>
<tr><td>Permissions</td><td>the job's <b>run-as</b> identity needs <code>SELECT</code> on every monitored table (+ USE CATALOG / USE SCHEMA)</td></tr>
</table>
""" + callout("tip", "Table update triggers decouple producers and consumers: the consumer job doesn't need to know <i>when</i> or <i>by whom</i> "
              "the table is written — no fragile “wait 30 minutes after their schedule” logic."))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 · Continuous jobs
# MAGIC * Exactly **one active run**; when it ends (success or failure) the next run starts immediately.
# MAGIC * Failures are retried with **exponential back-off**; no schedule, no other trigger on the same job.
# MAGIC * Use for always-on **streaming** notebooks (Structured Streaming with the default trigger). For streaming
# MAGIC   *pipelines*, use a **continuous pipeline** (pipeline setting) instead of a job.
# MAGIC * Pause it to stop. 💸 It keeps compute busy 24 × 7.
# MAGIC
# MAGIC ## 6 · Time-based or data-driven? (objective 4.4)

# COMMAND ----------

# DBTITLE 1,Slide · Decision guide
show("""
<div class="kicker">Slide 6 · Ask: <i>what tells me the work can start?</i> — the clock, or the data?</div>
<div class="grid two">
 <div class="card"><h3>⏰ Time-based (schedule)</h3><ul>
 <li>a <b>business deadline</b>: “report at 07:00 every Monday”, month-end close</li>
 <li>sources that are <b>complete at a known time</b> (a daily export at 01:00 that never runs late)</li>
 <li>periodic maintenance: OPTIMIZE / VACUUM, cost reports</li>
 <li>⚠️ runs even when no data arrived; runs on stale data if the upstream is late</li></ul></div>
 <div class="card green"><h3>📥 Data-driven (file arrival / table update)</h3><ul>
 <li>data arrives at <b>unpredictable</b> times (partners drop files, upstream jobs vary 20–90 min)</li>
 <li><b>dependencies across jobs/teams</b>: start when the upstream table is updated</li>
 <li>freshness matters: process as soon as data lands, no idle polling</li>
 <li>⚠️ use debounce / cool-down options when many small files or commits arrive</li></ul></div>
</div>
<table class="tbl">
<tr><th>Scenario</th><th>Best choice</th></tr>
<tr><td>A partner uploads CSV files to a volume at random times; process each delivery quickly</td><td>📥 <b>File arrival</b> (+ wait after last change)</td></tr>
<tr><td>The gold job must start when the silver pipeline (another team) has refreshed its table</td><td>📥 <b>Table update</b> on the silver table</td></tr>
<tr><td>The finance pack must be emailed every weekday at 07:00 Riyadh time</td><td>⏰ <b>Scheduled</b> <code>0 0 7 ? * MON-FRI</code>, time zone Asia/Riyadh</td></tr>
<tr><td>Ingest, transform and refresh a dashboard in strict order</td><td>one job: pipeline task → <b>dashboard task</b> (dependencies), triggered by the data or one schedule</td></tr>
<tr><td>A stream must process events within seconds, 24 × 7</td><td>♾️ <b>Continuous</b> job (or continuous pipeline)</td></tr>
</table>
""" + callout("exam", "Wrong answers love <i>“schedule the downstream job 2 hours after the upstream one”</i>. If the upstream duration varies, the "
              "right answer is a <b>dependency</b> (same job) or a <b>table update trigger</b> (different jobs)."))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7 · Orchestrating pipelines, SQL, alerts and dashboards
# MAGIC
# MAGIC | Task | Settings | Notes |
# MAGIC |---|---|---|
# MAGIC | **Pipeline** | choose the pipeline · *Full refresh* checkbox | runs one **update** of a *triggered* pipeline with the pipeline's own compute/settings; runs from a job count as **production** (retries) — Section 08. The pipeline may also have its own schedule — don't schedule it twice. |
# MAGIC | **SQL → Query** | saved query + SQL warehouse (+ parameters) | e.g. `REFRESH MATERIALIZED VIEW`, gold aggregations |
# MAGIC | **SQL → File** | `.sql` file (workspace or Git) + warehouse | several statements separated by `;` — version-controlled SQL |
# MAGIC | **SQL → Alert** | alert + warehouse | evaluates the alert now; `{{tasks.<task>.output.alert_state}}` can feed an If/else |
# MAGIC | **Dashboard** | published AI/BI dashboard + warehouse (+ subscribers) | refresh right after the data is ready (data-driven freshness) |
# MAGIC
# MAGIC ```text
# MAGIC        ┌──────────────┐    ┌─────────────────────┐    ┌───────────────┐    ┌──────────────────┐
# MAGIC  ⏰/📥 │ land / ingest │ ─▶ │ pipeline task (SDP) │ ─▶ │ SQL task      │ ─▶ │ dashboard task   │
# MAGIC        └──────────────┘    └─────────────────────┘    │ (checks/alert)│    │ (refresh + email)│
# MAGIC                                                        └───────────────┘    └──────────────────┘
# MAGIC ```
# MAGIC
# MAGIC ## 8 · Patterns worth knowing
# MAGIC
# MAGIC | Pattern | Building blocks |
# MAGIC |---|---|
# MAGIC | **Medallion DAG** | ingest → bronze → silver → gold → serve (one task per layer or one pipeline task) |
# MAGIC | **Quality gate** | quality task sets a task value → **If/else** → publish (false) / quarantine + notify (true) |
# MAGIC | **Fan-out / fan-in** | **For each** (or parallel tasks) → a final task with *All succeeded* / *None failed* |
# MAGIC | **Failure handler** | a task with **At least one failed** → custom alert / ticket; **All done** → cleanup |
# MAGIC | **Modular jobs** | parent job with **Run Job** tasks reusing standard child jobs |
# MAGIC | **Backfill** | re-run a scheduled job for past dates (*Run backfill*); tasks read `{{backfill.iso_date}}` / parameters |
# MAGIC | **Chained by data** | job B has a **table update** trigger on job A's output table |
# MAGIC
# MAGIC ## ✅ Key takeaways
# MAGIC 1. Triggers: **manual**, **scheduled** (simple or **Quartz cron** + time zone), **file arrival** (UC volume / external
# MAGIC    location), **table update** (≤ 10 UC tables, any/all), **continuous** (one active run). All can be **paused**.
# MAGIC 2. Quartz cron = `sec min hour day-of-month month day-of-week [year]` with `?`; `0 0 6 * * ?` = daily 06:00.
# MAGIC 3. File arrival / table update have **minimum time between triggers** (cool-down) and **wait after last change** (debounce).
# MAGIC 4. **Time-based** for business deadlines and predictable sources; **data-driven** when arrival times vary or another
# MAGIC    job/team produces the input. Never “guess” upstream durations with offset schedules.
# MAGIC 5. One job can chain **pipeline → SQL (query/file/alert) → dashboard** tasks; SQL and dashboard tasks need a **SQL warehouse**.
# MAGIC
# MAGIC ➡️ Labs: **10-L1** (build a multi-task job) → **10-L2** (control flow & repair) → **10-L3** (triggers & pipeline task) →
# MAGIC **10-L4** (challenge) → **10-Q** (exam questions)
