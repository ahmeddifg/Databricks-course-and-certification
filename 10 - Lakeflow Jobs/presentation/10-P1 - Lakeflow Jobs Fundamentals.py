# Databricks notebook source
# MAGIC %md
# MAGIC # ⚙️ 10-P1 · Lakeflow Jobs Fundamentals
# MAGIC **Section 10** · exam objective **4.2** — *configure common tasks (notebook, SQL query, dashboard and pipeline tasks) and
# MAGIC their dependencies using Lakeflow Jobs* (+ the building blocks of 4.1, 4.3, 4.4)
# MAGIC
# MAGIC | After this presentation you can… |
# MAGIC |---|
# MAGIC | Place **Lakeflow Jobs** next to Lakeflow Connect and Spark Declarative Pipelines |
# MAGIC | Describe a job: **tasks** in a **DAG**, **triggers**, **compute**, **parameters**, **notifications**, **permissions** |
# MAGIC | Choose the right **task type** (notebook, SQL, pipeline, dashboard, Run Job, If/else, For each…) and **compute** |
# MAGIC | Pass information **into** tasks (job & task parameters, dynamic value references) and **between** tasks (task values) |
# MAGIC | Monitor runs (Runs tab, run graph, task output, system tables) and secure a job (**permissions**, **run as**) |
# MAGIC
# MAGIC > ▶️ **Run all** to render the diagrams. Section 10 = Domain 4 of the exam (**16 %**).

# COMMAND ----------

# MAGIC %run ../../Includes/_style

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · Where Lakeflow Jobs fit

# COMMAND ----------

# DBTITLE 1,Slide · Lakeflow = Connect + Pipelines + Jobs
show("""
<div class="kicker">Slide 1 · Databricks Lakeflow — one product for data engineering</div>
<h2>Ingest → transform → <u>orchestrate</u></h2>
<div class="flow">
 <div class="step"><b>🔌 Lakeflow Connect</b>managed connectors (SaaS, databases), Auto Loader, COPY INTO<br><span class="muted">Section 05/06</span></div><div class="arrow">➜</div>
 <div class="step"><b>🧱 Spark Declarative Pipelines</b>streaming tables, materialized views, expectations, AUTO CDC<br><span class="muted">Section 08</span></div><div class="arrow">➜</div>
 <div class="step" style="border:2px solid #e8590c"><b>⚙️ Lakeflow Jobs</b>runs <strong>anything</strong> in the right order, at the right time, with retries, alerts and history<br><span class="muted">Section 10</span></div>
</div>
<div class="grid">
 <div class="card"><h3>🧠 What a job does</h3><ul><li><b>sequences</b> work (dependencies)</li><li><b>starts</b> it (schedule, file arrival, table update, continuous, manual/API)</li>
 <li><b>recovers</b> from errors (retries, repair)</li><li><b>tells you</b> (notifications, run history)</li></ul></div>
 <div class="card green"><h3>🧩 What it orchestrates</h3>notebooks · Python scripts & wheels · JARs · <b>SQL</b> (queries, files, alerts) · <b>pipelines</b> ·
 <b>dashboards</b> · Power BI · dbt · other jobs</div>
 <div class="card gray"><h3>🏷️ Names you will see</h3><b>Lakeflow Jobs</b> (2025+) = formerly <b>Databricks Workflows</b> / <b>Databricks Jobs</b>.
 The UI lives under <b>Jobs &amp; Pipelines</b> in the sidebar.</div>
</div>
""" + callout("exam", "The exam uses <b>Lakeflow Jobs</b>. Older questions say <i>Workflows</i>, <i>multi-task job</i> or <i>Delta Live "
              "Tables pipeline task</i> — same concepts: a job with several tasks and dependencies."))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · Anatomy of a job

# COMMAND ----------

# DBTITLE 1,Slide · Job, task, run
show("""
<div class="kicker">Slide 2 · A job is a DAG of tasks plus everything needed to run it unattended</div>
<h2>Job = tasks + dependencies + trigger + compute + parameters + notifications + permissions</h2>
<div class="flow">
 <div class="step"><b>📥 land_orders</b>notebook</div><div class="arrow">➜</div>
 <div class="step"><b>🥉 load_bronze</b>notebook</div><div class="arrow">➜</div>
 <div class="step"><b>🥈🥇 build_silver_gold</b>notebook</div><div class="arrow">➜</div>
 <div class="step"><b>📊 gold_kpis</b>SQL task (file) on a SQL warehouse</div><div class="arrow">➜</div>
 <div class="step"><b>📈 refresh dashboard</b>dashboard task</div>
</div>
<table class="tbl">
<tr><th>Term</th><th>Meaning</th></tr>
<tr><td><b>Job</b></td><td>the definition: tasks, triggers, compute, parameters, notifications, permissions, tags (≤ 1,000 tasks per job)</td></tr>
<tr><td><b>Task</b></td><td>one unit of work (a notebook, a SQL file, a pipeline update…) with its own settings: <b>depends on</b>, <b>run if</b>, retries, timeout, parameters, compute</td></tr>
<tr><td><b>Dependency (DAG)</b></td><td>“run B after A”. Tasks without a dependency between them run <b>in parallel</b>; a task with several dependencies waits for all of them (fan-in)</td></tr>
<tr><td><b>Job run</b> / <b>task run</b></td><td>one execution of the job / of one task, with its own id, state, output, logs and duration</td></tr>
<tr><td><b>Trigger</b></td><td>what starts a run: manual (<i>Run now</i>, API), <b>scheduled</b>, <b>file arrival</b>, <b>table update</b>, <b>continuous</b> (10-P3)</td></tr>
</table>
""" + callout("tip", "Design tasks to be <b>idempotent</b> (safe to run twice): a retry or a repair re-runs the task from the start. In the labs, "
              "<code>load_bronze</code> loads only files it hasn't loaded, and <code>build_silver_gold</code> fully rebuilds its tables."))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · Task types

# COMMAND ----------

# DBTITLE 1,Slide · Task types
show("""
<div class="kicker">Slide 3 · Choose the task type that matches the asset you run</div>
<h2>Task types (Type drop-down when you add a task)</h2>
<table class="tbl">
<tr><th>Task type</th><th>Runs…</th><th>Compute</th><th>Typical use</th></tr>
<tr><td><b>Notebook</b> ⭐</td><td>a notebook (workspace or Git); parameters arrive as <b>widgets</b></td><td>serverless · jobs compute · all-purpose</td><td>PySpark / SQL ETL steps</td></tr>
<tr><td><b>Python script</b> · <b>Python wheel</b> · <b>JAR</b> · <b>Spark Submit</b></td><td>a .py file · a packaged wheel entry point · a Scala/Java JAR</td><td>serverless (script/wheel/JAR) or classic</td><td>software-engineered code</td></tr>
<tr><td><b>SQL</b> ⭐</td><td>a saved <b>query</b>, an <b>alert</b>, or a <b>.sql file</b> (several statements)</td><td><b>SQL warehouse</b> (serverless or pro)</td><td>gold refresh, data checks, alert evaluation</td></tr>
<tr><td><b>Pipeline</b> ⭐</td><td>an update of a Lakeflow Spark Declarative Pipeline (option: full refresh)</td><td>the pipeline's own compute</td><td>ingest → bronze/silver/gold inside SDP</td></tr>
<tr><td><b>Dashboard</b> ⭐</td><td>refreshes a <b>published</b> AI/BI dashboard and can notify its subscribers</td><td>SQL warehouse</td><td>refresh right after the load</td></tr>
<tr><td><b>Run Job</b></td><td>another job (parent → child) with job parameters</td><td>the child's compute</td><td>reusable, modular jobs</td></tr>
<tr><td><b>If/else condition</b> · <b>For each</b></td><td>control flow: branch on a comparison · loop over a list</td><td>none (orchestration only)</td><td>quality gates, per-country processing (10-P2)</td></tr>
<tr><td>dbt · dbt platform · Power BI · Ingestion pipeline · Database table sync · Clean Room notebook · Genie Code (Beta)</td><td colspan="3">specialised types — recognise the names</td></tr>
</table>
""" + callout("exam", "Objective 4.2 names four tasks: <b>notebook</b>, <b>SQL query</b>, <b>dashboard</b> and <b>pipeline</b>. Remember which "
              "compute each needs: SQL/dashboard tasks → <b>SQL warehouse</b>; pipeline task → <b>pipeline compute</b> (you don't pick a cluster on the task)."))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · Compute for job tasks

# COMMAND ----------

# DBTITLE 1,Slide · Compute choices
show("""
<div class="kicker">Slide 4 · Production jobs should not run on interactive clusters</div>
<h2>Serverless · jobs compute · all-purpose · SQL warehouse</h2>
<div class="grid">
 <div class="card green"><h3>☁️ Serverless jobs (default for new notebook/script tasks)</h3><ul>
 <li>no cluster to configure, fast start, auto-scaling, managed by Databricks</li>
 <li><b>Performance optimized</b> (fast start, more DBUs) vs <b>Standard</b> (4–6 min start-up, cheaper)</li>
 <li><b>auto-optimization</b> also retries failed tasks (turn it off for non-idempotent work)</li>
 <li>Python &amp; SQL; libraries via the <b>environment</b></li></ul></div>
 <div class="card"><h3>🖥️ Jobs compute (classic job cluster)</h3><ul>
 <li>created <b>for the run</b>, terminated when it ends</li>
 <li>lower DBU rate than all-purpose · can be <b>shared by several tasks</b> of the job</li>
 <li>you choose runtime, node types, autoscaling, spot, policies</li></ul></div>
 <div class="card red"><h3>🧑‍💻 All-purpose (existing) cluster</h3><ul>
 <li>allowed, but <b>more expensive</b> and shared with interactive users</li>
 <li>✅ only for quick tests</li></ul></div>
 <div class="card orange"><h3>🗄️ SQL warehouse</h3>required by <b>SQL</b> and <b>dashboard</b> tasks (serverless or pro)</div>
</div>
""" + callout("trap", "“Cheapest reliable compute for a nightly notebook job?” → <b>jobs compute</b> or <b>serverless</b>, never an all-purpose cluster. "
              "A new job cluster per run also means a clean environment every time."))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 · Parameters: getting values *into* tasks
# MAGIC
# MAGIC ```text
# MAGIC Job parameters  (job level, key → default)          files = 1 · max_bad_rows = 5 · run_date = {{job.start_time.iso_date}}
# MAGIC       │  pushed down to every task that takes key-value parameters (notebook, SQL, Python…)
# MAGIC       ▼
# MAGIC Task parameters (task level)                        attempt = {{task.execution_count}} · country = {{input}}
# MAGIC       │  arrive in a notebook as WIDGETS
# MAGIC       ▼
# MAGIC dbutils.widgets.get("files")   →  "1"   (always a STRING)
# MAGIC ```
# MAGIC
# MAGIC | Concept | Details |
# MAGIC |---|---|
# MAGIC | **Job parameters** | defined once on the job; **pushed down** to tasks; a run can override them (**Run now with different parameters**, API `job_parameters`) — needs **CAN MANAGE RUN**. Same key on job and task → the **job** parameter wins. |
# MAGIC | **Task parameters** | key-value pairs (or a JSON array for scripts) set on one task |
# MAGIC | **Dynamic value references** | `{{ … }}` placeholders replaced when the run starts: `{{job.id}}`, `{{job.run_id}}`, `{{job.start_time.iso_date}}`, `{{job.trigger.type}}`, `{{job.parameters.files}}`, `{{task.name}}`, `{{task.execution_count}}`, `{{tasks.load_bronze.values.batch_id}}`, `{{tasks.x.result_state}}`, `{{backfill.iso_date}}`, `{{workspace.url}}` |
# MAGIC | **In the notebook** | `dbutils.widgets.text("files", "1")` gives a default for interactive runs; `dbutils.widgets.get("files")` reads the value the job passed |
# MAGIC
# MAGIC > 🕰️ **Legacy references** (still recognised): `{{job_id}}`, `{{run_id}}`, `{{start_date}}`, `{{task_retry_count}}`,
# MAGIC > `{{parent_run_id}}`, `{{task_key}}` → today `{{job.id}}`, `{{task.run_id}}`, `{{job.start_time.iso_date}}`,
# MAGIC > `{{task.execution_count}}`, `{{job.run_id}}`, `{{task.name}}`. References cannot be typed **inside** notebook code — pass
# MAGIC > them as parameters.
# MAGIC
# MAGIC ## 6 · Task values: passing results *between* tasks

# COMMAND ----------

# DBTITLE 1,Slide · Task values
show("""
<div class="kicker">Slide 6 · A task publishes small results; later tasks read them</div>
<h2><code>dbutils.jobs.taskValues</code></h2>
<div class="grid two">
 <div class="card"><h3>📤 In <code>quality_check</code> (upstream)</h3>
 <code>dbutils.jobs.taskValues.set(key="bad_rows", value=3)</code><br>
 <code>dbutils.jobs.taskValues.set(key="countries", value=["Japan","Egypt"])</code>
 <p class="muted">value = anything JSON-serialisable, ≤ 48 KiB as JSON · Python notebooks</p></div>
 <div class="card green"><h3>📥 Downstream</h3>
 in a notebook: <code>dbutils.jobs.taskValues.get(taskKey="quality_check", key="bad_rows", default=0, debugValue=0)</code><br>
 in task settings: <code>{{tasks.quality_check.values.bad_rows}}</code> (parameters, If/else operands, For each inputs)
 <p class="muted"><code>debugValue</code> is returned when you run the notebook interactively (outside a job)</p></div>
</div>
<table class="tbl">
<tr><th>Mechanism</th><th>Direction</th><th>Example</th></tr>
<tr><td>job / task parameters</td><td>into a task</td><td><code>files = 2</code></td></tr>
<tr><td>task values</td><td>task → later tasks</td><td><code>{{tasks.quality_check.values.countries}}</code></td></tr>
<tr><td>SQL task output</td><td>SQL task → later tasks</td><td><code>{{tasks.gold_kpis.output.first_row.revenue}}</code> · <code>…output.alert_state</code></td></tr>
<tr><td><code>dbutils.notebook.exit(value)</code></td><td>notebook → its run output (UI / API)</td><td>a JSON summary of what the task did</td></tr>
<tr><td>tables</td><td>anything bigger</td><td>write a table, read it in the next task</td></tr>
</table>
""" + callout("tip", "Prefer the reference <code>{{tasks.&lt;task&gt;.values.&lt;key&gt;}}</code> in task settings over <code>get()</code> in code — the "
              "dependency on the upstream task is then visible in the job definition."))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7 · Building and running a job (UI)
# MAGIC
# MAGIC | Step | Where |
# MAGIC |---|---|
# MAGIC | Create | sidebar **Jobs & Pipelines** → **Create → Job** (or from a notebook: **Schedule**) |
# MAGIC | Add a task | **Task name**, **Type**, **Source** (Workspace / Git provider), **Path**, **Compute**, **Depends on**, **Parameters** → **Create task** |
# MAGIC | Job settings (right panel) | **Job parameters**, **Schedules & Triggers**, **Compute**, **Notifications**, **Permissions**, **Run as**, **Tags**, **Max concurrent runs**, **Queue**, **Duration thresholds** |
# MAGIC | Run | **Run now** · **Run now with different parameters** (▾) |
# MAGIC | Watch | **Runs** tab: list + **matrix view** (task × run); a run: **graph** and **timeline** (Gantt) views, each task's **output**, **logs**, **query profile / Spark UI**, **task values** |
# MAGIC | As code | **⋮ → View as code** → YAML (bundle), JSON (API) or Python — the bridge to Section 11 |
# MAGIC
# MAGIC ## 8 · Who can do what — job permissions and *run as*

# COMMAND ----------

# DBTITLE 1,Slide · Permissions and identity
show("""
<div class="kicker">Slide 8 · Two different questions: who may touch the job, and whose identity the job runs with</div>
<div class="grid two">
<div>
<table class="tbl">
<tr><th>Job permission</th><th>Adds the ability to…</th></tr>
<tr><td><b>CAN VIEW</b></td><td>see the job, its runs and results</td></tr>
<tr><td><b>CAN MANAGE RUN</b></td><td>run now (also with different parameters), cancel, <b>repair</b> runs</td></tr>
<tr><td><b>IS OWNER</b></td><td>the single owner; by default the job <b>runs as</b> the owner</td></tr>
<tr><td><b>CAN MANAGE</b></td><td>edit settings, change permissions, delete</td></tr>
</table>
</div>
<div class="card orange"><h3>🪪 Run as</h3><ul>
<li>every task uses the data permissions of the <b>run-as</b> identity (Unity Catalog grants)</li>
<li>default = the <b>job owner</b>; production best practice = a <b>service principal</b> (survives people leaving)</li>
<li>setting a service principal requires the <b>Service Principal User</b> role on it</li>
<li>table-update / file-arrival triggers check the run-as identity's access to the monitored objects</li></ul></div>
</div>
""" + callout("trap", "A user with <b>CAN MANAGE RUN</b> can trigger the job but cannot edit it — and the run still uses the <b>run-as</b> identity's "
              "data access, not the clicker's."))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9 · Monitoring at scale
# MAGIC
# MAGIC ```sql
# MAGIC -- Failed job runs in the last 7 days (system tables, Unity Catalog)
# MAGIC SELECT job_id, run_id, result_state, period_start_time, period_end_time
# MAGIC FROM system.lakeflow.job_run_timeline
# MAGIC WHERE result_state = 'FAILED' AND period_start_time > current_timestamp() - INTERVAL 7 DAYS
# MAGIC ORDER BY period_start_time DESC;
# MAGIC ```
# MAGIC
# MAGIC | System table (`system.lakeflow.*`) | One row per… |
# MAGIC |---|---|
# MAGIC | `jobs` · `job_tasks` | job definition / task definition (latest + history) |
# MAGIC | `job_run_timeline` · `job_task_run_timeline` | job run / task run (state, result, timing) |
# MAGIC | `pipelines` · `pipeline_update_timeline` | pipeline / pipeline update |
# MAGIC
# MAGIC Plus: **notifications** (email, Slack, Teams, PagerDuty, webhooks), the **Jobs REST API 2.2** (`/api/2.2/jobs/...`), and
# MAGIC cost per job in `system.billing.usage` (`usage_metadata.job_id`).
# MAGIC
# MAGIC ## ✅ Key takeaways
# MAGIC 1. **Lakeflow Jobs** orchestrate any workload: a job = **tasks** in a **DAG** + trigger + compute + parameters +
# MAGIC    notifications + permissions. Independent tasks run in parallel.
# MAGIC 2. Exam task types: **notebook**, **SQL** (query / alert / file — SQL warehouse), **pipeline** (pipeline compute),
# MAGIC    **dashboard** (SQL warehouse); plus Run Job, If/else, For each.
# MAGIC 3. Production compute = **serverless** or **jobs compute**, not all-purpose clusters.
# MAGIC 4. **Job parameters** are pushed down to tasks and can be overridden per run; notebooks read them as **widgets**;
# MAGIC    `{{ }}` **dynamic value references** inject run metadata.
# MAGIC 5. **Task values** (`dbutils.jobs.taskValues.set/get`, `{{tasks.x.values.y}}`) pass small results between tasks.
# MAGIC 6. Permissions **CAN VIEW < CAN MANAGE RUN < CAN MANAGE** (+ IS OWNER); the job runs **as** the owner or a service principal.
# MAGIC
# MAGIC ➡️ Next: **10-P2 · Control Flow, Retries and Repair**
