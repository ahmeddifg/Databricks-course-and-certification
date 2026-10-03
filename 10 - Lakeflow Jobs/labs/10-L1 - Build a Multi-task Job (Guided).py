# Databricks notebook source
# MAGIC %md
# MAGIC # 🧪 Lab 10-L1 · Build a Multi-task Job (Guided)
# MAGIC **Time:** ~60 min · **Compute:** Serverless notebook + serverless job compute + your **SQL warehouse** · **Works on Free Edition**
# MAGIC
# MAGIC ShopWave receives one file of orders per day. Today you turn the manual “run these notebooks in order” routine into a
# MAGIC **Lakeflow Job**: four tasks, dependencies, a **SQL task**, **job parameters**, **task values** — then you run it, read the
# MAGIC run like an engineer on call, and re-run it **with different parameters**.
# MAGIC
# MAGIC ```
# MAGIC land_orders ──▶ load_bronze ──▶ build_silver_gold ──▶ gold_kpis (SQL file on the warehouse)
# MAGIC  (notebook)      (notebook)       (notebook)
# MAGIC ```
# MAGIC
# MAGIC | Part | You will… |
# MAGIC |---|---|
# MAGIC | 1 | Prepare: clean data, generate the SQL file, find the task notebooks |
# MAGIC | 2 | See how a notebook receives **parameters** (widgets) |
# MAGIC | 3 | 🖱️ Create the job and its first **notebook task** |
# MAGIC | 4 | 🖱️ Add **dependent tasks** and a **SQL task** |
# MAGIC | 5 | 🖱️ Add a **job parameter** and run the job |
# MAGIC | 6 | 🖱️ Read the run: graph, timeline, output, **task values** |
# MAGIC | 7 | 🖱️ **Run now with different parameters** |
# MAGIC | 8 | 🖱️ Job settings tour: notifications, thresholds, concurrency, permissions, run as, **View as code** |
# MAGIC | 9 | ✅ Automatic checks |

# COMMAND ----------

# MAGIC %run ../../Includes/_setup

# COMMAND ----------

# MAGIC %run ./_10_prepare

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1 · Prepare
# MAGIC Start clean: empty inbox, empty `jobs_*` tables. This also **pauses** the triggers of any `10-L…` job you built in a
# MAGIC previous attempt (so nothing starts by itself while you work), and writes the **.sql file** used by the SQL task.

# COMMAND ----------

# DBTITLE 1,Start clean (safe to re-run)
JOB = JOB_NAMES10["L1"]
try:
    pause_lab10_triggers()
except Exception as e:
    print("note:", _first_line(e))
reset_lab10_data()
SQL_FILE = write_sql_task_file()
print("\nTask notebooks (you will pick them in the job UI):")
show_task_paths()
try:
    _wh = warehouse()
    print(f"\nSQL warehouse for the SQL task: {_wh['name']} ({_wh.get('state')})")
except Exception as e:
    print("SQL warehouse not found:", _first_line(e))

# COMMAND ----------

# MAGIC %md
# MAGIC > 📂 The task notebooks live in **`labs/tasks/`** of this section. Open `labs/tasks/land_orders` in a new tab: it is a
# MAGIC > normal notebook — a markdown header, a **widgets** cell, two `%run` cells and **one line** that calls a function from
# MAGIC > `_10_prepare`. Small task notebooks with logic in a shared library is a good pattern: the job graph shows *what* runs, the
# MAGIC > library holds *how*.
# MAGIC
# MAGIC ## Part 2 · How a notebook receives parameters
# MAGIC In a job, **job parameters** and **task parameters** arrive in a notebook as **widgets**. The notebook declares each widget
# MAGIC with a **default** (used when you run it by hand) and reads the value with `dbutils.widgets.get()` — **always a string**.

# COMMAND ----------

# DBTITLE 1,Widgets = the parameter interface of a notebook task
dbutils.widgets.text("files", "1", "Files to land")        # default for interactive runs
files = dbutils.widgets.get("files")                        # in a job: the value of the job/task parameter 'files'
print("files =", repr(files), "->", type(files).__name__, "- convert it yourself: int(files) =", int(files))
dbutils.widgets.remove("files")                             # tidy up this lab notebook

# COMMAND ----------

# MAGIC %md
# MAGIC The same notebook can therefore run **interactively** (defaults) and **in a job** (values from the job). Task notebooks
# MAGIC can also **return** a value with `dbutils.notebook.exit(json.dumps(result))` (it appears as the task's *output*) and
# MAGIC **publish task values** for later tasks with `dbutils.jobs.taskValues.set(key, value)`.
# MAGIC
# MAGIC ## Part 3 · 🖱️ Create the job and its first task
# MAGIC 1. Left sidebar → **Jobs & Pipelines** → **Create** → **Job**.
# MAGIC 2. Rename the job (click the title *New Job …* at the top): **`10-L1 ShopWave Daily Load`** — exactly this name (the checks
# MAGIC    look for it).
# MAGIC 3. The first task form opens (if you see task-type tiles, click **Notebook**). Fill it in:
# MAGIC
# MAGIC | Field | Value |
# MAGIC |---|---|
# MAGIC | **Task name** | `land_orders` |
# MAGIC | **Type** | Notebook |
# MAGIC | **Source** | Workspace |
# MAGIC | **Path** | browse to this section → `labs/tasks/land_orders` (path printed in Part 1) |
# MAGIC | **Compute** | **Serverless** (the default; on classic workspaces a *new job cluster* is the production choice) |
# MAGIC
# MAGIC 4. Click **Create task**.
# MAGIC
# MAGIC > 💡 **Source = Git provider** would check out the notebook from a remote repository branch/tag/commit at run time — the
# MAGIC > production pattern when the job is not deployed with bundles (Section 11).
# MAGIC
# MAGIC ## Part 4 · 🖱️ Add dependent tasks and a SQL task
# MAGIC Click **+ Add task** under the graph for each new task:
# MAGIC
# MAGIC | # | Task name | Type | Path / settings | Depends on |
# MAGIC |---|---|---|---|---|
# MAGIC | 2 | `load_bronze` | Notebook | `labs/tasks/load_bronze` · Serverless | `land_orders` |
# MAGIC | 3 | `build_silver_gold` | Notebook | `labs/tasks/build_silver_gold` · Serverless | `load_bronze` |
# MAGIC | 4 | `gold_kpis` | **SQL** | **SQL task: File** · **Source:** Workspace · **Path:** the `.sql` file printed in Part 1 (Workspace → Users → you → `shopwave_jobs/10_gold_kpis.sql`) · **SQL warehouse:** yours | `build_silver_gold` |
# MAGIC
# MAGIC *Depends on* is pre-filled with the task you added from — check it every time. The graph should be a straight line.
# MAGIC
# MAGIC > 🧠 **Why a SQL task?** SQL files/queries run on a **SQL warehouse** (serverless or pro): Photon, result cache, the right
# MAGIC > compute for SQL-only steps. Open the `.sql` file: two statements separated by `;` — a `CREATE OR REPLACE TABLE … AS
# MAGIC > SELECT` and a `SELECT` whose result you will see in the task output.
# MAGIC
# MAGIC ## Part 5 · 🖱️ A job parameter, then the first run
# MAGIC 1. In the **Job details** panel (right) → **Job parameters** → **Edit parameters** → **Add**: key **`files`**, default
# MAGIC    **`1`** → **Save**. Job parameters are **pushed down** to every notebook task — `land_orders` reads it as the widget
# MAGIC    `files`.
# MAGIC 2. Click **Run now** (top right). The run appears in the **Runs** tab; click it to watch the graph turn green.
# MAGIC 3. Expected (about 2–5 minutes): **4 green tasks**. Then run the next cell.

# COMMAND ----------

# DBTITLE 1,What did the run produce?
_r = last_run(JOB)
print()
display(spark.sql("""
    SELECT 'jobs_bronze_orders' AS table_name, count(*) AS rows FROM jobs_bronze_orders
    UNION ALL SELECT 'jobs_silver_orders', count(*) FROM jobs_silver_orders
    UNION ALL SELECT 'jobs_gold_daily', count(*) FROM jobs_gold_daily"""))
if spark.catalog.tableExists("jobs_gold_kpis"):
    display(spark.table("jobs_gold_kpis"))
display(task_log(10))

# COMMAND ----------

# MAGIC %md
# MAGIC Expected after the first run: inbox `01.json` · bronze **121** rows · silver **119** (the duplicate and the cancelled order
# MAGIC are gone) · gold **1** day · `jobs_gold_kpis`: **119** orders, revenue **60,716.61**. `jobs_task_log` shows one line per
# MAGIC task, written **by the job**, not by you.
# MAGIC
# MAGIC ## Part 6 · 🖱️ Read the run like an on-call engineer
# MAGIC Open the run (Runs tab → click the start time):
# MAGIC
# MAGIC | Look at | What you learn |
# MAGIC |---|---|
# MAGIC | **Graph** view | task states and dependencies; click a task to open its run |
# MAGIC | **Timeline** view (Gantt) | which task took the time — start-up vs work |
# MAGIC | `land_orders` → **Output** | the notebook ran top-to-bottom; the last cell's `dbutils.notebook.exit(...)` value is shown as the result |
# MAGIC | `load_bronze` → output / **Task values** | `new_rows = 121`, `batch_id = 1` (set with `dbutils.jobs.taskValues.set`) |
# MAGIC | `gold_kpis` → output | the result of the last `SELECT` of the SQL file, and a link to the query profile |
# MAGIC | Run **parameters** (run details panel) | `files = 1` — the values this run actually used |
# MAGIC
# MAGIC > 🧠 **Task values** are how tasks talk to each other: in 10-L2 an **If/else** task will compare
# MAGIC > `{{tasks.quality_check.values.bad_rows}}` with a threshold, and a **For each** task will loop over
# MAGIC > `{{tasks.quality_check.values.countries}}`.
# MAGIC
# MAGIC ## Part 7 · 🖱️ Run now with different parameters
# MAGIC Production example: after an outage, two days of files must be processed in one run.
# MAGIC
# MAGIC 1. Click the arrow next to **Run now** → **Run now with different parameters** → set **`files` = `2`** → **Run**.
# MAGIC 2. When it is green, run the cell below.

# COMMAND ----------

# DBTITLE 1,After the run with files = 2
_r = last_run(JOB)
print("\nlanded files:", landed10())
display(spark.sql("""
    SELECT (SELECT count(*) FROM jobs_bronze_orders) AS bronze_rows,
           (SELECT count(*) FROM jobs_silver_orders) AS silver_rows,
           (SELECT count(*) FROM jobs_gold_daily)    AS gold_days"""))
if spark.catalog.tableExists("jobs_gold_kpis"):
    display(spark.table("jobs_gold_kpis"))

# COMMAND ----------

# MAGIC %md
# MAGIC Expected: 3 files (`01`–`03`) · bronze **363** · silver **357** · gold **3** days · KPIs **357** orders, revenue
# MAGIC **201,082.14**. `load_bronze` loaded **only the two new files** as batch 2 — re-running a task never loads a file twice
# MAGIC (idempotent design).
# MAGIC
# MAGIC > 🧪 **Try it:** click **Run now** again (default `files = 1`) → day 4 is processed. Then look at the **Runs** tab →
# MAGIC > **matrix view**: one column per run, one row per task — the fastest way to see patterns (a task that always fails, a
# MAGIC > task that gets slower).
# MAGIC
# MAGIC ## Part 8 · 🖱️ Job settings tour
# MAGIC Open the job (not a run) and use the **Job details** panel on the right:
# MAGIC
# MAGIC | Setting | Do this | Why |
# MAGIC |---|---|---|
# MAGIC | **Notifications** | **Add notification** → your e-mail → **Failure** | someone must know when the nightly load fails |
# MAGIC | **Duration thresholds** (job) | Warning **10 min**, Timeout **30 min** | a warning when the run is slower than expected; a hard stop when it hangs |
# MAGIC | **Max concurrent runs** / **Queue** | leave **1** / **on** | a second trigger waits in the queue instead of running in parallel or being skipped |
# MAGIC | **Tags** | **Add tag** `section` = `10` | find and **charge back** jobs (tags reach `system.billing.usage`) |
# MAGIC | **Permissions** | look only | CAN VIEW · CAN MANAGE RUN · CAN MANAGE · IS OWNER |
# MAGIC | **Run as** | look only | you (the owner). Production: a **service principal** |
# MAGIC | **Compute** (serverless) | look at **Performance optimized** | on = fast start-up; off = *standard* mode, cheaper, 4–6 min start-up |
# MAGIC | **⋮ (kebab) → View as code** | look at **YAML** and **JSON** | the whole job as code — what bundles (Section 11) and the Jobs API use |
# MAGIC
# MAGIC Click **Save** where needed, then run the cell — it reads your job back through the **Jobs API**.

# COMMAND ----------

# DBTITLE 1,Your job, as the Jobs API sees it
_s = job_summary(JOB)
if _s:
    print("\nnotifications on failure:", _s["email_on_failure"])
    print("health rules (duration warning):", _s["health_rules"])
    print("timeout_seconds:", _s["timeout_seconds"], "· max concurrent runs:", _s["max_concurrent_runs"],
          "· queue:", _s["queue"], "· tags:", _s["tags"])
    job_link(JOB)

# COMMAND ----------

# MAGIC %md
# MAGIC > 🧩 **Optional — a dashboard task.** If you built the dashboard **`09-L2 ShopWave Sales`** in Section 09, add a fifth task:
# MAGIC > **Type: Dashboard** · dashboard `09-L2 ShopWave Sales` · your SQL warehouse · **Depends on** `gold_kpis`. A dashboard task
# MAGIC > refreshes a *published* dashboard right after the data is ready (and can e-mail its subscribers) — data-driven freshness
# MAGIC > instead of a dashboard schedule at a guessed time. (That dashboard reads the `bi_*` tables, so the numbers won't change —
# MAGIC > the point is the wiring.)
# MAGIC
# MAGIC ## Part 9 · ✅ Automatic checks

# COMMAND ----------

# DBTITLE 1,✅ Checks
def check(label, condition):
    print(("✅ " if condition else "❌ ") + label)
    return bool(condition)


_s = job_summary(JOB, verbose=False)
check(f"job '{JOB}' exists", _s is not None)
if _s:
    _t = _s["tasks"]
    check("tasks land_orders, load_bronze, build_silver_gold, gold_kpis",
          {"land_orders", "load_bronze", "build_silver_gold", "gold_kpis"} <= set(_t))
    check("three notebook tasks pointing at labs/tasks/…",
          all(_t.get(k, {}).get("type") == "notebook" and _t[k].get("path", "").endswith("/labs/tasks/" + k)
              for k in ("land_orders", "load_bronze", "build_silver_gold")))
    check("gold_kpis is a SQL task running a FILE on a warehouse",
          _t.get("gold_kpis", {}).get("type") == "sql (file)" and bool(_t["gold_kpis"].get("warehouse_id")))
    check("dependencies form the chain land → bronze → silver/gold → kpis",
          [d for d, _ in _t.get("load_bronze", {}).get("depends_on", [])] == ["land_orders"]
          and [d for d, _ in _t.get("build_silver_gold", {}).get("depends_on", [])] == ["load_bronze"]
          and [d for d, _ in _t.get("gold_kpis", {}).get("depends_on", [])] == ["build_silver_gold"])
    check("job parameter files (default 1)", str(_s["parameters"].get("files")) == "1")
    check("a failure notification is configured", len(_s["email_on_failure"]) >= 1)
    _runs = [run_summary(r["run_id"], verbose=False) for r in job_runs(JOB, 10)]
    _ok = [r for r in _runs if r["state"] == "SUCCESS"]
    check("at least 2 successful runs", len(_ok) >= 2)
    check("one of them ran with files = 2 (Run now with different parameters)",
          any(str(r["parameters"].get("files")) == "2" for r in _ok))
_files = len(landed10())
_exp_rev = spark.table("jobs_gold_daily").agg(F.round(F.sum("revenue"), 2)).first()[0] \
    if spark.catalog.tableExists("jobs_gold_daily") else None
check(f"bronze has 121 rows per landed file ({_files} files)",
      _files > 0 and spark.table("jobs_bronze_orders").count() == 121 * _files)
check("silver has 119 valid orders per file",
      spark.catalog.tableExists("jobs_silver_orders") and spark.table("jobs_silver_orders").count() == 119 * _files)
check("jobs_gold_kpis was refreshed by the SQL task and matches gold",
      spark.catalog.tableExists("jobs_gold_kpis") and _exp_rev is not None
      and abs((spark.table("jobs_gold_kpis").first()["revenue"] or 0) - _exp_rev) < 0.01)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧹 Before you leave
# MAGIC The job has no trigger, so nothing runs by itself. Keep it — **10-L3** adds a schedule to it.
# MAGIC
# MAGIC ## ✅ What you practised
# MAGIC * A job = **tasks** + **dependencies**; notebook tasks on **serverless** compute, a **SQL file task** on a **SQL warehouse**.
# MAGIC * **Job parameters** are pushed down to notebook tasks as **widgets**; **Run now with different parameters** overrides them
# MAGIC   for one run.
# MAGIC * Each task run has **output**, **logs** and **task values**; the **matrix view** compares runs.
# MAGIC * Job settings: **notifications**, **duration thresholds**, **max concurrent runs & queue**, **tags**, **permissions**,
# MAGIC   **run as**, **View as code**.
# MAGIC
# MAGIC ➡️ Next: **10-L2 · Control Flow, Retries and Repair Runs**
