# Databricks notebook source
# MAGIC %md
# MAGIC # 🧪 Lab 12-L4 · Troubleshooting Clinic — Startup, Libraries and Memory (Guided)
# MAGIC **Time:** ~40 min · **Compute:** Serverless notebook · **Works on Free Edition**
# MAGIC
# MAGIC The on-call pager rings. In this clinic you **read evidence** (event logs, error messages, job termination codes) the way
# MAGIC you would in production, and you reproduce a real **library version conflict** on serverless.
# MAGIC
# MAGIC | Part | You will… |
# MAGIC |---|---|
# MAGIC | 1 | 🗂️ **Case files**: 8 real-looking incidents — start-up failures, libraries, OOM. Diagnose each one |
# MAGIC | 2 | Driver vs executor memory: **safe and unsafe** ways to bring data to the driver (measured, not crashed) |
# MAGIC | 3 | Evidence from a real job: termination code and error of the 12-L1 failed run |
# MAGIC | 4 | Where libraries come from on **serverless**: the Environment panel |
# MAGIC | 5 | A real **library conflict**: install, upgrade, stale import, `restartPython()` — *runs last: it resets Python* |
# MAGIC
# MAGIC > Free Edition has serverless compute only — there is no cluster you could break. The cluster cases come with the event-log
# MAGIC > and error text you would see on classic compute; Parts 2–5 run for real.

# COMMAND ----------

# MAGIC %run ../../Includes/_setup

# COMMAND ----------

# MAGIC %run ./_12_prepare

# COMMAND ----------

# MAGIC %run ../../Includes/_quiz

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1 · 🗂️ Case files
# MAGIC Each case shows the evidence. Pick the diagnosis / fix, then click **Check answer** for the explanation. Aim for 8/8.

# COMMAND ----------

# DBTITLE 1,Case files
cases = [
    {"topic": "Start-up",
     "q": "**Case 1.** A nightly job on a new job cluster (40 × `r5.4xlarge` workers) fails before any task starts. Event log:",
     "code": "CREATING      Cluster creation requested\nTERMINATING   Reason: CLOUD_PROVIDER_LAUNCH_FAILURE (CLOUD_FAILURE)\n              InsufficientInstanceCapacity: We currently do not have sufficient\n              r5.4xlarge capacity in the Availability Zone you requested",
     "options": ["Increase the driver memory and re-run",
                 "Retry later or allow other instance types / zones (pools, flexible node types) — or run the job on serverless",
                 "Fix the init script that installs the libraries",
                 "Grant the job owner CAN MANAGE on the job"],
     "answer": 1,
     "explanation": "The cloud provider had no capacity for that instance type in that zone (type CLOUD_FAILURE - not your code). Retry, use another type/zone, keep warm VMs in a pool, or move the job to serverless compute where Databricks manages capacity."},
    {"topic": "Start-up",
     "q": "**Case 2.** A cluster starts, runs its init scripts and terminates after 2 minutes:",
     "code": "INIT_SCRIPTS_STARTED   /Volumes/ops/tools/scripts/install_odbc.sh\nTERMINATING            Reason: INIT_SCRIPT_FAILURE (CLIENT_ERROR)\n                       Script exit status is non-zero",
     "options": ["Open the init script logs, fix the failing command (or the path / read permission on the volume) and restart",
                 "Request a higher vCPU quota from the cloud provider",
                 "Switch the cluster to Photon",
                 "Disable auto-termination"],
     "answer": 0,
     "explanation": "INIT_SCRIPT_FAILURE is a CLIENT_ERROR: the script returned a non-zero exit code. The init script logs (cluster log delivery) show the failing command - e.g. an unreachable package repository, a typo, or missing read permission on the script."},
    {"topic": "Start-up",
     "q": "**Case 3.** Every new cluster in a freshly deployed workspace (customer-managed VPC) ends with `BOOTSTRAP_TIMEOUT` / `Node daemon ping timeout in 600000 ms`. What is the most likely cause?",
     "options": ["A Python library conflict",
                 "The Databricks Runtime version is too new",
                 "Network configuration: the VMs can't reach the Databricks control plane / storage (firewall, routes, DNS, endpoints)",
                 "The cluster is too small"],
     "answer": 2,
     "explanation": "The VMs were created but could never register with the control plane - a networking problem (security groups/NSG, firewall egress rules, routes, DNS, VPC/VNet endpoints)."},
    {"topic": "Start-up",
     "q": "**Case 4.** A data engineer can't create a cluster with 64 workers: *“Validation failed for num_workers, the value must be at most 10”*. What happened?",
     "options": ["The cloud is out of capacity",
                 "The cluster's compute policy limits the number of workers - use an allowed value or ask an admin for another policy",
                 "The workspace is on Free Edition",
                 "The init script failed"],
     "answer": 1,
     "explanation": "Compute policies restrict cluster settings (worker count, instance types, auto-termination...). The request violates the policy, so it is refused before any VM is requested."},
    {"topic": "Libraries",
     "q": "**Case 5.** A job task fails with `ImportError: cannot import name 'TypeAliasType' from 'typing_extensions'`. The same notebook worked interactively last week. The job cluster installs `great_expectations` (unpinned) as a cluster library. Best fix?",
     "options": ["Increase the number of retries of the task",
                 "Run the job on an all-purpose cluster",
                 "Pin compatible versions (requirements file / ==), test on the same runtime as the job, restart Python after notebook-scoped installs",
                 "Delete the cluster's event log"],
     "answer": 2,
     "explanation": "An unpinned library pulled a new version whose dependencies conflict with what the runtime provides. Pin versions so dev and jobs use the same environment; with %pip, restart Python so the new versions are loaded."},
    {"topic": "Libraries",
     "q": "**Case 6.** In a notebook you run `%pip install pandas==2.2.3` after `import pandas`. `pandas.__version__` still prints the runtime's version. Why?",
     "options": ["The install failed silently",
                 "The module was already imported in the running Python process - call dbutils.library.restartPython() (or put %pip at the top)",
                 "Notebook-scoped libraries can't change pandas",
                 "pandas can only be installed as a cluster library"],
     "answer": 1,
     "explanation": "The package on disk changed, but the Python process keeps the already-imported module. Put %pip cells at the top of the notebook and restart Python after changing versions (Part 5 shows it for real)."},
    {"topic": "Memory",
     "q": "**Case 7.** An analyst's notebook runs `pdf = spark.table('sales_2025').toPandas()` (400 M rows). After a few minutes:",
     "code": "The spark driver has stopped unexpectedly and is restarting.\nYour notebook will be automatically reattached.",
     "options": ["Executor out of memory - add more workers",
                 "Data skew in sales_2025",
                 "Driver out of memory - aggregate / filter / limit in Spark (or write to a table) and only bring the small result to pandas",
                 "A cloud capacity problem"],
     "answer": 2,
     "explanation": "toPandas() and collect() pull every row into the driver's memory. More workers don't help - the driver is the bottleneck. Keep the work distributed; bring only small results to the driver (or use the pandas API on Spark)."},
    {"topic": "Memory",
     "q": "**Case 8.** A join stage runs 199 of 200 tasks in seconds; the last task runs 40 min, then:",
     "code": "ExecutorLostFailure (executor 7 exited caused by one of the running tasks)\nReason: Container killed ... exceeding memory limits\nStage 14: Spill (Memory) 38 GB / Spill (Disk) 11 GB on task 117",
     "options": ["Driver OOM - increase spark.driver.maxResultSize",
                 "Executor OOM caused by a skewed key - salt the key / let AQE split the skewed partition / broadcast the small side",
                 "The init script failed",
                 "Too many small files - run VACUUM"],
     "answer": 1,
     "explanation": "One task gets far more data than the others (skew), spills heavily and finally kills its executor. Fix the skew (12-L2): AQE skew join, broadcast, salting - or isolate the hot key."},
]
render_quiz(cases, title="Case files · start-up, libraries and memory", pass_mark=1.0,
            meta="8 incidents · read the evidence, pick the diagnosis")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2 · Driver memory: what may come back to the driver?
# MAGIC `collect()`, `toPandas()`, `take()`, `first()` and `display()` bring rows **to the driver**. Everything else stays
# MAGIC distributed on the executors. Measure how big the result of each pattern would be — **before** running it.

# COMMAND ----------

# DBTITLE 1,How many rows would each pattern bring to the driver?
events = spark.table("perf_events")
patterns = {
    "❌ events.toPandas()  (whole table)": events,
    "❌ events.where(\"customer_id = 'C0007'\").collect()  (the bot's events)": events.where("customer_id = 'C0007'"),
    "✅ events.groupBy('event_date').count().toPandas()  (one row per day)": events.groupBy("event_date").count(),
    "✅ events.limit(1000).toPandas()  (a sample)": events.limit(1000),
}
_sizes = {}
for _label, _df in patterns.items():
    _sizes[_label] = _df.count()
    print(f"{_sizes[_label]:>10,} rows  ← {_label}")
_bytes = int(table_detail("perf_events").get("sizeInBytes") or 0)
print(f"\nperf_events is {_bytes / 1048576:.0f} MB compressed on disk — as Python objects on the driver it would be many times that.")

# COMMAND ----------

# MAGIC %md
# MAGIC Run only the ✅ ones:

# COMMAND ----------

# DBTITLE 1,The safe patterns
daily = events.groupBy("event_date").agg(F.count(F.lit(1)).alias("events"), F.sum("amount").alias("revenue")).toPandas()
sample = events.limit(1000).toPandas()
print(f"daily: {len(daily)} rows · sample: {len(sample)} rows — both tiny on the driver")
# For big results: write them to a table instead of collecting
events.where("customer_id = 'C0007'").write.mode("overwrite").saveAsTable("perf_bot_events")
print("bot events written to perf_bot_events:", spark.table("perf_bot_events").count(), "rows")

# COMMAND ----------

# MAGIC %md
# MAGIC | Pattern | Risk | Instead |
# MAGIC |---|---|---|
# MAGIC | `collect()` / `toPandas()` of a big table | **driver OOM** (*driver has stopped unexpectedly*), `spark.driver.maxResultSize` error | aggregate / filter / `limit` first; write to a table; pandas API on Spark (`pyspark.pandas`) |
# MAGIC | `F.broadcast(big_df)` / huge broadcast hint | driver **and** executor OOM (the table is built on the driver, copied to every executor) | broadcast only small tables; let AQE decide |
# MAGIC | Python `for` loop with one Spark job per item | slow, driver-bound (gaps in the Spark UI timeline) | one set-based query (join / group by) |
# MAGIC | `df.cache()` everything | executor memory pressure → spill / OOM | cache only what's reused; `unpersist()` |
# MAGIC | One giant partition (skew), `explode` of big arrays | **executor OOM** (*executor lost*), heavy spill | salting, AQE, more partitions (12-L2) |

# COMMAND ----------

# DBTITLE 1,✅ Check Part 2
check("the daily summary is small enough for the driver (≤ 200 rows)", len(daily) <= 200)
check("the bot's events went to a table, not to the driver", spark.table("perf_bot_events").count() == _sizes[list(patterns)[1]])
spark.sql("DROP TABLE IF EXISTS perf_bot_events")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 3 · Evidence from a real job run
# MAGIC If you did **12-L1**, its job has a run that failed (and was repaired). Every job run carries a **termination code** and every
# MAGIC failed task its **error** — the first things to read.

# COMMAND ----------

# DBTITLE 1,Termination codes and errors of the 12-L1 runs
try:
    _rows = run_history(JOB_NAMES12["L1"]) if find_job(JOB_NAMES12["L1"]) else []
except Exception as e:
    print("Jobs API not reachable:", _first_line(e))
    _rows = []
if not _rows:
    print("No 12-L1 job yet - do lab 12-L1 first (or skip this part).")
for _r in _rows[-6:]:
    print(f"run {_r['run_id']}: {_r['state']:<10} termination_code={_r['termination_code'] or '-':<22} params: {_r['params']}")

# COMMAND ----------

# MAGIC %md
# MAGIC | Run termination code (Jobs API / system tables) | Means | Look at |
# MAGIC |---|---|---|
# MAGIC | `SUCCESS` | finished OK | duration vs baseline |
# MAGIC | `RUN_EXECUTION_ERROR` | a task failed (your code / data) | the failed task's error + stack trace |
# MAGIC | `CLUSTER_ERROR`, `DRIVER_ERROR`, `CLOUD_FAILURE` | compute failed (start-up, driver crash, cloud) | compute event log, driver logs |
# MAGIC | `LIBRARY_INSTALLATION_ERROR` | a task library couldn't be installed | Libraries / environment, versions |
# MAGIC | `MAX_CONCURRENT_RUNS_EXCEEDED`, `SKIPPED` | run not started (concurrency / queue) | job concurrency + queue settings |
# MAGIC | `USER_CANCELED`, `CANCELED`, timeout | stopped | who / which timeout |
# MAGIC
# MAGIC ## Part 4 · Where libraries come from on serverless
# MAGIC 🖱️ On the right edge of this notebook open the **Environment** side panel (🧩 icon):
# MAGIC 1. **Environment version** (*Base environment*): the set of pre-installed Python packages and the Python version. Jobs that
# MAGIC    run this notebook should use the **same** version.
# MAGIC 2. **Dependencies**: pinned packages (`humanize==4.11.0`, a wheel in a volume, a `requirements.txt`) installed for this
# MAGIC    notebook every time it starts — the serverless replacement for **cluster libraries**.
# MAGIC 3. A job task on serverless gets its libraries from the task's **environment** (or `%pip` in the notebook).
# MAGIC 4. On classic compute: cluster → **Libraries** tab (status *Installed / Pending / Failed* per library) — and
# MAGIC    notebook-scoped `%pip` wins over cluster libraries.
# MAGIC
# MAGIC ## Part 5 · A real library conflict (runs last — it restarts Python)
# MAGIC Steps: install an **old** version → import it → upgrade **without** restarting → the running Python still uses the old
# MAGIC module → `restartPython()` → the new version is active.

# COMMAND ----------

# DBTITLE 1,Is the package available?
import importlib.util
print("humanize installed:", importlib.util.find_spec("humanize") is not None)

# COMMAND ----------

# MAGIC %pip install humanize==4.9.0 --quiet

# COMMAND ----------

# DBTITLE 1,Import the old version
import humanize
import importlib.metadata as md
print("module in memory :", humanize.__version__)
print("package on disk  :", md.version("humanize"))
print(humanize.intword(3_000_000), "events ·", humanize.naturalsize(52_000_000))

# COMMAND ----------

# MAGIC %pip install humanize==4.11.0 --quiet

# COMMAND ----------

# DBTITLE 1,Upgraded - but which version runs?
import importlib.metadata as md
print("package on disk  :", md.version("humanize"))
try:
    print("module in memory :", humanize.__version__, "  ← still the OLD module: it was imported before the upgrade")
    print("🔁 this mismatch is a classic 'library conflict' - fix: restart Python")
except NameError:
    print("module in memory : (none) - this compute restarted Python after %pip by itself; on most runtimes it does not")

# COMMAND ----------

# DBTITLE 1,Restart Python (all variables are lost)
dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,After the restart
import humanize
import importlib.metadata as md
_ok = humanize.__version__ == md.version("humanize") == "4.11.0"
print("module in memory :", humanize.__version__)
print("package on disk  :", md.version("humanize"))
print(("✅ " if _ok else "❌ ") + "after restartPython() the new version is loaded")

# COMMAND ----------

# MAGIC %md
# MAGIC > ⛔ If `%pip install` failed with a network or index error, that **is** a library-installation incident: the compute can't
# MAGIC > reach PyPI (egress rules / a private index needed). In companies the fix is an internal mirror or wheels in a **volume**.
# MAGIC
# MAGIC **Rules you just applied**
# MAGIC
# MAGIC * Put `%pip install` cells at the **top** of a notebook, pin versions with `==`, then `dbutils.library.restartPython()`
# MAGIC   (or the `%restart_python` magic).
# MAGIC * Notebook-scoped libraries (`%pip`) win over cluster libraries, which win over the runtime's versions.
# MAGIC * Same **runtime / environment version** in development and in the job.
# MAGIC
# MAGIC ## ✅ Summary
# MAGIC | Incident | Evidence | Fix |
# MAGIC |---|---|---|
# MAGIC | Capacity / quota | `CLOUD_PROVIDER_LAUNCH_FAILURE`, quota errors (CLOUD_FAILURE) | retry, other types/zones, pools, serverless |
# MAGIC | Init script | `INIT_SCRIPT_FAILURE` (CLIENT_ERROR), init-script logs | fix script, path, permissions, repo access |
# MAGIC | Network | bootstrap timeout, instances unreachable | firewall, routes, DNS, endpoints |
# MAGIC | Policy | validation error on create | allowed values / other policy |
# MAGIC | Library conflict | `ImportError`, wrong `__version__`, install failed | pin versions, `%pip` + `restartPython()`, same environment |
# MAGIC | Driver OOM | *driver has stopped unexpectedly*, maxResultSize | don't collect / toPandas big data; small broadcasts |
# MAGIC | Executor OOM | *executor lost*, spill on one task | fix skew, more partitions, memory-optimized nodes |
