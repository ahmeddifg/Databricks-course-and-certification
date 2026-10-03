# Databricks notebook source
# MAGIC %md
# MAGIC # ⚙️ Job task · `work` — one step of the 12-L1 job “12-L1 ShopWave Nightly”
# MAGIC **Section 12 job-task notebook.** You don't run this notebook yourself — the 12-L1 job runs it four times (one **notebook
# MAGIC task** per step). Each task runs a small real Spark query on `perf_events` and then **simulates** the rest of its processing
# MAGIC time with a pause, so that run durations are predictable and you can create a *regression* or a *failure* on purpose.
# MAGIC
# MAGIC | Parameter (widget) | Set by | Meaning |
# MAGIC |---|---|---|
# MAGIC | `step` · `base_seconds` · `run_id` | task parameters | which step this task is, its normal duration, the job run id (`{{job.run_id}}`) |
# MAGIC | `slow_step` · `slow_factor` | job parameters | make one step `slow_factor` times slower (default: none) |
# MAGIC | `fail_step` | job parameter | make one step fail (default: none) |
# MAGIC | `catalog` · `schema` | job parameters | the course catalog and schema |
# MAGIC
# MAGIC Every execution writes one line into `perf_job_log`.

# COMMAND ----------

# DBTITLE 1,Parameters (widgets)
for _name, _default in (("step", "extract"), ("base_seconds", "10"), ("run_id", "interactive"), ("slow_step", "none"),
                        ("slow_factor", "1"), ("fail_step", "none"), ("catalog", ""), ("schema", "")):
    dbutils.widgets.text(_name, _default)
COURSE_CATALOG = dbutils.widgets.get("catalog").strip() or None
COURSE_SCHEMA = dbutils.widgets.get("schema").strip() or None

# COMMAND ----------

# MAGIC %run ../../../Includes/_setup

# COMMAND ----------

# MAGIC %run ../_12_prepare

# COMMAND ----------

# DBTITLE 1,Run the step
result = task_work(step=dbutils.widgets.get("step"), base_seconds=dbutils.widgets.get("base_seconds"),
                   run_id=dbutils.widgets.get("run_id"), slow_step=dbutils.widgets.get("slow_step"),
                   slow_factor=dbutils.widgets.get("slow_factor"), fail_step=dbutils.widgets.get("fail_step"))

# COMMAND ----------

# DBTITLE 1,Notebook output (shown in the task run)
dbutils.notebook.exit(json.dumps(result, default=str))
