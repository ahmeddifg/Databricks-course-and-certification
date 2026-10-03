# Databricks notebook source
# MAGIC %md
# MAGIC # ⚙️ Job task · `notify_failure` — Failure handler (run if: At least one failed)
# MAGIC **Section 10 job-task notebook.** You don't run this notebook yourself during the labs — a **Lakeflow Job** runs it as a
# MAGIC **notebook task**. (Running it interactively is fine too: it uses the default values of the widgets.)
# MAGIC
# MAGIC Writes an `ALERT` line to `jobs_task_log`. Configure it with **Run if dependencies = At least one failed**.
# MAGIC
# MAGIC | Parameter (widget) | Default | Meaning |
# MAGIC |---|---|---|
# MAGIC | `reason` | *(empty)* | Optional text, e.g. {{tasks.publish.result_state}} |
# MAGIC | `catalog` | *(empty)* | optional - only if you use `COURSE_CATALOG` in the labs |
# MAGIC
# MAGIC **Task values set:** -
# MAGIC
# MAGIC > How parameters reach this notebook: **job parameters** are pushed down to every notebook task and **task parameters**
# MAGIC > are set on the task — both arrive as **widgets** and are read with `dbutils.widgets.get()`.

# COMMAND ----------

# DBTITLE 1,Parameters (widgets)
dbutils.widgets.text("reason", "", "Optional text, e.g. {{tasks.publish.result_state}}")
dbutils.widgets.text("catalog", "", "Catalog (optional)")
COURSE_CATALOG = dbutils.widgets.get("catalog").strip() or None

# COMMAND ----------

# MAGIC %run ../../../Includes/_setup

# COMMAND ----------

# MAGIC %run ../_10_prepare

# COMMAND ----------

# DBTITLE 1,Run the task
result = task_notify_failure(reason=dbutils.widgets.get("reason"))

# COMMAND ----------

# DBTITLE 1,Notebook output (shown in the run's task output)
dbutils.notebook.exit(json.dumps(result, default=str))
