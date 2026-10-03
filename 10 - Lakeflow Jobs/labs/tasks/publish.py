# Databricks notebook source
# MAGIC %md
# MAGIC # ⚙️ Job task · `publish` — Publish the gold KPIs (fails while publishing is disabled)
# MAGIC **Section 10 job-task notebook.** You don't run this notebook yourself during the labs — a **Lakeflow Job** runs it as a
# MAGIC **notebook task**. (Running it interactively is fine too: it uses the default values of the widgets.)
# MAGIC
# MAGIC Reads `jobs_control.publish_enabled`; if it is `false` the task **fails on purpose** (repair-run demo), otherwise it appends a KPI snapshot to `jobs_published`.
# MAGIC
# MAGIC | Parameter (widget) | Default | Meaning |
# MAGIC |---|---|---|
# MAGIC | *(none)* | | |
# MAGIC | `catalog` | *(empty)* | optional - only if you use `COURSE_CATALOG` in the labs |
# MAGIC
# MAGIC **Task values set:** -
# MAGIC
# MAGIC > How parameters reach this notebook: **job parameters** are pushed down to every notebook task and **task parameters**
# MAGIC > are set on the task — both arrive as **widgets** and are read with `dbutils.widgets.get()`.

# COMMAND ----------

# DBTITLE 1,Parameters (widgets)
dbutils.widgets.text("catalog", "", "Catalog (optional)")
COURSE_CATALOG = dbutils.widgets.get("catalog").strip() or None

# COMMAND ----------

# MAGIC %run ../../../Includes/_setup

# COMMAND ----------

# MAGIC %run ../_10_prepare

# COMMAND ----------

# DBTITLE 1,Run the task
result = task_publish()

# COMMAND ----------

# DBTITLE 1,Notebook output (shown in the run's task output)
dbutils.notebook.exit(json.dumps(result, default=str))
