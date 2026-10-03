# Databricks notebook source
# MAGIC %md
# MAGIC # ⚙️ Job task · `land_orders` — Land the next daily file(s) into the inbox
# MAGIC **Section 10 job-task notebook.** You don't run this notebook yourself during the labs — a **Lakeflow Job** runs it as a
# MAGIC **notebook task**. (Running it interactively is fine too: it uses the default values of the widgets.)
# MAGIC
# MAGIC Simulates the arrival of new data: copies the next `files` July file(s) from `orders-staging/` to `raw/lab10/inbox/`.
# MAGIC
# MAGIC | Parameter (widget) | Default | Meaning |
# MAGIC |---|---|---|
# MAGIC | `files` | `1` | How many daily files to land |
# MAGIC | `catalog` | *(empty)* | optional - only if you use `COURSE_CATALOG` in the labs |
# MAGIC
# MAGIC **Task values set:** `landed_files` (list of file names)
# MAGIC
# MAGIC > How parameters reach this notebook: **job parameters** are pushed down to every notebook task and **task parameters**
# MAGIC > are set on the task — both arrive as **widgets** and are read with `dbutils.widgets.get()`.

# COMMAND ----------

# DBTITLE 1,Parameters (widgets)
dbutils.widgets.text("files", "1", "How many daily files to land")
dbutils.widgets.text("catalog", "", "Catalog (optional)")
COURSE_CATALOG = dbutils.widgets.get("catalog").strip() or None

# COMMAND ----------

# MAGIC %run ../../../Includes/_setup

# COMMAND ----------

# MAGIC %run ../_10_prepare

# COMMAND ----------

# DBTITLE 1,Run the task
result = task_land_orders(files=dbutils.widgets.get("files"))

# COMMAND ----------

# DBTITLE 1,Notebook output (shown in the run's task output)
dbutils.notebook.exit(json.dumps(result, default=str))
