# Databricks notebook source
# MAGIC %md
# MAGIC # ⚙️ Job task · `build_silver_gold` — Rebuild jobs_silver_orders and jobs_gold_daily
# MAGIC **Section 10 job-task notebook.** You don't run this notebook yourself during the labs — a **Lakeflow Job** runs it as a
# MAGIC **notebook task**. (Running it interactively is fine too: it uses the default values of the widgets.)
# MAGIC
# MAGIC Full, idempotent rebuild of silver (dedup, no cancelled orders, `order_date`, `country`) and the daily gold table from all of bronze.
# MAGIC
# MAGIC | Parameter (widget) | Default | Meaning |
# MAGIC |---|---|---|
# MAGIC | *(none)* | | |
# MAGIC | `catalog` | *(empty)* | optional - only if you use `COURSE_CATALOG` in the labs |
# MAGIC
# MAGIC **Task values set:** `silver_rows`, `gold_days`
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
result = task_build_silver_gold()

# COMMAND ----------

# DBTITLE 1,Notebook output (shown in the run's task output)
dbutils.notebook.exit(json.dumps(result, default=str))
