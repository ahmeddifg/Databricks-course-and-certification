# Databricks notebook source
# MAGIC %md
# MAGIC # ⚙️ Job task · `ingest_bronze`
# MAGIC Refreshes the one-off bronze tables and runs the Auto Loader stream (availableNow) for the enrollments.
# MAGIC Has **1 retry** in the job: a schema change (new column) stops Auto Loader once - the retry continues.
# MAGIC
# MAGIC Part of the job **14 SkillWave Daily Refresh** (Lab 14-L4). You can also run it by hand.

# COMMAND ----------

# DBTITLE 1,Parameters (job parameters arrive as widgets)
dbutils.widgets.text("catalog", "skillwave", "Catalog")          # job parameter (environment-specific)
CAPSTONE_CATALOG = dbutils.widgets.get("catalog")

# COMMAND ----------

# MAGIC %run ../_14_prepare

# COMMAND ----------

# DBTITLE 1,Run the task
result = task_ingest_bronze()
print(result)
dbutils.notebook.exit(json.dumps(result, default=str))
