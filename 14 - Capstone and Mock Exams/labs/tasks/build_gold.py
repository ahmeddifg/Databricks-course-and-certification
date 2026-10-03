# Databricks notebook source
# MAGIC %md
# MAGIC # ⚙️ Job task · `build_gold`
# MAGIC Rewrites the gold star schema with INSERT OVERWRITE (keeps masks, grants, clustering) and refreshes the
# MAGIC materialized view. Runs only when `check_quality` is **false** and the pipeline task succeeded.
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
result = task_build_gold()
print(result)
dbutils.notebook.exit(json.dumps(result, default=str))
