# Databricks notebook source
# MAGIC %md
# MAGIC # ⚙️ Job task · `build_silver`
# MAGIC MERGEs students and the new enrollments into silver and quarantines bad rows.
# MAGIC Publishes the **task values** `quarantined` and `new_valid` - the If/else task `check_quality` reads `quarantined`.
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
result = task_build_silver()
print(result)
dbutils.notebook.exit(json.dumps(result, default=str))
