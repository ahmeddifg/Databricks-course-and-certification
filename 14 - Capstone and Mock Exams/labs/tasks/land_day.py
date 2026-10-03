# Databricks notebook source
# MAGIC %md
# MAGIC # ⚙️ Job task · `land_day`
# MAGIC Lands the next daily enrollment file(s) into the landing volume (simulates the source system).
# MAGIC
# MAGIC Part of the job **14 SkillWave Daily Refresh** (Lab 14-L4). You can also run it by hand.

# COMMAND ----------

# DBTITLE 1,Parameters (job parameters arrive as widgets)
dbutils.widgets.text("catalog", "skillwave", "Catalog")          # job parameter (environment-specific)
CAPSTONE_CATALOG = dbutils.widgets.get("catalog")
dbutils.widgets.text("days", "1", "Days to land")

# COMMAND ----------

# MAGIC %run ../_14_prepare

# COMMAND ----------

# DBTITLE 1,Run the task
result = task_land_day(dbutils.widgets.get("days"))
print(result)
dbutils.notebook.exit(json.dumps(result, default=str))
