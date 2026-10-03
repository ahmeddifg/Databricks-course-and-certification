# Databricks notebook source
# MAGIC %md
# MAGIC # ⚙️ Job task · `quarantine_alert`
# MAGIC Runs only when `check_quality` is **true** (too many quarantined rows): prints the quarantine by reason.
# MAGIC In real life: send an e-mail / Slack message, open a ticket.
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
result = task_quarantine_alert()
print(result)
dbutils.notebook.exit(json.dumps(result, default=str))
