# Databricks notebook source
# MAGIC %md
# MAGIC # ⚙️ Job task · `call_partner_api` — A flaky external call (retries demo)
# MAGIC **Section 10 job-task notebook.** You don't run this notebook yourself during the labs — a **Lakeflow Job** runs it as a
# MAGIC **notebook task**. (Running it interactively is fine too: it uses the default values of the widgets.)
# MAGIC
# MAGIC Fails while `attempt <= fail_attempts`. Give it the parameter `attempt = {{task.execution_count}}` and **retries** to see attempt 1 fail and attempt 2 succeed. Interactively (empty `attempt`) it always succeeds.
# MAGIC
# MAGIC | Parameter (widget) | Default | Meaning |
# MAGIC |---|---|---|
# MAGIC | `attempt` | *(empty)* | Attempt number - pass {{task.execution_count}} |
# MAGIC | `fail_attempts` | `1` | Fail while attempt <= this |
# MAGIC | `catalog` | *(empty)* | optional - only if you use `COURSE_CATALOG` in the labs |
# MAGIC
# MAGIC **Task values set:** -
# MAGIC
# MAGIC > How parameters reach this notebook: **job parameters** are pushed down to every notebook task and **task parameters**
# MAGIC > are set on the task — both arrive as **widgets** and are read with `dbutils.widgets.get()`.

# COMMAND ----------

# DBTITLE 1,Parameters (widgets)
dbutils.widgets.text("attempt", "", "Attempt number")
dbutils.widgets.text("fail_attempts", "1", "Fail while attempt <= this")
dbutils.widgets.text("catalog", "", "Catalog (optional)")
COURSE_CATALOG = dbutils.widgets.get("catalog").strip() or None

# COMMAND ----------

# MAGIC %run ../../../Includes/_setup

# COMMAND ----------

# MAGIC %run ../_10_prepare

# COMMAND ----------

# DBTITLE 1,Run the task
result = task_call_partner_api(attempt=dbutils.widgets.get("attempt"), fail_attempts=dbutils.widgets.get("fail_attempts"))

# COMMAND ----------

# DBTITLE 1,Notebook output (shown in the run's task output)
dbutils.notebook.exit(json.dumps(result, default=str))
