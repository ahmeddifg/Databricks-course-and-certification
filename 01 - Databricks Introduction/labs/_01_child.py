# Databricks notebook source
# MAGIC %md
# MAGIC # 👶 _01_child — called by Lab 01-L1 with `dbutils.notebook.run("./_01_child", 300, {...})`
# MAGIC It runs in its **own** context: it cannot see the caller's variables, so everything it needs arrives as **arguments**,
# MAGIC which show up here as **widget** values. It returns a **string** with `dbutils.notebook.exit()`.
# MAGIC
# MAGIC You *can* run it on its own to test it — the widget defaults are used then.

# COMMAND ----------

# DBTITLE 1,Parameters (arguments from the caller override these defaults)
dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "shopwave")
dbutils.widgets.text("country", "Saudi Arabia")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
country = dbutils.widgets.get("country")

# COMMAND ----------

# DBTITLE 1,Work + return a value
import json

n = spark.sql(
    "SELECT count(*) AS n FROM IDENTIFIER(:tbl) WHERE profile:address:country = :country",
    args={"tbl": f"{catalog}.{schema}.lab01_customers", "country": country},
).first()["n"]

dbutils.notebook.exit(json.dumps({"country": country, "customers": n}))   # must be a string
