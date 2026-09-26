# Databricks notebook source
# MAGIC %md
# MAGIC # 👶 _04_child — called by Lab 04-L2 with `dbutils.notebook.run("./_04_child", 300, {...})`
# MAGIC `dbutils.notebook.run` starts this notebook as a **separate run with its own Spark session**. We use that to prove that
# MAGIC a **temporary view** created by the caller is **not visible** here, while a **stored view** (saved in Unity Catalog) is.
# MAGIC
# MAGIC Returns a JSON string with `dbutils.notebook.exit()`. You can also run it on its own — the widget defaults are used then.

# COMMAND ----------

# DBTITLE 1,Parameters
dbutils.widgets.text("temp_view", "tv_big_spenders")
dbutils.widgets.text("stored_view", "")

temp_view = dbutils.widgets.get("temp_view")
stored_view = dbutils.widgets.get("stored_view")

# COMMAND ----------

# DBTITLE 1,Can this session see the views?
import json


def visible(name: str) -> bool:
    if not name:
        return False
    try:
        spark.table(name).limit(1).collect()
        return True
    except Exception:
        return False


result = {"temp_view": temp_view, "temp_view_visible": visible(temp_view),
          "stored_view": stored_view, "stored_view_visible": visible(stored_view)}
print(result)
dbutils.notebook.exit(json.dumps(result))
