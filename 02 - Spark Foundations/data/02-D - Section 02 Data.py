# Databricks notebook source
# MAGIC %md
# MAGIC # 🗂️ 02-D · Data used in Section 02
# MAGIC
# MAGIC Section 02 uses the ShopWave raw files turned into three small **Delta tables**, plus DataFrames generated on the fly
# MAGIC (`spark.range(...)`) when a lab needs millions of rows.
# MAGIC
# MAGIC | Table / data | Rows | Built from | Used in |
# MAGIC |---|---|---|---|
# MAGIC | `lab02_orders` (`order_id, order_ts, customer_id, quantity, total, items`) | 2,010 | `raw/orders-parquet` | 02-L1, 02-L2, 02-L3 |
# MAGIC | `lab02_customers` (`customer_id, first_name, last_name, city, country, email`) | 300 | `raw/customers-json` (JSON profile flattened) | 02-L1, 02-L2, 02-L3 |
# MAGIC | `lab02_products` (`product_id, title, brand, category, price`) | 36 | `raw/products-csv` | 02-L1, 02-L3 |
# MAGIC | `spark.range(...)` demo data | 2 M – 5 B (lazy!) | generated in the lab | 02-L1, 02-L2 |
# MAGIC
# MAGIC The tables are created automatically by `labs/_02_prepare`. Run this notebook to **rebuild** them from scratch
# MAGIC (e.g. after experimenting) and to look at them.

# COMMAND ----------

# MAGIC %run ../../Includes/_setup

# COMMAND ----------

# MAGIC %run ../labs/_02_prepare

# COMMAND ----------

# DBTITLE 1,Rebuild the tables from the raw files
print("Rebuilt:", _prepare_lab02_tables(force=True))

# COMMAND ----------

# DBTITLE 1,The three tables
# MAGIC %sql
# MAGIC SHOW TABLES LIKE 'lab02*'

# COMMAND ----------

# DBTITLE 1,Preview
for t in ("lab02_orders", "lab02_customers", "lab02_products"):
    print(f"── {t}: {spark.table(t).count()} rows")
    spark.table(t).printSchema()

display(spark.table("lab02_orders").limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Why these tables are good for Spark lessons
# MAGIC * `lab02_orders.items` is an **array of structs** → `explode()` multiplies rows (a classic source of unexpected data volume).
# MAGIC * `lab02_customers` is **small** → Spark broadcasts it automatically in joins.
# MAGIC * Orders reference customer **`C9999`**, which doesn't exist → inner joins silently drop those rows (5 orders).
