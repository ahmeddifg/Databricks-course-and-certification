# Databricks notebook source
# MAGIC %md
# MAGIC # 🗂️ 03-D · Data used in Section 03
# MAGIC
# MAGIC | Data | Where | Created by | Used in |
# MAGIC |---|---|---|---|
# MAGIC | Products CSV (36) | `raw/products-csv/` | `Includes/_setup` | 03-L1 (`lab03_products`), 03-L2 (`lab03_catalog`), 03-L3 (`lab03_inventory`) |
# MAGIC | Orders Parquet (2,010) + July batch `01.json` (121) | `raw/orders-parquet/`, `raw/orders-staging/` | `Includes/_setup` | 03-L2 (`lab03_orders`) |
# MAGIC | Customers JSON (300) | `raw/customers-json/` | `Includes/_setup` | 03-L2 (`lab03_customers`) |
# MAGIC | **Customer changes** (40 changed + 20 new) | `raw/customers-updates-json/` | `labs/_03_prepare` | 03-L2 `MERGE` upsert |
# MAGIC | **New products feed** (3 duplicates + 3 new) | `raw/products-new-csv/` | `labs/_03_prepare` | 03-L2 insert-only `MERGE` |
# MAGIC | Generated events (12 × 1,000 rows) | table `lab03_events` | 03-L2 | `OPTIMIZE` / `VACUUM` |
# MAGIC
# MAGIC The labs create their own `lab03_*` Delta tables, because **creating and changing tables is what this section teaches**.
# MAGIC Run this notebook to generate the Section 03 source files, preview them, or reset the section.

# COMMAND ----------

# MAGIC %run ../../Includes/_setup

# COMMAND ----------

# MAGIC %run ../labs/_03_prepare

# COMMAND ----------

# MAGIC %md
# MAGIC ## 👥 customers-updates-json

# COMMAND ----------

# DBTITLE 1,Preview the customer change feed
updates = spark.read.json(f"{dataset_path}/customers-updates-json")
existing = spark.read.json(f"{dataset_path}/customers-json").select("customer_id")
print("rows:", updates.count(),
      "| existing customers changed:", updates.join(existing, "customer_id").count(),
      "| new customers:", updates.join(existing, "customer_id", "left_anti").count())
display(updates.orderBy("customer_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📦 products-new-csv

# COMMAND ----------

# DBTITLE 1,Preview the new products feed
display(spark.read.option("header", "true").option("delimiter", ";").csv(f"{dataset_path}/products-new-csv"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧹 Reset Section 03
# MAGIC Uncomment to drop every `lab03_*` table (dropped managed tables stay recoverable with `UNDROP` for 7 days).

# COMMAND ----------

# DBTITLE 1,Reset (optional)
# reset_lab03()
