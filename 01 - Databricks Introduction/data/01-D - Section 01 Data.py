# Databricks notebook source
# MAGIC %md
# MAGIC # 🗂️ 01-D · Data used in Section 01
# MAGIC
# MAGIC Section 01 is about the **platform**, so it needs very little data:
# MAGIC
# MAGIC | Data | Where | Created by | Used in |
# MAGIC |---|---|---|---|
# MAGIC | ShopWave raw files (customers JSON, products CSV, orders Parquet) | `/Volumes/<catalog>/shopwave/raw/` | `Includes/_setup` (automatic) | 01-L1, 01-L3 |
# MAGIC | `welcome.txt` + your sandbox files | `/Volumes/<catalog>/shopwave/files/` | `Includes/_setup` / you | 01-L1 (`dbutils.fs`), 01-L3 |
# MAGIC | Table `lab01_customers` | `<catalog>.shopwave` | Lab 01-L1 | 01-L1, 01-L2 |
# MAGIC | Table `lab01_products` | `<catalog>.shopwave` | Lab 01-L3 | 01-L3 |
# MAGIC | Databricks sample datasets (optional) | catalog **`samples`** | Databricks (read-only) | exploration |
# MAGIC
# MAGIC Run this notebook to make sure everything exists, or to rebuild the Section 01 tables if you deleted them.

# COMMAND ----------

# MAGIC %run ../../Includes/_setup

# COMMAND ----------

# DBTITLE 1,Build (or rebuild) the Section 01 tables
spark.read.json(f"{dataset_path}/customers-json").write.mode("overwrite").saveAsTable("lab01_customers")

(spark.read
      .option("header", "true").option("delimiter", ";").option("inferSchema", "true")
      .csv(f"{dataset_path}/products-csv")
      .write.mode("overwrite").saveAsTable("lab01_products"))

print("lab01_customers:", spark.table("lab01_customers").count(), "rows")
print("lab01_products :", spark.table("lab01_products").count(), "rows")

# COMMAND ----------

# DBTITLE 1,Section 01 tables in the course schema
# MAGIC %sql
# MAGIC SHOW TABLES LIKE 'lab01*'

# COMMAND ----------

# DBTITLE 1,The welcome file in the files volume
print(dbutils.fs.head(f"{files_path}/welcome.txt"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🎁 Bonus: the `samples` catalog
# MAGIC Most workspaces (including Free Edition) include a read-only **`samples`** catalog with ready-made datasets —
# MAGIC handy for practising SQL without creating anything.

# COMMAND ----------

# DBTITLE 1,Explore the samples catalog (if available)
try:
    display(spark.sql("SHOW SCHEMAS IN samples"))
    display(spark.sql("SELECT * FROM samples.nyctaxi.trips LIMIT 10"))
except Exception as e:
    print("ℹ️ The samples catalog isn't available in this workspace:", str(e).splitlines()[0][:150])
