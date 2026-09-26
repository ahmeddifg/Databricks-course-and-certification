# Databricks notebook source
# MAGIC %md
# MAGIC # 🗂️ 04-D · Data used in Section 04
# MAGIC
# MAGIC Section 04 is about **where data objects live**, so its labs create a **catalog of their own** instead of new source files.
# MAGIC
# MAGIC | Object | Name | Created by | Used in |
# MAGIC |---|---|---|---|
# MAGIC | Lab catalog | `lab04_<your-user-name>` (fallback: schemas `lab04_*` in the course catalog) | `ensure_lab_catalog()` in `labs/_04_prepare` | all labs |
# MAGIC | Schemas | `bronze`, `silver`, `gold` (+ `mart` in 04-L3) | 04-L1 / `ensure_lab_schemas()` | all labs |
# MAGIC | Bronze tables | `bronze.customers` (300), `bronze.products` (36), `bronze.stores` (5) | 04-L1, 04-L2 | 04-L1, 04-L2 |
# MAGIC | Silver tables | `silver.customers` (300), `silver.products` (36) · `silver.orders` (1,980) | customers/products: 04-L1 or `build_lab_tables()` · orders: `build_lab_tables()` (04-L2, 04-D) | 04-L1, 04-L2 |
# MAGIC | Gold objects | `gold.customers_by_country` (table), `gold.v_customers_per_country`, `gold.v_order_details` (views), `gold.mv_revenue_by_country` (materialized view) | 04-L1, 04-L2 | 04-L2 |
# MAGIC | Volume | `bronze.landing` → `stores/stores_2026.csv`, `readme.txt` | 04-L2 | 04-L2 |
# MAGIC | Functions | `silver.mask_email` (SQL), `silver.email_domain` (Python) | 04-L2 | 04-L2 |
# MAGIC | Challenge mart | `mart.products`, `mart.price_band()`, `mart.v_category_bands`, volume `mart.exports` | 04-L3 | 04-L3 |
# MAGIC
# MAGIC All source rows come from the ShopWave files in the course volume `raw/` (created by `Includes/_setup`):
# MAGIC `customers-json`, `products-csv` and `orders-parquet` (duplicates and cancelled orders removed → 1,980 orders).

# COMMAND ----------

# MAGIC %run ../../Includes/_setup

# COMMAND ----------

# MAGIC %run ../labs/_04_prepare

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔨 Build the lab objects without doing 04-L1
# MAGIC Creates the catalog, the three schemas and the silver tables (idempotent).

# COMMAND ----------

# DBTITLE 1,Build
ensure_lab_catalog()
build_lab_tables()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔍 Everything Section 04 created

# COMMAND ----------

# DBTITLE 1,Inventory from information_schema
_schemas = [r[0] for r in spark.sql(f"SHOW SCHEMAS IN {LAB_CATALOG}").collect()
            if r[0].startswith(SCHEMA_PREFIX) and r[0] not in ("information_schema", "default")]
_in = "', '".join(_schemas)
display(spark.sql(f"""
    SELECT table_schema, table_name, table_type, comment
    FROM {LAB_CATALOG}.information_schema.tables
    WHERE table_schema IN ('{_in}')
    ORDER BY table_schema, table_type, table_name"""))
display(spark.sql(f"""
    SELECT volume_schema, volume_name, volume_type FROM {LAB_CATALOG}.information_schema.volumes
    WHERE volume_schema IN ('{_in}')"""))
display(spark.sql(f"""
    SELECT routine_schema, routine_name, routine_body FROM {LAB_CATALOG}.information_schema.routines
    WHERE routine_schema IN ('{_in}')"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧹 Clean up Section 04
# MAGIC When you have finished the section (including the challenge), uncomment and run this cell. It drops your lab catalog with
# MAGIC `DROP CATALOG … CASCADE` (or the `lab04_*` schemas in fallback mode). The course schema `shopwave` is **not** touched.

# COMMAND ----------

# DBTITLE 1,Clean up (optional)
# drop_lab_catalog()
