# Databricks notebook source
# MAGIC %md
# MAGIC # 🗂️ 07-D · Data used in Section 07
# MAGIC
# MAGIC Section 07 turns **bronze** data into **silver** and **gold**. It reuses the course dataset (customers, products,
# MAGIC orders) and adds three deliberately **messy** feeds. Everything is generated once by `labs/_07_prepare` —
# MAGIC deterministic, so your numbers match the labs.
# MAGIC
# MAGIC | Source (raw volume) | Rows | Problems built in | Used in |
# MAGIC |---|---|---|---|
# MAGIC | `customers-json/` → `lab07_bronze_customers` | 300 | `profile` is a JSON **string**; ~5 % NULL e-mails | 07-L1, 07-L3 |
# MAGIC | `products-csv/` → `lab07_bronze_products` | 36 | all columns are **strings** (price too) | 07-L1, 07-L2 |
# MAGIC | `orders-parquet/` → `lab07_bronze_orders` | 2010 | 10 exact duplicates, 20 cancelled (quantity 0, empty `items`), 5 orders of unknown customer `C9999` | 07-L1 … 07-L4 |
# MAGIC | `payments-messy/` → `lab07_bronze_payments` | 431 (2 CSV files) | up to 5 spellings per method, amounts like `" 12.50 "`, `"USD 12.50"`, `"N/A"`, two date formats, empty currencies, 10 exact duplicates, 15 re-sent status updates, 3 rows without `payment_id`, 2 negative amounts, 3 unknown orders | 07-L1, 07-L2, 07-L3 |
# MAGIC | `customer-updates/` | 25 + 27 (2 CSV batches) | address/e-mail changes (one NULL → value), 5 new customers, 2 rows that change nothing | 07-L3 SCD |
# MAGIC | `shipments-messy/` | 318 (1 CSV) | carrier spellings, two date formats, `"USD 9.99"` / `"N/A"` costs, re-sent records, 6 exact duplicates, 5 deliveries before shipping | 07-L4 challenge |
# MAGIC | `lab07_country_targets` (table) | 56 | monthly targets per country — 4 country-months missing | 07-L2 multi-key joins |
# MAGIC
# MAGIC **Tables the labs create:** `lab07_silver_*` (07-L1), `lab07_payments_valid` / `_quarantine`, `lab07_dim_*`,
# MAGIC `lab07_fact_sales`, `lab07_gold_*` and the SQL function `lab07_clean_method` (07-L3), `lab07_ch_*` (07-L4).

# COMMAND ----------

# MAGIC %run ../../Includes/_setup

# COMMAND ----------

# MAGIC %run ../labs/_07_prepare

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔍 Preview the messy sources

# COMMAND ----------

# DBTITLE 1,Payments export - raw text
print(dbutils.fs.head(f"{dataset_path}/payments-messy/payments_export_1.csv", 700))

# COMMAND ----------

# DBTITLE 1,Payments - spellings per column
display(spark.table("lab07_bronze_payments")
             .groupBy("method").count().orderBy("method"))

# COMMAND ----------

# DBTITLE 1,Customer update batches
display(spark.read.option("header", True).csv(f"{dataset_path}/customer-updates")
             .select("*", F.col("_metadata.file_name").alias("file"))
             .groupBy("file", "updated_at").count().orderBy("file"))

# COMMAND ----------

# DBTITLE 1,Shipments export - raw text
print(dbutils.fs.head(f"{dataset_path}/shipments-messy/shipments_export.csv", 600))

# COMMAND ----------

# DBTITLE 1,Country targets
display(spark.table("lab07_country_targets").groupBy("country").agg(F.count("*").alias("months")).orderBy("country"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧹 Reset Section 07
# MAGIC `reset_lab07()` drops every `lab07_*` table, view and function **except** the bronze tables and the targets
# MAGIC (pass `keep_bronze=False` to drop those too — they are recreated by the next `%run ./_07_prepare`).

# COMMAND ----------

# DBTITLE 1,Reset (optional)
# reset_lab07()
