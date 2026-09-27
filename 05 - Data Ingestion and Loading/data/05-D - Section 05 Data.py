# Databricks notebook source
# MAGIC %md
# MAGIC # 🗂️ 05-D · Data used in Section 05
# MAGIC
# MAGIC Section 05 is about **getting data in**, so it adds a variety of **source files** — different formats, a changing schema,
# MAGIC bad records, nested JSON, logs and images — to the course volume `…/shopwave/raw/`.
# MAGIC
# MAGIC | Folder | Format | Rows / files | Special feature | Used in |
# MAGIC |---|---|---|---|---|
# MAGIC | `returns-staging/` | CSV `,` + header | 5 × 40 rows + `manifest.txt` | batches 4–5 add **`refund_method`** | 05-L1 (`read_files`, `pathGlobFilter`), source for `land_returns()` |
# MAGIC | `returns-landing/` | CSV | grows 1 file per `land_returns()` | simulates daily delivery | 05-L2 `COPY INTO` |
# MAGIC | `returns-messy/` | CSV | 1 × 12 rows | 3 bad values (`n/a`, `12,50`, `31/07/2026`) | schema hints & `_rescued_data` |
# MAGIC | `reviews-json/` | JSON Lines | 2 × 30 reviews | nested objects & arrays, optional `photos` (20) / `verified_purchase` (6) | `VARIANT` |
# MAGIC | `reviews-multiline/` | JSON array (pretty-printed) | 1 × 5 reviews | needs `multiLine` | 05-L1 |
# MAGIC | `app-logs/` | text | 2 × 50 lines | `ts LEVEL event order=… ms=…` | `text` format, 05-L3 |
# MAGIC | `product-images/` | PNG | 6 images | binary files | `binaryFile` format |
# MAGIC | `supplier-staging/` → `supplier-landing/` | JSON Lines | 3 × 12 prices | nested `specs`; batch 3 adds **`discount`** | 05-L3 challenge |
# MAGIC | `files/fx-api/` | JSON | 1 file per API call | raw REST response | 05-L2 Part 8 |
# MAGIC
# MAGIC Tables created by the labs are all named **`lab05_*`** in `<catalog>.shopwave`.

# COMMAND ----------

# MAGIC %run ../../Includes/_setup

# COMMAND ----------

# MAGIC %run ../labs/_05_prepare

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔍 Preview the sources

# COMMAND ----------

# DBTITLE 1,Returns (clean) - schemas merged across batches
display(spark.sql(f"""
    SELECT _metadata.file_name AS file, count(*) AS rows, count(refund_method) AS with_method
    FROM read_files('{dataset_path}/returns-staging', format => 'csv', header => true, pathGlobFilter => '*.csv')
    GROUP BY ALL ORDER BY file"""))

# COMMAND ----------

# DBTITLE 1,Returns (messy) - the raw text
print(dbutils.fs.head(f"{dataset_path}/returns-messy/returns_bad.csv"))

# COMMAND ----------

# DBTITLE 1,Reviews - nested JSON
display(spark.read.json(f"{dataset_path}/reviews-json").limit(5))

# COMMAND ----------

# DBTITLE 1,Supplier feed - staged batches
for f in dbutils.fs.ls(f"{dataset_path}/supplier-staging"):
    print(f.name, f.size, "bytes")
print(dbutils.fs.head(f"{dataset_path}/supplier-staging/supplier_prices_03.json", 400))

# COMMAND ----------

# DBTITLE 1,Logs and images
display(spark.read.text(f"{dataset_path}/app-logs").limit(5))
display(spark.read.format("binaryFile").load(f"{dataset_path}/product-images").select("path", "length"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧹 Reset Section 05
# MAGIC Uncomment to drop every `lab05_*` table and reset both landing folders (returns → batch 01, supplier → batch 01).

# COMMAND ----------

# DBTITLE 1,Reset (optional)
# reset_lab05()
