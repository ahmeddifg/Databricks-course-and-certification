# Databricks notebook source
# MAGIC %md
# MAGIC # 🗂️ 08-D · Data used in Section 08
# MAGIC
# MAGIC Section 08 builds the **same kind of medallion pipeline twice** — by hand with Structured Streaming (08-L1) and
# MAGIC declaratively with Lakeflow Spark Declarative Pipelines (08-L2 … 08-L4). Every feed has a **staging** folder (all files,
# MAGIC never read directly) and a **landing** folder under `…/shopwave/raw/lab08/` that a helper fills **one file at a time**.
# MAGIC
# MAGIC | Feed | Staging → landing | Files × rows | Special features | Used in |
# MAGIC |---|---|---|---|---|
# MAGIC | `hc_orders` | `orders-staging/` → `lab08/hc-orders/` | 10 × 121 JSON orders | per file: 1 cancelled (`quantity = 0`), 1 unknown customer `C9999`, 1 exact duplicate | 08-L1 |
# MAGIC | `sdp_orders` | `orders-staging/` → `lab08/orders/` | same files | + `zz_bad_order.json` (no `order_id`) written by 08-L2 Part 6 | 08-L2 |
# MAGIC | `cdc` | `customers-cdc-staging/` → `lab08/customers-cdc/` | 300 / 40 / 20 change events | `INSERT`/`UPDATE`/`DELETE` + `sequence_num`; file 02: 3 deletes, 5 new customers, 2 invalid `MERGE`s; file 03: 4 **late** updates | 08-L3 |
# MAGIC | `ratings` | `ratings-staging/` → `lab08/ratings/` | 60 / 63 / 60 JSON ratings | invalid ratings (0, 6, 10), missing / unknown (`P999`) products, empty comments, 3 re-sent ratings in file 02, new column `helpful_votes` in file 03 | 08-L4 |
# MAGIC
# MAGIC Also used: `customers-json/` and `products-csv/` (course base data) as dimensions.
# MAGIC
# MAGIC | Pipeline (created in the labs) | Source folder (`labs/pipelines/…`) | Publishes |
# MAGIC |---|---|---|
# MAGIC | `08-L2 ShopWave orders (SQL)` | `shopwave_sql/transformations/*.sql` | `sdp_*` |
# MAGIC | `08-L3 ShopWave customers CDC (Python)` | `shopwave_cdc_python/transformations/customers_cdc.py` | `cdc_*` |
# MAGIC | `08-L4 Ratings challenge` | `ratings_challenge/` (your code) · reference: `ratings_solution/`, `ratings_solution_final/` | `ch08_*` |
# MAGIC
# MAGIC 08-L1 tables are named `lab08_hc_*`; their checkpoints live in `…/shopwave/checkpoints/lab08/`.

# COMMAND ----------

# MAGIC %run ../../Includes/_setup

# COMMAND ----------

# MAGIC %run ../labs/_08_prepare

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔍 Preview the sources

# COMMAND ----------

# DBTITLE 1,Customer change events - operations per file
display(spark.read.json(f"{dataset_path}/customers-cdc-staging")
             .select("*", F.col("_metadata.file_name").alias("file"))
             .groupBy("file").pivot("operation").count().orderBy("file"))

# COMMAND ----------

# DBTITLE 1,Ratings - rows and problems per file
display(spark.read.json(f"{dataset_path}/ratings-staging")
             .select("*", F.col("_metadata.file_name").alias("file"))
             .groupBy("file")
             .agg(F.count("*").alias("rows"),
                  F.sum(F.when(~F.col("rating").between(1, 5), 1).otherwise(0)).alias("invalid_rating"),
                  F.sum(F.when(F.col("product_id").isNull(), 1).otherwise(0)).alias("no_product"),
                  F.sum(F.when(F.col("product_id") == "P999", 1).otherwise(0)).alias("unknown_product"),
                  F.sum(F.when(F.col("comment") == "", 1).otherwise(0)).alias("empty_comment"),
                  F.count("helpful_votes").alias("with_helpful_votes"))
             .orderBy("file"))

# COMMAND ----------

# DBTITLE 1,What is landed right now? Which lab pipelines exist?
for feed in ("hc_orders", "sdp_orders", "cdc", "ratings"):
    print(f"{feed:<11} {landing(feed).replace(dataset_path, '…/raw')}: {landed(feed)}")
for key, p in PIPELINES08.items():
    try:
        found = find_pipeline(key)
        print(f"pipeline {p['name']:<40} {'✅ ' + str(found[1]) if found else '— not created'}")
    except Exception as e:
        print("pipeline lookup not available:", _first_line(e))
        break

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧹 Reset Section 08
# MAGIC Uncomment to delete the three lab pipelines (and their `sdp_*`, `cdc_*`, `ch08_*` tables), drop the `lab08_*` tables,
# MAGIC delete the `lab08` checkpoints and empty every `lab08/` landing folder. Your pipeline **code** (files in the repo) is not
# MAGIC touched.

# COMMAND ----------

# DBTITLE 1,Reset (optional)
# reset_lab08(confirm="YES")
