# Databricks notebook source
# MAGIC %md
# MAGIC # 🧪 Lab 03-L2 · Writing, Merging & Maintaining Delta Tables (Guided)
# MAGIC **Time:** ~60 min · **Compute:** Serverless (Part 6 shows one extra step that only works on classic compute)
# MAGIC
# MAGIC | Part | You will… |
# MAGIC |---|---|
# MAGIC | 1 | Create tables with **CTAS** (comment, properties) |
# MAGIC | 2 | Compare `CREATE OR REPLACE`, `INSERT OVERWRITE` and `INSERT INTO` — and see why a blind append isn't idempotent |
# MAGIC | 3 | **Upsert** customer changes with `MERGE` (and prove it's idempotent) |
# MAGIC | 4 | **Deduplicate** on load with an insert-only `MERGE`; see the *multiple source rows* error |
# MAGIC | 5 | **Clone** tables: deep vs shallow |
# MAGIC | 6 | Fix the **small-files problem** with `OPTIMIZE … ZORDER BY`; understand `VACUUM` |
# MAGIC | 7 | `DROP TABLE` and `UNDROP TABLE` |
# MAGIC | 8 | ✅ Automatic checks |

# COMMAND ----------

# MAGIC %run ../../Includes/_setup

# COMMAND ----------

# MAGIC %run ./_03_prepare

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1 · CTAS — Create Table As Select
# MAGIC CTAS creates **and fills** a table in one statement; the schema comes from the query (cast in the `SELECT` to control types).

# COMMAND ----------

# DBTITLE 1,Source files as temporary views
spark.read.parquet(f"{dataset_path}/orders-parquet").createOrReplaceTempView("orders_parquet_v")
spark.read.json(f"{dataset_path}/customers-json").createOrReplaceTempView("customers_json_v")
spark.read.json(f"{dataset_path}/customers-updates-json").createOrReplaceTempView("customers_updates_json_v")
(spark.read.option("header", "true").option("delimiter", ";").option("inferSchema", "true")
      .csv(f"{dataset_path}/products-csv").createOrReplaceTempView("products_csv_v"))
(spark.read.option("header", "true").option("delimiter", ";").option("inferSchema", "true")
      .csv(f"{dataset_path}/products-new-csv").createOrReplaceTempView("products_new_v"))
# JSON batch with the SAME schema as the Parquet orders (ORDER_SCHEMA comes from Includes/_setup)
spark.read.schema(ORDER_SCHEMA).json(f"{dataset_path}/orders-staging/01.json").createOrReplaceTempView("orders_batch01_v")
print("Temporary views ready")

# COMMAND ----------

# DBTITLE 1,CTAS → lab03_orders (version 0)
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS lab03_orders;
# MAGIC
# MAGIC CREATE TABLE lab03_orders
# MAGIC COMMENT 'Historical ShopWave orders loaded with CTAS'
# MAGIC TBLPROPERTIES ('shopwave.layer' = 'bronze')
# MAGIC AS SELECT order_id,
# MAGIC           timestamp_seconds(order_timestamp) AS order_ts,
# MAGIC           customer_id, quantity, total, items
# MAGIC    FROM orders_parquet_v;
# MAGIC
# MAGIC SELECT count(*) AS orders FROM lab03_orders;

# COMMAND ----------

# DBTITLE 1,Check the result
# MAGIC %sql
# MAGIC DESCRIBE EXTENDED lab03_orders

# COMMAND ----------

# MAGIC %md
# MAGIC | | `CREATE TABLE t (col TYPE, …)` | `CREATE TABLE t AS SELECT …` (CTAS) |
# MAGIC |---|---|---|
# MAGIC | Schema | Declared by you | Inferred from the query |
# MAGIC | Data | Empty — load with `INSERT` | Filled immediately |
# MAGIC | Constraints / generated columns | ✅ declared up front | Add afterwards with `ALTER TABLE` |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2 · Replace vs overwrite vs append

# COMMAND ----------

# DBTITLE 1,CREATE OR REPLACE TABLE → version 1 (history is kept)
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE lab03_orders AS
# MAGIC SELECT order_id, timestamp_seconds(order_timestamp) AS order_ts, customer_id, quantity, total, items
# MAGIC FROM orders_parquet_v;

# COMMAND ----------

# DBTITLE 1,INSERT OVERWRITE → version 2 (same schema, data replaced)
# MAGIC %sql
# MAGIC INSERT OVERWRITE lab03_orders
# MAGIC SELECT order_id, timestamp_seconds(order_timestamp) AS order_ts, customer_id, quantity, total, items
# MAGIC FROM orders_parquet_v;

# COMMAND ----------

# DBTITLE 1,❌ INSERT OVERWRITE cannot change the schema
try:
    spark.sql("""
        INSERT OVERWRITE lab03_orders
        SELECT order_id, timestamp_seconds(order_timestamp) AS order_ts, customer_id, quantity, total, items,
               current_timestamp() AS loaded_at
        FROM orders_parquet_v""")
    print("Overwritten (unexpected!)")
except Exception as e:
    print("🚫 INSERT OVERWRITE with an extra column fails:\n   ", str(e).strip().splitlines()[0][:220])
print("👉 To change the schema, use CREATE OR REPLACE TABLE (or overwrite with overwriteSchema=true).")

# COMMAND ----------

# DBTITLE 1,History so far
# MAGIC %sql
# MAGIC DESCRIBE HISTORY lab03_orders

# COMMAND ----------

# MAGIC %md
# MAGIC **Append with `INSERT INTO`** — we load the first July batch (121 orders)…

# COMMAND ----------

# DBTITLE 1,INSERT INTO → version 3
# MAGIC %sql
# MAGIC INSERT INTO lab03_orders
# MAGIC SELECT order_id, timestamp_seconds(order_timestamp), customer_id, quantity, total, items
# MAGIC FROM orders_batch01_v;
# MAGIC
# MAGIC SELECT count(*) AS orders FROM lab03_orders;

# COMMAND ----------

# MAGIC %md
# MAGIC …and now the job is **retried** after a network glitch, running the same `INSERT INTO` again:

# COMMAND ----------

# DBTITLE 1,Retry the same append → version 4 😬
# MAGIC %sql
# MAGIC INSERT INTO lab03_orders
# MAGIC SELECT order_id, timestamp_seconds(order_timestamp), customer_id, quantity, total, items
# MAGIC FROM orders_batch01_v;
# MAGIC
# MAGIC SELECT count(*) AS orders, count(DISTINCT order_id) AS distinct_order_ids FROM lab03_orders;

# COMMAND ----------

# MAGIC %md
# MAGIC The batch was loaded **twice** — `INSERT INTO` is **not idempotent**. Undo it with `RESTORE`, then load the batch the
# MAGIC **idempotent** way: an insert-only `MERGE` that skips orders that already exist.

# COMMAND ----------

# DBTITLE 1,Undo the duplicate load
# INSERT INTO is logged as operation WRITE with mode Append (INSERT OVERWRITE is WRITE with mode Overwrite)
v_first_insert = (spark.sql("DESCRIBE HISTORY lab03_orders")
                       .where("operation = 'WRITE' AND operationParameters['mode'] = 'Append'")
                       .agg({"version": "min"}).first()[0])
spark.sql(f"RESTORE TABLE lab03_orders TO VERSION AS OF {v_first_insert}")
print(f"Restored to version {v_first_insert} →", spark.table("lab03_orders").count(), "orders")

# COMMAND ----------

# DBTITLE 1,Idempotent load: MERGE … WHEN NOT MATCHED THEN INSERT (run it twice!)
# MAGIC %sql
# MAGIC MERGE INTO lab03_orders AS t
# MAGIC USING (SELECT order_id, timestamp_seconds(order_timestamp) AS order_ts, customer_id, quantity, total, items
# MAGIC        FROM orders_batch01_v) AS s
# MAGIC ON t.order_id = s.order_id
# MAGIC WHEN NOT MATCHED THEN INSERT *

# COMMAND ----------

# MAGIC %md
# MAGIC The result shows `num_inserted_rows = 0`: every order of batch 01 is already there, so nothing is duplicated. Run the cell again — still 0.
# MAGIC *(Batch 01 contains one exact duplicate row, which `distinct_order_ids` below reveals — cleaning duplicates **inside** a source batch is covered in Section 07.)*

# COMMAND ----------

# DBTITLE 1,Still no duplicates from the retry
# MAGIC %sql
# MAGIC SELECT count(*) AS orders, count(DISTINCT order_id) AS distinct_order_ids FROM lab03_orders

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 3 · Upsert with `MERGE`
# MAGIC The CRM sends a file of customer changes: **40 existing customers changed** (new email / city) and **20 are new**.

# COMMAND ----------

# DBTITLE 1,Target table: current customers
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS lab03_customers;
# MAGIC
# MAGIC CREATE TABLE lab03_customers AS
# MAGIC SELECT customer_id,
# MAGIC        email,
# MAGIC        profile:first_name        AS first_name,
# MAGIC        profile:last_name         AS last_name,
# MAGIC        profile:address:city      AS city,
# MAGIC        profile:address:country   AS country,
# MAGIC        CAST(updated AS TIMESTAMP) AS updated
# MAGIC FROM customers_json_v;

# COMMAND ----------

# DBTITLE 1,Source: the change feed, same shape
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW customer_updates_v AS
# MAGIC SELECT customer_id,
# MAGIC        email,
# MAGIC        profile:first_name        AS first_name,
# MAGIC        profile:last_name         AS last_name,
# MAGIC        profile:address:city      AS city,
# MAGIC        profile:address:country   AS country,
# MAGIC        CAST(updated AS TIMESTAMP) AS updated
# MAGIC FROM customers_updates_json_v;
# MAGIC
# MAGIC SELECT count(*) AS changes FROM customer_updates_v;

# COMMAND ----------

# DBTITLE 1,MERGE (upsert)
# MAGIC %sql
# MAGIC MERGE INTO lab03_customers AS t
# MAGIC USING customer_updates_v AS s
# MAGIC ON t.customer_id = s.customer_id
# MAGIC WHEN MATCHED AND s.updated > t.updated THEN
# MAGIC   UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT *

# COMMAND ----------

# MAGIC %md
# MAGIC Expected: **40 updated, 20 inserted**. The condition `s.updated > t.updated` makes the MERGE **idempotent** — run it
# MAGIC again and nothing changes because the target is already up to date:

# COMMAND ----------

# DBTITLE 1,Run the same MERGE again
# MAGIC %sql
# MAGIC MERGE INTO lab03_customers AS t
# MAGIC USING customer_updates_v AS s
# MAGIC ON t.customer_id = s.customer_id
# MAGIC WHEN MATCHED AND s.updated > t.updated THEN
# MAGIC   UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT *

# COMMAND ----------

# DBTITLE 1,MERGE metrics from the history
_m = F.col("operationMetrics")
display(history("lab03_customers").where("operation = 'MERGE'")
        .select("version",
                _m["numSourceRows"].alias("source_rows"),
                _m["numTargetRowsUpdated"].alias("updated"),
                _m["numTargetRowsInserted"].alias("inserted")))

# COMMAND ----------

# MAGIC %md
# MAGIC ### MERGE clause cheat sheet
# MAGIC ```sql
# MAGIC MERGE INTO target t USING source s ON t.key = s.key
# MAGIC WHEN MATCHED AND <cond> THEN UPDATE SET t.col = s.col, ...   -- or UPDATE SET *
# MAGIC WHEN MATCHED AND s.op = 'DELETE' THEN DELETE
# MAGIC WHEN NOT MATCHED [BY TARGET] THEN INSERT *                   -- or INSERT (cols) VALUES (...)
# MAGIC WHEN NOT MATCHED BY SOURCE THEN DELETE                        -- rows in target missing from source
# MAGIC ```
# MAGIC A classic variant (from many exam questions): only fill in missing emails —
# MAGIC `WHEN MATCHED AND t.email IS NULL AND s.email IS NOT NULL THEN UPDATE SET email = s.email, updated = s.updated`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 4 · Insert-only MERGE for deduplication
# MAGIC The supplier re-sends products we already have (`P005, P012, P020`) together with 3 new ones.

# COMMAND ----------

# DBTITLE 1,Catalog table (CTAS)
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE lab03_catalog AS
# MAGIC SELECT product_id, title, brand, category, CAST(price AS DOUBLE) AS price FROM products_csv_v;

# COMMAND ----------

# DBTITLE 1,Insert only what's new
# MAGIC %sql
# MAGIC MERGE INTO lab03_catalog AS t
# MAGIC USING (SELECT product_id, title, brand, category, CAST(price AS DOUBLE) AS price FROM products_new_v) AS s
# MAGIC ON t.product_id = s.product_id
# MAGIC WHEN NOT MATCHED THEN INSERT *

# COMMAND ----------

# DBTITLE 1,36 + 3 = 39 products
# MAGIC %sql
# MAGIC SELECT count(*) AS products FROM lab03_catalog

# COMMAND ----------

# MAGIC %md
# MAGIC ### ⚠️ A classic MERGE failure: duplicate keys in the source
# MAGIC If **two source rows match the same target row**, MERGE can't decide which one wins and fails.

# COMMAND ----------

# DBTITLE 1,❌ Duplicate source keys
one = spark.table("customer_updates_v").orderBy("customer_id").limit(1)
one.unionAll(one).createOrReplaceTempView("dup_updates_v")
try:
    spark.sql("""
        MERGE INTO lab03_customers t USING dup_updates_v s ON t.customer_id = s.customer_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *""")
    print("Merged (unexpected!)")
except Exception as e:
    print("🚫", str(e).strip().splitlines()[0][:230])
print("👉 Fix: deduplicate the source first (e.g. keep the latest row per key with row_number()).")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 5 · Clones
# MAGIC
# MAGIC | | `DEEP CLONE` | `SHALLOW CLONE` |
# MAGIC |---|---|---|
# MAGIC | Copies | data files **and** metadata | metadata / transaction log only — **points to the source's files** |
# MAGIC | Speed & cost | slower, more storage | instant, almost free |
# MAGIC | Independent of source files? | ✅ yes | ❌ breaks if the source's files are vacuumed |
# MAGIC | Re-running it | incrementally syncs new changes | re-creates the pointer |
# MAGIC | Typical use | backups, migrating tables | dev/test copies, experiments |
# MAGIC
# MAGIC In both cases **changes to the clone don't affect the source** (and vice versa).

# COMMAND ----------

# DBTITLE 1,Create a backup (deep) and a dev copy (shallow)
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE lab03_catalog_backup DEEP CLONE lab03_catalog;
# MAGIC CREATE OR REPLACE TABLE lab03_catalog_dev SHALLOW CLONE lab03_catalog;

# COMMAND ----------

# DBTITLE 1,Experiment on the dev copy
# MAGIC %sql
# MAGIC UPDATE lab03_catalog_dev SET price = round(price * 0.5, 2) WHERE category = 'Books';
# MAGIC
# MAGIC SELECT 'source' AS table, round(avg(price), 2) AS avg_book_price FROM lab03_catalog     WHERE category = 'Books'
# MAGIC UNION ALL
# MAGIC SELECT 'dev clone',       round(avg(price), 2)                    FROM lab03_catalog_dev WHERE category = 'Books'

# COMMAND ----------

# DBTITLE 1,The clone's history starts with a CLONE operation
display(history("lab03_catalog_dev").select("version", "operation", "operationParameters"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 6 · Small files, `OPTIMIZE`, `ZORDER` and `VACUUM`
# MAGIC Every write adds at least one file. Many small writes (think: a job inserting every few minutes) → **many small files**
# MAGIC → slow reads. We simulate 12 small appends:

# COMMAND ----------

# DBTITLE 1,Create a table with many small files
spark.sql("DROP TABLE IF EXISTS lab03_events")
spark.sql("CREATE TABLE lab03_events (event_id BIGINT, customer_id STRING, amount DOUBLE)")
for batch in range(12):
    spark.sql(f"""
        INSERT INTO lab03_events
        SELECT id + {batch * 1000} AS event_id,
               concat('C', lpad(CAST(id % 300 + 1 AS STRING), 4, '0')) AS customer_id,
               round(rand() * 100, 2) AS amount
        FROM range(1000)""")
files_before = num_files("lab03_events")
print("Files before OPTIMIZE:", files_before)

# COMMAND ----------

# MAGIC %md
# MAGIC > ℹ️ If you see fewer than 12 files, **auto compaction** / **predictive optimization** already merged some — Databricks does
# MAGIC > this automatically for Unity Catalog managed tables. `OPTIMIZE` is still the command the exam asks about.

# COMMAND ----------

# DBTITLE 1,OPTIMIZE with Z-ordering
# MAGIC %sql
# MAGIC OPTIMIZE lab03_events ZORDER BY (customer_id)

# COMMAND ----------

# DBTITLE 1,Files after OPTIMIZE
files_after = num_files("lab03_events")
print(f"Files: {files_before} → {files_after}")
print("OPTIMIZE metrics:", {k: v for k, v in last_operation_metrics("lab03_events", "OPTIMIZE").items()
                            if k in ("numRemovedFiles", "numAddedFiles", "numRemovedBytes", "numAddedBytes")})

# COMMAND ----------

# MAGIC %md
# MAGIC * **`OPTIMIZE`** = **bin-packing**: rewrites many small files into fewer large ones (target ≈ 1 GB). It's a new version in the history; the old
# MAGIC   small files are only *logically* removed.
# MAGIC * **`ZORDER BY (col)`** = also **co-locates** similar values of `col` in the same files, so filters on `col` skip more files
# MAGIC   (data skipping). It is **not incremental** (re-sorts all data each time). New tables should prefer **liquid clustering** (Section 12).
# MAGIC
# MAGIC ### `VACUUM` — physically deleting old files
# MAGIC Old files stay in storage so **time travel** keeps working. `VACUUM` deletes files that are **no longer referenced** by the
# MAGIC current version **and** are older than the retention period (default **7 days**).

# COMMAND ----------

# DBTITLE 1,What would VACUUM delete now? (DRY RUN)
# MAGIC %sql
# MAGIC VACUUM lab03_events DRY RUN

# COMMAND ----------

# MAGIC %md
# MAGIC Nothing (or almost nothing): the files replaced by `OPTIMIZE` are younger than 7 days, so they're protected.

# COMMAND ----------

# DBTITLE 1,❌ The retention safety check
try:
    spark.sql("VACUUM lab03_events RETAIN 0 HOURS")
    print("Vacuumed with 0 hours retention (the safety check is disabled on this compute)")
except Exception as e:
    print("🚫 Blocked by the retention safety check:\n   ", str(e).strip().splitlines()[0][:230])

# COMMAND ----------

# MAGIC %md
# MAGIC On **classic** compute you can disable the check with `SET spark.databricks.delta.retentionDurationCheck.enabled = false`
# MAGIC (**never do this in production** — running readers/writers may still need those files). On **serverless** this setting
# MAGIC can't be changed.
# MAGIC
# MAGIC > 🎯 **Exam essentials:** `VACUUM table [RETAIN n HOURS] [DRY RUN]` · default retention 7 days (`delta.deletedFileRetentionDuration`) ·
# MAGIC > after VACUUM you **cannot time travel** to versions whose files were deleted · VACUUM does **not** delete the transaction log
# MAGIC > (log entries are cleaned separately after `delta.logRetentionDuration`, default 30 days).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 7 · `DROP TABLE` and `UNDROP TABLE`
# MAGIC Dropping a **managed** table deletes its data too — but Unity Catalog keeps dropped tables recoverable for **7 days**.

# COMMAND ----------

# DBTITLE 1,Drop, list dropped tables, undrop
spark.sql("DROP TABLE IF EXISTS lab03_catalog_backup")
print("Exists after DROP?", spark.catalog.tableExists("lab03_catalog_backup"))
try:
    display(spark.sql("SHOW TABLES DROPPED"))
    spark.sql("UNDROP TABLE lab03_catalog_backup")
    print("♻️ UNDROP worked - exists again?", spark.catalog.tableExists("lab03_catalog_backup"))
except Exception as e:
    print("ℹ️ UNDROP not available here:", str(e).strip().splitlines()[0][:200])
    spark.sql("CREATE OR REPLACE TABLE lab03_catalog_backup DEEP CLONE lab03_catalog")   # recreate for the checks

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 8 · ✅ Automatic checks

# COMMAND ----------

# DBTITLE 1,Check your work
_merge = last_operation_metrics("lab03_customers", "MERGE")
_merges = (spark.sql("DESCRIBE HISTORY lab03_customers").where("operation = 'MERGE'")
                .orderBy("version").collect())
_first_merge = dict(_merges[0]["operationMetrics"]) if _merges else {}
_checks = {
    "lab03_orders has no duplicate order loads": spark.sql(
        "SELECT count(DISTINCT order_id) FROM lab03_orders").first()[0] == 2000 + 120,
    "RESTORE is in lab03_orders history": {"RESTORE"} <= {
        r["operation"] for r in spark.sql("DESCRIBE HISTORY lab03_orders").collect()},
    "First MERGE updated 40 customers": _first_merge.get("numTargetRowsUpdated") == "40",
    "First MERGE inserted 20 customers": _first_merge.get("numTargetRowsInserted") == "20",
    "Second MERGE changed nothing (idempotent)": _merge.get("numTargetRowsUpdated") == "0"
                                                 and _merge.get("numTargetRowsInserted") == "0",
    "lab03_customers now has 320 customers": spark.table("lab03_customers").count() == 320,
    "lab03_catalog has 39 products": spark.table("lab03_catalog").count() == 39,
    "Shallow clone change didn't touch the source": spark.sql(
        "SELECT count(*) FROM lab03_catalog c JOIN lab03_catalog_dev d USING (product_id) "
        "WHERE c.category = 'Books' AND c.price = d.price").first()[0] == 0,
    "OPTIMIZE did not increase the number of files": files_after <= files_before,
    "Backup table exists": spark.catalog.tableExists("lab03_catalog_backup"),
}
for name, ok in _checks.items():
    print(("✅ " if ok else "❌ ") + name)
print("\n🎉 Lab complete - next: 03-L3 · Challenge Lab")
