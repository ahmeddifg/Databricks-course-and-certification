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
# MAGIC | `CHECK` constraints | added with `ALTER TABLE … ADD CONSTRAINT` | same — added afterwards |
# MAGIC | Generated / identity columns | ✅ declared up front | ❌ not possible with CTAS (create the table first, then `INSERT … SELECT`) |
# MAGIC | `COMMENT`, `TBLPROPERTIES`, `PARTITIONED BY`, `CLUSTER BY` | ✅ | ✅ (next cell) |

# COMMAND ----------

# MAGIC %md
# MAGIC CTAS accepts the same **table clauses** as `CREATE TABLE`. Here we partition a copy of the orders by month and look at the
# MAGIC result with `DESCRIBE DETAIL` (`partitionColumns`, `location`):

# COMMAND ----------

# DBTITLE 1,CTAS with PARTITIONED BY
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE lab03_orders_by_month
# MAGIC COMMENT 'Orders partitioned by month (demo)'
# MAGIC PARTITIONED BY (order_month)
# MAGIC AS SELECT *, date_format(order_ts, 'yyyy-MM') AS order_month FROM lab03_orders;

# COMMAND ----------

# DBTITLE 1,Where is it stored, and how is it partitioned?
_d = table_detail("lab03_orders_by_month")
print("location        :", _d["location"])
print("partitionColumns:", _d["partitionColumns"])
print("numFiles        :", _d["numFiles"])

# COMMAND ----------

# MAGIC %md
# MAGIC > 🎯 Managed tables live in **managed storage** chosen by Unity Catalog. Adding `LOCATION '<path>'` to a `CREATE TABLE`/CTAS
# MAGIC > makes an **external** table instead (it needs an *external location*, which Free Edition doesn't offer) → Section 04.
# MAGIC > Partitioning is shown for the exam; for new tables Databricks recommends **liquid clustering** (`CLUSTER BY`) → Section 12.

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
if v_first_insert is None:   # fallback: first version holding the history + one copy of batch 01
    _versions = sorted(r["version"] for r in spark.sql("DESCRIBE HISTORY lab03_orders").collect())
    v_first_insert = next(v for v in _versions
                          if spark.read.option("versionAsOf", v).table("lab03_orders").count() == 2010 + 121)
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
# MAGIC *(`orders` and `distinct_order_ids` differ by 11: the historical Parquet data already contains 10 exact duplicate rows and batch 01 one more —
# MAGIC cleaning duplicates **inside** source data is covered in Section 07.)*

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
# MAGIC Expected: **40 updated, 20 inserted** (see `num_updated_rows` / `num_inserted_rows` in the result). The condition `s.updated > t.updated` makes the MERGE **idempotent** — run it
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
# MAGIC WHEN MATCHED AND <cond> THEN UPDATE SET col = s.col, ...     -- or UPDATE SET *
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
print("   (a failed MERGE commits nothing - the table is unchanged)")
print("👉 Fix: deduplicate the source first (e.g. keep the latest row per key with row_number()).")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 5 · Clones
# MAGIC
# MAGIC | | `DEEP CLONE` | `SHALLOW CLONE` |
# MAGIC |---|---|---|
# MAGIC | Copies | data files **and** metadata | metadata / transaction log only — **points to the source's files** |
# MAGIC | Speed & cost | slower, more storage | instant, almost free |
# MAGIC | Independent of the source? | ✅ fully | shares the source's files — in **Unity Catalog** a `VACUUM` of the source doesn't break it (UC tracks the files clones still use); in the legacy Hive metastore it **does** break |
# MAGIC | Re-running it | `CREATE OR REPLACE … DEEP CLONE` incrementally syncs new changes | in UC you **can't** `CREATE OR REPLACE` a shallow clone — drop it and clone again |
# MAGIC | Typical use | backups, migrating tables | dev/test copies, experiments |
# MAGIC
# MAGIC In both cases **changes to the clone don't affect the source** (and vice versa).

# COMMAND ----------

# DBTITLE 1,Create a backup (deep) and a dev copy (shallow)
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE lab03_catalog_backup DEEP CLONE lab03_catalog;
# MAGIC
# MAGIC DROP TABLE IF EXISTS lab03_catalog_dev;              -- UC: shallow clones can't be CREATE OR REPLACEd
# MAGIC CREATE TABLE lab03_catalog_dev SHALLOW CLONE lab03_catalog;

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
# MAGIC   (data skipping). Unlike plain `OPTIMIZE`, Z-ordering is **not idempotent** — running it again can rewrite large parts of the
# MAGIC   data. New tables should prefer **liquid clustering**, which clusters **incrementally** (Section 12).
# MAGIC
# MAGIC ### `VACUUM` — physically deleting old files
# MAGIC Old files stay in storage so **time travel** keeps working. `VACUUM` deletes files that are **no longer referenced** by the
# MAGIC current version **and** were removed longer ago than the retention period (default **7 days**). The clock starts when a
# MAGIC file is *logically removed* by a commit (e.g. by `OPTIMIZE`), not when it was written.

# COMMAND ----------

# DBTITLE 1,What would VACUUM delete now? (DRY RUN)
# MAGIC %sql
# MAGIC VACUUM lab03_events DRY RUN

# COMMAND ----------

# MAGIC %md
# MAGIC Nothing (or almost nothing): the files replaced by `OPTIMIZE` were removed only minutes ago — less than 7 days — so they're protected.

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
    print("ℹ️ UNDROP by name didn't work here:", str(e).strip().splitlines()[0][:200])
    print("   (if several dropped tables share the name, use UNDROP TABLE WITH ID '<id from SHOW TABLES DROPPED>')")
    spark.sql("CREATE OR REPLACE TABLE lab03_catalog_backup DEEP CLONE lab03_catalog")   # recreate for the checks

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 8 · ✅ Automatic checks

# COMMAND ----------

# DBTITLE 1,Check your work
_merges = (spark.sql("DESCRIBE HISTORY lab03_customers").where("operation = 'MERGE'")
                .orderBy("version").collect())
_first_merge = dict(_merges[0]["operationMetrics"]) if _merges else {}
# A MERGE that changes nothing may or may not be recorded as a commit - both prove idempotency
_second_merge = dict(_merges[1]["operationMetrics"]) if len(_merges) > 1 else {"numTargetRowsUpdated": "0",
                                                                                "numTargetRowsInserted": "0"}
_checks = {
    "lab03_orders holds batch 01 exactly once (2,131 rows)": spark.table("lab03_orders").count() == 2010 + 121,
    "lab03_orders has 2,120 distinct orders": spark.sql(
        "SELECT count(DISTINCT order_id) FROM lab03_orders").first()[0] == 2000 + 120,
    "lab03_orders_by_month is partitioned by order_month": list(
        table_detail("lab03_orders_by_month")["partitionColumns"]) == ["order_month"],
    "RESTORE is in lab03_orders history": {"RESTORE"} <= {
        r["operation"] for r in spark.sql("DESCRIBE HISTORY lab03_orders").collect()},
    "First MERGE updated 40 customers": _first_merge.get("numTargetRowsUpdated") == "40",
    "First MERGE inserted 20 customers": _first_merge.get("numTargetRowsInserted") == "20",
    "Second MERGE changed nothing (idempotent)": _second_merge.get("numTargetRowsUpdated") == "0"
                                                 and _second_merge.get("numTargetRowsInserted") == "0",
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
