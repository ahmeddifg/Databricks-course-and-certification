# Databricks notebook source
# MAGIC %md
# MAGIC # 🧪 Lab 12-L3 · Data Layout — Partitioning, Z-order and Liquid Clustering (Guided)
# MAGIC **Time:** ~45 min · **Compute:** Serverless notebook · **Works on Free Edition**
# MAGIC
# MAGIC The ShopWave dashboards filter `perf_events` by **customer** and by **date**. How many files does Delta have to open for
# MAGIC those filters — and how do you get that number down? You'll measure **data skipping** for every layout.
# MAGIC
# MAGIC | Part | You will… |
# MAGIC |---|---|
# MAGIC | 1 | Create the **small-files** problem and fix it with `OPTIMIZE` (idempotent) |
# MAGIC | 2 | **Partition** by date: pruning works… and the files get tiny |
# MAGIC | 3 | **Z-order** by customer: skipping works… and every run rewrites everything |
# MAGIC | 4 | **Liquid clustering**: `CLUSTER BY`, incremental `OPTIMIZE`, change keys, `OPTIMIZE FULL`, `CLUSTER BY AUTO` |
# MAGIC | 5 | Prove the **incompatibilities** (ZORDER / PARTITIONED BY on a clustered table) |
# MAGIC | 6 | **Predictive optimization**: where it's enabled, how to change it, where its history is |
# MAGIC | 7 | 🖱️ Confirm data skipping in the **query profile** |
# MAGIC
# MAGIC **How we measure.** `skipping_report(table, column, value)` groups the table's rows by their data file
# MAGIC (`_metadata.file_path`), computes **min/max of the column per file** — the statistics Delta keeps in its transaction log — and
# MAGIC counts the files whose range can contain the value. That is exactly the test Delta's **data skipping** does before reading.
# MAGIC
# MAGIC > 📏 To see several files on a small (~50 MB) table, the demo tables set `delta.targetFileSize = '4mb'` (production tables
# MAGIC > use 256 MB – 1 GB, auto-tuned). File counts can differ a little between runs and runtimes — the checks compare layouts,
# MAGIC > not exact numbers.

# COMMAND ----------

# MAGIC %run ../../Includes/_setup

# COMMAND ----------

# MAGIC %run ./_12_prepare

# COMMAND ----------

# DBTITLE 1,Start clean (safe to re-run)
for _t in ("perf_small", "perf_part_date", "perf_zorder", "perf_liquid", "perf_auto", "perf_conflict", "perf_liquid_df"):
    spark.sql(f"DROP TABLE IF EXISTS {_t}")
NEW_DAY = "SELECT event_id + 10000000 AS event_id, event_ts, event_date, customer_id, product_id, event_type, device, amount " \
          "FROM perf_events WHERE event_date = DATE'2026-06-30'"        # one more day of events, new ids
print("ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1 · Small files and OPTIMIZE
# MAGIC A chatty writer (micro-batches, row-by-row inserts) produces one small file per write. `make_small_files` runs **30 tiny
# MAGIC INSERTs** (auto compaction switched off for the demo).

# COMMAND ----------

# DBTITLE 1,30 small inserts
make_small_files("perf_small", batches=30)
_before = layout("perf_small")

# COMMAND ----------

# DBTITLE 1,OPTIMIZE (bin-packing) - and run it twice
_opt1 = optimize("perf_small")
_after = layout("perf_small")
_opt2 = optimize("perf_small")             # nothing left to compact -> nothing rewritten (idempotent)
display(history("perf_small", 5))

# COMMAND ----------

# DBTITLE 1,✅ Check Part 1
check("30 inserts left at least 25 files", _before["files"] >= 25)
check("OPTIMIZE compacted them into at most 3 files", _after["files"] <= 3)
check("a second OPTIMIZE rewrote nothing (idempotent)", _opt2["removed"] == 0)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2 · Partitioning by date
# MAGIC `event_date` has 181 values. Partition by it and look at the files.

# COMMAND ----------

# DBTITLE 1,PARTITIONED BY (event_date)
spark.sql("CREATE TABLE perf_part_date PARTITIONED BY (event_date) AS SELECT * FROM perf_events")
_part = layout("perf_part_date")
print("partitions:", spark.sql("SELECT count(DISTINCT event_date) FROM perf_part_date").first()[0])
skipping_report("perf_part_date", "event_date", "2026-03-01")
skipping_report("perf_part_date", "customer_id", "C0123")

# COMMAND ----------

# MAGIC %md
# MAGIC * **Partition pruning** works great for a date filter: only that folder's files are read.
# MAGIC * But a filter on **customer** reads every file.
# MAGIC * And look at the **average file size**: ~50 MB spread over 181 folders → files of a few hundred KB (rule of thumb: a
# MAGIC   partition should hold **≥ 1 GB**; don't partition tables under ~1 TB). Partition by `customer_id` (high cardinality) and it
# MAGIC   gets worse.

# COMMAND ----------

# DBTITLE 1,✅ Check Part 2
check("one folder per day: at least 181 files", _part["files"] >= 181)
check("files are tiny (average < 1 MB)", _part["avg_file_mb"] < 1)
_h, _tot = files_scanned("perf_part_date", "event_date", "2026-03-01")
check("a one-day filter reads under 2 % of the files", _h / _tot < 0.02)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 3 · Z-order by customer

# COMMAND ----------

# DBTITLE 1,Unclustered copy - how many files for one customer?
spark.sql("CREATE TABLE perf_zorder TBLPROPERTIES ('delta.targetFileSize' = '4mb') AS SELECT * FROM perf_events")
layout("perf_zorder")
_z0 = skipping_report("perf_zorder", "customer_id", "C0123")

# COMMAND ----------

# DBTITLE 1,OPTIMIZE ... ZORDER BY (customer_id)
_zo1 = optimize("perf_zorder", "ZORDER BY (customer_id)")
_zl = layout("perf_zorder")
_z1 = skipping_report("perf_zorder", "customer_id", "C0123")
display(file_ranges("perf_zorder", "customer_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC After Z-order each file holds a narrow range of customers (look at `min_value` / `max_value`) — except the hot customer
# MAGIC `C0007`, which fills several files on its own. Now **one more day** of data arrives, and you Z-order again:

# COMMAND ----------

# DBTITLE 1,New data + Z-order again
spark.sql(f"INSERT INTO perf_zorder {NEW_DAY}")
skipping_report("perf_zorder", "customer_id", "C0123")      # the new file contains all customers again
_zo2 = optimize("perf_zorder", "ZORDER BY (customer_id)")
_zl2 = layout("perf_zorder")

# COMMAND ----------

# MAGIC %md
# MAGIC The second Z-order **rewrote the whole table** again (compare *rewrote N files* with the number of files) — for one small new
# MAGIC file. Z-order isn't stored in the table: new data lands unclustered, and every `ZORDER` run re-sorts the whole partition
# MAGIC (here: the whole unpartitioned table).

# COMMAND ----------

# DBTITLE 1,✅ Check Part 3
check("before Z-order a customer filter reads every file", _z0[0] == _z0[1])
if _z1[1] < 3:
    print("ℹ️ OPTIMIZE wrote fewer than 3 files on your runtime - too few to show skipping; compare file_ranges() instead")
check("after Z-order it reads at most half of the files", _z1[1] < 3 or _z1[0] <= _z1[1] / 2)
check("the second Z-order rewrote (almost) every file of the table", _zo2["removed"] >= 0.8 * _zl["files"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 4 · Liquid clustering

# COMMAND ----------

# DBTITLE 1,CREATE TABLE ... CLUSTER BY
# MAGIC %sql
# MAGIC CREATE TABLE perf_liquid
# MAGIC CLUSTER BY (customer_id)
# MAGIC TBLPROPERTIES ('delta.targetFileSize' = '4mb')
# MAGIC COMMENT 'Section 12: liquid clustering demo'
# MAGIC AS SELECT * FROM perf_events

# COMMAND ----------

# DBTITLE 1,Layout, OPTIMIZE (clusters the data), skipping
layout("perf_liquid")
skipping_report("perf_liquid", "customer_id", "C0123")
_lo1 = optimize("perf_liquid")
_ll = layout("perf_liquid")
_l1 = skipping_report("perf_liquid", "customer_id", "C0123")

# COMMAND ----------

# MAGIC %md
# MAGIC Small writes are only clustered **on write** above a size threshold, so the CTAS may not be clustered yet — `OPTIMIZE`
# MAGIC clusters it (and predictive optimization would do it for you). Now the same new day as in Part 3:

# COMMAND ----------

# DBTITLE 1,New data + OPTIMIZE again = incremental
spark.sql(f"INSERT INTO perf_liquid {NEW_DAY}")
_lo2 = optimize("perf_liquid")
print(f"Z-order rewrote {_zo2['removed']} files for the new day · liquid clustering rewrote {_lo2['removed']}")
skipping_report("perf_liquid", "customer_id", "C0123")

# COMMAND ----------

# MAGIC %md
# MAGIC **Incremental**: liquid clustering only rewrites the data that isn't clustered yet. The keys are part of the table:

# COMMAND ----------

# DBTITLE 1,Change the clustering keys - no rewrite
_files_before_alter = layout("perf_liquid", verbose=False)["files"]
spark.sql("ALTER TABLE perf_liquid CLUSTER BY (customer_id, event_date)")
_after_alter = layout("perf_liquid")
display(history("perf_liquid", 6))

# COMMAND ----------

# MAGIC %md
# MAGIC `ALTER TABLE … CLUSTER BY` only changed metadata — the same files are still there (history: a `CLUSTER BY` operation, no
# MAGIC files rewritten). New writes and the next `OPTIMIZE` use the new keys; `OPTIMIZE … FULL` reclusters **everything** now:

# COMMAND ----------

# DBTITLE 1,OPTIMIZE FULL - recluster all data with the new keys
_lof = optimize("perf_liquid", "FULL")
layout("perf_liquid")
skipping_report("perf_liquid", "customer_id", "C0123")
skipping_report("perf_liquid", "event_date", "2026-03-01", "2026-03-07")

# COMMAND ----------

# DBTITLE 1,The DataFrame API and CLUSTER BY AUTO
(spark.table("perf_events").limit(100000)
      .write.clusterBy("customer_id").mode("overwrite").saveAsTable("perf_liquid_df"))
print("perf_liquid_df clustered by:", layout("perf_liquid_df", verbose=False)["clustered_by"])
_auto_ok = try_sql("CREATE TABLE perf_auto CLUSTER BY AUTO AS SELECT * FROM perf_events LIMIT 100000")
if _auto_ok:
    _props = {r["key"]: r["value"] for r in spark.sql("SHOW TBLPROPERTIES perf_auto").collect()}
    print("clustering properties:", {k: v for k, v in _props.items() if "cluster" in k.lower()} or "none shown")
    print("keys chosen so far:", layout("perf_auto", verbose=False)["clustered_by"] or
          "none yet - predictive optimization picks them from your query patterns over time")

# COMMAND ----------

# MAGIC %md
# MAGIC `CLUSTER BY AUTO` needs **predictive optimization** and a Unity Catalog **managed** table: Databricks watches the queries on
# MAGIC the table and chooses (and later changes) the keys. Right after creation there are usually no keys yet.

# COMMAND ----------

# DBTITLE 1,✅ Check Part 4
check("perf_liquid was created with clustering key customer_id", _ll["clustered_by"] == ["customer_id"])
check("after OPTIMIZE a customer filter reads at most half of the files", _l1[1] < 3 or _l1[0] <= _l1[1] / 2)
check("incremental: the 2nd OPTIMIZE rewrote fewer files than the 2nd Z-order", _lo2["removed"] < _zo2["removed"])
check("ALTER TABLE ... CLUSTER BY changed the keys without rewriting files",
      _after_alter["clustered_by"] == ["customer_id", "event_date"] and _after_alter["files"] == _files_before_alter)
check("the DataFrame writer created a clustered table", layout("perf_liquid_df", verbose=False)["clustered_by"] == ["customer_id"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 5 · What liquid clustering can't be combined with

# COMMAND ----------

# DBTITLE 1,ZORDER on a clustered table, PARTITIONED BY + CLUSTER BY
_zorder_on_liquid = try_sql("OPTIMIZE perf_liquid ZORDER BY (product_id)")
_part_and_cluster = try_sql("CREATE TABLE perf_conflict (a INT, d DATE) PARTITIONED BY (d) CLUSTER BY (a)")

# COMMAND ----------

# DBTITLE 1,✅ Check Part 5
check("OPTIMIZE ... ZORDER BY is refused on a liquid-clustered table", not _zorder_on_liquid)
check("PARTITIONED BY together with CLUSTER BY is refused", not _part_and_cluster)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 6 · Predictive optimization
# MAGIC Where is it enabled? Settings are **inherited**: account → catalog → schema → table.

# COMMAND ----------

# DBTITLE 1,Status at three levels
for _kind, _name in (("CATALOG", catalog_name), ("SCHEMA", f"{catalog_name}.{schema_name}"), ("TABLE", "perf_liquid")):
    print(f"{_kind:<8}{_name:<32}{po_status(_kind, _name)}")

# COMMAND ----------

# MAGIC %md
# MAGIC You **own** the `shopwave` schema, so you can set it there. `ENABLE` / `DISABLE` set it explicitly, `INHERIT` goes back to
# MAGIC the parent's setting (the account must have the feature available — on some workspaces only an admin can enable it).

# COMMAND ----------

# DBTITLE 1,ENABLE on the schema, look, then INHERIT again
try_sql(f"ALTER SCHEMA {catalog_name}.{schema_name} ENABLE PREDICTIVE OPTIMIZATION")
print("schema:", po_status("SCHEMA", f"{catalog_name}.{schema_name}"))
print("table :", po_status("TABLE", "perf_liquid"))
try_sql(f"ALTER SCHEMA {catalog_name}.{schema_name} INHERIT PREDICTIVE OPTIMIZATION")
print("schema:", po_status("SCHEMA", f"{catalog_name}.{schema_name}"))

# COMMAND ----------

# MAGIC %md
# MAGIC What did predictive optimization do for your tables? It records every operation (it may take hours or days before it acts
# MAGIC on new tables — it decides when maintenance pays off):

# COMMAND ----------

# DBTITLE 1,system.storage.predictive_optimization_operations_history
try:
    display(spark.sql(f"""
        SELECT start_time, table_name, operation_type, operation_status, usage_quantity, operation_metrics
        FROM system.storage.predictive_optimization_operations_history
        WHERE catalog_name = '{catalog_name}' AND schema_name = '{schema_name}'
        ORDER BY start_time DESC LIMIT 20"""))
except Exception as e:
    print("history not available:", _first_line(e))

# COMMAND ----------

# MAGIC %md
# MAGIC `ANALYZE` is the third operation it runs — collecting statistics for the optimizer. Done by hand it looks like this:

# COMMAND ----------

# DBTITLE 1,ANALYZE TABLE - what predictive optimization runs for you
# MAGIC %sql
# MAGIC ANALYZE TABLE perf_liquid COMPUTE STATISTICS FOR COLUMNS customer_id, event_date

# COMMAND ----------

# DBTITLE 1,Column statistics
# MAGIC %sql
# MAGIC DESCRIBE EXTENDED perf_liquid customer_id

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 7 · 🖱️ Confirm it in the query profile
# MAGIC Run the two queries below, then **See performance** → each query → **See query profile** → click the **Scan** node:
# MAGIC compare **files read** and **files pruned** (or *size of files pruned*).

# COMMAND ----------

# DBTITLE 1,Same filter on the unclustered and the clustered table
# MAGIC %sql
# MAGIC SELECT 'perf_events (unclustered)' AS t, count(*) AS events FROM perf_events WHERE customer_id = 'C0123'
# MAGIC UNION ALL
# MAGIC SELECT 'perf_liquid (clustered)', count(*) FROM perf_liquid WHERE customer_id = 'C0123'

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Summary
# MAGIC | Layout | Customer filter | Date filter | Maintenance |
# MAGIC |---|---|---|---|
# MAGIC | unclustered | all files | all files | `OPTIMIZE` only fixes small files |
# MAGIC | `PARTITIONED BY (event_date)` | all files | ✅ one folder | tiny files; columns fixed forever |
# MAGIC | `ZORDER BY (customer_id)` | ✅ few files | all files | every run rewrites the whole partition |
# MAGIC | `CLUSTER BY (customer_id, event_date)` | ✅ few files | ✅ fewer files | incremental `OPTIMIZE`, keys changeable, `AUTO`, predictive optimization |
# MAGIC
# MAGIC **Clean-up** (optional — keep `perf_liquid` for the challenge):

# COMMAND ----------

# DBTITLE 1,Clean-up (optional)
# for _t in ("perf_small", "perf_part_date", "perf_zorder", "perf_auto", "perf_liquid_df"):
#     spark.sql(f"DROP TABLE IF EXISTS {_t}")
