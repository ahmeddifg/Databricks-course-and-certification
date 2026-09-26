# Databricks notebook source
# MAGIC %md
# MAGIC # 🧪 Lab 03-L1 · Delta Table Fundamentals (Guided)
# MAGIC **Time:** ~45 min · **Compute:** Serverless · **Story:** you run ShopWave's **product catalog** table.
# MAGIC
# MAGIC | Part | You will… |
# MAGIC |---|---|
# MAGIC | 1 | Create a managed Delta table with an explicit schema and load it |
# MAGIC | 2 | Inspect it: `DESCRIBE DETAIL`, `DESCRIBE EXTENDED`, `SHOW TBLPROPERTIES` |
# MAGIC | 3 | Change data with `UPDATE`, `DELETE`, `INSERT` and read the **transaction history** |
# MAGIC | 4 | **Time travel** by version and by timestamp; diff two versions |
# MAGIC | 5 | Survive a disaster with `RESTORE` |
# MAGIC | 6 | See **schema enforcement** block bad writes, and **schema evolution** allow good ones |
# MAGIC | 7 | Protect data quality with `NOT NULL` and `CHECK` constraints |
# MAGIC | 8 | Let Delta fill columns for you: **generated** and **identity** columns |
# MAGIC | 9 | ✅ Automatic checks |
# MAGIC
# MAGIC > 🔁 **Re-running?** Part 1 drops and recreates the table, so version numbers always start at 0. Run the parts **in order**.

# COMMAND ----------

# MAGIC %run ../../Includes/_setup

# COMMAND ----------

# MAGIC %run ./_03_prepare

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1 · Create and load a Delta table
# MAGIC
# MAGIC We declare the schema explicitly. `product_id` is `NOT NULL`. No `USING DELTA` is needed — **Delta is the default format** on Databricks.

# COMMAND ----------

# DBTITLE 1,Start fresh (drop + create) → version 0
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS lab03_products;
# MAGIC
# MAGIC CREATE TABLE lab03_products (
# MAGIC   product_id STRING NOT NULL COMMENT 'Business key, e.g. P001',
# MAGIC   title      STRING,
# MAGIC   brand      STRING,
# MAGIC   category   STRING,
# MAGIC   price      DOUBLE
# MAGIC )
# MAGIC COMMENT 'ShopWave product catalog (Section 03 lab)';

# COMMAND ----------

# DBTITLE 1,Expose the CSV files as a temporary view
(spark.read.option("header", "true").option("delimiter", ";").option("inferSchema", "true")
      .csv(f"{dataset_path}/products-csv")
      .createOrReplaceTempView("products_csv_v"))
display(spark.table("products_csv_v").limit(5))

# COMMAND ----------

# DBTITLE 1,Load it → version 1
# MAGIC %sql
# MAGIC INSERT INTO lab03_products
# MAGIC SELECT product_id, title, brand, category, CAST(price AS DOUBLE)
# MAGIC FROM products_csv_v;
# MAGIC
# MAGIC SELECT count(*) AS products FROM lab03_products;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2 · Inspect the table

# COMMAND ----------

# DBTITLE 1,DESCRIBE DETAIL — the physical facts
# MAGIC %sql
# MAGIC DESCRIBE DETAIL lab03_products

# COMMAND ----------

# MAGIC %md
# MAGIC Look at: **`format` = delta**, `location` (managed storage chosen by Unity Catalog), **`numFiles`**, `sizeInBytes`,
# MAGIC `properties`, and `tableFeatures` (e.g. `deletionVectors`).

# COMMAND ----------

# DBTITLE 1,DESCRIBE EXTENDED — schema + catalog metadata
# MAGIC %sql
# MAGIC DESCRIBE EXTENDED lab03_products

# COMMAND ----------

# DBTITLE 1,Table properties
# MAGIC %sql
# MAGIC SHOW TBLPROPERTIES lab03_products

# COMMAND ----------

# MAGIC %md
# MAGIC > 🔒 On Unity Catalog **managed** tables you can't browse the storage folder (it's protected), so you can't list `_delta_log/`
# MAGIC > directly. `DESCRIBE HISTORY` and `DESCRIBE DETAIL` expose what the log contains. The presentation **03-P1** shows a real
# MAGIC > commit file.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 3 · Change the data — every write is a new version

# COMMAND ----------

# DBTITLE 1,UPDATE → version 2 (Electronics +10 %)
# MAGIC %sql
# MAGIC UPDATE lab03_products
# MAGIC SET price = round(price * 1.10, 2)
# MAGIC WHERE category = 'Electronics';

# COMMAND ----------

# DBTITLE 1,DELETE → version 3 (discontinue Toys)
# MAGIC %sql
# MAGIC DELETE FROM lab03_products WHERE category = 'Toys';

# COMMAND ----------

# DBTITLE 1,INSERT → version 4 (two new products)
# MAGIC %sql
# MAGIC INSERT INTO lab03_products VALUES
# MAGIC   ('P101', 'Solar Power Bank', 'Voltix', 'Electronics', 59.90),
# MAGIC   ('P102', 'Desert Camping Tent', 'Peakline', 'Sports', 189.00);

# COMMAND ----------

# DBTITLE 1,DESCRIBE HISTORY — the transaction log, one row per commit
# MAGIC %sql
# MAGIC DESCRIBE HISTORY lab03_products

# COMMAND ----------

# MAGIC %md
# MAGIC Read the history like an audit log:
# MAGIC
# MAGIC | Column | Meaning |
# MAGIC |---|---|
# MAGIC | `version` | 0, 1, 2 … one per commit |
# MAGIC | `timestamp`, `userName` | when and who |
# MAGIC | `operation` | `CREATE TABLE`, `WRITE`, `UPDATE`, `DELETE`, `MERGE`, `OPTIMIZE`, `RESTORE` … |
# MAGIC | `operationParameters` | e.g. the predicate of the UPDATE/DELETE |
# MAGIC | `operationMetrics` | rows/files added & removed, `numDeletionVectorsAdded` … |

# COMMAND ----------

# DBTITLE 1,Operation metrics per version
display(history("lab03_products").select("version", "operation", "operationMetrics"))

# COMMAND ----------

# DBTITLE 1,✔️ Sanity check: are the versions what this lab expects?
expected = {0: "CREATE TABLE", 1: "WRITE", 2: "UPDATE", 3: "DELETE", 4: "WRITE"}
actual = {r["version"]: r["operation"] for r in spark.sql("DESCRIBE HISTORY lab03_products").collect()}
if all(actual.get(v) == op for v, op in expected.items()):
    print("✅ Versions 0-4 are exactly as expected - the version numbers used below will work.")
else:
    print("⚠️ The history differs from what the lab expects (a cell was re-run or skipped, or Databricks added a background commit):")
    for v in sorted(actual):
        print(f"   v{v}: {actual[v]}" + ("" if expected.get(v, actual[v]) == actual[v] else f"   (expected {expected[v]})"))
    print("👉 Re-run the lab from Part 1 (it drops and recreates the table).")

# COMMAND ----------

# MAGIC %md
# MAGIC 💡 If the `DELETE` shows **`numDeletionVectorsAdded`** instead of rewritten files, **deletion vectors** are on: Delta just
# MAGIC *marked* the rows as deleted instead of rewriting the Parquet files. Files are physically rewritten later by `OPTIMIZE`
# MAGIC (or `REORG TABLE … APPLY (PURGE)`).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 4 · Time travel

# COMMAND ----------

# DBTITLE 1,By version number
# MAGIC %sql
# MAGIC SELECT 'v1 (original load)' AS snapshot, count(*) AS products FROM lab03_products VERSION AS OF 1
# MAGIC UNION ALL
# MAGIC SELECT 'v3 (after delete)', count(*) FROM lab03_products@v3
# MAGIC UNION ALL
# MAGIC SELECT 'current', count(*) FROM lab03_products

# COMMAND ----------

# MAGIC %md
# MAGIC `VERSION AS OF 3` and the shorthand **`@v3`** are equivalent.
# MAGIC
# MAGIC **By timestamp** — we take the commit timestamp of version 2 from the history:

# COMMAND ----------

# DBTITLE 1,TIMESTAMP AS OF
ts_v2 = spark.sql("DESCRIBE HISTORY lab03_products").where("version = 2").first()["timestamp"]
print("Version 2 was committed at", ts_v2)

display(spark.sql(f"""
    SELECT category, round(avg(price), 2) AS avg_price
    FROM lab03_products TIMESTAMP AS OF '{ts_v2}'
    GROUP BY category ORDER BY category"""))

# COMMAND ----------

# MAGIC %md
# MAGIC The same in **Python** with DataFrame reader options:

# COMMAND ----------

# DBTITLE 1,Time travel from Python
v1_df = spark.read.option("versionAsOf", 1).table("lab03_products")
v2_df = spark.read.option("timestampAsOf", str(ts_v2)).table("lab03_products")
print("rows in v1:", v1_df.count(), "| rows at the timestamp of v2:", v2_df.count())

# COMMAND ----------

# MAGIC %md
# MAGIC **Diff two versions** — which rows changed between v1 and v2? (`EXCEPT` returns rows in the first query that aren't in the second.)

# COMMAND ----------

# DBTITLE 1,What did the UPDATE change?
# MAGIC %sql
# MAGIC SELECT 'before' AS side, b.* FROM (SELECT * FROM lab03_products VERSION AS OF 1 EXCEPT SELECT * FROM lab03_products VERSION AS OF 2) AS b
# MAGIC UNION ALL
# MAGIC SELECT 'after', a.* FROM (SELECT * FROM lab03_products VERSION AS OF 2 EXCEPT SELECT * FROM lab03_products VERSION AS OF 1) AS a
# MAGIC ORDER BY product_id, side DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 5 · 💥 Disaster & `RESTORE`
# MAGIC Someone runs a `DELETE` without a `WHERE` clause…

# COMMAND ----------

# DBTITLE 1,Oops → version 5
# MAGIC %sql
# MAGIC DELETE FROM lab03_products;
# MAGIC SELECT count(*) AS products_left FROM lab03_products;

# COMMAND ----------

# DBTITLE 1,Find the last good version from the history
bad_version = (spark.sql("DESCRIBE HISTORY lab03_products")
                    .where("operation = 'DELETE'").agg({"version": "max"}).first()[0])
last_good = bad_version - 1
print(f"The bad DELETE is version {bad_version} → last good version is {last_good}")

# COMMAND ----------

# DBTITLE 1,RESTORE → version 6
# SQL equivalent (with the lab's expected numbers):  RESTORE TABLE lab03_products TO VERSION AS OF 4
display(spark.sql(f"RESTORE TABLE lab03_products TO VERSION AS OF {last_good}"))
print("Products after restore:", spark.table("lab03_products").count())

# COMMAND ----------

# MAGIC %md
# MAGIC `RESTORE` doesn't erase history — it **adds a new commit** (version 6) that makes the table look like version 4 again.
# MAGIC Its output reports how many files were restored/removed (`num_restored_files`, `num_removed_files`, …).
# MAGIC The broken version 5 is still in the history (you could even time-travel to it).

# COMMAND ----------

# DBTITLE 1,History after restore
display(history("lab03_products").select("version", "operation", "operationParameters"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 6 · Schema enforcement vs schema evolution
# MAGIC
# MAGIC **Enforcement:** Delta rejects writes whose schema doesn't match the table — protecting it from bad data.

# COMMAND ----------

# DBTITLE 1,❌ Append a DataFrame with an extra column
from pyspark.sql import Row

new_rows = spark.createDataFrame([Row(product_id="P103", title="Smart Scale", brand="Nexa",
                                      category="Electronics", price=49.0, rating=4.6)])
try:
    new_rows.write.mode("append").saveAsTable("lab03_products")
    print("Appended (unexpected!) - automatic schema merging must be enabled on this compute")
except Exception as e:
    print("🚫 Rejected by schema enforcement:\n   ", str(e).strip().splitlines()[0][:220])

# COMMAND ----------

# DBTITLE 1,❌ Wrong data type
bad_type = spark.createDataFrame([("P104", "Mystery Box", "FunLab", "Toys", "not-a-number")],
                                 "product_id STRING, title STRING, brand STRING, category STRING, price STRING")
try:
    bad_type.write.mode("append").saveAsTable("lab03_products")
    print("Appended (unexpected!) - cleaning it up so the lab stays consistent")
    spark.sql("DELETE FROM lab03_products WHERE product_id = 'P104'")
except Exception as e:
    print("🚫 Rejected - STRING can't be written into a DOUBLE column:\n   ", str(e).strip().splitlines()[0][:220])

# COMMAND ----------

# MAGIC %md
# MAGIC **Evolution:** when the new column is intended, allow it explicitly with **`mergeSchema`**. Existing rows get `NULL` for the new column.

# COMMAND ----------

# DBTITLE 1,✅ Append with mergeSchema → version 7
if spark.sql("SELECT count(*) FROM lab03_products WHERE product_id = 'P103'").first()[0] == 0:
    new_rows.write.mode("append").option("mergeSchema", "true").saveAsTable("lab03_products")
display(spark.sql("SELECT product_id, title, price, rating FROM lab03_products WHERE product_id IN ('P101', 'P103')"))

# COMMAND ----------

# DBTITLE 1,The new column is in the schema
# MAGIC %sql
# MAGIC DESCRIBE lab03_products

# COMMAND ----------

# MAGIC %md
# MAGIC > 🎯 Other ways to evolve a schema: `ALTER TABLE … ADD COLUMNS (…)`, `MERGE WITH SCHEMA EVOLUTION INTO …`, and
# MAGIC > `.option("overwriteSchema", "true")` with `mode("overwrite")` to **replace** the schema entirely.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 7 · Constraints
# MAGIC `product_id` is **NOT NULL** (declared at creation). **CHECK** constraints are added with `ALTER TABLE … ADD CONSTRAINT`
# MAGIC (existing rows must already satisfy them).

# COMMAND ----------

# DBTITLE 1,Add a CHECK constraint → version 8
# MAGIC %sql
# MAGIC ALTER TABLE lab03_products ADD CONSTRAINT positive_price CHECK (price > 0);

# COMMAND ----------

# DBTITLE 1,Constraints show up as table properties
# MAGIC %sql
# MAGIC SHOW TBLPROPERTIES lab03_products

# COMMAND ----------

# DBTITLE 1,❌ Try to break the constraints
for label, stmt in [
    ("negative price", "INSERT INTO lab03_products (product_id, title, brand, category, price) VALUES ('P105', 'Broken', 'X', 'Toys', -5)"),
    ("NULL product_id", "INSERT INTO lab03_products (product_id, title, brand, category, price) VALUES (NULL, 'No id', 'X', 'Toys', 10)"),
]:
    try:
        spark.sql(stmt)
        print(f"Inserted {label} (unexpected!)")
    except Exception as e:
        print(f"🚫 {label}: {str(e).strip().splitlines()[0][:160]}")

# COMMAND ----------

# MAGIC %md
# MAGIC A violating write fails **as a whole** — no rows from that transaction are written (atomicity).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 8 · Generated and identity columns
# MAGIC A **price history** table where Delta fills two columns itself:
# MAGIC * `log_id` — an **identity** column (unique, increasing surrogate key)
# MAGIC * `change_date` — a **generated** column computed from `changed_at`
# MAGIC
# MAGIC Both must be declared in `CREATE TABLE` (they can't be added later or created with CTAS). When inserting, **leave them out** of the column list.

# COMMAND ----------

# DBTITLE 1,Create the table with generated & identity columns
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS lab03_price_log;
# MAGIC
# MAGIC CREATE TABLE lab03_price_log (
# MAGIC   log_id      BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC   product_id  STRING NOT NULL,
# MAGIC   price       DOUBLE,
# MAGIC   changed_at  TIMESTAMP,
# MAGIC   change_date DATE GENERATED ALWAYS AS (CAST(changed_at AS DATE))
# MAGIC );
# MAGIC
# MAGIC INSERT INTO lab03_price_log (product_id, price, changed_at)
# MAGIC SELECT product_id, price, current_timestamp() FROM lab03_products WHERE category = 'Electronics';
# MAGIC
# MAGIC SELECT * FROM lab03_price_log ORDER BY log_id;

# COMMAND ----------

# DBTITLE 1,❌ You can't write your own values into a GENERATED ALWAYS identity column
try:
    spark.sql("INSERT INTO lab03_price_log (log_id, product_id, price, changed_at) "
              "VALUES (999, 'P001', 1.0, current_timestamp())")
    print("Inserted (unexpected!)")
except Exception as e:
    print("🚫", str(e).strip().splitlines()[0][:200])

# COMMAND ----------

# MAGIC %md
# MAGIC > 🎯 Identity values are **unique and increasing but not necessarily consecutive**. `GENERATED BY DEFAULT AS IDENTITY`
# MAGIC > would allow explicit values. A generated column's value must always equal its expression — Delta computes it on write.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 9 · ✅ Automatic checks

# COMMAND ----------

# DBTITLE 1,Check your work
_h = spark.sql("DESCRIBE HISTORY lab03_products")
_ops = [r["operation"] for r in _h.orderBy("version").collect()]
_props = {r["key"]: r["value"] for r in spark.sql("SHOW TBLPROPERTIES lab03_products").collect()}
_checks = {
    "Table is Delta": table_detail("lab03_products")["format"] == "delta",
    "History contains UPDATE, DELETE and RESTORE": {"UPDATE", "DELETE", "RESTORE"} <= set(_ops),
    "The initial load (first WRITE) had 36 products": spark.sql(
        "SELECT count(*) FROM lab03_products VERSION AS OF "
        + str(_h.where("operation = 'WRITE'").agg({"version": "min"}).first()[0])).first()[0] == 36,
    "Restored table has 33 products (32 + P103)": spark.table("lab03_products").count() == 33,
    "Column 'rating' added by schema evolution": "rating" in spark.table("lab03_products").columns,
    "CHECK constraint registered": any(k.startswith("delta.constraints.positive_price") for k in _props),
    "Generated column filled (change_date = date of changed_at)": spark.sql(
        "SELECT count(*) FROM lab03_price_log WHERE change_date = CAST(changed_at AS DATE)").first()[0] > 0,
    "Identity column produced unique ids": spark.sql(
        "SELECT count(DISTINCT log_id) = count(*) FROM lab03_price_log").first()[0],
}
for name, ok in _checks.items():
    print(("✅ " if ok else "❌ ") + name)
print("\n🎉 Lab complete - next: 03-L2 · Writing, Merging & Maintaining Delta Tables")
