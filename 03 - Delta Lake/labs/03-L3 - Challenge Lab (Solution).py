# Databricks notebook source
# MAGIC %md
# MAGIC # 🏆 Lab 03-L3 · Challenge Lab — Delta Lake — ✅ SOLUTION
# MAGIC > Try the challenge yourself first! This notebook contains the reference answers and runs end-to-end.
# MAGIC
# MAGIC **Time:** ~50 min · **Compute:** Serverless · **Story:** you own ShopWave's **inventory** table.
# MAGIC
# MAGIC Write the SQL or Python yourself — add `%sql` cells where you prefer SQL. Replace every `None`, then run each **✅ Check**.
# MAGIC Helpers available: `history()`, `latest_version()`, `table_detail()`, `num_files()`, `last_operation_metrics()`.
# MAGIC
# MAGIC | Task | Skill |
# MAGIC |---|---|
# MAGIC | 1 | Create a Delta table with `NOT NULL` + a `CHECK` constraint |
# MAGIC | 2 | Load it |
# MAGIC | 3 | `UPDATE` — and a constraint violation (atomicity) |
# MAGIC | 4 | Recover from an accidental delete with the history + `RESTORE` |
# MAGIC | 5 | Upsert a restock feed with `MERGE` |
# MAGIC | 6 | Time travel: what changed since the initial load? |
# MAGIC | 7 | Schema evolution |
# MAGIC | 8 | `VACUUM` & time travel |

# COMMAND ----------

# MAGIC %run ../../Includes/_setup

# COMMAND ----------

# MAGIC %run ./_03_prepare

# COMMAND ----------

# DBTITLE 1,Setup for the challenge (run me - don't edit)
from pyspark.sql import functions as F

(spark.read.option("header", "true").option("delimiter", ";").option("inferSchema", "true")
      .csv(f"{dataset_path}/products-csv").createOrReplaceTempView("products_csv_v"))

restock_df = spark.createDataFrame(
    [("P001", 50), ("P002", 20), ("P013", 15), ("P014", 15), ("P030", 40), ("P037", 25), ("P038", 10)],
    "product_id STRING, qty INT")
restock_df.createOrReplaceTempView("restock_v")


def check(label, condition):
    print(("✅ " if condition else "❌ ") + label)


def inv_exists():
    return spark.catalog.tableExists("lab03_inventory")


def inv_count():
    return spark.table("lab03_inventory").count() if inv_exists() else -1


def inv_ops():
    return {r["operation"] for r in spark.sql("DESCRIBE HISTORY lab03_inventory").collect()} if inv_exists() else set()


def inv_columns():
    return spark.table("lab03_inventory").columns if inv_exists() else []


def _stock(pid):
    r = spark.sql(f"SELECT stock FROM lab03_inventory WHERE product_id = '{pid}'").first() if inv_exists() else None
    return None if r is None else r[0]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 1 · Create the table
# MAGIC Create a **new** (drop it first if it exists) managed Delta table **`lab03_inventory`**:
# MAGIC
# MAGIC | Column | Type | Rule |
# MAGIC |---|---|---|
# MAGIC | `product_id` | STRING | **NOT NULL** |
# MAGIC | `stock` | INT | |
# MAGIC | `updated_at` | TIMESTAMP | |
# MAGIC
# MAGIC Then add a CHECK constraint named **`stock_non_negative`** requiring `stock >= 0`.

# COMMAND ----------

# DBTITLE 1,Task 1 · SOLUTION
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS lab03_inventory;
# MAGIC
# MAGIC CREATE TABLE lab03_inventory (
# MAGIC   product_id STRING NOT NULL,
# MAGIC   stock      INT,
# MAGIC   updated_at TIMESTAMP
# MAGIC );
# MAGIC
# MAGIC ALTER TABLE lab03_inventory ADD CONSTRAINT stock_non_negative CHECK (stock >= 0);

# COMMAND ----------

# DBTITLE 1,✅ Check 1
_exists = spark.catalog.tableExists("lab03_inventory")
_props = {r["key"]: r["value"] for r in spark.sql("SHOW TBLPROPERTIES lab03_inventory").collect()} if _exists else {}
check("lab03_inventory exists and is Delta", _exists and table_detail("lab03_inventory")["format"] == "delta")
check("CHECK constraint stock_non_negative exists", any(k.endswith("stock_non_negative") for k in _props))
check("product_id is NOT NULL", _exists and not [f for f in spark.table("lab03_inventory").schema.fields
                                                  if f.name == "product_id"][0].nullable)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 2 · Initial load
# MAGIC Insert one row per product from `products_csv_v` with **`stock = 100`** and `updated_at = current_timestamp()`.

# COMMAND ----------

# DBTITLE 1,Task 2 · SOLUTION
# MAGIC %sql
# MAGIC INSERT INTO lab03_inventory
# MAGIC SELECT product_id, 100, current_timestamp() FROM products_csv_v;

# COMMAND ----------

# DBTITLE 1,✅ Check 2
check("36 products loaded with stock 100", _exists and spark.sql(
    "SELECT count(*) FROM lab03_inventory WHERE stock = 100").first()[0] == 36)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 3 · A sales day
# MAGIC 1. Electronics sold **30** units each → decrease their `stock` by 30 (set `updated_at` too).
# MAGIC 2. Books sold **120** units each → try to decrease their stock by 120. This must **fail** (why?).
# MAGIC    Catch the error in Python and set `books_update_failed = True`.

# COMMAND ----------

# DBTITLE 1,Task 3 · SOLUTION
spark.sql("""
    UPDATE lab03_inventory
    SET stock = stock - 30, updated_at = current_timestamp()
    WHERE product_id IN (SELECT product_id FROM products_csv_v WHERE category = 'Electronics')""")

try:
    spark.sql("""
        UPDATE lab03_inventory
        SET stock = stock - 120, updated_at = current_timestamp()
        WHERE product_id IN (SELECT product_id FROM products_csv_v WHERE category = 'Books')""")
    books_update_failed = False
except Exception as e:
    # 100 - 120 = -20 violates CHECK (stock >= 0): the WHOLE update is rejected, no row changes (atomicity)
    print("🚫", str(e).strip().splitlines()[0][:200])
    books_update_failed = True

# COMMAND ----------

# DBTITLE 1,✅ Check 3
check("Electronics stock is 70", _stock("P001") == 70 and _stock("P006") == 70)
check("Books update failed and changed nothing", books_update_failed is True and _stock("P013") == 100)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 4 · The accident
# MAGIC Run this cell — it simulates a colleague's mistake. Then:
# MAGIC 1. Using `DESCRIBE HISTORY`, compute in Python the version **just before** the bad `DELETE` → `restore_to`.
# MAGIC 2. Restore the table to that version.

# COMMAND ----------

# DBTITLE 1,💥 The accident (run once)
if inv_exists():
    spark.sql("DELETE FROM lab03_inventory WHERE stock < 1000")
print("Rows left:", inv_count())

# COMMAND ----------

# DBTITLE 1,Task 4 · SOLUTION
bad_delete = (spark.sql("DESCRIBE HISTORY lab03_inventory")
                   .where("operation = 'DELETE'").agg(F.max("version")).first()[0])
restore_to = bad_delete - 1
spark.sql(f"RESTORE TABLE lab03_inventory TO VERSION AS OF {restore_to}")
print(f"Restored to version {restore_to}")

# COMMAND ----------

# DBTITLE 1,✅ Check 4
check("36 products are back", inv_count() == 36)
check("Electronics stock is still 70 after the restore", _stock("P001") == 70)
check("History contains a RESTORE", "RESTORE" in inv_ops())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 5 · Restock feed
# MAGIC `restock_v` (`product_id`, `qty`) contains restocks for 5 existing products and 2 **new** ones (`P037`, `P038`).
# MAGIC With **one `MERGE`**:
# MAGIC * existing product → `stock = stock + qty`, `updated_at = current_timestamp()`
# MAGIC * new product → insert it with `stock = qty`

# COMMAND ----------

# DBTITLE 1,Task 5 · SOLUTION
# MAGIC %sql
# MAGIC MERGE INTO lab03_inventory AS t
# MAGIC USING restock_v AS s
# MAGIC ON t.product_id = s.product_id
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET stock = t.stock + s.qty, updated_at = current_timestamp()
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (product_id, stock, updated_at) VALUES (s.product_id, s.qty, current_timestamp())

# COMMAND ----------

# DBTITLE 1,✅ Check 5
check("P001 restocked to 120", _stock("P001") == 120)
check("P013 restocked to 115", _stock("P013") == 115)
check("P037 inserted with 25", _stock("P037") == 25)
check("38 products in total", inv_count() == 38)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 6 · What changed since the initial load?
# MAGIC Using **time travel**, build `changed_since_load`: a DataFrame with the `product_id`s whose `stock` differs between the
# MAGIC version created by your **initial load (Task 2)** and the **current** version (ignore products that didn't exist then).
# MAGIC Find the load version from the history — don't hard-code it.

# COMMAND ----------

# DBTITLE 1,Task 6 · SOLUTION
load_version = (spark.sql("DESCRIBE HISTORY lab03_inventory")
                     .where("operation = 'WRITE' AND operationParameters['mode'] = 'Append'")
                     .agg(F.min("version")).first()[0])

changed_since_load = spark.sql(f"""
    SELECT c.product_id, o.stock AS stock_at_load, c.stock AS stock_now
    FROM lab03_inventory c
    JOIN lab03_inventory VERSION AS OF {load_version} o USING (product_id)
    WHERE c.stock <> o.stock
    ORDER BY c.product_id""")
display(changed_since_load)

# COMMAND ----------

# DBTITLE 1,✅ Check 6
_load_v = (spark.sql("DESCRIBE HISTORY lab03_inventory")
                .where("operation = 'WRITE' AND operationParameters['mode'] = 'Append'")
                .agg(F.min("version")).first()[0]) if inv_exists() else None
_expected6 = {r[0] for r in spark.sql(f"""
    SELECT c.product_id FROM lab03_inventory c JOIN lab03_inventory VERSION AS OF {_load_v} o USING (product_id)
    WHERE c.stock <> o.stock""").collect()} if _load_v is not None else set()
_got6 = set() if changed_since_load is None else {r["product_id"] for r in changed_since_load.select("product_id").collect()}
check(f"changed_since_load has the right {len(_expected6)} products", _got6 == _expected6 and len(_got6) > 0)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 7 · Schema evolution
# MAGIC Append **one** row for product `P039` (stock 5, `updated_at` now) that also has a new column **`warehouse`** = `'RUH-1'`,
# MAGIC so that the table gains the `warehouse` column. Existing rows should get `NULL`.

# COMMAND ----------

# DBTITLE 1,Task 7 · SOLUTION
new_row = spark.sql("SELECT 'P039' AS product_id, 5 AS stock, current_timestamp() AS updated_at, 'RUH-1' AS warehouse")
new_row.write.mode("append").option("mergeSchema", "true").saveAsTable("lab03_inventory")
display(spark.table("lab03_inventory").orderBy("product_id"))

# COMMAND ----------

# DBTITLE 1,✅ Check 7
_cols = inv_columns()
check("warehouse column exists", "warehouse" in _cols)
check("P039 has warehouse RUH-1", "warehouse" in _cols and spark.sql(
    "SELECT warehouse FROM lab03_inventory WHERE product_id = 'P039'").first()[0] == "RUH-1")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 8 · VACUUM & time travel
# MAGIC Last week someone ran `VACUUM big_table RETAIN 0 HOURS` (with the safety check disabled) on a table that is
# MAGIC updated every day. Today you try `SELECT * FROM big_table VERSION AS OF 2` — a version whose data files are no longer
# MAGIC used by the current version. What happens?
# MAGIC
# MAGIC * **A** — It works: time travel only needs the transaction log
# MAGIC * **B** — It fails because the data files that version 2 needs were physically deleted
# MAGIC * **C** — VACUUM also deleted the transaction log, so the table is gone
# MAGIC * **D** — It returns the current version instead

# COMMAND ----------

# DBTITLE 1,Task 8 · SOLUTION
answer_task8 = "B"
# VACUUM deletes data files that the current version no longer references and that were removed longer ago
# than the retention period. The log still lists old versions, but their files are gone -> time travel fails.
# (Files still used by the current version are never vacuumed.)

# COMMAND ----------

# DBTITLE 1,✅ Check 8
check("Task 8 answer", answer_task8 == "B")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🏁 Final score

# COMMAND ----------

# DBTITLE 1,Score
_final = {
    "Task 1": any(k.endswith("stock_non_negative") for k in _props),
    "Task 2": inv_exists(),
    "Task 3": books_update_failed is True,
    "Task 4": restore_to is not None,
    "Task 5": _stock("P037") == 25,
    "Task 6": changed_since_load is not None and _got6 == _expected6,
    "Task 7": "warehouse" in inv_columns(),
    "Task 8": answer_task8 == "B",
}
for k, v in _final.items():
    print(("✅ " if v else "❌ ") + k)
print(f"\nScore: {sum(_final.values())} / {len(_final)}")
