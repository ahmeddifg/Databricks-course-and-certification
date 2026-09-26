# Databricks notebook source
# MAGIC %md
# MAGIC # 🏆 Lab 04-L3 · Challenge Lab — Unity Catalog & Data Objects
# MAGIC **Time:** ~45 min · **Compute:** Serverless · **Story:** ShopWave's pricing team wants a small, well-governed **sales mart**.
# MAGIC
# MAGIC Write the SQL or Python yourself. Replace every `None` / `# TODO`, then run each **✅ Check**.
# MAGIC
# MAGIC **Names to use** (printed by the setup cell): the schema **`CH`** (e.g. `lab04_ahmed.mart`) and the volume path **`EXPORTS`**.
# MAGIC In Python use f-strings (`spark.sql(f"CREATE ... {CH}.products ...")`); for `%sql` cells, run `USE CATALOG` / `USE SCHEMA`
# MAGIC first (Task 1) and use short names.
# MAGIC
# MAGIC | Task | Skill |
# MAGIC |---|---|
# MAGIC | 1 | Create a schema with a comment and make it your current schema |
# MAGIC | 2 | Create a **managed** table from files |
# MAGIC | 3 | Create a **SQL UDF** in Unity Catalog |
# MAGIC | 4 | Create a **stored view** that uses the UDF |
# MAGIC | 5 | Create a **temporary view** |
# MAGIC | 6 | Create a **volume** and export files into it |
# MAGIC | 7 | Recover a dropped table |
# MAGIC | 8 | 🧠 Managed vs external — `DROP` behaviour |
# MAGIC | 9 | 🧠 Name resolution |

# COMMAND ----------

# MAGIC %run ../../Includes/_setup

# COMMAND ----------

# MAGIC %run ./_04_prepare

# COMMAND ----------

# DBTITLE 1,Setup for the challenge (run me - don't edit) - resets the mart schema
ensure_lab_catalog(verbose=False)
MART_SCHEMA = f"{SCHEMA_PREFIX}mart"
CH = f"{LAB_CATALOG}.{MART_SCHEMA}"
EXPORTS = f"/Volumes/{LAB_CATALOG}/{MART_SCHEMA}/exports"
restore_course_context()
spark.sql(f"DROP SCHEMA IF EXISTS {CH} CASCADE")
spark.sql("DROP VIEW IF EXISTS tv_premium")
_schem, _bands, _tmp, _ok6 = None, None, [], False     # filled by the check cells
answer_task8 = answer_task9 = None

print(f"CH      = {CH}\nEXPORTS = {EXPORTS}")


def check(label, condition):
    print(("✅ " if condition else "❌ ") + label)


def _exists(name):
    try:
        return spark.catalog.tableExists(f"{CH}.{name}")
    except Exception:
        return False


def _type(name):
    try:
        return table_type(f"{CH}.{name}")
    except Exception:
        return None

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 1 · The mart schema
# MAGIC 1. Create the schema **`CH`** with the comment **`ShopWave sales mart`**.
# MAGIC 2. Make it your **current** catalog and schema (two statements), so short names work in `%sql` cells.

# COMMAND ----------

# DBTITLE 1,Task 1 (you may use %sql cells instead)
# TODO

# COMMAND ----------

# DBTITLE 1,✅ Check 1
_schem = (spark.table(f"{LAB_CATALOG}.information_schema.schemata").where(f"schema_name = '{MART_SCHEMA}'").first()
          if spark.catalog.databaseExists(CH) else None)
check("schema exists with comment 'ShopWave sales mart'", _schem is not None and _schem["comment"] == "ShopWave sales mart")
check("current catalog/schema is the mart",
      tuple(spark.sql("SELECT current_catalog(), current_schema()").first()) == (LAB_CATALOG, MART_SCHEMA))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 2 · Managed products table
# MAGIC Create the **managed** table **`products`** in `CH` from the CSV files in `f"{dataset_path}/products-csv"`
# MAGIC (header row, `;` delimiter) with the columns `product_id`, `title`, `category` and `price` as **`DECIMAL(10,2)`**.
# MAGIC
# MAGIC 💡 `read_files()` needs a **literal** path, so either use Python with an f-string, or run `print(dataset_path)` and paste the path into a `%sql` cell.

# COMMAND ----------

# DBTITLE 1,Task 2
# TODO

# COMMAND ----------

# DBTITLE 1,✅ Check 2
check("products has 36 rows", _exists("products") and spark.table(f"{CH}.products").count() == 36)
check("products is a MANAGED table", _type("products") == "MANAGED")
check("price is DECIMAL(10,2)", _exists("products") and
      dict(spark.table(f"{CH}.products").dtypes).get("price") == "decimal(10,2)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 3 · A SQL UDF
# MAGIC Create the function **`price_band(price DOUBLE)`** in `CH` returning a STRING:
# MAGIC
# MAGIC | price | band |
# MAGIC |---|---|
# MAGIC | < 50 | `budget` |
# MAGIC | < 200 | `standard` |
# MAGIC | otherwise | `premium` |

# COMMAND ----------

# DBTITLE 1,Task 3
# TODO

# COMMAND ----------

# DBTITLE 1,✅ Check 3
try:
    _bands = tuple(spark.sql(f"SELECT {CH}.price_band(10), {CH}.price_band(100), {CH}.price_band(500)").first())
except Exception:
    _bands = None
check("price_band returns budget / standard / premium", _bands == ("budget", "standard", "premium"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 4 · A stored view
# MAGIC Create the view **`v_category_bands`** in `CH` with one row per (`category`, `band`) and the number of products
# MAGIC (`products`), using your UDF on the `products` table.

# COMMAND ----------

# DBTITLE 1,Task 4
# TODO

# COMMAND ----------

# DBTITLE 1,✅ Check 4
check("v_category_bands is a VIEW", _type("v_category_bands") == "VIEW")
check("the view covers all 36 products", _exists("v_category_bands") and
      spark.sql(f"SELECT sum(products) FROM {CH}.v_category_bands").first()[0] == 36)
check("the view has a band column", _exists("v_category_bands") and
      "band" in spark.table(f"{CH}.v_category_bands").columns)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 5 · A temporary view
# MAGIC Create the **temporary** view **`tv_premium`** with the `premium` products only.

# COMMAND ----------

# DBTITLE 1,Task 5
# TODO

# COMMAND ----------

# DBTITLE 1,✅ Check 5
_tmp = [r["viewName"] for r in spark.sql("SHOW VIEWS").where("isTemporary").collect()]
check("tv_premium is a temporary view", "tv_premium" in _tmp)
check("tv_premium only has premium products", "tv_premium" in _tmp and spark.sql(
    f"SELECT count_if({CH}.price_band(price) <> 'premium') FROM tv_premium").first()[0] == 0)
check("tv_premium is not in information_schema", "tv_premium" in _tmp and _type("tv_premium") is None)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 6 · Export to a volume
# MAGIC 1. Create the **managed volume** **`exports`** in `CH`.
# MAGIC 2. Write the content of `v_category_bands` as **CSV with a header** into the folder `f"{EXPORTS}/category_bands"`.

# COMMAND ----------

# DBTITLE 1,Task 6
# TODO

# COMMAND ----------

# DBTITLE 1,✅ Check 6
try:
    _exp = spark.read.option("header", "true").csv(f"{EXPORTS}/category_bands")
    _ok6 = "band" in _exp.columns and _exp.count() == spark.table(f"{CH}.v_category_bands").count()
except Exception:
    _ok6 = False
check("exports volume contains the category_bands CSV export", _ok6)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 7 · The accident
# MAGIC Run the next cell — a colleague drops your `products` table by mistake. Bring it back **with its data** (don't rebuild it from
# MAGIC the files), then prove that your view works again.
# MAGIC *Hint:* `SHOW TABLES DROPPED` lists what can be recovered. `UNDROP TABLE` restores the most recently dropped table of that name
# MAGIC (`… WITH ID '<tableId>'` picks an older one). ⚠️ Don't re-run the Setup cell before this task — it drops the whole schema, and a
# MAGIC table can only be undropped while its schema exists.

# COMMAND ----------

# DBTITLE 1,💥 The accident (run once)
spark.sql(f"DROP TABLE IF EXISTS {CH}.products")
print("products exists:", _exists("products"))

# COMMAND ----------

# DBTITLE 1,Task 7
# TODO

# COMMAND ----------

# DBTITLE 1,✅ Check 7
check("products is back with 36 rows", _exists("products") and spark.table(f"{CH}.products").count() == 36)
check("the view works again", _exists("v_category_bands") and spark.table(f"{CH}.v_category_bands").count() > 0)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 8 · 🧠 Managed vs external
# MAGIC `sales_ext` was created with `CREATE TABLE sales_ext (...) LOCATION 's3://acme-data/sales'`. Someone runs `DROP TABLE sales_ext`.
# MAGIC What happens?
# MAGIC
# MAGIC * **A** — The table and all files under `s3://acme-data/sales` are deleted immediately
# MAGIC * **B** — The table is removed from Unity Catalog; the files under `s3://acme-data/sales` stay where they are
# MAGIC * **C** — The statement fails: external tables must be dropped with `CASCADE`
# MAGIC * **D** — The files are kept for 7 days, then Unity Catalog deletes them

# COMMAND ----------

# DBTITLE 1,Task 8
answer_task8 = None   # "A", "B", "C" or "D"

# COMMAND ----------

# DBTITLE 1,✅ Check 8
check("Task 8 answer", answer_task8 == "B")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 9 · 🧠 Name resolution
# MAGIC A notebook runs `USE CATALOG prod;` then `USE SCHEMA sales;` and then `SELECT * FROM finance.budget`. Which table is read?
# MAGIC
# MAGIC * **A** — `prod.sales.budget`
# MAGIC * **B** — `prod.finance.budget`
# MAGIC * **C** — `hive_metastore.finance.budget`
# MAGIC * **D** — `sales.finance.budget`

# COMMAND ----------

# DBTITLE 1,Task 9
answer_task9 = None   # "A", "B", "C" or "D"

# COMMAND ----------

# DBTITLE 1,✅ Check 9
check("Task 9 answer", answer_task9 == "B")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🏁 Final score
# MAGIC Run every **✅ Check** cell first — the score uses their results.

# COMMAND ----------

# DBTITLE 1,Score
_final = {
    "Task 1": _schem is not None and _schem["comment"] == "ShopWave sales mart",
    "Task 2": _type("products") == "MANAGED",
    "Task 3": _bands == ("budget", "standard", "premium"),
    "Task 4": _type("v_category_bands") == "VIEW",
    "Task 5": "tv_premium" in _tmp,
    "Task 6": _ok6,
    "Task 7": _exists("products") and spark.table(f"{CH}.products").count() == 36,
    "Task 8": answer_task8 == "B",
    "Task 9": answer_task9 == "B",
}
for k, v in _final.items():
    print(("✅ " if v else "❌ ") + k)
print(f"\nScore: {sum(_final.values())} / {len(_final)}")
restore_course_context()
print("\nWhen you're done with Section 04, run the clean-up cell in 04-D (drop_lab_catalog()).")
