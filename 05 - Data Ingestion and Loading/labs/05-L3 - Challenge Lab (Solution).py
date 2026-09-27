# Databricks notebook source
# MAGIC %md
# MAGIC # 🏆 Lab 05-L3 · Challenge Lab — Data Ingestion & Loading — ✅ SOLUTION
# MAGIC > Try the challenge yourself first! This notebook contains the reference answers and runs end-to-end.
# MAGIC
# MAGIC **Time:** ~50 min · **Compute:** Serverless · **Story:** a new **supplier price feed** (JSON, one file per day) must be
# MAGIC onboarded into ShopWave's lakehouse — plus two quick clean-up jobs on older sources.
# MAGIC
# MAGIC Write the SQL or Python yourself. Replace every `None` / `# TODO`, then run each **✅ Check**.
# MAGIC Paths: `SUPPLIER_LANDING`, `dataset_path` (printed by the setup cell). `read_files()` / `COPY INTO` need **literal** paths, so
# MAGIC the easiest way is Python f-strings: `spark.sql(f"... '{SUPPLIER_LANDING}' ...")` (or `run_sql`, which also prints the SQL).
# MAGIC
# MAGIC | Task | Skill |
# MAGIC |---|---|
# MAGIC | 1 | Query a file **directly** |
# MAGIC | 2 | `COPY INTO` a schemaless table, adding the **source file name** |
# MAGIC | 3 | Incremental re-runs + a **new field** (schema evolution) |
# MAGIC | 4 | Load JSON as **`VARIANT`** and query a nested value |
# MAGIC | 5 | Find bad rows with **schema hints + rescued data** |
# MAGIC | 6 | Build a table from **text** log files |
# MAGIC | 7 | 🧠 Choosing an ingestion method |
# MAGIC | 8 | 🧠 `COPY INTO` idempotency |
# MAGIC | 9 | 🧠 File metadata |

# COMMAND ----------

# MAGIC %run ../../Includes/_setup

# COMMAND ----------

# MAGIC %run ./_05_prepare

# COMMAND ----------

# DBTITLE 1,Setup for the challenge (run me - don't edit) - resets the supplier feed
SUPPLIER_LANDING = f"{dataset_path}/supplier-landing"
for _t in ("lab05_supplier_prices", "lab05_supplier_raw", "lab05_app_errors"):
    spark.sql(f"DROP TABLE IF EXISTS {_t}")
reset_supplier_landing()                     # supplier-landing/ now holds batch 01 only
print("SUPPLIER_LANDING =", SUPPLIER_LANDING)

supplier_file_rows = heaviest_product = messy_bad_ids = None
answer_task7 = answer_task8 = answer_task9 = None
_rows3 = _rows3b = None
_expected4, _errors = "(run check 4)", -1         # filled by the check cells


def check(label, condition):
    print(("✅ " if condition else "❌ ") + label)


def _exists(t):
    return spark.catalog.tableExists(t)


def _count(t):
    return spark.table(t).count() if _exists(t) else -1

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 1 · Peek at the first file
# MAGIC Using a **direct file query** (``json.`…` ``), count the rows in `supplier_prices_01.json` inside `SUPPLIER_LANDING`
# MAGIC and store the number in `supplier_file_rows`.

# COMMAND ----------

# DBTITLE 1,Task 1 · SOLUTION
supplier_file_rows = run_sql(f"SELECT * FROM json.`{SUPPLIER_LANDING}/supplier_prices_01.json`").count()
print("rows:", supplier_file_rows)

# COMMAND ----------

# DBTITLE 1,✅ Check 1
check("supplier_file_rows = 12", supplier_file_rows == 12)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 2 · First load with `COPY INTO`
# MAGIC 1. Create an **empty, schemaless** table `lab05_supplier_prices`.
# MAGIC 2. `COPY INTO` it from `SUPPLIER_LANDING` (JSON). Load **all columns plus** a column **`source_file`** with the file name.
# MAGIC    Let the table schema evolve.

# COMMAND ----------

# DBTITLE 1,Task 2 · SOLUTION
run_sql("CREATE TABLE IF NOT EXISTS lab05_supplier_prices")          # schemaless - COPY INTO infers the schema

supplier_copy = f"""
    COPY INTO lab05_supplier_prices
    FROM (SELECT *, _metadata.file_name AS source_file FROM '{SUPPLIER_LANDING}')
    FILEFORMAT = JSON
    FORMAT_OPTIONS ('mergeSchema' = 'true')
    COPY_OPTIONS ('mergeSchema' = 'true')"""
display(run_sql(supplier_copy))

# COMMAND ----------

# DBTITLE 1,✅ Check 2
check("lab05_supplier_prices has 12 rows", _count("lab05_supplier_prices") == 12)
check("source_file column holds supplier_prices_01.json", _exists("lab05_supplier_prices") and
      "source_file" in spark.table("lab05_supplier_prices").columns and
      spark.table("lab05_supplier_prices").where("source_file = 'supplier_prices_01.json'").count() == 12)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 3 · Two more days
# MAGIC Run the next cell (two files arrive — the third one has a **new field `discount`**). Then run your `COPY INTO` from Task 2
# MAGIC again so that the table has all rows **and** a `discount` column. Store the row count in `_rows3`, run the load **once more**,
# MAGIC and store the count again in `_rows3b`.

# COMMAND ----------

# DBTITLE 1,📦 Two more files arrive (run once)
_ = land_supplier_batch(2)

# COMMAND ----------

# DBTITLE 1,Task 3 · SOLUTION
display(run_sql(supplier_copy))                        # loads only the 2 new files; mergeSchema adds `discount`
_rows3 = spark.table("lab05_supplier_prices").count()
display(run_sql(supplier_copy))                        # nothing new -> 0 rows inserted
_rows3b = spark.table("lab05_supplier_prices").count()
print(_rows3, _rows3b)

# COMMAND ----------

# DBTITLE 1,✅ Check 3
check("36 rows after the new files", _rows3 == 36)
check("a second run added nothing (idempotent)", _rows3b == 36)
check("discount column exists", _exists("lab05_supplier_prices") and "discount" in spark.table("lab05_supplier_prices").columns)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 4 · The same feed as `VARIANT`
# MAGIC 1. Create `lab05_supplier_raw` with one column **`raw VARIANT`** and load **all** files of `SUPPLIER_LANDING` into it with `COPY INTO`.
# MAGIC 2. Using the VARIANT column, find the `product_id` with the **largest `specs.weight_kg`** and store it in `heaviest_product`.
# MAGIC
# MAGIC 💡 A VARIANT value can't be used directly in `ORDER BY` / comparisons — cast it first (`raw:specs.weight_kg::double`).

# COMMAND ----------

# DBTITLE 1,Task 4 · SOLUTION
run_sql("CREATE TABLE IF NOT EXISTS lab05_supplier_raw (raw VARIANT)")
run_sql(f"""
    COPY INTO lab05_supplier_raw
    FROM '{SUPPLIER_LANDING}'
    FILEFORMAT = JSON
    FORMAT_OPTIONS ('singleVariantColumn' = 'raw')""")
heaviest_product = spark.sql("""
    SELECT raw:product_id::string AS product_id
    FROM lab05_supplier_raw
    ORDER BY raw:specs.weight_kg::double DESC
    LIMIT 1""").first()["product_id"]
print("heaviest:", heaviest_product)

# COMMAND ----------

# DBTITLE 1,✅ Check 4
_src = spark.read.json(SUPPLIER_LANDING)
_expected4 = _src.orderBy(F.col("specs.weight_kg").desc()).first()["product_id"]
check("lab05_supplier_raw has 36 rows", _count("lab05_supplier_raw") == 36)
check("raw is a VARIANT column", _exists("lab05_supplier_raw") and
      dict(spark.table("lab05_supplier_raw").dtypes).get("raw") == "variant")
check(f"heaviest product found", heaviest_product == _expected4)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 5 · Which rows are broken?
# MAGIC Read `f"{dataset_path}/returns-messy"` with `read_files` so that `refund_amount` is a **DOUBLE** and `return_date` a **DATE**
# MAGIC (infer everything else). Put the **sorted list** of `return_id`s whose values didn't fit into `messy_bad_ids`.

# COMMAND ----------

# DBTITLE 1,Task 5 · SOLUTION
messy = run_sql(f"""
    SELECT * FROM read_files('{dataset_path}/returns-messy', format => 'csv', header => true,
                             schemaHints => 'refund_amount DOUBLE, return_date DATE')""")
messy_bad_ids = sorted(r["return_id"] for r in messy.where("_rescued_data IS NOT NULL").select("return_id").collect())
print(messy_bad_ids)

# COMMAND ----------

# DBTITLE 1,✅ Check 5
check("three bad rows identified", messy_bad_ids == ["M0003", "M0007", "M0010"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 6 · Errors from the application logs
# MAGIC Create the table **`lab05_app_errors`** from the text files in `f"{dataset_path}/app-logs"` with **only the `ERROR` lines** and
# MAGIC the columns `ts` (string is fine), `event`, `ms` (INT) and `source_file` (file name).
# MAGIC A line looks like: `2026-07-01T09:07:00Z ERROR checkout order=O000123 ms=532`.
# MAGIC
# MAGIC 💡 Inside a **SQL string literal** a backslash is consumed (`'\d'` arrives as `d`), so prefer character classes such as
# MAGIC `[0-9]+` and `[^ ]+` in SQL regexes — or use `F.regexp_extract` in Python, where a raw string `r"ms=(\d+)"` works (as in 05-L1).

# COMMAND ----------

# DBTITLE 1,Task 6 · SOLUTION
line_re = "^([^ ]+) ([A-Z]+) ([a-z_]+)"          # ts, level, event - character classes avoid backslash escaping
run_sql(f"""
    CREATE OR REPLACE TABLE lab05_app_errors AS
    SELECT regexp_extract(value, '{line_re}', 1)                    AS ts,
           regexp_extract(value, '{line_re}', 3)                    AS event,
           CAST(regexp_extract(value, 'ms=([0-9]+)', 1) AS INT)     AS ms,
           _metadata.file_name                                     AS source_file
    FROM read_files('{dataset_path}/app-logs', format => 'text')
    WHERE regexp_extract(value, '{line_re}', 2) = 'ERROR'""")
display(spark.table("lab05_app_errors"))

# COMMAND ----------

# DBTITLE 1,✅ Check 6
_logs = spark.read.text(f"{dataset_path}/app-logs")
_errors = _logs.where(F.col("value").contains(" ERROR ")).count()
check(f"lab05_app_errors has the {_errors} ERROR lines", _count("lab05_app_errors") == _errors and _errors > 0)
check("columns ts, event, ms, source_file", _exists("lab05_app_errors") and
      {"ts", "event", "ms", "source_file"} <= set(spark.table("lab05_app_errors").columns))
check("ms is an integer", _exists("lab05_app_errors") and dict(spark.table("lab05_app_errors").dtypes).get("ms") == "int")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 7 · 🧠 Choose the ingestion method
# MAGIC Sales operations wants the **Salesforce** `Opportunity` and `Account` objects in Unity Catalog **every day**, with
# MAGIC **as little code and maintenance as possible**, including automatic handling of schema changes and API limits. Best choice?
# MAGIC
# MAGIC * **A** — A notebook calling the Salesforce REST API, scheduled by a Lakeflow Job
# MAGIC * **B** — A Lakeflow Connect **managed connector** for Salesforce
# MAGIC * **C** — `COPY INTO` from the Salesforce API URL
# MAGIC * **D** — Auto Loader pointed at Salesforce

# COMMAND ----------

# DBTITLE 1,Task 7 · SOLUTION
answer_task7 = "B"
# Managed connectors handle auth, incremental reads, API limits, retries and schema evolution for you (most automation).
# A works but YOU maintain it. COPY INTO and Auto Loader read files from cloud storage, not SaaS APIs.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 8 · 🧠 `COPY INTO` idempotency
# MAGIC Yesterday `COPY INTO` loaded `prices_0901.json`. Today the supplier **overwrites** that file with corrected prices (same name)
# MAGIC and the scheduled `COPY INTO` runs again (default options). What happens?
# MAGIC
# MAGIC * **A** — The corrected file is loaded again, replacing yesterday's rows
# MAGIC * **B** — The corrected file is loaded again, so both versions are in the table
# MAGIC * **C** — The file is **skipped**, because it was already loaded — you must fix the rows another way (or reload with `'force' = 'true'` into a cleaned table)
# MAGIC * **D** — `COPY INTO` fails with a duplicate-file error

# COMMAND ----------

# DBTITLE 1,Task 8 · SOLUTION
answer_task8 = "C"
# COPY INTO tracks loaded files and skips them on later runs - even if they were modified after loading.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 9 · 🧠 File metadata
# MAGIC An old notebook uses `input_file_name()` and fails on serverless compute. What should replace it?
# MAGIC
# MAGIC * **A** — `current_file()`
# MAGIC * **B** — `_metadata.file_path` (or `_metadata.file_name`)
# MAGIC * **C** — `dbutils.fs.ls()` inside the query
# MAGIC * **D** — `DESCRIBE DETAIL`

# COMMAND ----------

# DBTITLE 1,Task 9 · SOLUTION
answer_task9 = "B"
# legacy-demo: input_file_name() is deprecated and not available on Unity Catalog / serverless;
# use the hidden _metadata column.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🏁 Final score
# MAGIC Run every **✅ Check** cell first — the score uses their results.

# COMMAND ----------

# DBTITLE 1,Score
_final = {
    "Task 1": supplier_file_rows == 12,
    "Task 2": _count("lab05_supplier_prices") >= 12 and "source_file" in (spark.table("lab05_supplier_prices").columns
                                                                         if _exists("lab05_supplier_prices") else []),
    "Task 3": _rows3 == 36 and _rows3b == 36,
    "Task 4": heaviest_product == _expected4,
    "Task 5": messy_bad_ids == ["M0003", "M0007", "M0010"],
    "Task 6": _count("lab05_app_errors") == _errors and _errors > 0,
    "Task 7": answer_task7 == "B",
    "Task 8": answer_task8 == "C",
    "Task 9": answer_task9 == "B",
}
for k, v in _final.items():
    print(("✅ " if v else "❌ ") + k)
print(f"\nScore: {sum(_final.values())} / {len(_final)}")
