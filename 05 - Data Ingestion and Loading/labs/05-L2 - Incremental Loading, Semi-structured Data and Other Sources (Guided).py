# Databricks notebook source
# MAGIC %md
# MAGIC # 🧪 Lab 05-L2 · Incremental Loading with `COPY INTO`, Semi-structured Data & Other Sources (Guided)
# MAGIC **Time:** ~60 min · **Compute:** Serverless (Free Edition) or any Unity Catalog compute
# MAGIC
# MAGIC ShopWave's returns system drops **one CSV file per day** into a volume. You'll load it **incrementally** — each file exactly
# MAGIC once — then ingest **nested JSON as `VARIANT`**, land data from a **REST API**, and explore the **upload** and
# MAGIC **Lakeflow Connect** screens.
# MAGIC
# MAGIC | Part | You will… |
# MAGIC |---|---|
# MAGIC | 1 | Reset the landing folder and create a **schemaless** target table |
# MAGIC | 2 | Load the first file with `COPY INTO` |
# MAGIC | 3 | Prove **idempotency**: re-runs skip loaded files; new files are picked up |
# MAGIC | 4 | Handle a **new column** with `mergeSchema` |
# MAGIC | 5 | `VALIDATE`, `PATTERN`/`FILES` and `force` |
# MAGIC | 6 | Transform while loading: `COPY INTO … FROM (SELECT …)` with **file metadata** |
# MAGIC | 7 | Ingest nested JSON as **`VARIANT`** and query it |
# MAGIC | 8 | A **REST API** → volume → table (and the JDBC pattern) |
# MAGIC | 9 | 🧭 UI: **upload a local file** and tour **Lakeflow Connect** |
# MAGIC | 10 | ✅ Automatic checks |

# COMMAND ----------

# MAGIC %run ../../Includes/_setup

# COMMAND ----------

# MAGIC %run ./_05_prepare

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1 · A fresh start
# MAGIC The helper `land_returns()` simulates the source system: each call "delivers" the next daily file into `returns-landing/`.

# COMMAND ----------

# DBTITLE 1,Reset the landing folder and the lab tables
landing = f"{dataset_path}/returns-landing"
for t in ("lab05_returns", "lab05_returns_force_demo", "lab05_returns_audit", "lab05_reviews_raw", "lab05_fx_raw"):
    spark.sql(f"DROP TABLE IF EXISTS {t}")
reset_returns_landing()
display(run_sql(f"LIST '{landing}'"))

# COMMAND ----------

# MAGIC %md
# MAGIC `COPY INTO` needs an existing **Delta** table. You may define the columns yourself — or create an **empty, schemaless**
# MAGIC table and let `COPY INTO` infer the schema from the files (`mergeSchema`).

# COMMAND ----------

# DBTITLE 1,CREATE TABLE IF NOT EXISTS - no columns yet
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS lab05_returns

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2 · The first load
# MAGIC
# MAGIC ```sql
# MAGIC COPY INTO <table>
# MAGIC FROM '<folder or file>'
# MAGIC FILEFORMAT = CSV | JSON | PARQUET | AVRO | ORC | TEXT | BINARYFILE
# MAGIC [VALIDATE [ALL | n ROWS]]
# MAGIC [FILES = ('a.csv', 'b.csv') | PATTERN = '<glob>']
# MAGIC [FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'true', 'mergeSchema' = 'true', ...)]   -- how to READ the files
# MAGIC [COPY_OPTIONS ('mergeSchema' = 'true', 'force' = 'false')]                                  -- how to WRITE the table
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,COPY INTO #1
copy_sql = f"""
    COPY INTO lab05_returns
    FROM '{landing}'
    FILEFORMAT = CSV
    FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'true', 'mergeSchema' = 'true')
    COPY_OPTIONS ('mergeSchema' = 'true')"""
m1 = copy_into_metrics(run_sql(copy_sql))
print("\nmetrics:", m1)
rows_after_1 = spark.table("lab05_returns").count()
print("rows in lab05_returns:", rows_after_1)
spark.table("lab05_returns").printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC `COPY INTO` returns one row of metrics (`num_affected_rows`, `num_inserted_rows`, `num_skipped_corrupt_files`).
# MAGIC The empty table now has the columns and types inferred from the CSV header and values.
# MAGIC
# MAGIC ## Part 3 · Idempotency — the reason `COPY INTO` exists

# COMMAND ----------

# DBTITLE 1,Run the SAME statement again: nothing new -> nothing loaded
m2 = copy_into_metrics(run_sql(copy_sql))
rows_after_rerun = spark.table("lab05_returns").count()
print("\nmetrics:", m2, "| rows still:", rows_after_rerun)

# COMMAND ----------

# DBTITLE 1,Two new files arrive -> only they are loaded
land_returns(2)
m3 = copy_into_metrics(run_sql(copy_sql))
rows_after_3 = spark.table("lab05_returns").count()
print("\nmetrics:", m3, "| rows now:", rows_after_3)

# COMMAND ----------

# MAGIC %md
# MAGIC `COPY INTO` remembers **which files** it has loaded into this table (in the table's metadata) and **skips them** on every
# MAGIC later run — even if a file was modified afterwards. That makes it safe to **schedule** (e.g. hourly in a Lakeflow Job) and to
# MAGIC **retry** after a failure: you never get duplicates from re-running it.
# MAGIC
# MAGIC > 🎯 **Exam:** "incrementally load new files from cloud storage into a UC table, safely re-runnable, SQL only" → `COPY INTO`.
# MAGIC > Thousands of files per run → fine. **Millions** of files, or continuous arrival → **Auto Loader** (Section 06).
# MAGIC
# MAGIC ## Part 4 · A new column appears
# MAGIC From batch 4 on, the source system also sends `refund_method`.

# COMMAND ----------

# DBTITLE 1,Land batch 4 and load it with mergeSchema
land_returns(1)
print(dbutils.fs.head(f"{landing}/returns_2026_07_04.csv", 160), "...")
m4 = copy_into_metrics(run_sql(copy_sql))
rows_after_4 = spark.table("lab05_returns").count()
display(spark.sql("SELECT return_date, count(*) AS rows, count(refund_method) AS with_method "
                  "FROM lab05_returns GROUP BY return_date ORDER BY return_date"))

# COMMAND ----------

# MAGIC %md
# MAGIC The table **evolved**: `refund_method` was added, older rows show `NULL`. The two `mergeSchema` switches do different jobs:
# MAGIC
# MAGIC | Where | Meaning |
# MAGIC |---|---|
# MAGIC | `FORMAT_OPTIONS ('mergeSchema' = 'true')` | when **reading**, merge the schemas of all files in this run |
# MAGIC | `COPY_OPTIONS ('mergeSchema' = 'true')` | when **writing**, allow new columns to be added to the **table** |
# MAGIC
# MAGIC Without the `COPY_OPTIONS` one, Delta **schema enforcement** rejects files with extra columns.
# MAGIC
# MAGIC ## Part 5 · `VALIDATE`, `PATTERN` / `FILES` and `force`

# COMMAND ----------

# MAGIC %md
# MAGIC The result of `VALIDATE` is a **preview** of what would be loaded (up to 50 rows; here 5) — nothing is written.

# COMMAND ----------

# DBTITLE 1,VALIDATE: check the next file WITHOUT writing
land_returns(1)                                        # batch 5 arrives
display(run_sql(f"""
    COPY INTO lab05_returns
    FROM '{landing}'
    FILEFORMAT = CSV
    VALIDATE 5 ROWS
    FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'true')"""))
rows_after_validate = spark.table("lab05_returns").count()
print("rows after VALIDATE (unchanged):", rows_after_validate)

# COMMAND ----------

# DBTITLE 1,PATTERN: load only the files you name
m5 = copy_into_metrics(run_sql(f"""
    COPY INTO lab05_returns
    FROM '{landing}'
    FILEFORMAT = CSV
    PATTERN = 'returns_2026_07_05*.csv'
    FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'true', 'mergeSchema' = 'true')
    COPY_OPTIONS ('mergeSchema' = 'true')"""))
rows_after_5 = spark.table("lab05_returns").count()
print("\nmetrics:", m5, "| rows now:", rows_after_5)

# COMMAND ----------

# MAGIC %md
# MAGIC `force = true` **disables idempotency**: every matching file is loaded again, **creating duplicates**. Use it only on purpose
# MAGIC (e.g. after truncating a table). Watch on a scratch table:

# COMMAND ----------

# DBTITLE 1,force = true loads the same file again
spark.sql("CREATE TABLE IF NOT EXISTS lab05_returns_force_demo")
one_file = f"""
    COPY INTO lab05_returns_force_demo
    FROM '{landing}'
    FILEFORMAT = CSV
    FILES = ('returns_2026_07_01.csv')
    FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'true')
    COPY_OPTIONS ('mergeSchema' = 'true'{{force}})"""
run_sql(one_file.format(force=""))
run_sql(one_file.format(force=""))                               # skipped
run_sql(one_file.format(force=", 'force' = 'true'"))             # loaded again!
force_rows = spark.table("lab05_returns_force_demo").count()
force_distinct = spark.table("lab05_returns_force_demo").select("return_id").distinct().count()
print(f"\nrows: {force_rows} | distinct return_id: {force_distinct}  -> duplicates created by force")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 6 · Transform while loading + file metadata
# MAGIC `COPY INTO` accepts a `SELECT` over the source path: rename, cast, add columns — including the **`_metadata`** file columns.
# MAGIC (`GROUP BY` isn't allowed there; keep heavy transformations for later layers.)

# COMMAND ----------

# DBTITLE 1,COPY INTO ... FROM (SELECT ... _metadata ...)
spark.sql("CREATE TABLE IF NOT EXISTS lab05_returns_audit")
run_sql(f"""
    COPY INTO lab05_returns_audit
    FROM (SELECT return_id, order_id, product_id,
                 CAST(return_date AS DATE)       AS return_date,
                 reason,
                 CAST(refund_amount AS DOUBLE)   AS refund_amount,
                 _metadata.file_name             AS source_file,
                 current_timestamp()             AS loaded_at
          FROM '{landing}')
    FILEFORMAT = CSV
    FORMAT_OPTIONS ('header' = 'true')
    COPY_OPTIONS ('mergeSchema' = 'true')""")
display(spark.sql("SELECT source_file, count(*) AS rows FROM lab05_returns_audit GROUP BY source_file ORDER BY 1"))

# COMMAND ----------

# MAGIC %md
# MAGIC > 💡 Here we listed the columns, so `refund_method` is simply not selected — the `SELECT` controls the table's shape.
# MAGIC
# MAGIC ## Part 7 · Semi-structured data as `VARIANT`
# MAGIC Reviews are nested JSON: objects (`review`, `meta`), arrays (`tags`, `photos`) and fields that appear only sometimes
# MAGIC (`photos`, `verified_purchase`). Three ways to store them:
# MAGIC
# MAGIC | Store as | Pros | Cons |
# MAGIC |---|---|---|
# MAGIC | **STRING** (raw JSON text) | never fails | re-parsed on every query (slow), `:` paths only |
# MAGIC | **STRUCT / ARRAY** (inferred or `from_json`) | typed, fast | schema must be known; new fields need schema evolution |
# MAGIC | **`VARIANT`** | flexible **and** fast (binary-encoded), any shape | cast values when you read them (`::type`) |
# MAGIC
# MAGIC `COPY INTO` (and Auto Loader, `read_files`) can load every JSON record into **one `VARIANT` column** with `singleVariantColumn`.

# COMMAND ----------

# DBTITLE 1,COPY INTO a single VARIANT column
spark.sql("CREATE TABLE IF NOT EXISTS lab05_reviews_raw (raw VARIANT) COMMENT 'Reviews as VARIANT (bronze)'")
run_sql(f"""
    COPY INTO lab05_reviews_raw
    FROM '{dataset_path}/reviews-json'
    FILEFORMAT = JSON
    FORMAT_OPTIONS ('singleVariantColumn' = 'raw')""")
display(spark.table("lab05_reviews_raw").limit(3))

# COMMAND ----------

# DBTITLE 1,Query VARIANT: colon paths, dots, [index] and ::casts
# MAGIC %sql
# MAGIC SELECT raw:review_id::string          AS review_id,
# MAGIC        raw:rating::int                AS rating,
# MAGIC        raw:review.title::string       AS title,
# MAGIC        raw:meta.device::string        AS device,
# MAGIC        raw:photos[0].url::string      AS first_photo,
# MAGIC        raw:verified_purchase::boolean AS verified
# MAGIC FROM lab05_reviews_raw
# MAGIC ORDER BY review_id
# MAGIC LIMIT 10

# COMMAND ----------

# DBTITLE 1,Optional fields are simply NULL - no schema change needed
variant_stats = spark.sql("""
    SELECT count(*)                          AS reviews,
           count(raw:photos)                 AS with_photos,
           count(raw:verified_purchase)      AS verified,
           round(avg(raw:rating::int), 2)    AS avg_rating
    FROM lab05_reviews_raw""").first().asDict()
print(variant_stats)

# COMMAND ----------

# DBTITLE 1,Discover the structure: schema_of_variant_agg
# MAGIC %sql
# MAGIC SELECT schema_of_variant_agg(raw) AS merged_schema FROM lab05_reviews_raw

# COMMAND ----------

# DBTITLE 1,Explode an array inside a VARIANT
# MAGIC %sql
# MAGIC SELECT raw:review_id::string AS review_id, t.value::string AS tag
# MAGIC FROM lab05_reviews_raw, LATERAL variant_explode(raw:tags) AS t
# MAGIC ORDER BY review_id
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC > 🎯 Remember the difference: `profile:address.city` on a **STRING** column parses JSON text on every query (Section 03/04);
# MAGIC > on a **`VARIANT`** column the same syntax reads a binary-encoded value. `parse_json(json_string)` converts text to `VARIANT`,
# MAGIC > `from_json(json_string, 'schema')` converts it to a typed `STRUCT`. Field names in paths are **case-sensitive**.
# MAGIC > Deep JSON transformations (explode, `from_json`, higher-order functions) come in Section 07.
# MAGIC
# MAGIC ## Part 8 · Other sources: REST APIs and JDBC
# MAGIC The exam expects the **pattern**: a notebook (scheduled by a Lakeflow Job) calls the source, **lands the raw response in cloud
# MAGIC storage / a volume**, then an idempotent load (`COPY INTO`, Auto Loader) moves it into a Unity Catalog table.

# COMMAND ----------

# DBTITLE 1,Call a REST API (falls back to a sample if the internet is blocked) and land the raw JSON
import json

api_url = "https://api.frankfurter.dev/v1/latest?base=USD&symbols=EUR,GBP,SAR,AED,JPY"
try:
    import requests                                           # pre-installed on Databricks compute
    resp = requests.get(api_url, timeout=10)
    resp.raise_for_status()
    payload, source = resp.json(), "live API"
except Exception as e:
    print("ℹ️ API not reachable from this compute (Free Edition allows only trusted domains) - using a sample response.",
          (str(e).splitlines() or [repr(e)])[0][:100])
    payload = {"amount": 1.0, "base": "USD", "date": "2026-09-25",
               "rates": {"EUR": 0.92, "GBP": 0.79, "SAR": 3.75, "AED": 3.67, "JPY": 148.2}}
    source = "sample"

fx_landing = f"{files_path}/fx-api"
fx_file = f"{fx_landing}/fx_{payload['date']}.json"
dbutils.fs.put(fx_file, json.dumps(payload), True)          # 1. land the RAW response, one file per call
print(f"Landed {fx_file} ({source})")

spark.sql("CREATE TABLE IF NOT EXISTS lab05_fx_raw (raw VARIANT)")
run_sql(f"""
    COPY INTO lab05_fx_raw
    FROM '{fx_landing}'
    FILEFORMAT = JSON
    FORMAT_OPTIONS ('singleVariantColumn' = 'raw')""")    # 2. idempotent load into a UC table

display(spark.sql("""
    SELECT raw:base::string AS base, raw:date::date AS rate_date, r.key AS currency, r.value::double AS rate
    FROM lab05_fx_raw, LATERAL variant_explode(raw:rates) AS r
    ORDER BY rate_date, currency"""))

# COMMAND ----------

# MAGIC %md
# MAGIC **Credentials never go in code.** Store them in a secret scope and read them at run time:
# MAGIC `token = dbutils.secrets.get(scope="shopwave", key="api_token")` (values are redacted in notebook output).
# MAGIC
# MAGIC **JDBC / ODBC.** On **classic** compute a notebook can read a database over JDBC and land the result:
# MAGIC
# MAGIC ```python
# MAGIC jdbc_df = (spark.read.format("jdbc")
# MAGIC              .option("url", "jdbc:postgresql://db.example.com:5432/erp")
# MAGIC              .option("dbtable", "public.suppliers")          # or .option("query", "SELECT ...")
# MAGIC              .option("user", dbutils.secrets.get("erp", "user"))
# MAGIC              .option("password", dbutils.secrets.get("erp", "password"))
# MAGIC              .load())
# MAGIC jdbc_df.write.mode("overwrite").saveAsTable("main.bronze.suppliers")
# MAGIC ```
# MAGIC
# MAGIC On **serverless** compute the URL-and-driver style above isn't supported. Use a Unity Catalog **connection** instead — a
# MAGIC **JDBC connection** (preview: `spark.read.format("jdbc").option("databricks.connection", "erp_conn").option("query", "SELECT …")`,
# MAGIC driver JAR stored in a volume), Lakehouse Federation, or a Lakeflow Connect database connector (CDC).
# MAGIC
# MAGIC **ODBC** works the same way from Python on classic compute (e.g. `pyodbc.connect(conn_str)` → pandas → `spark.createDataFrame`)
# MAGIC for sources that only offer ODBC. In the other direction, BI tools and applications use the **Databricks JDBC/ODBC drivers**
# MAGIC to query SQL warehouses.
# MAGIC
# MAGIC ## Part 9 · 🧭 UI: upload a local file and tour Lakeflow Connect
# MAGIC ### 9a · Create a table from a file on your laptop
# MAGIC 1. Download a sample file: sidebar **Catalog** ▸ your catalog ▸ `shopwave` ▸ **Volumes** ▸ `raw` ▸ `returns-staging` ▸
# MAGIC    click `returns_2026_07_01.csv` ▸ **⋮ ▸ Download**.
# MAGIC 2. Sidebar **+ New ▸ Add or upload data ▸ Create or modify a table**. Drag the file in.
# MAGIC 3. Destination: your catalog ▸ schema **`shopwave`** ▸ table name **`lab05_returns_upload`**.
# MAGIC 4. Check the **preview** (up to 50 rows): header detected, delimiter `,`, column types (change `refund_amount` to DOUBLE if needed).
# MAGIC 5. Click **Create table**. A **managed Delta table** is created (limits: up to 10 files, 2 GB total; CSV, TSV, JSON, Avro, Parquet, text).
# MAGIC
# MAGIC Alternatively **Upload files to a volume** puts the raw file into a volume (up to 5 GB per file in the UI) — then load it with SQL.
# MAGIC
# MAGIC ### 9b · Tour Lakeflow Connect (look, don't create)
# MAGIC 1. **+ New ▸ Add or upload data**. Besides the upload tiles you see **connectors**: databases (SQL Server, PostgreSQL, MySQL, Oracle),
# MAGIC    SaaS apps (Salesforce, Workday, ServiceNow, Google Analytics…), files (SharePoint, Google Drive), message buses (Kafka).
# MAGIC 2. Open one (e.g. **Salesforce** or **SQL Server**) and read the wizard steps: **connection** (a Unity Catalog securable holding
# MAGIC    the credentials) → **ingestion gateway** (databases only) → **source tables** → **destination catalog/schema** → **schedule**
# MAGIC    (and history tracking **SCD type 1 / type 2**).
# MAGIC 3. Cancel — managed connectors need a real source system. Availability varies by workspace/edition; in Free Edition some tiles
# MAGIC    may be disabled.
# MAGIC
# MAGIC ## Part 10 · ✅ Automatic checks

# COMMAND ----------

# DBTITLE 1,Check your work
_ret = spark.table("lab05_returns")
_checks = {
    "first COPY INTO loaded batch 01 (40 rows)": rows_after_1 == 40,
    "re-running COPY INTO loaded nothing": rows_after_rerun == 40,
    "two new files -> 120 rows": rows_after_3 == 120,
    "mergeSchema added refund_method": "refund_method" in _ret.columns and rows_after_4 == 160,
    "VALIDATE wrote nothing": rows_after_validate == 160,
    "PATTERN load -> 200 rows, no duplicates": rows_after_5 == 200 and _ret.select("return_id").distinct().count() == 200,
    "force created duplicates (80 rows, 40 distinct)": (force_rows, force_distinct) == (80, 40),
    "audit table has 5 source files": spark.table("lab05_returns_audit").select("source_file").distinct().count() == 5,
    "60 reviews loaded as VARIANT, 20 with photos": (variant_stats["reviews"], variant_stats["with_photos"]) == (60, 20),
    "FX response landed and loaded": spark.table("lab05_fx_raw").count() >= 1,
}
for name, ok in _checks.items():
    print(("✅ " if ok else "❌ ") + name)
_up = spark.catalog.tableExists("lab05_returns_upload")
print(("✅ " if _up else "➖ ") + "optional UI upload: lab05_returns_upload " + ("exists" if _up else "not created (Part 9a)"))
print("\n🎉 Lab complete - next: 05-L3 · Challenge Lab")
