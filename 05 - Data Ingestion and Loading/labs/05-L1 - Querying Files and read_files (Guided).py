# Databricks notebook source
# MAGIC %md
# MAGIC # 🧪 Lab 05-L1 · Querying Files Directly & `read_files()` (Guided)
# MAGIC **Time:** ~50 min · **Compute:** Serverless (Free Edition) or any Unity Catalog compute
# MAGIC
# MAGIC Before you load data into tables you have to **look at it**. Databricks can query files in place, and `read_files()` gives
# MAGIC you full control over formats, schemas, bad records and file metadata.
# MAGIC
# MAGIC | Part | You will… |
# MAGIC |---|---|
# MAGIC | 1 | See which source files arrived |
# MAGIC | 2 | Query JSON files directly: **one file, a wildcard, a whole directory** |
# MAGIC | 3 | Compare **self-describing** (Parquet, JSON) and **non-self-describing** (CSV) formats |
# MAGIC | 4 | Read **text** logs and **binary** images (unstructured data) |
# MAGIC | 5 | Capture **file metadata** with `_metadata` (and see why `input_file_name()` is gone) |
# MAGIC | 6 | Master `read_files()`: filters, schema inference, **schema hints**, **rescued data**, `multiLine` |
# MAGIC | 7 | Create a **bronze table** from files with CTAS |
# MAGIC | 8 | 🕰️ Legacy patterns you must recognise: temp view `USING CSV`, external CSV tables, `REFRESH TABLE` |
# MAGIC | 9 | The same with the **DataFrame reader** (Python) |
# MAGIC | 10 | ✅ Automatic checks |
# MAGIC
# MAGIC > File paths contain your catalog name, so most cells are Python f-strings that **print the SQL** they run (`run_sql`).
# MAGIC > In a `%sql` cell you would type the path literally, e.g. ``SELECT * FROM json.`/Volumes/workspace/shopwave/raw/customers-json` ``.

# COMMAND ----------

# MAGIC %run ../../Includes/_setup

# COMMAND ----------

# MAGIC %run ./_05_prepare

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1 · What arrived?
# MAGIC Source files live in the Unity Catalog **volume** `raw`. `LIST` works on volume paths in SQL; `dbutils.fs.ls` in Python.

# COMMAND ----------

# DBTITLE 1,LIST the raw volume
display(run_sql(f"LIST '{dataset_path}'"))

# COMMAND ----------

# DBTITLE 1,Look inside the returns export
display(run_sql(f"LIST '{dataset_path}/returns-staging'"))
print(dbutils.fs.head(f"{dataset_path}/returns-staging/returns_2026_07_01.csv", 300))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2 · Query files directly: ``format.`path` ``
# MAGIC Syntax: ``SELECT * FROM <format>.`<path>` `` — the path can be **one file**, a **wildcard** or a **directory**
# MAGIC (all files in it are read as one table).

# COMMAND ----------

# DBTITLE 1,One file, a wildcard, a directory
cust = f"{dataset_path}/customers-json"
n_single = run_sql(f"SELECT * FROM json.`{cust}/export_001.json`").count()
n_wild = run_sql(f"SELECT * FROM json.`{cust}/export_00*.json`").count()
n_dir = run_sql(f"SELECT * FROM json.`{cust}`").count()
print(f"\none file: {n_single} rows | wildcard: {n_wild} rows | directory: {n_dir} rows")

# COMMAND ----------

# DBTITLE 1,JSON is (partly) self-describing: field names come from the data
display(run_sql(f"SELECT * FROM json.`{cust}` LIMIT 5"))

# COMMAND ----------

# MAGIC %md
# MAGIC > 💡 Direct queries are perfect for **exploring**. The schema is inferred at query time, you can't pass options (header,
# MAGIC > delimiter…), and nothing is governed as a table. For loading, use `read_files()`, `COPY INTO` or Auto Loader.
# MAGIC
# MAGIC ## Part 3 · Self-describing vs non-self-describing formats
# MAGIC
# MAGIC | Self-describing (schema travels with the data) | Not self-describing |
# MAGIC |---|---|
# MAGIC | **Parquet, Delta, Avro, ORC** (names + types), **JSON** (names; types inferred) | **CSV, TSV, text** — you must tell Spark about header, delimiter, types |

# COMMAND ----------

# DBTITLE 1,Parquet: names and types come from the file
orders_pq = run_sql(f"SELECT * FROM parquet.`{dataset_path}/orders-parquet`")
orders_pq.printSchema()

# COMMAND ----------

# DBTITLE 1,CSV queried directly: no header, wrong delimiter
csv_direct = run_sql(f"SELECT * FROM csv.`{dataset_path}/products-csv`")
display(csv_direct.limit(5))
print("columns:", csv_direct.columns, "| rows:", csv_direct.count(), "(36 products + 2 header lines read as data!)")

# COMMAND ----------

# MAGIC %md
# MAGIC Everything landed in one column `_c0` and the header lines became data rows. The fix: **`read_files()` with options**.

# COMMAND ----------

# DBTITLE 1,read_files with header and delimiter
products = run_sql(f"""
    SELECT * FROM read_files('{dataset_path}/products-csv',
                             format => 'csv', header => true, sep => ';')""")
display(products.limit(5))
print(dict(products.dtypes))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 4 · Unstructured and log data: `text` and `binaryFile`
# MAGIC
# MAGIC | Format | One row per… | Columns |
# MAGIC |---|---|---|
# MAGIC | `text` | **line** (or per file with `wholetext`) | `value` |
# MAGIC | `binaryFile` | **file** | `path`, `modificationTime`, `length`, `content` (bytes) |

# COMMAND ----------

# DBTITLE 1,text: one row per log line, parse with regular expressions
logs = run_sql(f"SELECT * FROM text.`{dataset_path}/app-logs`")
display(logs.limit(5))

parsed = logs.select(
    F.regexp_extract("value", r"^(\S+) (\w+) (\w+)", 1).alias("ts"),
    F.regexp_extract("value", r"^(\S+) (\w+) (\w+)", 2).alias("level"),
    F.regexp_extract("value", r"^(\S+) (\w+) (\w+)", 3).alias("event"),
    F.regexp_extract("value", r"ms=(\d+)", 1).cast("int").alias("ms"))
display(parsed.groupBy("level").agg(F.count("*").alias("lines"), F.avg("ms").alias("avg_ms")).orderBy("level"))

# COMMAND ----------

# DBTITLE 1,binaryFile: one row per file, raw bytes in `content`
images = run_sql(f"""
    SELECT path, length, modificationTime, hex(substring(content, 1, 8)) AS first_bytes
    FROM binaryFile.`{dataset_path}/product-images`""")
display(images)

# COMMAND ----------

# DBTITLE 1,Bonus: the bytes really are images
import base64

imgs = run_sql(f"SELECT path, content FROM binaryFile.`{dataset_path}/product-images`").collect()
displayHTML("".join(
    f'<figure style="display:inline-block;margin:6px;text-align:center">'
    f'<img src="data:image/png;base64,{base64.b64encode(r["content"]).decode()}" width="48" height="48" '
    f'style="image-rendering:pixelated;border:1px solid #ccc"/><figcaption style="font:12px sans-serif">'
    f'{r["path"].split("/")[-1]}</figcaption></figure>' for r in imgs))

# COMMAND ----------

# MAGIC %md
# MAGIC `89504E470D0A1A0A` is the **PNG signature**. Images, PDFs and audio are ingested exactly like this — the bytes land in a
# MAGIC Delta table (or stay in a volume) and AI functions / ML models process them later.
# MAGIC
# MAGIC ## Part 5 · File metadata with `_metadata`
# MAGIC Every file-based read exposes a hidden **`_metadata`** column (not part of `SELECT *`): `file_path`, `file_name`,
# MAGIC `file_size`, `file_modification_time`, `file_block_start`, `file_block_length`. Record where each row came from —
# MAGIC indispensable for auditing and debugging bronze tables.

# COMMAND ----------

# DBTITLE 1,_metadata with read_files (SQL)
display(run_sql(f"""
    SELECT _metadata.file_name AS file_name, _metadata.file_size AS bytes, count(*) AS rows
    FROM read_files('{cust}', format => 'json')
    GROUP BY ALL
    ORDER BY file_name"""))

# COMMAND ----------

# DBTITLE 1,input_file_name() - legacy, not available on Unity Catalog compute
# legacy-demo: this cell shows the OLD function on purpose
try:
    display(spark.sql(f"SELECT input_file_name() AS f, count(*) FROM json.`{cust}` GROUP BY 1"))
    print("It still ran here - but it is deprecated and can return wrong results. Use _metadata.file_path.")
except Exception as e:
    print("🚫 input_file_name() is not supported here (expected on serverless / Unity Catalog):",
          (str(e).strip().splitlines() or [repr(e)])[0][:150])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 6 · `read_files()` in depth
# MAGIC `read_files(path, format => …, <options>)` is a **table-valued function** that reads files from a volume or cloud path
# MAGIC with the full power of Spark readers + Auto Loader schema inference.
# MAGIC
# MAGIC ### 6a · Filter which files are read
# MAGIC The returns folder also contains a `manifest.txt`. Watch what happens if we read the folder as CSV blindly:

# COMMAND ----------

# DBTITLE 1,Reading everything - the manifest sneaks in
staging = f"{dataset_path}/returns-staging"
all_files = run_sql(f"SELECT * FROM read_files('{staging}', format => 'csv', header => true)")
n_all = all_files.count()
print("rows:", n_all, "| columns:", all_files.columns)

# COMMAND ----------

# DBTITLE 1,pathGlobFilter keeps only *.csv
returns = run_sql(f"""
    SELECT * FROM read_files('{staging}', format => 'csv', header => true, pathGlobFilter => '*.csv')""")
n_csv = returns.count()
print("rows:", n_csv)
print("columns and inferred types:", dict(returns.dtypes))   # clean data: return_date -> date, refund_amount -> double

# COMMAND ----------

# MAGIC %md
# MAGIC The first result is a lesson in itself: the manifest's first line became a **column** (`ShopWave returns export - 5 daily CSV files`)
# MAGIC and its second line a **row** (`owner: returns-team`) — `read_files` parses every file with **its own header** and merges them all.
# MAGIC
# MAGIC In the filtered result, notice two things in the column list:
# MAGIC * **`refund_method`** — only batches 4–5 have it; `read_files` **merged** the schemas, earlier rows get `NULL`.
# MAGIC * **`_rescued_data`** — added automatically; it collects values that don't fit the schema (next part).
# MAGIC
# MAGIC Other file filters: `fileNamePattern`, `modifiedAfter` / `modifiedBefore`, `recursiveFileLookup`.
# MAGIC
# MAGIC ### 6b · Schema inference, schema hints and rescued data
# MAGIC `returns-messy/returns_bad.csv` has 3 bad values: `n/a` and `12,50` as amounts and `31/07/2026` as a date.

# COMMAND ----------

# DBTITLE 1,1. Inference alone: bad values force whole columns to STRING
messy = f"{dataset_path}/returns-messy"
inferred = run_sql(f"SELECT * FROM read_files('{messy}', format => 'csv', header => true)")
print({c: t for c, t in inferred.dtypes})

# COMMAND ----------

# DBTITLE 1,2. schemaHints: force the types you expect; bad values go to _rescued_data
hinted = run_sql(f"""
    SELECT * FROM read_files('{messy}', format => 'csv', header => true,
                             schemaHints => 'refund_amount DOUBLE, return_date DATE')""")
print({c: t for c, t in hinted.dtypes})
bad_rows = hinted.where("_rescued_data IS NOT NULL")
rescued_rows = bad_rows.count()
display(bad_rows.select("return_id", "return_date", "refund_amount", "_rescued_data"))

# COMMAND ----------

# MAGIC %md
# MAGIC The row is kept, the bad value becomes `NULL` in its column, and the original text is preserved as JSON in
# MAGIC `_rescued_data` (with the file path in `_file_path`) — **no data is lost** and you can quarantine or fix those rows later.
# MAGIC
# MAGIC | Option | Effect |
# MAGIC |---|---|
# MAGIC | `schema => 'col TYPE, …'` | Full explicit schema — no inference at all |
# MAGIC | `schemaHints => 'col TYPE'` | Infer everything, but **override** some columns |
# MAGIC | `inferColumnTypes => false` | Everything as `STRING` (safest for bronze) |
# MAGIC | `rescuedDataColumn => 'name'` | Rename `_rescued_data` · `schemaEvolutionMode => 'none'` turns it off |
# MAGIC
# MAGIC ### 6c · JSON spread over several lines: `multiLine`
# MAGIC Normal JSON sources have **one record per line** (JSON Lines). A pretty-printed JSON **array** needs `multiLine => true`.

# COMMAND ----------

# DBTITLE 1,multiLine JSON
print(dbutils.fs.head(f"{dataset_path}/reviews-multiline/reviews_pretty.json", 250), "...\n")
pretty = run_sql(f"SELECT * FROM read_files('{dataset_path}/reviews-multiline', format => 'json', multiLine => true)")
n_multiline = pretty.count()
display(pretty.select("review_id", "rating", "review.title", "tags"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 7 · Create a bronze table from files (CTAS)
# MAGIC A bronze table keeps the raw data **plus** lineage columns. `CREATE TABLE … AS SELECT` (CTAS) infers the table schema from the query.

# COMMAND ----------

# DBTITLE 1,CTAS from read_files with metadata columns
run_sql(f"""
    CREATE OR REPLACE TABLE lab05_returns_bronze
    COMMENT 'Returns export, raw (bronze)'
    AS SELECT *,
              _metadata.file_name  AS source_file,
              current_timestamp()  AS ingested_at
       FROM read_files('{staging}', format => 'csv', header => true, pathGlobFilter => '*.csv')""")
display(spark.sql("SELECT source_file, count(*) AS rows, count(refund_method) AS with_method "
                  "FROM lab05_returns_bronze GROUP BY source_file ORDER BY source_file"))

# COMMAND ----------

# MAGIC %md
# MAGIC > ⚠️ CTAS is a **full reload**: run it tomorrow and it re-reads **every** file. For *incremental* loading (only new
# MAGIC > files) use `COPY INTO` (05-L2), Auto Loader (Section 06) or streaming tables (Section 08).
# MAGIC
# MAGIC ## Part 8 · 🕰️ Legacy patterns you must recognise
# MAGIC Before `read_files()` existed, CSV with options was loaded in two steps: a **temp view with a schema and options**, then CTAS.

# COMMAND ----------

# DBTITLE 1,Temp view USING CSV OPTIONS -> CTAS
temp_view_ok = False
try:
    run_sql(f"""
        CREATE OR REPLACE TEMP VIEW products_csv_v
          (product_id STRING, title STRING, brand STRING, category STRING, price DOUBLE)
        USING CSV
        OPTIONS (path '{dataset_path}/products-csv', header 'true', delimiter ';')""")
    run_sql("CREATE OR REPLACE TABLE lab05_products AS SELECT * FROM products_csv_v")
    temp_view_ok = True
except Exception as e:
    print("🚫 Path-based temp views aren't allowed on this compute:", (str(e).strip().splitlines() or [repr(e)])[0][:150])
    print("   Modern equivalent (works everywhere):")
    run_sql(f"""CREATE OR REPLACE TABLE lab05_products AS
                SELECT * FROM read_files('{dataset_path}/products-csv', format => 'csv', header => true, sep => ';',
                                         schema => 'product_id STRING, title STRING, brand STRING, category STRING, price DOUBLE')""")
print("lab05_products rows:", spark.table("lab05_products").count())

# COMMAND ----------

# MAGIC %md
# MAGIC The other classic: a **table defined directly on CSV files**:
# MAGIC
# MAGIC ```sql
# MAGIC CREATE TABLE sales_csv (order_id STRING, total DOUBLE)
# MAGIC USING CSV
# MAGIC OPTIONS (header = 'true', delimiter = ';')
# MAGIC LOCATION 's3://acme-data/exports/sales_csv';     -- in UC: must be inside an external location
# MAGIC ```
# MAGIC
# MAGIC That is an **external, non-Delta** table. What you must know for the exam:
# MAGIC
# MAGIC | | External CSV/JSON table | Delta table |
# MAGIC |---|---|---|
# MAGIC | ACID, time travel, `MERGE`/`UPDATE` | ❌ | ✅ |
# MAGIC | Performance | re-parses text on every query | columnar + statistics |
# MAGIC | New files added outside Spark | Spark may cache the old file list → run **`REFRESH TABLE sales_csv`** | always consistent (transaction log) |
# MAGIC | Unity Catalog managed table? | ❌ never — **managed tables must be Delta (or Iceberg)** | ✅ |
# MAGIC
# MAGIC Its cousin **`CREATE TABLE t USING JDBC OPTIONS (url …, dbtable …)`** (legacy, classic compute) queries a live database on
# MAGIC every read. In Unity Catalog you use **Lakehouse Federation** (a connection + foreign catalog) instead.
# MAGIC
# MAGIC Let's prove the last row: without `LOCATION` it would have to be managed, and Unity Catalog refuses.

# COMMAND ----------

# DBTITLE 1,CSV table without LOCATION -> rejected in Unity Catalog
csv_managed_rejected = False
try:
    run_sql("CREATE TABLE lab05_csv_try (id STRING) USING CSV")
    print("⚠️ Created - you're probably on the legacy hive_metastore. Dropping it again.")
    spark.sql("DROP TABLE IF EXISTS lab05_csv_try")
except Exception as e:
    csv_managed_rejected = True
    print("🚫 Expected:", (str(e).strip().splitlines() or [repr(e)])[0][:200])

# COMMAND ----------

# MAGIC %md
# MAGIC > 🎯 **Exam pattern:** "A table was created on CSV files; new files arrived but queries don't show them" → `REFRESH TABLE`.
# MAGIC > "Why convert it to Delta?" → ACID, time travel, performance, DML. The fix: **CTAS from the files into a Delta table.**
# MAGIC
# MAGIC ## Part 9 · The same in Python: the DataFrame reader

# COMMAND ----------

# DBTITLE 1,spark.read with options + _metadata, then saveAsTable
returns_df = (spark.read.format("csv")
                   .option("header", True)
                   .option("inferSchema", True)
                   .option("pathGlobFilter", "*.csv")
                   .load(staging)
                   .select("*", F.col("_metadata.file_name").alias("source_file")))
returns_df.printSchema()
returns_df.write.mode("overwrite").saveAsTable("lab05_returns_bronze_py")
print("lab05_returns_bronze_py rows:", spark.table("lab05_returns_bronze_py").count())

# COMMAND ----------

# MAGIC %md
# MAGIC > ⚠️ The plain CSV reader takes **one** header (from whichever file it happens to read first) and maps every file **by
# MAGIC > position** — it never merges headers by name like `read_files` does. Here `refund_method` probably appears only because a
# MAGIC > 7-column file was read first; if a source renamed or reordered a column, values would shift **silently**. Type inference
# MAGIC > suffers too: the other files' header lines are parsed as data rows, so columns often come back as STRING (look at the
# MAGIC > schema above). For evolving sources prefer `read_files` (or Auto Loader).
# MAGIC
# MAGIC | SQL | Python |
# MAGIC |---|---|
# MAGIC | ``SELECT * FROM json.`path` `` | `spark.read.json(path)` / `spark.read.format("json").load(path)` |
# MAGIC | `read_files(path, format => 'csv', header => true)` | `spark.read.format("csv").option("header", True).load(path)` |
# MAGIC | `_metadata.file_name` | `.select("*", "_metadata.file_name")` |
# MAGIC | `CREATE TABLE t AS SELECT …` | `df.write.saveAsTable("t")` |
# MAGIC
# MAGIC ## Part 10 · ✅ Automatic checks

# COMMAND ----------

# DBTITLE 1,Check your work
_bronze = spark.table("lab05_returns_bronze")
_checks = {
    "directory read = 300 customers, one file = 100": (n_dir, n_single, n_wild) == (300, 100, 300),
    "CSV queried directly lands in one column _c0": csv_direct.columns == ["_c0"] and csv_direct.count() == 38,
    "read_files parsed 36 products with a numeric price": products.count() == 36 and dict(products.dtypes)["price"] in ("double", "decimal(5,2)"),
    "100 log lines read with the text format": logs.count() == 100,
    "6 images read with binaryFile (PNG signature)": images.count() == 6 and images.first()["first_bytes"] == "89504E470D0A1A0A",
    "manifest.txt sneaked in (201 rows) until pathGlobFilter (200)": (n_all, n_csv) == (201, 200),
    "3 bad rows caught in _rescued_data": rescued_rows == 3,
    "multiLine JSON gave 5 reviews": n_multiline == 5,
    "bronze table: 200 rows from 5 files, with refund_method": (_bronze.count() == 200
                                                               and _bronze.select("source_file").distinct().count() == 5
                                                               and "refund_method" in _bronze.columns),
    "lab05_products has 36 rows": spark.table("lab05_products").count() == 36,
    "UC refused a managed CSV table": csv_managed_rejected,
}
for name, ok in _checks.items():
    print(("✅ " if ok else "❌ ") + name)
print("ℹ️ legacy temp view USING CSV", "worked" if temp_view_ok else "not allowed here (read_files used instead)")
print("\n🎉 Lab complete - next: 05-L2 · Incremental Loading with COPY INTO")
