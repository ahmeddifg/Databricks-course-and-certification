# Databricks notebook source
# MAGIC %md
# MAGIC # 🧮 02-P3 · DataFrames & Spark SQL Essentials
# MAGIC **Section 02 — Spark Foundations** · the vocabulary for **D2 Ingestion** and **D3 Transformation**
# MAGIC
# MAGIC | After this presentation you can… |
# MAGIC |---|
# MAGIC | Create DataFrames in the common ways and define schemas |
# MAGIC | Translate the core DataFrame operations to SQL and back |
# MAGIC | Choose between CSV, JSON, Parquet and Delta and use the right read/write options |
# MAGIC | Explain ETL vs ELT and why Databricks favours ELT |
# MAGIC | Use `repartition` vs `coalesce` and know what caching does (and where it isn't available) |
# MAGIC
# MAGIC > Deep dives: reading files & ingestion → **Section 05** · transformations → **Section 07**.

# COMMAND ----------

# MAGIC %run ../../Includes/_style

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · What is a DataFrame?
# MAGIC
# MAGIC A **DataFrame** is a **distributed, immutable table** with a **schema** (column names + types), split into partitions across the cluster.
# MAGIC In Databricks you'll use it from **Python (PySpark)** and **SQL** — both run on the same engine.
# MAGIC
# MAGIC | Create it from… | Python | SQL |
# MAGIC |---|---|---|
# MAGIC | A table | `spark.table("shopwave.orders")` / `spark.read.table(...)` | `SELECT * FROM shopwave.orders` |
# MAGIC | Files | `spark.read.format("json").load(path)` / `spark.read.json(path)` | ``SELECT * FROM json.`/Volumes/…` `` or `read_files(...)` |
# MAGIC | A query | `spark.sql("SELECT …")` | — |
# MAGIC | Local data | `spark.createDataFrame(rows, schema)` | `VALUES (1, 'a'), (2, 'b')` |
# MAGIC | A number range | `spark.range(1_000_000)` | `SELECT * FROM range(1000000)` |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · Schemas
# MAGIC
# MAGIC ```python
# MAGIC df.printSchema()          # tree view
# MAGIC df.schema                 # StructType object
# MAGIC df.dtypes                 # [('order_id', 'string'), ('total', 'double'), ...]
# MAGIC
# MAGIC # Declare a schema (faster & safer than inferring): DDL string ...
# MAGIC schema = "order_id STRING, order_timestamp BIGINT, customer_id STRING, quantity INT, total DOUBLE"
# MAGIC # ... or StructType
# MAGIC from pyspark.sql.types import StructType, StructField, StringType, DoubleType
# MAGIC schema = StructType([StructField("order_id", StringType()), StructField("total", DoubleType())])
# MAGIC
# MAGIC spark.read.schema(schema).json(path)
# MAGIC ```
# MAGIC > ⚠️ **Exam trap:** `inferSchema` / schema inference **reads the data an extra time** and can guess wrong types (e.g. zip codes as numbers).
# MAGIC > In production, **declare** schemas — or use Auto Loader's schema tracking (Section 06).

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · The core operations — Python ⇄ SQL

# COMMAND ----------

# DBTITLE 1,Slide · DataFrame vs SQL cheat sheet
show("""
<div class="kicker">Slide 3 · Rosetta stone</div>
<h2>Same result, two syntaxes</h2>
<table class="tbl">
<tr><th>Goal</th><th>PySpark DataFrame API</th><th>Spark SQL</th></tr>
<tr><td>Pick / compute columns</td><td><code>df.select("a", F.col("b").alias("c"))</code><br><code>df.withColumn("x", F.col("a") * 2)</code></td><td><code>SELECT a, b AS c, a * 2 AS x</code></td></tr>
<tr><td>Filter rows</td><td><code>df.filter(F.col("total") &gt; 100)</code> = <code>df.where("total &gt; 100")</code></td><td><code>WHERE total &gt; 100</code></td></tr>
<tr><td>Aggregate</td><td><code>df.groupBy("country").agg(F.sum("total").alias("revenue"))</code></td><td><code>SELECT country, sum(total) AS revenue … GROUP BY country</code></td></tr>
<tr><td>Sort</td><td><code>df.orderBy(F.desc("revenue"))</code></td><td><code>ORDER BY revenue DESC</code></td></tr>
<tr><td>Join</td><td><code>orders.join(customers, "customer_id", "left")</code></td><td><code>FROM orders o LEFT JOIN customers c ON o.customer_id = c.customer_id</code></td></tr>
<tr><td>Stack rows</td><td><code>df1.union(df2)</code> (by position) · <code>df1.unionByName(df2)</code></td><td><code>UNION ALL</code> (keeps duplicates) · <code>UNION</code> (removes them)</td></tr>
<tr><td>Rename / drop</td><td><code>withColumnRenamed("a", "b")</code> · <code>drop("a")</code></td><td><code>SELECT a AS b</code> · <code>SELECT * EXCEPT (a)</code></td></tr>
<tr><td>Deduplicate</td><td><code>distinct()</code> · <code>dropDuplicates(["id"])</code></td><td><code>SELECT DISTINCT …</code></td></tr>
<tr><td>Limit</td><td><code>df.limit(10)</code></td><td><code>LIMIT 10</code></td></tr>
</table>
""" + callout("trap", "<code>df.union(other)</code> matches columns <b>by position</b>, not by name, and keeps duplicates (like <code>UNION ALL</code>). "
              "Use <code>unionByName</code> when column order may differ.")
    + callout("tip", "Bridge the two worlds with <code>df.createOrReplaceTempView(\"v\")</code> (Python → SQL) and "
              "<code>spark.sql(\"…\", args={...})</code> (SQL → Python)."))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · File formats
# MAGIC
# MAGIC | Format | Layout | Schema | Strengths | Weaknesses |
# MAGIC |---|---|---|---|---|
# MAGIC | **CSV** | Row, text | Not stored (header only) → infer or declare | Human-readable, universal | No types, slow, delimiter/quote issues |
# MAGIC | **JSON** | Row, text | Self-describing per record, nested | APIs, events, nested data | Verbose, slower analytics |
# MAGIC | **Parquet** | **Columnar**, compressed | Stored in the file | Fast analytics, column pruning, predicate pushdown | Files are immutable; no transactions |
# MAGIC | **Delta** | Parquet + **transaction log** | Stored + **enforced** | ACID, `UPDATE/DELETE/MERGE`, time travel, streaming | Needs a Delta-aware engine (open source, widely supported) |
# MAGIC
# MAGIC > 🎯 **Self-describing** formats (JSON, Parquet, Delta) carry their schema; **non-self-describing** formats (CSV, TSV, TXT) need options such as `header`, `delimiter` and a schema.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 · Reading & writing — options and modes
# MAGIC
# MAGIC ```python
# MAGIC df = (spark.read.format("csv")
# MAGIC         .option("header", "true").option("delimiter", ";").option("inferSchema", "true")
# MAGIC         .load(f"{dataset_path}/products-csv"))
# MAGIC
# MAGIC df.write.format("delta").mode("overwrite").saveAsTable("products")   # Delta is the default format
# MAGIC ```
# MAGIC
# MAGIC | Write mode | Behaviour if the target already exists |
# MAGIC |---|---|
# MAGIC | `errorifexists` / `error` *(default)* | Fails |
# MAGIC | `append` | Adds rows |
# MAGIC | `overwrite` | Replaces the data (Delta keeps the old version for time travel) |
# MAGIC | `ignore` | Does nothing, silently |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6 · ETL vs ELT

# COMMAND ----------

# DBTITLE 1,Slide · ETL vs ELT
show("""
<div class="kicker">Slide 6 · Where does the T happen?</div>
<h2>ETL vs ELT</h2>
<div class="grid two">
  <div class="card gray"><h3>ETL — Extract, Transform, Load</h3>
   <div class="flow"><div class="step">Extract</div><div class="arrow">➜</div><div class="step">Transform<br><small>staging engine</small></div><div class="arrow">➜</div><div class="step">Load<br><small>warehouse</small></div></div>
   Only clean data lands. Raw data is often lost; schema changes are painful. Traditional warehouses.</div>
  <div class="card green"><h3>ELT — Extract, Load, Transform ✅</h3>
   <div class="flow"><div class="step">Extract</div><div class="arrow">➜</div><div class="step">Load raw<br><small>lakehouse (bronze)</small></div><div class="arrow">➜</div><div class="step">Transform<br><small>Spark / SQL (silver, gold)</small></div></div>
   Raw data kept → reprocess any time, schema-on-read, scales with Spark. The <b>medallion architecture</b> is ELT.</div>
</div>
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7 · `repartition` vs `coalesce` — and caching
# MAGIC
# MAGIC | | `df.repartition(n)` / `df.repartition(n, "col")` | `df.coalesce(n)` |
# MAGIC |---|---|---|
# MAGIC | Can increase partitions | ✅ | ❌ (only decreases) |
# MAGIC | Shuffle | **Full shuffle** → evenly balanced partitions | **No full shuffle** — merges neighbouring partitions (may be uneven) |
# MAGIC | Typical use | Increase parallelism, rebalance skew, partition by a key before a write | Reduce the number of output files cheaply after a filter |
# MAGIC
# MAGIC **Caching** (`df.cache()` / `df.persist(StorageLevel.MEMORY_AND_DISK)`, SQL `CACHE TABLE`) keeps a computed DataFrame in executor memory/disk:
# MAGIC * it is **lazy** — data is stored on the **first action**;
# MAGIC * only worth it when the same result is **reused several times** (iterative ML, several reports);
# MAGIC * free memory with `df.unpersist()`;
# MAGIC * ❌ **not supported on serverless** — Databricks' automatic **disk cache** already speeds up repeated reads of Parquet/Delta files.

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Key takeaways
# MAGIC 1. DataFrame = distributed, immutable, schema'd table; Python and SQL compile to the same plans.
# MAGIC 2. **Declare schemas** in production; inference costs an extra pass and can be wrong.
# MAGIC 3. CSV/JSON are row text formats; **Parquet** is columnar; **Delta** = Parquet + transaction log (ACID, DML, time travel).
# MAGIC 4. Write modes: `errorifexists` (default), `append`, `overwrite`, `ignore`.
# MAGIC 5. Databricks favours **ELT**: land raw data first, transform with Spark.
# MAGIC 6. `repartition` = full shuffle (up or down); `coalesce` = merge down without full shuffle. Cache only reused data (not on serverless).
# MAGIC
# MAGIC ➡️ Now practise: **labs/02-L1** and **labs/02-L2**.
