# Databricks notebook source
# MAGIC %md
# MAGIC # 🧹 07-P1 · Cleaning and Shaping Data — Bronze to Silver
# MAGIC **Section 07 — Data Transformation & Modeling** · exam domain **D3 (22 %)** · objectives: *implement data cleaning (nulls,
# MAGIC data types) from bronze to silver* · *manipulate columns, rows and table structures (add, drop, split, rename, filter,
# MAGIC explode)* · *deduplication and aggregate operations (count, approx count distinct, mean, summary)*
# MAGIC
# MAGIC | After this presentation you can… |
# MAGIC |---|
# MAGIC | Say what changes between **bronze** and **silver** |
# MAGIC | Use the core **column operations** in PySpark and SQL (select, withColumn, rename, drop, cast, split) |
# MAGIC | **Profile** a table: `describe`, `summary`, null counts, `approx_count_distinct` |
# MAGIC | Fix **types** safely under **ANSI mode** (`try_cast`, `try_to_timestamp`) and handle **NULLs** |
# MAGIC | Parse **JSON strings** and **explode** arrays |
# MAGIC | Pick the right **deduplication** tool and write silver tables with the right **save mode** |
# MAGIC
# MAGIC > ▶️ **Run all** to render the diagrams (a few seconds, any compute).

# COMMAND ----------

# MAGIC %run ../../Includes/_style

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · Where we are: the medallion architecture

# COMMAND ----------

# DBTITLE 1,Slide · Bronze -> Silver -> Gold
show("""
<div class="kicker">Slide 1 · Medallion architecture</div>
<h2>Each layer has one job</h2>
<div class="flow">
 <div class="step"><b>🥉 Bronze</b>raw, as delivered · all columns often <strong>strings</strong> · duplicates · + ingestion metadata</div><div class="arrow">➜</div>
 <div class="step"><b>🥈 Silver</b>clean, typed, deduplicated, <strong>conformed</strong> · one row per business entity/event</div><div class="arrow">➜</div>
 <div class="step"><b>🥇 Gold</b>business-level: aggregates, star schemas, features · ready for BI / ML (07-P3)</div>
</div>
<div class="grid">
 <div class="card"><h3>Bronze → Silver = this presentation</h3><ul>
  <li>standardise values (case, spaces, codes)</li><li>fix <b>data types</b></li><li>handle <b>NULLs</b></li>
  <li>parse nested data</li><li><b>deduplicate</b></li></ul></div>
 <div class="card green"><h3>Why not clean in bronze?</h3>Bronze is your <b>replayable history</b>. If a cleaning rule was wrong,
 you rebuild silver from bronze — the raw data was never lost.</div>
 <div class="card purple"><h3>Same engine, two languages</h3>Every transformation can be written with the
 <b>DataFrame API</b> or <b>SQL</b> — both compile to the same Catalyst plan (Section 02).</div>
</div>
""" + callout("exam", "Objective 3.1 wording: <i>read bronze tables with PySpark/SQL, clean nulls, standardise data types, write to "
              "new silver tables</i>. Expect code-reading questions in <b>both</b> languages."))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · Column operations — PySpark ⇄ SQL
# MAGIC
# MAGIC | Task | PySpark (`from pyspark.sql import functions as F`) | SQL |
# MAGIC |---|---|---|
# MAGIC | choose / compute columns | `df.select("a", F.col("b").alias("c"), (F.col("x") * 2).alias("y"))` | `SELECT a, b AS c, x * 2 AS y` |
# MAGIC | add or replace a column | `df.withColumn("y", F.col("x") * 2)` · many: `withColumns({"y": …, "z": …})` | `SELECT *, x * 2 AS y` |
# MAGIC | rename | `df.withColumnRenamed("old", "new")` · many: `withColumnsRenamed({...})` | `SELECT old AS new` |
# MAGIC | drop | `df.drop("a", "b")` | `SELECT * EXCEPT (a, b)` |
# MAGIC | filter rows | `df.filter("x > 0")` = `df.where(F.col("x") > 0)` | `WHERE x > 0` |
# MAGIC | cast | `F.col("p").cast("decimal(10,2)")` | `CAST(p AS DECIMAL(10,2))` · `p::decimal(10,2)` |
# MAGIC | split a string | `F.split("email", "@").getItem(1)` | `split(email, '@')[1]` · `split_part(email, '@', 2)` |
# MAGIC | conditional value | `F.when(cond, v).when(…).otherwise(v2)` | `CASE WHEN cond THEN v … ELSE v2 END` |
# MAGIC | string clean-up | `F.trim`, `F.lower`, `F.upper`, `F.initcap`, `F.regexp_replace`, `F.substring` | same names |
# MAGIC | dates | `F.to_date`, `F.to_timestamp(c, fmt)`, `F.date_format`, `F.timestamp_seconds` | same names |
# MAGIC
# MAGIC > 💡 DataFrames are **immutable**: every call returns a **new** DataFrame — assign it (`df = df.withColumn(…)`) or
# MAGIC > chain calls. `withColumn` in a long loop builds a huge plan — prefer one `select` / `withColumns`.
# MAGIC
# MAGIC ## 3 · Profile before you clean

# COMMAND ----------

# DBTITLE 1,Slide · EDA tools
show("""
<div class="kicker">Slide 3 · Exploratory data analysis</div>
<h2>Know the data: counts, distributions, NULLs, duplicates</h2>
<table class="tbl">
<tr><th>Call</th><th>Returns</th><th>Notes</th></tr>
<tr><td><code>df.count()</code></td><td>number of rows</td><td>an <b>action</b> — runs a job</td></tr>
<tr><td><code>df.describe()</code></td><td>count, mean, stddev, min, max</td><td>per column; string columns: count/min/max meaningful, mean/stddev NULL</td></tr>
<tr><td><code>df.summary()</code></td><td>describe <b>+ 25 % / 50 % / 75 %</b></td><td><code>summary("count", "min", "max")</code> to choose</td></tr>
<tr><td><code>F.count_distinct(c)</code> · SQL <code>count(DISTINCT c)</code></td><td>exact distinct count</td><td>memory-heavy on huge data</td></tr>
<tr><td><code>F.approx_count_distinct(c, rsd=0.05)</code></td><td>estimate (HyperLogLog++)</td><td>default <code>rsd</code> = 0.05 (max relative standard deviation ≈ <b>5 %</b>), very fast</td></tr>
<tr><td><code>F.mean(c)</code> = <code>F.avg(c)</code></td><td>average</td><td><b>ignores NULLs</b></td></tr>
<tr><td><code>sum(col.isNull().cast("int"))</code></td><td>NULLs per column</td><td>one <code>select</code> over all columns</td></tr>
</table>
""" + callout("trap", "<code>count(*)</code> counts rows; <code>count(col)</code> counts <b>non-NULL</b> values of that column. "
              "<code>avg(col)</code> ignores NULLs too — a column with NULLs has a different denominator.")
  + callout("info", "In a notebook, <code>display(df)</code> also offers a <b>Data profile</b> tab with the same statistics "
            "and histograms — no code needed."))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · Types, formats and ANSI mode

# COMMAND ----------

# DBTITLE 1,Slide · ANSI mode and try_ functions
show("""
<div class="kicker">Slide 4 · Standardising data types</div>
<h2>With ANSI mode, bad values <u>fail</u> the query — unless you use <code>try_</code> functions</h2>
<div class="grid two">
 <div class="card red"><h3>ANSI on (serverless, Databricks SQL, Spark 4 default)</h3><ul>
  <li><code>CAST('N/A' AS INT)</code> → <b>error</b></li>
  <li><code>to_timestamp('01/03/2026', 'yyyy-MM-dd')</code> → <b>error</b></li>
  <li>integer overflow, divide by zero → <b>error</b></li></ul></div>
 <div class="card green"><h3>Tolerant versions → NULL</h3><ul>
  <li><code>try_cast(x AS DECIMAL(10,2))</code></li>
  <li><code>try_to_timestamp(x, fmt)</code> · <code>try_to_number</code></li>
  <li><code>try_divide</code>, <code>try_add</code>, <code>try_element_at</code></li></ul></div>
</div>
<div class="card purple"><h3>Several formats in one column</h3>
<code>coalesce(try_to_timestamp(c, 'yyyy-MM-dd HH:mm:ss'), try_to_timestamp(c, 'dd/MM/yyyy HH:mm'))</code>
— the first format that parses wins.</div>
""" + callout("tip", "Clean text <b>before</b> casting: <code>try_cast(regexp_replace(trim(amount), '[^0-9.-]', '') AS DECIMAL(10,2))</code> "
              "turns <code>' 12.50 '</code> and <code>'USD 12.50'</code> into 12.50 and <code>'N/A'</code> into NULL.")
  + callout("exam", "Money → <b>DECIMAL(p,s)</b>, not DOUBLE (exact arithmetic). Epoch seconds → <code>timestamp_seconds()</code> "
            "(or <code>from_unixtime()</code> for a string)."))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 · NULLs: fill, drop — or keep and flag
# MAGIC
# MAGIC | Need | PySpark | SQL |
# MAGIC |---|---|---|
# MAGIC | replace NULLs in some columns | `df.fillna({"currency": "USD", "qty": 0})` = `df.na.fill(...)` | `coalesce(currency, 'USD')` · `nvl` · `ifnull` |
# MAGIC | drop rows with NULLs | `df.dropna(subset=["id"])` · `how="all"` · `thresh=2` = `df.na.drop(...)` | `WHERE id IS NOT NULL` |
# MAGIC | replace specific values | `df.na.replace("usd", "USD", subset=["currency"])` | `CASE WHEN …` |
# MAGIC | test for NULL | `F.col("c").isNull()` / `isNotNull()` | `c IS NULL` |
# MAGIC | NULL-safe equality | `F.col("a").eqNullSafe(F.col("b"))` | `a <=> b` · `a IS NOT DISTINCT FROM b` |

# COMMAND ----------

# DBTITLE 1,Slide · NULL semantics
show("""
<div class="kicker">Slide 5 · Three-valued logic</div>
<h2>NULL means <i>unknown</i> — comparisons with it are unknown too</h2>
<table class="tbl">
<tr><th>Expression</th><th>Result</th><th>Consequence</th></tr>
<tr><td><code>NULL = NULL</code> · <code>NULL &lt;&gt; 'x'</code></td><td>NULL</td><td>a <code>WHERE</code> treats it as false → row filtered out</td></tr>
<tr><td><code>NULL &lt;=&gt; NULL</code></td><td>TRUE</td><td>null-safe comparison (change detection, SCD — 07-P3)</td></tr>
<tr><td><code>join ON a.k = b.k</code> with NULL keys</td><td>no match</td><td>NULL keys never join (use <code>&lt;=&gt;</code> if they should)</td></tr>
<tr><td><code>count(c)</code>, <code>sum</code>, <code>avg</code></td><td>skip NULLs</td><td><code>count(*)</code> doesn't</td></tr>
<tr><td><code>1 + NULL</code> · <code>concat('a', NULL)</code></td><td>NULL</td><td>use <code>coalesce</code> or <code>concat_ws</code></td></tr>
</table>
""" + callout("tip", "Don't drop rows just because a non-key column is NULL — that silently loses data. Fix it (known default), "
              "or keep the row and <b>flag / quarantine</b> it (07-P3)."))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6 · Nested and semi-structured data

# COMMAND ----------

# DBTITLE 1,Slide · JSON strings, structs and arrays
show("""
<div class="kicker">Slide 6 · JSON strings, structs, arrays</div>
<h2>From a JSON string to columns and rows</h2>
<div class="flow">
 <div class="step"><b>JSON string</b><code>'{"address":{"city":"Riyadh"}}'</code></div><div class="arrow">➜</div>
 <div class="step"><b>STRUCT</b><code>from_json(profile, schema)</code><br><span class="muted">schema from <code>schema_of_json(sample)</code></span></div><div class="arrow">➜</div>
 <div class="step"><b>Columns</b><code>select("p.*")</code> · <code>p.address.city</code></div>
</div>
<div class="grid">
 <div class="card"><h3>Quick path access</h3><code>profile:address:city</code> (Databricks SQL on a JSON string)<br>
 <code>get_json_object(profile, '$.address.city')</code><br><span class="muted">VARIANT columns use the same <code>:</code> syntax (Section 05)</span></div>
 <div class="card green"><h3>Structs</h3>dot notation <code>s.field</code>; <code>s.*</code> expands all fields;
 build one with <code>struct(...)</code> / <code>named_struct</code>; back to JSON with <code>to_json</code>.</div>
 <div class="card orange"><h3>Arrays → rows</h3><code>explode</code>: one row per element, <b>drops</b> empty/NULL arrays ·
 <code>explode_outer</code>: <b>keeps</b> them (NULL) · <code>posexplode</code>: + position</div>
</div>
""" + callout("trap", "<code>explode</code> silently removes rows whose array is empty or NULL (e.g. cancelled orders with no items). "
              "Use <code>explode_outer</code> when those rows must survive.")
  + callout("info", "The reverse of explode is an aggregate: <code>collect_list</code> (keeps duplicates) / "
            "<code>collect_set</code> (distinct) — 07-P2."))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7 · Deduplication

# COMMAND ----------

# DBTITLE 1,Slide · Three dedup tools
show("""
<div class="kicker">Slide 7 · Removing duplicates</div>
<h2>Which duplicates — and which row should survive?</h2>
<table class="tbl">
<tr><th>Tool</th><th>Duplicate means</th><th>Survivor</th></tr>
<tr><td><code>df.distinct()</code> = <code>df.dropDuplicates()</code> · SQL <code>SELECT DISTINCT</code></td><td>all columns equal</td><td>any (they're identical)</td></tr>
<tr><td><code>df.dropDuplicates(["id"])</code></td><td>same key</td><td><b>arbitrary</b> ⚠️</td></tr>
<tr><td><code>row_number() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1</code></td><td>same key</td><td><b>latest</b> — deterministic ✅</td></tr>
<tr><td><code>MERGE … WHEN NOT MATCHED THEN INSERT</code></td><td>key already in the <b>target</b></td><td>existing row (insert-only merge, Section 03)</td></tr>
</table>
""" + callout("exam", "“Keep the most recent record for each customer” → window function <code>row_number()</code> partitioned by the key, "
              "ordered by the timestamp <b>descending</b>, keep 1. Databricks SQL shortcut: <code>QUALIFY row_number() OVER (…) = 1</code>.")
  + callout("trap", "Ingestion metadata (<code>_ingested_at</code>, file name) makes copies look different — drop those columns "
            "<b>before</b> <code>distinct()</code>. In streams, dedup needs a watermark (Section 06)."))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8 · Writing the silver table
# MAGIC
# MAGIC | DataFrame writer | If the table exists | SQL equivalent |
# MAGIC |---|---|---|
# MAGIC | `df.write.saveAsTable("t")` (mode **`errorifexists`** — default) | error | `CREATE TABLE t AS SELECT …` |
# MAGIC | `.mode("overwrite")` (same schema) | data replaced | `INSERT OVERWRITE t …` |
# MAGIC | `.mode("overwrite").option("overwriteSchema", "true")` | data **and schema** replaced | `CREATE OR REPLACE TABLE t AS …` |
# MAGIC | `.mode("append")` | rows added | `INSERT INTO t …` |
# MAGIC | `.mode("ignore")` | nothing happens | `CREATE TABLE IF NOT EXISTS t AS …` |
# MAGIC | `df.writeTo("t").createOrReplace()` / `.append()` | DataFrameWriterV2 API | — |
# MAGIC
# MAGIC ```python
# MAGIC (silver_df.write
# MAGIC           .mode("overwrite")
# MAGIC           .option("overwriteSchema", "true")
# MAGIC           .saveAsTable("workspace.shopwave.silver_payments"))   # managed Delta table: catalog.schema.table
# MAGIC ```
# MAGIC
# MAGIC Upserts into an existing silver table (only changed rows) use **`MERGE INTO`** — Section 03 and 07-P3.
# MAGIC
# MAGIC ## ✅ Key takeaways
# MAGIC 1. **Bronze → silver** = standardise values, fix **types**, handle **NULLs**, parse nested data, **deduplicate** — never edit bronze.
# MAGIC 2. Column ops: `select`, `withColumn(s)`, `withColumn(s)Renamed`, `drop`, `cast`, `split().getItem()`, `when/otherwise` — each has a SQL twin.
# MAGIC 3. Profile first: `describe()`, `summary()` (+ percentiles), null counts, `approx_count_distinct` (default `rsd` 0.05 ≈ 5 % relative error, cheap).
# MAGIC 4. **ANSI mode** raises errors on bad casts/formats → `try_cast`, `try_to_timestamp`; several formats → `coalesce` of attempts; money → DECIMAL.
# MAGIC 5. `fillna` / `dropna(subset=…)` / `coalesce`; NULL comparisons are NULL — use `<=>` to compare nullable values.
# MAGIC 6. JSON string → `from_json` (+ `schema_of_json`) or `:` paths; arrays → `explode` (drops empties) vs `explode_outer`.
# MAGIC 7. Dedup: `distinct` (identical rows) · `dropDuplicates(subset)` (arbitrary survivor) · `row_number()` window (latest survivor).
# MAGIC 8. `saveAsTable` default mode = **errorifexists**; `overwrite` + `overwriteSchema` to replace.
# MAGIC
# MAGIC ➡️ Next: **07-P2 · Combining and Aggregating Data**
