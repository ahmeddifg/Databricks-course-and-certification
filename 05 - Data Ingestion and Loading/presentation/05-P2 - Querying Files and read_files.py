# Databricks notebook source
# MAGIC %md
# MAGIC # 🔎 05-P2 · Querying Files & `read_files()`
# MAGIC **Section 05 — Data Ingestion & Loading** · supports **D2** (objectives 2.1, 2.7) and **D3 Transformation**
# MAGIC
# MAGIC | After this presentation you can… |
# MAGIC |---|
# MAGIC | Query files **directly** with ``format.`path` `` — one file, a wildcard or a directory |
# MAGIC | Explain **self-describing** vs **non-self-describing** formats and why it matters |
# MAGIC | Use **`read_files()`** options: header, delimiter, schema, **schema hints**, **rescued data**, `multiLine`, file filters |
# MAGIC | Read **text** and **binary** (unstructured) files and capture **file metadata** with `_metadata` |
# MAGIC | Load files into Delta tables with **CTAS** — and recognise the legacy patterns (temp view `USING CSV`, external CSV tables, `REFRESH TABLE`) |
# MAGIC
# MAGIC > ▶️ **Run all** to render the diagrams (a few seconds, any compute).

# COMMAND ----------

# MAGIC %run ../../Includes/_style

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · Query files in place

# COMMAND ----------

# DBTITLE 1,Slide · Direct file queries
show("""
<div class="kicker">Slide 1 · SELECT straight from files</div>
<h2><code>SELECT * FROM &lt;format&gt;.`&lt;path&gt;`</code></h2>
<table class="tbl">
<tr><th>Path points to…</th><th>Example</th><th>Reads</th></tr>
<tr><td>one file</td><td><code>json.`/Volumes/main/raw/files/customers/export_001.json`</code></td><td>that file</td></tr>
<tr><td>a wildcard</td><td><code>json.`/Volumes/main/raw/files/customers/export_00*.json`</code></td><td>matching files</td></tr>
<tr><td>a directory</td><td><code>json.`/Volumes/main/raw/files/customers`</code></td><td>all files in it — as <b>one</b> table (same format &amp; schema expected)</td></tr>
</table>
<div class="grid">
 <div class="card"><h3>Formats</h3><code>json</code> · <code>csv</code> · <code>parquet</code> · <code>avro</code> · <code>orc</code> · <code>text</code> · <code>binaryFile</code> · <code>delta</code></div>
 <div class="card"><h3>Paths</h3>volumes <code>/Volumes/c/s/v/…</code> or cloud URIs <code>s3://…</code> (needs <code>READ FILES</code> on the external location)</div>
 <div class="card"><h3>Ignored</h3>files starting with <code>_</code> or <code>.</code> (e.g. <code>_SUCCESS</code>, <code>_delta_log</code>)</div>
</div>
""" + callout("trap", "The path is wrapped in <b>backticks</b> <code>`…`</code>, not quotes. Direct queries take <b>no options</b> — "
              "fine for exploring JSON/Parquet, useless for a <code>;</code>-delimited CSV with a header."))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · Self-describing or not?

# COMMAND ----------

# DBTITLE 1,Slide · Self-describing formats
show("""
<div class="kicker">Slide 2 · Where does the schema come from?</div>
<h2>Self-describing formats carry their schema; the others need your help</h2>
<div class="grid two">
 <div class="card green"><h3>✅ Self-describing</h3><ul>
  <li><b>Parquet, ORC, Avro, Delta</b> — column names <b>and types</b> are stored in the files</li>
  <li><b>JSON</b> — field names in every record; types are <b>inferred</b> from values</li>
  <li>Direct queries just work</li></ul></div>
 <div class="card red"><h3>❌ Not self-describing</h3><ul>
  <li><b>CSV / TSV</b> — header? delimiter? quote? types? all unknown</li>
  <li><b>text</b> — just lines</li>
  <li>Queried directly: columns <code>_c0, _c1…</code>, header line becomes data, wrong delimiter → one column</li>
  <li>→ use <code>read_files()</code> (or a reader) with <b>options</b></li></ul></div>
</div>
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · `read_files()` — the Swiss-army knife
# MAGIC
# MAGIC ```sql
# MAGIC SELECT *, _metadata.file_name AS source_file
# MAGIC FROM read_files(
# MAGIC   '/Volumes/main/raw/files/returns',
# MAGIC   format        => 'csv',                       -- json, csv, xml, parquet, avro, orc, text, binaryFile (inferred if omitted)
# MAGIC   header        => true,
# MAGIC   sep           => ',',                         -- or delimiter
# MAGIC   schemaHints   => 'refund_amount DOUBLE, return_date DATE',
# MAGIC   pathGlobFilter => '*.csv')
# MAGIC ```
# MAGIC
# MAGIC | Option | Purpose |
# MAGIC |---|---|
# MAGIC | `schema => 'a INT, b STRING'` | explicit schema — no inference |
# MAGIC | `inferColumnTypes` (default **true**) | exact types from sampled data; `false` → everything `STRING` |
# MAGIC | `schemaHints => 'col TYPE, …'` | infer, but **override** chosen columns |
# MAGIC | `rescuedDataColumn` (default **`_rescued_data`**) | keeps values that don't match the schema (`schemaEvolutionMode => 'none'` disables it) |
# MAGIC | `multiLine => true` | JSON records spanning lines (e.g. a pretty-printed array) |
# MAGIC | `pathGlobFilter`, `fileNamePattern`, `modifiedAfter`/`modifiedBefore`, `recursiveFileLookup` | choose which files |
# MAGIC | `partitionColumns` | parse Hive-style `col=value/` folders into columns |
# MAGIC | CSV: `header`, `sep`, `quote`, `escape`, `mode`… · JSON: `multiLine`, … | same names as the Spark readers |
# MAGIC
# MAGIC * Schemas of **all** files are **merged** (a column that exists only in newer files is added, older rows `NULL`).
# MAGIC * `read_files` is a **batch** read; with `STREAM read_files(...)` it becomes an **Auto Loader** source for streaming tables (Section 08).
# MAGIC * It needs a **literal path** — in Python build the SQL with an f-string.
# MAGIC
# MAGIC ## 4 · Rescued data — never lose a row

# COMMAND ----------

# DBTITLE 1,Slide · Rescued data
show("""
<div class="kicker">Slide 4 · Bad values don't crash the load</div>
<h2>Schema says <code>refund_amount DOUBLE</code>, the file says <code>n/a</code></h2>
<table class="tbl">
<tr><th>return_id</th><th>value in the file</th><th>value in the table</th><th>_rescued_data</th></tr>
<tr><td>M0002</td><td>refund_amount = 116.78</td><td>refund_amount = 116.78</td><td>NULL</td></tr>
<tr><td>M0003</td><td>refund_amount = n/a</td><td>refund_amount = <b>NULL</b></td><td><code>{"refund_amount":"n/a","_file_path":"…/returns_bad.csv"}</code></td></tr>
<tr><td>M0010</td><td>return_date = 31/07/2026</td><td>return_date = <b>NULL</b> (refund_amount 101.82 kept)</td><td><code>{"return_date":"31/07/2026",…}</code></td></tr>
</table>
<div class="grid">
 <div class="card"><h3>Rescued when…</h3>type mismatch · column missing from the schema · case mismatch of a column name</div>
 <div class="card green"><h3>Then…</h3>filter <code>_rescued_data IS NOT NULL</code> → quarantine, fix, alert</div>
 <div class="card orange"><h3>Compare: CSV <code>mode</code></h3><code>PERMISSIVE</code> (nulls), <code>DROPMALFORMED</code> (drops rows!), <code>FAILFAST</code> (error)</div>
</div>
""" + callout("exam", "Rescued data is <b>on by default</b> in <b>Auto Loader / read_files</b> (COPY INTO can enable it with the <code>rescuedDataColumn</code> format option). The exam likes to ask where malformed values go: "
              "into the <b><code>_rescued_data</code></b> column, as JSON."))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 · File metadata and unstructured files
# MAGIC
# MAGIC | `_metadata.` field | Type | Example |
# MAGIC |---|---|---|
# MAGIC | `file_path` | STRING | `/Volumes/main/raw/files/returns/returns_2026_07_01.csv` |
# MAGIC | `file_name` | STRING | `returns_2026_07_01.csv` |
# MAGIC | `file_size` | BIGINT | bytes |
# MAGIC | `file_modification_time` | TIMESTAMP | last modified |
# MAGIC | `file_block_start`, `file_block_length` | BIGINT | split info |
# MAGIC
# MAGIC * `_metadata` is **hidden**: not included in `SELECT *` — select it explicitly. Works with `read_files`, direct queries,
# MAGIC   `spark.read` (`.select("*", "_metadata")`), `COPY INTO` and Auto Loader.
# MAGIC * 🕰️ **`input_file_name()`** is **deprecated** and **not available on Unity Catalog** (and gone in recent runtimes) → use `_metadata.file_path`.
# MAGIC
# MAGIC | Unstructured format | One row per | Columns |
# MAGIC |---|---|---|
# MAGIC | `text` | line (`wholetext => true`: per file) | `value` |
# MAGIC | `binaryFile` | file | `path`, `modificationTime`, `length`, `content` |
# MAGIC
# MAGIC Images, PDFs, audio: keep the files in a **volume** (governed by `READ VOLUME`), or ingest the bytes with `binaryFile`
# MAGIC and process them later (AI functions, ML models).
# MAGIC
# MAGIC ## 6 · From files to Delta tables

# COMMAND ----------

# DBTITLE 1,Slide · CTAS and legacy patterns
show("""
<div class="kicker">Slide 6 · Loading files into tables</div>
<h2>Modern: CTAS from <code>read_files</code> · Legacy: temp view / external table</h2>
<div class="grid two">
 <div class="card green"><h3>✅ Modern (one step)</h3>
<pre style="font-size:12px;margin:0">CREATE OR REPLACE TABLE bronze.returns AS
SELECT *, _metadata.file_name AS source_file,
       current_timestamp() AS ingested_at
FROM read_files('/Volumes/…/returns',
       format => 'csv', header => true);</pre>
 Schema comes from the query. A <b>full reload</b> every time it runs.</div>
 <div class="card gray"><h3>🕰️ Legacy (two steps)</h3>
<pre style="font-size:12px;margin:0">CREATE TEMP VIEW products_v
  (product_id STRING, title STRING, brand STRING,
   category STRING, price DOUBLE)
USING CSV
OPTIONS (path '/…/products', header 'true',
         delimiter ';');
CREATE TABLE products AS SELECT * FROM products_v;</pre>
 Why? CTAS over <code>csv.`path`</code> can't take options.</div>
</div>
<table class="tbl">
<tr><th>External table on CSV/JSON files (<code>USING CSV … LOCATION</code>)</th><th>Delta table</th></tr>
<tr><td>Not Delta: no ACID, no time travel, no <code>MERGE/UPDATE/DELETE</code></td><td>all of those</td></tr>
<tr><td>Re-parses text on every query; no data skipping</td><td>columnar, statistics, caching</td></tr>
<tr><td>Spark may cache the file list → new files invisible until <b><code>REFRESH TABLE t</code></b></td><td>always consistent (transaction log)</td></tr>
<tr><td>Unity Catalog: must be <b>external</b> (LOCATION in an external location)</td><td>managed or external</td></tr>
</table>
<table class="tbl">
<tr><th>🕰️ <code>CREATE TABLE t USING JDBC OPTIONS (url '…', dbtable '…', user '…', password '…')</code></th></tr>
<tr><td>Legacy Hive-metastore pattern: a table that <b>queries the live database on every read</b> (classic compute). Not Delta, no copy,
performance depends on the source. In Unity Catalog the replacement is <b>Lakehouse Federation</b> (connection + foreign catalog).</td></tr>
</table>
""" + callout("exam", "“A table defined on CSV files doesn't show newly added files” → <code>REFRESH TABLE</code>. "
              "“Need ACID/time travel on that data” → CTAS it into a <b>Delta</b> table.")
  + callout("trap", "CTAS / <code>CREATE OR REPLACE</code> re-reads <b>all</b> files each run. For only-new-files loading use "
            "<code>COPY INTO</code> or Auto Loader (05-P3, Section 06)."))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7 · SQL ↔ Python
# MAGIC
# MAGIC | SQL | Python (DataFrame reader) |
# MAGIC |---|---|
# MAGIC | ``SELECT * FROM json.`/path` `` | `spark.read.json("/path")` · `spark.read.format("json").load("/path")` |
# MAGIC | `read_files('/path', format => 'csv', header => true, sep => ';')` | `spark.read.format("csv").option("header", True).option("sep", ";").load("/path")` |
# MAGIC | `schema => 'a INT, b STRING'` | `.schema("a INT, b STRING")` |
# MAGIC | `multiLine => true` | `.option("multiLine", True)` |
# MAGIC | `_metadata.file_name` | `.select("*", "_metadata.file_name")` |
# MAGIC | `CREATE TABLE t AS SELECT …` | `df.write.saveAsTable("t")` (default format Delta, mode `errorifexists`) |
# MAGIC
# MAGIC > 💡 `read_files` merges the schemas of all files **by column name** and adds `_rescued_data`. The plain CSV reader takes **one**
# MAGIC > header (from whichever file it reads first) and maps every file **by position** — it never merges headers by name, and it
# MAGIC > doesn't rescue values; with `inferSchema` the other files' header lines are parsed as data, so columns often come back STRING.
# MAGIC > For evolving sources prefer `read_files` / Auto Loader. (A CSV temp view with a schema is positional too.)
# MAGIC
# MAGIC ## ✅ Key takeaways
# MAGIC 1. ``SELECT * FROM json.`path` `` queries a file, a wildcard or a directory; direct queries take **no options**.
# MAGIC 2. **Self-describing** (Parquet, ORC, Avro, Delta; JSON names) vs **not** (CSV, text) → CSV needs header/delimiter/schema options.
# MAGIC 3. **`read_files()`**: options, `schemaHints`, **`_rescued_data`**, `multiLine`, file filters, schema **merging** — batch; `STREAM read_files` for streaming.
# MAGIC 4. **`_metadata.file_path` / `file_name`** replaces the deprecated `input_file_name()`.
# MAGIC 5. `text` → one row per line; `binaryFile` → one row per file with `content` bytes.
# MAGIC 6. CTAS from `read_files` = modern full load. Legacy: temp view `USING CSV OPTIONS` → CTAS; external CSV tables need **`REFRESH TABLE`** and lack Delta features.
# MAGIC
# MAGIC ➡️ Next: **05-P3 · Loading Patterns: COPY INTO, Semi-structured & Unstructured Data**
