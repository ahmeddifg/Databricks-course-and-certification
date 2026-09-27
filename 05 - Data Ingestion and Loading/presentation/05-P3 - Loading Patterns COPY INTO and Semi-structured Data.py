# Databricks notebook source
# MAGIC %md
# MAGIC # 📥 05-P3 · Loading Patterns: `COPY INTO`, Semi-structured & Unstructured Data
# MAGIC **Section 05 — Data Ingestion & Loading** · supports **D2** objectives 2.2 (`COPY INTO`), 2.6 (choosing) and 2.7 (semi-/unstructured)
# MAGIC
# MAGIC | After this presentation you can… |
# MAGIC |---|
# MAGIC | Contrast **full reloads** with **incremental** loads |
# MAGIC | Write `COPY INTO` with `FORMAT_OPTIONS`, `COPY_OPTIONS`, `FILES`/`PATTERN`, `VALIDATE` and a `SELECT` |
# MAGIC | Explain `COPY INTO` **idempotency**, `force` and `mergeSchema` |
# MAGIC | Choose between `COPY INTO` and **Auto Loader** |
# MAGIC | Store and query semi-structured data as **STRING**, **STRUCT** or **`VARIANT`**, and ingest unstructured files |
# MAGIC
# MAGIC > ▶️ **Run all** to render the diagrams (a few seconds, any compute).

# COMMAND ----------

# MAGIC %run ../../Includes/_style

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · Full reload vs incremental load
# MAGIC
# MAGIC | Pattern | Statement | Re-reads old files? | Typical use |
# MAGIC |---|---|---|---|
# MAGIC | Full reload | `CREATE OR REPLACE TABLE t AS SELECT … FROM read_files(…)` · `INSERT OVERWRITE` | ✅ every run | small sources, snapshots |
# MAGIC | Append everything | `INSERT INTO t SELECT … FROM read_files(…)` | ✅ → **duplicates** | ❌ don't |
# MAGIC | **Incremental** | **`COPY INTO`** · Auto Loader · streaming tables · managed connectors | ❌ only new files/changes | growing sources |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · `COPY INTO` anatomy

# COMMAND ----------

# DBTITLE 1,Slide · COPY INTO
show("""
<div class="kicker">Slide 2 · COPY INTO — idempotent, incremental file loading in SQL</div>
<h2>Load <b>only files it hasn't loaded before</b> into a Delta table</h2>
<pre style="font-size:13px;background:#f7f9fc;border:1px solid #e3e8ef;border-radius:8px;padding:10px">COPY INTO main.bronze.returns                      <span class="muted">-- target: an existing Delta table (may be schemaless)</span>
FROM '/Volumes/main/raw/landing/returns'           <span class="muted">-- a folder (or a file) in a volume / cloud storage</span>
FILEFORMAT = CSV                                   <span class="muted">-- CSV, JSON, PARQUET, AVRO, ORC, TEXT, BINARYFILE</span>
PATTERN = 'returns_2026_*.csv'                     <span class="muted">-- or FILES = ('a.csv', 'b.csv')  (max 1,000)</span>
FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'true', 'mergeSchema' = 'true')   <span class="muted">-- how to READ</span>
COPY_OPTIONS ('mergeSchema' = 'true')              <span class="muted">-- how to WRITE the table ('force' = 'true' reloads everything)</span></pre>
<div class="flow">
 <div class="step"><b>Run 1</b>files 01 → <strong>loaded</strong></div><div class="arrow">➜</div>
 <div class="step"><b>Run 2</b>no new files → <strong>0 rows</strong></div><div class="arrow">➜</div>
 <div class="step"><b>Files 02, 03 arrive</b></div><div class="arrow">➜</div>
 <div class="step"><b>Run 3</b>02, 03 loaded · 01 <strong>skipped</strong></div>
</div>
""" + callout("exam", "Files already loaded are <b>skipped even if they were modified</b> afterwards. That's what makes "
              "<code>COPY INTO</code> safe to schedule and to retry — and why a corrected file with the same name is <b>not</b> reloaded.")
  + callout("tip", "Output: one row with <code>num_affected_rows</code>, <code>num_inserted_rows</code>, "
            "<code>num_skipped_corrupt_files</code>. The operation also appears in <code>DESCRIBE HISTORY</code>."))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · The options that matter
# MAGIC
# MAGIC | Feature | Syntax | Remember |
# MAGIC |---|---|---|
# MAGIC | Schemaless target | `CREATE TABLE IF NOT EXISTS t;` then `COPY INTO t … FORMAT_OPTIONS('mergeSchema'='true') COPY_OPTIONS('mergeSchema'='true')` | schema inferred from the files |
# MAGIC | Read-side merge | `FORMAT_OPTIONS ('mergeSchema' = 'true')` | merge schemas **of the files** in this run |
# MAGIC | Table evolution | `COPY_OPTIONS ('mergeSchema' = 'true')` | allow **new columns** in the **table**; without it schema enforcement rejects them |
# MAGIC | Reload | `COPY_OPTIONS ('force' = 'true')` | ignores the load history → **duplicates** |
# MAGIC | Dry run | `VALIDATE ALL` / `VALIDATE 10 ROWS` | returns a preview (≤ 50 rows); checks parsing, schema, constraints — **writes nothing** |
# MAGIC | Select files | `FILES = (…)` or `PATTERN = 'glob'` | not both |
# MAGIC | Transform | `FROM (SELECT a::int, b AS c, _metadata.file_name AS f FROM '/path')` | casts, renames, metadata; no `GROUP BY` |
# MAGIC | Corrupt files | `FORMAT_OPTIONS ('ignoreCorruptFiles' = 'true')` | counted in `num_skipped_corrupt_files` |
# MAGIC | VARIANT | `FORMAT_OPTIONS ('singleVariantColumn' = 'raw')` | whole record → one `VARIANT` column |
# MAGIC | Privileges | source: `READ VOLUME` (volume) or `READ FILES` (external location) · target: `USE CATALOG`, `USE SCHEMA`, `MODIFY` (+ `SELECT`) on the table | in practice the job's identity (often a service principal) owns the bronze table |
# MAGIC
# MAGIC ## 4 · `COPY INTO` or Auto Loader?

# COMMAND ----------

# DBTITLE 1,Slide · COPY INTO vs Auto Loader
show("""
<div class="kicker">Slide 4 · Two incremental file loaders</div>
<h2>Same goal, different scale</h2>
<table class="tbl">
<tr><th></th><th>📥 COPY INTO</th><th>⚡ Auto Loader (<code>cloudFiles</code>)</th></tr>
<tr><td>API</td><td>SQL command</td><td>Structured Streaming (Python/Scala) · SQL via streaming tables (<code>STREAM read_files</code>)</td></tr>
<tr><td>Processing</td><td>batch — each run a new command</td><td>streaming — continuous or incremental batch (<code>availableNow</code>)</td></tr>
<tr><td>Remembers loaded files in</td><td>the <b>target table</b>'s metadata</td><td>the stream's <b>checkpoint</b> (RocksDB)</td></tr>
<tr><td>Scale</td><td><b>thousands</b> of files</td><td><b>millions</b> of files / hour; file-notification mode</td></tr>
<tr><td>Schema</td><td>infer + <code>mergeSchema</code></td><td>inference, <b>schema location</b>, evolution modes, rescued data</td></tr>
<tr><td>Best for</td><td>simple, scheduled SQL loads; ad-hoc backfills of a few files</td><td>large or continuously growing sources, evolving schemas</td></tr>
</table>
""" + callout("exam", "Keywords: <b>“millions of files”</b>, “continuously arriving”, “schema evolution with rescued data” → Auto Loader. "
              "<b>“SQL”, “idempotent”, “thousands of files”, “re-runnable command”</b> → COPY INTO. Databricks now recommends "
              "streaming tables (Auto Loader) for most new SQL ingestion.")
  + callout("info", "Auto Loader in depth: <b>Section 06</b>."))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 · Semi-structured data (objective 2.7)

# COMMAND ----------

# DBTITLE 1,Slide · STRING vs STRUCT vs VARIANT
show("""
<div class="kicker">Slide 5 · Three ways to keep JSON</div>
<h2>STRING · STRUCT/ARRAY · VARIANT</h2>
<table class="tbl">
<tr><th></th><th>JSON as <b>STRING</b></th><th><b>STRUCT / ARRAY</b></th><th><b>VARIANT</b></th></tr>
<tr><td>How</td><td>load the text as is</td><td>schema inference or <code>from_json(col, 'schema')</code></td><td><code>parse_json(col)</code>, <code>singleVariantColumn</code>, schema type <code>VARIANT</code></td></tr>
<tr><td>Query</td><td><code>col:a.b</code> (parses text each time)</td><td><code>col.a.b</code> (typed)</td><td><code>col:a.b::int</code> (binary, fast)</td></tr>
<tr><td>New fields</td><td>no problem</td><td>schema evolution needed</td><td>no problem</td></tr>
<tr><td>Speed</td><td>🐢 slow</td><td>🚀 fast</td><td>🚀 fast</td></tr>
<tr><td>Good for</td><td>tiny/rare use</td><td>stable, well-known schemas (silver/gold)</td><td>flexible or changing JSON (bronze), IoT/events</td></tr>
</table>
<div class="grid two">
 <div class="card"><h3>Create VARIANT</h3>
  <code>parse_json(json_str)</code> (error on bad JSON) · <code>try_parse_json</code> (NULL instead)<br>
  <code>COPY INTO t FROM '…' FILEFORMAT = JSON FORMAT_OPTIONS ('singleVariantColumn' = 'raw')</code><br>
  Auto Loader: <code>.option("singleVariantColumn", "raw")</code> or schema hints <code>'col VARIANT'</code></div>
 <div class="card"><h3>Query VARIANT</h3>
  <code>raw:review.title::string</code> · <code>raw:photos[0].url</code> · <code>raw:['zip code']</code><br>
  <code>variant_get(raw, '$.rating', 'int')</code> · <code>try_variant_get</code><br>
  <code>schema_of_variant(raw)</code> · <code>schema_of_variant_agg(raw)</code> · <code>variant_explode(raw:tags)</code></div>
</div>
""" + callout("trap", "Paths are <b>case-sensitive</b>; a missing field returns <code>NULL</code>. VARIANT columns can't be used for "
              "comparisons, <code>GROUP BY</code>, <code>ORDER BY</code>, or as partition / clustering / Z-order keys — cast the value first.")
  + callout("info", "Nested JSON transformations (<code>explode</code>, <code>from_json</code>, <code>struct.*</code>, higher-order functions) "
            "are covered in Section 07."))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6 · Unstructured data
# MAGIC
# MAGIC | Goal | How |
# MAGIC |---|---|
# MAGIC | Keep files governed, read them from code | store them in a **volume** (`READ VOLUME`/`WRITE VOLUME`); open with Python or Spark |
# MAGIC | Put file bytes into a table | `read_files(…, format => 'binaryFile')` / `binaryFile` reader / `COPY INTO … FILEFORMAT = BINARYFILE` → `path, modificationTime, length, content` |
# MAGIC | Filter by name | `pathGlobFilter => '*.png'` |
# MAGIC | Process later | AI functions (e.g. document parsing), ML models, Python UDFs |
# MAGIC
# MAGIC ## 7 · Putting it together — a daily ingestion job
# MAGIC
# MAGIC ```sql
# MAGIC -- Task 1 (notebook): call the partner API, land the raw JSON in /Volumes/main/raw/landing/partner/<date>.json
# MAGIC -- Task 2 (SQL):      load only the new files
# MAGIC CREATE TABLE IF NOT EXISTS main.bronze.partner_events (raw VARIANT);
# MAGIC
# MAGIC COPY INTO main.bronze.partner_events
# MAGIC FROM '/Volumes/main/raw/landing/partner'
# MAGIC FILEFORMAT = JSON
# MAGIC FORMAT_OPTIONS ('singleVariantColumn' = 'raw');
# MAGIC -- Task 3: silver transformations (Section 07/08) - scheduled daily by a Lakeflow Job with retries
# MAGIC ```
# MAGIC
# MAGIC ## ✅ Key takeaways
# MAGIC 1. Full reloads re-read everything; **`COPY INTO`**, Auto Loader and managed connectors load **only new data**.
# MAGIC 2. `COPY INTO` is **idempotent**: loaded files are skipped (even if modified); `force` reloads (duplicates); `VALIDATE` writes nothing.
# MAGIC 3. `FORMAT_OPTIONS` = how to read; `COPY_OPTIONS` = how to write. Schemaless table + `mergeSchema` in both = schema inferred and evolved.
# MAGIC 4. **COPY INTO**: SQL, thousands of files. **Auto Loader**: millions, streaming, schema evolution + rescued data.
# MAGIC 5. JSON as **VARIANT** (`parse_json`, `singleVariantColumn`) — flexible and fast; query with `:` paths and `::` casts.
# MAGIC 6. Unstructured files: volumes + `binaryFile`.
# MAGIC
# MAGIC ➡️ Practice: **labs/05-L1**, **05-L2**, then **05-L3 Challenge** · Test yourself: **questions/05-Q**
