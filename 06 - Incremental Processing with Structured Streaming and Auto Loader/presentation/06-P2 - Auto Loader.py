# Databricks notebook source
# MAGIC %md
# MAGIC # ⚡ 06-P2 · Auto Loader
# MAGIC **Section 06 — Incremental Processing** · exam objective **2.3**: *use Auto Loader with schema enforcement and schema
# MAGIC evolution in batch modes (directory listing or file notification) to land data into Unity Catalog tables*
# MAGIC
# MAGIC | After this presentation you can… |
# MAGIC |---|
# MAGIC | Write an Auto Loader stream (`cloudFiles`) and name its required options |
# MAGIC | Explain **schema inference** (strings by default, `inferColumnTypes`, `schemaHints`) and the **schema location** |
# MAGIC | Predict what each **schema evolution mode** does when a new column appears |
# MAGIC | Explain **rescued data** |
# MAGIC | Compare **directory listing** and **file notification** (file events) modes |
# MAGIC | Choose between Auto Loader, `COPY INTO` and streaming tables |
# MAGIC
# MAGIC > ▶️ **Run all** to render the diagrams (a few seconds, any compute).

# COMMAND ----------

# MAGIC %run ../../Includes/_style

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · What is Auto Loader?

# COMMAND ----------

# DBTITLE 1,Slide · Auto Loader overview
show("""
<div class="kicker">Slide 1 · Auto Loader = the cloudFiles streaming source</div>
<h2>Incrementally ingest new files from cloud storage — exactly once, at any scale</h2>
<div class="flow">
 <div class="step"><b>☁️ Folder</b>volume or cloud path; files keep arriving</div><div class="arrow">➜</div>
 <div class="step"><b>🔎 Discover new files</b>directory listing or file notifications</div><div class="arrow">➜</div>
 <div class="step"><b>🧬 Schema</b>infer · track · evolve · rescue</div><div class="arrow">➜</div>
 <div class="step"><b>🌊 Structured Streaming</b>micro-batches, checkpoint</div><div class="arrow">➜</div>
 <div class="step"><b>📋 Delta table</b>bronze, in Unity Catalog</div>
</div>
<div class="grid">
 <div class="card"><h3>Formats</h3>JSON, CSV, XML, Parquet, Avro, ORC, text, binaryFile</div>
 <div class="card green"><h3>Scale</h3>millions of files; state of processed files kept in the checkpoint (RocksDB)</div>
 <div class="card purple"><h3>Where</h3>Python/Scala streams · SQL: <code>STREAM read_files(…)</code> in streaming tables</div>
</div>
""" + callout("exam", "Auto Loader is a <b>Structured Streaming source</b>. Run it continuously, or as an incremental batch with "
              "<code>trigger(availableNow=True)</code> — the “batch mode” the exam guide mentions."))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · The code
# MAGIC
# MAGIC ```python
# MAGIC (spark.readStream
# MAGIC       .format("cloudFiles")
# MAGIC       .option("cloudFiles.format", "json")                              # required: file format
# MAGIC       .option("cloudFiles.schemaLocation", "/Volumes/main/ops/ckpt/orders")   # required for inference: where the schema is stored
# MAGIC       .option("cloudFiles.inferColumnTypes", "true")                   # typed columns instead of strings
# MAGIC       .option("cloudFiles.schemaHints", "order_ts TIMESTAMP, price DECIMAL(10,2)")
# MAGIC       .load("/Volumes/main/raw/landing/orders")
# MAGIC       .select("*", "_metadata.file_path")
# MAGIC       .writeStream
# MAGIC       .option("checkpointLocation", "/Volumes/main/ops/ckpt/orders")  # often the same folder as the schema location
# MAGIC       .option("mergeSchema", "true")                                    # let the Delta TABLE accept new columns
# MAGIC       .trigger(availableNow=True)
# MAGIC       .toTable("main.bronze.orders"))
# MAGIC ```
# MAGIC
# MAGIC | Option | Default | Purpose |
# MAGIC |---|---|---|
# MAGIC | `cloudFiles.format` | — (required) | json, csv, parquet, avro, orc, text, binaryFile, xml |
# MAGIC | `cloudFiles.schemaLocation` | — (required when inferring) | stores the inferred schema and its history (`_schemas/`) |
# MAGIC | `cloudFiles.inferColumnTypes` | **false** → JSON/CSV/XML columns are **strings** | infer numbers, booleans, … |
# MAGIC | `cloudFiles.schemaHints` | — | override / add types for chosen columns |
# MAGIC | `cloudFiles.schemaEvolutionMode` | **addNewColumns** (when inferring) | what to do with new columns |
# MAGIC | `cloudFiles.includeExistingFiles` | **true** | also process files already in the folder at the **first** start |
# MAGIC | `cloudFiles.maxFilesPerTrigger` / `maxBytesPerTrigger` | 1000 files / none (DBR 18.0+: tuned dynamically if unset) | size of each micro-batch |
# MAGIC | `cloudFiles.useManagedFileEvents` / `useNotifications` | false | file notification modes |
# MAGIC | `cloudFiles.cleanSource` | off | move or delete source files after processing (DBR 16.4+) |
# MAGIC | `rescuedDataColumn` | `_rescued_data` (when inferring) | where non-matching data goes |
# MAGIC
# MAGIC ## 3 · Schema inference

# COMMAND ----------

# DBTITLE 1,Slide · Inference
show("""
<div class="kicker">Slide 3 · How Auto Loader decides the schema</div>
<h2>Infer once, store it, reuse it</h2>
<div class="grid two">
 <div class="card"><h3>🔍 First start</h3><ul>
  <li>Samples the first <b>50 GB or 1,000 files</b></li>
  <li>JSON/CSV/XML → every column <b>STRING</b> unless <code>inferColumnTypes</code></li>
  <li>Parquet/Avro/ORC → types from the files</li>
  <li>Applies <code>schemaHints</code>, adds <code>_rescued_data</code></li>
  <li>Writes version <code>0</code> to <code>&lt;schemaLocation&gt;/_schemas/</code></li></ul></div>
 <div class="card green"><h3>▶️ Every later start</h3><ul>
  <li>Reads the <b>latest</b> schema version — no re-inference</li>
  <li>Each evolution adds a new version file (1, 2, …)</li>
  <li>Hive-style folders <code>col=value/</code> become partition columns</li>
  <li>Provide a full schema with <code>.schema(...)</code> to skip inference (evolution mode then defaults to <code>none</code>)</li></ul></div>
</div>
""" + callout("tip", "Strings-by-default is deliberate: nothing in bronze can fail to parse. Type the data in silver — or use "
              "<code>inferColumnTypes</code> + <code>schemaHints</code> and let rescued data catch the exceptions."))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · Schema evolution modes

# COMMAND ----------

# DBTITLE 1,Slide · Evolution modes
show("""
<div class="kicker">Slide 4 · A new column appears in the files. Now what?</div>
<h2><code>cloudFiles.schemaEvolutionMode</code></h2>
<table class="tbl">
<tr><th>Mode</th><th>Schema</th><th>Stream</th><th>New column's data</th></tr>
<tr><td><b>addNewColumns</b> (default)</td><td>column <b>added</b> (new schema version)</td><td>fails <b>once</b> with <code>UnknownFieldException</code>; <b>restart</b> continues</td><td>in the new column</td></tr>
<tr><td>addNewColumnsWithTypeWidening <span class="muted">(DBR 16.4+)</span></td><td>added; compatible types widened (int → long…)</td><td>fails once</td><td>in the new column</td></tr>
<tr><td><b>rescue</b></td><td>never changes</td><td>never fails for schema changes</td><td>in <code>_rescued_data</code></td></tr>
<tr><td><b>failOnNewColumns</b></td><td>not changed</td><td>fails until you change the schema / hints</td><td>—</td></tr>
<tr><td><b>none</b></td><td>not changed</td><td>doesn't fail</td><td>ignored (lost unless <code>rescuedDataColumn</code> is set)</td></tr>
</table>
<div class="grid two">
 <div class="card"><h3>🔁 Production pattern</h3><code>addNewColumns</code> + a <b>Lakeflow Job with retries</b>: the retry restarts the stream
 with the new schema automatically. (Lakeflow pipelines restart for you.)</div>
 <div class="card orange"><h3>📋 Don't forget the sink</h3>The Delta <b>table</b> also has to accept the column:
 <code>.option("mergeSchema", "true")</code> on the writer (or table-level schema evolution).</div>
</div>
""" + callout("exam", "“The Auto Loader stream stopped after the source added a column; after a restart it worked” → "
              "that is the expected behaviour of the default <b>addNewColumns</b> mode."))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 · Rescued data
# MAGIC
# MAGIC `_rescued_data` (a JSON string) collects values that **could not be parsed into the schema** — the row itself is kept:
# MAGIC
# MAGIC | Rescued when… | Example |
# MAGIC |---|---|
# MAGIC | **type mismatch** | `"amount": "12.50 EUR"` in a DOUBLE column |
# MAGIC | column **not in the schema** (`rescue` mode, or a provided schema with `rescuedDataColumn` set) | `"coupon": "SUMMER10"` |
# MAGIC | **case mismatch** of a column name | `Amount` vs `amount` |
# MAGIC
# MAGIC The JSON also contains `_file_path`. Filter `WHERE _rescued_data IS NOT NULL` to quarantine, alert, or repair in silver.
# MAGIC
# MAGIC ## 6 · File detection modes

# COMMAND ----------

# DBTITLE 1,Slide · Directory listing vs notifications
show("""
<div class="kicker">Slide 6 · How does Auto Loader find new files?</div>
<h2>Directory listing vs file notification</h2>
<div class="grid two">
 <div class="card"><h3>📂 Directory listing <span class="pill">default</span></h3><ul>
  <li>Lists the input path on each micro-batch</li>
  <li>No extra setup or permissions</li>
  <li>Lists the <b>whole</b> path each time (the old incremental-listing option is deprecated)</li>
  <li>Cost/latency grow with the number of files in the folder</li></ul></div>
 <div class="card green"><h3>🔔 File notification <span class="pill green">recommended for most workloads</span></h3><ul>
  <li><b>With file events</b> (<code>cloudFiles.useManagedFileEvents</code>, DBR 14.3+): needs <b>file events enabled on the external location</b>; no extra cloud permissions</li>
  <li>Legacy variant (<code>cloudFiles.useNotifications</code>): Auto Loader creates cloud queues / notifications (needs permissions)</li>
  <li>Scales to huge folders and high arrival rates</li></ul></div>
</div>
""" + callout("info", "Both modes give exactly-once processing and neither guarantees the <b>order</b> in which files are processed. "
              "The labs in this section use directory listing (the default)."))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7 · Landing into Unity Catalog — permissions
# MAGIC
# MAGIC | What | Privilege |
# MAGIC |---|---|
# MAGIC | Read the source files | `READ VOLUME` on the volume — or `READ FILES` on the external location for a cloud path |
# MAGIC | Checkpoint & schema location | `WRITE VOLUME` on the volume that holds them (or `WRITE FILES` on the external location) |
# MAGIC | Target table | `USE CATALOG`, `USE SCHEMA`, `CREATE TABLE` on the schema (first run), then `MODIFY` (and `SELECT`) on the table — or own it |
# MAGIC | Scheduled job | run it as a **service principal** that holds these grants |
# MAGIC
# MAGIC ## 8 · Auto Loader, `COPY INTO` or a streaming table?
# MAGIC
# MAGIC | | Auto Loader (Python stream) | Streaming table (SQL) | `COPY INTO` |
# MAGIC |---|---|---|---|
# MAGIC | Written as | `readStream.format("cloudFiles")` | `CREATE OR REFRESH STREAMING TABLE … AS SELECT * FROM STREAM read_files(…)` | `COPY INTO t FROM '…' FILEFORMAT = …` |
# MAGIC | Engine | Structured Streaming + checkpoint | Auto Loader inside a Lakeflow pipeline | batch command |
# MAGIC | File tracking | checkpoint (RocksDB) | pipeline-managed checkpoint | target table metadata |
# MAGIC | Scale | millions of files | millions of files | thousands of files |
# MAGIC | Schema evolution | modes + rescued data | same | `mergeSchema` |
# MAGIC | Best for | Python pipelines, full control | SQL-first, managed pipelines (Section 08) | simple, re-runnable SQL loads |
# MAGIC
# MAGIC > 🕰️ Legacy DLT: `cloud_files("/path", "json")` = today's `STREAM read_files("/path", format => "json")`.
# MAGIC
# MAGIC ## ✅ Key takeaways
# MAGIC 1. Auto Loader = `format("cloudFiles")` + `cloudFiles.format` + `cloudFiles.schemaLocation`; writes need a **checkpoint**.
# MAGIC 2. JSON/CSV/XML are inferred as **strings** unless `cloudFiles.inferColumnTypes = true`; refine with `schemaHints`.
# MAGIC 3. **addNewColumns** (default): new column → schema updated → stream fails **once** → restart continues (+ `mergeSchema` on the sink).
# MAGIC    **rescue** never fails (new data in `_rescued_data`), **failOnNewColumns** fails, **none** ignores.
# MAGIC 4. `_rescued_data` keeps type mismatches, unknown columns and case mismatches — no data loss.
# MAGIC 5. **Directory listing** (default) vs **file notification with file events** (recommended for most workloads). `availableNow` = incremental batch.
# MAGIC 6. UC permissions: `READ VOLUME`/`READ FILES` on the source, `WRITE VOLUME` for checkpoints, `USE CATALOG`/`USE SCHEMA` + `CREATE TABLE`/`MODIFY` on the target.
# MAGIC
# MAGIC ➡️ Next: **06-P3 · Stateful Streaming: Windows, Watermarks and foreachBatch**
