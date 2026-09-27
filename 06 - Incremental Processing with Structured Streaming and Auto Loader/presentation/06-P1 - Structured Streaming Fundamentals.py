# Databricks notebook source
# MAGIC %md
# MAGIC # 🌊 06-P1 · Structured Streaming Fundamentals
# MAGIC **Section 06 — Incremental Processing** · supports **D2 Data Ingestion (21 %)** (objective 2.3) and **D3/D4** pipeline design
# MAGIC
# MAGIC | After this presentation you can… |
# MAGIC |---|
# MAGIC | Explain the **unbounded table** model and **micro-batches** |
# MAGIC | Read the anatomy of a stream: **source → transformations → sink**, with **checkpoint**, **trigger** and **output mode** |
# MAGIC | Choose a **trigger** — and know which ones work on **serverless** |
# MAGIC | Pick the right **output mode** (append / complete / update) |
# MAGIC | Explain how checkpoints + replayable sources + idempotent sinks give **exactly-once** results |
# MAGIC | List the operations a stream can't do, and how to manage running queries |
# MAGIC
# MAGIC > ▶️ **Run all** to render the diagrams (a few seconds, any compute).

# COMMAND ----------

# MAGIC %run ../../Includes/_style

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · The big idea: a table that never ends

# COMMAND ----------

# DBTITLE 1,Slide · Unbounded table
show("""
<div class="kicker">Slide 1 · The Structured Streaming model</div>
<h2>A stream is just a table that keeps getting new rows</h2>
<div class="flow">
 <div class="step"><b>📥 Input = unbounded table</b>every new file / event = new rows appended</div><div class="arrow">➜</div>
 <div class="step"><b>🧮 Your query</b>the <strong>same</strong> DataFrame / SQL code as batch</div><div class="arrow">➜</div>
 <div class="step"><b>📤 Result table</b>updated incrementally</div><div class="arrow">➜</div>
 <div class="step"><b>💾 Sink</b>Delta table, Kafka, foreachBatch…</div>
</div>
<div class="grid two">
 <div class="card"><h3>⏱️ Micro-batches</h3>Spark checks the source, takes <b>only the new data</b>, runs your query on it as a
 small batch job, commits the result — and repeats (when and how often: the <b>trigger</b>).</div>
 <div class="card green"><h3>♻️ Same API as batch</h3><code>spark.read</code> → <code>spark.readStream</code><br>
 <code>df.write</code> → <code>df.writeStream</code><br>Transformations (<code>select</code>, <code>where</code>, <code>join</code>,
 <code>groupBy</code>…) are the same.</div>
</div>
""" + callout("exam", "Structured Streaming = <b>incremental</b> processing with <b>exactly-once</b> guarantees. It powers Auto Loader, "
              "streaming tables in Lakeflow pipelines and Delta-to-Delta streams."))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · Anatomy of a streaming query
# MAGIC
# MAGIC ```python
# MAGIC (spark.readStream                                   # 1 SOURCE
# MAGIC       .format("cloudFiles")                         #   Auto Loader, Delta table (.table), Kafka, files (.json/.csv…)
# MAGIC       .option("cloudFiles.format", "json")
# MAGIC       .option("cloudFiles.schemaLocation", ckpt)
# MAGIC       .load("/Volumes/main/raw/landing/orders")
# MAGIC       .where("quantity > 0")                        # 2 TRANSFORMATIONS (stateless or stateful)
# MAGIC       .writeStream                                  # 3 SINK
# MAGIC       .option("checkpointLocation", ckpt)           #   REQUIRED - unique per stream
# MAGIC       .outputMode("append")                         #   append | complete | update
# MAGIC       .trigger(availableNow=True)                   #   when to run
# MAGIC       .toTable("main.bronze.orders"))               #   starts the query and returns a StreamingQuery
# MAGIC ```
# MAGIC
# MAGIC | Source | Read with | Notes |
# MAGIC |---|---|---|
# MAGIC | Files (Auto Loader) | `.format("cloudFiles")` | infers & evolves schema, scales to millions of files (06-P2) |
# MAGIC | Files (plain) | `.format("json")` / `.json(path)` … | needs an explicit **schema** |
# MAGIC | Delta table | `spark.readStream.table("t")` | append-only by default; `skipChangeCommits`, `readChangeFeed` |
# MAGIC | Kafka / Kinesis / Pub/Sub / Event Hubs | `.format("kafka")` · `"kinesis"` · `"pubsub"` · Event Hubs via its Kafka endpoint | message buses; Kafka `value` is binary |
# MAGIC
# MAGIC ## 3 · Triggers — when does it run?

# COMMAND ----------

# DBTITLE 1,Slide · Triggers
show("""
<div class="kicker">Slide 3 · Triggers</div>
<h2>Continuous micro-batches or “process what's there, then stop”</h2>
<table class="tbl">
<tr><th>Trigger</th><th>Behaviour</th><th>Runs until</th><th>Serverless</th></tr>
<tr><td><i>none</i> (default)</td><td>next micro-batch as soon as the previous one finishes (<code>processingTime</code> 0)</td><td>stopped</td><td>❌ must set one</td></tr>
<tr><td><code>processingTime="1 minute"</code></td><td>a micro-batch every interval</td><td>stopped</td><td>❌</td></tr>
<tr><td><code>availableNow=True</code></td><td>all data available now, in one or more micro-batches (respecting <code>maxFilesPerTrigger</code>…)</td><td><b>stops by itself</b></td><td>✅ recommended</td></tr>
<tr><td><code>once=True</code></td><td>one single micro-batch</td><td>stops by itself</td><td>✅ deprecated → use <code>availableNow</code></td></tr>
<tr><td>continuous / real-time mode</td><td>record-at-a-time, sub-second latency</td><td>stopped</td><td>❌</td></tr>
</table>
<div class="grid two">
 <div class="card green"><h3>🧱 Incremental batch</h3><code>availableNow</code> + a schedule (Lakeflow Job) = cheap, simple, <b>exactly-once</b>,
 no always-on compute. Most pipelines need nothing more.</div>
 <div class="card orange"><h3>⚡ Always on</h3><code>processingTime</code> on classic compute, or a <b>continuous Lakeflow pipeline</b> on serverless —
 latency in seconds, compute runs 24/7.</div>
</div>
""" + callout("trap", "“Streaming” ≠ “always running”. A query with <code>availableNow</code> is a <b>streaming</b> query "
              "(checkpoint, exactly-once) that behaves like a batch job."))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · Output modes

# COMMAND ----------

# DBTITLE 1,Slide · Output modes
show("""
<div class="kicker">Slide 4 · What is written each trigger?</div>
<h2>append · complete · update</h2>
<table class="tbl">
<tr><th>Mode</th><th>Writes</th><th>Allowed for</th><th>Delta table sink</th></tr>
<tr><td><b>append</b> (default)</td><td>only <b>new</b> result rows — never changed later</td><td>projections, filters, stream-static joins; aggregations <b>only with a watermark</b></td><td>✅</td></tr>
<tr><td><b>complete</b></td><td>the <b>entire</b> result table every trigger</td><td>aggregations</td><td>✅ (table overwritten)</td></tr>
<tr><td><b>update</b></td><td>only rows that <b>changed</b> since the last trigger</td><td>aggregations (and non-aggregations)</td><td>via <code>foreachBatch</code> + <code>MERGE</code></td></tr>
</table>
""" + callout("exam", "“A streaming aggregation fails with <i>Append output mode not supported … without watermark</i>” → "
              "use <b>complete</b> mode, or add a <b>watermark</b> (06-P3)."))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 · Checkpoints and exactly-once

# COMMAND ----------

# DBTITLE 1,Slide · Exactly-once
show("""
<div class="kicker">Slide 5 · Fault tolerance</div>
<h2>Replayable source + checkpoint (write-ahead log) + idempotent sink = exactly-once</h2>
<div class="flow">
 <div class="step"><b>1 · offsets/</b>before a micro-batch: record <strong>what</strong> it will read (WAL)</div><div class="arrow">➜</div>
 <div class="step"><b>2 · process</b>run the query on exactly that input</div><div class="arrow">➜</div>
 <div class="step"><b>3 · sink commit</b>Delta commits atomically; repeated batch ids are ignored</div><div class="arrow">➜</div>
 <div class="step"><b>4 · commits/</b>mark the batch done</div>
</div>
<div class="grid">
 <div class="card"><h3>💥 Crash between 1 and 4?</h3>Restart replays the same offsets; the idempotent sink ignores the duplicate commit.</div>
 <div class="card orange"><h3>🔑 One checkpoint per stream</h3>Never share it between queries; keep it next to the pipeline (e.g. a volume).</div>
 <div class="card red"><h3>🗑️ Checkpoint deleted?</h3>The stream starts over: all source data is re-read → <b>duplicates</b> in an append sink.</div>
</div>
""" + callout("info", "The checkpoint also stores <b>state</b> for aggregations, deduplication and joins, and the source's "
              "bookkeeping (for files: which files were already processed).")
  + callout("trap", "Some query changes are incompatible with an existing checkpoint (e.g. changing the aggregation keys or "
            "the stateful operator). Then you need a <b>new checkpoint</b> and usually a full reprocess."))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6 · Streaming SQL and what a stream can't do
# MAGIC
# MAGIC * Register a stream as a **temp view** → SQL on it is streaming: `spark.readStream.table("bronze").createOrReplaceTempView("v")`, then `spark.sql("SELECT … FROM v GROUP BY …")`.
# MAGIC * **Stream-static join**: join a stream with a normal table. New stream rows drive it; when the static side is a **Delta table**, each micro-batch joins its **latest version**.
# MAGIC * `display(streaming_df)` **starts a stream** (demo only; on serverless write with `availableNow` and query the table).
# MAGIC
# MAGIC | Not supported on a streaming DataFrame | Why / instead |
# MAGIC |---|---|
# MAGIC | `orderBy` / `sort` — unless after an aggregation in **complete** mode | a stream never ends; sort when reading the result table |
# MAGIC | `limit`, `first`, `take`, `count()`, `collect()`, `show()` as actions | write to a sink, then query the table |
# MAGIC | Chained stateful aggregations **without** watermarks (supported with watermarks in append mode since Spark 3.4) | add watermarks, or use `foreachBatch` |
# MAGIC | Outer stream-stream joins without watermarks | add watermarks (06-P3) |
# MAGIC
# MAGIC ## 7 · Managing queries
# MAGIC
# MAGIC | Handle | Use |
# MAGIC |---|---|
# MAGIC | `query = writer.toTable(…)` / `.start()` | returns a `StreamingQuery` |
# MAGIC | `query.status`, `query.lastProgress`, `query.recentProgress` | what it's doing; rows per batch, durations, source/sink details |
# MAGIC | `query.awaitTermination()` | block until it ends (e.g. an `availableNow` run) |
# MAGIC | `query.stop()` | stop it; `spark.streams.active` lists all running queries of the session |
# MAGIC | Query name | `.queryName("orders_bronze")` shows up in the UI and in progress events |
# MAGIC
# MAGIC > 💰 A stream with `processingTime` keeps compute busy forever — always stop experiments (`for q in spark.streams.active: q.stop()`).
# MAGIC
# MAGIC ## ✅ Key takeaways
# MAGIC 1. A stream = an **unbounded table**; each **micro-batch** processes only new data with the same DataFrame/SQL API.
# MAGIC 2. Every sink needs a **unique checkpoint** — it gives **exactly-once** together with replayable sources and idempotent sinks (Delta).
# MAGIC 3. Triggers: default (back-to-back micro-batches, `processingTime` 0), `processingTime`, **`availableNow`** (serverless ✅), `once` (deprecated).
# MAGIC 4. Output modes: **append** (default; aggregations need a watermark), **complete** (whole result), **update** (via `foreachBatch`).
# MAGIC 5. No sorting/limits on raw streams; `display()` starts a stream; stop what you start.
# MAGIC
# MAGIC ➡️ Next: **06-P2 · Auto Loader**
