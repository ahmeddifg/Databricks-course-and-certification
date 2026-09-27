# Databricks notebook source
# MAGIC %md
# MAGIC # ⏳ 06-P3 · Stateful Streaming: Windows, Watermarks & `foreachBatch`
# MAGIC **Section 06 — Incremental Processing** · supports **D2** and **D3** (streaming transformations)
# MAGIC
# MAGIC | After this presentation you can… |
# MAGIC |---|
# MAGIC | Tell **stateless** from **stateful** streaming operations |
# MAGIC | Distinguish **event time** from **processing time** |
# MAGIC | Use **tumbling, sliding and session windows** |
# MAGIC | Explain **watermarks**: late data, state cleanup, and how they interact with output modes |
# MAGIC | Deduplicate a stream with bounded state |
# MAGIC | Use **`foreachBatch`** for `MERGE` upserts and multiple sinks — and know its guarantees |
# MAGIC | Describe stream-static vs stream-stream joins, Delta and Kafka sources |
# MAGIC
# MAGIC > ▶️ **Run all** to render the diagrams (a few seconds, any compute).

# COMMAND ----------

# MAGIC %run ../../Includes/_style

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · Stateless vs stateful

# COMMAND ----------

# DBTITLE 1,Slide · State
show("""
<div class="kicker">Slide 1 · Does the query need to remember anything?</div>
<h2>Stateless operations forget; stateful operations remember across micro-batches</h2>
<div class="grid two">
 <div class="card green"><h3>🪶 Stateless</h3><ul>
  <li><code>select</code>, <code>where</code>, <code>withColumn</code>, <code>explode</code></li>
  <li>stream-<b>static</b> joins</li>
  <li>each row handled on its own → nothing kept</li></ul></div>
 <div class="card orange"><h3>🧠 Stateful</h3><ul>
  <li><code>groupBy</code> aggregations (with or without windows)</li>
  <li><code>dropDuplicates</code> / <code>dropDuplicatesWithinWatermark</code></li>
  <li>stream-<b>stream</b> joins, custom state (<code>transformWithState</code>)</li>
  <li>state lives in the <b>checkpoint</b> (<code>state/</code>) — it grows unless something cleans it</li></ul></div>
</div>
""" + callout("exam", "The thing that cleans up state is the <b>watermark</b>. Without it, state for an aggregation or dedup "
              "keeps growing for the lifetime of the stream."))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · Event time and windows
# MAGIC
# MAGIC | Time | Meaning | Example |
# MAGIC |---|---|---|
# MAGIC | **Event time** | when it happened — a column in the data | `event_time` of a click |
# MAGIC | **Processing time** | when Spark processes it | `current_timestamp()` in the micro-batch |
# MAGIC
# MAGIC Windows group rows by **event time**, so out-of-order data still lands in the right bucket:

# COMMAND ----------

# DBTITLE 1,Slide · Window types
show("""
<div class="kicker">Slide 2 · Window types</div>
<h2>Tumbling · Sliding · Session</h2>
<table class="tbl">
<tr><th>Window</th><th>Code</th><th>Shape</th></tr>
<tr><td><b>Tumbling</b></td><td><code>F.window("event_time", "30 minutes")</code></td><td>fixed, non-overlapping: 10:00–10:30, 10:30–11:00 …</td></tr>
<tr><td><b>Sliding</b></td><td><code>F.window("event_time", "30 minutes", "10 minutes")</code></td><td>fixed size, overlapping: 10:00–10:30, 10:10–10:40 … (a row can be in several)</td></tr>
<tr><td><b>Session</b></td><td><code>F.session_window("event_time", "15 minutes")</code></td><td>dynamic: closes after 15 min without events (per key)</td></tr>
</table>
<pre style="font-size:12.5px;background:#f7f9fc;border:1px solid #e3e8ef;border-radius:8px;padding:10px">(clicks.withWatermark("event_time", "1 hour")
       .groupBy(F.window("event_time", "30 minutes"), "page")
       .count())                                   # result column `window` is a struct&lt;start, end&gt;</pre>
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · Watermarks

# COMMAND ----------

# DBTITLE 1,Slide · Watermark
show("""
<div class="kicker">Slide 3 · How late is too late?</div>
<h2>Watermark = max event time seen − threshold</h2>
<div class="flow">
 <div class="step"><b>Batch 1</b>max event time 11:57<br>→ watermark <b>10:57</b></div><div class="arrow">➜</div>
 <div class="step"><b>Batch 2</b>windows ending ≤ 10:57 are <b>final</b> → written (append) &amp; state dropped</div><div class="arrow">➜</div>
 <div class="step"><b>Batch 4</b>watermark ~15:00; a click from 10:12 arrives → <b>dropped</b> (too late)</div>
</div>
<table class="tbl">
<tr><th></th><th>no watermark</th><th>watermark + <b>append</b></th><th>watermark + <b>update</b></th></tr>
<tr><td>When is a window written?</td><td>complete mode: every trigger (whole table)</td><td><b>once</b>, after the watermark passes its end</td><td>every trigger in which it changed</td></tr>
<tr><td>Late data</td><td>always counted</td><td>dropped once older than the watermark</td><td>dropped once older than the watermark</td></tr>
<tr><td>State</td><td>kept forever</td><td>cleaned up</td><td>cleaned up</td></tr>
</table>
""" + callout("trap", "In <b>append</b> mode the newest windows appear only <b>after</b> newer data moves the watermark — results are "
              "delayed by (window + threshold). That's the price of never rewriting a row.")
  + callout("info", "Data within the threshold is always processed; data beyond it <i>may</i> be dropped (not guaranteed). "
            "With several inputs the global watermark is the <b>minimum</b> by default."))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · Deduplication on a stream
# MAGIC
# MAGIC | Code | State | Use |
# MAGIC |---|---|---|
# MAGIC | `df.dropDuplicates(["id"])` (no watermark) | every id **forever** | tiny streams only |
# MAGIC | `df.withWatermark("ts", "1 hour").dropDuplicates(["id", "ts"])` | bounded by the watermark | classic pattern (event time must be in the key) |
# MAGIC | `df.withWatermark("ts", "1 hour").dropDuplicatesWithinWatermark(["id"])` | bounded; duplicates may have **different** timestamps | recommended (Spark 3.5+ / DBR 13.3+) |
# MAGIC
# MAGIC ## 5 · `foreachBatch` — batch power inside a stream

# COMMAND ----------

# DBTITLE 1,Slide · foreachBatch
show("""
<div class="kicker">Slide 5 · foreachBatch</div>
<h2>Run any batch code on every micro-batch</h2>
<pre style="font-size:12.5px;background:#f7f9fc;border:1px solid #e3e8ef;border-radius:8px;padding:10px">def upsert(batch_df, batch_id):
    batch_df.createOrReplaceTempView("updates")
    batch_df.sparkSession.sql(\"\"\"
        MERGE INTO main.silver.customers t USING updates s ON t.id = s.id
        WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *\"\"\")

(stream.writeStream.foreachBatch(upsert)
       .option("checkpointLocation", ckpt).trigger(availableNow=True).start())</pre>
<div class="grid">
 <div class="card green"><h3>Use it for</h3><code>MERGE</code> upserts (CDC, SCD), writing one micro-batch to <b>several</b> tables, sinks without streaming support, update-mode aggregates into Delta</div>
 <div class="card orange"><h3>Guarantee</h3><b>at-least-once</b> for your function: a failed batch is retried with the same <code>batch_id</code> — make the logic idempotent</div>
 <div class="card"><h3>Idempotent Delta writes</h3><code>.option("txnAppId", app_id).option("txnVersion", batch_id)</code> on each <code>write</code> inside the function</div>
</div>
""" + callout("tip", "Inside <code>foreachBatch</code> use <b>fully qualified</b> table names and <code>batch_df.sparkSession</code>; "
              "the batch may run in a different session than your notebook."))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6 · Joins and sources you should recognise
# MAGIC
# MAGIC | Topic | What to know |
# MAGIC |---|---|
# MAGIC | **Stream-static join** | stateless; a static **Delta** table is joined at its latest version each micro-batch; inner/left (stream on the left) supported |
# MAGIC | **Stream-stream join** | both sides buffered in state; use **watermarks on both sides** + an event-time range condition so state can be dropped; outer joins **require** them |
# MAGIC | **Delta table source** | `spark.readStream.table(t)`; new commits = new data; fails on update/delete unless `skipChangeCommits`; `readChangeFeed` for CDC; `startingVersion` / `startingTimestamp`; `maxFilesPerTrigger` default 1000 |
# MAGIC | **Kafka source** | `format("kafka")`, `kafka.bootstrap.servers`, `subscribe`; columns `key`, `value` (binary → `CAST(value AS STRING)` → `from_json`), `topic`, `partition`, `offset`, `timestamp`; `startingOffsets` |
# MAGIC | **Custom state** | `transformWithState` / `applyInPandasWithState` for arbitrary stateful logic (sessions, alerts) |
# MAGIC
# MAGIC ## ✅ Key takeaways
# MAGIC 1. Aggregations, dedup and stream-stream joins are **stateful**; state lives in the checkpoint.
# MAGIC 2. Windows work on **event time**: tumbling, sliding, session.
# MAGIC 3. **Watermark** = max event time − threshold → drops too-late data and **cleans state**; append mode emits a window **once**, after the watermark passes it.
# MAGIC 4. Deduplicate with `dropDuplicatesWithinWatermark` (or `dropDuplicates` incl. the event-time column) to bound state.
# MAGIC 5. **`foreachBatch`** = batch code (MERGE, multi-sink) per micro-batch, **at-least-once** → make it idempotent.
# MAGIC
# MAGIC ➡️ Practice: **labs/06-L1 … 06-L3**, then **06-L4 Challenge** · Test yourself: **questions/06-Q**
