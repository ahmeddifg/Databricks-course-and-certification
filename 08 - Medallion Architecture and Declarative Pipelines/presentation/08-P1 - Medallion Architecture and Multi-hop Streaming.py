# Databricks notebook source
# MAGIC %md
# MAGIC # 🏅 08-P1 · Medallion Architecture and Multi-hop Streaming
# MAGIC **Section 08 — Medallion Architecture: hand-coded → declarative** · exam objectives **3.1** (bronze → silver cleaning),
# MAGIC **3.6** (gold layer objects), **2.x** (incremental ingestion)
# MAGIC
# MAGIC | After this presentation you can… |
# MAGIC |---|
# MAGIC | Explain the **bronze / silver / gold** layers and what belongs in each |
# MAGIC | Build a **multi-hop** pipeline with Structured Streaming: one stream + one checkpoint per hop |
# MAGIC | Add **ingestion metadata** in bronze and explain why |
# MAGIC | Apply the rules of **stream-static joins** |
# MAGIC | Choose the right **gold object** (table, view, materialized view, streaming table) |
# MAGIC | Name the pain points of hand-coded pipelines that **declarative pipelines** solve |
# MAGIC
# MAGIC > ▶️ **Run all** to render the diagrams (a few seconds, any compute).

# COMMAND ----------

# MAGIC %run ../../Includes/_style

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · The medallion architecture

# COMMAND ----------

# DBTITLE 1,Slide · Bronze, silver, gold
show("""
<div class="kicker">Slide 1 · A data design pattern, not a product</div>
<h2>Improve data quality step by step — every layer is a Delta table in Unity Catalog</h2>
<div class="layer" style="background:#9c5b2e"><b>🥉 BRONZE — raw</b> &nbsp; <small>data as it arrived (append-only) + ingestion metadata ·
 minimal or no validation · the replayable history of the source</small></div>
<div class="layer" style="background:#6c757d"><b>🥈 SILVER — cleaned &amp; conformed</b> &nbsp; <small>types fixed, nulls handled, invalid rows
 dropped/quarantined, duplicates removed, joined/enriched · "single source of truth" at record level</small></div>
<div class="layer" style="background:#b8860b"><b>🥇 GOLD — business-ready</b> &nbsp; <small>aggregates, KPIs, star schemas, feature tables ·
 shaped for one use case (BI dashboard, report, ML) · read by analysts</small></div>
<div class="flow">
 <div class="step"><b>Sources</b>files, Kafka, CDC, SaaS</div><div class="arrow">➜</div>
 <div class="step"><b>🥉 Bronze</b>ingest</div><div class="arrow">➜</div>
 <div class="step"><b>🥈 Silver</b>clean · dedup · enrich</div><div class="arrow">➜</div>
 <div class="step"><b>🥇 Gold</b>aggregate · model</div><div class="arrow">➜</div>
 <div class="step"><b>Consumers</b>BI · SQL · ML · apps</div>
</div>
""" + callout("exam", "Also called a <b>multi-hop</b> architecture. Quality goes <b>up</b> from bronze to gold; the number of "
              "consumers goes up too. Data engineers own bronze/silver; analysts mostly read gold."))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · What goes where?
# MAGIC
# MAGIC | | 🥉 Bronze | 🥈 Silver | 🥇 Gold |
# MAGIC |---|---|---|---|
# MAGIC | **Content** | raw records, all columns, often as strings / JSON | typed, validated, deduplicated, enriched records | aggregates, dimensions & facts, KPIs |
# MAGIC | **Schema** | from the source (schema evolution, `_rescued_data`) | enforced, explicit types | designed for the question |
# MAGIC | **Quality rules** | none — keep everything (so you can replay) | expectations / filters / quarantine | business rules, reconciliation |
# MAGIC | **Extra columns** | `source_file` (`_metadata.file_path`/`file_name`), `ingested_at` | `order_ts` (typed), flags | measures, keys |
# MAGIC | **Writes** | append | append / merge (upserts, CDC, SCD) | overwrite / complete / MV refresh |
# MAGIC | **Typical object** | streaming table | streaming table | materialized view, view, table |
# MAGIC | **Who reads** | data engineers | engineers, data scientists | analysts, BI tools |
# MAGIC
# MAGIC > 💡 **Why keep bronze at all?** Silver logic *will* change (a bug, a new rule). With bronze you rebuild silver from your
# MAGIC > own copy — even if the source system has already deleted the data.
# MAGIC
# MAGIC ## 3 · Multi-hop with Structured Streaming

# COMMAND ----------

# DBTITLE 1,Slide · One stream per hop
show("""
<div class="kicker">Slide 3 · Hand-coded medallion = a chain of streaming queries</div>
<h2>Each hop reads the previous table as a stream and has its own checkpoint</h2>
<div class="flow">
 <div class="step"><b>📂 Landing folder</b>JSON files</div><div class="arrow">➜</div>
 <div class="step"><b>Auto Loader</b><code>cloudFiles</code><br><span class="muted">ckpt 1 + schema location</span></div><div class="arrow">➜</div>
 <div class="step"><b>🥉 bronze table</b>append</div><div class="arrow">➜</div>
 <div class="step"><b>readStream.table</b>clean · dedup · join dim<br><span class="muted">ckpt 2</span></div><div class="arrow">➜</div>
 <div class="step"><b>🥈 silver table</b>append</div><div class="arrow">➜</div>
 <div class="step"><b>readStream.table</b>groupBy · agg<br><span class="muted">ckpt 3</span></div><div class="arrow">➜</div>
 <div class="step"><b>🥇 gold table</b><code>complete</code></div>
</div>
<div class="grid">
 <div class="card"><h3>▶️ Run it</h3>continuously (classic compute), or as <b>incremental batches</b> with
 <code>trigger(availableNow=True)</code> — serverless-friendly and cheap</div>
 <div class="card green"><h3>🔁 Incremental everywhere</h3>each hop reads only rows it hasn't seen — the checkpoint
 remembers the Delta table version / files already processed</div>
 <div class="card orange"><h3>🔗 Order matters</h3>bronze → silver → gold must run in this order: a job with 3 dependent
 tasks, or one notebook running them one after the other</div>
</div>
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ```python
# MAGIC # 🥉 bronze - Auto Loader + ingestion metadata
# MAGIC (spark.readStream.format("cloudFiles")
# MAGIC       .option("cloudFiles.format", "json")
# MAGIC       .option("cloudFiles.schemaLocation", f"{ckpt}/bronze")
# MAGIC       .load(landing)
# MAGIC       .select("*", F.col("_metadata.file_name").alias("source_file"), F.current_timestamp().alias("ingested_at"))
# MAGIC       .writeStream.option("checkpointLocation", f"{ckpt}/bronze")
# MAGIC       .trigger(availableNow=True).toTable("orders_bronze"))
# MAGIC
# MAGIC # 🥈 silver - typed, filtered, deduplicated, enriched (stream-static join)
# MAGIC (spark.readStream.table("orders_bronze")
# MAGIC       .where("quantity > 0")
# MAGIC       .withColumn("order_ts", F.from_unixtime("order_timestamp").cast("timestamp"))   # Unix seconds -> TIMESTAMP
# MAGIC       .withWatermark("order_ts", "1 day").dropDuplicatesWithinWatermark(["order_id"])
# MAGIC       .join(spark.read.table("customers"), "customer_id", "left")
# MAGIC       .writeStream.option("checkpointLocation", f"{ckpt}/silver")
# MAGIC       .trigger(availableNow=True).toTable("orders_silver"))
# MAGIC
# MAGIC # 🥇 gold - streaming aggregate, rewritten completely on each trigger
# MAGIC (spark.readStream.table("orders_silver")
# MAGIC       .groupBy(F.to_date("order_ts").alias("order_date"), "country")
# MAGIC       .agg(F.count("*").alias("orders"), F.sum("total").alias("revenue"))
# MAGIC       .writeStream.outputMode("complete").option("checkpointLocation", f"{ckpt}/gold")
# MAGIC       .trigger(availableNow=True).toTable("daily_country_revenue"))
# MAGIC ```
# MAGIC
# MAGIC ## 4 · Stream-static joins

# COMMAND ----------

# DBTITLE 1,Slide · Stream-static join rules
show("""
<div class="kicker">Slide 4 · Enrich a stream with a dimension table</div>
<h2><code>stream.join(static_df, key, how)</code></h2>
<div class="grid two">
 <div class="card"><h3>✅ How it behaves</h3><ul>
  <li>Only the <b>streaming</b> side drives processing — new rows trigger work</li>
  <li>A change of the <b>static</b> table alone triggers nothing</li>
  <li>Static side = a <b>Delta table</b> → each micro-batch joins its <b>latest version</b></li>
  <li>Rows already written are <b>never re-joined</b> — old silver rows keep old values</li>
  <li>Stateless: no watermark needed</li></ul></div>
 <div class="card orange"><h3>⚠️ Choose the join type deliberately</h3><ul>
  <li><b>inner</b>: rows without a match are <b>dropped silently</b> (e.g. a customer not yet in the dimension)</li>
  <li><b>left</b> (stream on the left): keep them with NULLs → flag or quarantine in silver</li>
  <li>Late-arriving dimension rows? → re-process, or model the fact with an <b>SCD2</b> point-in-time join</li></ul></div>
</div>
""" + callout("trap", "“The dimension table was updated but the silver rows didn't change” → expected: a stream-static join "
              "only enriches <b>new</b> streaming rows. Stream-<b>stream</b> joins (both sides streaming) need watermarks — see 06-P3."))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 · Gold: which object?
# MAGIC Exam objective 3.6 asks you to know the **difference** between the gold objects in Unity Catalog:
# MAGIC
# MAGIC | Object | Stores data? | Freshness | Cost | Use it when… |
# MAGIC |---|---|---|---|---|
# MAGIC | **View** | ❌ (a saved query) | always current | compute on every read | light logic, access control (dynamic views), few readers |
# MAGIC | **Table** (CTAS / `saveAsTable` / streaming `complete`) | ✅ | as fresh as your last write | you schedule the writes | full control, custom logic, hand-coded pipelines |
# MAGIC | **Materialized view** | ✅ precomputed | refreshed by a pipeline (schedule / trigger), **incrementally** when possible | cheap reads | aggregates & joins read by BI — the default gold object in SDP |
# MAGIC | **Streaming table** | ✅ | each refresh appends **new** input only | cheap incremental | append-only feeds — bronze/silver; gold only for append-style facts |
# MAGIC
# MAGIC > 🎯 A **streaming aggregation** in `complete` mode rewrites the whole result each trigger — fine for small gold tables.
# MAGIC > A **materialized view** recomputes (incrementally when possible) from its **current** inputs, so corrections in silver
# MAGIC > show up in gold. That's why SDP pipelines usually end with MVs.
# MAGIC
# MAGIC ## 6 · Running the hops
# MAGIC
# MAGIC | Pattern | Latency | Cost | Where |
# MAGIC |---|---|---|---|
# MAGIC | `availableNow` streams in a **scheduled job** (every 15 min, hourly…) | minutes | pay only while it runs | serverless ✅ · classic ✅ |
# MAGIC | Continuous streams (`processingTime` trigger) | seconds | compute runs 24/7 | classic compute only |
# MAGIC | **Declarative pipeline** — triggered or continuous | minutes / seconds | managed | serverless ✅ · classic ✅ |
# MAGIC
# MAGIC ## 7 · The pain points of hand-coded pipelines

# COMMAND ----------

# DBTITLE 1,Slide · Why declarative?
show("""
<div class="kicker">Slide 7 · What you manage yourself when you hand-code a medallion</div>
<h2>Imperative: you say <i>how</i>. Declarative: you say <i>what</i> — the platform works out the how.</h2>
<table class="tbl">
<tr><th>Concern</th><th>Hand-coded Structured Streaming</th><th>Lakeflow Spark Declarative Pipelines</th></tr>
<tr><td>Order of the hops</td><td>you wire tasks / call streams in order</td><td>dependency graph derived from the code</td></tr>
<tr><td>Checkpoints &amp; schema locations</td><td>one per stream, never share, never lose</td><td>managed for you</td></tr>
<tr><td>Retries, restarts, schema evolution</td><td>a job with retries; restart after <code>addNewColumns</code></td><td>automatic in production runs</td></tr>
<tr><td>Data quality</td><td><code>where()</code> silently drops rows</td><td><b>expectations</b> with metrics (warn / drop / fail)</td></tr>
<tr><td>Gold recomputation</td><td>rebuild by hand</td><td><b>materialized views</b>, incremental refresh</td></tr>
<tr><td>CDC / SCD</td><td><code>foreachBatch</code> + <code>MERGE</code></td><td><code>AUTO CDC … STORED AS SCD TYPE 1 | 2</code></td></tr>
<tr><td>Lineage &amp; monitoring</td><td>query progress, your own logging</td><td>graph, <b>event log</b>, data-quality tab</td></tr>
</table>
""" + callout("tip", "Hand-coded streaming is still the right tool for special sinks, arbitrary <code>foreachBatch</code> logic "
              "or when you need full control. For standard medallion ETL, declarative pipelines remove most of the plumbing."))

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Key takeaways
# MAGIC 1. **Bronze** = raw + ingestion metadata (append, replayable) · **silver** = typed, valid, deduplicated, enriched ·
# MAGIC    **gold** = aggregated / modeled for consumers.
# MAGIC 2. Hand-coded multi-hop = **one streaming query and one checkpoint per hop**, run in dependency order —
# MAGIC    `availableNow` + a scheduled job for incremental batches.
# MAGIC 3. `from_unixtime(unix_seconds)` → string → `.cast("timestamp")` (or `timestamp_seconds()`); do it in **silver**.
# MAGIC 4. **Stream-static join**: the stream drives; a Delta static side is read at its **latest version** per micro-batch;
# MAGIC    old rows are never re-joined; inner joins drop unmatched rows silently.
# MAGIC 5. Gold objects: **view** (no storage), **table**, **materialized view** (precomputed, incrementally refreshed),
# MAGIC    **streaming table** (append-only) — know when to use each.
# MAGIC 6. Streaming aggregates without a watermark → **complete** mode (or update via `foreachBatch`).
# MAGIC
# MAGIC ➡️ Next: **08-P2 · Lakeflow Spark Declarative Pipelines**
