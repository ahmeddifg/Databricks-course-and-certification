# Databricks notebook source
# MAGIC %md
# MAGIC # 🧊 12-P3 · Data Layout — Liquid Clustering and Predictive Optimization
# MAGIC **Section 12** · exam objective **6.4** — *understand the features of Liquid Clustering and predictive optimization*
# MAGIC
# MAGIC | After this presentation you can… |
# MAGIC |---|
# MAGIC | Explain **data skipping** (per-file min/max statistics) and the **small-files** problem |
# MAGIC | Know when **partitioning** helps and why it usually hurts (cardinality, partition size, over-partitioning) |
# MAGIC | Explain **Z-order** and why it isn't incremental |
# MAGIC | Use **liquid clustering**: `CLUSTER BY`, `CLUSTER BY AUTO`, `ALTER TABLE … CLUSTER BY`, `OPTIMIZE`, `OPTIMIZE FULL` — and its limits |
# MAGIC | Explain **predictive optimization**: what it runs, on which tables, how it is enabled and monitored |
# MAGIC
# MAGIC > ▶️ **Run all** to render the diagrams. Practice: **labs/12-L3** (small files → OPTIMIZE, partitioning, Z-order, liquid clustering).

# COMMAND ----------

# MAGIC %run ../../Includes/_style

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · Why layout matters: data skipping

# COMMAND ----------

# DBTITLE 1,Slide · Data skipping
_files = [("file 1", "C0001", "C0300", True), ("file 2", "C0001", "C0300", True), ("file 3", "C0002", "C0299", True), ("file 4", "C0001", "C0300", True)]
_clus = [("file 1", "C0001", "C0075", False), ("file 2", "C0076", "C0150", True), ("file 3", "C0151", "C0225", False), ("file 4", "C0226", "C0300", False)]


def _filebox(rows):
    return "".join(f'<div class="step" style="border:2px solid {"#e03131" if hit else "#2f9e44"}"><b>{n}</b>'
                   f'min {lo} · max {hi}<br>{"📖 must read" if hit else "⏭️ skipped"}</div>' for n, lo, hi, hit in rows)


show(f"""
<div class="kicker">Slide 1 · Delta keeps min/max per column for every file — a filter can skip files whose range can't match</div>
<p><code>SELECT … FROM events WHERE customer_id = 'C0123'</code></p>
<p><b>Unclustered</b> — every file contains every customer → 4 of 4 files read</p>
<div class="flow">{_filebox(_files)}</div>
<p><b>Clustered by customer_id</b> — each file holds a narrow range → 1 of 4 files read</p>
<div class="flow">{_filebox(_clus)}</div>
""" + callout("info", "Statistics are collected on the <b>first 32 columns</b> by default (<code>delta.dataSkippingNumIndexedCols</code>, or choose with "
              "<code>delta.dataSkippingStatsColumns</code>). Skipping only helps if rows with similar values are <b>physically together</b> — that is what "
              "partitioning, Z-order and liquid clustering do."))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · The small-files problem
# MAGIC Streaming micro-batches, frequent small `INSERT`s and over-partitioning create **thousands of tiny files**: every query pays
# MAGIC for listing, opening and reading the metadata of each file, and the Spark UI shows thousands of tiny tasks.
# MAGIC
# MAGIC | Fix | What it does |
# MAGIC |---|---|
# MAGIC | `OPTIMIZE t` | **bin-packing**: rewrites small files into fewer large ones (≈ 256 MB–1 GB, auto-tuned by table size); **idempotent** |
# MAGIC | **Auto compaction** (`delta.autoOptimize.autoCompact`) | compacts small files right after a write, on the same compute |
# MAGIC | **Optimized writes** (`delta.autoOptimize.optimizeWrite`) | shuffles data before writing so each write produces fewer, larger files |
# MAGIC | **Predictive optimization** | Databricks runs `OPTIMIZE` for you when it is worth it (section 6) |
# MAGIC
# MAGIC ## 3 · Hive-style partitioning — the old tool

# COMMAND ----------

# DBTITLE 1,Slide · Partitioning
show("""
<div class="kicker">Slide 3 · PARTITIONED BY = one folder per value — great pruning, easy to get wrong</div>
<div class="grid two">
 <div class="card green"><h3>✅ When it helps</h3><ul>
  <li>very large tables (rule of thumb: <b>&gt; 1 TB</b>)</li>
  <li>a <b>low-cardinality</b> column almost every query filters on (date, region)</li>
  <li>each partition holds <b>≥ 1 GB</b> of data</li>
  <li>you need to delete/overwrite whole partitions (data retention)</li></ul>
  <code>CREATE TABLE t (…) PARTITIONED BY (event_date)</code><br><code>WHERE event_date = '2026-03-01'</code> → only that folder is read (<b>partition pruning</b>)</div>
 <div class="card red"><h3>⛔ Where it hurts</h3><ul>
  <li><b>high-cardinality</b> column (customer_id, timestamp) → millions of folders and <b>tiny files</b></li>
  <li>small tables: 181 days × 16 writer tasks = up to <b>2,896 files</b> of a few KB</li>
  <li>partition columns are <b>fixed</b>: changing them = rewriting the whole table</li>
  <li>skew: one huge partition (today, a hot region)</li>
  <li>only helps queries that filter on the partition column</li></ul></div>
</div>
""" + callout("exam", "Databricks recommends <b>not</b> partitioning tables under ~1 TB and using <b>liquid clustering</b> for new tables. "
              "Liquid clustering and partitioning are <b>mutually exclusive</b> on a table."))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · Z-order — co-locate values inside files
# MAGIC
# MAGIC ```sql
# MAGIC OPTIMIZE perf_events ZORDER BY (customer_id, product_id)
# MAGIC ```
# MAGIC
# MAGIC * Sorts data along a **space-filling curve** so files have narrow min/max ranges on the Z-order columns → data skipping on
# MAGIC   several columns at once (effectiveness drops with each extra column; 1–4 columns).
# MAGIC * Works **within partitions** and can't use a partition column.
# MAGIC * ⚠️ **Not incremental**: Z-order isn't remembered by the table — new data arrives unclustered and the next `ZORDER` run
# MAGIC   rewrites the whole partition again (for an unpartitioned table: the whole table — only very large, already Z-ordered
# MAGIC   “cubes” of ~100 GB are left alone). It is also **not idempotent**.
# MAGIC * You must re-run it yourself (or in a job) — and choose the columns again each time.
# MAGIC
# MAGIC ## 5 · Liquid clustering — the recommended layout

# COMMAND ----------

# DBTITLE 1,Slide · Liquid clustering
show("""
<div class="kicker">Slide 5 · CLUSTER BY = clustering keys stored in the table, applied incrementally, changeable any time</div>
<div class="grid">
 <div class="card green"><h3>🆕 Create</h3><code>CREATE TABLE events (…) CLUSTER BY (customer_id, event_date)</code><br><code>CREATE TABLE t CLUSTER BY (c) AS SELECT …</code><br><code>df.write.clusterBy("c").saveAsTable("t")</code><br><code>writeStream.clusterBy("c").toTable("t")</code></div>
 <div class="card"><h3>🔁 Maintain</h3><code>OPTIMIZE events</code> → <b>incremental</b>: only data that isn't clustered yet is rewritten<br><code>OPTIMIZE events FULL</code> → recluster <b>all</b> data (e.g. after changing keys)<br>small writes are clustered on write only above a size threshold; <code>OPTIMIZE</code> (or predictive optimization) does the rest</div>
 <div class="card purple"><h3>✏️ Change keys</h3><code>ALTER TABLE events CLUSTER BY (product_id)</code> — metadata only, <b>no rewrite</b>; new data / next <code>OPTIMIZE</code> follow the new keys<br><code>ALTER TABLE events CLUSTER BY NONE</code> — stop clustering</div>
 <div class="card orange"><h3>🤖 Automatic</h3><code>CLUSTER BY AUTO</code> — Databricks picks (and changes) the keys from your <b>query patterns</b>. Needs <b>predictive optimization</b> → Unity Catalog <b>managed</b> tables.</div>
</div>
<table class="tbl">
<tr><th>Fact</th><th>Detail</th></tr>
<tr><td>Keys</td><td>up to <b>4</b> columns that appear in <b>filters</b> (and joins); types: numbers, dates, timestamps, strings, nested struct fields — not map/array/struct columns. Correlated columns: keep one.</td></tr>
<tr><td>Incompatible with</td><td><b>PARTITIONED BY</b> and <b>ZORDER</b> (an <code>OPTIMIZE … ZORDER BY</code> on a clustered table fails)</td></tr>
<tr><td>Good for</td><td>high-cardinality filter columns, <b>skewed</b> data, fast-growing tables, <b>concurrent writes</b>, access patterns that change</td></tr>
<tr><td>Inspect</td><td><code>DESCRIBE DETAIL t</code> → <code>clusteringColumns</code> · <code>DESCRIBE TABLE EXTENDED</code> · history shows <code>OPTIMIZE</code> / <code>CLUSTER BY</code> operations</td></tr>
<tr><td>Availability</td><td>GA for Delta on DBR 15.4 LTS+ (and serverless / SQL warehouses); writers need a recent runtime (table feature <code>clustering</code>)</td></tr>
<tr><td>Migrate</td><td>unpartitioned table: <code>ALTER TABLE t CLUSTER BY (…)</code> + <code>OPTIMIZE t FULL</code> · partitioned table: <code>ALTER TABLE t REPLACE PARTITIONED BY WITH CLUSTER BY (…)</code> or CTAS / DEEP CLONE into a new clustered table</td></tr>
</table>
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Partitioning vs Z-order vs liquid clustering
# MAGIC
# MAGIC | | Partitioning | Z-order | **Liquid clustering** |
# MAGIC |---|---|---|---|
# MAGIC | Defined | at `CREATE` (`PARTITIONED BY`) | at each `OPTIMIZE … ZORDER BY` | at `CREATE` or `ALTER` (`CLUSTER BY`) — stored in the table |
# MAGIC | High-cardinality columns | ❌ tiny files | ✅ | ✅ |
# MAGIC | Incremental | — | ❌ rewrites the partition each time | ✅ `OPTIMIZE` only clusters what's new |
# MAGIC | Change the columns | rewrite the table | next run (full rewrite) | `ALTER TABLE … CLUSTER BY` — no rewrite |
# MAGIC | Combine with the others | ✅ with Z-order | ✅ inside partitions | ❌ neither |
# MAGIC | Automatic choice | — | — | ✅ `CLUSTER BY AUTO` |
# MAGIC | Recommended for new tables | ❌ (only > 1 TB, special cases) | ❌ | ✅ |
# MAGIC
# MAGIC ## 6 · Predictive optimization — maintenance on autopilot

# COMMAND ----------

# DBTITLE 1,Slide · Predictive optimization
show("""
<div class="kicker">Slide 6 · Databricks watches how tables are used and runs maintenance when it pays off</div>
<div class="flow">
 <div class="step"><b>👀 Observe</b>writes, reads, query patterns, file sizes</div><div class="arrow">➜</div>
 <div class="step" style="border:2px solid #2f9e44"><b>🧠 Decide</b>is maintenance worth its cost?</div><div class="arrow">➜</div>
 <div class="step"><b>⚙️ Run</b><code>OPTIMIZE</code> (compaction + incremental <b>clustering</b>, incl. AUTO keys)<br><code>VACUUM</code><br><code>ANALYZE</code> (statistics)</div><div class="arrow">➜</div>
 <div class="step"><b>🧾 Record</b><code>system.storage.predictive_optimization_operations_history</code></div>
</div>
<table class="tbl">
<tr><th>Fact</th><th>Detail</th></tr>
<tr><td>Tables</td><td>Unity Catalog <b>managed</b> tables only (Delta and Iceberg) — <b>not external</b> tables</td></tr>
<tr><td>Default</td><td><b>enabled by default</b> for accounts created on or after 11 Nov 2024; being rolled out to older accounts</td></tr>
<tr><td>Control</td><td><code>ALTER CATALOG c { ENABLE | DISABLE | INHERIT } PREDICTIVE OPTIMIZATION</code> — same for <code>SCHEMA</code> and <code>TABLE</code>; objects <b>inherit</b> from their parent by default (account → catalog → schema → table)</td></tr>
<tr><td>Check</td><td><code>DESCRIBE CATALOG|SCHEMA|TABLE EXTENDED …</code> shows <i>Predictive Optimization: ENABLE (inherited from …)</i>; Catalog Explorer → table → <i>Details</i>/<i>History</i></td></tr>
<tr><td>Cost</td><td>runs on <b>serverless</b> compute, billed under the serverless jobs SKU — you don't schedule or size anything</td></tr>
<tr><td>Requirements</td><td>Premium plan or above, supported region; works with tables written from SQL warehouses or DBR 12.2 LTS+</td></tr>
</table>
""" + callout("tip", "With predictive optimization + liquid clustering (or <code>CLUSTER BY AUTO</code>) you no longer need nightly <code>OPTIMIZE</code>/<code>VACUUM</code> jobs: "
              "remove them — they cost compute and can conflict with concurrent writes."))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7 · Decision guide
# MAGIC
# MAGIC | Situation | Choose |
# MAGIC |---|---|
# MAGIC | New table, you know the filter columns | `CLUSTER BY (col1, col2)` |
# MAGIC | New UC managed table, query patterns unknown or changing | `CLUSTER BY AUTO` (+ predictive optimization) |
# MAGIC | Existing partitioned table that suffers from small files | `REPLACE PARTITIONED BY WITH CLUSTER BY` (or CTAS / clone into a clustered table) |
# MAGIC | Existing Z-ordered table | `ALTER TABLE … CLUSTER BY (same cols)` + `OPTIMIZE … FULL`, stop running ZORDER |
# MAGIC | Table > 1 TB, every query filters on a low-cardinality date, partitions ≥ 1 GB | partitioning can still be OK — liquid clustering is usually still better |
# MAGIC | Many small files, no clear filter column | `OPTIMIZE` / auto compaction / predictive optimization |
# MAGIC | External table | no predictive optimization → schedule `OPTIMIZE` / `VACUUM` / `ANALYZE` yourself (or convert to managed) |
# MAGIC
# MAGIC ## 🕰️ Recognize on the exam
# MAGIC * *Auto Optimize* = old umbrella name for **optimized writes + auto compaction**.
# MAGIC * Old courses put `OPTIMIZE … ZORDER BY` in a nightly job — today: **liquid clustering + predictive optimization**.
# MAGIC * `VACUUM` deletes unreferenced files older than the retention (7 days default) — predictive optimization runs it for you
# MAGIC   on managed tables; time travel beyond the retention stops working (Section 03).
# MAGIC
# MAGIC ## ✅ Key takeaways
# MAGIC 1. **Data skipping** uses per-file **min/max**; layout decides how many files a filter can skip. Small files hurt every query.
# MAGIC 2. **Partitioning**: only for very large tables on low-cardinality columns with ≥ 1 GB partitions; never on high cardinality.
# MAGIC 3. **Z-order**: multi-column co-location, but **not incremental** — each run rewrites the partition; can't combine with liquid.
# MAGIC 4. **Liquid clustering**: `CLUSTER BY` (≤ 4 keys) / `CLUSTER BY AUTO`, **incremental** `OPTIMIZE`, `OPTIMIZE FULL`,
# MAGIC    keys changeable with `ALTER TABLE … CLUSTER BY` without rewrite, **incompatible** with partitioning and ZORDER.
# MAGIC 5. **Predictive optimization**: automatic `OPTIMIZE` (incl. clustering), `VACUUM`, `ANALYZE` on **UC managed** tables;
# MAGIC    `ENABLE | DISABLE | INHERIT` at catalog / schema / table; history in `system.storage.predictive_optimization_operations_history`.
