# Databricks notebook source
# MAGIC %md
# MAGIC # 🔬 12-P2 · Finding Bottlenecks — Skew, Shuffle and Spill
# MAGIC **Section 12** · exam objective **6.3** — *identify common performance bottlenecks such as data skew, shuffling, and disk
# MAGIC spilling by interpreting stage-level metrics in the Spark UI*
# MAGIC
# MAGIC | After this presentation you can… |
# MAGIC |---|
# MAGIC | Map **action → job → stage → task** and find the **longest stage** of a slow query |
# MAGIC | Read a stage's **Summary Metrics** (Min / 25th / Median / 75th / Max) and the **Shuffle** and **Spill** columns |
# MAGIC | Recognize **skew** (Max ≫ 75th percentile), heavy **shuffle** (Exchange, shuffle read/write) and **spill** (Spill disk > 0) |
# MAGIC | Pick the fix: AQE, **broadcast**, **salting**, filter/project early, right partition sizes, more memory, caching |
# MAGIC | Do the same on **serverless** with the **query profile** (serverless has no Spark UI) |
# MAGIC
# MAGIC > ▶️ **Run all** to render the diagrams. Practice: **labs/12-L2** (a real skewed dataset on serverless).

# COMMAND ----------

# MAGIC %run ../../Includes/_style

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · Where the time goes: job → stage → task

# COMMAND ----------

# DBTITLE 1,Slide · Hierarchy
show("""
<div class="kicker">Slide 1 · One action, several jobs; a shuffle cuts a job into stages; a stage = one task per partition</div>
<div class="flow">
 <div class="step"><b>⚡ Action</b><code>count()</code>, <code>write</code>, <code>display</code>, a SQL query</div><div class="arrow">➜</div>
 <div class="step"><b>🧾 Job</b>Jobs tab · <b>event timeline</b><br>gaps between jobs = driver busy</div><div class="arrow">➜</div>
 <div class="step" style="border:2px solid #1c7ed6"><b>🧱 Stage</b>boundary = <b>shuffle</b> (Exchange)<br>Stages tab: find the <b>longest</b> one</div><div class="arrow">➜</div>
 <div class="step"><b>🔩 Task</b>1 task = 1 partition on 1 core<br>stage ends when the <b>slowest</b> task ends</div>
</div>
<div class="grid">
 <div class="card"><h3>🖥️ Classic compute</h3><b>Spark UI</b>: Compute → your cluster → <i>Spark UI</i>, or the <i>Spark Jobs</i> link under a notebook cell, or a job task run → <i>Spark UI</i>. Tabs: Jobs · <b>Stages</b> · Storage · Environment · <b>Executors</b> · SQL/DataFrame</div>
 <div class="card green"><h3>☁️ Serverless &amp; SQL warehouses</h3><b>no Spark UI</b> — use the <b>query profile</b>: under a cell <i>See performance</i> → a query, Query history → <i>See query profile</i>, or from a job run's timeline</div>
 <div class="card orange"><h3>🔁 The diagnosis loop</h3>1 longest stage → 2 skew or spill? → 3 I/O bound (huge input, small files)? → 4 other: shuffle size, GC, driver</div>
</div>
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · Reading a stage page

# COMMAND ----------

# DBTITLE 1,Slide · Stage page anatomy
show("""
<div class="kicker">Slide 2 · Stages tab → click the longest stage → “Summary Metrics for 200 Completed Tasks”</div>
<table class="tbl">
<tr><th>Metric</th><th>Min</th><th>25th percentile</th><th>Median</th><th>75th percentile</th><th>Max</th></tr>
<tr><td><b>Duration</b></td><td>0.4 s</td><td>0.9 s</td><td>1.1 s</td><td>1.3 s</td><td style="background:#ffe8e8"><b>4.8 min</b></td></tr>
<tr><td>GC Time</td><td>0 ms</td><td>10 ms</td><td>20 ms</td><td>30 ms</td><td>41 s</td></tr>
<tr><td>Shuffle Read Size / Records</td><td>1.2 MB / 9 k</td><td>2.8 MB / 21 k</td><td>3.0 MB / 23 k</td><td>3.3 MB / 25 k</td><td style="background:#ffe8e8"><b>2.1 GB / 905 k</b></td></tr>
<tr><td>Spill (Memory)</td><td>0</td><td>0</td><td>0</td><td>0</td><td style="background:#fff0e6">6.4 GB</td></tr>
<tr><td>Spill (Disk)</td><td>0</td><td>0</td><td>0</td><td>0</td><td style="background:#fff0e6">1.9 GB</td></tr>
</table>
<div class="grid">
 <div class="card red"><h3>🐘 Skew</h3>Max ≫ 75th percentile — Databricks' rule of thumb: <b>Max more than 50 % above the 75th percentile</b>. One task reads far more (here 905 k records vs 25 k): one hot key.</div>
 <div class="card orange"><h3>💧 Spill</h3>Spill columns appear only when there is spill. <b>Spill (Memory)</b> = size in memory of what was spilled, <b>Spill (Disk)</b> = size written to disk. Any spill = memory pressure → slow (disk I/O + serialization).</div>
 <div class="card purple"><h3>🔀 Shuffle</h3><b>Shuffle Write</b> (map side) / <b>Shuffle Read</b> (reduce side) per stage. Many GB moved = network + disk cost → can the shuffle be avoided or made smaller?</div>
</div>
""" + callout("exam", "The three symptoms on the exam: <b>skew</b> = a few tasks much slower than the median (Max vs 75th percentile, Shuffle Read of one task); "
              "<b>shuffle</b> = big Shuffle Read/Write, Exchange operators, wide transformations; <b>spill</b> = non-zero Spill (Memory)/(Disk) columns."))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · The three bottlenecks — symptoms, causes, fixes
# MAGIC
# MAGIC | | 🐘 **Data skew** | 🔀 **Shuffle** | 💧 **Disk spill** |
# MAGIC |---|---|---|---|
# MAGIC | **What it is** | a few partitions hold most of the data (hot key, NULL key, one huge day) | data is redistributed across the network between stages | a task's data doesn't fit in execution memory → written to local disk and read back |
# MAGIC | **Spark UI** | Max duration / shuffle read ≫ 75th percentile; event timeline: one long bar, idle executors | Exchange in the DAG; large **Shuffle Read/Write**; many stages | **Spill (Memory)** / **Spill (Disk)** columns > 0; high GC time; *Peak execution memory* |
# MAGIC | **Query profile** | one operator with much higher *task time* than wall time share; uneven rows per task | **Shuffle** operators, bytes shuffled | **spilled bytes** in the operator details |
# MAGIC | **Typical causes** | `groupBy`/`join` on a skewed key (bots, `'unknown'`, NULL), partitioning by a skewed column | joins of two large tables, `groupBy`, `distinct`, window functions, `orderBy`, `repartition()` | too few / too large partitions, skewed partition, big sort/aggregation/join, exploding joins (`explode`, many-to-many) |
# MAGIC | **Fixes** | ✅ **AQE skew join** (on by default) · **broadcast** the small side · **salting** · filter or handle the hot key separately · pre-aggregate | ✅ **broadcast** small tables · **filter and select early** (less data to move) · avoid unnecessary `repartition`/`distinct`/`orderBy` · aggregate before joining · **liquid clustering** on join/filter keys (12-P3) | ✅ more / smaller partitions (`spark.sql.shuffle.partitions`, AQE) · fix the **skew** first · **memory-optimized** workers or bigger nodes · reduce data per row (select fewer columns) |
# MAGIC
# MAGIC ## 4 · Adaptive Query Execution (AQE) — on by default

# COMMAND ----------

# DBTITLE 1,Slide · AQE
show("""
<div class="kicker">Slide 4 · AQE re-plans at every stage boundary using the REAL sizes of the shuffle files</div>
<div class="grid">
 <div class="card green"><h3>🧩 Coalesce partitions</h3>merges many small shuffle partitions into fewer, right-sized ones — you no longer tune <code>spark.sql.shuffle.partitions</code> by hand (serverless default: <code>auto</code>)</div>
 <div class="card"><h3>📡 Switch join strategy</h3>a side that turns out small at runtime is <b>broadcast</b> (sort-merge → broadcast hash join)</div>
 <div class="card red"><h3>🐘 Skew join</h3>splits a skewed partition into several tasks (<code>spark.sql.adaptive.skewJoin.enabled</code>; a partition is skewed when it is &gt; 5× the median and &gt; 256 MB). Works for <b>joins</b> — not for a skewed <code>groupBy</code>.</div>
</div>
<table class="tbl">
<tr><th>Setting</th><th>Default</th><th>Note</th></tr>
<tr><td><code>spark.sql.adaptive.enabled</code></td><td>true</td><td>AQE on</td></tr>
<tr><td><code>spark.sql.autoBroadcastJoinThreshold</code></td><td>10 MB</td><td>static broadcast decision at planning time; <code>-1</code> disables</td></tr>
<tr><td><code>spark.sql.shuffle.partitions</code></td><td>200 (classic) · <code>auto</code> (serverless)</td><td>number of shuffle partitions; AQE coalesces them</td></tr>
<tr><td><code>spark.databricks.adaptive.autoBroadcastJoinThreshold</code></td><td>30 MB</td><td>runtime (AQE) broadcast threshold on Databricks</td></tr>
</table>
""" + callout("info", "On <b>serverless</b> only a handful of Spark configs can be set (e.g. <code>spark.sql.shuffle.partitions</code>, <code>spark.sql.session.timeZone</code>, "
              "<code>spark.sql.ansi.enabled</code>) — memory, executor and AQE settings are managed for you. Use <b>hints</b> and better code/layout instead."))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 · Fixing skew: broadcast or salt
# MAGIC
# MAGIC **Broadcast** (best when one side is small): every task gets a copy of the small table, so the big side is **not shuffled**
# MAGIC by the skewed key at all.
# MAGIC
# MAGIC ```python
# MAGIC from pyspark.sql import functions as F
# MAGIC events.join(F.broadcast(customers), "customer_id")          # DataFrame API
# MAGIC # SQL:  SELECT /*+ BROADCAST(c) */ ... FROM perf_events e JOIN perf_customers c USING (customer_id)
# MAGIC ```
# MAGIC
# MAGIC **Salting** (both sides big, or a skewed **aggregation**): add a random-ish **salt** to the hot key so its rows spread over
# MAGIC N tasks, aggregate per (key, salt), then aggregate again per key.
# MAGIC
# MAGIC ```python
# MAGIC N = 16
# MAGIC salted = events.withColumn("salt", F.pmod(F.col("event_id"), F.lit(N)))            # 0..15
# MAGIC step1  = salted.groupBy("customer_id", "salt").agg(F.sum("amount").alias("partial"))  # hot key spread over 16 tasks
# MAGIC result = step1.groupBy("customer_id").agg(F.sum("partial").alias("revenue"))          # tiny second aggregation
# MAGIC
# MAGIC # salted JOIN: replicate the other side N times
# MAGIC cust_x = customers.crossJoin(spark.range(N).withColumnRenamed("id", "salt"))
# MAGIC joined = salted.join(cust_x, ["customer_id", "salt"])
# MAGIC ```
# MAGIC
# MAGIC | Technique | When | Cost |
# MAGIC |---|---|---|
# MAGIC | **AQE skew join** | skewed **join**, nothing to do | automatic |
# MAGIC | **Broadcast** | one side small (≲ hundreds of MB) | memory on driver + every executor — a too-big broadcast causes **OOM** (12-P4) |
# MAGIC | **Salting** | skewed **groupBy**, or two big tables | extra code, the other side is replicated N times |
# MAGIC | **Isolate the hot key** | one known key (`'unknown'`, NULL, a bot) | process it separately, `UNION` the results; or filter it out if it's junk |
# MAGIC
# MAGIC ## 6 · Shuffle & partition sizing in practice
# MAGIC * A shuffle is needed for `join` (non-broadcast), `groupBy`, `distinct`, window functions, `orderBy`, `repartition(n, col)`.
# MAGIC   **Narrow** transformations (`select`, `filter`, `withColumn`, `union`) don't shuffle.
# MAGIC * **Filter and project early**: fewer rows and columns into the shuffle = less to move and less to spill. Catalyst pushes
# MAGIC   filters down automatically, but not through every UDF or cache.
# MAGIC * Target partitions of roughly **100–200 MB**: too few → spill and stragglers, too many → scheduling overhead and small
# MAGIC   output files. `repartition(n)` = full shuffle (can increase), `coalesce(n)` = no shuffle (only decreases).
# MAGIC * `EXPLAIN` / `df.explain()` shows the plan: `Exchange hashpartitioning(...)` = shuffle, `BroadcastExchange` +
# MAGIC   `BroadcastHashJoin` = broadcast join, `SortMergeJoin` / `ShuffledHashJoin` = both sides shuffled. On serverless,
# MAGIC   **Photon** operators appear (e.g. `PhotonShuffleExchangeSink`), and Photon prefers shuffled hash joins over sort-merge joins.
# MAGIC
# MAGIC ## 7 · Other signals on the Spark UI
# MAGIC
# MAGIC | Signal | Where | Likely cause → fix |
# MAGIC |---|---|---|
# MAGIC | Thousands of **tiny tasks**, each reading KBs | stage input size per task | **small files** → `OPTIMIZE`, auto compaction, predictive optimization (12-P3) |
# MAGIC | Stage reads far more data than the result needs | stage *Input*; query profile *files/bytes pruned* | no data skipping → filter on clustering/partition columns, liquid clustering |
# MAGIC | High **GC time** (> ~10 % of task time) | Summary Metrics, Executors tab | memory pressure → fewer columns, more partitions, bigger memory |
# MAGIC | **Gaps** in the jobs timeline, executors idle | Jobs tab event timeline | driver-side work (Python loops, `collect`, pandas, planning many small files) |
# MAGIC | **Executor lost** / failed tasks retried | Executors tab, stage *Failed tasks* | OOM on an executor, spot instance reclaimed (12-P4) |
# MAGIC
# MAGIC ## 8 · Caching: two different things

# COMMAND ----------

# DBTITLE 1,Slide · Disk cache vs Spark cache
show("""
<div class="kicker">Slide 8 · “Cache” on Databricks can mean the automatic DISK cache or the manual SPARK cache</div>
<div class="grid two">
 <div class="card green"><h3>💽 Disk cache (Databricks)</h3><ul>
  <li>copies of remote <b>Parquet/Delta files</b> on the workers' local SSDs</li>
  <li><b>automatic</b> on first read (enabled by default on cache-accelerated / storage-optimized instances; <code>spark.databricks.io.cache.enabled</code>)</li>
  <li>always consistent: changed files are detected, LRU eviction</li>
  <li>serverless &amp; SQL warehouses: managed for you (<code>CACHE SELECT</code> is ignored there)</li></ul></div>
 <div class="card orange"><h3>🧠 Spark cache</h3><ul>
  <li>any <b>DataFrame</b>: <code>df.cache()</code> = <code>persist(StorageLevel.MEMORY_AND_DISK)</code> for DataFrames · <code>CACHE TABLE t</code></li>
  <li><b>lazy</b>: filled by the next action · reused only by queries on the <b>same</b> DataFrame</li>
  <li>you must <code>unpersist()</code> / <code>UNCACHE TABLE</code>; can go <b>stale</b>; uses executor memory (→ spill/OOM)</li>
  <li>❌ <b>not supported on serverless</b> — use it on classic compute for a DataFrame reused many times</li></ul></div>
</div>
<table class="tbl">
<tr><th>Storage level</th><th>Where</th><th>Note</th></tr>
<tr><td><code>MEMORY_ONLY</code></td><td>memory (deserialized)</td><td>partitions that don't fit are <b>recomputed</b>; RDD default for <code>cache()</code></td></tr>
<tr><td><code>MEMORY_AND_DISK</code></td><td>memory, overflow to disk</td><td><b>DataFrame default</b> for <code>cache()</code></td></tr>
<tr><td><code>DISK_ONLY</code></td><td>local disk</td><td>slower, frees memory</td></tr>
<tr><td><code>…_2</code> (e.g. <code>MEMORY_AND_DISK_2</code>)</td><td>replicated on 2 nodes</td><td>survives an executor loss without recompute</td></tr>
</table>
""" + callout("trap", "Caching is not a speed button: caching a DataFrame used <b>once</b> only adds work, and a big cache steals memory from joins and aggregations "
              "(→ spill). Cache what you reuse several times, check the <b>Storage</b> tab, and <code>unpersist()</code> when done."))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9 · Serverless: the query profile instead of the Spark UI
# MAGIC
# MAGIC | Look at | Tells you |
# MAGIC |---|---|
# MAGIC | **Wall-clock** vs **total task time** | high task time / wall time = lots of parallel work; a long wall time with little task time = waiting (compute, I/O, driver) |
# MAGIC | **Top operators** tab | the most expensive operator — scan, join, aggregate, shuffle, sort |
# MAGIC | Operator details: **rows**, **peak memory**, **spill**, **time** | exploding joins (rows out ≫ rows in), spill, the slow step |
# MAGIC | **Files / bytes pruned** on scans | data skipping working or not (layout — 12-P3) |
# MAGIC | **Photon** share | operators not supported by Photon (e.g. some Python UDFs) fall back to slower execution |
# MAGIC
# MAGIC ## 🕰️ Recognize on the exam
# MAGIC * Old answers say *“increase `spark.sql.shuffle.partitions` from 200”* — today **AQE** coalesces automatically; it's still
# MAGIC   the knob to **raise** when tasks spill.
# MAGIC * `/*+ SKEW('table', 'column') */` hints are a legacy Databricks feature — AQE skew join replaced them.
# MAGIC * *Delta cache* / *IO cache* = old names of the **disk cache**.
# MAGIC
# MAGIC ## ✅ Key takeaways
# MAGIC 1. Slow query → **longest stage** → **Summary Metrics**: Max ≫ 75th percentile = **skew**; Spill (Memory/Disk) > 0 =
# MAGIC    **spill**; big Shuffle Read/Write + Exchange = **shuffle**.
# MAGIC 2. Skew: AQE skew **join**, **broadcast** the small side, **salting** (also for `groupBy`), isolate the hot key.
# MAGIC 3. Shuffle: broadcast small tables, **filter/select early**, avoid needless repartition/distinct/sort, cluster the data.
# MAGIC 4. Spill: more/smaller partitions, fix skew, more memory per core (memory-optimized nodes), fewer columns.
# MAGIC 5. **Disk cache** = automatic file cache on local SSD; **Spark cache** = manual, lazy, needs `unpersist`, not on serverless.
# MAGIC 6. Serverless has **no Spark UI** → **query profile** (top operators, rows, spill, pruning).
