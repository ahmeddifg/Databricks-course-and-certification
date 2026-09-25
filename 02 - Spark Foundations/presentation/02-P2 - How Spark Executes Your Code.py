# Databricks notebook source
# MAGIC %md
# MAGIC # 🧠 02-P2 · How Spark Executes Your Code
# MAGIC **Section 02 — Spark Foundations** · supports **D3** and **D6 (Spark UI bottlenecks, Spark tuning)**
# MAGIC
# MAGIC | After this presentation you can… |
# MAGIC |---|
# MAGIC | Explain **lazy evaluation** and tell **transformations** from **actions** |
# MAGIC | Classify transformations as **narrow** or **wide** and explain why **shuffles** are expensive |
# MAGIC | Describe what the **Catalyst optimizer**, **AQE** and **Photon** do for you |
# MAGIC | **Read an execution plan** (`explain()` / `EXPLAIN`) and spot shuffles, broadcast joins and pushed filters |
# MAGIC | Apply the core performance rules used in exam questions |
# MAGIC
# MAGIC > ▶️ **Run all** to render the diagrams.

# COMMAND ----------

# MAGIC %run ../../Includes/_style

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · Lazy evaluation
# MAGIC
# MAGIC Spark does **not** run a transformation when you write it. It records it in a **plan**. Only an **action** makes Spark
# MAGIC optimize the whole plan and execute it.

# COMMAND ----------

# DBTITLE 1,Slide · Transformations vs actions
show("""
<div class="kicker">Slide 1 · Lazy evaluation</div>
<h2>Transformations build the plan — actions run it</h2>
<div class="grid two">
  <div class="card"><h3>💤 Transformations (lazy)</h3>return a <b>new DataFrame</b>, nothing runs
    <p><code>select</code> · <code>withColumn</code> · <code>filter</code>/<code>where</code> · <code>groupBy().agg()</code> · <code>join</code>
    · <code>union</code> · <code>orderBy</code> · <code>distinct</code> · <code>dropDuplicates</code> · <code>repartition</code> · <code>limit</code></p></div>
  <div class="card orange"><h3>⚡ Actions (eager)</h3>trigger a <b>job</b> and return a result or write data
    <p><code>show()</code> · <code>display()</code> · <code>count()</code> · <code>collect()</code> · <code>take(n)</code> · <code>first()</code>
    · <code>toPandas()</code> · <code>write…save()/saveAsTable()</code> · <code>foreach()</code></p></div>
</div>
<div class="flow">
  <div class="step"><b>read</b>plan node</div><div class="arrow">➜</div>
  <div class="step"><b>filter</b>plan node</div><div class="arrow">➜</div>
  <div class="step"><b>groupBy/agg</b>plan node</div><div class="arrow">➜</div>
  <div class="step" style="background:#fff0e6"><b>show()</b>⚡ optimize + execute everything</div>
</div>
""" + callout("exam", "Why lazy? Spark sees the <b>whole</b> pipeline before running it, so it can drop unused columns, push filters "
              "down to the files, combine steps into one stage and pick the best join strategy.")
    + callout("trap", "DataFrames are <b>immutable</b>: <code>df.withColumn(...)</code> returns a new DataFrame. If you don't assign it "
              "(<code>df = df.withColumn(...)</code>), the change is lost."))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · Narrow vs wide transformations

# COMMAND ----------

# DBTITLE 1,Slide · Narrow vs wide
show("""
<div class="kicker">Slide 2 · Data movement</div>
<h2>Narrow = stay in place · Wide = shuffle across the cluster</h2>
<div class="grid two">
  <div class="card green"><h3>➡️ Narrow</h3>Each output partition depends on <b>one</b> input partition — no data moves.
    <p><code>select</code>, <code>filter</code>, <code>withColumn</code>, <code>drop</code>, <code>union</code>, <code>coalesce</code>, map-like functions</p>
    <p class="muted">Many narrow steps are <b>pipelined</b> into a single stage.</p></div>
  <div class="card red"><h3>🔀 Wide (shuffle)</h3>Output partitions need rows from <b>many</b> input partitions — data is redistributed by key.
    <p><code>groupBy</code>/<code>agg</code>, <code>join</code> (non-broadcast), <code>distinct</code>, <code>dropDuplicates</code>, <code>orderBy</code>/<code>sort</code>, <code>repartition</code>, window functions</p>
    <p class="muted">Each shuffle = a <b>new stage</b>: write to disk → network transfer → read.</p></div>
</div>
""" + callout("tip", "A shuffle costs disk I/O + network + serialization. Most tuning is about <b>moving less data</b>: filter early, "
              "select only needed columns, broadcast small tables, avoid unnecessary <code>orderBy</code>/<code>distinct</code>."))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · The Catalyst optimizer — from your code to a physical plan
# MAGIC
# MAGIC Python DataFrame code and SQL are compiled into **the same plans**, so they perform the same (no "SQL is faster" myth).

# COMMAND ----------

# DBTITLE 1,Slide · Catalyst pipeline
show("""
<div class="kicker">Slide 3 · Catalyst</div>
<h2>Four planning phases</h2>
<div class="flow">
  <div class="step"><b>1 · Unresolved logical plan</b>parsed from SQL / DataFrame code</div><div class="arrow">➜</div>
  <div class="step"><b>2 · Analyzed plan</b>tables &amp; columns resolved via the <b>catalog</b> (errors like "column not found" here)</div><div class="arrow">➜</div>
  <div class="step"><b>3 · Optimized logical plan</b>rule-based rewrites</div><div class="arrow">➜</div>
  <div class="step"><b>4 · Physical plan</b>join strategies, operators, code generation (or Photon)</div>
</div>
<table class="tbl">
<tr><th>Optimization</th><th>What it does</th></tr>
<tr><td><b>Predicate pushdown</b></td><td>Moves filters as close to the source as possible → read fewer files/row groups (data skipping)</td></tr>
<tr><td><b>Column pruning / projection pushdown</b></td><td>Reads only the columns the query uses (columnar Parquet/Delta)</td></tr>
<tr><td><b>Constant folding</b></td><td><code>price * (1 + 0.15)</code> → <code>price * 1.15</code> computed once</td></tr>
<tr><td><b>Join selection</b></td><td>Broadcast hash join when one side is small, otherwise shuffle (sort-merge / shuffled hash) join</td></tr>
</table>
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · Adaptive Query Execution (AQE) and Photon
# MAGIC
# MAGIC | | What | Why it matters |
# MAGIC |---|---|---|
# MAGIC | **AQE** (on by default) | Re-optimizes the plan **at runtime**, using real statistics collected at each shuffle | • **Coalesces** many small shuffle partitions<br>• **Switches** a sort-merge join to a **broadcast** join when one side turns out small<br>• **Splits skewed** partitions in joins |
# MAGIC | **Photon** | Databricks' **vectorized C++ engine** that runs Spark SQL / DataFrame operators | Much faster scans, joins, aggregations and writes. **Always on for serverless**, optional on classic. Plan operators appear as `Photon…`. Does **not** speed up Python UDFs. |
# MAGIC
# MAGIC > 🎯 In plans run with AQE you'll see `AdaptiveSparkPlan isFinalPlan=false` before execution; the **final** plan appears in the
# MAGIC > query profile / Spark UI after the query ran.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 · Reading an execution plan
# MAGIC
# MAGIC Get a plan with `df.explain()`, `df.explain("formatted")`, or in SQL `EXPLAIN [EXTENDED | FORMATTED] SELECT …`.
# MAGIC **Read physical plans bottom-up** (from the scan to the result).

# COMMAND ----------

# DBTITLE 1,Slide · Plan anatomy
show("""
<div class="kicker">Slide 5 · What to look for</div>
<h2>Plan keywords → meaning</h2>
<table class="tbl">
<tr><th>You see…</th><th>It means…</th></tr>
<tr><td><code>Scan parquet / FileScan / PhotonScan</code> + <code>ReadSchema</code></td><td>Files read and which columns (column pruning)</td></tr>
<tr><td><code>PushedFilters</code> / <code>DataFilters</code> / <code>PartitionFilters</code></td><td>Filters applied at the source → data skipping</td></tr>
<tr><td><code>Filter</code>, <code>Project</code></td><td>Narrow operations (row filter, column selection/expressions)</td></tr>
<tr><td><code>Exchange hashpartitioning(col, n)</code> (<code>PhotonShuffleExchange…</code>)</td><td>🔀 <b>A shuffle</b> by key into n partitions → stage boundary</td></tr>
<tr><td><code>Exchange rangepartitioning</code></td><td>🔀 Shuffle for a global <b>sort</b> (<code>orderBy</code>)</td></tr>
<tr><td><code>HashAggregate</code> (partial + final)</td><td>Aggregation done in two steps around a shuffle</td></tr>
<tr><td><code>BroadcastExchange</code> + <code>BroadcastHashJoin</code></td><td>Small side copied to every executor — <b>no shuffle of the big side</b></td></tr>
<tr><td><code>SortMergeJoin</code> / <code>ShuffledHashJoin</code></td><td>Both sides shuffled by the join key</td></tr>
</table>
""" + callout("exam", "\"Which operation causes the <code>Exchange</code> in this plan?\" → the wide transformation (groupBy, join, orderBy, distinct, repartition)."))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6 · Joins: broadcast vs shuffle
# MAGIC
# MAGIC | | 📡 Broadcast hash join | 🔀 Shuffle join (sort-merge / shuffled hash) |
# MAGIC |---|---|---|
# MAGIC | When | One side is **small** (default auto-threshold 10 MB, configurable `spark.sql.autoBroadcastJoinThreshold`) | Both sides large |
# MAGIC | How | Small table sent to **every executor**; big table stays where it is | **Both** tables shuffled by the join key |
# MAGIC | Force it | `from pyspark.sql.functions import broadcast` → `big.join(broadcast(small), "key")` or SQL hint `/*+ BROADCAST(small) */` | hint `/*+ MERGE(t) */` or `/*+ SHUFFLE_HASH(t) */` |
# MAGIC | Risk | Broadcasting something big → **driver/executor OOM** | Expensive network + disk; sensitive to **skew** |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7 · 🎯 Performance rules the exam loves
# MAGIC
# MAGIC | # | Rule | Why |
# MAGIC |---|---|---|
# MAGIC | 1 | **Filter early, select only needed columns** | Less data read, shuffled and written |
# MAGIC | 2 | Prefer **built-in functions** over Python UDFs | Stay inside the optimized engine (and Photon); UDFs serialize rows to Python |
# MAGIC | 3 | **Broadcast** small dimension tables | Removes the shuffle of the big side |
# MAGIC | 4 | Watch for **skew** (one straggler task) | AQE skew handling, salting, isolate hot keys |
# MAGIC | 5 | `coalesce(n)` to **reduce** partitions, `repartition(n)` to **increase** or rebalance | coalesce avoids a full shuffle |
# MAGIC | 6 | Avoid `collect()`/`toPandas()` on big data | Driver OOM |
# MAGIC | 7 | Cache only data **reused** several times (classic compute) — then `unpersist()` | Caching is lazy and costs memory; not available on serverless |
# MAGIC | 8 | **Read the plan / query profile** before tuning | Measure, don't guess |

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Key takeaways
# MAGIC 1. **Transformations are lazy**, **actions** trigger jobs; DataFrames are **immutable**.
# MAGIC 2. **Narrow** = no data movement; **wide** = **shuffle** = new stage = expensive.
# MAGIC 3. **Catalyst**: analyzed → optimized (pushdown, pruning) → physical plan. SQL and DataFrame code get the **same** plan.
# MAGIC 4. **AQE** re-optimizes at runtime (coalesce partitions, switch to broadcast, split skew). **Photon** = vectorized C++ engine.
# MAGIC 5. In plans: **Exchange = shuffle**, **BroadcastHashJoin = no big-side shuffle**, **PushedFilters = data skipping**.
# MAGIC
# MAGIC ➡️ Next: **02-P3 · DataFrames & Spark SQL Essentials**
