# Databricks notebook source
# MAGIC %md
# MAGIC # ⚙️ 02-P1 · Spark Architecture
# MAGIC **Section 02 — Spark Foundations** · supports **D3 Transformation (22 %)** and **D6 Troubleshooting & Optimization (10 %)**
# MAGIC
# MAGIC | After this presentation you can… |
# MAGIC |---|
# MAGIC | Explain why Spark distributes work and what the **driver**, **executors** and **cluster manager** do |
# MAGIC | Break an application into **jobs → stages → tasks** and say where stage boundaries come from |
# MAGIC | Relate **partitions**, **cores/slots** and **parallelism** |
# MAGIC | Explain how **Spark Connect** (serverless, Standard access mode) changes where your code runs |
# MAGIC | Recognise driver-side mistakes such as `collect()` on big data |
# MAGIC
# MAGIC > ▶️ **Run all** to render the diagrams (a few seconds, any compute).

# COMMAND ----------

# MAGIC %run ../../Includes/_style

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · Why a distributed engine?
# MAGIC
# MAGIC One machine hits hard limits: RAM, disk throughput and CPU cores. **Apache Spark** splits data into **partitions**,
# MAGIC processes them **in parallel on many machines**, and combines the results. The same code scales from megabytes to petabytes —
# MAGIC and Spark is the engine behind every Databricks notebook, job and pipeline (with **Photon** accelerating it).

# COMMAND ----------

# DBTITLE 1,Slide · Cluster anatomy
show("""
<div class="kicker">Slide 1 · Anatomy of a Spark cluster</div>
<h2>One driver, many executors</h2>
<div class="grid two">
  <div class="card purple"><h3>🧠 Driver <span class="pill purple">the brain</span></h3>
    <ul><li>Runs your program's <code>SparkSession</code></li>
    <li>Turns code into a <b>logical plan → optimized physical plan</b></li>
    <li>Splits work into <b>jobs, stages and tasks</b> and schedules tasks on executors</li>
    <li>Collects small results (<code>show()</code>, <code>collect()</code>, <code>count()</code>)</li></ul></div>
  <div class="card green"><h3>💪 Executors <span class="pill green">the muscles</span></h3>
    <ul><li>Processes on the <b>worker</b> nodes</li>
    <li>Each executor has <b>cores</b> = <b>task slots</b> (1 task per core at a time)</li>
    <li>Read data, run tasks on partitions, <b>shuffle</b> data to each other</li>
    <li>Hold memory for execution, shuffle and caching</li></ul></div>
</div>
<div class="flow">
  <div class="step"><b>🧑‍💻 Your notebook</b>submits code</div><div class="arrow">➜</div>
  <div class="step"><b>🧠 Driver</b>plans &amp; schedules</div><div class="arrow">⇄</div>
  <div class="step"><b>🗂️ Cluster manager</b>allocates VMs / resources</div><div class="arrow">➜</div>
  <div class="step"><b>💪 Executor 1 … N</b>run tasks in parallel</div><div class="arrow">⇄</div>
  <div class="step"><b>☁️ Storage</b>Delta tables, volumes</div>
</div>
""" + callout("trap", "Data is processed on the <b>executors</b>. The driver only coordinates — pulling a large dataset to the driver "
              "(<code>collect()</code>, <code>toPandas()</code>) can crash it with an out-of-memory error.")
    + callout("info", "<b>Single-node compute</b> = the driver does everything (no workers): fine for learning and small data."))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · Spark Connect — why your notebook is a "thin client" on serverless
# MAGIC
# MAGIC On **serverless** and on classic compute with **Standard** access mode, notebooks talk to Spark through **Spark Connect**:
# MAGIC your Python process builds an *unresolved plan* and sends it to the Spark server, which analyzes, optimizes and executes it.
# MAGIC
# MAGIC | Consequence | What you notice |
# MAGIC |---|---|
# MAGIC | Only the **DataFrame / SQL API** is available | `spark.sparkContext` and RDD APIs (`df.rdd`) are **not** supported |
# MAGIC | Analysis can be **deferred** until an action or schema access | Some errors (e.g. a misspelled column) appear when you *use* the DataFrame, not when you define it |
# MAGIC | Isolation between users | Many users share the compute safely (Unity Catalog enforced) |
# MAGIC
# MAGIC > 🎯 On serverless there is **no Spark UI** — use `explain()` and the **query profile** ("See performance" under a cell) instead.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · From an action to tasks: job → stage → task

# COMMAND ----------

# DBTITLE 1,Slide · Execution hierarchy
show("""
<div class="kicker">Slide 3 · Execution hierarchy</div>
<h2>What happens when you call an action</h2>
<div class="layer" style="background:#0b2a4a">📦 <b>Application</b> <small>— your SparkSession (a notebook's session, or a job run)</small></div>
<div style="margin-left:22px"><div class="layer" style="background:#7048e8">⚡ <b>Job</b> <small>— created by each <b>action</b> (<code>count()</code>, <code>show()</code>, <code>write</code>, <code>collect()</code>…)</small></div>
<div style="margin-left:22px"><div class="layer" style="background:#1c7ed6">🧱 <b>Stage</b> <small>— a set of tasks that run <b>without moving data</b>; a new stage starts at every <b>shuffle</b> (wide transformation)</small></div>
<div style="margin-left:22px"><div class="layer" style="background:#2f9e44">🔹 <b>Task</b> <small>— the smallest unit of work: <b>one task per partition</b>, runs on one core of one executor</small></div></div></div></div>
<h3 style="margin-top:14px">Example: <code>orders.filter(...).groupBy("country").count().show()</code></h3>
<div class="flow">
  <div class="step"><b>Stage 1</b>read files → filter → partial count<br><span class="muted">8 partitions → 8 tasks</span></div>
  <div class="arrow">🔀<br><small>shuffle</small></div>
  <div class="step"><b>Stage 2</b>final count per country → show<br><span class="muted">shuffle partitions → N tasks</span></div>
</div>
""" + callout("exam", "<b>1 action = at least 1 job</b>. <b>Stage boundaries = shuffles.</b> <b>Tasks per stage = partitions.</b>"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · Partitions & parallelism
# MAGIC
# MAGIC A **partition** is a chunk of rows that one task processes. How many partitions you have decides how much parallelism you get.
# MAGIC
# MAGIC | Where partitions come from | Default / setting |
# MAGIC |---|---|
# MAGIC | Reading files | Split into ~**128 MB** chunks (`spark.sql.files.maxPartitionBytes`) |
# MAGIC | After a shuffle (`groupBy`, `join`, `orderBy`, `distinct`…) | `spark.sql.shuffle.partitions` — **200** in Apache Spark / classic compute, **`auto`** on serverless (auto-optimized shuffle picks the number). **AQE** coalesces small ones automatically |
# MAGIC | You explicitly | `df.repartition(n)` (full shuffle) or `df.coalesce(n)` (merge, no full shuffle) |

# COMMAND ----------

# DBTITLE 1,Slide · Slots and waves
show("""
<div class="kicker">Slide 4 · Slots and waves</div>
<h2>Cores = slots. Partitions ÷ slots = waves</h2>
<div class="grid">
  <div class="card"><h3>🖥️ Cluster</h3>4 workers × 4 cores<br>= <b>16 task slots</b></div>
  <div class="card orange"><h3>🧩 Stage</h3>64 partitions<br>= <b>64 tasks</b></div>
  <div class="card green"><h3>🌊 Result</h3>64 ÷ 16 = <b>4 waves</b> of tasks</div>
</div>
<table class="tbl">
<tr><th>Too few partitions</th><th>Too many tiny partitions</th><th>Skewed partitions</th></tr>
<tr><td>Idle cores, large tasks that may <b>spill</b> to disk or run out of memory</td>
<td>Scheduling overhead; writing many <b>small files</b></td>
<td>One huge partition → one <b>straggler</b> task keeps the whole stage waiting</td></tr>
</table>
""" + callout("tip", "Rule of thumb: partitions ≈ 2–4 × total cores, each ~100–200 MB. On Databricks, <b>AQE</b> and "
              "<b>auto-optimized shuffles</b> handle most of this for you."))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 · Where things fail — driver or executor?
# MAGIC
# MAGIC | Symptom | Likely cause | Fix |
# MAGIC |---|---|---|
# MAGIC | Driver out of memory | `collect()` / `toPandas()` on large data, huge broadcast | Keep results distributed, write to a table, `limit()` before collecting |
# MAGIC | Executor out of memory / heavy spill | Very large partitions, skew, wide aggregations | More/smaller partitions, fix skew, memory-optimized instances |
# MAGIC | One task runs much longer than others | **Data skew** | AQE skew-join handling, salting, filter hot keys |
# MAGIC | Job slow before it even starts (classic) | Cluster start-up / scaling | Pools, serverless |

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Key takeaways
# MAGIC 1. **Driver** plans & schedules; **executors** do the work in parallel on **partitions**; the **cluster manager** allocates resources.
# MAGIC 2. **Action → job → stages (split at shuffles) → tasks (one per partition).**
# MAGIC 3. **Cores = task slots**; partitions decide parallelism. Shuffle partitions default to 200 (`auto` on serverless); AQE coalesces them.
# MAGIC 4. Serverless & Standard mode use **Spark Connect**: DataFrame/SQL API only, no `sparkContext`/RDDs, no Spark UI on serverless.
# MAGIC 5. Keep big data **off the driver**.
# MAGIC
# MAGIC ➡️ Next: **02-P2 · How Spark Executes Your Code**
