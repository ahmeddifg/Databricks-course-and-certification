# Databricks notebook source
# MAGIC %md
# MAGIC # 🖥️ 01-P3 · Compute & Cost
# MAGIC **Section 01 — Databricks Introduction & Platform** · Exam objective **1.2 Compute services selection & cost models**
# MAGIC
# MAGIC | After this presentation you can… |
# MAGIC |---|
# MAGIC | Name every compute type and what it's for |
# MAGIC | Compare **serverless vs classic** and **all-purpose vs job** compute |
# MAGIC | Configure classic compute: access mode, runtime, Photon, node types, autoscaling, auto-termination, spot, pools |
# MAGIC | Explain how Databricks is billed (DBUs) and pick the most cost-effective option for a scenario |
# MAGIC
# MAGIC > ▶️ **Run all** to render the diagrams. Hands-on practice is in **labs/01-L2**.

# COMMAND ----------

# MAGIC %run ../../Includes/_style

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · The compute landscape

# COMMAND ----------

# DBTITLE 1,Slide · Compute tree
show("""
<div class="kicker">Slide 1 · Compute types</div>
<h2>Two families, many flavours</h2>
<div class="grid two">
 <div class="card green"><h3>⚡ Serverless <span class="pill green">Databricks manages the VMs</span></h3>
   <ul><li><b>Serverless for notebooks</b> — interactive Python & SQL</li>
   <li><b>Serverless for jobs</b> — scheduled / triggered tasks</li>
   <li><b>Serverless for pipelines</b> — Lakeflow Spark Declarative Pipelines</li>
   <li><b>Serverless SQL warehouses</b> — Databricks SQL, BI tools</li></ul></div>
 <div class="card"><h3>🏗️ Classic <span class="pill">VMs in your cloud account</span></h3>
   <ul><li><b>All-purpose compute</b> — interactive development, shared by users, created manually</li>
   <li><b>Job compute</b> — created by the job scheduler for one job run, terminated afterwards</li>
   <li><b>Instance pools</b> — idle, pre-warmed VMs to cut start-up time</li>
   <li><b>Pro / Classic SQL warehouses</b> — SQL compute in your account</li></ul></div>
</div>
""" + callout("exam", "The exam asks you to <b>choose</b>: interactive vs automated, serverless vs classic, SQL warehouse vs Spark compute — "
              "usually with a constraint like <i>lowest cost</i>, <i>fastest start</i> or <i>least management</i>."))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · All-purpose vs job compute (classic)
# MAGIC
# MAGIC | | 🧑‍💻 All-purpose compute | 🤖 Job compute |
# MAGIC |---|---|---|
# MAGIC | Purpose | **Interactive** notebooks, exploration, development | **Automated** production jobs |
# MAGIC | Created by | You — UI, CLI, REST API | The **job scheduler**, when a job run starts |
# MAGIC | Shared? | Yes, many users & notebooks can attach | No — dedicated to one job run |
# MAGIC | Terminated | Manually or by **auto-termination** after inactivity | **Automatically when the job finishes** |
# MAGIC | Restart | Can be restarted | Cannot be restarted (a new one is created per run) |
# MAGIC | DBU price | 💲💲 **Higher** | 💲 **Lower** |
# MAGIC
# MAGIC > ⚠️ **Exam trap:** running scheduled production jobs on an **all-purpose** cluster works, but costs more and mixes workloads.
# MAGIC > The recommended choice is **job compute** or **serverless jobs**.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · Serverless vs classic

# COMMAND ----------

# DBTITLE 1,Slide · Serverless vs classic
show("""
<div class="kicker">Slide 3 · Trade-offs</div>
<h2>Serverless vs classic compute</h2>
<table class="tbl">
<tr><th></th><th>⚡ Serverless</th><th>🏗️ Classic</th></tr>
<tr><td>Configuration</td><td>None — fully managed</td><td>You choose everything (runtime, VM types, workers, …)</td></tr>
<tr><td>Start-up</td><td><b>Seconds</b></td><td>Several minutes (less with pools)</td></tr>
<tr><td>Runtime version</td><td>Always the latest, upgraded automatically</td><td>You pick & upgrade (prefer <b>LTS</b> for production)</td></tr>
<tr><td>Photon</td><td>Always on</td><td>Optional</td></tr>
<tr><td>Scaling</td><td>Automatic</td><td>Autoscaling if you configure min/max workers</td></tr>
<tr><td>Languages</td><td>Python &amp; SQL</td><td>Python, SQL, Scala, R, Java</td></tr>
<tr><td>Billing</td><td>One DBU price includes the VMs</td><td>DBUs to Databricks <b>+</b> VM cost to your cloud provider</td></tr>
<tr><td>Where it runs</td><td>Serverless compute plane (Databricks' account)</td><td>Your cloud account / VPC</td></tr>
</table>
""" + callout("trap", "Serverless is <b>not</b> always cheaper per hour — it's cheaper in <b>idle time and admin effort</b> (no warm clusters, no tuning)."))

# COMMAND ----------

# MAGIC %md
# MAGIC ### ⚠️ Serverless limitations you should know (they explain several exam answers)
# MAGIC
# MAGIC | Not supported on serverless | Use instead |
# MAGIC |---|---|
# MAGIC | **Scala / R** notebooks, RDD APIs, `sparkContext` | Python/SQL DataFrame APIs (Spark Connect) |
# MAGIC | `df.cache()`, `persist()`, `CACHE TABLE` | Let the engine cache automatically; write intermediate results to tables |
# MAGIC | **Global temporary views** | Temporary views or real views/tables |
# MAGIC | Most `spark.conf.set(...)` settings (only a few, e.g. `spark.sql.shuffle.partitions`, `spark.sql.session.timeZone`, `spark.sql.ansi.enabled`) | Defaults are auto-tuned |
# MAGIC | Streaming triggers `processingTime` / `continuous` | `trigger(availableNow=True)` |
# MAGIC | Spark UI | **Query profile** |
# MAGIC | Init scripts, instance pools, compute policies, JAR libraries in notebooks | Environment panel / notebook-scoped `%pip` |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · SQL warehouses — compute for SQL & BI
# MAGIC
# MAGIC | Type | Where it runs | Start-up | Notes |
# MAGIC |---|---|---|---|
# MAGIC | **Serverless** ✅ | Databricks' account | Seconds | Recommended: fastest, auto-scales, Photon, intelligent workload management |
# MAGIC | **Pro** | Your cloud account | Minutes | Use when serverless isn't available or you need **custom networking** (e.g. reach databases in your VPC / on-prem) |
# MAGIC | **Classic** | Your cloud account | Minutes | Entry-level, fewer performance features |
# MAGIC
# MAGIC * Sized in **T-shirt sizes** (2X-Small … 4X-Large) = size of *one* cluster.
# MAGIC * **Scaling** = min/max number of clusters to handle **concurrent** queries.
# MAGIC * **Auto stop** after N minutes idle saves money.
# MAGIC * Used by the SQL editor, dashboards, alerts, Genie and external BI tools (Power BI, Tableau) via JDBC/ODBC.
# MAGIC
# MAGIC > 🎯 **Exam focus:** *BI dashboards / analysts running SQL* → **SQL warehouse (serverless)**. *Python ETL notebook* → Spark compute (serverless or classic), not a SQL warehouse.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 · Configuring classic compute

# COMMAND ----------

# DBTITLE 1,Slide · Compute settings
show("""
<div class="kicker">Slide 5 · The "Create compute" form</div>
<h2>Settings and what they mean</h2>
<table class="tbl">
<tr><th>Setting</th><th>Options</th><th>Guidance</th></tr>
<tr><td><b>Policy</b></td><td>Unrestricted, Personal Compute, custom…</td><td>Admins use policies to limit sizes / enforce tags</td></tr>
<tr><td><b>Nodes</b></td><td>Single node · Multi node</td><td>Single node = driver only (small data, learning). Multi node = driver + workers (distributed)</td></tr>
<tr><td><b>Access mode</b></td><td><b>Standard</b> (🕰️ Shared) · <b>Dedicated</b> (🕰️ Single user) · Auto</td><td>Standard: many users, Unity Catalog isolation. Dedicated: one user/group, needed for R, ML runtimes, RDDs</td></tr>
<tr><td><b>Databricks Runtime</b></td><td>Standard · <b>ML</b> · versions (<b>LTS</b> = long-term support, 3 years)</td><td>LTS for production jobs; ML runtime for ML libraries</td></tr>
<tr><td><b>Photon</b></td><td>On / off</td><td>Vectorized C++ engine: faster SQL/DataFrame work, higher DBU rate</td></tr>
<tr><td><b>Worker / driver type</b></td><td>Instance families (general, memory, compute, storage, GPU)</td><td>See next slide</td></tr>
<tr><td><b>Autoscaling</b></td><td>Min / max workers</td><td>Adds workers under load, removes them when idle</td></tr>
<tr><td><b>Auto-termination</b></td><td>Minutes of inactivity</td><td>Always set it on all-purpose compute (e.g. 30–60 min)</td></tr>
<tr><td><b>Spot instances</b></td><td>On-demand · spot (with fallback)</td><td>Much cheaper, can be reclaimed → fine for workers of fault-tolerant jobs</td></tr>
<tr><td><b>Pool</b></td><td>Attach to an instance pool</td><td>Faster start and scale-up using idle VMs</td></tr>
<tr><td><b>Tags</b></td><td>key = value</td><td>Flow into usage logs → cost attribution per team/project</td></tr>
</table>
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6 · Choosing an instance family
# MAGIC
# MAGIC | VM category | Best for |
# MAGIC |---|---|
# MAGIC | **Memory optimized** | Heavy shuffles & spills, caching, ML, Structured Streaming |
# MAGIC | **Compute optimized** | ELT with full scans and no data reuse, `OPTIMIZE` / Z-ordering |
# MAGIC | **Storage optimized** | Using the Databricks **disk cache**, ad hoc interactive analysis |
# MAGIC | **GPU optimized** | Deep learning, very memory-hungry ML |
# MAGIC | **General purpose** | No special requirement; `VACUUM` |
# MAGIC
# MAGIC ## 7 · Pools and spot instances
# MAGIC * **Instance pool** = a set of **idle, ready-to-use VMs**. Compute attached to a pool starts and scales faster.
# MAGIC   Databricks charges **no DBUs for idle pool instances**, but the **cloud provider still bills the VMs**.
# MAGIC * **Spot instances** = spare cloud capacity at a big discount that can be **reclaimed** at any time.
# MAGIC   Use for **workers** of fault-tolerant batch jobs; keep the **driver on-demand**.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8 · How Databricks bills you

# COMMAND ----------

# DBTITLE 1,Slide · Cost model
show("""
<div class="kicker">Slide 8 · Cost model</div>
<h2>DBU = Databricks Unit — processing capacity per hour</h2>
<div class="flow">
  <div class="step"><b>DBUs consumed</b>depend on compute size × hours</div><div class="arrow">×</div>
  <div class="step"><b>$ per DBU</b>depends on compute type, tier, cloud, region</div><div class="arrow">+</div>
  <div class="step"><b>Cloud VM cost</b>classic compute only — billed by AWS/Azure/GCP</div>
</div>
<div class="grid">
  <div class="card green"><h3>💲 Cheaper per DBU</h3>Job compute &lt; all-purpose compute<br>(same VMs, lower rate for automated work)</div>
  <div class="card orange"><h3>⏱️ Pay for idle?</h3>All-purpose clusters left running · warm pools (VM cost) · SQL warehouses without auto stop</div>
  <div class="card purple"><h3>🔎 See your spend</h3>System table <code>system.billing.usage</code> (+ <code>system.billing.list_prices</code>), budgets, usage dashboards</div>
</div>
""" + callout("tip", "Cost levers: <b>auto-termination</b>, <b>autoscaling</b>, <b>job compute / serverless</b> for production, "
              "<b>spot workers</b>, right-sized instances, <b>policies</b> and <b>tags</b> for accountability.")
    + callout("trap", "Exact $/DBU rates vary by cloud, region and pricing tier — the exam tests the <b>relative</b> comparisons, not prices."))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9 · 🎯 Decision guide — which compute?
# MAGIC
# MAGIC | Scenario | Best choice | Why |
# MAGIC |---|---|---|
# MAGIC | Explore data in a notebook, no admin work wanted | **Serverless notebooks** | Instant start, no config |
# MAGIC | Nightly production ETL, lowest management | **Serverless jobs** | Auto-scaling, no clusters to manage |
# MAGIC | Nightly ETL needing Scala/JAR library or custom init script | **Job compute** (classic) | Serverless doesn't support these |
# MAGIC | Analysts & Power BI dashboards | **Serverless SQL warehouse** | Built for concurrent SQL/BI |
# MAGIC | SQL warehouse must reach a database inside your private network | **Pro SQL warehouse** | Runs in your VPC/VNet |
# MAGIC | Team develops in R | **Classic all-purpose, Dedicated access mode** | R isn't supported on serverless or Standard mode |
# MAGIC | Many short jobs, cluster start time is the bottleneck (classic) | **Instance pool** | Pre-warmed VMs |
# MAGIC | Big, fault-tolerant batch job, minimise cost (classic) | **Job compute + spot workers** | Discounted VMs, job survives lost workers |
# MAGIC | Declarative pipeline | **Serverless pipeline** (or classic pipeline compute) | Managed by Lakeflow |

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Key takeaways
# MAGIC 1. **Serverless** (notebooks, jobs, pipelines, SQL warehouses): seconds to start, no config, Python & SQL, latest runtime, Photon always on.
# MAGIC 2. **Classic**: all-purpose (interactive, pricier) vs job compute (automated, cheaper, terminates after the run).
# MAGIC 3. Classic settings: **access mode** (Standard/Dedicated), **LTS runtime**, Photon, node type, **autoscaling**, **auto-termination**, spot, pools, policies, tags.
# MAGIC 4. **SQL warehouses** (serverless / pro / classic) serve SQL & BI; pro when you need custom networking.
# MAGIC 5. Billing = **DBUs × $/DBU (+ VMs for classic)**. Track it in `system.billing.usage`.
# MAGIC
# MAGIC ➡️ Next: practise in **labs/01-L1** and **labs/01-L2**, then take the **01-Q** quiz.
