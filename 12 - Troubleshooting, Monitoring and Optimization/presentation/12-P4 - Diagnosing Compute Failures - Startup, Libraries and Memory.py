# Databricks notebook source
# MAGIC %md
# MAGIC # 🚑 12-P4 · Diagnosing Compute Failures — Startup, Libraries and Memory
# MAGIC **Section 12** · exam objective **6.5** — *diagnose cluster startup failures, library conflicts, and out-of-memory issues*
# MAGIC
# MAGIC | After this presentation you can… |
# MAGIC |---|
# MAGIC | Know **where to look**: compute **event log**, termination reason, **driver logs**, init-script logs, task output |
# MAGIC | Diagnose **startup failures**: cloud capacity / quota, network, init scripts, Spark start-up, policies & permissions |
# MAGIC | Diagnose **library conflicts**: scopes and precedence, version clashes, `%pip` + `restartPython()`, serverless environments |
# MAGIC | Tell **driver OOM** from **executor OOM** from the error message — and fix each |
# MAGIC | Right-size compute: autoscaling, auto-termination, spot with fallback, instance families, serverless |
# MAGIC
# MAGIC > ▶️ **Run all** to render the diagrams. Practice: **labs/12-L4** (troubleshooting clinic: case files + real library demo).

# COMMAND ----------

# MAGIC %run ../../Includes/_style

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · Where to look

# COMMAND ----------

# DBTITLE 1,Slide · Evidence
show("""
<div class="kicker">Slide 1 · Every failure leaves evidence — know which drawer to open</div>
<div class="grid">
 <div class="card"><h3>📜 Compute event log</h3>Compute → cluster → <b>Event log</b>: CREATING, STARTING, RUNNING, RESIZING, <b>INIT_SCRIPTS_STARTED/FINISHED</b>, <b>DRIVER_NOT_RESPONDING</b>, <b>NODES_LOST</b>, TERMINATING + <b>termination reason</b></div>
 <div class="card orange"><h3>🛑 Termination reason</h3>a <b>code</b> (e.g. <code>CLOUD_PROVIDER_LAUNCH_FAILURE</code>, <code>INIT_SCRIPT_FAILURE</code>) and a <b>type</b>: <code>CLIENT_ERROR</code> (your config → fix it), <code>SERVICE_FAULT</code> / <code>CLOUD_FAILURE</code> (provider side → often retry)</div>
 <div class="card green"><h3>🧾 Driver logs</h3>Compute → <b>Driver logs</b>: <code>stdout</code>, <code>stderr</code>, <code>log4j</code> — stack traces, OOM, Python errors. Cluster log delivery copies them to a volume / storage.</div>
 <div class="card purple"><h3>📦 Libraries tab</h3>status per library: Installed · Pending · <b>Failed</b> (with the pip/maven error)</div>
 <div class="card"><h3>🧩 Job task run</h3>the error + stack trace of the task, its compute, the Spark UI / query profile link; the run's <b>termination code</b></div>
 <div class="card teal"><h3>🗃️ System tables</h3><code>system.compute.clusters</code> (config history), <code>system.compute.node_timeline</code> (CPU/memory per node), <code>system.lakeflow.*</code> (termination codes)</div>
</div>
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · Cluster start-up failures
# MAGIC
# MAGIC | Symptom / code | Meaning | Fix |
# MAGIC |---|---|---|
# MAGIC | `CLOUD_PROVIDER_LAUNCH_FAILURE`, `CLOUD_PROVIDER_RESOURCE_STOCKOUT`, *insufficient capacity* | the cloud could not give you those VMs in that region/zone now | retry, choose another instance type / zone, use **pools** or **fleet / flexible** node types, or **serverless** |
# MAGIC | `…QUOTA_EXCEEDED…`, `SUBNET_EXHAUSTED_FAILURE`, `INSTANCE_POOL_MAX_CAPACITY_REACHED` | your **account limits**: vCPU quota, free IPs, pool size | request more quota, smaller cluster, bigger subnet / pool |
# MAGIC | `INIT_SCRIPT_FAILURE`, `GLOBAL_INIT_SCRIPT_FAILURE` | a (cluster or global) **init script** returned an error | read the **init script logs**; check the script path and read permission (scripts from **workspace files or volumes**, not DBFS), the package repo is reachable |
# MAGIC | `BOOTSTRAP_TIMEOUT`, `INSTANCE_UNREACHABLE`, `NETWORK_CONFIGURATION_FAILURE`, *node daemon ping timeout* | VMs started but can't reach the control plane / storage | **network**: firewall, routes, DNS, VPC/VNet endpoints, proxy |
# MAGIC | `SPARK_STARTUP_FAILURE` | the Spark driver didn't come up in time | remove custom Spark configs / init scripts, try another instance type |
# MAGIC | `SPOT_INSTANCE_TERMINATION`, *NODES_LOST* | spot VMs reclaimed by the cloud | on-demand driver, *spot with fallback to on-demand*, retries — or on-demand for SLAs |
# MAGIC | Cluster creation refused: *policy* / *permission* / `INVALID_ARGUMENT` | the config violates a **compute policy**, or you lack permission (*Unrestricted cluster creation*, *CAN USE* on the policy / pool) | pick an allowed policy / value, ask an admin |
# MAGIC | `EOS_SPARK_IMAGE`, `SPARK_IMAGE_NOT_FOUND` | Databricks Runtime version out of support / unavailable | move to a supported (**LTS**) runtime |
# MAGIC | Cluster *Running* but a job task fails with `LIBRARY_INSTALLATION_ERROR` | a cluster/task **library** could not be installed | Libraries tab → error; fix the version / repository (section 3) |
# MAGIC
# MAGIC > 💡 **Serverless** compute removes most of this list: no VMs to request, no init scripts, no quota per cluster —
# MAGIC > Databricks manages capacity. What remains is **your code**, **your libraries** (environment) and **memory**.
# MAGIC
# MAGIC ## 3 · Library conflicts

# COMMAND ----------

# DBTITLE 1,Slide · Library scopes
show("""
<div class="kicker">Slide 3 · Who wins when the same package is installed twice? (highest priority first)</div>
<div class="flow">
 <div class="step"><b>1 · Git folder</b>current directory / repo root modules</div><div class="arrow">➜</div>
 <div class="step" style="border:2px solid #1c7ed6"><b>2 · Notebook-scoped</b><code>%pip install pkg==1.2</code><br>only this notebook session</div><div class="arrow">➜</div>
 <div class="step"><b>3 · Compute-scoped</b>cluster <b>Libraries</b> tab / job task libraries<br>all notebooks on the cluster</div><div class="arrow">➜</div>
 <div class="step"><b>4 · Databricks Runtime</b>pre-installed versions (pandas, numpy, pyarrow…)</div>
</div>
<div class="grid">
 <div class="card red"><h3>Typical symptoms</h3><ul>
  <li><code>ModuleNotFoundError</code> — not installed on this compute / environment</li>
  <li><code>ImportError: cannot import name …</code>, <code>AttributeError</code> — <b>wrong version</b></li>
  <li><i>pip's dependency resolver … incompatible</i> — two packages need different versions</li>
  <li>works on my cluster, fails in the job — different runtime / environment</li>
  <li>installed the new version, still see the old one — module already <b>imported</b></li></ul></div>
 <div class="card green"><h3>Fixes</h3><ul>
  <li><b>pin versions</b> (<code>==</code>), keep a <code>requirements.txt</code> in workspace files / a volume</li>
  <li><code>%pip</code> at the <b>top</b> of the notebook, then <code>dbutils.library.restartPython()</code> (Python state is lost)</li>
  <li>don't upgrade/uninstall runtime core packages (IPython, pyarrow…) unless you must</li>
  <li>same runtime / <b>environment version</b> in dev and jobs; job tasks declare their libraries / environment</li>
  <li>JVM jar clashes: shade the jar or align versions; one jar version per cluster</li></ul></div>
</div>
""" + callout("info", "<b>Serverless</b>: no cluster libraries — dependencies go in the notebook's <b>Environment</b> side panel (environment version + pinned "
              "dependencies) or <code>%pip</code>; jobs use an <b>environment</b> per task. JARs can't be installed notebook-scoped."))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · Out of memory — driver or executor?

# COMMAND ----------

# DBTITLE 1,Slide · OOM
show("""
<div class="kicker">Slide 4 · The message tells you WHERE memory ran out — and the fix is different</div>
<div class="grid two">
 <div class="card red"><h3>🧠 Driver OOM</h3>
 <b>Messages</b><ul>
  <li><i>The spark driver has stopped unexpectedly and is restarting. Your notebook will be automatically reattached.</i></li>
  <li><code>java.lang.OutOfMemoryError: Java heap space</code> / <i>GC overhead limit exceeded</i> in the driver log</li>
  <li><i>Total size of serialized results … is bigger than spark.driver.maxResultSize</i></li>
  <li>Python: <i>Fatal error: The Python kernel is unresponsive</i>, exit code 137 (killed)</li></ul>
 <b>Causes</b>: <code>collect()</code> / <code>toPandas()</code> of big data · broadcasting a large table · huge Python objects / loops on the driver · too many notebooks on one cluster · planning over millions of small files<br>
 <b>Fixes</b>: keep data distributed (aggregate / <code>limit</code> / write to a table instead of collecting) · don't force-broadcast big tables · bigger driver · compact small files</div>
 <div class="card orange"><h3>🔩 Executor OOM</h3>
 <b>Messages</b><ul>
  <li><code>ExecutorLostFailure … exited caused by one of the running tasks</code> / <i>Container killed … exceeding memory limits</i></li>
  <li><code>java.lang.OutOfMemoryError</code> inside a task, <i>FetchFailedException</i> after a lost executor</li>
  <li>heavy <b>spill</b> and GC before it dies (12-P2)</li></ul>
 <b>Causes</b>: <b>skewed</b> partition · too-large partitions · exploding joins / <code>explode</code> · big Python UDFs / pandas UDF batches · caching too much<br>
 <b>Fixes</b>: fix skew (salting, AQE), more / smaller partitions, select fewer columns, memory-optimized workers, release caches</div>
</div>
""" + callout("exam", "Exam pattern: <i>“A notebook calls <code>df.toPandas()</code> on a 200 GB table and the driver restarts”</i> → driver OOM → aggregate or "
              "write to a table instead. <i>“One task of a join fails repeatedly with executor lost while the others finish”</i> → skew → salting / AQE / broadcast."))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 · A diagnosis decision tree
# MAGIC
# MAGIC ```
# MAGIC Run failed / slow
# MAGIC ├─ Did the compute start?  ── no ──▶ event log + termination reason
# MAGIC │                                   ├─ capacity / quota / spot  → retry, other type, pools, on-demand, serverless
# MAGIC │                                   ├─ init script / network    → init-script logs, firewall / DNS / endpoints
# MAGIC │                                   └─ policy / permission      → allowed policy, CAN USE, admin
# MAGIC ├─ Did it fail on import / install? ─▶ Libraries tab, task error → pin versions, %pip + restartPython, environment
# MAGIC ├─ Out of memory?
# MAGIC │   ├─ driver restarted / maxResultSize   → stop collecting, smaller broadcasts, bigger driver
# MAGIC │   └─ executor lost / one task dies      → skew, partition size, spill → 12-P2 fixes
# MAGIC └─ Just slow?  ─▶ run history (12-P1) → Spark UI / query profile (12-P2) → layout (12-P3) → right-size compute
# MAGIC ```
# MAGIC
# MAGIC ## 6 · Right-sizing compute (cost and reliability)
# MAGIC
# MAGIC | Setting | Use it for | Watch out |
# MAGIC |---|---|---|
# MAGIC | **Autoscaling** (min–max workers) | variable load, interactive clusters | scaling up takes minutes; streaming → *enhanced autoscaling* in pipelines |
# MAGIC | **Auto-termination** (idle minutes) | all-purpose clusters — stop paying for idle compute | not for job clusters (they end with the job) |
# MAGIC | **Job compute** instead of all-purpose | scheduled production jobs — cheaper DBU rate, isolated, ends with the run | start-up time per run (pools or serverless help) |
# MAGIC | **Spot / preemptible** workers (+ fallback to on-demand) | cost savings for fault-tolerant batch | keep the **driver on-demand**; lost nodes = recomputed tasks |
# MAGIC | **Instance family**: memory-optimized · compute-optimized · storage-optimized (disk cache) · GPU | spill/OOM-heavy work · CPU-bound transformations · repeated reads · ML | match the bottleneck you measured |
# MAGIC | **Pools** | faster start of classic clusters (idle VMs kept warm) | you pay the cloud provider for idle pool VMs |
# MAGIC | **Photon** | SQL/DataFrame heavy workloads | Python UDF-heavy code gains less |
# MAGIC | **Serverless** | no tuning, seconds to start, Databricks-managed capacity | limited Spark confs, no Spark UI, no init scripts / JARs on notebooks |
# MAGIC
# MAGIC ## 🕰️ Recognize on the exam
# MAGIC * Init scripts stored on **DBFS** are deprecated → use **workspace files** or **UC volumes**.
# MAGIC * *Libraries installed from DBFS* are deprecated (disabled by default on recent runtimes).
# MAGIC * `%conda` is gone — use `%pip`.
# MAGIC
# MAGIC ## ✅ Key takeaways
# MAGIC 1. **Start-up failure** → compute **event log** + **termination reason** (code + type CLIENT_ERROR vs SERVICE_FAULT/CLOUD_FAILURE):
# MAGIC    capacity/quota, network, init script, Spark start-up, policy/permission.
# MAGIC 2. **Library conflict** → precedence notebook `%pip` > cluster libraries > runtime; pin versions, `%pip` at the top +
# MAGIC    `dbutils.library.restartPython()`, same environment in dev and jobs.
# MAGIC 3. **Driver OOM** = collecting / broadcasting too much (driver restarts, maxResultSize); **executor OOM** = skew, big
# MAGIC    partitions, explode (executor lost, spill first).
# MAGIC 4. Right-size: job compute for jobs, autoscaling + auto-termination for interactive, spot with on-demand driver,
# MAGIC    memory-optimized for spill, **serverless** to avoid most start-up problems.
