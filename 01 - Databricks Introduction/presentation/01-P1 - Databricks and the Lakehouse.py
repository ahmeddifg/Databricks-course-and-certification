# Databricks notebook source
# MAGIC %md
# MAGIC # 🏛️ 01-P1 · Databricks & the Lakehouse
# MAGIC **Section 01 — Databricks Introduction & Platform** · Exam domain **D1 · Databricks Intelligence Platform (6 %)**
# MAGIC
# MAGIC | After this presentation you can… | Exam objective |
# MAGIC |---|---|
# MAGIC | Explain why the lakehouse exists and how it differs from a warehouse and a data lake | 1.1 Platform core components |
# MAGIC | Describe the Databricks architecture: control plane, compute plane, cloud storage | 1.1 |
# MAGIC | Place Delta Lake, Unity Catalog, Lakeflow and Databricks SQL on the platform map | 1.1 |
# MAGIC | Recognise old and new product names used in exam questions | all domains |
# MAGIC
# MAGIC > ▶️ **Run all** to render the diagrams (no data needed, a few seconds on any compute).

# COMMAND ----------

# MAGIC %run ../../Includes/_style

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · Why a lakehouse?
# MAGIC
# MAGIC For years companies ran **two systems**: a **data warehouse** for BI and a **data lake** for raw data and ML.
# MAGIC Data was copied between them, governed twice, and was often stale or inconsistent.

# COMMAND ----------

# DBTITLE 1,Slide · Warehouse vs lake vs lakehouse
show("""
<div class="kicker">Slide 1 · The trade-off the lakehouse removes</div>
<h2>Warehouse vs Data Lake vs Lakehouse</h2>
<table class="tbl">
<tr><th></th><th>🏢 Data warehouse</th><th>🌊 Data lake</th><th>🏛️ Lakehouse (Databricks)</th></tr>
<tr><td><b>Data types</b></td><td>Structured only</td><td>Any: structured, semi-, unstructured</td><td>Any</td></tr>
<tr><td><b>Storage</b></td><td>Proprietary, expensive</td><td>Cheap cloud object storage</td><td>Cheap cloud object storage in <b>open formats</b> (Delta Lake / Iceberg)</td></tr>
<tr><td><b>Reliability</b></td><td>ACID transactions, schemas</td><td>No transactions → partial writes, "data swamp"</td><td><b>ACID transactions</b> + schema enforcement via Delta Lake</td></tr>
<tr><td><b>Workloads</b></td><td>SQL / BI</td><td>Data science, ML</td><td>BI, SQL, streaming, data engineering, ML & AI — <b>one copy of data</b></td></tr>
<tr><td><b>Governance</b></td><td>Strong, but only inside the warehouse</td><td>Weak, file-level</td><td>One model for all assets: <b>Unity Catalog</b></td></tr>
<tr><td><b>Scaling</b></td><td>Costly</td><td>Elastic</td><td>Elastic, compute separated from storage</td></tr>
</table>
""" + callout("exam", "Lakehouse = the <b>openness & low cost of a data lake</b> + the <b>reliability, performance & governance of a warehouse</b>, "
              "on a single copy of the data.")
    + callout("trap", "A lakehouse does <b>not</b> copy data into a proprietary warehouse format. Data stays in <i>your</i> cloud storage in open formats."))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · The Databricks Data Intelligence Platform
# MAGIC
# MAGIC Databricks calls itself a **Data Intelligence Platform**: a lakehouse with an AI engine that learns your data's semantics
# MAGIC (used by the Databricks Assistant, Genie, predictive optimization …). Read the diagram **bottom → top**.

# COMMAND ----------

# DBTITLE 1,Slide · Platform layers
show("""
<div class="kicker">Slide 2 · Platform map</div>
<h2>From cloud storage to workloads</h2>
<div class="grid" style="grid-template-columns:repeat(5,1fr)">
  <div class="card orange"><h3>⚙️ Lakeflow</h3>Ingest (Connect)<br>Transform (Spark Declarative Pipelines)<br>Orchestrate (Jobs)</div>
  <div class="card"><h3>📊 Databricks SQL</h3>SQL warehouses, SQL editor, AI/BI dashboards, Genie</div>
  <div class="card purple"><h3>🤖 AI / ML</h3>Mosaic AI: notebooks, MLflow, model serving, agents</div>
  <div class="card teal"><h3>🔁 Streaming</h3>Structured Streaming, real-time pipelines</div>
  <div class="card gray"><h3>🧩 Apps & more</h3>Databricks Apps, Lakebase, Marketplace</div>
</div>
<div class="layer" style="background:#e8590c">🧠 <b>Data Intelligence Engine</b> <small>— AI that understands your data: Assistant, Genie, predictive optimization, smart search</small></div>
<div class="layer" style="background:#7048e8">🛡️ <b>Unity Catalog</b> <small>— one governance layer: access control, lineage, auditing, discovery for tables, files, models, dashboards</small></div>
<div class="layer" style="background:#1c7ed6">📐 <b>Delta Lake (+ Apache Iceberg via UniForm)</b> <small>— open table format: ACID transactions, schema enforcement, time travel</small></div>
<div class="layer" style="background:#495057">☁️ <b>Cloud object storage</b> <small>— Amazon S3 · Azure Data Lake Storage (ADLS) · Google Cloud Storage (GCS)</small></div>
<p class="muted">Compute (Apache Spark + the <b>Photon</b> vectorized engine) runs every workload on top of this stack — compute is separated from storage.</p>
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · Architecture: control plane vs compute plane
# MAGIC
# MAGIC This split is a classic exam topic: **what runs where, and where does my data live?**

# COMMAND ----------

# DBTITLE 1,Slide · Control plane / compute plane
show("""
<div class="kicker">Slide 3 · Architecture</div>
<h2>Where things run</h2>
<div class="grid two">
  <div class="card purple"><h3>🎛️ Control plane <span class="pill purple">Databricks' account</span></h3>
    <ul><li>Web application (the workspace UI)</li><li>Notebooks, workspace files & job definitions</li>
    <li>Job scheduler, compute manager</li><li>Unity Catalog metadata services</li></ul>
    <p class="muted">Managed by Databricks. You interact with it through the UI, REST API, CLI and SDKs.</p></div>
  <div class="card"><h3>🖥️ Compute plane <span class="pill">where data is processed</span></h3>
    <ul><li><b>Classic compute plane</b> — VMs run <b>in your cloud account</b> (your VPC/VNet). You pay the cloud provider for VMs + Databricks for DBUs.</li>
    <li><b>Serverless compute plane</b> — compute runs <b>in Databricks' cloud account</b> (same region, network-isolated per workspace). Starts in seconds; one bill.</li></ul></div>
</div>
<div class="flow">
  <div class="step"><b>🧑‍💻 You</b>browser · CLI · API</div><div class="arrow">➜</div>
  <div class="step"><b>🎛️ Control plane</b>schedules & manages</div><div class="arrow">➜</div>
  <div class="step"><b>🖥️ Compute plane</b>classic or serverless Spark</div><div class="arrow">⇄</div>
  <div class="step"><b>☁️ Your cloud storage</b>Delta tables, volumes (S3 / ADLS / GCS)</div>
</div>
""" + callout("exam", "Your <b>data stays in cloud object storage</b> (your account, or Databricks-managed default storage). "
              "The control plane stores metadata and code, not your table data.")
    + callout("trap", "Serverless ≠ \"runs in the control plane\". It runs in a separate <b>serverless compute plane</b> managed by Databricks."))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · The core building blocks
# MAGIC
# MAGIC | Building block | What it is | You'll use it for | Section |
# MAGIC |---|---|---|---|
# MAGIC | **Workspace** | The web environment: folders, notebooks, files, Git folders, dashboards | Developing & collaborating | 01 |
# MAGIC | **Compute** | Serverless, all-purpose & job compute, SQL warehouses | Running code | 01 |
# MAGIC | **Delta Lake** | Open-source storage layer that adds ACID transactions to Parquet files | Every table in the lakehouse | 03 |
# MAGIC | **Unity Catalog** | Governance: `catalog.schema.object` namespace, privileges, lineage, audit | Organising & securing data | 04, 13 |
# MAGIC | **Lakeflow Connect** | Managed & standard ingestion connectors | Getting data in | 05 |
# MAGIC | **Lakeflow Spark Declarative Pipelines** | Declarative ETL: streaming tables, materialized views, expectations | Building pipelines | 08 |
# MAGIC | **Lakeflow Jobs** | Orchestration: tasks, dependencies, schedules, triggers | Running things in production | 10 |
# MAGIC | **Databricks SQL** | SQL warehouses, SQL editor, dashboards, alerts | Serving data to analysts & BI | 09 |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 · Delta Lake in one slide *(deep dive → Section 03)*
# MAGIC
# MAGIC - An **open-source storage layer**: data in **Parquet** files + a **transaction log** (`_delta_log/`) of JSON commits.
# MAGIC - Adds to a plain data lake: **ACID transactions**, **schema enforcement & evolution**, **time travel**,
# MAGIC   `UPDATE` / `DELETE` / `MERGE`, unified **batch + streaming**, and file-layout optimizations.
# MAGIC - **Default table format** on Databricks: `CREATE TABLE t (...)` creates a Delta table unless you say otherwise.
# MAGIC
# MAGIC > ⚠️ **Exam trap:** Delta Lake is **not** a database or a separate storage service — it is a **file format + protocol** on top of your object storage.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6 · Unity Catalog in one slide *(deep dive → Sections 04 & 13)*

# COMMAND ----------

# DBTITLE 1,Slide · Unity Catalog hierarchy
show("""
<div class="kicker">Slide 6 · Unity Catalog object model</div>
<h2>The 3-level namespace: <code>catalog.schema.object</code></h2>
<div class="layer" style="background:#0b2a4a">🗄️ <b>Metastore</b> <small>— top-level container, one per region; attached to workspaces</small></div>
<div style="margin-left:24px"><div class="layer" style="background:#7048e8">📚 <b>Catalog</b> <small>— e.g. <code>workspace</code>, <code>prod</code>, <code>dev</code> (isolation boundary)</small></div>
<div style="margin-left:24px"><div class="layer" style="background:#1c7ed6">📁 <b>Schema</b> (database) <small>— e.g. <code>shopwave</code></small></div>
<div style="margin-left:24px" class="grid">
  <div class="card"><h3>🧮 Table</h3>managed / external</div>
  <div class="card"><h3>👓 View</h3>stored query · materialized view</div>
  <div class="card green"><h3>📦 Volume</h3>governed <b>files</b>: <code>/Volumes/cat/sch/vol/</code></div>
  <div class="card purple"><h3>ƒ Function</h3>SQL / Python UDFs</div>
  <div class="card orange"><h3>🤖 Model</h3>registered ML models</div>
</div></div></div>
<p><b>Example:</b> <code>SELECT * FROM workspace.shopwave.orders</code> — or run <code>USE CATALOG workspace; USE SCHEMA shopwave;</code> and just write <code>orders</code>.</p>
""" + callout("legacy", "Before Unity Catalog each workspace had its own <b>Hive metastore</b> (2-level <code>schema.table</code>). "
              "It still appears as the catalog <code>hive_metastore</code> in older workspaces."))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7 · Lakeflow — the data engineering family

# COMMAND ----------

# DBTITLE 1,Slide · Lakeflow
show("""
<div class="kicker">Slide 7 · Lakeflow</div>
<h2>Ingest → Transform → Orchestrate</h2>
<div class="flow">
  <div class="step"><b>🔌 Lakeflow Connect</b>Managed connectors (Salesforce, SQL Server, …)<br>Standard connectors (Auto Loader, COPY INTO, Kafka)</div>
  <div class="arrow">➜</div>
  <div class="step"><b>🧱 Lakeflow Spark Declarative Pipelines</b>Streaming tables · materialized views · expectations<br><span class="muted">formerly Delta Live Tables (DLT)</span></div>
  <div class="arrow">➜</div>
  <div class="step"><b>🗓️ Lakeflow Jobs</b>Tasks, DAGs, schedules, triggers, retries<br><span class="muted">formerly Databricks Workflows / Jobs</span></div>
</div>
""" + callout("exam", "Expect questions that describe a need (\"ingest from Salesforce with no code\", \"declare data-quality rules\", "
              "\"run B after A every night\") and ask which Lakeflow component fits."))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8 · Where do files live? DBFS vs Unity Catalog volumes
# MAGIC
# MAGIC | | 🕰️ DBFS (legacy) | ✅ Unity Catalog volumes |
# MAGIC |---|---|---|
# MAGIC | What | Workspace-level file system abstraction over object storage | Governed UC object for **non-tabular files** |
# MAGIC | Paths | `dbfs:/FileStore/…`, `dbfs:/mnt/<mount>/…`, `dbfs:/user/hive/warehouse/…` | `/Volumes/<catalog>/<schema>/<volume>/…` |
# MAGIC | Access control | Coarse; **mounts** expose storage to *every* user of the workspace | Unity Catalog privileges: `READ VOLUME`, `WRITE VOLUME` |
# MAGIC | Governance | None (no lineage, no audit per user) | Lineage, audit, discovery like any UC object |
# MAGIC | Serverless / Free Edition | ❌ DBFS root and mounts are limited / not available | ✅ Fully supported |
# MAGIC
# MAGIC > 🎯 **Exam focus:** for new work, store files in **volumes** and tables in **Unity Catalog**.
# MAGIC > Mount points and the DBFS root are legacy patterns — recognise them, don't choose them.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9 · Who uses what?
# MAGIC
# MAGIC | Persona | Main tools on Databricks |
# MAGIC |---|---|
# MAGIC | 🛠️ **Data engineer** *(you!)* | Notebooks, Lakeflow (Connect, Declarative Pipelines, Jobs), Delta Lake, Unity Catalog, bundles & Git folders |
# MAGIC | 📈 **Data analyst** | SQL editor, SQL warehouses, AI/BI dashboards, Genie spaces, alerts |
# MAGIC | 🔬 **Data scientist / ML engineer** | Notebooks, MLflow, feature engineering, model serving, Mosaic AI |
# MAGIC | 🛡️ **Admin** | Account console, workspace settings, Unity Catalog metastore, compute policies, system tables |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10 · 🕰️ Name-change cheat sheet (memorise!)
# MAGIC Exam questions and older study material mix old and new names. They mean the **same thing**:
# MAGIC
# MAGIC | Old name | Current name |
# MAGIC |---|---|
# MAGIC | Delta Live Tables (DLT) · Lakeflow Declarative Pipelines | **Lakeflow Spark Declarative Pipelines** |
# MAGIC | `LIVE.` prefix, `CREATE STREAMING LIVE TABLE`, `cloud_files()` | `CREATE OR REFRESH STREAMING TABLE`, `STREAM read_files()` |
# MAGIC | Databricks Workflows / Jobs | **Lakeflow Jobs** |
# MAGIC | Databricks Asset Bundles (DABs) | **Declarative Automation Bundles** |
# MAGIC | Repos | **Git folders** |
# MAGIC | SQL endpoints | **SQL warehouses** |
# MAGIC | Data Explorer | **Catalog Explorer** |
# MAGIC | Access mode *Shared* / *Single user* | **Standard** / **Dedicated** |
# MAGIC | Clusters (UI) | **Compute** |
# MAGIC | DBFS mounts (`/mnt/…`) | **Unity Catalog volumes** & external locations |

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Key takeaways
# MAGIC 1. **Lakehouse** = one copy of data in open formats on cheap storage, with warehouse-grade reliability (Delta Lake) and governance (Unity Catalog).
# MAGIC 2. **Control plane** (Databricks account): UI, notebooks, jobs, metadata. **Compute plane**: classic (your cloud account) or serverless (Databricks' account). **Data** stays in object storage.
# MAGIC 3. **Delta Lake** = Parquet + transaction log → ACID, time travel, DML, streaming.
# MAGIC 4. **Unity Catalog** = metastore → catalog → schema → tables / views / volumes / functions / models.
# MAGIC 5. **Lakeflow** = Connect (ingest) + Spark Declarative Pipelines (transform) + Jobs (orchestrate).
# MAGIC 6. Use **volumes**, not DBFS mounts, for files.
# MAGIC
# MAGIC ➡️ Next: **01-P2 · Workspace & Notebooks**
