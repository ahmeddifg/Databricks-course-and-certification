# Databricks notebook source
# MAGIC %md
# MAGIC # 🚚 05-P1 · Ingestion Patterns & Lakeflow Connect
# MAGIC **Section 05 — Data Ingestion & Loading** · supports **D2 Data Ingestion & Loading (21 %)** — objectives 2.1, 2.4, 2.5, 2.6
# MAGIC
# MAGIC | After this presentation you can… |
# MAGIC |---|
# MAGIC | Tell **batch**, **incremental batch** and **streaming** ingestion apart — and pick one |
# MAGIC | Map every source type (files, databases, SaaS apps, event streams, APIs, local files) to a Databricks ingestion tool |
# MAGIC | Explain **Lakeflow Connect**: **managed connectors** vs **standard connectors** |
# MAGIC | Describe how a managed connector works (connection, ingestion gateway, ingestion pipeline, streaming tables) and how to configure one |
# MAGIC | Choose between Auto Loader, `COPY INTO`, managed connectors, partner connectors and custom code (JDBC/ODBC/REST) |
# MAGIC
# MAGIC > ▶️ **Run all** to render the diagrams (a few seconds, any compute).

# COMMAND ----------

# MAGIC %run ../../Includes/_style

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · Ingestion = the front door of the lakehouse

# COMMAND ----------

# DBTITLE 1,Slide · Sources to bronze
show("""
<div class="kicker">Slide 1 · From every source into Unity Catalog</div>
<h2>Many sources, one destination: governed Delta tables</h2>
<div class="flow">
 <div class="step"><b>☁️ Files in cloud storage</b>S3 · ADLS · GCS · volumes<br><span class="muted">CSV, JSON, Parquet, images…</span></div>
 <div class="step"><b>🗄️ Databases</b>SQL Server, PostgreSQL, MySQL, Oracle</div>
 <div class="step"><b>🧾 SaaS apps</b>Salesforce, Workday, ServiceNow…</div>
 <div class="step"><b>📡 Event streams</b>Kafka, Kinesis, Event Hubs, Pub/Sub</div>
 <div class="step"><b>🌐 APIs</b>REST, custom apps</div>
 <div class="step"><b>💻 Local files</b>a CSV on your laptop</div>
</div>
<div style="text-align:center;font-size:26px;color:#8a94a6">⬇️ ingestion ⬇️</div>
<div class="layer" style="background:#b08968">🥉 <b>Bronze</b> <small>— raw data as it arrived (+ source file, load time) in <b>Unity Catalog</b> Delta tables and volumes</small></div>
<div class="layer" style="background:#868e96">🥈 <b>Silver</b> <small>— cleaned, conformed (Sections 07–08)</small></div>
<div class="layer" style="background:#e0a800">🥇 <b>Gold</b> <small>— business aggregates for BI and AI</small></div>
""" + callout("exam", "Every exam objective in this domain ends the same way: land the data in <b>Unity Catalog-governed</b> tables. "
              "Governance (permissions, lineage, auditing) starts at ingestion."))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · Three ingestion patterns

# COMMAND ----------

# DBTITLE 1,Slide · Batch vs incremental vs streaming
show("""
<div class="kicker">Slide 2 · How much data does each run process?</div>
<h2>Batch · Incremental batch · Streaming</h2>
<table class="tbl">
<tr><th></th><th>🧱 Batch (full load)</th><th>➕ Incremental batch</th><th>🌊 Streaming</th></tr>
<tr><td>Each run reads</td><td><b>all</b> source data again</td><td>only data that is <b>new since the last run</b></td><td>new data <b>continuously</b>, as it arrives</td></tr>
<tr><td>Runs</td><td>on a schedule / on demand</td><td>on a schedule / on file arrival (triggered)</td><td>always on (continuous) — or micro-batches</td></tr>
<tr><td>Latency</td><td>hours</td><td>minutes – hours</td><td>seconds</td></tr>
<tr><td>Cost</td><td>grows with <b>total</b> data size</td><td>grows with <b>new</b> data only</td><td>compute always running</td></tr>
<tr><td>Databricks tools</td><td>CTAS / <code>CREATE OR REPLACE</code>, <code>INSERT OVERWRITE</code> from <code>read_files</code></td>
    <td><b><code>COPY INTO</code></b>, Auto Loader with <code>availableNow</code>, streaming tables (triggered), managed connectors</td>
    <td>Auto Loader / Structured Streaming (continuous), Kafka, continuous pipelines, Zerobus</td></tr>
</table>
""" + callout("tip", "Incremental batch is the sweet spot for most pipelines: low cost, simple scheduling, and — with idempotent tools "
              "like <code>COPY INTO</code> or Auto Loader — safe to re-run after a failure.")
  + callout("trap", "“Streaming” describes <b>how</b> data is processed, not the schedule. An Auto Loader stream with "
            "<code>trigger(availableNow=True)</code> runs like a batch job but only processes new files (incremental batch)."))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · The Databricks ingestion toolbox

# COMMAND ----------

# DBTITLE 1,Slide · Toolbox
show("""
<div class="kicker">Slide 3 · Lakeflow Connect and friends</div>
<h2>From “fully managed” to “write it yourself”</h2>
<div class="grid two">
 <div class="card green"><h3>🤖 Lakeflow Connect — <b>managed connectors</b> <span class="pill green">most automation</span></h3>
  <ul><li><b>Database</b> connectors with <b>CDC</b>: SQL Server, PostgreSQL, MySQL, Oracle…</li>
  <li><b>SaaS</b> connectors: Salesforce, Workday, ServiceNow, HubSpot, Google Analytics…</li>
  <li><b>File</b> connectors: SharePoint, Google Drive</li>
  <li><b>Query-based</b> database connectors (no CDC needed)</li>
  <li><b>Streaming</b> connectors: managed Kafka, RabbitMQ</li>
  <li>Databricks handles auth, incremental reads, API limits, retries, <b>schema evolution</b></li></ul></div>
 <div class="card"><h3>🔧 Lakeflow Connect — <b>standard connectors</b> <span class="pill">more customization</span></h3>
  <ul><li><b>Cloud object storage</b>: <b>Auto Loader</b> (Python, or SQL via streaming tables) · related SQL tools: <code>COPY INTO</code>, <code>read_files</code></li>
  <li><b>Message buses</b>: Kafka, Kinesis, Pub/Sub, Pulsar (Structured Streaming)</li>
  <li><b>SFTP</b> servers</li>
  <li>You write the ingestion logic; the connector reads the source</li></ul></div>
 <div class="card orange"><h3>🤝 Partner & community connectors</h3>
  <ul><li><b>Partner Connect</b>: Fivetran, Qlik, Rivery… set up from the workspace</li>
  <li>Community / custom connectors for anything else</li></ul></div>
 <div class="card purple"><h3>🧑‍💻 DIY in a notebook</h3>
  <ul><li><b>REST</b> APIs (Python <code>requests</code>), <b>JDBC/ODBC</b> (classic compute; serverless via a UC JDBC connection)</li>
  <li>Land raw → load idempotently → schedule with <b>Lakeflow Jobs</b></li></ul></div>
</div>
<div class="flow">
 <div class="step"><b>💻 Local files</b><b style="font-size:12px">New ▸ Add or upload data</b>
 <span class="muted">Create or modify a table (≤ 10 files, ≤ 2 GB: CSV, TSV, JSON, Avro, Parquet, text) · Upload files to a volume</span></div>
 <div class="step"><b>🌉 Lakehouse Federation</b>query an external database <strong>in place</strong> (foreign catalog) — no copy</div>
 <div class="step"><b>⚡ Zerobus Ingest</b>push API (part of Lakeflow Connect): apps write records <strong>directly</strong> into UC Delta tables — no message bus</div>
</div>
""" + callout("exam", "Databricks' guidance: <b>start with the most managed option</b> that meets the requirements, and move to "
              "standard connectors or custom code only when you need more control."))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · How a managed connector works

# COMMAND ----------

# DBTITLE 1,Slide · Managed connector architecture
show("""
<div class="kicker">Slide 4 · Anatomy of a Lakeflow Connect managed connector</div>
<h2>Connection → (gateway) → ingestion pipeline → streaming tables</h2>
<div class="flow">
 <div class="step"><b>🗄️ / 🧾 Source</b>database or SaaS app</div><div class="arrow">➜</div>
 <div class="step"><b>🔐 Connection</b>Unity Catalog securable that stores the credentials (<code>CREATE CONNECTION</code>, <code>USE CONNECTION</code>)</div><div class="arrow">➜</div>
 <div class="step"><b>🚪 Ingestion gateway</b><span class="pill orange">databases only</span><br>runs <strong>continuously</strong>, captures snapshots + <strong>change data (CDC)</strong> into a staging volume</div><div class="arrow">➜</div>
 <div class="step"><b>⚙️ Ingestion pipeline</b>serverless Lakeflow pipeline, runs on a <strong>schedule</strong> (a Lakeflow Job)</div><div class="arrow">➜</div>
 <div class="step"><b>📋 Streaming tables</b>in your <strong>catalog.schema</strong> — governed, with lineage</div>
</div>
<table class="tbl">
<tr><th>Behaviour</th><th>What you get</th></tr>
<tr><td>First run</td><td>full snapshot of the selected tables/objects</td></tr>
<tr><td>Later runs</td><td><b>incremental</b>: only changes since the last run (CDC / cursor columns)</td></tr>
<tr><td>History tracking</td><td><b>SCD type 1</b> (overwrite, default) or <b>SCD type 2</b> (keep history)</td></tr>
<tr><td>Schema changes</td><td>new columns handled automatically (per connector rules)</td></tr>
<tr><td>Deployment</td><td>UI, API/CLI, or <b>Declarative Automation Bundles</b> (formerly Databricks Asset Bundles) for CI/CD</td></tr>
</table>
""" + callout("info", "SaaS connectors don't need a gateway: the ingestion pipeline calls the app's API directly. "
              "The diagram shows the <b>standard</b> database architecture; some connectors (e.g. SQL Server's integrated CDC option) "
              "fold change extraction into the pipeline itself."))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 · Configuring Lakeflow Connect (UI walkthrough)
# MAGIC
# MAGIC | Step | Where / what | Notes |
# MAGIC |---|---|---|
# MAGIC | 1 | **+ New ▸ Add or upload data** → pick a connector (e.g. *SQL Server*, *Salesforce*) | The same page offers file upload and Partner Connect |
# MAGIC | 2 | **Connection** — select or create one (host, credentials / OAuth) | Needs `CREATE CONNECTION` on the metastore (or `USE CONNECTION` on an existing one) |
# MAGIC | 3 | **Ingestion gateway** (databases) — name + staging catalog/schema | Source DB must have CDC / change tracking enabled |
# MAGIC | 4 | **Source** — choose schemas, tables or objects (and columns) | |
# MAGIC | 5 | **Destination** — catalog + schema for the streaming tables | Needs `USE CATALOG`, `USE SCHEMA`, `CREATE TABLE` |
# MAGIC | 6 | **Settings** — schedule, SCD type 1/2, notifications | The schedule is a Lakeflow Job with the pipeline as a task |
# MAGIC | 7 | **Run & monitor** — pipeline page: event log, row counts, errors | Downstream jobs can start on table updates |
# MAGIC
# MAGIC > 🧭 In **05-L2 Part 9** you tour these screens without creating anything (a managed connector needs a real source system).
# MAGIC
# MAGIC ## 6 · Choosing the right method

# COMMAND ----------

# DBTITLE 1,Slide · Decision guide
show("""
<div class="kicker">Slide 6 · Decision guide (objective 2.6)</div>
<h2>Which ingestion method?</h2>
<table class="tbl">
<tr><th>Requirement</th><th>Best fit</th><th>Why</th></tr>
<tr><td>A one-off CSV/JSON export from your laptop</td><td><b>Create or modify a table</b> (file upload) / upload to a volume</td><td>no code, managed Delta table in seconds</td></tr>
<tr><td>Files land in cloud storage; SQL team; thousands of files; scheduled</td><td><b><code>COPY INTO</code></b> (or a streaming table on <code>read_files</code>)</td><td>idempotent, pure SQL, simple</td></tr>
<tr><td>Files land continuously; millions of files; schema drifts</td><td><b>Auto Loader</b> (Section 06)</td><td>scalable file discovery, schema inference/evolution, exactly-once</td></tr>
<tr><td>Salesforce / Workday / ServiceNow… daily, minimal maintenance</td><td><b>Managed SaaS connector</b></td><td>API limits, auth, retries, schema changes handled</td></tr>
<tr><td>Operational database, capture inserts/updates/deletes</td><td><b>Managed database connector (CDC)</b></td><td>gateway + incremental change capture</td></tr>
<tr><td>Kafka / Kinesis event streams, low latency</td><td><b>Structured Streaming</b> (standard connector), a pipeline, or the managed Kafka connector</td><td>continuous, seconds of latency</td></tr>
<tr><td>A source only a partner supports</td><td><b>Partner Connect</b> (e.g. Fivetran)</td><td>validated third-party integration</td></tr>
<tr><td>A custom REST API or legacy system</td><td><b>Notebook</b> (REST / JDBC) + <b>Lakeflow Jobs</b></td><td>full control — you own maintenance</td></tr>
<tr><td>Query an external DB without copying data</td><td><b>Lakehouse Federation</b></td><td>foreign catalog, live queries</td></tr>
</table>
""" + callout("trap", "<code>COPY INTO</code> and Auto Loader read <b>files</b> from cloud storage — they cannot call a SaaS API or a database. "
              "For those, think managed connector, partner connector or a notebook."))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7 · Custom ingestion in notebooks: JDBC/ODBC & REST (objective 2.5)

# COMMAND ----------

# DBTITLE 1,Slide · Land raw then load
show("""
<div class="kicker">Slide 7 · The DIY pattern</div>
<h2>Extract → land raw → load idempotently → schedule</h2>
<div class="flow">
 <div class="step"><b>1 · Extract</b>Python <code>requests.get(url)</code> · <code>spark.read.format("jdbc")</code> (classic compute)</div><div class="arrow">➜</div>
 <div class="step"><b>2 · Land raw</b>write the response unchanged to a <strong>volume</strong> / cloud path, one file per run</div><div class="arrow">➜</div>
 <div class="step"><b>3 · Load</b><code>COPY INTO</code> / Auto Loader → UC table (idempotent) — or write directly with <code>saveAsTable</code></div><div class="arrow">➜</div>
 <div class="step"><b>4 · Orchestrate</b><strong>Lakeflow Jobs</strong>: schedule, retries, alerts, parameters</div>
</div>
<div class="grid two">
 <div class="card"><h3>🔑 Secrets</h3><code>dbutils.secrets.get(scope, key)</code> — never hard-code passwords or tokens; values are redacted in output.</div>
 <div class="card orange"><h3>⚠️ Serverless &amp; JDBC</h3>The classic <code>jdbc</code> source with a URL + driver runs on <b>classic</b> compute. On serverless use a UC <b>connection</b>: a <b>JDBC connection</b> (<code>.option('databricks.connection', 'erp_conn')</code>, driver JAR in a volume — preview), Lakehouse Federation, or Lakeflow Connect database connectors.</div>
</div>
""" + callout("info", "The <b>Databricks JDBC/ODBC drivers</b> work in the other direction too: BI tools and applications connect to a "
              "SQL warehouse to query (or insert into) Unity Catalog tables."))

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Key takeaways
# MAGIC 1. **Batch** re-reads everything; **incremental batch** reads only new data per run; **streaming** processes continuously.
# MAGIC 2. **Lakeflow Connect managed connectors** (databases with CDC, SaaS, files, streaming such as Kafka) = most automation; **standard connectors**
# MAGIC    (Auto Loader — plus `COPY INTO`/`read_files` — Kafka/Kinesis via Structured Streaming, SFTP) = more control. **Zerobus** = push API into UC tables.
# MAGIC 3. Managed connector = **connection** (UC securable) → **ingestion gateway** (databases) → serverless **ingestion pipeline** → **streaming tables**, scheduled by a job, SCD 1/2.
# MAGIC 4. Local files: **Create or modify a table** (≤10 files, ≤2 GB) or **upload to a volume**.
# MAGIC 5. Custom sources: notebook with **REST/JDBC** → land raw in a volume → idempotent load → **Lakeflow Jobs**; secrets via `dbutils.secrets`.
# MAGIC 6. Start with the **most managed** option that fits.
# MAGIC
# MAGIC ➡️ Next: **05-P2 · Querying Files and read_files**
