# Databricks notebook source
# MAGIC %md
# MAGIC # ⚙️ 08-P2 · Lakeflow Spark Declarative Pipelines
# MAGIC **Section 08** · exam objectives **2.x** (incremental ingestion into UC), **3.6** (streaming tables & materialized views as
# MAGIC gold objects), **4.x** (pipeline tasks in jobs), **6.x** (pipeline health)
# MAGIC
# MAGIC | After this presentation you can… |
# MAGIC |---|
# MAGIC | Recognise every **name** the product has had (DLT → Lakeflow Declarative Pipelines → Lakeflow **Spark** Declarative Pipelines) |
# MAGIC | Choose between a **streaming table**, a **materialized view** and a **temporary view** |
# MAGIC | Write pipeline code in **SQL** (`CREATE OR REFRESH …`, `STREAM`, `read_files`) and **Python** (`pyspark.pipelines as dp`) |
# MAGIC | Explain how a pipeline finds its code (**root folder**, `transformations/`) and its **configuration** |
# MAGIC | Pick the right **update** (refresh, full refresh, selective, dry run) and **settings** (triggered/continuous, dev/prod, editions, channel) |
# MAGIC
# MAGIC > ▶️ **Run all** to render the diagrams.

# COMMAND ----------

# MAGIC %run ../../Includes/_style

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · One product, many names

# COMMAND ----------

# DBTITLE 1,Slide · Names
show("""
<div class="kicker">Slide 1 · Same engine, new names — you must recognise all of them</div>
<h2>Delta Live Tables → Lakeflow Declarative Pipelines → Lakeflow <u>Spark</u> Declarative Pipelines</h2>
<div class="flow">
 <div class="step"><b>🕰️ Delta Live Tables (DLT)</b><code>import dlt</code> · <code>LIVE.</code> · <code>cloud_files()</code></div><div class="arrow">➜</div>
 <div class="step"><b>Lakeflow Declarative Pipelines</b>2025 · part of <b>Lakeflow</b> (Connect · Pipelines · Jobs)</div><div class="arrow">➜</div>
 <div class="step"><b>Apache Spark Declarative Pipelines</b>the framework is open source — Apache Spark 4.1+</div><div class="arrow">➜</div>
 <div class="step"><b>Lakeflow Spark Declarative Pipelines (SDP)</b><code>from pyspark import pipelines as dp</code> · docs also say “Lakeflow pipelines”</div>
</div>
<div class="grid">
 <div class="card"><h3>What stayed</h3>the concepts: streaming tables, materialized views, expectations, event log, CDC</div>
 <div class="card green"><h3>What changed</h3>Python module <code>dp</code>, <code>@dp.materialized_view</code>, <code>@dp.temporary_view</code>,
 <code>AUTO CDC</code> (was <code>APPLY CHANGES</code>), no <code>LIVE.</code> prefix, a multi-file <b>editor</b></div>
 <div class="card gray"><h3>Still says “DLT”</h3>classic billing SKUs, event log schemas, the legacy <code>dlt</code> module — old code keeps working</div>
</div>
""" + callout("exam", "The exam guide uses <b>Lakeflow Spark Declarative Pipelines</b>. Older questions and dumps say DLT / "
              "Delta Live Tables / Lakeflow Declarative Pipelines — treat them as the same thing."))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · Core concepts

# COMMAND ----------

# DBTITLE 1,Slide · Pipeline, flows, datasets
show("""
<div class="kicker">Slide 2 · You declare datasets — the pipeline runs flows to keep them up to date</div>
<h2>A pipeline = source files + settings → a dependency graph of datasets</h2>
<div class="grid">
 <div class="card"><h3>🌊 Streaming table</h3>Delta table fed by one or more <b>streaming flows</b>. Each input row is processed
 <b>once</b> → append-only, incremental, low latency. Bronze, silver, CDC targets.</div>
 <div class="card green"><h3>🧮 Materialized view</h3>Delta table holding the <b>result of a query</b>, kept correct on each update;
 refreshed <b>incrementally</b> when possible (serverless), else fully. Joins, aggregates, gold.</div>
 <div class="card purple"><h3>👻 Temporary view</h3>Named query <b>inside the pipeline only</b>; not published. Intermediate steps,
 reusable logic, expectations before a CDC flow.</div>
 <div class="card orange"><h3>🔀 Flow</h3>The unit of work that reads a source and writes a target (append, update, <code>AUTO CDC</code>).
 Usually implicit: one query = one flow.</div>
 <div class="card gray"><h3>📤 Sink</h3>Write a stream to something that isn't a pipeline table (Kafka, Event Hubs, external Delta).</div>
 <div class="card teal"><h3>▶️ Update</h3>One run of the pipeline: resolve the graph, start compute, run flows, publish tables.</div>
</div>
""" + callout("info", "Streaming tables and materialized views are <b>Unity Catalog objects</b> (<code>catalog.schema.name</code>) that anyone with "
              "<code>SELECT</code> can query — but only their pipeline writes them. Temporary views are never published."))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · Streaming table, materialized view or temporary view?
# MAGIC
# MAGIC | Question | Streaming table | Materialized view | Temporary view |
# MAGIC |---|---|---|---|
# MAGIC | Processing | incremental, each row **once** | result recomputed to match sources (**incremental refresh** when possible) | computed when read by another dataset |
# MAGIC | Source requirement | **append-only** (or `skipChangeCommits`) | any: updates / deletes in sources are reflected | any |
# MAGIC | Typical layer | bronze, silver, CDC targets | silver joins, **gold** aggregates | helper steps |
# MAGIC | Stateful ops (agg, joins, dedup) | need watermarks / bounded state | ✅ natural | ✅ |
# MAGIC | Dimension changes | old rows **not** re-joined | ✅ re-computed | — |
# MAGIC | Published in UC | ✅ | ✅ | ❌ |
# MAGIC | SQL | `CREATE OR REFRESH STREAMING TABLE … FROM STREAM …` | `CREATE OR REFRESH MATERIALIZED VIEW` | `CREATE TEMPORARY VIEW` |
# MAGIC | Python | `@dp.table` returning `spark.readStream…` | `@dp.materialized_view` (or `@dp.table` returning a batch DF) | `@dp.temporary_view` |
# MAGIC
# MAGIC > ⚠️ **Exam trap:** "the aggregate must reflect deletes/corrections in the source" → **materialized view**, not a streaming
# MAGIC > table. "Ingest files incrementally, each file once" → **streaming table**.
# MAGIC
# MAGIC ## 4 · SQL syntax
# MAGIC
# MAGIC ```sql
# MAGIC -- BRONZE: incremental file ingestion (Auto Loader under the hood)
# MAGIC CREATE OR REFRESH STREAMING TABLE orders_bronze
# MAGIC COMMENT 'raw orders'
# MAGIC AS SELECT *, _metadata.file_name AS source_file
# MAGIC FROM STREAM read_files('${dataset_path}/orders', format => 'json');
# MAGIC
# MAGIC -- SILVER: stream from another pipeline table + expectations + stream-static join
# MAGIC CREATE OR REFRESH STREAMING TABLE orders_silver (
# MAGIC   CONSTRAINT valid_id      EXPECT (order_id IS NOT NULL) ON VIOLATION FAIL UPDATE,
# MAGIC   CONSTRAINT not_cancelled EXPECT (quantity > 0)         ON VIOLATION DROP ROW,
# MAGIC   CONSTRAINT has_country   EXPECT (country IS NOT NULL)                        -- warn
# MAGIC )
# MAGIC AS SELECT o.*, c.country
# MAGIC FROM STREAM(orders_bronze) o LEFT JOIN customers c USING (customer_id);
# MAGIC
# MAGIC -- helper, not published
# MAGIC CREATE TEMPORARY VIEW orders_unique AS SELECT DISTINCT order_id, order_date, total FROM orders_silver;
# MAGIC
# MAGIC -- GOLD: batch query -> materialized view
# MAGIC CREATE OR REFRESH MATERIALIZED VIEW daily_revenue
# MAGIC AS SELECT order_date, count(*) AS orders, sum(total) AS revenue FROM orders_unique GROUP BY order_date;
# MAGIC ```
# MAGIC
# MAGIC * `STREAM` (or `STREAM(t)`) = read **incrementally** → only allowed where the target is a streaming table.
# MAGIC   Without `STREAM` the source is read as a batch.
# MAGIC * `CREATE OR REFRESH` — the pipeline creates the dataset the first time and refreshes it on each update.
# MAGIC * Reference other datasets **by name** — unqualified names resolve in the pipeline's default catalog/schema; fully
# MAGIC   qualified names are allowed. The legacy `LIVE.` prefix is ignored in new pipelines.
# MAGIC * Not supported in pipeline SQL: `PIVOT`. Also table options: `COMMENT`, `TBLPROPERTIES`, `PARTITIONED BY` /
# MAGIC   `CLUSTER BY`, column definitions, `PRIVATE` (pipeline-internal table that persists).
# MAGIC
# MAGIC ## 5 · Python syntax
# MAGIC
# MAGIC ```python
# MAGIC from pyspark import pipelines as dp
# MAGIC from pyspark.sql import functions as F
# MAGIC
# MAGIC path = spark.conf.get("dataset_path")                    # pipeline Configuration
# MAGIC
# MAGIC @dp.table(comment="raw orders")                          # streaming DF -> streaming table
# MAGIC def orders_bronze():
# MAGIC     return (spark.readStream.format("cloudFiles")        # Auto Loader - no checkpoint/schemaLocation to manage
# MAGIC                  .option("cloudFiles.format", "json")
# MAGIC                  .load(f"{path}/orders"))
# MAGIC
# MAGIC @dp.table
# MAGIC @dp.expect_or_fail("valid_id", "order_id IS NOT NULL")
# MAGIC @dp.expect_or_drop("not_cancelled", "quantity > 0")
# MAGIC @dp.expect("has_country", "country IS NOT NULL")
# MAGIC def orders_silver():
# MAGIC     return (spark.readStream.table("orders_bronze")      # another dataset, by name
# MAGIC                  .join(spark.read.table("customers"), "customer_id", "left"))
# MAGIC
# MAGIC @dp.materialized_view                                    # batch DF -> materialized view
# MAGIC def daily_revenue():
# MAGIC     return (spark.read.table("orders_silver")
# MAGIC                  .groupBy(F.to_date("order_ts").alias("order_date"))
# MAGIC                  .agg(F.count("*").alias("orders"), F.sum("total").alias("revenue")))
# MAGIC ```
# MAGIC
# MAGIC * The dataset name defaults to the **function name** (or `name=` in the decorator).
# MAGIC * Functions must **only return a DataFrame**: no `collect()`, `count()`, `toPandas()`, `save()`, `saveAsTable()`,
# MAGIC   `start()`, `toTable()` — the pipeline decides when and how to materialize.
# MAGIC * Mix freely: one pipeline can have SQL **and** Python files.
# MAGIC
# MAGIC ## 6 · Anatomy of a pipeline

# COMMAND ----------

# DBTITLE 1,Slide · Root folder, editor, settings
show("""
<div class="kicker">Slide 6 · What a pipeline is made of</div>
<h2>Code in workspace files + settings (JSON) — edited in the Lakeflow Pipelines Editor</h2>
<div class="grid two">
 <div class="card"><h3>📁 Root folder</h3><ul>
  <li><code>transformations/</code> — the <b>source files</b> (.sql / .py) that define datasets</li>
  <li><code>explorations/</code> — ad-hoc notebooks/queries, <b>not</b> part of the pipeline</li>
  <li>Keep it in a <b>Git folder</b> (Section 11) or deploy it with <b>Declarative Automation Bundles</b></li>
  <li>File names and statement order don't matter — the <b>graph</b> does</li></ul></div>
 <div class="card green"><h3>🖥️ Editor toolbar</h3><ul>
  <li><b>Run pipeline</b> — update all datasets</li>
  <li><b>Run file</b> — only the datasets of the open file</li>
  <li><b>Dry run</b> — validate code &amp; graph, write nothing</li>
  <li><b>Settings</b> · <b>Schedule</b> (creates a Lakeflow Job with a pipeline task)</li>
  <li>Graph, tables preview, data-quality tab, update history</li></ul></div>
</div>
<div class="flow">
 <div class="step"><b>New → ETL pipeline</b>name, default catalog + schema</div><div class="arrow">➜</div>
 <div class="step"><b>Code</b>sample code · empty file · <b>add existing assets</b></div><div class="arrow">➜</div>
 <div class="step"><b>Settings</b>configuration, mode, compute</div><div class="arrow">➜</div>
 <div class="step"><b>Run / Schedule</b>update → tables in UC</div>
</div>
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7 · Configuration — no hard-coded paths
# MAGIC
# MAGIC | | Where you set it | In SQL | In Python |
# MAGIC |---|---|---|---|
# MAGIC | **Configuration** (key-value, GA) | Settings → Configuration | `'${dataset_path}/orders'` (text substitution) | `spark.conf.get("dataset_path")` |
# MAGIC | **Parameters** (Beta, SQL only) | Settings → Parameters, or overridden per update / by a job | `:start_date`, `IDENTIFIER(:catalog)` | — use Configuration |
# MAGIC
# MAGIC Keys may contain letters, digits, `_`, `-`, `.`. The same code then runs against dev and prod data by changing only the
# MAGIC settings (or a bundle target — Section 11).
# MAGIC
# MAGIC ## 8 · Updates

# COMMAND ----------

# DBTITLE 1,Slide · Refresh types
show("""
<div class="kicker">Slide 8 · What happens when you click Run?</div>
<h2>Four kinds of updates</h2>
<table class="tbl">
<tr><th>Update</th><th>Streaming tables</th><th>Materialized views</th><th>When</th></tr>
<tr><td><b>Refresh</b> (default)</td><td>process <b>new</b> input only</td><td>refresh — incrementally if cheaper</td><td>every scheduled run</td></tr>
<tr><td><b>Full refresh</b></td><td><b>truncate</b>, clear checkpoints, reprocess <b>all</b> source data still available</td><td>recompute fully</td><td>logic changed and history must be rebuilt</td></tr>
<tr><td><b>Refresh selection</b></td><td colspan="2">only the chosen tables (optionally with full refresh)</td><td>development, after fixing one table</td></tr>
<tr><td><b>Dry run</b> (validate)</td><td colspan="2">resolve code, schemas and graph — <b>no data written</b></td><td>before deploying a change</td></tr>
</table>
""" + callout("trap", "Full refresh re-reads the <b>source</b>. Kafka retention or file clean-up → data <b>lost</b> from the "
              "streaming table. Prevent it per table: <code>TBLPROPERTIES ('pipelines.reset.allowed' = 'false')</code>."))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9 · Settings you must know
# MAGIC
# MAGIC | Setting | Options | Notes |
# MAGIC |---|---|---|
# MAGIC | **Pipeline mode** | **Triggered** (default) · Continuous | triggered: refresh everything available, then **stop** (schedule it) · continuous: keep running, low latency, compute always on |
# MAGIC | **Compute** | **Serverless** (default for new pipelines) · classic | serverless: no cluster settings, standard vs **performance optimized** mode; classic: workers, (enhanced) autoscaling, Photon, instance types |
# MAGIC | **Product edition** (classic) | Core · Pro · **Advanced** (default) | **Core** = streaming tables/MVs · **Pro** = + CDC (`AUTO CDC`) · **Advanced** = + **expectations**. Serverless includes everything |
# MAGIC | **Channel** | **Current** · Preview | runtime version; Preview = test upcoming changes |
# MAGIC | **Default catalog / schema** | UC catalog + schema | where unqualified datasets are published; code may write to other schemas with full names |
# MAGIC | **Configuration / Parameters** | key-values | see §7 |
# MAGIC | **Notifications**, **Run as**, **Tags** | | e-mail on failure; production = service principal |
# MAGIC | **Event log** | hidden (default) or published to a UC table | read it with `event_log()` (08-P3) |
# MAGIC
# MAGIC **Development vs production behaviour**
# MAGIC
# MAGIC | | Development (runs from the editor / UI) | Production (jobs, API, continuous) |
# MAGIC |---|---|---|
# MAGIC | Compute | **reused** between runs (fast iteration) | restarted on recoverable errors, released after the update |
# MAGIC | Retries | **disabled** — see errors immediately | **automatic** retries |
# MAGIC
# MAGIC (Older UIs: a **Development / Production** toggle, and `"development": true` in the pipeline JSON — same meaning.)
# MAGIC
# MAGIC ## 10 · Pipelines everywhere
# MAGIC
# MAGIC * **Standalone** streaming tables & MVs: `CREATE MATERIALIZED VIEW … SCHEDULE EVERY 1 HOUR` / `CREATE STREAMING TABLE …`
# MAGIC   in **Databricks SQL** (or a serverless notebook) — Databricks creates and manages the pipeline for you; `REFRESH
# MAGIC   MATERIALIZED VIEW mv` refreshes on demand (you did this in Section 07).
# MAGIC * **Lakeflow Jobs**: a **pipeline task** runs an update — e.g. *land files → pipeline → refresh dashboard* (Section 10).
# MAGIC * **Lakeflow Connect** managed connectors run **as** pipelines that write streaming tables (Section 05).

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11 · 🕰️ Legacy syntax (recognise it!)
# MAGIC
# MAGIC | Legacy | Today |
# MAGIC |---|---|
# MAGIC | `CREATE LIVE TABLE x AS …` · `CREATE OR REFRESH LIVE TABLE` | `CREATE OR REFRESH MATERIALIZED VIEW x AS …` |
# MAGIC | `CREATE STREAMING LIVE TABLE x` / `CREATE INCREMENTAL LIVE TABLE` | `CREATE OR REFRESH STREAMING TABLE x` |
# MAGIC | `FROM cloud_files('/path', 'json', map('cloudFiles.inferColumnTypes', 'true'))` | `FROM STREAM read_files('/path', format => 'json')` |
# MAGIC | `FROM LIVE.orders` · `FROM STREAM(LIVE.orders)` | `FROM orders` · `FROM STREAM(orders)` |
# MAGIC | `import dlt` · `@dlt.table` · `@dlt.view` · `dlt.read("x")` · `dlt.read_stream("x")` | `from pyspark import pipelines as dp` · `@dp.table` / `@dp.materialized_view` · `@dp.temporary_view` · `spark.read.table("x")` · `spark.readStream.table("x")` |
# MAGIC | target **schema** setting (`target`), `LIVE` virtual schema | default **catalog + schema** (`catalog`, `schema`), direct publishing |
# MAGIC | DLT notebooks as source code | source **files** in the pipeline root folder (notebooks still supported) |
# MAGIC
# MAGIC ## ✅ Key takeaways
# MAGIC 1. **SDP** (formerly **DLT** / Lakeflow Declarative Pipelines) = declare datasets; the pipeline builds the graph and manages
# MAGIC    checkpoints, retries, schema evolution, quality metrics and lineage.
# MAGIC 2. **Streaming table** = incremental, each row once, append-only sources · **materialized view** = always-correct query
# MAGIC    result, incremental refresh · **temporary view** = pipeline-internal.
# MAGIC 3. SQL: `CREATE OR REFRESH STREAMING TABLE … FROM STREAM read_files(…)` / `… FROM STREAM(other)`;
# MAGIC    `CREATE OR REFRESH MATERIALIZED VIEW`. Python: `@dp.table`, `@dp.materialized_view`, `@dp.temporary_view`.
# MAGIC 4. Config values: `${key}` in SQL, `spark.conf.get("key")` in Python.
# MAGIC 5. Updates: **refresh**, **full refresh** (truncate + reprocess; block with `pipelines.reset.allowed = false`), **selective**,
# MAGIC    **dry run**.
# MAGIC 6. **Triggered** vs **continuous**; **development** (reuse compute, no retries) vs **production** (retries, restarts);
# MAGIC    editions **Core < Pro (CDC) < Advanced (expectations)**; channel **current / preview**.
# MAGIC
# MAGIC ➡️ Next: **08-P3 · Expectations, AUTO CDC and Pipeline Monitoring**
