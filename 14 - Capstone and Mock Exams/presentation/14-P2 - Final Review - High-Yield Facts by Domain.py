# Databricks notebook source
# MAGIC %md
# MAGIC # ⚡ 14-P2 · Final Review — High-Yield Facts by Domain
# MAGIC **Section 14** · the night-before-the-exam deck: the facts, defaults and traps that decide most questions, one domain at
# MAGIC a time (May 2026 exam guide). Each slide ends with the **traps** that wrong answers are built from.
# MAGIC
# MAGIC | Domain | Approx. weight | Course sections |
# MAGIC |---|---|---|
# MAGIC | D1 Databricks Intelligence Platform | ~6 % | 01, 03, 04 |
# MAGIC | D2 Data Ingestion and Loading | ~21 % | 05, 06, 08 |
# MAGIC | D3 Data Transformation and Modeling | ~22 % | 02, 07, 08, 09 |
# MAGIC | D4 Working with Lakeflow Jobs | ~16 % | 10 |
# MAGIC | D5 Implementing CI/CD | ~10 % | 11, 14-L4 |
# MAGIC | D6 Troubleshooting, Monitoring and Optimization | ~10 % | 12, 14-L3/L4 |
# MAGIC | D7 Governance and Security | ~15 % | 13 |
# MAGIC
# MAGIC > ▶️ **Run all** to render the slides.

# COMMAND ----------

# MAGIC %run ../../Includes/_style

# COMMAND ----------

# MAGIC %md
# MAGIC ## D1 · Databricks Intelligence Platform

# COMMAND ----------

# DBTITLE 1,D1 · Architecture, Delta Lake, Unity Catalog
show("""
<div class="kicker">D1 · Architecture</div>
<div class="grid">
 <div class="card"><h3>🏛️ Lakehouse</h3>open formats (Delta, Iceberg) on cheap <b>cloud object storage</b> + ACID + one governance layer (Unity Catalog) for BI, ETL, streaming and AI</div>
 <div class="card green"><h3>🧭 Control plane vs compute plane</h3><b>control</b> (Databricks account): web app, notebooks, jobs scheduler, cluster manager, UC metadata · <b>compute</b>: classic = VMs in <b>your</b> cloud account; serverless = in <b>Databricks'</b> account · <b>data</b> stays in your storage</div>
 <div class="card orange"><h3>🔺 Delta Lake</h3>Parquet data files + <code>_delta_log</code> (JSON commit per transaction, Parquet checkpoints) → ACID, schema enforcement, time travel, MERGE/UPDATE/DELETE, deletion vectors</div>
 <div class="card purple"><h3>🗂️ Unity Catalog</h3>metastore (one per region) → <b>catalog</b> → <b>schema</b> → table · view · volume · function · model · identities at the <b>account</b> · lineage, audit, fine-grained access</div>
</div>
""" + callout("trap", "DBFS root and <code>/mnt</code> mounts are <b>legacy</b> — files go in <b>volumes</b> (<code>/Volumes/c/s/v</code>). <code>hive_metastore</code> is the legacy per-workspace metastore, visible as a catalog."))

# COMMAND ----------

# DBTITLE 1,D1 · Compute
show("""
<div class="kicker">D1 · Compute: characteristics, limits, cost, selection</div>
<table class="tbl">
<tr><th>Compute</th><th>For</th><th>Key facts</th></tr>
<tr><td><b>Serverless</b> (notebooks, jobs, pipelines)</td><td>default choice</td><td>starts in seconds, auto-scaled and updated by Databricks, VM cost included in the DBU price · Python &amp; SQL · no RDD API / init scripts / most Spark confs · <code>availableNow</code> streaming only · jobs: <i>performance optimized</i> vs <i>standard</i> (cheaper, slower start)</td></tr>
<tr><td><b>All-purpose</b> (classic)</td><td>interactive development</td><td>shared by users, stays up until auto-termination · most expensive DBU rate for jobs</td></tr>
<tr><td><b>Jobs compute</b> (classic)</td><td>scheduled production jobs</td><td>created for the run, terminated after it · <b>cheaper</b> than all-purpose · isolation per job</td></tr>
<tr><td><b>SQL warehouse</b></td><td>SQL, dashboards, BI tools</td><td>serverless (instant, recommended) · pro · classic · auto stop · scaling by clusters</td></tr>
<tr><td>Pools</td><td>faster classic start</td><td>keep idle instances ready</td></tr>
<tr><td>Spot instances</td><td>cheap workers</td><td>can be reclaimed — keep the <b>driver on-demand</b></td></tr>
</table>
<div class="grid two">
 <div class="card"><h3>⚙️ Classic options</h3>single node vs multi node · <b>LTS</b> runtime for production · <b>Photon</b> (vectorized engine: faster SQL/DataFrame) · access mode <b>standard</b> (shared, UC) vs <b>dedicated</b> (single user/group, UC) · autoscaling · auto-termination</div>
 <div class="card orange"><h3>💰 Cost model</h3>DBU (Databricks Unit) per hour × rate of the compute type/tier (+ cloud VM cost for classic). Cut cost: serverless or jobs compute for jobs, auto-termination, spot workers, right-size, autoscale</div>
</div>
""" + callout("trap", "“Cheapest way to run a nightly production job on classic compute?” → <b>jobs compute</b>, not an all-purpose cluster. “Start in seconds, no infrastructure to manage?” → <b>serverless</b>."))

# COMMAND ----------

# MAGIC %md
# MAGIC ## D2 · Data Ingestion and Loading

# COMMAND ----------

# DBTITLE 1,D2 · Patterns and tools
show("""
<div class="kicker">D2 · Choose the ingestion method</div>
<table class="tbl">
<tr><th>Need</th><th>Answer</th></tr>
<tr><td>new files keep arriving in cloud storage, millions, schema may change</td><td><b>Auto Loader</b> (<code>cloudFiles</code>) — or a streaming table <code>FROM STREAM read_files()</code></td></tr>
<tr><td>SQL, re-runnable, thousands of files, occasional loads</td><td><b>COPY INTO</b> (idempotent: skips loaded files)</td></tr>
<tr><td>one-off load / exploration</td><td>CTAS + <code>read_files()</code> or <code>format.`path`</code></td></tr>
<tr><td>SaaS app (Salesforce, Workday, ServiceNow, Google Analytics…) or database CDC (SQL Server…) with minimal code</td><td><b>Lakeflow Connect managed connectors</b> (connection → gateway for DBs → serverless pipeline → streaming tables)</td></tr>
<tr><td>Kafka / Kinesis / Event Hubs</td><td>Structured Streaming source / standard connectors</td></tr>
<tr><td>apps push events directly</td><td>Zerobus (push API) · REST (Statement Execution API)</td></tr>
<tr><td>external database queried in place</td><td>Lakehouse Federation (connection + foreign catalog); JDBC/ODBC clients connect <b>to</b> SQL warehouses</td></tr>
<tr><td>a spreadsheet, once</td><td>UI upload (create table / upload to volume)</td></tr>
</table>
<p class="muted">Loading patterns: <b>batch</b> (everything each time) · <b>incremental batch</b> (only new data, scheduled — COPY INTO, Auto Loader <code>availableNow</code>) · <b>streaming</b> (continuous, low latency).</p>
""")

# COMMAND ----------

# DBTITLE 1,D2 · Auto Loader and COPY INTO facts
show("""
<div class="kicker">D2 · Auto Loader &amp; COPY INTO — the details that get asked</div>
<div class="grid two">
 <div class="card"><h3>⚡ Auto Loader</h3><ul>
 <li><code>cloudFiles.format</code> · <code>cloudFiles.schemaLocation</code> (required for inference/evolution) · <code>checkpointLocation</code></li>
 <li>JSON/CSV/XML columns inferred as <b>STRING</b> unless <code>cloudFiles.inferColumnTypes = true</code>; <code>schemaHints</code> fix some types</li>
 <li><code>schemaEvolutionMode</code>: <b>addNewColumns</b> (default: stream stops once, restart continues) · rescue · failOnNewColumns · none (default when you give a schema)</li>
 <li><code>_rescued_data</code> keeps values that don't fit the schema</li>
 <li>file discovery: directory listing (default) or file notification / <b>file events</b> (scales better)</li>
 <li><code>trigger(availableNow=True)</code> → incremental batch</li></ul></div>
 <div class="card orange"><h3>📥 COPY INTO</h3><ul>
 <li><code>COPY INTO t FROM 'path' FILEFORMAT = CSV|JSON|PARQUET|… FORMAT_OPTIONS (…) COPY_OPTIONS (…)</code></li>
 <li>idempotent — tracks loaded files in the table; <code>'force' = 'true'</code> reloads</li>
 <li><code>'mergeSchema' = 'true'</code> (in both option groups for a schemaless target)</li>
 <li><code>FILES = (…)</code> or <code>PATTERN = '…'</code> to select files; <code>VALIDATE</code> to preview</li>
 <li>can transform with <code>FROM (SELECT …, _metadata.file_name FROM 'path')</code></li></ul></div>
</div>
""" + callout("trap", "<code>csv.`path`</code> can't take options (header becomes a row, columns <code>_c0…</code>) → use <code>read_files(…, header =&gt; true)</code>. <code>input_file_name</code> → <code>_metadata.file_path</code>. Auto Loader without a checkpoint can't remember files."))

# COMMAND ----------

# DBTITLE 1,D2 · Semi-structured and unstructured
show("""
<div class="kicker">D2 · Semi-structured &amp; unstructured data</div>
<table class="tbl">
<tr><th>Data</th><th>How</th></tr>
<tr><td>JSON string column</td><td><code>col:field:sub::TYPE</code> · <code>from_json(col, schema)</code> + <code>schema_of_json('sample')</code> · <code>struct.*</code> · <code>get_json_object</code></td></tr>
<tr><td>VARIANT</td><td><code>parse_json()</code>, <code>v:path::TYPE</code>, flexible and fast; cast before comparing/sorting; <code>singleVariantColumn</code> to load whole records</td></tr>
<tr><td>arrays</td><td><code>explode</code> / <code>explode_outer</code>, <code>collect_list</code> / <code>collect_set</code>, <code>size</code>, <code>array_contains</code>, <code>flatten</code>, <code>array_distinct</code>, HOFs <code>transform</code> / <code>filter</code> / <code>exists</code> / <code>aggregate</code></td></tr>
<tr><td>images, PDFs, audio, text</td><td>store in a <b>volume</b>; <code>binaryFile</code> (path, modificationTime, length, content) · <code>text</code> / <code>wholeText</code></td></tr>
</table>
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## D3 · Data Transformation and Modeling

# COMMAND ----------

# DBTITLE 1,D3 · Transformations
show("""
<div class="kicker">D3 · Cleaning, combining, deduplicating, aggregating</div>
<div class="grid">
 <div class="card"><h3>🧹 Cleaning</h3><code>trim/lower</code>, casts (<code>try_cast</code> under ANSI), <code>coalesce</code>, <code>fillna</code>/<code>dropna</code>, filter invalid rows → quarantine with a reason</div>
 <div class="card green"><h3>🔗 Joins</h3>inner · left/right · full · <b>left semi</b> (exists) · <b>left anti</b> (orphans) · cross (A×B) · broadcast small tables · multi-key conditions</div>
 <div class="card orange"><h3>➕ Set operations</h3><code>UNION</code> (distinct) vs <code>UNION ALL</code> · <code>INTERSECT</code> · <code>EXCEPT</code>/<code>MINUS</code> · PySpark <code>union</code> = by <b>position</b> + keeps dups; <code>unionByName(allowMissingColumns=True)</code></div>
 <div class="card purple"><h3>🔁 Dedup</h3><code>DISTINCT</code> / <code>distinct()</code> = whole row · <code>dropDuplicates(["id"])</code> = arbitrary row per id · latest per key = <code>row_number() … = 1</code> · across loads = insert-only <code>MERGE</code></div>
 <div class="card teal"><h3>📊 Aggregates</h3><code>count(*)</code> vs <code>count(col)</code> (skips NULLs) · <code>count_if</code> · <code>approx_count_distinct</code> · <code>GROUP BY ALL</code> · <code>HAVING</code> · ROLLUP/CUBE · PIVOT</div>
 <div class="card red"><h3>🪟 Windows</h3><code>row_number</code> (1,2,3) · <code>rank</code> (1,1,3) · <code>dense_rank</code> (1,1,2) · <code>lag/lead</code> · running totals · <code>QUALIFY</code></div>
</div>
""")

# COMMAND ----------

# DBTITLE 1,D3 · Writing and table structure
show("""
<div class="kicker">D3 · Writing data — which statement?</div>
<table class="tbl">
<tr><th>Statement</th><th>Effect</th><th>Trap</th></tr>
<tr><td><code>CREATE OR REPLACE TABLE … AS</code></td><td>new definition + data, history kept</td><td>can change the schema</td></tr>
<tr><td><code>INSERT OVERWRITE</code></td><td>replace data, keep definition (grants, masks, clustering)</td><td>fails if the schema differs</td></tr>
<tr><td><code>INSERT INTO</code></td><td>append</td><td>re-running = duplicates</td></tr>
<tr><td><code>MERGE INTO</code></td><td>upsert / delete / insert-only, atomic</td><td>fails if several source rows match one target row</td></tr>
<tr><td><code>COPY INTO</code></td><td>append new files only</td><td>—</td></tr>
<tr><td><code>df.write.mode(…)</code></td><td><b>errorifexists</b> (default) · append · overwrite · ignore</td><td>new columns need <code>mergeSchema</code>; new schema needs <code>overwriteSchema</code></td></tr>
</table>
<div class="grid two">
 <div class="card"><h3>🏗️ Table structure</h3><code>ALTER TABLE … ADD COLUMNS</code>, <code>ADD CONSTRAINT … CHECK</code>, <code>ALTER COLUMN … SET NOT NULL</code>, generated &amp; identity columns (CREATE time only), <code>CLUSTER BY</code>, comments, <code>RENAME TO</code></div>
 <div class="card orange"><h3>⚙️ Tuning &amp; measuring</h3><code>spark.sql.shuffle.partitions</code> (200; <code>auto</code> with AQE) · <code>spark.sql.autoBroadcastJoinThreshold</code> (10 MB) · <code>spark.sql.files.maxPartitionBytes</code> (128 MB) · executor/driver memory · measure with the <b>query profile</b> / Spark UI and compare durations</div>
</div>
""")

# COMMAND ----------

# DBTITLE 1,D3 · Gold objects and data quality
show("""
<div class="kicker">D3 · Gold layer objects &amp; data quality</div>
<table class="tbl">
<tr><th>Object</th><th>Stores data</th><th>Freshness</th><th>Typical use</th></tr>
<tr><td>view</td><td>no</td><td>always current</td><td>light logic, security layer, single definition of a metric</td></tr>
<tr><td>materialized view</td><td>yes</td><td>as of last refresh (incremental when possible; <code>SCHEDULE</code>, job, pipeline)</td><td>expensive aggregations read often</td></tr>
<tr><td>streaming table</td><td>yes</td><td>appended incrementally</td><td>append-only sources, ingestion, CDC targets</td></tr>
<tr><td>table</td><td>yes</td><td>as of last write</td><td>full control (CTAS, MERGE)</td></tr>
<tr><td>temp view</td><td>no</td><td>session only</td><td>intermediate steps</td></tr>
</table>
<div class="grid two">
 <div class="card"><h3>✅ Quality checks</h3>Delta <code>CHECK</code>/<code>NOT NULL</code> constraints (write fails) · pipeline <b>expectations</b>: <code>EXPECT</code> (warn) · <code>ON VIOLATION DROP ROW</code> · <code>ON VIOLATION FAIL UPDATE</code> (metrics in the event log) · quarantine tables · reconciliation (anti joins, counts)</div>
 <div class="card orange"><h3>⭐ Modeling</h3>star schema: fact (events, measures, keys) + dimensions (attributes) · SCD 1 (overwrite) vs SCD 2 (history rows: valid_from/valid_to/is_current) · <code>AUTO CDC … STORED AS SCD TYPE 2</code> in pipelines</div>
</div>
""" + callout("trap", "<code>dropDuplicates()</code> does not choose the <b>latest</b> row. <code>INSERT OVERWRITE</code> cannot change the schema. A standard view recomputes every time — a dashboard hammering it wants a <b>materialized view</b>."))

# COMMAND ----------

# MAGIC %md
# MAGIC ## D4 · Lakeflow Jobs

# COMMAND ----------

# DBTITLE 1,D4 · Tasks, control flow, triggers
show("""
<div class="kicker">D4 · Jobs — DAG, control flow, triggers</div>
<div class="grid">
 <div class="card"><h3>🧩 Tasks</h3>notebook · Python script/wheel · JAR · SQL (query/file/alert/dashboard) · <b>pipeline</b> · dbt · Power BI · <b>Run Job</b> · <b>If/else</b> · <b>For each</b> · up to 1,000 tasks</div>
 <div class="card green"><h3>🔗 Dependencies</h3><i>Depends on</i> builds the DAG; tasks without dependencies run in parallel · <b>Run if</b>: All succeeded (default) · At least one succeeded · None failed · All done · At least one failed · All failed</div>
 <div class="card orange"><h3>🔀 Control flow</h3><b>If/else</b>: <code>==</code> <code>!=</code> (string) and <code>&gt; &gt;= &lt; &lt;=</code> (numeric) on task values / job parameters · <b>For each</b>: loop over a JSON array, <code>{{input}}</code>, concurrency (default 1) · <b>retries</b> per task (count, interval, on timeout)</div>
 <div class="card purple"><h3>🎛️ Parameters</h3>job parameters (pushed to every task, win over task params) · task values (<code>dbutils.jobs.taskValues</code>) · dynamic references <code>{{job.id}}</code>, <code>{{job.start_time.iso_date}}</code>, <code>{{task.execution_count}}</code>, <code>{{tasks.t.values.k}}</code></div>
 <div class="card teal"><h3>⏰ Triggers</h3><b>time-based</b>: schedule (Quartz cron, time zone) · <b>data-driven</b>: <b>file arrival</b> (UC volume / external location), <b>table update</b> (UC tables) · <b>continuous</b> (always one run) · manual / API</div>
 <div class="card red"><h3>🔧 Operations</h3><b>Repair run</b> = re-run failed/skipped tasks + dependents · Run now with different parameters · max concurrent runs (default 1) + queueing · timeouts, duration warnings, notifications · run as a service principal</div>
</div>
""" + callout("trap", "An If/else branch not taken is <b>Excluded</b> (the run still succeeds). A file-arrival trigger fires on <b>new</b> files only. A schedule runs even if no data arrived — data-driven triggers don't."))

# COMMAND ----------

# MAGIC %md
# MAGIC ## D5 · CI/CD

# COMMAND ----------

# DBTITLE 1,D5 · Git folders, bundles, CLI
show("""
<div class="kicker">D5 · Git folders, Declarative Automation Bundles, CLI</div>
<div class="grid two">
 <div class="card"><h3>🌿 Git folders</h3><ul>
 <li>clone a remote repo (GitHub, GitLab, Azure DevOps, Bitbucket…) into the workspace</li>
 <li>create/switch <b>branches</b>, pull, commit &amp; push, merge, rebase, resolve conflicts, reset — in the UI</li>
 <li>credentials: personal access token or the GitHub app; notebooks stored as <b>source files</b></li>
 <li>PRs and code review happen in the Git provider; production pulls the main branch (e.g. via CI or the Repos API)</li></ul></div>
 <div class="card orange"><h3>📦 Declarative Automation Bundles <span class="pill gray">formerly Databricks Asset Bundles</span></h3><ul>
 <li><code>databricks.yml</code>: <code>bundle</code>, <code>include</code>, <code>variables</code>, <code>resources</code> (jobs, pipelines, dashboards…), <code>targets</code></li>
 <li>targets <code>dev</code> (<code>mode: development</code>: prefixed names, paused schedules) / <code>prod</code> (<code>mode: production</code>, run_as SP)</li>
 <li>variables <code>${var.catalog}</code>; override per target, with <code>--var="k=v"</code> or <code>BUNDLE_VAR_k</code></li></ul></div>
</div>
<table class="tbl">
<tr><th>CLI</th><th>Does</th></tr>
<tr><td><code>databricks bundle init</code></td><td>new bundle from a template</td></tr>
<tr><td><code>databricks bundle validate -t dev</code></td><td>check config, resolve variables</td></tr>
<tr><td><code>databricks bundle deploy -t prod</code></td><td>upload files, create/update resources in the target</td></tr>
<tr><td><code>databricks bundle run -t dev &lt;resource_key&gt;</code></td><td>run a deployed job/pipeline</td></tr>
<tr><td><code>databricks bundle destroy -t dev</code></td><td>remove what the bundle deployed</td></tr>
<tr><td><code>databricks auth login</code> · profiles in <code>~/.databrickscfg</code></td><td>authenticate (OAuth); CI uses a <b>service principal</b></td></tr>
</table>
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## D6 · Troubleshooting, Monitoring and Optimization

# COMMAND ----------

# DBTITLE 1,D6 · Monitoring and bottlenecks
show("""
<div class="kicker">D6 · Find the problem</div>
<table class="tbl">
<tr><th>Question</th><th>Where</th></tr>
<tr><td>is the job getting slower? which task fails intermittently?</td><td>job <b>Runs</b> tab — <b>matrix view</b> (run × task, durations, colours) · run history · <code>system.lakeflow.job_run_timeline</code></td></tr>
<tr><td>is the pipeline healthy? data quality?</td><td>pipeline UI graph (rows, expectations) · <b>event log</b> <code>event_log(TABLE(t))</code> · Lakeflow Jobs UI for pipeline tasks</td></tr>
<tr><td>why is this query/stage slow?</td><td><b>query profile</b> (serverless, SQL warehouses) · <b>Spark UI</b> stages/tasks (classic)</td></tr>
<tr><td>why won't the cluster start?</td><td>compute <b>event log</b> · driver logs</td></tr>
</table>
<div class="grid">
 <div class="card red"><h3>📐 Data skew</h3>one task far longer than the others in a stage → AQE skew-join handling, salting, better keys</div>
 <div class="card orange"><h3>💾 Disk spill</h3>memory too small for the partition → bigger memory / more partitions / less data per task</div>
 <div class="card purple"><h3>🔀 Shuffle</h3>large Exchange read/write → broadcast small tables, filter early, cluster data on join keys</div>
 <div class="card teal"><h3>🚦 Startup failures</h3>cloud quota or instance type unavailable, spot capacity, init script errors, network/permissions, policy limits → change instance type, on-demand, fix script, serverless</div>
</div>
""")

# COMMAND ----------

# DBTITLE 1,D6 · Layout optimization
show("""
<div class="kicker">D6 · Liquid clustering &amp; predictive optimization</div>
<div class="grid two">
 <div class="card green"><h3>💧 Liquid clustering</h3><ul>
 <li><code>CREATE TABLE … CLUSTER BY (a, b)</code> / <code>ALTER TABLE … CLUSTER BY (…)</code> / <code>CLUSTER BY AUTO</code> / <code>CLUSTER BY NONE</code></li>
 <li><b>incremental</b> (OPTIMIZE clusters only what's new), keys can change without rewriting</li>
 <li>replaces partitioning + Z-order for most tables; <b>not combinable</b> with them</li>
 <li>choose keys used in filters/joins; up to 4 keys</li></ul></div>
 <div class="card"><h3>🤖 Predictive optimization</h3><ul>
 <li>Databricks runs <code>OPTIMIZE</code>, <code>VACUUM</code>, <code>ANALYZE</code> automatically on <b>Unity Catalog managed</b> tables</li>
 <li>enabled at account/catalog/schema level (default on for many accounts)</li>
 <li>with <code>CLUSTER BY AUTO</code> it also picks clustering keys from the query history</li></ul></div>
</div>
""" + callout("trap", "Z-order is <b>not incremental</b> (re-sorts everything each OPTIMIZE). Partition only very large tables on a low-cardinality column (≥ ~1 GB per partition) — over-partitioning creates the small-file problem."))

# COMMAND ----------

# MAGIC %md
# MAGIC ## D7 · Governance and Security

# COMMAND ----------

# DBTITLE 1,D7 · Tables, privileges, fine-grained access
show("""
<div class="kicker">D7 · Managed vs external · GRANT / REVOKE / DENY · masks, filters, ABAC</div>
<div class="grid two">
 <div class="card"><h3>📦 Managed vs external</h3>managed: UC chooses the storage, <code>DROP</code> deletes data (UNDROP within 7 days) · external: <code>LOCATION</code> in an <b>external location</b> (storage credential), <code>DROP</code> keeps files · convert: <code>ALTER TABLE … SET MANAGED</code> · non-Delta formats only external</div>
 <div class="card orange"><h3>🔑 Privileges</h3>reach: <code>USE CATALOG</code> + <code>USE SCHEMA</code> · read: <code>SELECT</code> · write: <code>MODIFY</code> · create: <code>CREATE TABLE/VOLUME/FUNCTION/SCHEMA</code> · <code>EXECUTE</code>, <code>READ/WRITE VOLUME</code>, <code>BROWSE</code> · <code>ALL PRIVILEGES</code> · <code>MANAGE</code> · owner = full control · inherited downwards to current <b>and future</b> objects</div>
</div>
<table class="tbl">
<tr><th>Mechanism</th><th>How</th><th>Protects</th></tr>
<tr><td>dynamic view</td><td><code>CASE WHEN is_account_group_member('g')</code> / <code>current_user()</code> in a view</td><td>only readers of the view</td></tr>
<tr><td>row filter</td><td><code>ALTER TABLE t SET ROW FILTER f ON (col)</code> — BOOLEAN UDF</td><td>every query on the table (owners too)</td></tr>
<tr><td>column mask</td><td><code>ALTER TABLE t ALTER COLUMN c SET MASK f [USING COLUMNS (…)]</code></td><td>every query on the column</td></tr>
<tr><td>ABAC policy</td><td>governed <b>tags</b> + <code>CREATE POLICY … ROW FILTER | COLUMN MASK … MATCH COLUMNS has_tag_value(…)</code> on catalog/schema</td><td>all matching tables, current and future, centrally</td></tr>
</table>
""" + callout("trap", "<b>DENY</b> exists only for legacy <code>hive_metastore</code> table ACLs — Unity Catalog is <b>allow-only</b>: remove access with <code>REVOKE</code> at the level where it was granted. <code>SELECT</code> without <code>USE CATALOG</code>/<code>USE SCHEMA</code> is useless."))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔁 The 20 traps that cost the most points

# COMMAND ----------

# DBTITLE 1,Trap list
show("""
<div class="kicker">Read this list the morning of the exam</div>
<table class="tbl">
<tr><th>#</th><th>Trap</th><th>Truth</th></tr>
<tr><td>1</td><td>“INSERT INTO is safe to re-run”</td><td>it appends duplicates — use MERGE / COPY INTO / Auto Loader</td></tr>
<tr><td>2</td><td>“VACUUM keeps all history”</td><td>it deletes unreferenced files older than the retention (7 days) → no time travel beyond</td></tr>
<tr><td>3</td><td>“DROP TABLE on an external table deletes the files”</td><td>only metadata</td></tr>
<tr><td>4</td><td>“UC has DENY”</td><td>allow-only; REVOKE</td></tr>
<tr><td>5</td><td>“GRANT SELECT is enough”</td><td>plus USE CATALOG + USE SCHEMA</td></tr>
<tr><td>6</td><td>“A view stores data”</td><td>no — a materialized view does</td></tr>
<tr><td>7</td><td>“Temp views survive the session / are shared”</td><td>session-scoped</td></tr>
<tr><td>8</td><td>“Auto Loader infers JSON numbers as numbers”</td><td>strings unless <code>inferColumnTypes</code></td></tr>
<tr><td>9</td><td>“addNewColumns evolves silently”</td><td>the stream stops once, restart continues</td></tr>
<tr><td>10</td><td>“Two streams can share a checkpoint”</td><td>never</td></tr>
<tr><td>11</td><td>“EXPECT without ON VIOLATION drops rows”</td><td>it only warns (counts)</td></tr>
<tr><td>12</td><td>“dropDuplicates keeps the newest row”</td><td>arbitrary — use row_number</td></tr>
<tr><td>13</td><td>“union matches columns by name”</td><td>by position — unionByName</td></tr>
<tr><td>14</td><td>“INSERT OVERWRITE can change the schema”</td><td>no — CREATE OR REPLACE / overwriteSchema</td></tr>
<tr><td>15</td><td>“Z-order is incremental”</td><td>liquid clustering is</td></tr>
<tr><td>16</td><td>“Repair run re-runs the whole job”</td><td>only failed/skipped tasks + dependents</td></tr>
<tr><td>17</td><td>“Task parameters override job parameters”</td><td>job parameters win for the same key</td></tr>
<tr><td>18</td><td>“All-purpose clusters are cheapest for jobs”</td><td>jobs compute / serverless</td></tr>
<tr><td>19</td><td>“Row filters don't apply to the table owner”</td><td>they apply to everyone; exemptions go inside the function</td></tr>
<tr><td>20</td><td>“A shallow clone is an independent backup”</td><td>it references the source's files — use DEEP CLONE</td></tr>
</table>
""")
