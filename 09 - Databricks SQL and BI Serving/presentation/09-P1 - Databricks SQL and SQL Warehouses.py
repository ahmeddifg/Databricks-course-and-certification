# Databricks notebook source
# MAGIC %md
# MAGIC # 🏛️ 09-P1 · Databricks SQL and SQL Warehouses
# MAGIC **Section 09** · exam objectives **1.2** (compute services: characteristics, limitations, cost — choose the right one),
# MAGIC **2.5** (JDBC/ODBC & REST clients), **6.x** (monitoring queries)
# MAGIC
# MAGIC | After this presentation you can… |
# MAGIC |---|
# MAGIC | Explain what **Databricks SQL** is and who uses it |
# MAGIC | Pick a **SQL warehouse type** (serverless · pro · classic) and explain the differences |
# MAGIC | Size a warehouse: **cluster size** (latency) vs **scaling** (concurrency) vs **auto stop** (cost) |
# MAGIC | Set **warehouse permissions** and explain how they combine with Unity Catalog privileges |
# MAGIC | Explain the three **caches** and how to read **query history** and the **query profile** |
# MAGIC
# MAGIC > ▶️ **Run all** to render the diagrams.

# COMMAND ----------

# MAGIC %run ../../Includes/_style

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · Databricks SQL — the warehouse experience of the lakehouse

# COMMAND ----------

# DBTITLE 1,Slide · Databricks SQL in one picture
show("""
<div class="kicker">Slide 1 · Same data, new audience</div>
<h2>Databricks SQL = SQL warehouses + the tools analysts use on top of them</h2>
<div class="flow">
 <div class="step"><b>🥇 Gold data in Unity Catalog</b>tables · views · materialized views · metric views<br>(Delta, governed)</div><div class="arrow">➜</div>
 <div class="step"><b>⚡ SQL warehouse</b>Photon compute made for SQL<br>serverless · pro · classic</div><div class="arrow">➜</div>
 <div class="step"><b>👩‍💼 Consumers</b>SQL editor · AI/BI dashboards · alerts · Genie · Power BI / Tableau (JDBC/ODBC) · apps (REST) · job SQL tasks</div>
</div>
<div class="grid">
 <div class="card"><h3>🧑‍💻 SQL editor</h3>write, run, parameterize, visualize and save <b>queries</b></div>
 <div class="card green"><h3>📊 AI/BI dashboards</h3>datasets + widgets + filters, publish &amp; share, schedules</div>
 <div class="card orange"><h3>🔔 Alerts</h3>a query + a condition + a schedule → notifications</div>
 <div class="card purple"><h3>💬 Genie</h3>natural-language questions → SQL over curated tables (<b>Genie Agents</b>)</div>
 <div class="card teal"><h3>🔎 Query history &amp; profile</h3>every statement, its source, timings and operators</div>
 <div class="card gray"><h3>🔌 Connectivity</h3>JDBC / ODBC, Partner Connect, Python / Node / Go connectors, Statement Execution API</div>
</div>
""" + callout("info", "No copy into a separate warehouse product: BI users query the <b>same Delta tables</b> that your pipelines "
              "write, with the <b>same Unity Catalog</b> permissions and lineage. That is the lakehouse promise for BI."))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · Which compute for which workload?
# MAGIC
# MAGIC | | **SQL warehouse** | **Serverless** (notebooks / jobs / pipelines) | **All-purpose cluster** | **Jobs cluster** |
# MAGIC |---|---|---|---|---|
# MAGIC | Built for | **SQL** & **BI**: dashboards, ad-hoc SQL, BI tools, alerts | Python + SQL notebooks, jobs, pipelines without managing clusters | interactive development (Python, SQL, Scala, R), libraries, ML | scheduled production jobs |
# MAGIC | Languages | SQL only | Python, SQL | all | all |
# MAGIC | Who connects | SQL editor, dashboards, Genie, **JDBC/ODBC**, REST | notebooks, jobs | notebooks, JDBC/ODBC (possible, not recommended for BI) | the job only |
# MAGIC | Start-up | serverless 2–6 s · pro/classic minutes | seconds | minutes | minutes (per run) |
# MAGIC | Scale | **clusters** for concurrency (min/max) + size per cluster | automatic | autoscaling workers | autoscaling workers |
# MAGIC | Typical exam answer | “analysts / dashboards / BI tool / SQL-only workload” | “no infrastructure to manage, Python + SQL” | “interactive, collaborative, ad hoc” | “cheapest for scheduled production” |
# MAGIC
# MAGIC > ⚠️ **Exam trap:** a **SQL task**, **dashboard task**, **alert task** or **Power BI task** in a Lakeflow Job needs a **SQL
# MAGIC > warehouse** (serverless or pro) — not a cluster.
# MAGIC
# MAGIC ## 3 · Warehouse types

# COMMAND ----------

# DBTITLE 1,Slide · Serverless, pro, classic
show("""
<div class="kicker">Slide 3 · Three types — same SQL, different performance features and plumbing</div>
<h2>Serverless is the default (where available) and the recommendation</h2>
<table class="tbl">
<tr><th></th><th>⚡ Serverless</th><th>Pro</th><th>Classic</th></tr>
<tr><td><b>Compute runs in</b></td><td>the <b>Databricks</b> (serverless) account</td><td>your cloud account</td><td>your cloud account</td></tr>
<tr><td><b>Start-up</b></td><td><b>2–6 seconds</b></td><td>several minutes</td><td>several minutes (~4)</td></tr>
<tr><td><b>Photon engine</b></td><td>✅</td><td>✅</td><td>✅</td></tr>
<tr><td><b>Predictive IO</b> (faster selective scans)</td><td>✅</td><td>✅</td><td>—</td></tr>
<tr><td><b>Intelligent Workload Management</b> (AI-driven queueing &amp; fast scaling)</td><td>✅</td><td>—</td><td>—</td></tr>
<tr><td><b>Default auto stop</b></td><td>10 min (UI min 5, API min 1)</td><td>45 min (min 10)</td><td>45 min (min 10)</td></tr>
<tr><td><b>Pick it when…</b></td><td>almost always: spiky BI, ETL in SQL, exploration</td><td>serverless not available, or custom networking to on-prem / private sources</td><td>entry level, basic needs</td></tr>
</table>
""" + callout("exam", "Remember the feature ladder: <b>classic</b> = Photon · <b>pro</b> = + Predictive IO (pro or serverless is required for "
              "job SQL / dashboard tasks and materialized views) · <b>serverless</b> = + Intelligent Workload Management + seconds to start. "
              "In the UI new warehouses default to serverless; the API defaults to classic unless told otherwise."))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · Size, scaling and auto stop

# COMMAND ----------

# DBTITLE 1,Slide · Latency vs concurrency
show("""
<div class="kicker">Slide 4 · Two different dials</div>
<h2>Cluster <u>size</u> = how fast one query runs · <u>number of clusters</u> = how many run at once</h2>
<div class="grid two">
 <div class="card"><h3>📏 Cluster size (T-shirt)</h3>
 <span class="pill">2X-Small</span><span class="pill">X-Small</span><span class="pill">Small</span><span class="pill">Medium</span>
 <span class="pill">Large</span><span class="pill">X-Large</span><span class="pill">2X-Large</span><span class="pill">3X-Large</span><span class="pill">4X-Large</span>
 <ul><li>more workers per cluster → <b>lower latency</b> for big/complex queries</li>
 <li>more DBUs per hour</li>
 <li>symptom to fix: <b>a single query is slow</b>, spills to disk</li></ul></div>
 <div class="card green"><h3>↔️ Scaling: min / max clusters</h3>
 <ul><li>each cluster runs a batch of queries; rule of thumb <b>~1 cluster per 10 concurrent queries</b></li>
 <li>extra clusters start when queries <b>queue</b>, stop when load drops</li>
 <li>symptom to fix: <b>fast alone, slow at 9 a.m.</b> (queued / “waiting at capacity”)</li></ul></div>
</div>
<div class="flow">
 <div class="step"><b>🐢 one slow query</b>→ bigger <strong>size</strong> (and better data layout — Section 12)</div>
 <div class="step"><b>🚦 queueing under load</b>→ raise <strong>max clusters</strong></div>
 <div class="step"><b>💸 paying while idle</b>→ shorter <strong>auto stop</strong>, serverless</div>
 <div class="step"><b>🧊 slow first query</b>→ serverless (seconds to start), dashboard schedules warm the cache</div>
</div>
""" + callout("trap", "“Add more clusters” does <b>not</b> make one query faster, and “make the cluster bigger” does not fix "
              "queueing caused by many users. Read the symptom carefully."))

# COMMAND ----------

# MAGIC %md
# MAGIC **Other settings** (Edit warehouse → *Advanced options*): **Tags** (key-value pairs that flow into usage/billing data —
# MAGIC chargeback per team), **Channel** (*Current* or *Preview* — test upcoming SQL features), **Unity Catalog** (always on in UC
# MAGIC workspaces). Classic/pro on AWS also have a **spot instance policy** (cost optimized vs reliability optimized).
# MAGIC
# MAGIC ## 5 · Cost model
# MAGIC
# MAGIC | | Serverless SQL warehouse | Pro / classic SQL warehouse |
# MAGIC |---|---|---|
# MAGIC | You pay | DBUs (compute **included**) for the time it runs | DBUs **plus** the cloud VMs in your account |
# MAGIC | Idle | stops after auto stop (default 10 min), restarts in seconds | stops after auto stop (default 45 min), restarts in minutes |
# MAGIC | Levers | size, max clusters, auto stop, tags | the same + instance policy |
# MAGIC | Observe | warehouse **Monitoring** tab · `system.billing.usage` (by warehouse / tags) · `system.query.history` | the same |
# MAGIC
# MAGIC > 💡 The cheapest query is the one never executed: the **result cache** (§7), **materialized views** for dashboard
# MAGIC > aggregates (09-P3) and dashboard **schedules** that warm the cache before people arrive.
# MAGIC
# MAGIC ## 6 · Who may use a warehouse?

# COMMAND ----------

# DBTITLE 1,Slide · Two locks
show("""
<div class="kicker">Slide 6 · Two separate locks: compute and data</div>
<h2>A user needs permission on the <u>warehouse</u> AND privileges on the <u>data</u></h2>
<div class="grid two">
 <div class="card orange"><h3>🔐 Warehouse permissions (workspace ACL)</h3><ul>
 <li><b>Can view</b> — see the warehouse, its query history and profiles; can't run queries (newer workspaces)</li>
 <li><b>Can use</b> — run queries on it ← analysts, BI service principals</li>
 <li><b>Can monitor</b> — use + monitor everyone's queries (history, profiles)</li>
 <li><b>Can manage</b> — edit size/scaling, permissions, delete</li>
 <li><b>Is owner</b> — the creator (transferable)</li></ul>
 Creating a warehouse: <b>workspace admin</b> or users allowed to create unrestricted clusters.</div>
 <div class="card"><h3>🗂️ Unity Catalog privileges</h3><ul>
 <li><code>USE CATALOG</code> on the catalog</li>
 <li><code>USE SCHEMA</code> on the schema</li>
 <li><code>SELECT</code> on tables / views / materialized views</li>
 <li>row filters &amp; column masks still apply per user</li></ul>
 Granted with <code>GRANT … TO `group`</code> — Section 13.</div>
</div>
""" + callout("trap", "“Can use” on a warehouse grants <b>no data access</b>. And <code>SELECT</code> on a table is useless without a "
              "warehouse (or other compute) to run the query on."))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7 · Caching in Databricks SQL
# MAGIC
# MAGIC | Cache | Where | Lifetime | Notes |
# MAGIC |---|---|---|---|
# MAGIC | **UI cache** | per user, in the SQL editor / dashboards | up to 7 days | shows the last result when you reopen a query |
# MAGIC | **Query result cache — local** | per warehouse cluster, in memory | 24 h, or until the cluster stops | identical query text + unchanged tables → no execution |
# MAGIC | **Query result cache — remote** | serverless only, shared by **all** warehouses of the workspace | 24 h, survives restarts | |
# MAGIC | **Disk cache** | local SSDs of the warehouse | while the cluster runs | caches **data files**, not results — speeds up re-reads |
# MAGIC
# MAGIC * Result caches are **invalidated** when an underlying table changes, and not used for non-deterministic queries
# MAGIC   (`current_timestamp()`, `rand()`…).
# MAGIC * Benchmarking? `SET use_cached_result = false`.
# MAGIC * Query history shows whether a result came **from cache** (column `from_result_cache` in `system.query.history`).
# MAGIC
# MAGIC ## 8 · Query history, query profile, monitoring

# COMMAND ----------

# DBTITLE 1,Slide · Following a query
show("""
<div class="kicker">Slide 8 · Every statement leaves a trace</div>
<h2>Queued → compiling → executing → fetching results</h2>
<div class="flow">
 <div class="step"><b>⏳ Waiting for compute</b>warehouse starting</div><div class="arrow">➜</div>
 <div class="step"><b>🚦 Waiting at capacity</b>all clusters busy → scale out</div><div class="arrow">➜</div>
 <div class="step"><b>🧠 Compilation</b>parse, analyze, optimize</div><div class="arrow">➜</div>
 <div class="step"><b>⚙️ Execution</b>tasks on Photon — see the profile</div><div class="arrow">➜</div>
 <div class="step"><b>📤 Result fetching</b>to the client</div>
</div>
<div class="grid">
 <div class="card"><h3>🔎 Query History page</h3>filter by user, warehouse, time, status, <b>source</b> (editor, dashboard, alert, job, Genie, API…)</div>
 <div class="card green"><h3>🧬 Query profile</h3>operator tree (scan → filter → join → aggregate → exchange) with time, rows, memory, spill, files pruned</div>
 <div class="card orange"><h3>📈 Monitoring tab</h3>running vs queued queries and cluster count over time — sizing decisions</div>
 <div class="card purple"><h3>🗄️ <code>system.query.history</code></h3>SQL-queryable history for warehouses, serverless notebooks/jobs and pipelines (admins by default)</div>
</div>
""" + callout("tip", "Same data, three doors: the UI (Query History), the REST API (<code>/api/2.0/sql/history/queries</code>, real time) "
              "and the system table (analytics across days, arrives with some delay)."))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9 · Talking to a warehouse from code
# MAGIC
# MAGIC | Client | How | Typical use |
# MAGIC |---|---|---|
# MAGIC | **JDBC / ODBC driver** | Server hostname + port 443 + **HTTP path** (`/sql/1.0/warehouses/<id>`) + OAuth or token | Power BI, Tableau, Excel, DBeaver, Java apps |
# MAGIC | **Databricks SQL Connector** (Python) / Node.js / Go drivers | same three values | apps and scripts outside Databricks |
# MAGIC | **Statement Execution API** | `POST /api/2.0/sql/statements` (warehouse_id, statement, parameters) → poll → JSON / Arrow results | serverless apps, services, notebooks orchestrated by jobs |
# MAGIC | **SQL task** in Lakeflow Jobs | a saved query, a `.sql` file or an alert, on a warehouse | scheduled SQL |
# MAGIC
# MAGIC Parameters are sent **separately** from the SQL text (named markers `:name`) — never concatenate user input into SQL.
# MAGIC
# MAGIC ## 10 · 🆓 Free Edition & 🕰️ legacy names
# MAGIC
# MAGIC | Free Edition | |
# MAGIC |---|---|
# MAGIC | Warehouses | **one** SQL warehouse, size **2X-Small** (serverless) |
# MAGIC | Everything else in this section | SQL editor, dashboards, alerts, query history work |
# MAGIC
# MAGIC | 🕰️ Legacy term | Today |
# MAGIC |---|---|
# MAGIC | **SQL endpoint** | **SQL warehouse** (renamed in 2022 — still appears in old questions and APIs: `/sql/endpoints`) |
# MAGIC | Databricks SQL *Analytics* / *SQL persona* | Databricks SQL inside the unified workspace |
# MAGIC | *Legacy dashboards* (built from query visualizations) | **AI/BI dashboards** (formerly *Lakeview*) — 09-P2 |
# MAGIC | Databricks Assistant | **Genie Code** (renamed 2026) |
# MAGIC
# MAGIC ## ✅ Key takeaways
# MAGIC 1. **Databricks SQL** serves gold data to analysts and BI tools through **SQL warehouses** — same Delta tables, same Unity
# MAGIC    Catalog.
# MAGIC 2. Types: **serverless** (seconds to start, IWM, compute in Databricks' account — default & recommended) · **pro** (Predictive
# MAGIC    IO, your account, custom networking) · **classic** (Photon only).
# MAGIC 3. **Size** fixes slow single queries; **max clusters** fixes queueing (≈ 10 concurrent queries per cluster); **auto stop**
# MAGIC    fixes idle cost (serverless 10 min default).
# MAGIC 4. Users need **Can use** on the warehouse **and** UC privileges (`USE CATALOG`, `USE SCHEMA`, `SELECT`).
# MAGIC 5. Result cache: 24 h, invalidated on table change; local + remote (serverless); `SET use_cached_result = false`.
# MAGIC 6. Query history (UI / API / `system.query.history`) + **query profile** show where time goes.
# MAGIC
# MAGIC ➡️ Next: **09-P2 · Queries, Dashboards and Alerts**
