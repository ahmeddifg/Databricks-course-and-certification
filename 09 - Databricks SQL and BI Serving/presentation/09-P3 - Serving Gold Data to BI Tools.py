# Databricks notebook source
# MAGIC %md
# MAGIC # 🔌 09-P3 · Serving Gold Data to BI Tools
# MAGIC **Section 09** · exam objectives **3.6** (gold objects *“for BI and analytics teams”*), **2.5** (JDBC/ODBC & REST clients),
# MAGIC **4.2** (SQL / dashboard / Power BI tasks), **7.x** (who may see what)
# MAGIC
# MAGIC | After this presentation you can… |
# MAGIC |---|
# MAGIC | Design a gold layer that BI tools and humans understand (grain, star schema, names, comments, keys) |
# MAGIC | Choose between **table, view, materialized view, streaming table and metric view** for a BI use case |
# MAGIC | Keep materialized views fresh: `REFRESH`, `SCHEDULE`, `TRIGGER ON UPDATE`, job tasks |
# MAGIC | Connect **Power BI, Tableau, JDBC/ODBC clients and apps** — what they need and how they authenticate |
# MAGIC | Grant the **minimum permissions** a BI consumer needs, and monitor BI usage |
# MAGIC
# MAGIC > ▶️ **Run all** to render the diagrams.

# COMMAND ----------

# MAGIC %run ../../Includes/_style

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · From gold tables to decisions

# COMMAND ----------

# DBTITLE 1,Slide · Serving architecture
show("""
<div class="kicker">Slide 1 · The last mile of the medallion architecture</div>
<h2>Gold is designed for its <u>consumers</u>, not for the pipeline</h2>
<div class="layer" style="background:#b08900">🥇 <b>Gold — serving layer</b> <small>star schema (facts + dimensions) · pre-aggregates / materialized views ·
metric views · business names &amp; comments · PK/FK</small></div>
<div class="layer" style="background:#868e96">🥈 <b>Silver</b> <small>clean, conformed, deduplicated entities (Sections 07–08)</small></div>
<div class="flow">
 <div class="step"><b>⚡ SQL warehouse</b>serverless · auto stop · scaling</div><div class="arrow">➜</div>
 <div class="step"><b>📊 Databricks-native</b>AI/BI dashboards · Genie Agents · alerts · SQL editor</div>
 <div class="step"><b>🧩 External BI</b>Power BI · Tableau · Looker · Excel (JDBC/ODBC)</div>
 <div class="step"><b>🧑‍💻 Apps &amp; services</b>SQL connectors · Statement Execution API · Databricks Apps</div>
</div>
""" + callout("tip", "Good gold layer checklist: one clear <b>grain</b> per table · <b>star schema</b> (facts with keys + descriptive "
              "dimensions) · business-friendly names · <b>comments</b> on tables and columns · informational <b>PK/FK</b> · "
              "pre-aggregates only where dashboards need speed · one definition per KPI (<b>metric view</b>)."))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · Which gold object?
# MAGIC
# MAGIC | | **Table** (Delta) | **View** | **Materialized view** | **Streaming table** | **Metric view** |
# MAGIC |---|---|---|---|---|---|
# MAGIC | Stores data | ✅ | ❌ (a saved query) | ✅ precomputed result | ✅ | ❌ (definitions; can be materialized) |
# MAGIC | Freshness | as of the last write by your job | **always current** | as of the last **refresh** | as of the last update (incremental) | as its source |
# MAGIC | Read cost | low | the full query **every read** | low | low | the generated query |
# MAGIC | Who writes it | your code (`INSERT`, `MERGE`, SCD2…) | nobody | the MV's pipeline (refresh, **incremental** when possible) | its pipeline / flow | nobody |
# MAGIC | Created in | anywhere | anywhere | SQL warehouse (pro/serverless), serverless notebook or SDP pipeline | SQL warehouse or SDP pipeline | SQL (YAML) or Catalog Explorer |
# MAGIC | Best for BI when… | you need custom write logic or history (SCD2) | small/medium data, must be live, light logic | **dashboard aggregates** read often, changed rarely | ingestion — rarely read by BI directly | the **same KPI** must mean the same everywhere |
# MAGIC
# MAGIC > ⚠️ **Exam traps.** *“Always the latest data, cheap to store, the query is light”* → **view**. *“Expensive aggregation read
# MAGIC > by many dashboards, refreshed nightly”* → **materialized view**. *“Each source row processed once, append-only”* →
# MAGIC > **streaming table**. A view adds **no** speed: it runs its query every time.
# MAGIC
# MAGIC ## 3 · Materialized views in Databricks SQL
# MAGIC
# MAGIC ```sql
# MAGIC CREATE MATERIALIZED VIEW gold.daily_category_sales
# MAGIC   SCHEDULE EVERY 1 DAY                                   -- or: SCHEDULE CRON '0 0 6 * * ?' AT TIME ZONE 'Asia/Riyadh'
# MAGIC                                                          -- or: TRIGGER ON UPDATE [AT MOST EVERY INTERVAL 1 HOUR]
# MAGIC   COMMENT 'Revenue per day and category for the sales dashboard'
# MAGIC AS SELECT order_date, category, count(DISTINCT order_id) AS orders, sum(subtotal) AS revenue
# MAGIC    FROM silver.order_items GROUP BY ALL;
# MAGIC
# MAGIC REFRESH MATERIALIZED VIEW gold.daily_category_sales;           -- on demand (FULL = recompute everything)
# MAGIC ALTER MATERIALIZED VIEW gold.daily_category_sales ALTER SCHEDULE EVERY 2 HOURS;
# MAGIC ALTER MATERIALIZED VIEW gold.daily_category_sales DROP SCHEDULE;
# MAGIC ```
# MAGIC
# MAGIC * Behind every MV is a **serverless pipeline** (Section 08) that computes and refreshes it — **incrementally** when the
# MAGIC   query and sources allow it, otherwise fully.
# MAGIC * `SCHEDULE` = time-based · `TRIGGER ON UPDATE` = data-driven (refresh when upstream tables change, at most every N) ·
# MAGIC   a **SQL task** `REFRESH MATERIALIZED VIEW …` after the load task = orchestrated in a job.
# MAGIC * Requirements: Unity Catalog, a **pro or serverless** SQL warehouse (or serverless notebook / pipeline), `SELECT` on the
# MAGIC   sources, `CREATE MATERIALIZED VIEW` + `USE SCHEMA`/`USE CATALOG` on the target. No `OPTIMIZE`/`VACUUM`/`CLONE` on MVs
# MAGIC   (managed for you), no identity columns.
# MAGIC
# MAGIC ## 4 · Metric views — one definition per KPI

# COMMAND ----------

# DBTITLE 1,Slide · Metric views
show("""
<div class="kicker">Slide 4 · Unity Catalog metric views (the semantic layer of the lakehouse)</div>
<h2>Define dimensions and measures once — query them at any grain with <code>MEASURE()</code></h2>
<div class="grid two">
 <div class="card"><h3>📝 Definition (YAML in a view)</h3>
<pre style="font-size:12px;margin:4px 0">CREATE VIEW sales_metrics WITH METRICS LANGUAGE YAML AS $$
version: 1.1
source: workspace.shopwave.bi_order_items
dimensions:
  - name: order_month
    expr: DATE_TRUNC('MONTH', order_date)
  - name: category
    expr: category
measures:
  - name: revenue
    expr: SUM(subtotal)
  - name: orders
    expr: COUNT(DISTINCT order_id)
$$</pre></div>
 <div class="card green"><h3>🔎 Usage</h3>
<pre style="font-size:12px;margin:4px 0">SELECT category,
       MEASURE(revenue) AS revenue,
       MEASURE(orders)  AS orders
FROM sales_metrics
GROUP BY category</pre>
 <ul><li><code>orders</code> is a <b>distinct</b> count — correct for any grouping (no double counting across categories)</li>
 <li>joins to dimensions, filters, comments, certification</li>
 <li>consumed by SQL, <b>dashboards</b>, <b>Genie</b>, alerts — and BI tools</li>
 <li>governed by Unity Catalog like any view</li></ul></div>
</div>
""" + callout("info", "Without a metric view, every dashboard author re-implements “revenue” and “orders” — and someone will sum a "
              "pre-aggregated count. Metric views move KPI logic out of the BI tool into Unity Catalog."))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 · Metadata BI tools (and Genie) use
# MAGIC
# MAGIC ```sql
# MAGIC COMMENT ON TABLE gold.orders IS 'One row per valid order. Grain: order.';
# MAGIC ALTER TABLE gold.orders ALTER COLUMN total COMMENT 'Order value in USD';
# MAGIC ALTER TABLE gold.customers ALTER COLUMN customer_id SET NOT NULL;
# MAGIC ALTER TABLE gold.customers ADD CONSTRAINT customers_pk PRIMARY KEY (customer_id);
# MAGIC ALTER TABLE gold.orders    ADD CONSTRAINT orders_customer_fk FOREIGN KEY (customer_id) REFERENCES gold.customers;
# MAGIC ```
# MAGIC
# MAGIC | Metadata | Who benefits |
# MAGIC |---|---|
# MAGIC | **Comments** | Catalog Explorer, Genie (better SQL), Power BI field descriptions, AI-generated documentation |
# MAGIC | **PK / FK** (informational, **not enforced**; `RELY` lets the optimizer use them) | ER diagram in Catalog Explorer, **Power BI relationships** on publish, Genie joins |
# MAGIC | **Tags** / governed tags | discovery, ABAC policies (Section 13) |
# MAGIC | **Certified / deprecated** status, lineage | trust: which table should a dashboard use? |
# MAGIC
# MAGIC ## 6 · Connecting external BI tools

# COMMAND ----------

# DBTITLE 1,Slide · What a BI tool needs
show("""
<div class="kicker">Slide 6 · Three values + a credential</div>
<h2>Server hostname · Port 443 · HTTP path (<code>/sql/1.0/warehouses/&lt;id&gt;</code>) · OAuth / token</h2>
<div class="grid">
 <div class="card"><h3>📍 Where to find them</h3>SQL warehouse → <b>Connection details</b> tab (cluster: Advanced options → JDBC/ODBC).
 Drivers: Databricks <b>JDBC</b> &amp; <b>ODBC</b>, Python SQL connector, Node.js, Go.</div>
 <div class="card green"><h3>🔑 Authentication</h3><ul><li><b>OAuth user-to-machine</b> — users sign in (desktop tools)</li>
 <li><b>OAuth machine-to-machine</b> with a <b>service principal</b> — scheduled refreshes, production</li>
 <li><b>Personal access token</b> — simple, tied to a person (avoid for production)</li></ul></div>
 <div class="card orange"><h3>🤝 Partner Connect</h3>one-click trials &amp; connections (Power BI, Tableau, Fivetran, dbt…). For cloud partners it
 creates a <b>SQL warehouse, service principal and token</b>; for desktop tools it downloads a connection file. Workspace admin needed.</div>
</div>
<table class="tbl">
<tr><th>Power BI</th><th>How</th></tr>
<tr><td><b>Power BI Desktop</b></td><td>Azure Databricks / Databricks connector (or a <code>.pbids</code> file from Partner Connect) → hostname + HTTP path → OAuth</td></tr>
<tr><td><b>Publish to Power BI service</b></td><td>Catalog Explorer → schema → <b>Use with BI tools</b> → <b>Publish to Power BI workspace</b>: creates a semantic model; FKs → relationships, comments → descriptions</td></tr>
<tr><td><b>Import</b> vs <b>DirectQuery</b> (vs Dual)</td><td>Import: data copied into Power BI, refreshed on a schedule, fast visuals · DirectQuery: every visual queries the SQL warehouse — always current, warehouse load</td></tr>
<tr><td><b>Power BI task</b> (Lakeflow Jobs)</td><td>publishes / refreshes the semantic model after the pipeline (needs a SQL warehouse + a Power BI UC connection)</td></tr>
</table>
""" + callout("exam", "Objective 2.5 — <b>JDBC/ODBC or REST clients in notebooks, orchestrated with Lakeflow Jobs</b>: JDBC/ODBC need "
              "hostname + HTTP path + credential; the REST route is the <b>Statement Execution API</b> "
              "(<code>POST /api/2.0/sql/statements</code> with a <code>warehouse_id</code>)."))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7 · Permissions for BI consumers
# MAGIC
# MAGIC | Consumer | Needs |
# MAGIC |---|---|
# MAGIC | Analyst in the SQL editor | **Can use** on a warehouse + `USE CATALOG`, `USE SCHEMA`, `SELECT` on the gold objects |
# MAGIC | Dashboard viewer (shared data permission) | **CAN VIEW** on the dashboard — nothing else (publisher's data and compute rights) |
# MAGIC | Dashboard viewer (individual data permission) | **CAN VIEW** + `USE CATALOG` / `USE SCHEMA` / `SELECT` — row filters & masks apply to them |
# MAGIC | Power BI service refresh | a **service principal** with **Can use** + the UC privileges; OAuth M2M |
# MAGIC | Partner tool (Fivetran, dbt Cloud…) | the service principal Partner Connect created (check its grants!) |
# MAGIC
# MAGIC ```sql
# MAGIC GRANT USE CATALOG ON CATALOG workspace          TO `bi_analysts`;
# MAGIC GRANT USE SCHEMA  ON SCHEMA  workspace.shopwave TO `bi_analysts`;
# MAGIC GRANT SELECT      ON SCHEMA  workspace.shopwave TO `bi_analysts`;   -- every current and future table/view in it
# MAGIC ```
# MAGIC
# MAGIC > 💡 Granting on the **schema** is inherited by all tables and views in it — simpler than per-table grants, but make sure
# MAGIC > the schema holds only what analysts should see (a dedicated `gold` / `serving` schema).
# MAGIC
# MAGIC ## 8 · Monitoring BI usage and cost
# MAGIC
# MAGIC | Question | Where |
# MAGIC |---|---|
# MAGIC | Which dashboards / tools hit the warehouse? | Query History filtered by **source**; `system.query.history.query_source` (dashboard_id, alert_id, job_info, genie_space_id) and `client_application` |
# MAGIC | Are queries queueing? | warehouse **Monitoring** tab; `waiting_at_capacity_duration_ms` in `system.query.history` → raise max clusters |
# MAGIC | Which queries are slow / read too much? | query **profile**; `read_bytes`, `pruned_files`, `spilled_local_bytes` → data layout (liquid clustering, Section 12), MVs |
# MAGIC | What does BI cost? | `system.billing.usage` per warehouse and **tags**; auto stop and serverless to avoid idle cost |
# MAGIC | How often is the cache used? | `from_result_cache` in `system.query.history` |
# MAGIC
# MAGIC ## 9 · Beyond the exam (recognise the names)
# MAGIC * **Lakehouse Federation** — query external databases (PostgreSQL, Snowflake, SQL Server…) through **foreign catalogs**
# MAGIC   without copying the data; handy for BI that needs a small external lookup.
# MAGIC * **Delta Sharing** — share gold tables with other organizations or Databricks accounts without copying (removed from the
# MAGIC   current DE Associate guide; Section 13 keeps a short overview).
# MAGIC * **Databricks Apps** — host a custom data app (Streamlit, Dash, React…) next to your data when a dashboard isn't enough.
# MAGIC
# MAGIC ## ✅ Key takeaways
# MAGIC 1. Gold = designed for consumers: clear **grain**, **star schema**, business names, **comments**, informational **PK/FK**.
# MAGIC 2. **View** = always fresh, pays per read · **materialized view** = precomputed, refreshed (`REFRESH`, `SCHEDULE`, `TRIGGER ON
# MAGIC    UPDATE`, job task), the default for dashboard aggregates · **table** = your write logic · **streaming table** = incremental
# MAGIC    ingestion · **metric view** = one KPI definition, `MEASURE()`.
# MAGIC 3. BI tools need **hostname + port 443 + HTTP path** + OAuth (U2M users / **M2M service principal** production) or a PAT.
# MAGIC 4. **Partner Connect** creates the warehouse / service principal / token for partner tools (admin).
# MAGIC 5. Power BI: **Import** (copy, scheduled refresh) vs **DirectQuery** (live queries on the warehouse); publish from Catalog
# MAGIC    Explorer keeps FK relationships and comments; **Power BI task** in jobs.
# MAGIC 6. Consumers need **Can use** on the warehouse + `USE CATALOG` / `USE SCHEMA` / `SELECT` — except viewers of dashboards
# MAGIC    published with **shared** data permission.
# MAGIC
# MAGIC ➡️ Labs: **09-L1 · 09-L2 · 09-L3 · 09-L4** · Quiz: **09-Q**
