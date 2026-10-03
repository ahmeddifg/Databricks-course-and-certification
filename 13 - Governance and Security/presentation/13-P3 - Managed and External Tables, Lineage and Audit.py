# Databricks notebook source
# MAGIC %md
# MAGIC # 🗄️ 13-P3 · Managed and External Tables, Lineage and Audit
# MAGIC **Section 13** · exam objective **7.1** — *differentiate between managed and external tables in Unity Catalog and perform
# MAGIC basic operations (create, modify, delete, and convert between managed and external tables)* + the rest of the governance
# MAGIC toolbox: storage credentials, external locations, volumes, lineage, audit, discovery, sharing
# MAGIC
# MAGIC | After this presentation you can… |
# MAGIC |---|
# MAGIC | Compare **managed** and **external** tables: storage, lifecycle, DROP behaviour, features |
# MAGIC | Create, alter, drop and **convert** tables in both directions (`SET MANAGED` / `UNSET MANAGED`, CTAS/CLONE) |
# MAGIC | Explain **storage credentials**, **external locations** and **volumes** and their privileges |
# MAGIC | Find **lineage** (Catalog Explorer, system tables) and **audit** events (`system.access.audit`) |
# MAGIC
# MAGIC > ▶️ **Run all** to render the diagrams. Section 04 introduced managed vs external tables — this is the governance view.

# COMMAND ----------

# MAGIC %run ../../Includes/_style

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · Managed vs external tables

# COMMAND ----------

# DBTITLE 1,Slide · Managed vs external
show("""
<div class="kicker">Slide 1 · Who manages the files — Unity Catalog, or you?</div>
<table class="tbl">
<tr><th></th><th>📦 Managed table</th><th>🔗 External table</th></tr>
<tr><td>Data files live in</td><td>the <b>managed storage location</b> of the schema, catalog or metastore (UC chooses the path)</td><td>a path <b>you</b> give with <code>LOCATION 'abfss://…'</code>, inside an <b>external location</b></td></tr>
<tr><td>Create</td><td><code>CREATE TABLE t (…)</code> / CTAS / <code>saveAsTable</code></td><td><code>CREATE TABLE t (…) LOCATION '…'</code> — needs <b>CREATE EXTERNAL TABLE</b> on the external location</td></tr>
<tr><td>Formats</td><td>Delta (and managed Iceberg)</td><td>Delta, Parquet, CSV, JSON, Avro, ORC, text…</td></tr>
<tr><td><code>DROP TABLE</code></td><td>metadata <b>and data</b> removed (data deleted after the recovery period; <code>UNDROP TABLE</code> within <b>7 days</b>)</td><td><b>metadata only</b> — the files stay in your storage</td></tr>
<tr><td>Optimisations</td><td>✅ predictive optimization (auto OPTIMIZE/VACUUM/ANALYZE), automatic liquid clustering, faster metadata</td><td>❌ you run maintenance yourself</td></tr>
<tr><td>Other engines</td><td>through UC APIs / credential vending</td><td>can read the path directly (with their own credentials)</td></tr>
<tr><td>Recommended</td><td>✅ <b>default choice</b></td><td>files must stay in a given place, shared with non-Databricks tools, or existing data you can't move</td></tr>
</table>
""" + callout("trap", "Path-based access to a managed table's files is not allowed — you use the table name. And <code>DESCRIBE TABLE EXTENDED</code> → "
              "<b>Type</b> (<code>MANAGED</code> / <code>EXTERNAL</code>) and <b>Location</b> is how you tell them apart."))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · Basic operations
# MAGIC
# MAGIC ```sql
# MAGIC -- managed: no LOCATION
# MAGIC CREATE TABLE sec_payments (payment_id STRING, order_id STRING, amount DECIMAL(10,2)) COMMENT 'managed';
# MAGIC
# MAGIC -- external: LOCATION inside an external location you can use (CREATE EXTERNAL TABLE on it)
# MAGIC CREATE TABLE sec_payments_ext (payment_id STRING, order_id STRING, amount DECIMAL(10,2))
# MAGIC LOCATION 'abfss://lake@myaccount.dfs.core.windows.net/shopwave/payments';
# MAGIC
# MAGIC -- modify (both kinds)
# MAGIC ALTER TABLE sec_payments ADD COLUMN method STRING;
# MAGIC ALTER TABLE sec_payments RENAME TO sec_payments_v2;
# MAGIC ALTER TABLE sec_payments_v2 SET TBLPROPERTIES ('quality' = 'gold');
# MAGIC COMMENT ON TABLE sec_payments_v2 IS 'Payments of ShopWave';
# MAGIC
# MAGIC -- delete
# MAGIC DROP TABLE sec_payments_v2;       -- managed: data goes too  →  UNDROP TABLE sec_payments_v2 (≤ 7 days)
# MAGIC DROP TABLE sec_payments_ext;      -- external: files stay
# MAGIC ```
# MAGIC
# MAGIC ## 3 · Converting between managed and external

# COMMAND ----------

# DBTITLE 1,Slide · Conversions
show("""
<div class="kicker">Slide 3 · Two directions, two different tools</div>
<div class="grid two">
 <div class="card green"><h3>🔗 ➜ 📦 External → managed</h3>
 <code>ALTER TABLE main.sales.orders SET MANAGED;</code>
 <ul><li>copies the data into managed storage and switches the table — same name, history, grants, comments; minimal downtime</li>
 <li>needs serverless or DBR <b>17.3 LTS+</b></li>
 <li>roll back within <b>14 days</b>: <code>ALTER TABLE … UNSET MANAGED</code> (points back to the original location)</li>
 <li>foreign (federated) tables: <code>SET MANAGED MOVE | COPY</code></li></ul></div>
 <div class="card orange"><h3>📦 ➜ 🔗 Managed → external</h3>no single command — create a new table at a location and swap:
 <ul><li><code>CREATE TABLE orders_ext LOCATION '…' AS SELECT * FROM orders</code> (CTAS)</li>
 <li>or <code>CREATE TABLE orders_ext DEEP CLONE orders LOCATION '…'</code> (keeps properties)</li>
 <li>re-apply grants, then <code>DROP</code> / <code>RENAME</code></li></ul></div>
</div>
<table class="tbl">
<tr><th>Migration</th><th>Tool</th></tr>
<tr><td>hive_metastore table → UC external table (same files)</td><td><code>SYNC TABLE</code> / <code>SYNC SCHEMA</code>, or <code>CREATE TABLE … LOCATION</code> on the same path</td></tr>
<tr><td>hive_metastore table → UC managed table</td><td>CTAS or <code>DEEP CLONE</code> into a UC schema (then <code>SET MANAGED</code> if you created it external)</td></tr>
</table>
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · Storage governance: credentials, external locations, volumes
# MAGIC
# MAGIC ```text
# MAGIC storage credential  (cloud identity: IAM role / managed identity / service account)
# MAGIC         └── external location  = credential + path  'abfss://lake@acct.dfs.core.windows.net/shopwave'
# MAGIC                 ├── external tables      (CREATE EXTERNAL TABLE on the location)
# MAGIC                 ├── external volumes     (CREATE EXTERNAL VOLUME)
# MAGIC                 ├── managed storage of a catalog/schema  (CREATE MANAGED STORAGE)
# MAGIC                 └── path access          (READ FILES / WRITE FILES)
# MAGIC ```
# MAGIC
# MAGIC ```sql
# MAGIC CREATE EXTERNAL LOCATION shopwave_lake
# MAGIC URL 'abfss://lake@myaccount.dfs.core.windows.net/shopwave'
# MAGIC WITH (STORAGE CREDENTIAL lake_mi) COMMENT 'ShopWave landing zone';
# MAGIC
# MAGIC GRANT READ FILES, CREATE EXTERNAL TABLE ON EXTERNAL LOCATION shopwave_lake TO `shopwave_engineers`;
# MAGIC GRANT READ VOLUME ON VOLUME workspace.shopwave.raw TO `shopwave_analysts`;
# MAGIC ```
# MAGIC
# MAGIC | Object | Privileges |
# MAGIC |---|---|
# MAGIC | Storage credential | CREATE EXTERNAL LOCATION, (CREATE EXTERNAL TABLE, READ/WRITE FILES — prefer granting them on locations) |
# MAGIC | External location | READ FILES · WRITE FILES · CREATE EXTERNAL TABLE · CREATE EXTERNAL VOLUME · CREATE MANAGED STORAGE · BROWSE |
# MAGIC | Volume (managed or external) | READ VOLUME · WRITE VOLUME (+ USE CATALOG / USE SCHEMA) |
# MAGIC
# MAGIC > 🕰️ DBFS mounts (`/mnt/...`) bypassed table permissions — anybody on the cluster could read the files. Unity Catalog
# MAGIC > replaces them with external locations and volumes, governed and audited.
# MAGIC
# MAGIC ## 5 · Lineage, audit and discovery

# COMMAND ----------

# DBTITLE 1,Slide · Lineage and audit
show("""
<div class="kicker">Slide 5 · Know where data comes from, who uses it, and who changed access</div>
<div class="grid">
 <div class="card"><h3>🧬 Lineage (automatic)</h3>captured for queries run on UC from notebooks, jobs, pipelines, SQL, dashboards — <b>table and column level</b>.<br>
 Catalog Explorer → table → <b>Lineage</b> tab / <b>See lineage graph</b> (upstream, downstream, notebooks, jobs, dashboards).<br>
 SQL: <code>system.access.table_lineage</code>, <code>system.access.column_lineage</code>. You only see objects you may see.</div>
 <div class="card orange"><h3>📜 Audit</h3><code>system.access.audit</code>: who did what, when, from where — logins, queries, <b>GRANT/REVOKE</b> (<code>updatePermissions</code>),
 table creation/deletion, policy changes, downloads… (<code>action_name</code>, <code>user_identity.email</code>, <code>request_params</code>).</div>
 <div class="card green"><h3>🔎 Discovery</h3>comments, tags, <b>certification</b> (<code>system.certification_status</code>), search, <b>BROWSE</b> privilege + <b>request access</b>,
 <code>information_schema</code> (tables, columns, privileges, tags, row filters, column masks, ABAC policies).</div>
</div>
""" + callout("tip", "Before dropping or changing a table, open its <b>Lineage</b> tab: which jobs, notebooks and dashboards will break? "
              "Lineage is the impact analysis you get for free with Unity Catalog."))

# COMMAND ----------

# MAGIC %md
# MAGIC ```sql
# MAGIC -- Who changed permissions in the last 7 days?
# MAGIC SELECT event_time, user_identity.email AS who, action_name, request_params
# MAGIC FROM system.access.audit
# MAGIC WHERE service_name = 'unityCatalog' AND action_name = 'updatePermissions'          -- GRANT / REVOKE
# MAGIC   AND event_date >= current_date() - INTERVAL 7 DAYS
# MAGIC ORDER BY event_time DESC;
# MAGIC
# MAGIC -- What reads my table?
# MAGIC SELECT DISTINCT entity_type, entity_id, target_table_full_name
# MAGIC FROM system.access.table_lineage
# MAGIC WHERE source_table_full_name = 'workspace.shopwave.sec_orders';
# MAGIC ```
# MAGIC
# MAGIC ## 6 · Sharing outside the account (brief — removed from the current exam)
# MAGIC **Delta Sharing** — now evolving into **OpenSharing** — shares tables, views, volumes, models and notebooks with other
# MAGIC organisations without copying: a **share** (what) + a **recipient** (who) — *Databricks-to-Databricks* (UC to UC) or *open*
# MAGIC sharing (token/OIDC, any client). **Clean rooms** let parties join data without seeing each other's raw records.
# MAGIC **Lakehouse Federation** (connections, foreign catalogs) queries external databases under UC governance.
# MAGIC
# MAGIC ## ✅ Key takeaways
# MAGIC 1. **Managed** tables: UC-managed storage, Delta/Iceberg, DROP removes data (UNDROP ≤ 7 days), predictive optimization —
# MAGIC    the default. **External** tables: your `LOCATION` in an external location, many formats, DROP removes metadata only.
# MAGIC 2. External → managed: **`ALTER TABLE … SET MANAGED`** (roll back with `UNSET MANAGED` within 14 days); managed → external:
# MAGIC    **CTAS / DEEP CLONE with LOCATION**, then swap.
# MAGIC 3. **Storage credential** (cloud identity) → **external location** (credential + path) → external tables/volumes, path
# MAGIC    access with READ/WRITE FILES; volumes use READ/WRITE VOLUME.
# MAGIC 4. **Lineage** (table + column, automatic) in Catalog Explorer and `system.access.*_lineage`; **audit** in
# MAGIC    `system.access.audit`; discovery with comments, tags, BROWSE, information_schema.
# MAGIC
# MAGIC ➡️ Labs: **13-L1** (privileges) → **13-L2** (row filters, masks, dynamic views) → **13-L3** (ABAC, lineage, audit) →
# MAGIC **13-L4** (challenge) → **13-Q** (exam questions)
