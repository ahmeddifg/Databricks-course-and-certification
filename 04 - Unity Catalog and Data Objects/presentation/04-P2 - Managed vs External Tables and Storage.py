# Databricks notebook source
# MAGIC %md
# MAGIC # 🗄️ 04-P2 · Managed vs External Tables & Storage
# MAGIC **Section 04 — Unity Catalog & Data Objects** · supports **D7 Governance & Security (15 %)** — *"managed vs external tables"*
# MAGIC is an explicit exam objective
# MAGIC
# MAGIC | After this presentation you can… |
# MAGIC |---|
# MAGIC | Say **where the files** of a managed table live and how that location is chosen |
# MAGIC | Compare **managed** and **external** tables: creation syntax, formats, `DROP` behaviour, recovery |
# MAGIC | Explain **storage credentials** and **external locations** and the privileges they need |
# MAGIC | Use `DROP`, `UNDROP`, `SHOW TABLES DROPPED` and `DROP SCHEMA … CASCADE` safely |
# MAGIC | Tell a table's type from `DESCRIBE EXTENDED`, `DESCRIBE DETAIL` or `information_schema` |
# MAGIC | Recognise the legacy Hive metastore rules (`LOCATION` on a database!) |
# MAGIC
# MAGIC > ▶️ **Run all** to render the diagrams (a few seconds, any compute).

# COMMAND ----------

# MAGIC %run ../../Includes/_style

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · Two ways to own a table's files

# COMMAND ----------

# DBTITLE 1,Slide · Managed vs external
show("""
<div class="kicker">Slide 1 · Who manages the files?</div>
<h2>Managed table = Unity Catalog manages metadata <b>and</b> data · External table = metadata only</h2>
<div class="grid two">
 <div class="card green"><h3>🏠 Managed table <span class="pill green">recommended default</span></h3>
  <ul><li>Created <b>without</b> <code>LOCATION</code></li>
  <li>Files go to the <b>managed storage location</b> of the schema / catalog / metastore</li>
  <li>Formats: <b>Delta</b> (default) or <b>Iceberg</b></li>
  <li><code>DROP TABLE</code> → removed from UC; data files <b>deleted</b> after the recovery window (7 days by default)</li>
  <li>Automatic maintenance: <b>predictive optimization</b> (OPTIMIZE/VACUUM/ANALYZE), auto liquid clustering, faster metadata</li>
  <li>Access only through Unity Catalog (names), not by path</li></ul></div>
 <div class="card orange"><h3>🌍 External table</h3>
  <ul><li>Created <b>with</b> <code>LOCATION 's3://…'</code> inside an <b>external location</b></li>
  <li>You manage the files and their lifecycle</li>
  <li>Formats: <b>Delta</b>, CSV, JSON, Avro, Parquet, ORC, text</li>
  <li><code>DROP TABLE</code> → only the <b>metadata</b> is removed; the <b>files stay</b></li>
  <li>Use when other tools must read/write the same files directly, or for existing data you can't move</li>
  <li>Needs <code>CREATE EXTERNAL TABLE</code> on the external location</li></ul></div>
</div>
""" + callout("exam", "The single fastest way to tell them apart in a question: <b>was a <code>LOCATION</code> given for the table?</b> "
              "Yes → external. No → managed (even if the schema or catalog has a <code>MANAGED LOCATION</code>).")
  + callout("tip", "Databricks recommends <b>managed tables</b> for everything that doesn't need direct path access by other systems."))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · Where do managed files go? The managed storage hierarchy

# COMMAND ----------

# DBTITLE 1,Slide · Managed storage locations
show("""
<div class="kicker">Slide 2 · Managed storage locations</div>
<h2>The most specific location wins</h2>
<div class="layer" style="background:#0b2a4a">🏛️ Metastore storage <small>— optional (older metastores had one; new ones often don't)</small></div>
<div style="margin-left:22px"><div class="layer" style="background:#7048e8">📚 Catalog: <code>CREATE CATALOG sales MANAGED LOCATION 's3://sales-bucket/uc'</code></div>
<div style="margin-left:22px"><div class="layer" style="background:#1c7ed6">🗂️ Schema: <code>CREATE SCHEMA sales.hr MANAGED LOCATION 's3://hr-bucket/uc'</code></div>
<div style="margin-left:22px"><div class="layer" style="background:#2f9e44">📋 Managed tables &amp; volumes of <code>sales.hr</code> → stored under <b>s3://hr-bucket/uc/…</b></div></div></div></div>
<table class="tbl">
<tr><th>Schema location?</th><th>Catalog location?</th><th>Metastore location?</th><th>Managed data goes to</th></tr>
<tr><td>✅</td><td>any</td><td>any</td><td>schema location</td></tr>
<tr><td>—</td><td>✅</td><td>any</td><td>catalog location</td></tr>
<tr><td>—</td><td>—</td><td>✅</td><td>metastore location</td></tr>
<tr><td>—</td><td>—</td><td>—</td><td><b>default storage</b> (serverless / Free Edition workspaces) — otherwise creating the catalog fails</td></tr>
</table>
""" + callout("info", "Databricks creates sub-folders with generated ids under that location; you never pick a table's path, "
              "and you should never read or write those files directly — always use the table name.")
  + callout("trap", "<code>MANAGED LOCATION</code> (catalog/schema) ≠ <code>LOCATION</code> (table). A schema with a managed "
            "location still produces <b>managed</b> tables."))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🧪 Try it: which location wins?
# MAGIC Change the three variables and run the cell — a pure-Python model of the rule (no Spark needed).

# COMMAND ----------

# DBTITLE 1,Managed location resolver (edit me)
metastore_location = None                          # e.g. "s3://company-metastore"
catalog_location = "s3://sales-bucket/uc"          # CREATE CATALOG ... MANAGED LOCATION
schema_location = None                             # CREATE SCHEMA ... MANAGED LOCATION
default_storage = True                             # serverless workspaces / Free Edition


def managed_root(schema_loc, catalog_loc, metastore_loc, default_storage_available):
    for level, loc in (("schema", schema_loc), ("catalog", catalog_loc), ("metastore", metastore_loc)):
        if loc:
            return f"{level} managed location -> {loc}/__unitystorage/..."
    if default_storage_available:
        return "Databricks default storage (fully managed, not browsable)"
    return "ERROR: no managed location - add MANAGED LOCATION to the catalog or schema"


print("Managed tables of this schema are stored in:",
      managed_root(schema_location, catalog_location, metastore_location, default_storage))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · Reaching your own storage: storage credentials & external locations

# COMMAND ----------

# DBTITLE 1,Slide · Credential + location
show("""
<div class="kicker">Slide 3 · Securables that guard cloud storage</div>
<h2>External location = <b>path</b> + <b>storage credential</b></h2>
<div class="flow">
 <div class="step"><b>🔑 Storage credential</b>AWS IAM role · Azure managed identity · GCP service account<br><span class="muted">created by an admin</span></div>
 <div class="arrow">➕</div>
 <div class="step"><b>📍 External location</b><code>s3://acme-data/landing</code><br><span class="muted">path + credential</span></div>
 <div class="arrow">➜</div>
 <div class="step"><b>🌍 External tables</b><code>CREATE TABLE … LOCATION</code></div>
 <div class="step"><b>📁 External volumes</b><code>CREATE EXTERNAL VOLUME … LOCATION</code></div>
 <div class="step"><b>🏠 Managed locations</b>catalog / schema <code>MANAGED LOCATION</code></div>
</div>
<table class="tbl">
<tr><th>Privilege on the external location</th><th>Lets you…</th></tr>
<tr><td><code>READ FILES</code> / <code>WRITE FILES</code></td><td>read / write files by path directly (e.g. <code>LIST</code>, <code>read_files('s3://…')</code>, COPY INTO)</td></tr>
<tr><td><code>CREATE EXTERNAL TABLE</code></td><td>register external tables under that path</td></tr>
<tr><td><code>CREATE EXTERNAL VOLUME</code></td><td>register external volumes under that path</td></tr>
<tr><td><code>CREATE MANAGED STORAGE</code></td><td>use the path as a catalog/schema <code>MANAGED LOCATION</code></td></tr>
</table>
""" + callout("trap", "Users should get access to data through <b>tables and volumes</b>, not through <code>READ FILES</code> "
              "on a broad external location — path access bypasses table-level permissions.")
  + callout("info", "Free Edition uses <b>default storage</b> only: no storage credentials or external locations to create "
            "(the external-table demo in 04-L1 is optional). Data in default storage is reachable only through Unity Catalog from "
            "<b>serverless</b> compute — not from classic clusters or by cloud path."))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · Syntax cheat sheet
# MAGIC
# MAGIC ```sql
# MAGIC -- Managed (no LOCATION) ------------------------------------------------------
# MAGIC CREATE TABLE prod.sales.orders (order_id STRING, ts TIMESTAMP, total DOUBLE);
# MAGIC CREATE TABLE prod.sales.orders_2026 AS SELECT * FROM prod.sales.orders WHERE year(ts) = 2026;   -- CTAS
# MAGIC
# MAGIC -- External (LOCATION inside an external location) ------------------------------
# MAGIC CREATE TABLE prod.sales.orders_ext (order_id STRING, total DOUBLE)
# MAGIC LOCATION 's3://acme-data/tables/orders';                          -- Delta by default
# MAGIC CREATE TABLE prod.sales.orders_csv (order_id STRING, total DOUBLE)
# MAGIC USING CSV OPTIONS (header 'true') LOCATION 's3://acme-data/exports/orders_csv';
# MAGIC CREATE TABLE prod.sales.orders_ext2 LOCATION 's3://acme-data/tables/orders2' AS SELECT * FROM prod.sales.orders;
# MAGIC ALTER TABLE prod.sales.orders_ext SET MANAGED;   -- newer: converts an external Delta table to managed (data moves to managed storage)
# MAGIC
# MAGIC -- Where managed data goes -----------------------------------------------------
# MAGIC CREATE CATALOG prod MANAGED LOCATION 's3://prod-bucket/uc';
# MAGIC CREATE SCHEMA prod.hr MANAGED LOCATION 's3://hr-bucket/uc';
# MAGIC ```
# MAGIC
# MAGIC > 🎯 `CREATE EXTERNAL TABLE` also parses on Databricks, but the keyword that matters is **`LOCATION`**.
# MAGIC > A `LOCATION` pointing into **managed storage** or overlapping another table's path is rejected — paths can't overlap.
# MAGIC
# MAGIC ## 5 · Dropping things — and getting them back

# COMMAND ----------

# DBTITLE 1,Slide · DROP behaviour
show("""
<div class="kicker">Slide 5 · DROP, UNDROP and CASCADE</div>
<h2>What does <code>DROP</code> really delete?</h2>
<table class="tbl">
<tr><th>Statement</th><th>Managed table</th><th>External table</th></tr>
<tr><td><code>DROP TABLE t</code></td><td>Removed from UC now; data files purged after the <b>recovery period</b> (7 days default)</td><td>Metadata removed; <b>files untouched</b></td></tr>
<tr><td>Recovery period</td><td colspan="2">7 days by default; per catalog or schema set <b>0</b> (recovery off) or <b>7–30 days</b>: <code>ALTER SCHEMA s SET RETAIN DROPPED TO 14 DAYS</code> (schema setting wins over catalog)</td></tr>
<tr><td><code>UNDROP TABLE t</code></td><td>✅ within the recovery period</td><td>✅ within the recovery period (re-registers the metadata; needs <code>CREATE EXTERNAL TABLE</code> on the external location)</td></tr>
<tr><td><code>TRUNCATE TABLE t</code> / <code>DELETE FROM t</code></td><td colspan="2">Rows removed in a new version (Delta) — the table stays; old versions still reachable by time travel until VACUUM</td></tr>
</table>
<div class="grid two">
 <div class="card"><h3>♻️ Recover a table</h3>
  <code>SHOW TABLES DROPPED IN prod.sales</code><br><code>UNDROP TABLE prod.sales.orders</code><br>
  Restores the <b>most recently dropped</b> table with that name; to pick an older one: <code>UNDROP TABLE WITH ID '&lt;tableId&gt;'</code><br>
  <span class="muted">The parent schema must still exist. Materialized views created in Databricks SQL/notebooks can't be undropped.</span></div>
 <div class="card red"><h3>🧨 Schemas and catalogs</h3>
  <code>DROP SCHEMA s</code> = <code>… RESTRICT</code> → <b>fails</b> if the schema is not empty<br>
  <code>DROP SCHEMA s CASCADE</code> → drops every object in it<br>
  <code>DROP CATALOG c</code> fails while any schema other than <code>information_schema</code> exists — <b>even the auto-created <code>default</code></b><br>
  <code>DROP CATALOG c CASCADE</code> → drops every schema and object</div>
</div>
""" + callout("exam", "Classic scenario: “a data engineer drops a table and the underlying files are deleted as well” "
              "→ it was a <b>managed</b> table. “The files remain in storage” → <b>external</b>."))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6 · How to tell what kind of table you have
# MAGIC
# MAGIC | Method | Look for |
# MAGIC |---|---|
# MAGIC | `DESCRIBE EXTENDED t` | row **Type**: `MANAGED` / `EXTERNAL` / `VIEW` / `MATERIALIZED_VIEW`; **Location**; **Provider** |
# MAGIC | `DESCRIBE DETAIL t` | `format`, `location`, `numFiles`, `sizeInBytes` (Delta tables) |
# MAGIC | `information_schema.tables` | `table_type`, `data_source_format`, `storage_path` |
# MAGIC | Catalog Explorer ▸ table ▸ **Details** | *Type* and *Storage location* |
# MAGIC
# MAGIC ## 7 · 🕰️ Legacy Hive metastore rules (still on the exam)

# COMMAND ----------

# DBTITLE 1,Slide · Hive metastore paths
show("""
<div class="kicker">Slide 7 · Legacy Hive metastore (hive_metastore)</div>
<h2>Same idea, different locations — and one famous trap</h2>
<table class="tbl">
<tr><th>Statement (Hive metastore)</th><th>Table type</th><th>Files at</th><th>DROP TABLE deletes files?</th></tr>
<tr><td><code>CREATE TABLE t …</code> in schema <code>default</code></td><td>managed</td><td><code>dbfs:/user/hive/warehouse/t</code></td><td>✅ yes</td></tr>
<tr><td><code>CREATE SCHEMA db;</code> then <code>CREATE TABLE db.t …</code></td><td>managed</td><td><code>dbfs:/user/hive/warehouse/db.db/t</code></td><td>✅ yes</td></tr>
<tr><td><code>CREATE SCHEMA db2 LOCATION 'dbfs:/custom/db2';</code> then <code>CREATE TABLE db2.t …</code></td><td><b>managed</b></td><td><code>dbfs:/custom/db2/t</code></td><td>✅ <b>yes</b></td></tr>
<tr><td><code>CREATE TABLE db.t … LOCATION 'dbfs:/mnt/data/t'</code></td><td>external</td><td>that path</td><td>❌ no</td></tr>
</table>
""" + callout("legacy", "Trap: a <b>schema</b> created with <code>LOCATION</code> only changes where its <b>managed</b> tables live — "
              "dropping those tables still deletes their files. Only a <b>table-level</b> <code>LOCATION</code> makes a table external. "
              "(In Unity Catalog the schema clause is called <code>MANAGED LOCATION</code>.)"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Key takeaways
# MAGIC 1. **Managed** = UC manages metadata **and** files (Delta/Iceberg) → `DROP` deletes the data after the recovery window. **Recommended.**
# MAGIC 2. **External** = table created **with `LOCATION`** inside an **external location** → `DROP` removes only metadata.
# MAGIC 3. Managed files go to the **most specific managed location**: schema → catalog → metastore → default storage.
# MAGIC 4. **External location = path + storage credential**; privileges `READ FILES`, `WRITE FILES`, `CREATE EXTERNAL TABLE`, `CREATE EXTERNAL VOLUME`, `CREATE MANAGED STORAGE`.
# MAGIC 5. `UNDROP TABLE` within the recovery period (7 days default); `DROP SCHEMA` is `RESTRICT` unless you add `CASCADE`.
# MAGIC 6. Legacy Hive: `dbfs:/user/hive/warehouse/<db>.db/<table>`; a schema `LOCATION` does **not** make its tables external.
# MAGIC
# MAGIC ➡️ Next: **04-P3 · Views, Volumes and Functions**
