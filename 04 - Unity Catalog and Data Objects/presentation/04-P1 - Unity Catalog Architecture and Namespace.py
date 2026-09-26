# Databricks notebook source
# MAGIC %md
# MAGIC # 🏛️ 04-P1 · Unity Catalog Architecture & the Three-Level Namespace
# MAGIC **Section 04 — Unity Catalog & Data Objects** · supports **D7 Governance & Security (15 %)**, **D1 Platform** and every
# MAGIC domain that reads or writes tables
# MAGIC
# MAGIC | After this presentation you can… |
# MAGIC |---|
# MAGIC | Explain **what Unity Catalog is** and what changed compared with the per-workspace Hive metastore |
# MAGIC | Draw the object hierarchy **metastore → catalog → schema → objects** and name every object type |
# MAGIC | Use **three-level names** and `USE CATALOG` / `USE SCHEMA` correctly (name resolution) |
# MAGIC | Know the storage-related securables: **storage credential**, **external location**, (connection) |
# MAGIC | Query **`information_schema`** and read `DESCRIBE … EXTENDED` output |
# MAGIC | Recognise **legacy** Hive metastore paths and syntax on the exam |
# MAGIC
# MAGIC > ▶️ **Run all** to render the diagrams (a few seconds, any compute).

# COMMAND ----------

# MAGIC %run ../../Includes/_style

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · Why Unity Catalog?

# COMMAND ----------

# DBTITLE 1,Slide · Before and after
show("""
<div class="kicker">Slide 1 · Governance before and after Unity Catalog</div>
<h2>From one metastore per workspace to one governance layer for the account</h2>
<div class="grid two">
  <div class="card gray"><h3>🕰️ Before: Hive metastore <span class="pill gray">per workspace</span></h3>
    <ul><li>Each workspace had its <b>own</b> metastore and its own users/permissions (table ACLs)</li>
    <li>Same data in two workspaces → define tables and grants <b>twice</b></li>
    <li>Two-level names: <code>database.table</code></li>
    <li>Files reachable through DBFS mounts — often <b>bypassing</b> table permissions</li>
    <li>No built-in lineage, audit or discovery across workspaces</li></ul></div>
  <div class="card green"><h3>🏛️ With Unity Catalog <span class="pill green">per account &amp; region</span></h3>
    <ul><li><b>One metastore per region</b>, attached to many workspaces</li>
    <li>Users &amp; groups come from the <b>account</b> — define access <b>once</b>, enforce everywhere</li>
    <li>Three-level names: <code>catalog.schema.object</code></li>
    <li>Governs <b>tables, views, volumes (files), functions, models</b></li>
    <li>Built-in <b>auditing, lineage, discovery</b> (Catalog Explorer, search, tags)</li></ul></div>
</div>
<div class="flow">
  <div class="step"><b>🔐 Access control</b>GRANT/REVOKE, row &amp; column security</div>
  <div class="step"><b>🔎 Discovery</b>Catalog Explorer, comments, tags, search</div>
  <div class="step"><b>🧬 Lineage</b>table &amp; column level, automatic</div>
  <div class="step"><b>📜 Auditing</b>who accessed what — system tables</div>
  <div class="step"><b>🤝 Sharing</b>Delta Sharing, federation</div>
</div>
""" + callout("exam", "Unity Catalog is the <b>centralized governance</b> solution for data and AI on Databricks: "
              "<b>define once, secure everywhere</b>, across all workspaces attached to the same metastore."))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · The object hierarchy

# COMMAND ----------

# DBTITLE 1,Slide · Metastore to objects
show("""
<div class="kicker">Slide 2 · The Unity Catalog object model</div>
<h2>Metastore → Catalog → Schema → Data &amp; AI objects</h2>
<div class="layer" style="background:#0b2a4a">🏛️ <b>Metastore</b> <small>— top-level container, one per <b>region</b>; attached to workspaces; holds catalogs + the storage/connection securables below</small></div>
<div style="margin-left:22px">
 <div class="layer" style="background:#7048e8">📚 <b>Catalog</b> <small>— 1st level of the namespace: environment (<code>dev</code>/<code>prod</code>), business unit, or data product</small></div>
 <div style="margin-left:22px">
  <div class="layer" style="background:#1c7ed6">🗂️ <b>Schema</b> (= database) <small>— 2nd level: a project, domain or medallion layer</small></div>
  <div style="margin-left:22px" class="grid">
   <div class="card"><h3>📋 Table</h3>managed or external · streaming table</div>
   <div class="card"><h3>👓 View</h3>view · materialized view · metric view</div>
   <div class="card teal"><h3>📁 Volume</h3>files (any format) · managed or external</div>
   <div class="card purple"><h3>ƒ Function</h3>SQL / Python UDFs</div>
   <div class="card orange"><h3>🤖 Model</h3>ML models (MLflow registry)</div>
  </div>
 </div>
</div>
<h3 style="margin-top:12px">Also defined at the metastore level (not inside a catalog)</h3>
<table class="tbl">
<tr><th>Securable</th><th>What it is</th></tr>
<tr><td>🔑 <b>Storage credential</b></td><td>A cloud identity (IAM role, managed identity, service account) Databricks uses to reach your storage</td></tr>
<tr><td>📍 <b>External location</b></td><td>A cloud <b>path</b> + the <b>storage credential</b> that can access it — the only way to register external tables/volumes</td></tr>
<tr><td>🔌 <b>Connection</b></td><td>Credentials for an external database, used by <b>Lakehouse Federation</b> (foreign catalogs)</td></tr>
<tr><td>🤝 <b>Share / Recipient / Provider</b></td><td>Delta Sharing objects</td></tr>
</table>
""" + callout("trap", "A <b>schema</b> and a <b>database</b> are the same thing: <code>CREATE DATABASE</code> = <code>CREATE SCHEMA</code>, "
              "<code>SHOW DATABASES</code> = <code>SHOW SCHEMAS</code>. The catalog sits <b>above</b> them.")
  + callout("info", "Privileges are <b>inherited downwards</b>: a grant on a catalog applies to all current and future schemas "
            "and objects in it (details in Section 13)."))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · Three-level names & name resolution

# COMMAND ----------

# DBTITLE 1,Slide · Namespace
show("""
<div class="kicker">Slide 3 · catalog.schema.object</div>
<h2>How Databricks finds <code>customers</code></h2>
<div class="flow">
  <div class="step"><b>prod</b>catalog</div><div class="arrow">.</div>
  <div class="step"><b>sales</b>schema</div><div class="arrow">.</div>
  <div class="step"><b>customers</b>table / view / volume / function</div>
</div>
<table class="tbl">
<tr><th>You write…</th><th>…after <code>USE CATALOG prod; USE SCHEMA sales;</code> it means</th></tr>
<tr><td><code>customers</code></td><td><code>prod.sales.customers</code> — current catalog <b>and</b> current schema<br>
<span class="muted">…unless a <b>temp view</b> called <code>customers</code> exists: one-part names check temp views first, so a temp view shadows a table</span></td></tr>
<tr><td><code>finance.budget</code></td><td><code>prod.finance.budget</code> — a <b>2-part</b> name is <b>schema.object</b> in the current catalog</td></tr>
<tr><td><code>dev.sales.customers</code></td><td>exactly that — a 3-part name ignores the context</td></tr>
</table>
<div class="grid two">
 <div class="card"><h3>🧭 Set / read the context</h3>
 <code>USE CATALOG prod</code> (or <code>SET CATALOG prod</code>) · <code>USE SCHEMA sales</code> (<code>USE sales</code> also works)<br>
 <code>SELECT current_catalog(), current_schema()</code><br>
 Python: <code>spark.catalog.setCurrentCatalog("prod")</code>, <code>spark.catalog.setCurrentDatabase("sales")</code></div>
 <div class="card orange"><h3>🏠 Default catalog</h3>
 Each workspace has a <b>default catalog</b> used when a session starts — the <b>workspace catalog</b> (named after the
 workspace, <code>workspace</code> on Free Edition) or <code>hive_metastore</code> in older workspaces.
 Admins can change it.</div>
</div>
""" + callout("exam", "The context lasts for the <b>session</b> (a notebook, a SQL editor tab, a job task). "
              "Best practice for production code: use <b>fully-qualified</b> names or parameterize the catalog "
              "(e.g. <code>IDENTIFIER(:catalog || '.sales.customers')</code>) so the same code runs in dev and prod."))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · Designing catalogs and schemas
# MAGIC
# MAGIC | Pattern | Catalogs | Schemas | Good for |
# MAGIC |---|---|---|---|
# MAGIC | **By environment** | `dev`, `test`, `prod` | `bronze`, `silver`, `gold` or by domain | Isolating environments; promote code by switching the catalog parameter |
# MAGIC | **By business unit / domain** | `sales`, `finance`, `hr` | projects or layers | Clear ownership, delegated administration |
# MAGIC | **By medallion layer** | `bronze`, `silver`, `gold` | domains | Simple permission rules per layer |
# MAGIC
# MAGIC * **Catalog types:** *standard* catalogs (your data), *foreign* catalogs (Lakehouse Federation — mirror an external database through a connection), *shared* catalogs (created from a Delta Sharing share), plus the read-only `system` catalog.
# MAGIC * **Workspace–catalog binding** can make a catalog available only in chosen workspaces (e.g. `prod` only in the prod workspace).
# MAGIC * Each catalog/schema can have its **own managed storage location** (04-P2) — e.g. keep `hr` data in a separate bucket.
# MAGIC * Keep names **lower case with underscores**; names are case-insensitive in Unity Catalog.
# MAGIC
# MAGIC ## 5 · Finding and describing objects

# COMMAND ----------

# DBTITLE 1,Slide · Discover metadata
show("""
<div class="kicker">Slide 5 · Metadata at your fingertips</div>
<h2>SQL commands, information_schema and Catalog Explorer</h2>
<div class="grid two">
 <div class="card"><h3>📜 SHOW / DESCRIBE</h3><ul>
  <li><code>SHOW CATALOGS</code> · <code>SHOW SCHEMAS IN prod</code> · <code>SHOW TABLES IN prod.sales</code></li>
  <li><code>SHOW VIEWS</code> · <code>SHOW VOLUMES</code> · <code>SHOW USER FUNCTIONS</code></li>
  <li><code>DESCRIBE CATALOG EXTENDED prod</code></li>
  <li><code>DESCRIBE SCHEMA EXTENDED prod.sales</code></li>
  <li><code>DESCRIBE [TABLE] EXTENDED t</code> → <b>Type</b> (MANAGED / EXTERNAL / VIEW), Location, Provider, Owner</li>
  <li><code>DESCRIBE DETAIL t</code> → format, location, numFiles, sizeInBytes (Delta)</li></ul></div>
 <div class="card teal"><h3>🗃️ information_schema</h3><ul>
  <li>Read-only ANSI views in <b>every catalog</b>: <code>prod.information_schema.tables</code></li>
  <li><code>system.information_schema</code> spans <b>all</b> catalogs</li>
  <li><code>catalogs</code>, <code>schemata</code>, <code>tables</code>, <code>columns</code>, <code>views</code>, <code>volumes</code>, <code>routines</code>, <code>*_privileges</code>…</li>
  <li>Shows only what <b>you</b> are allowed to see</li>
  <li><code>tables.table_type</code>: MANAGED · EXTERNAL · VIEW · MATERIALIZED_VIEW · STREAMING_TABLE · FOREIGN · …</li></ul></div>
</div>
<div class="flow">
 <div class="step"><b>🧭 Catalog Explorer</b>sidebar ▸ Catalog</div><div class="arrow">➜</div>
 <div class="step"><b>Overview</b>columns, comments, tags</div>
 <div class="step"><b>Sample data</b>preview</div>
 <div class="step"><b>Details</b>type, location, owner</div>
 <div class="step"><b>Permissions</b>grants</div>
 <div class="step"><b>History / Lineage</b>Delta history, upstream/downstream</div>
</div>
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6 · Identities and ownership — preview
# MAGIC *(Full treatment in Section 13 — Governance & Security.)*
# MAGIC
# MAGIC * **Principals**: users, **service principals** (for automation) and **groups**, managed at the **account** level.
# MAGIC * Every securable has **one owner** (initially its creator) who can grant privileges on it; owners can be groups.
# MAGIC * To read `prod.sales.customers` you need **`USE CATALOG`** on `prod` **+** **`USE SCHEMA`** on `prod.sales` **+** **`SELECT`** on the table.
# MAGIC * Admin roles: **account admin**, **metastore admin** (optional), **workspace admin**.
# MAGIC
# MAGIC ## 7 · 🕰️ Legacy you must still recognise

# COMMAND ----------

# DBTITLE 1,Slide · Legacy Hive metastore
show("""
<div class="kicker">Slide 7 · Legacy: the workspace Hive metastore</div>
<h2><code>hive_metastore</code> — still visible as a catalog in older workspaces</h2>
<table class="tbl">
<tr><th>Topic</th><th>Legacy Hive metastore</th><th>Unity Catalog</th></tr>
<tr><td>Scope</td><td>one workspace</td><td>account / region, many workspaces</td></tr>
<tr><td>Names</td><td><code>db.table</code> (appears as <code>hive_metastore.db.table</code>)</td><td><code>catalog.schema.table</code></td></tr>
<tr><td>Default managed table path</td><td><code>dbfs:/user/hive/warehouse/&lt;db&gt;.db/&lt;table&gt;</code><br>(<code>default</code> schema: <code>dbfs:/user/hive/warehouse/&lt;table&gt;</code>)</td><td>catalog / schema <b>managed storage</b> — not a path you browse</td></tr>
<tr><td>Permissions</td><td>table ACLs per workspace (<code>USAGE</code>, <code>READ_METADATA</code>, <code>ANY FILE</code>)</td><td>UC privileges, inherited from catalog/schema</td></tr>
<tr><td>Files</td><td>DBFS root, <code>/mnt</code> mounts</td><td><b>Volumes</b> + external locations</td></tr>
<tr><td>Global temp views</td><td><code>global_temp</code> schema (classic compute)</td><td>not on serverless — use views</td></tr>
</table>
""" + callout("legacy", "Exam questions may show <code>dbfs:/user/hive/warehouse/sales.db/orders</code>. Read it as "
              "“managed table <code>orders</code> in database <code>sales</code> of the legacy Hive metastore”. "
              "Migration to UC: <code>SYNC</code>, <code>CREATE TABLE … DEEP CLONE</code>, or UCX."))

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Key takeaways
# MAGIC 1. Unity Catalog = **one governance layer** for all workspaces in a region: access control, discovery, lineage, auditing.
# MAGIC 2. **Metastore (1 per region) → catalog → schema → tables / views / volumes / functions / models.**
# MAGIC    Storage credentials, external locations and connections live at the **metastore** level.
# MAGIC 3. Names: **`catalog.schema.object`**; a 2-part name is `schema.object` in the **current catalog**; `USE CATALOG`/`USE SCHEMA` set the context for the session.
# MAGIC 4. Discover metadata with **`SHOW` / `DESCRIBE … EXTENDED`**, **`information_schema`** and **Catalog Explorer**.
# MAGIC 5. Recognise the legacy **`hive_metastore`**, `dbfs:/user/hive/warehouse/<db>.db/` paths and table ACLs.
# MAGIC
# MAGIC ➡️ Next: **04-P2 · Managed vs External Tables and Storage**
