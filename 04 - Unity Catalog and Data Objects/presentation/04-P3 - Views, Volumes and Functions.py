# Databricks notebook source
# MAGIC %md
# MAGIC # 👓 04-P3 · Views, Volumes & Functions
# MAGIC **Section 04 — Unity Catalog & Data Objects** · supports **D3 Transformation**, **D2 Ingestion** (volumes) and **D7 Governance**
# MAGIC
# MAGIC | After this presentation you can… |
# MAGIC |---|
# MAGIC | Choose between a **view**, **temporary view**, **global temp view**, **materialized view** and a **table** |
# MAGIC | List the events that end a **Spark session** — and with it every temp view |
# MAGIC | Explain **volumes**: managed vs external, `/Volumes/…` paths, what they replace, privileges |
# MAGIC | Create and inspect **SQL** and **Python UDFs** stored in Unity Catalog |
# MAGIC | Know the `SHOW` / `DESCRIBE` / `DROP` commands for each object type |
# MAGIC
# MAGIC > ▶️ **Run all** to render the diagrams (a few seconds, any compute).

# COMMAND ----------

# MAGIC %run ../../Includes/_style

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · The view family

# COMMAND ----------

# DBTITLE 1,Slide · View types
show("""
<div class="kicker">Slide 1 · Saved queries, with different lifetimes</div>
<h2>Five kinds of "view" you should recognise</h2>
<table class="tbl">
<tr><th>Object</th><th>Create</th><th>Stored where</th><th>Data stored?</th><th>Lives until</th></tr>
<tr><td>👓 <b>View</b></td><td><code>CREATE VIEW cat.sch.v AS SELECT …</code></td><td>Unity Catalog</td><td>❌ query only</td><td><code>DROP VIEW</code></td></tr>
<tr><td>⏳ <b>Temporary view</b></td><td><code>CREATE TEMP VIEW v AS …</code><br><code>df.createOrReplaceTempView("v")</code></td><td>the Spark <b>session</b></td><td>❌</td><td>end of the session</td></tr>
<tr><td>🌐 <b>Global temp view</b> 🕰️</td><td><code>CREATE GLOBAL TEMP VIEW v AS …</code><br>query as <code>global_temp.v</code></td><td>the <b>cluster</b> (<code>global_temp</code> schema)</td><td>❌</td><td>cluster restart — classic compute only</td></tr>
<tr><td>💾 <b>Materialized view</b></td><td><code>CREATE MATERIALIZED VIEW cat.sch.mv AS …</code></td><td>Unity Catalog</td><td>✅ precomputed</td><td><code>DROP</code>; refreshed on demand / schedule / trigger</td></tr>
<tr><td>📐 <b>Metric view</b></td><td><code>CREATE VIEW … WITH METRICS LANGUAGE YAML AS $$ … $$</code></td><td>Unity Catalog</td><td>❌</td><td><code>DROP VIEW</code> — reusable measures &amp; dimensions for BI</td></tr>
</table>
""" + callout("info", "<b>Dynamic views</b> are ordinary views whose query uses <code>current_user()</code> / "
              "<code>is_account_group_member()</code> to hide rows or columns — Section 13. "
              "<b>Streaming tables</b> are tables, not views — Section 08.")
  + callout("tip", "Newer serverless runtimes and SQL warehouses also offer <b>temporary tables</b> "
            "(<code>CREATE TEMPORARY TABLE</code>): session-scoped like a temp view, but they <b>store</b> data. "
            "Global temp views are listed with <code>SHOW TABLES IN global_temp</code> / <code>SHOW VIEWS IN global_temp</code>.")
  + callout("exam", "<code>SHOW TABLES</code> lists views too (with <code>isTemporary</code>). "
            "<code>SHOW VIEWS</code> lists stored and temp views. Temp views have a <b>one-part</b> name and never appear "
            "in <code>information_schema</code> or Catalog Explorer."))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · Temporary views and the Spark session

# COMMAND ----------

# DBTITLE 1,Slide · Session lifecycle
show("""
<div class="kicker">Slide 2 · A temp view lives exactly as long as its Spark session</div>
<h2>What ends a session?</h2>
<div class="grid two">
 <div class="card green"><h3>✅ Temp view still there</h3><ul>
  <li>Running more cells in the <b>same</b> notebook</li>
  <li>Calling it from <b>SQL and Python</b> in that notebook (same session)</li>
  <li>A helper notebook included with <code>%run</code> (runs in the caller's session)</li></ul></div>
 <div class="card red"><h3>❌ Temp view gone / not visible</h3><ul>
  <li>Opening <b>another notebook</b> — each notebook has its own session</li>
  <li><code>dbutils.notebook.run()</code> — the child is a separate run</li>
  <li><b>Detaching and re-attaching</b> the notebook, or <b>restarting</b> the compute</li>
  <li>Serverless session expiring after inactivity</li>
  <li>Every <b>new job run</b></li></ul></div>
</div>
""" + callout("tip", "Need to share a result between notebooks or jobs? Use a <b>view</b> or a <b>table</b> in Unity Catalog — "
              "not a global temp view (unsupported on serverless).")
  + callout("trap", "Two notebooks attached to the <b>same classic cluster</b> still have <b>different</b> sessions: "
            "temp views aren't shared, global temp views are."))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · Stored views vs materialized views vs tables
# MAGIC
# MAGIC | | View | Materialized view | Table |
# MAGIC |---|---|---|---|
# MAGIC | Stores data | ❌ runs its query every time | ✅ stores the result | ✅ |
# MAGIC | Freshness | always current | as of **last refresh** | whatever you wrote |
# MAGIC | Cost profile | pays compute on every query | pays on refresh (often **incremental**), cheap reads | pays on writes |
# MAGIC | Updatable with DML | ❌ | ❌ | ✅ |
# MAGIC | Typical use | simple logic, security layer (share a subset), reuse | expensive aggregates for dashboards | everything else |
# MAGIC
# MAGIC **Materialized view facts to remember**
# MAGIC * `CREATE [OR REPLACE] MATERIALIZED VIEW …` from Databricks SQL or a serverless notebook; each MV is maintained by a **serverless pipeline** (same engine as Lakeflow Declarative Pipelines).
# MAGIC * Refresh: `REFRESH MATERIALIZED VIEW mv` · `SCHEDULE EVERY 1 HOUR` / `SCHEDULE CRON '…'` · `TRIGGER ON UPDATE` (when sources change).
# MAGIC * No time travel on an MV; you can't `INSERT`/`UPDATE` it — change its query instead.
# MAGIC * **View permissions:** a user needs `SELECT` on the view (+ `USE CATALOG`/`USE SCHEMA`), not on its sources — the **owner's** rights on the underlying tables are checked.
# MAGIC
# MAGIC ## 4 · Volumes — Unity Catalog for files

# COMMAND ----------

# DBTITLE 1,Slide · Volumes
show("""
<div class="kicker">Slide 4 · Volumes</div>
<h2>Governed storage for <b>non-tabular</b> data</h2>
<div class="flow">
 <div class="step"><b>📚 catalog</b></div><div class="arrow">▸</div>
 <div class="step"><b>🗂️ schema</b></div><div class="arrow">▸</div>
 <div class="step"><b>📁 volume</b></div><div class="arrow">➜</div>
 <div class="step"><b>/Volumes/&lt;catalog&gt;/&lt;schema&gt;/&lt;volume&gt;/path/file.csv</b>the file path you use everywhere</div>
</div>
<div class="grid two">
 <div class="card teal"><h3>🏠 Managed volume</h3>
  <code>CREATE VOLUME cat.sch.landing</code><br>stored in the schema's managed storage; <code>DROP VOLUME</code> removes it and Unity Catalog cleans up the files</div>
 <div class="card orange"><h3>🌍 External volume</h3>
  <code>CREATE EXTERNAL VOLUME cat.sch.raw LOCATION 's3://…'</code><br>files in an external location; <code>DROP VOLUME</code> keeps the files</div>
</div>
<table class="tbl">
<tr><th>Task</th><th>How</th></tr>
<tr><td>List files</td><td><code>LIST '/Volumes/…'</code> · <code>dbutils.fs.ls("/Volumes/…")</code> · Catalog Explorer</td></tr>
<tr><td>Read files</td><td><code>read_files('/Volumes/…', format =&gt; 'csv')</code> · <code>spark.read…load("/Volumes/…")</code> · <code>SELECT * FROM json.`/Volumes/…`</code> · plain Python <code>open()</code>, pandas</td></tr>
<tr><td>Write files</td><td><code>dbutils.fs.put/cp</code> · <code>df.write.csv/json/parquet("/Volumes/…")</code> · UI upload · <code>PUT INTO</code> (connectors/CLI)</td></tr>
<tr><td>Privileges</td><td><code>READ VOLUME</code>, <code>WRITE VOLUME</code> on the volume · <code>CREATE VOLUME</code> on the schema</td></tr>
</table>
""" + callout("exam", "Volumes replace the legacy <b>DBFS root</b> and <b>mounts</b> (<code>dbfs:/mnt/…</code>, <code>/FileStore</code>) "
              "for files: landing zones for ingestion, checkpoints, images, PDFs, libraries, ML artifacts.")
  + callout("trap", "Volumes hold <b>files</b>, not tables: you can't register a table on a volume path. Read the files "
            "and write a table (managed storage) instead."))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 · Functions in Unity Catalog

# COMMAND ----------

# DBTITLE 1,Slide · UDFs
show("""
<div class="kicker">Slide 5 · User-defined functions</div>
<h2>Reusable, governed logic: <code>catalog.schema.function</code></h2>
<div class="grid two">
 <div class="card purple"><h3>SQL UDF</h3>
<pre style="font-size:12px;margin:0">CREATE OR REPLACE FUNCTION sales.fx.to_eur(usd DOUBLE)
RETURNS DOUBLE
COMMENT 'USD to EUR at a fixed rate'
RETURN round(usd * 0.92, 2);

SELECT order_id, sales.fx.to_eur(total) FROM orders;</pre>
 Inlined by the optimizer → fast. Can also <b>return a table</b> (<code>RETURNS TABLE (…)</code>).</div>
 <div class="card"><h3>Python UDF</h3>
<pre style="font-size:12px;margin:0">CREATE OR REPLACE FUNCTION sales.fx.domain(email STRING)
RETURNS STRING
LANGUAGE PYTHON
AS $$
return email.split("@")[1] if email else None
$$;</pre>
 Runs in an isolated Python sandbox — flexible, slower than SQL. Serverless, SQL warehouses (pro/serverless), recent runtimes.</div>
</div>
<table class="tbl">
<tr><th>Command</th><th>Purpose</th></tr>
<tr><td><code>DESCRIBE FUNCTION EXTENDED f</code></td><td>signature, body, owner, comment, deterministic…</td></tr>
<tr><td><code>SHOW USER FUNCTIONS [IN sch]</code></td><td>your functions (<code>SHOW SYSTEM FUNCTIONS</code> = built-ins)</td></tr>
<tr><td><code>DROP FUNCTION [IF EXISTS] f</code></td><td>remove it</td></tr>
<tr><td><code>GRANT EXECUTE ON FUNCTION f TO `analysts`</code></td><td>let others call it</td></tr>
</table>
""" + callout("info", "<code>spark.udf.register('f', py_func)</code> and <code>CREATE TEMPORARY FUNCTION</code> create "
              "<b>session-scoped</b> functions — like temp views, they aren't stored in Unity Catalog. "
              "SQL UDFs and higher-order functions in depth: Section 07."))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6 · Command cheat sheet
# MAGIC
# MAGIC | Object | Create | Inspect | Remove |
# MAGIC |---|---|---|---|
# MAGIC | Catalog | `CREATE CATALOG [IF NOT EXISTS] c [MANAGED LOCATION '…'] [COMMENT '…']` | `DESCRIBE CATALOG EXTENDED c` · `SHOW CATALOGS` | `DROP CATALOG c [CASCADE]` |
# MAGIC | Schema | `CREATE SCHEMA c.s [MANAGED LOCATION '…'] [COMMENT '…']` | `DESCRIBE SCHEMA EXTENDED c.s` · `SHOW SCHEMAS IN c` | `DROP SCHEMA c.s [CASCADE]` |
# MAGIC | Table | `CREATE TABLE …` / CTAS (+ `LOCATION` = external) | `DESCRIBE EXTENDED` · `DESCRIBE DETAIL` · `SHOW TABLES` | `DROP TABLE` · `UNDROP TABLE` |
# MAGIC | View | `CREATE [OR REPLACE] [TEMP] VIEW` | `DESCRIBE EXTENDED` · `SHOW VIEWS` | `DROP VIEW` |
# MAGIC | Materialized view | `CREATE MATERIALIZED VIEW` | `DESCRIBE EXTENDED` | `DROP MATERIALIZED VIEW` |
# MAGIC | Volume | `CREATE [EXTERNAL] VOLUME` | `DESCRIBE VOLUME` · `SHOW VOLUMES` · `LIST '/Volumes/…'` | `DROP VOLUME` |
# MAGIC | Function | `CREATE [OR REPLACE] FUNCTION` | `DESCRIBE FUNCTION EXTENDED` · `SHOW USER FUNCTIONS` | `DROP FUNCTION` |
# MAGIC | Documentation | `COMMENT ON TABLE t IS '…'` (also `COMMENT ON SCHEMA`/`CATALOG`) · `ALTER TABLE t ALTER COLUMN c COMMENT '…'` | Catalog Explorer | — |
# MAGIC | Ownership / rename | `ALTER TABLE t OWNER TO data_eng` (also views, volumes, schemas, catalogs — not MVs) · `ALTER TABLE t RENAME TO t2` (tables, views, volumes — **schemas can't be renamed**) | `DESCRIBE EXTENDED` (Owner) · `SHOW CREATE TABLE t` | — |
# MAGIC
# MAGIC ## ✅ Key takeaways
# MAGIC 1. **View** = stored query in UC (no data) · **temp view** = session only · **global temp view** = cluster only, legacy, not serverless ·
# MAGIC    **materialized view** = stored, refreshed result.
# MAGIC 2. A temp view disappears with its **Spark session**: other notebooks, `dbutils.notebook.run`, detach/re-attach, compute restart, new job runs.
# MAGIC 3. **Volumes** govern **files** at `/Volumes/catalog/schema/volume/…` (managed or external) and replace DBFS mounts; privileges `READ VOLUME`/`WRITE VOLUME`.
# MAGIC 4. **UDFs** in UC (`CREATE FUNCTION`, SQL or Python) are permanent, shareable with `EXECUTE`; inspect with `DESCRIBE FUNCTION EXTENDED`.
# MAGIC
# MAGIC ➡️ Practice: **labs/04-L1**, **04-L2**, then **04-L3 Challenge** · Test yourself: **questions/04-Q**
