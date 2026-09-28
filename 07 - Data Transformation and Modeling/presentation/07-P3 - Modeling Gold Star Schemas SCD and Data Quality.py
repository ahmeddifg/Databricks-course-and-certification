# Databricks notebook source
# MAGIC %md
# MAGIC # 🥇 07-P3 · Modeling Gold: Star Schemas, SCD and Data Quality
# MAGIC **Section 07 — Data Transformation & Modeling** · exam domain **D3 (22 %)** · objectives: *understand the difference
# MAGIC between, and how to build, gold layer objects — materialized views, views, streaming tables and tables — for BI and
# MAGIC analytics teams in Unity Catalog* · *apply data quality checks and validation rules to ensure reliable silver and gold
# MAGIC datasets*
# MAGIC
# MAGIC | After this presentation you can… |
# MAGIC |---|
# MAGIC | Choose the right **gold object**: table, view, materialized view or streaming table |
# MAGIC | Design a **star schema** (facts, dimensions, grain) or a **denormalized** gold table |
# MAGIC | Explain **SCD type 1 vs type 2** and implement both with `MERGE` |
# MAGIC | Define **data quality rules** and choose a strategy: fix, drop, quarantine, flag or fail |
# MAGIC | Enforce rules with **Delta constraints** and know where **expectations** fit |
# MAGIC | Choose between **built-in functions, SQL UDFs, Python UDFs and pandas UDFs** |
# MAGIC
# MAGIC > ▶️ **Run all** to render the diagrams (a few seconds, any compute).

# COMMAND ----------

# MAGIC %run ../../Includes/_style

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · Gold objects in Unity Catalog

# COMMAND ----------

# DBTITLE 1,Slide · Gold objects
show("""
<div class="kicker">Slide 1 · Four ways to publish gold data</div>
<h2>Table, view, materialized view or streaming table?</h2>
<table class="tbl">
<tr><th>Object</th><th>Created with</th><th>Stores data</th><th>Up to date when…</th><th>Best for</th></tr>
<tr><td><b>Table</b> (managed Delta)</td><td><code>CREATE TABLE … AS</code> · <code>saveAsTable</code> · <code>MERGE</code></td><td>✅</td><td>your job rewrites / merges it</td><td>full control, complex or non-SQL logic, SCD dimensions</td></tr>
<tr><td><b>View</b></td><td><code>CREATE VIEW … AS SELECT</code></td><td>❌ query only</td><td>always — computed at query time</td><td>light logic, reuse, <b>security</b> (expose some columns/rows)</td></tr>
<tr><td><b>Materialized view</b></td><td><code>CREATE MATERIALIZED VIEW … AS SELECT</code> (or in a pipeline)</td><td>✅ precomputed</td><td>on <code>REFRESH</code> / schedule / source update — <b>incremental</b> when possible</td><td>expensive aggregations read often by BI</td></tr>
<tr><td><b>Streaming table</b></td><td><code>CREATE STREAMING TABLE … AS SELECT … FROM STREAM …</code></td><td>✅</td><td>each refresh processes <b>only new</b> source rows</td><td>append-only ingestion and transformations (bronze/silver), low latency</td></tr>
</table>
""" + callout("exam", "Dashboard reads the same heavy aggregate many times → <b>materialized view</b> (precomputed). "
              "Needs to be always current and cheap to define → <b>view</b>. New rows arrive continuously → <b>streaming table</b>. "
              "Custom logic / SCD merges → <b>table</b> written by a job.")
  + callout("info", "Materialized views and streaming tables are maintained by <b>Lakeflow Spark Declarative Pipelines</b> — created "
            "in a pipeline, from a Databricks SQL warehouse, or from a serverless notebook (each runs a serverless pipeline for you). "
            "Section 08 builds them."))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Making gold usable for BI teams
# MAGIC * **Names & docs:** `COMMENT ON TABLE …`, column comments, tags — they show up in Catalog Explorer and help Genie / AI search.
# MAGIC * **Access:** ``GRANT SELECT ON TABLE gold.daily_sales TO `analysts` `` (or on the schema); a **view** can expose only some columns/rows (Section 13).
# MAGIC * **Serving:** SQL warehouses and AI/BI dashboards read gold tables directly (Section 09).
# MAGIC
# MAGIC ## 2 · Dimensional modeling

# COMMAND ----------

# DBTITLE 1,Slide · Star schema
show("""
<div class="kicker">Slide 2 · Star schema</div>
<h2>Facts in the middle, dimensions around them</h2>
<div class="grid">
 <div class="card"><h3>📅 dim_date</h3>date_key, date, month, year, is_weekend…</div>
 <div class="card"><h3>👤 dim_customer</h3>customer_id, name, city, country (+ SCD2 columns)</div>
 <div class="card orange"><h3>🧾 fact_sales</h3><b>grain: one row per order line</b><br>keys: date_key, customer_id, product_id<br>measures: quantity, amount</div>
 <div class="card"><h3>📦 dim_product</h3>product_id, title, brand, category, price</div>
</div>
<table class="tbl">
<tr><th></th><th>Fact table</th><th>Dimension table</th></tr>
<tr><td>Contains</td><td>events / transactions: <b>measures</b> (numbers you sum) + foreign keys</td><td>descriptive <b>attributes</b> you filter and group by</td></tr>
<tr><td>Size</td><td>large, grows fast</td><td>small to medium, changes slowly</td></tr>
<tr><td>Decide first</td><td>the <b>grain</b> — what one row means</td><td>the key (natural id or surrogate key, e.g. an identity column)</td></tr>
</table>
""" + callout("tip", "Give each dimension an <b>unknown member</b> (e.g. <code>customer_id = 'UNKNOWN'</code>) so facts with a missing or "
              "unmatched key aren't dropped by inner joins.")
  + callout("info", "Alternative: a <b>denormalized</b> (“one big table”) gold table with all attributes pre-joined — the simplest for BI "
            "tools, at the cost of storage and harder updates. Many lakehouses serve both."))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · Slowly changing dimensions (SCD)

# COMMAND ----------

# DBTITLE 1,Slide · SCD 1 vs 2
show("""
<div class="kicker">Slide 3 · A customer moves from Riyadh to Dubai on 2026-04-01</div>
<h2>Overwrite (type 1) or keep history (type 2)?</h2>
<div class="grid two">
 <div class="card"><h3>SCD type 1 — overwrite</h3>
 <table class="tbl"><tr><th>customer_id</th><th>city</th></tr><tr><td>C0042</td><td>Dubai</td></tr></table>
 <ul><li>one row per key, history <b>lost</b></li><li>one <code>MERGE … WHEN MATCHED THEN UPDATE SET *</code></li>
 <li>corrections, attributes nobody analyses over time</li></ul></div>
 <div class="card green"><h3>SCD type 2 — new version</h3>
 <table class="tbl"><tr><th>customer_id</th><th>city</th><th>valid_from</th><th>valid_to</th><th>is_current</th></tr>
 <tr><td>C0042</td><td>Riyadh</td><td>2026-01-01</td><td>2026-04-01</td><td>false</td></tr>
 <tr><td>C0042</td><td>Dubai</td><td>2026-04-01</td><td>NULL</td><td>true</td></tr></table>
 <ul><li>one row per <b>version</b>, history kept</li><li>facts can be reported with the value <b>valid at the time</b> (point-in-time join)</li></ul></div>
</div>
""" + callout("exam", "“Track the full history of changes to customer addresses” → <b>SCD type 2</b>. "
              "“Only the current value matters, overwrite it” → <b>SCD type 1</b>."))

# COMMAND ----------

# MAGIC %md
# MAGIC ### SCD type 2 with one `MERGE` — the merge-key trick
# MAGIC A changed customer needs **two** actions (close the old row **and** insert the new one), but a MERGE does one action per
# MAGIC source row. So the source contains every update **twice**: once with `merge_key = customer_id` (matches the current row →
# MAGIC close it) and once with `merge_key = NULL` (never matches → insert the new version).
# MAGIC
# MAGIC ```sql
# MAGIC MERGE INTO dim_customer AS t
# MAGIC USING (
# MAGIC   SELECT u.customer_id AS merge_key, u.* FROM updates u
# MAGIC   UNION ALL
# MAGIC   SELECT NULL AS merge_key, u.*                                     -- second copy only for real changes
# MAGIC   FROM updates u JOIN dim_customer d ON u.customer_id = d.customer_id AND d.is_current
# MAGIC   WHERE NOT (d.city <=> u.city) OR NOT (d.email <=> u.email)
# MAGIC ) AS s
# MAGIC ON t.customer_id = s.merge_key AND t.is_current
# MAGIC WHEN MATCHED AND (NOT (t.city <=> s.city) OR NOT (t.email <=> s.email)) THEN
# MAGIC   UPDATE SET is_current = false, valid_to = s.updated_at             -- close the old version
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (customer_id, city, email, valid_from, valid_to, is_current)
# MAGIC   VALUES (s.customer_id, s.city, s.email, s.updated_at, NULL, true)  -- new version or new customer
# MAGIC ```
# MAGIC
# MAGIC | Detail | Why |
# MAGIC |---|---|
# MAGIC | `<=>` (null-safe) instead of `<>` | `NULL <> 'x'` is NULL → a change from/to NULL would be **missed** |
# MAGIC | change condition on `WHEN MATCHED` | rows that change nothing must not create versions |
# MAGIC | `AND t.is_current` in `ON` | only the current version may be closed |
# MAGIC | One source row per key per batch | otherwise MERGE fails: multiple source rows matched the same target row |
# MAGIC
# MAGIC > 🧰 In Lakeflow pipelines you don't hand-write this: `AUTO CDC INTO … STORED AS SCD TYPE 2` (formerly
# MAGIC > `APPLY CHANGES INTO`) manages `__START_AT` / `__END_AT` for you — Section 08.
# MAGIC
# MAGIC ## 4 · Data quality

# COMMAND ----------

# DBTITLE 1,Slide · Data quality rules and strategies
show("""
<div class="kicker">Slide 4 · Reliable silver and gold</div>
<h2>Rules describe good data — strategies decide what happens to bad data</h2>
<div class="grid">
 <div class="card"><h3>Typical rules</h3><ul>
  <li><b>completeness</b>: key / required fields not NULL</li>
  <li><b>validity</b>: ranges, allowed values, formats</li>
  <li><b>uniqueness</b>: one row per key</li>
  <li><b>referential integrity</b>: order exists for payment</li>
  <li><b>consistency</b>: delivered_at ≥ shipped_at</li>
  <li><b>freshness</b>: latest data not older than X</li></ul></div>
 <div class="card green"><h3>Strategies</h3><ul>
  <li><b>fix</b> — known correction (default currency)</li>
  <li><b>drop</b> — useless row (no key)</li>
  <li><b>quarantine</b> — move aside with the reason</li>
  <li><b>flag / warn</b> — keep, mark, count</li>
  <li><b>fail</b> — stop the pipeline</li></ul></div>
 <div class="card orange"><h3>Quarantine pattern</h3>
 <code>dq_errors = array_compact(array(when(rule1, 'rule1'), when(rule2, 'rule2')))</code><br>
 valid = <code>size(dq_errors) = 0</code> · quarantine = the rest<br><b>valid + quarantine = input</b></div>
</div>
""" + callout("tip", "Produce a small <b>DQ report</b> (failures per rule) on every run and keep it as a table — trends show when "
              "a source starts to degrade."))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Where to enforce rules
# MAGIC
# MAGIC | Mechanism | Where | On violation |
# MAGIC |---|---|---|
# MAGIC | Rules in your transformation code | PySpark / SQL job | whatever you code (fix, drop, quarantine…) |
# MAGIC | `NOT NULL` constraint | Delta table: `ALTER TABLE t ALTER COLUMN c SET NOT NULL` | the write **fails** (whole transaction) |
# MAGIC | `CHECK` constraint | `ALTER TABLE t ADD CONSTRAINT name CHECK (amount >= 0)` | the write **fails**; can't be added while existing rows violate it |
# MAGIC | `PRIMARY KEY` / `FOREIGN KEY` | Unity Catalog | **informational only** — not enforced (the optimizer uses them only when declared with `RELY`) |
# MAGIC | **Expectations** | Lakeflow Spark Declarative Pipelines | `EXPECT (…)` warn (default) · `ON VIOLATION DROP ROW` · `ON VIOLATION FAIL UPDATE` — metrics in the event log (Section 08) |
# MAGIC
# MAGIC ## 5 · User-defined functions

# COMMAND ----------

# DBTITLE 1,Slide · UDF choices
show("""
<div class="kicker">Slide 5 · Built-in vs SQL UDF vs Python UDF</div>
<h2>Prefer built-ins; reuse logic with SQL UDFs</h2>
<table class="tbl">
<tr><th>Kind</th><th>Example</th><th>Execution</th><th>Performance</th></tr>
<tr><td><b>Built-in function</b></td><td><code>regexp_replace(lower(trim(m)), '[ -]+', '_')</code></td><td>optimised by Catalyst, Photon</td><td>⚡⚡⚡</td></tr>
<tr><td><b>SQL UDF</b> (UC function)</td><td><code>CREATE FUNCTION clean_method(m STRING) RETURNS STRING RETURN …</code></td><td><b>inlined</b> into the plan</td><td>⚡⚡⚡</td></tr>
<tr><td><b>pandas UDF</b></td><td><code>@F.pandas_udf("double")</code></td><td>batches via Apache Arrow</td><td>⚡⚡</td></tr>
<tr><td><b>Python UDF</b></td><td><code>@F.udf("string")</code> · <code>CREATE FUNCTION … LANGUAGE PYTHON</code></td><td>row by row in a Python process</td><td>⚡</td></tr>
</table>
""" + callout("exam", "SQL UDFs are Unity Catalog objects (<code>catalog.schema.function</code>): governed with <code>GRANT EXECUTE</code>, "
              "inspected with <code>DESCRIBE FUNCTION EXTENDED</code>, removed with <code>DROP FUNCTION</code>.")
  + callout("trap", "A Python UDF is a black box for the optimizer: no predicate pushdown through it and data must be serialised "
            "to Python and back. Use it only when no built-in or SQL expression can do the job."))

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Key takeaways
# MAGIC 1. Gold objects: **table** (you write it) · **view** (query, always current, no storage) · **materialized view** (precomputed, refreshed, incremental when possible) · **streaming table** (processes only new rows).
# MAGIC 2. **Star schema**: facts (measures + keys, defined **grain**) + dimensions (attributes); unknown members; denormalized tables for simple BI.
# MAGIC 3. **SCD 1** overwrites; **SCD 2** closes the current row and inserts a new version (`valid_from`, `valid_to`, `is_current`) — one `MERGE` with the **merge-key trick** and **null-safe** `<=>` comparisons.
# MAGIC 4. Data quality: rules (completeness, validity, uniqueness, referential integrity, consistency) + strategy (fix, drop, **quarantine**, flag, fail); keep a DQ report.
# MAGIC 5. Delta **NOT NULL / CHECK** constraints reject bad writes as a whole; PK/FK are informational; pipeline **expectations** warn / drop / fail.
# MAGIC 6. Built-ins > **SQL UDF** (inlined, governed in UC) > pandas UDF > Python UDF.
# MAGIC
# MAGIC ➡️ Practice: **07-L1 … 07-L4** · then check yourself with **07-Q · Exam Questions**
