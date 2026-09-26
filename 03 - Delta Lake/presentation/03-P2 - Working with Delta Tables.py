# Databricks notebook source
# MAGIC %md
# MAGIC # 🛠️ 03-P2 · Working with Delta Tables
# MAGIC **Section 03 — Delta Lake** · feeds **D2 Ingestion (21 %)** and **D3 Transformation (22 %)**
# MAGIC
# MAGIC | After this presentation you can… |
# MAGIC |---|
# MAGIC | Create Delta tables with `CREATE TABLE`, CTAS and `CREATE OR REPLACE`, with comments, properties and constraints |
# MAGIC | Choose correctly between `INSERT INTO`, `INSERT OVERWRITE`, `CREATE OR REPLACE` and `MERGE` |
# MAGIC | Write `UPDATE`, `DELETE` and every `MERGE` clause |
# MAGIC | Explain schema **enforcement** vs **evolution** and how to allow evolution |
# MAGIC | Inspect tables with the `DESCRIBE` family |
# MAGIC
# MAGIC > ▶️ **Run all** to render the diagrams. Practice: **labs/03-L1** and **labs/03-L2**.

# COMMAND ----------

# MAGIC %run ../../Includes/_style

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · Creating Delta tables
# MAGIC
# MAGIC ```sql
# MAGIC -- 1) Explicit schema, empty table (Delta is the default: USING DELTA is optional)
# MAGIC CREATE TABLE sales.orders (
# MAGIC   order_id    STRING NOT NULL,
# MAGIC   order_ts    TIMESTAMP,
# MAGIC   total       DOUBLE,
# MAGIC   order_date  DATE GENERATED ALWAYS AS (CAST(order_ts AS DATE)),   -- generated column
# MAGIC   row_id      BIGINT GENERATED ALWAYS AS IDENTITY                  -- identity (surrogate key)
# MAGIC )
# MAGIC COMMENT 'Orders' TBLPROPERTIES ('quality' = 'silver');
# MAGIC
# MAGIC -- 2) CTAS: schema inferred from the query, table filled immediately
# MAGIC CREATE TABLE sales.big_orders
# MAGIC COMMENT 'Orders above 500' AS
# MAGIC SELECT * FROM sales.orders WHERE total > 500;
# MAGIC
# MAGIC -- 2b) Table clauses work with CTAS too
# MAGIC CREATE TABLE sales.orders_by_month
# MAGIC COMMENT 'Partitioned copy'
# MAGIC PARTITIONED BY (order_month)            -- or CLUSTER BY (col) for liquid clustering
# MAGIC TBLPROPERTIES ('owner_team' = 'sales')
# MAGIC -- LOCATION 's3://bucket/path'          -- ⇒ EXTERNAL table (needs an external location) → Section 04
# MAGIC AS SELECT *, date_format(order_ts, 'yyyy-MM') AS order_month FROM sales.orders;
# MAGIC
# MAGIC -- 3) Replace an existing table atomically (keeps the history!)
# MAGIC CREATE OR REPLACE TABLE sales.big_orders AS SELECT ...;
# MAGIC
# MAGIC -- 4) Only if it doesn't exist
# MAGIC CREATE TABLE IF NOT EXISTS sales.orders (...);
# MAGIC ```
# MAGIC
# MAGIC | | `CREATE TABLE (cols)` | CTAS |
# MAGIC |---|---|---|
# MAGIC | Schema | declared | inferred from `SELECT` (cast to control types) |
# MAGIC | Data | empty | loaded |
# MAGIC | Generated / identity columns | ✅ | ❌ (create the table first, then `INSERT … SELECT`) |
# MAGIC | Table clauses (`COMMENT`, `TBLPROPERTIES`, `PARTITIONED BY`, `CLUSTER BY`, `LOCATION`) | ✅ | ✅ |
# MAGIC | Typical use | contracts, constraints, generated columns | quick materialisation of a query |
# MAGIC
# MAGIC > ⚠️ **Exam trap:** `DROP TABLE` + `CREATE TABLE` **loses the history** (new table). `CREATE OR REPLACE TABLE` keeps it — you can still time-travel to versions before the replace.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · Constraints and special columns
# MAGIC
# MAGIC | Feature | Syntax | Enforced? |
# MAGIC |---|---|---|
# MAGIC | **NOT NULL** | `col STRING NOT NULL` at creation, or `ALTER TABLE t ALTER COLUMN col SET NOT NULL` | ✅ write fails |
# MAGIC | **CHECK** | `ALTER TABLE t ADD CONSTRAINT valid_price CHECK (price > 0)` (existing rows must pass) | ✅ write fails |
# MAGIC | **Primary / foreign key** | `CONSTRAINT pk PRIMARY KEY (id)` (Unity Catalog) | ❌ informational only |
# MAGIC | **Generated column** | `col TYPE GENERATED ALWAYS AS (expr)` | computed on write |
# MAGIC | **Identity column** | `id BIGINT GENERATED ALWAYS AS IDENTITY` | unique increasing values (not necessarily consecutive) |
# MAGIC
# MAGIC A violating write fails **entirely** (atomicity) — no rows of that transaction are written. `CHECK` constraints appear in
# MAGIC `SHOW TBLPROPERTIES` as `delta.constraints.<name>`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · 🎯 Writing data — which command when?

# COMMAND ----------

# DBTITLE 1,Slide · Write semantics
show("""
<div class="kicker">Slide 3 · The most tested table in this section</div>
<h2>Append, overwrite, replace or merge?</h2>
<table class="tbl">
<tr><th>Command</th><th>What happens</th><th>Schema change?</th><th>Idempotent on re-run?</th><th>Use it for</th></tr>
<tr><td><code>INSERT INTO t SELECT …</code><br><code>df.write.mode("append")</code></td><td>Adds rows</td><td>❌ (unless <code>mergeSchema</code> in the writer)</td><td>❌ duplicates on retry</td><td>New data you are sure isn't loaded yet</td></tr>
<tr><td><code>INSERT OVERWRITE t SELECT …</code><br><code>df.write.mode("overwrite")</code></td><td>Replaces all rows (or only matching partitions)</td><td>❌ must match the current schema (writer: <code>overwriteSchema</code>)</td><td>✅</td><td>Full reloads of a table whose schema is stable</td></tr>
<tr><td><code>CREATE OR REPLACE TABLE t AS SELECT …</code></td><td>Replaces data <b>and</b> schema/properties atomically</td><td>✅</td><td>✅</td><td>Rebuilding a table when the definition changes</td></tr>
<tr><td><code>MERGE INTO t USING s ON …</code></td><td>Update / insert / delete by key</td><td>only with <code>WITH SCHEMA EVOLUTION</code></td><td>✅ when written carefully</td><td>Upserts, CDC, dedup-on-load, SCD</td></tr>
<tr><td><code>COPY INTO t FROM 'path'</code></td><td>Loads only files not loaded before</td><td>with <code>mergeSchema</code> option</td><td>✅ (tracks files)</td><td>Incremental file ingestion → Section 05</td></tr>
</table>
""" + callout("trap", "All of these create a <b>new version</b>; the previous data is still available through time travel until it is VACUUMed. "
              "<code>INSERT OVERWRITE</code> does <b>not</b> drop the history.")
    + callout("tip", "In SQL, an overwrite with a different schema is done with <code>CREATE OR REPLACE TABLE</code>; in Python with "
              "<code>df.write.mode(\"overwrite\").option(\"overwriteSchema\", \"true\")</code>."))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Overwriting only part of a table
# MAGIC ```sql
# MAGIC -- Replace only the rows that match a predicate (atomic delete + insert)
# MAGIC -- (sales.daily_orders: order_id, order_date, total — no identity/generated columns)
# MAGIC INSERT INTO sales.daily_orders REPLACE WHERE order_date >= '2026-09-01'
# MAGIC SELECT order_id, order_date, total FROM staging_orders WHERE order_date >= '2026-09-01';
# MAGIC ```
# MAGIC ```python
# MAGIC (df.select("order_id", "order_date", "total")
# MAGIC    .write.mode("overwrite")
# MAGIC    .option("replaceWhere", "order_date >= '2026-09-01'")   # same idea in Python
# MAGIC    .saveAsTable("sales.daily_orders"))
# MAGIC ```
# MAGIC Useful to **re-process one day or month idempotently** without touching the rest of the table.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · UPDATE, DELETE and MERGE
# MAGIC
# MAGIC ```sql
# MAGIC UPDATE sales.products SET price = price * 1.1 WHERE category = 'Electronics';
# MAGIC
# MAGIC DELETE FROM sales.customers WHERE customer_id = 'C0042';        -- e.g. GDPR request
# MAGIC
# MAGIC MERGE INTO sales.customers AS t                                   -- target
# MAGIC USING customer_updates    AS s                                    -- source (table, view or subquery)
# MAGIC ON t.customer_id = s.customer_id                                  -- match condition (keys)
# MAGIC WHEN MATCHED AND s.op = 'DELETE'       THEN DELETE
# MAGIC WHEN MATCHED AND s.updated > t.updated THEN UPDATE SET email = s.email, updated = s.updated
# MAGIC WHEN NOT MATCHED                       THEN INSERT (customer_id, email, updated)      -- = NOT MATCHED BY TARGET
# MAGIC                                             VALUES (s.customer_id, s.email, s.updated)
# MAGIC WHEN NOT MATCHED BY SOURCE             THEN UPDATE SET active = false;  -- target rows absent from the source
# MAGIC ```
# MAGIC
# MAGIC | MERGE fact | Detail |
# MAGIC |---|---|
# MAGIC | Clause order | Multiple `WHEN MATCHED` / `WHEN NOT MATCHED` clauses are evaluated in order; each needs a condition except the last one |
# MAGIC | `UPDATE SET *` / `INSERT *` | Shorthand for "all **target** columns = source columns with the same name". The source must contain every target column; extra source columns are ignored (unless schema evolution is on) |
# MAGIC | Duplicate source keys | If several source rows match the same target row in an UPDATE/DELETE clause → **error**. Deduplicate the source first |
# MAGIC | Insert-only MERGE | `WHEN NOT MATCHED THEN INSERT *` only → a safe, idempotent way to append without duplicates |
# MAGIC | Metrics | The statement returns `num_affected_rows`, `num_updated_rows`, `num_deleted_rows`, `num_inserted_rows`; `DESCRIBE HISTORY` → `numTargetRowsUpdated`, `numTargetRowsInserted`, `numTargetRowsDeleted` |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 · Schema enforcement vs schema evolution

# COMMAND ----------

# DBTITLE 1,Slide · Enforcement vs evolution
show("""
<div class="kicker">Slide 5 · Schema management</div>
<h2>The gatekeeper and the escape hatch</h2>
<div class="grid two">
 <div class="card red"><h3>🛡️ Schema enforcement (default)</h3>
  <ul><li>Writes with an <b>unknown extra column</b> → rejected</li>
  <li>Incompatible <b>data type</b> (e.g. STRING into DOUBLE) → rejected</li>
  <li><b>Missing</b> nullable columns → allowed, filled with <code>NULL</code></li>
  <li>Protects downstream consumers from silent breakage</li></ul></div>
 <div class="card green"><h3>🌱 Schema evolution (opt-in)</h3>
  <ul><li><code>df.write.option("mergeSchema", "true")</code> → add new columns on append</li>
  <li><code>MERGE WITH SCHEMA EVOLUTION INTO …</code></li>
  <li><code>ALTER TABLE t ADD COLUMNS (rating DOUBLE)</code></li>
  <li><code>mode("overwrite").option("overwriteSchema", "true")</code> → replace the schema</li>
  <li>Auto Loader schema evolution → Section 06</li></ul></div>
</div>
""" + callout("exam", "\"A new column appeared in the source and the append fails\" → enable <b>mergeSchema</b> (or evolve the table explicitly). "
              "\"Protect the table from unexpected columns\" → that's the default behaviour (<b>enforcement</b>)."))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6 · The DESCRIBE family
# MAGIC
# MAGIC | Command | Shows |
# MAGIC |---|---|
# MAGIC | `DESCRIBE t` | columns and types |
# MAGIC | `DESCRIBE EXTENDED t` / `DESCRIBE TABLE EXTENDED t` | + catalog info: owner, type (MANAGED/EXTERNAL), location, provider, properties |
# MAGIC | `DESCRIBE DETAIL t` | Delta physical details: **format, location, numFiles, sizeInBytes**, partition/clustering columns, table features |
# MAGIC | `DESCRIBE HISTORY t` | one row per commit: version, timestamp, user, **operation**, parameters, **metrics** |
# MAGIC | `SHOW TBLPROPERTIES t` | properties incl. `delta.*` settings and constraints |
# MAGIC | `SHOW CREATE TABLE t` | the DDL to recreate the table |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7 · Change Data Feed (preview — used in Sections 06/08)
# MAGIC
# MAGIC ```sql
# MAGIC ALTER TABLE sales.customers SET TBLPROPERTIES (delta.enableChangeDataFeed = true);
# MAGIC SELECT * FROM table_changes('sales.customers', 5);   -- row-level changes since version 5
# MAGIC ```
# MAGIC Each row carries `_change_type` (`insert`, `update_preimage`, `update_postimage`, `delete`), `_commit_version` and
# MAGIC `_commit_timestamp` — perfect for propagating changes incrementally downstream.

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Key takeaways
# MAGIC 1. `CREATE TABLE (schema)` = empty table with a contract; **CTAS** = inferred schema + data; `CREATE OR REPLACE` keeps history.
# MAGIC 2. `NOT NULL` and `CHECK` are **enforced**; PK/FK are **informational**. Violations reject the **whole** write.
# MAGIC 3. `INSERT INTO` appends (not idempotent) · `INSERT OVERWRITE` replaces data with the **same schema** · `CREATE OR REPLACE` can change the schema · `MERGE` upserts by key.
# MAGIC 4. MERGE fails if **several source rows match one target row** — deduplicate the source.
# MAGIC 5. Schema **enforcement** is the default; **evolution** is opt-in (`mergeSchema`, `WITH SCHEMA EVOLUTION`, `ALTER TABLE`).
# MAGIC
# MAGIC ➡️ Next: **03-P3 · Time Travel, Clones & Maintenance**
