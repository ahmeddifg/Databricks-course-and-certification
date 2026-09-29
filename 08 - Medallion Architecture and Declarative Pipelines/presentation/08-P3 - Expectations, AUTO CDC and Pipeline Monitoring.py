# Databricks notebook source
# MAGIC %md
# MAGIC # 🛡️ 08-P3 · Expectations, AUTO CDC and Pipeline Monitoring
# MAGIC **Section 08** · exam objectives **3.7** (data quality checks & validation rules), **3.x** (SCD / CDC modeling),
# MAGIC **6.x** (monitor and troubleshoot pipelines)
# MAGIC
# MAGIC | After this presentation you can… |
# MAGIC |---|
# MAGIC | Write **expectations** in SQL and Python and predict what **warn / drop / fail** do |
# MAGIC | Build a **quarantine** with inverted expectations |
# MAGIC | Apply a CDC feed with **`AUTO CDC`** into **SCD type 1** and **type 2** tables — including deletes and late events |
# MAGIC | Read the pipeline **event log**: data-quality metrics, lineage, update history, audit |
# MAGIC | Troubleshoot a failed pipeline update |
# MAGIC
# MAGIC > ▶️ **Run all** to render the diagrams.

# COMMAND ----------

# MAGIC %run ../../Includes/_style

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · Expectations

# COMMAND ----------

# DBTITLE 1,Slide · Anatomy of an expectation
show("""
<div class="kicker">Slide 1 · Data-quality rules that are part of the dataset definition</div>
<h2>name + boolean condition + action</h2>
<p><code>CONSTRAINT <b>valid_rating</b> EXPECT (<b>rating BETWEEN 1 AND 5</b>) <b>ON VIOLATION DROP ROW</b></code></p>
<table class="tbl">
<tr><th>Action</th><th>SQL</th><th>Python</th><th>Invalid row</th><th>Update</th><th>Metrics</th></tr>
<tr><td><b>warn</b> (default)</td><td><code>EXPECT (cond)</code></td><td><code>@dp.expect</code></td><td>✅ <b>written</b></td><td>continues</td><td>✅ passed / failed counts</td></tr>
<tr><td><b>drop</b></td><td><code>… ON VIOLATION DROP ROW</code></td><td><code>@dp.expect_or_drop</code></td><td>❌ removed before writing</td><td>continues</td><td>✅ dropped counts</td></tr>
<tr><td><b>fail</b></td><td><code>… ON VIOLATION FAIL UPDATE</code></td><td><code>@dp.expect_or_fail</code></td><td>—</td><td>❌ <b>fails</b>, the flow's transaction is <b>rolled back</b></td><td>❌ (update didn't complete)</td></tr>
</table>
<div class="grid">
 <div class="card"><h3>Where</h3>streaming tables, materialized views, temporary views (also standalone MVs/STs in Databricks SQL)</div>
 <div class="card green"><h3>Many at once</h3><code>@dp.expect_all({...})</code>, <code>expect_all_or_drop</code>, <code>expect_all_or_fail</code> take a <b>dict</b> name → condition</div>
 <div class="card orange"><h3>Conditions</h3>any SQL boolean expression — no subqueries, no Python UDFs, no calls to external services</div>
</div>
""" + callout("exam", "“Keep the record but track it” → <b>warn</b> (no ON VIOLATION). “Remove bad records” → <b>DROP ROW</b>. "
              "“Stop processing on bad data” → <b>FAIL UPDATE</b>. Metrics are in the pipeline UI (Data quality tab) and the <b>event log</b>."))

# COMMAND ----------

# MAGIC %md
# MAGIC ```sql
# MAGIC CREATE OR REFRESH STREAMING TABLE orders_silver (
# MAGIC   CONSTRAINT valid_id      EXPECT (order_id IS NOT NULL) ON VIOLATION FAIL UPDATE,
# MAGIC   CONSTRAINT not_cancelled EXPECT (quantity > 0)         ON VIOLATION DROP ROW,
# MAGIC   CONSTRAINT has_country   EXPECT (country IS NOT NULL)
# MAGIC )
# MAGIC AS SELECT … FROM STREAM(orders_bronze);
# MAGIC ```
# MAGIC
# MAGIC ```python
# MAGIC RULES = {"valid_rating": "rating BETWEEN 1 AND 5", "has_product": "product_id IS NOT NULL"}
# MAGIC
# MAGIC @dp.table
# MAGIC @dp.expect_all_or_drop(RULES)                 # one dict, reused by several tables
# MAGIC @dp.expect("has_comment", "length(comment) > 0")
# MAGIC def ratings_silver():
# MAGIC     return spark.readStream.table("ratings_bronze")
# MAGIC ```
# MAGIC
# MAGIC ## 2 · Quarantine: keep the bad rows somewhere useful
# MAGIC DROP ROW throws rows away. To **inspect or repair** them, write valid and invalid rows to two tables with the same rules:
# MAGIC
# MAGIC ```python
# MAGIC RULES = {"valid_rating": "rating BETWEEN 1 AND 5", "has_product": "product_id IS NOT NULL"}
# MAGIC ALL_VALID = " AND ".join(f"({c})" for c in RULES.values())
# MAGIC
# MAGIC @dp.table
# MAGIC @dp.expect_all_or_drop(RULES)
# MAGIC def ratings_valid():
# MAGIC     return spark.readStream.table("ratings_bronze")
# MAGIC
# MAGIC @dp.table
# MAGIC def ratings_quarantine():                    # the inverse: rows that break at least one rule
# MAGIC     return spark.readStream.table("ratings_bronze").where(f"NOT ({ALL_VALID})")
# MAGIC ```
# MAGIC
# MAGIC | | Pipeline **expectation** | Delta **CHECK constraint** (Section 03) |
# MAGIC |---|---|---|
# MAGIC | Defined on | a pipeline dataset | any Delta table (`ALTER TABLE … ADD CONSTRAINT … CHECK`) |
# MAGIC | Violation | warn / drop / fail — your choice | the **whole write fails** |
# MAGIC | Metrics | ✅ event log & UI | ❌ |
# MAGIC
# MAGIC > ⚠️ Mind **NULLs**: `rating BETWEEN 1 AND 5` evaluates to NULL — not false — when `rating` is NULL (and `NOT (NULL)` is
# MAGIC > NULL too, so such a row also misses the quarantine filter above). Don't leave it to chance: write the rule explicitly,
# MAGIC > e.g. `rating IS NOT NULL AND rating BETWEEN 1 AND 5`, or `CASE WHEN col IS NOT NULL THEN col > 0 ELSE TRUE END` when a
# MAGIC > NULL is acceptable.
# MAGIC
# MAGIC ## 3 · Change data capture with `AUTO CDC`

# COMMAND ----------

# DBTITLE 1,Slide · The CDC problem
show("""
<div class="kicker">Slide 3 · Applying a change feed is harder than it looks</div>
<h2>INSERT / UPDATE / DELETE events → a table that reflects the source</h2>
<div class="grid">
 <div class="card red"><h3>⏱️ Out of order</h3>an older update arrives after a newer one — "last arrived wins" is wrong</div>
 <div class="card orange"><h3>🗑️ Deletes</h3>must remove (SCD1) or close (SCD2) the row</div>
 <div class="card purple"><h3>♻️ Several changes per key in one batch</h3>a plain <code>MERGE</code> fails when many source rows match one target row</div>
 <div class="card gray"><h3>📜 History</h3>SCD2 needs start/end markers per version — lots of hand-written MERGE logic</div>
</div>
<div class="flow">
 <div class="step"><b>CDC source</b>streaming table / Kafka / CDF</div><div class="arrow">➜</div>
 <div class="step"><b>AUTO CDC flow</b>KEYS · SEQUENCE BY · APPLY AS DELETE</div><div class="arrow">➜</div>
 <div class="step"><b>SCD type 1</b>current state</div>
 <div class="step"><b>SCD type 2</b>full history</div>
</div>
""" + callout("legacy", "<code>AUTO CDC</code> replaces <b>APPLY CHANGES</b> (<code>APPLY CHANGES INTO</code>, <code>dlt.apply_changes()</code>) — same syntax, new name."))

# COMMAND ----------

# MAGIC %md
# MAGIC ```sql
# MAGIC CREATE OR REFRESH STREAMING TABLE customers_scd2;                 -- 1) the target must exist first
# MAGIC
# MAGIC CREATE FLOW customers_cdc AS AUTO CDC INTO customers_scd2         -- 2) the flow that applies the changes
# MAGIC FROM STREAM(customers_cdc_clean)                                  --    the source must be read as a stream
# MAGIC KEYS (customer_id)                                                --    primary key of the target
# MAGIC APPLY AS DELETE WHEN operation = 'DELETE'                         --    which events are deletes
# MAGIC SEQUENCE BY sequence_num                                          --    logical order of the events (not NULL, sortable)
# MAGIC COLUMNS * EXCEPT (operation, sequence_num)                        --    don't store the CDC metadata
# MAGIC STORED AS SCD TYPE 2                                              --    1 = overwrite (default), 2 = keep history
# MAGIC TRACK HISTORY ON * EXCEPT (change_ts);                            --    SCD2: which columns open a new version
# MAGIC ```
# MAGIC
# MAGIC ```python
# MAGIC dp.create_streaming_table("customers_scd1")
# MAGIC dp.create_auto_cdc_flow(
# MAGIC     target="customers_scd1", source="customers_cdc_clean", keys=["customer_id"],
# MAGIC     sequence_by=F.col("sequence_num"),
# MAGIC     apply_as_deletes=F.expr("operation = 'DELETE'"),
# MAGIC     except_column_list=["operation", "sequence_num"],
# MAGIC     stored_as_scd_type=1)                      # SCD2: stored_as_scd_type=2, track_history_except_column_list=[...]
# MAGIC ```
# MAGIC
# MAGIC | Clause | Meaning |
# MAGIC |---|---|
# MAGIC | `KEYS` | columns that identify a row |
# MAGIC | `SEQUENCE BY` | orders the events **per key**; late events never overwrite newer ones |
# MAGIC | `APPLY AS DELETE WHEN` | condition that marks a delete |
# MAGIC | `APPLY AS TRUNCATE WHEN` | condition that empties the table (**SCD1 only**) |
# MAGIC | `IGNORE NULL UPDATES` | NULLs in an update keep the existing values (partial updates) |
# MAGIC | `COLUMNS` / `COLUMNS * EXCEPT` | which columns land in the target |
# MAGIC | `STORED AS SCD TYPE 1` or `2` | default **1** |
# MAGIC | `TRACK HISTORY ON` | SCD2: only these columns create a new version; others are updated in place |
# MAGIC
# MAGIC **Snapshots instead of events?** Python `dp.create_auto_cdc_from_snapshot_flow()` compares consecutive full snapshots and
# MAGIC derives the changes (AUTO CDC FROM SNAPSHOT). Requirement: **serverless** pipelines or the **Pro / Advanced** edition.

# COMMAND ----------

# DBTITLE 1,Slide · SCD1 vs SCD2 with a late event
show("""
<div class="kicker">Slide 4 · Customer C0018 in 08-L3 — event 1015 arrives <i>after</i> event 1020</div>
<h2>Same events, two targets</h2>
<table class="tbl">
<tr><th>seq</th><th>operation</th><th>city</th><th>arrived in</th></tr>
<tr><td>18</td><td>INSERT</td><td>Riyadh</td><td>file 01</td></tr>
<tr><td>1020</td><td>UPDATE</td><td>Riyadh (new e-mail)</td><td>file 02</td></tr>
<tr><td>1015</td><td>UPDATE</td><td>Osaka</td><td>file 03 — <b>late</b></td></tr>
</table>
<div class="grid two">
 <div class="card"><h3>SCD type 1 → current state</h3>
 <table class="tbl"><tr><th>customer_id</th><th>city</th></tr><tr><td>C0018</td><td>Riyadh</td></tr></table>
 1015 &lt; 1020 → the late event is <b>ignored</b>. A delete removes the row.</div>
 <div class="card green"><h3>SCD type 2 → history</h3>
 <table class="tbl"><tr><th>city</th><th>__START_AT</th><th>__END_AT</th></tr>
 <tr><td>Riyadh</td><td>18</td><td>1015</td></tr><tr><td>Osaka</td><td>1015</td><td>1020</td></tr>
 <tr><td>Riyadh</td><td>1020</td><td>NULL</td></tr></table>
 The late event is <b>inserted in the right place</b>. <code>__END_AT IS NULL</code> = current. A delete <b>closes</b> the current version.</div>
</div>
""" + callout("exam", "<code>__START_AT</code> / <code>__END_AT</code> have the type of the <code>SEQUENCE BY</code> column. Point-in-time: "
              "<code>WHERE __START_AT &lt;= x AND (__END_AT &gt; x OR __END_AT IS NULL)</code>."))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · Monitoring: the pipeline UI and the event log
# MAGIC
# MAGIC | Where | What you see |
# MAGIC |---|---|
# MAGIC | **Pipeline graph** | every dataset with its state (running / completed / failed / skipped), rows written, duration |
# MAGIC | **Data quality** tab (per table) | passed / failed / dropped records per expectation |
# MAGIC | **Update history** | every update, its trigger (manual, schedule, API), duration and result — the trend of pipeline health |
# MAGIC | **Event log** | all of the above as data you can query |
# MAGIC
# MAGIC Reading the event log:
# MAGIC
# MAGIC ```sql
# MAGIC SELECT * FROM event_log(TABLE(my_catalog.my_schema.orders_silver));   -- any table of the pipeline
# MAGIC SELECT * FROM event_log('<pipeline-id>');                              -- or the pipeline id
# MAGIC SELECT * FROM my_catalog.ops.orders_pipeline_events;                   -- if published to UC in the settings
# MAGIC ```
# MAGIC
# MAGIC * Only the **owner** of the pipeline's tables may call `event_log()` (a view on it can't be shared).
# MAGIC * Columns: `id`, `sequence`, `origin` (pipeline, update, **flow_name** …), `timestamp`, `message`, `level`
# MAGIC   (INFO / WARN / ERROR / METRICS), `error`, **`details`** (JSON string), **`event_type`**.
# MAGIC * Across all pipelines: the **`pipeline_events` system table** (Beta).
# MAGIC
# MAGIC | `event_type` | Use it for |
# MAGIC |---|---|
# MAGIC | `flow_progress` | rows written, **data-quality metrics** (`details:flow_progress.data_quality.expectations`), backlog |
# MAGIC | `update_progress` / `create_update` | update states and history |
# MAGIC | `flow_definition` | **lineage**: input / output datasets, schema |
# MAGIC | `planning_information` | how a materialized view was refreshed (incremental vs full recompute) |
# MAGIC | `user_action` | **audit**: who created, edited, started or stopped the pipeline |
# MAGIC | `cluster_resources`, `autoscale` | compute behaviour |
# MAGIC
# MAGIC ```sql
# MAGIC -- data-quality metrics per expectation
# MAGIC SELECT e.dataset, e.name, SUM(e.passed_records) AS passed, SUM(e.failed_records) AS failed
# MAGIC FROM (SELECT explode(from_json(details:flow_progress:data_quality:expectations,
# MAGIC              'array<struct<name: string, dataset: string, passed_records: bigint, failed_records: bigint>>')) AS e
# MAGIC       FROM event_log(TABLE(orders_silver)) WHERE event_type = 'flow_progress')
# MAGIC GROUP BY e.dataset, e.name;
# MAGIC ```
# MAGIC
# MAGIC ## 5 · Troubleshooting a failed update
# MAGIC
# MAGIC | Symptom (UI / event log `ERROR`) | Cause | Fix |
# MAGIC |---|---|---|
# MAGIC | Update fails on an expectation | a `FAIL UPDATE` rule was violated | fix the data upstream or relax the rule (e.g. DROP), then run again — the flow's checkpoint didn't advance |
# MAGIC | Streaming flow stops after the source got a new column | schema evolution in Auto Loader / `read_files` | production updates retry automatically; for a UI run just **run again** |
# MAGIC | "…detected an update or delete in the source…" | a streaming table reads a source that isn't append-only | `skipChangeCommits`, a materialized view instead, or a full refresh |
# MAGIC | Table "is already managed by pipeline …" | two pipelines define the same table | one table = one pipeline; rename or move the definition |
# MAGIC | Errors about unresolved datasets / columns | typo, missing file in `transformations/`, wrong default schema | **Dry run** shows it without touching data |
# MAGIC | Update waits / can't start (Free Edition) | only one active update per pipeline type | wait for the other pipeline to finish |
# MAGIC
# MAGIC After fixing: **refresh** (default), **refresh selection** for one table, or **full refresh** when history must be rebuilt.
# MAGIC
# MAGIC ## 6 · Running pipelines in production

# COMMAND ----------

# DBTITLE 1,Slide · Scheduling and orchestration
show("""
<div class="kicker">Slide 6 · Triggered pipelines need something to trigger them</div>
<h2>Schedule, orchestrate, alert</h2>
<div class="grid">
 <div class="card"><h3>🗓️ Schedule</h3>Editor → <b>Schedule</b> creates a <b>Lakeflow Job</b> with a <b>pipeline task</b> (cron, or triggers such as file arrival)</div>
 <div class="card green"><h3>🔗 Orchestrate</h3>A multi-task job: ingest files → <b>pipeline task</b> → SQL / dashboard task. Pipeline tasks can request a <b>full refresh</b></div>
 <div class="card orange"><h3>♾️ Continuous</h3>For always-fresh data: continuous mode (or a continuous job) — compute never stops</div>
 <div class="card purple"><h3>🔔 Alert &amp; own</h3>Notifications on failure; <b>run as</b> a service principal; permissions: <i>Can view / Can run / Can manage / Is owner</i></div>
</div>
""" + callout("tip", "Jobs and API runs use <b>production</b> behaviour (automatic retries, compute restarted on recoverable errors); "
              "runs you start in the editor use <b>development</b> behaviour (no retries, compute reused)."))

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Key takeaways
# MAGIC 1. Expectations = **name + condition + action**: **warn** (default, keep & count) · **DROP ROW** · **FAIL UPDATE** (update
# MAGIC    fails, flow rolled back). Python: `expect`, `expect_or_drop`, `expect_or_fail`, `expect_all*` with a dict.
# MAGIC 2. Quarantine = the same rules **inverted** into a second table. Write NULL handling into the rules explicitly.
# MAGIC 3. `AUTO CDC` (formerly `APPLY CHANGES`): create the target streaming table, then a flow with **KEYS**, **SEQUENCE BY**,
# MAGIC    `APPLY AS DELETE WHEN`, `COLUMNS * EXCEPT`, **`STORED AS SCD TYPE 1`** or **`2`**, `TRACK HISTORY ON`.
# MAGIC 4. **SEQUENCE BY** handles late events: SCD1 ignores older changes, SCD2 inserts them into the history
# MAGIC    (`__START_AT` / `__END_AT`, NULL end = current).
# MAGIC 5. Monitor with the graph, the **Data quality** tab, update history and the **event log** — `event_log(TABLE(t))` or
# MAGIC    `event_log('<id>')`; key event types `flow_progress`, `update_progress`, `flow_definition`, `user_action`.
# MAGIC 6. Troubleshoot with ERROR events + **Dry run**; recover with refresh / selective refresh / full refresh.
# MAGIC
# MAGIC ➡️ Next: labs **08-L1 … 08-L4**, then **08-Q**
