# Databricks notebook source
# MAGIC %md
# MAGIC # ⏳ 03-P3 · Time Travel, Clones & Maintenance
# MAGIC **Section 03 — Delta Lake** · supports **D1**, **D6 Optimization (10 %)** and **D7 (managed tables)**
# MAGIC
# MAGIC | After this presentation you can… |
# MAGIC |---|
# MAGIC | Audit changes with `DESCRIBE HISTORY` and query old versions with time travel |
# MAGIC | Roll back with `RESTORE` and recover dropped tables with `UNDROP` |
# MAGIC | Choose between **deep** and **shallow** clones |
# MAGIC | Explain `OPTIMIZE`, `ZORDER BY` and the small-files problem |
# MAGIC | Explain `VACUUM`, its retention rules and how it limits time travel |
# MAGIC | Know what **predictive optimization** automates for you |
# MAGIC
# MAGIC > ▶️ **Run all** to render the diagrams. Practice: **labs/03-L1** (time travel, RESTORE) and **labs/03-L2** (clones, OPTIMIZE, VACUUM).

# COMMAND ----------

# MAGIC %run ../../Includes/_style

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · Time travel

# COMMAND ----------

# DBTITLE 1,Slide · Versions on a timeline
show("""
<div class="kicker">Slide 1 · Every commit is a version you can query</div>
<h2>DESCRIBE HISTORY → pick a version → query it</h2>
<div class="flow">
  <div class="step"><b>v0</b>CREATE TABLE</div><div class="arrow">➜</div>
  <div class="step"><b>v1</b>WRITE (load 36 rows)</div><div class="arrow">➜</div>
  <div class="step"><b>v2</b>UPDATE prices</div><div class="arrow">➜</div>
  <div class="step"><b>v3</b>DELETE Toys</div><div class="arrow">➜</div>
  <div class="step"><b>v4</b>WRITE (2 new)</div><div class="arrow">➜</div>
  <div class="step" style="background:#ffe8e8"><b>v5</b>💥 DELETE all</div><div class="arrow">➜</div>
  <div class="step" style="background:#e6f7ea"><b>v6</b>RESTORE to v4</div>
</div>
<table class="tbl">
<tr><th>Goal</th><th>SQL</th><th>Python</th></tr>
<tr><td>Query by version</td><td><code>SELECT * FROM t VERSION AS OF 3</code> · <code>SELECT * FROM t@v3</code></td><td><code>spark.read.option("versionAsOf", 3).table("t")</code></td></tr>
<tr><td>Query by time</td><td><code>SELECT * FROM t TIMESTAMP AS OF '2026-09-01 10:00:00'</code></td><td><code>spark.read.option("timestampAsOf", "2026-09-01").table("t")</code></td></tr>
<tr><td>Roll back</td><td><code>RESTORE TABLE t TO VERSION AS OF 3</code> · <code>… TO TIMESTAMP AS OF '…'</code></td><td>—</td></tr>
<tr><td>Audit</td><td><code>DESCRIBE HISTORY t</code></td><td><code>spark.sql("DESCRIBE HISTORY t")</code></td></tr>
</table>
""" + callout("exam", "<code>RESTORE</code> is itself a <b>new commit</b> (v6 above — exactly what you do in lab 03-L1). History is never rewritten — you can still query v5.")
    + callout("trap", "Time travel needs the <b>old data files</b>. After <code>VACUUM</code> removes them, querying those versions fails."))

# COMMAND ----------

# MAGIC %md
# MAGIC **Use cases:** audit "what did this table look like when the report ran?", reproduce ML training data, compare versions
# MAGIC (`EXCEPT`), and instant rollback of a bad load.
# MAGIC
# MAGIC ## 2 · Recovering from mistakes
# MAGIC
# MAGIC | Mistake | Recovery |
# MAGIC |---|---|
# MAGIC | Bad `UPDATE`/`DELETE`/`MERGE`/load | `RESTORE TABLE t TO VERSION AS OF n` |
# MAGIC | Need a few old rows back | `INSERT INTO t SELECT * FROM t VERSION AS OF n WHERE …` |
# MAGIC | `DROP TABLE` on a **Unity Catalog** table | `UNDROP TABLE t` within the retention window (**7 days** by default, managed **and** external UC tables); `SHOW TABLES DROPPED` lists candidates — use `UNDROP TABLE WITH ID '<id>'` if several share a name |
# MAGIC | `CREATE OR REPLACE` by mistake | Time travel/`RESTORE` to a version before the replace (history is kept) |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · Clones

# COMMAND ----------

# DBTITLE 1,Slide · Deep vs shallow clone
show("""
<div class="kicker">Slide 3 · Copying tables</div>
<h2>DEEP CLONE vs SHALLOW CLONE</h2>
<div class="grid two">
 <div class="card"><h3>🧬 DEEP CLONE</h3>
  <code>CREATE OR REPLACE TABLE t_backup DEEP CLONE t</code>
  <ul><li>Copies <b>metadata + data files</b></li><li>Fully independent of the source</li>
  <li>Re-running it syncs only the changes (<b>incremental</b>)</li><li>Slower, doubles storage</li>
  <li>Use for: <b>backups</b>, disaster recovery, migrating tables</li></ul></div>
 <div class="card orange"><h3>🪶 SHALLOW CLONE</h3>
  <code>CREATE TABLE t_dev SHALLOW CLONE t</code>
  <ul><li>Copies <b>metadata only</b>; references the source's data files</li><li>Instant and nearly free</li>
  <li>New writes to the clone go to the clone's own files</li>
  <li>⚠️ Legacy Hive metastore: breaks if the source's files are <b>VACUUMed</b>. Unity Catalog tracks clone references, so a VACUUM of the source doesn't break it</li>
  <li>UC: can't be re-created with <code>CREATE OR REPLACE</code> — drop it and clone again; no clone of a clone</li>
  <li>Use for: <b>dev/test</b> copies, trying risky changes</li></ul></div>
</div>
""" + callout("exam", "In both cases changes to the clone do <b>not</b> affect the source. You can clone a specific version: "
              "<code>… DEEP CLONE t VERSION AS OF 12</code>."))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · The small-files problem and `OPTIMIZE`
# MAGIC
# MAGIC Streaming jobs, frequent small appends and over-partitioning create **thousands of tiny files** →
# MAGIC slow listing, too many tasks, poor compression.

# COMMAND ----------

# DBTITLE 1,Slide · OPTIMIZE and ZORDER
show("""
<div class="kicker">Slide 4 · File compaction</div>
<h2>OPTIMIZE = bin-packing · ZORDER BY = co-locating values</h2>
<div class="grid two">
 <div class="card green"><h3>📦 <code>OPTIMIZE t</code></h3>
  <div class="flow"><div class="step">▫▫▫▫▫▫▫▫<br>many small files</div><div class="arrow">➜</div><div class="step">▬ ▬<br>few ~1 GB files</div></div>
  <ul><li>New commit; old files only <i>logically</i> removed (time travel still works)</li>
  <li>Idempotent — running it again on a compacted table does little</li>
  <li>Can target a subset: <code>OPTIMIZE t WHERE date &gt;= '2026-09-01'</code> (partition columns)</li></ul></div>
 <div class="card purple"><h3>🧭 <code>OPTIMIZE t ZORDER BY (customer_id)</code></h3>
  <ul><li>Sorts data so similar values of the column(s) land in the <b>same files</b></li>
  <li>File statistics (min/max) then let queries <b>skip</b> most files → faster filters</li>
  <li>Best for <b>high-cardinality</b> columns used in filters/joins</li>
  <li>⚠️ <b>Not idempotent</b> (unlike plain OPTIMIZE): re-running can rewrite large parts of the data again</li></ul></div>
</div>
""" + callout("tip", "For new tables Databricks recommends <b>liquid clustering</b> (<code>CLUSTER BY</code> / <code>CLUSTER BY AUTO</code>) "
              "instead of partitioning + Z-order: incremental and keys can be changed without rewriting. Deep dive in Section 12."))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 · `VACUUM` — deleting old files for good
# MAGIC
# MAGIC ```sql
# MAGIC VACUUM t;                      -- delete files removed from the table more than 7 days ago (default retention)
# MAGIC VACUUM t RETAIN 240 HOURS;     -- custom retention (10 days)
# MAGIC VACUUM t DRY RUN;              -- list what would be deleted, delete nothing
# MAGIC ```
# MAGIC
# MAGIC | Fact | Detail |
# MAGIC |---|---|
# MAGIC | What gets deleted | Data files **not referenced by the current version** that were **removed** (by a commit) longer ago than the retention threshold, plus uncommitted leftovers. The clock starts at the **removal**, not at the file's creation |
# MAGIC | Default retention | **7 days** — table property `delta.deletedFileRetentionDuration` |
# MAGIC | Safety check | `RETAIN` below the table's retention fails unless `spark.databricks.delta.retentionDurationCheck.enabled = false` (classic compute only, never in production) |
# MAGIC | Effect on time travel | You **can't** time-travel to versions whose files were vacuumed |
# MAGIC | Transaction log | **Not** removed by VACUUM — log entries expire separately after `delta.logRetentionDuration` (default **30 days**) |
# MAGIC | Shallow clones | Hive metastore: vacuuming the source can break shallow clones. Unity Catalog: files still used by a shallow clone are kept |
# MAGIC
# MAGIC > 🎯 Retention = the **time-travel window**. Want 30 days of time travel? Set `delta.deletedFileRetentionDuration = 'interval 30 days'`
# MAGIC > (and keep `delta.logRetentionDuration` ≥ that).

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6 · Let Databricks do it: predictive optimization & auto compaction
# MAGIC
# MAGIC | Feature | What it does |
# MAGIC |---|---|
# MAGIC | **Predictive optimization** (Unity Catalog **managed** tables) | Databricks decides when to run `OPTIMIZE`, `VACUUM` and `ANALYZE` for you, based on usage |
# MAGIC | **Optimized writes** | Writes fewer, larger files (shuffles data before writing) |
# MAGIC | **Auto compaction** | Right after a write succeeds, compacts small files on the same compute |
# MAGIC | **Automatic liquid clustering** (`CLUSTER BY AUTO`) | Chooses clustering keys from your query patterns |
# MAGIC
# MAGIC This is one more reason to prefer **managed** tables: the platform maintains them.

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Key takeaways
# MAGIC 1. **Time travel**: `VERSION AS OF n`, `@vN`, `TIMESTAMP AS OF '…'`; **RESTORE** adds a new commit; `UNDROP` recovers dropped UC tables (7 days).
# MAGIC 2. **Deep clone** = full independent copy (backups); **shallow clone** = metadata-only copy for dev/test that shares the source's files (breaks after a source VACUUM only in the legacy Hive metastore).
# MAGIC 3. **OPTIMIZE** compacts small files (idempotent); **ZORDER BY** co-locates values for data skipping (not idempotent). Prefer incremental **liquid clustering** for new tables.
# MAGIC 4. **VACUUM** deletes unreferenced files older than **7 days** by default; afterwards those versions can't be time-travelled. It does **not** delete the log.
# MAGIC 5. **Predictive optimization** automates OPTIMIZE/VACUUM for UC managed tables.
# MAGIC
# MAGIC ➡️ Now practise in **labs/03-L1**, **labs/03-L2**, then the **03-L3 challenge** and **03-Q** quiz.
