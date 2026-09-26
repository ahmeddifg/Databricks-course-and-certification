# Databricks notebook source
# MAGIC %md
# MAGIC # 📐 03-P1 · Delta Lake & the Transaction Log
# MAGIC **Section 03 — Delta Lake** · Exam domain **D1 Platform (core components)** — and used in almost every other domain
# MAGIC
# MAGIC | After this presentation you can… |
# MAGIC |---|
# MAGIC | Explain which data-lake problems Delta Lake solves |
# MAGIC | Describe a Delta table on disk: **Parquet data files + `_delta_log`** |
# MAGIC | Explain how commits, checkpoints and **optimistic concurrency** deliver **ACID** transactions |
# MAGIC | Explain **deletion vectors** and why `DELETE`/`UPDATE` don't always rewrite files |
# MAGIC | Tell a Delta table from a plain Parquet/CSV table |
# MAGIC
# MAGIC > ▶️ **Run all** to render the diagrams.

# COMMAND ----------

# MAGIC %run ../../Includes/_style

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · Why plain data lakes hurt
# MAGIC
# MAGIC | Problem in a plain Parquet/CSV data lake | What goes wrong |
# MAGIC |---|---|
# MAGIC | **No atomic writes** | A job fails half-way → readers see partial data |
# MAGIC | **No isolation** | A reader lists files while a writer adds/removes them → inconsistent results |
# MAGIC | **No schema enforcement** | A bad file with different columns silently lands → downstream breaks ("data swamp") |
# MAGIC | **No updates/deletes** | Fixing one record or a GDPR delete means rewriting whole folders by hand |
# MAGIC | **Slow metadata** | Listing millions of files on object storage before each query |
# MAGIC | **No history** | Can't audit changes or roll back a bad load |
# MAGIC
# MAGIC **Delta Lake** is an **open-source storage layer** (Linux Foundation) that fixes all of these on top of the same cheap object storage.
# MAGIC It's the **default table format** on Databricks.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · What a Delta table looks like on storage

# COMMAND ----------

# DBTITLE 1,Slide · Anatomy
show("""
<div class="kicker">Slide 2 · Anatomy of a Delta table</div>
<h2>Parquet data files + a transaction log</h2>
<div class="grid two">
 <div class="card"><h3>📁 …/tables/&lt;table-id&gt;/</h3>
 <pre style="font-size:12.5px;line-height:1.5;margin:0">
part-00000-3f1a….snappy.parquet    ← data (Parquet)
part-00001-9bc2….snappy.parquet
part-00002-e07d….snappy.parquet
deletion_vector_7c1e….bin          ← marks deleted rows
<b>_delta_log/</b>
  00000000000000000000.json          ← version 0
  00000000000000000001.json          ← version 1
  …
  00000000000000000010.checkpoint.parquet
  00000000000000000011.json
  _last_checkpoint
</pre></div>
 <div class="card green"><h3>📜 The transaction log is the source of truth</h3>
 <ul><li>Each commit = one JSON file named by its <b>version</b></li>
 <li>It records <b>actions</b>: <code>add</code> file, <code>remove</code> file, <code>metaData</code> (schema, properties), <code>protocol</code>, <code>commitInfo</code></li>
 <li>A file in the folder that is <b>not</b> referenced by the log is <b>not part of the table</b></li>
 <li>Every ~10 commits a <b>checkpoint</b> (Parquet) summarises the whole state, so readers don't replay thousands of JSON files</li>
 <li>File-level <b>statistics</b> (min/max/null counts) are stored in the log → <b>data skipping</b></li></ul></div>
</div>
""" + callout("trap", "Delta Lake is <b>not</b> a separate database or storage service. It's a <b>file format + protocol</b>: Parquet files plus a JSON/Parquet log in the same folder."))

# COMMAND ----------

# MAGIC %md
# MAGIC ### A real commit file (simplified) — `00000000000000000002.json` after an `UPDATE`
# MAGIC
# MAGIC ```json
# MAGIC {"commitInfo":{"timestamp":1790000000000,"operation":"UPDATE",
# MAGIC                "operationParameters":{"predicate":"[\"(category = 'Electronics')\"]"},
# MAGIC                "readVersion":1,"isolationLevel":"WriteSerializable",
# MAGIC                "operationMetrics":{"numUpdatedRows":"6","numAddedFiles":"1","numRemovedFiles":"1"}}}
# MAGIC {"remove":{"path":"part-00000-3f1a….snappy.parquet","deletionTimestamp":1790000000000,"dataChange":true}}
# MAGIC {"add":{"path":"part-00003-a81f….snappy.parquet","size":4211,"dataChange":true,
# MAGIC         "stats":"{\"numRecords\":36,\"minValues\":{\"price\":6.4},\"maxValues\":{\"price\":503.33},\"nullCount\":{\"price\":0}}"}}
# MAGIC ```
# MAGIC * The `UPDATE` did **not** modify a Parquet file in place — Parquet files are **immutable**. It wrote a **new** file (`add`) and
# MAGIC   logically removed the old one (`remove`). The old file stays on storage → that's what makes **time travel** possible.
# MAGIC * `DESCRIBE HISTORY` is literally a view over these `commitInfo` entries.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🧪 Replay a transaction log yourself
# MAGIC Unity Catalog protects the storage of managed tables, so you can't open `_delta_log/` in a workspace. Instead, this cell
# MAGIC **simulates** exactly what a Delta reader does: replay `add`/`remove` actions version by version to find the **active files**.
# MAGIC Then it shows what `VACUUM` would delete and which versions would lose time travel. Edit the log and re-run to experiment!

# COMMAND ----------

# DBTITLE 1,Transaction log simulator (pure Python - no Spark needed)
toy_log = [  # version -> (operation, files added, files removed)
    (0, "CREATE TABLE",        [],                        []),
    (1, "WRITE (load)",        ["part-A", "part-B"],      []),
    (2, "UPDATE Electronics",  ["part-C"],                ["part-A"]),
    (3, "DELETE Toys",         ["part-D"],                ["part-B"]),
    (4, "OPTIMIZE",            ["part-E"],                ["part-C", "part-D"]),
]

snapshots, active = {}, set()
for version, op, added, removed in toy_log:            # the reader's replay loop
    active = (active - set(removed)) | set(added)
    snapshots[version] = sorted(active)

current_files = set(snapshots[max(snapshots)])
ever_written = {f for _, _, added, _ in toy_log for f in added}
vacuumed = ever_written - current_files                  # VACUUM (once the retention period has passed)

rows = ""
for version, op, added, removed in toy_log:
    ok = not (set(snapshots[version]) & vacuumed)
    actions = " ".join('<span class="pill">+' + f + "</span>" for f in added)
    actions += " ".join('<span class="pill red">-' + f + "</span>" for f in removed)
    files = ", ".join(snapshots[version]) or "(none)"
    status = "✅" if ok else "❌ files gone"
    rows += f"<tr><td><b>v{version}</b></td><td>{op}</td><td>{actions}</td><td>{files}</td><td>{status}</td></tr>"
show(f"""
<div class="kicker">Simulation · replaying _delta_log</div>
<h2>Active files per version — and the effect of VACUUM</h2>
<table class="tbl"><tr><th>Version</th><th>Operation</th><th>Actions in the commit</th><th>Active files (the table at this version)</th>
<th>Time travel after VACUUM</th></tr>{rows}</table>
<p>Files deleted by VACUUM: <b>{', '.join(sorted(vacuumed))}</b> · files kept: <b>{', '.join(sorted(current_files))}</b></p>
""" + callout("exam", "Only the <b>current</b> version is guaranteed to survive VACUUM. Older versions become unreadable once "
              "their files are removed — even though their commits are still in the log."))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · How reads and writes work

# COMMAND ----------

# DBTITLE 1,Slide · Read & write protocol
show("""
<div class="kicker">Slide 3 · The protocol</div>
<h2>Readers replay the log · writers race to commit the next version</h2>
<div class="grid two">
 <div class="card teal"><h3>📖 Read (snapshot)</h3>
  <div class="flow"><div class="step">latest <b>checkpoint</b></div><div class="arrow">➜</div>
   <div class="step">+ newer <b>JSON</b> commits</div><div class="arrow">➜</div>
   <div class="step">= list of <b>active files</b></div><div class="arrow">➜</div>
   <div class="step">read only files that can match (<b>stats</b>)</div></div>
  A reader always sees one consistent <b>version</b> — never a half-written one.</div>
 <div class="card orange"><h3>✍️ Write (optimistic concurrency)</h3>
  <div class="flow"><div class="step">1 · read version <b>N</b></div><div class="arrow">➜</div>
   <div class="step">2 · write new <b>Parquet</b> files</div><div class="arrow">➜</div>
   <div class="step">3 · atomically create <b>N+1.json</b></div></div>
  If another writer created N+1 first, Delta checks for <b>conflicts</b>: non-overlapping appends are retried automatically; real conflicts fail with a concurrent-modification error.</div>
</div>
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · ACID, in Delta terms
# MAGIC
# MAGIC | Property | How Delta delivers it | Example |
# MAGIC |---|---|---|
# MAGIC | **Atomicity** | A commit is a single file creation: all actions of the transaction appear together, or none do | A failed job leaves no partial data; a `CHECK` violation rejects the **whole** write |
# MAGIC | **Consistency** | Schema enforcement + constraints are checked before commit | Appending a DataFrame with an unexpected column fails |
# MAGIC | **Isolation** | Snapshot isolation for readers; optimistic concurrency for writers (default level **WriteSerializable**) | A dashboard query never sees a half-finished `MERGE` |
# MAGIC | **Durability** | Commits are files in durable cloud object storage | A cluster crash doesn't lose committed data |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 · Deletion vectors — cheaper `DELETE`, `UPDATE` and `MERGE`

# COMMAND ----------

# DBTITLE 1,Slide · Deletion vectors
show("""
<div class="kicker">Slide 5 · Deletion vectors</div>
<h2>Mark rows as deleted instead of rewriting whole files</h2>
<div class="grid two">
 <div class="card gray"><h3>Without deletion vectors (copy-on-write)</h3>
  Deleting 1 row from a 1 GB file ⇒ <b>rewrite the whole 1 GB file</b> without that row.</div>
 <div class="card green"><h3>With deletion vectors (merge-on-read)</h3>
  Delta writes a tiny <b>deletion vector</b> file marking the row; readers skip marked rows.
  Files are physically rewritten later by <code>OPTIMIZE</code>, <code>REORG TABLE … APPLY (PURGE)</code> or auto compaction.</div>
</div>
""" + callout("exam", "In <code>DESCRIBE HISTORY</code> you'll then see metrics like <code>numDeletionVectorsAdded</code> "
              "instead of large numbers of rewritten files. The deleted data still exists in storage until files are rewritten <b>and</b> <code>VACUUM</code> runs — "
              "important for GDPR \"right to be forgotten\" requests."))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6 · Delta table vs non-Delta table
# MAGIC
# MAGIC | | Delta table (`CREATE TABLE t …`) | External CSV/Parquet table (`USING CSV … LOCATION`) |
# MAGIC |---|---|---|
# MAGIC | ACID transactions | ✅ | ❌ |
# MAGIC | `UPDATE` / `DELETE` / `MERGE` | ✅ | ❌ |
# MAGIC | Time travel, `RESTORE` | ✅ | ❌ |
# MAGIC | Schema enforcement | ✅ | ❌ (read-time only) |
# MAGIC | New files picked up | Only through commits | Whenever files appear in the folder (cached listings may need `REFRESH TABLE`) |
# MAGIC | Performance features | stats, data skipping, `OPTIMIZE`, clustering, Photon | limited |
# MAGIC
# MAGIC > ✅ Convert Parquet in place with ``CONVERT TO DELTA parquet.`/path` `` or copy it into a new Delta table with CTAS.
# MAGIC > Delta tables can also be read as **Apache Iceberg** by other engines when **UniForm** is enabled.

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Key takeaways
# MAGIC 1. Delta Lake = **Parquet data files + `_delta_log`** (JSON commits + periodic Parquet checkpoints). The log is the source of truth.
# MAGIC 2. Every write creates a **new version**; files are immutable — changes add new files and logically remove old ones.
# MAGIC 3. **ACID**: atomic commit files, schema enforcement/constraints, snapshot isolation + optimistic concurrency, durable object storage.
# MAGIC 4. **Deletion vectors** make `DELETE/UPDATE/MERGE` cheap by marking rows; `OPTIMIZE`/`REORG … PURGE` rewrite files later.
# MAGIC 5. Delta is the **default** format on Databricks; plain CSV/Parquet tables lack all of the above.
# MAGIC
# MAGIC ➡️ Next: **03-P2 · Working with Delta Tables**
