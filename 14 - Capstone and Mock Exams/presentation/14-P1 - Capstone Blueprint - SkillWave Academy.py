# Databricks notebook source
# MAGIC %md
# MAGIC # 🎓 14-P1 · Capstone Blueprint — SkillWave Academy
# MAGIC **Section 14** · all exam domains in one project
# MAGIC
# MAGIC | After this presentation you can… |
# MAGIC |---|
# MAGIC | Describe the **SkillWave** business, its data model and the problems hidden in the source data |
# MAGIC | Draw the target architecture: **one catalog, one schema per medallion layer**, volumes for files |
# MAGIC | Justify the **ingestion tool** for each source (COPY INTO, `read_files`, Auto Loader, declarative pipeline) |
# MAGIC | Explain the **silver rules**, the **gold star schema**, the **job DAG** and the **access model** |
# MAGIC | Map every capstone step to an **exam objective** |
# MAGIC
# MAGIC > ▶️ **Run all** to render the slides (no compute-heavy work — runs in seconds).

# COMMAND ----------

# MAGIC %run ../../Includes/_style

# COMMAND ----------

# DBTITLE 1,Slide 1 · The mission
show("""
<div class="kicker">Slide 1 · Capstone</div>
<h1>🎓 SkillWave Academy needs a lakehouse</h1>
<p class="sub">An online learning platform: instructors publish courses, students enroll every day, the business wants
trustworthy dashboards by 07:00 — and the compliance team wants personal data protected.</p>
<div class="grid">
 <div class="card"><h3>🏗️ 14-L1 · Foundation</h3>catalog, schemas, volumes, source files, querying files, grants</div>
 <div class="card green"><h3>🥉🥈 14-L2 · Bronze &amp; silver</h3>COPY INTO, read_files, Auto Loader + schema evolution, MERGE, quarantine, RESTORE</div>
 <div class="card orange"><h3>🥇 14-L3 · Gold</h3>star schema, view, materialized view, windows, liquid clustering, mask &amp; row filter</div>
 <div class="card purple"><h3>🤖 14-L4 · Automate</h3>declarative pipeline, multi-task job with If/else, monitoring, Git &amp; bundles</div>
 <div class="card red"><h3>🏆 14-L5 · Challenge</h3>course reviews end-to-end, on your own</div>
 <div class="card teal"><h3>📗📘 Cheat sheets</h3>14-CS1 SQL · 14-CS2 PySpark — every exam statement, runnable</div>
 <div class="card gray"><h3>📝 Mock exam</h3>120 questions · timed 45-question simulation</div>
</div>
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · The business and its data model

# COMMAND ----------

# DBTITLE 1,Slide 2 · Entities and relationships
show("""
<div class="kicker">Slide 2 · Data model (logical)</div>
<div class="flow">
 <div class="step"><b>👩‍🏫 INSTRUCTOR</b>instructor_id · name · email · country · hire_date · specialty</div>
 <div class="arrow">1 ─ N</div>
 <div class="step"><b>📚 COURSE</b>course_id · title · category · level · instructor_id · list_price · duration_hours</div>
 <div class="arrow">1 ─ N</div>
 <div class="step" style="border-color:#e8590c"><b>🧾 ENROLLMENT</b>enrollment_id · student_id · course_id · enrolled_at · price_paid · coupon · channel · status · progress_pct · <i>referrer (from day 4)</i></div>
 <div class="arrow">N ─ 1</div>
 <div class="step"><b>🧑‍🎓 STUDENT</b>student_id · email · profile <i>(JSON string)</i> · plan · interests <i>(array)</i> · signup_ts</div>
</div>
<div class="grid two">
 <div class="card"><h3>⭐ REVIEW (challenge)</h3>review_id · enrollment_id · rating 1-5 · comment · reviewer_email</div>
 <div class="card gray"><h3>📄 SYLLABUS (unstructured)</h3>one text file per course — ingested as one row per file</div>
</div>
<p class="muted">An enrollment is the <b>fact</b> (an event with measures: price, progress). Students, courses, instructors and dates
are <b>dimensions</b> (who / what / when).</p>
""")

# COMMAND ----------

# DBTITLE 1,Slide 3 · The sources and the traps planted in them
show("""
<div class="kicker">Slide 3 · Sources in /Volumes/skillwave/landing/raw — and what's wrong with them</div>
<table class="tbl">
<tr><th>Source</th><th>Format</th><th>Arrives</th><th>Planted problem</th><th>Fixed in</th></tr>
<tr><td>instructors/</td><td>CSV + header</td><td>once</td><td>—</td><td>—</td></tr>
<tr><td>courses/</td><td>CSV + header</td><td>once</td><td>C120 duplicated · C118 has <b>no category</b></td><td>silver: <code>DISTINCT</code>, <code>coalesce</code>, CHECK</td></tr>
<tr><td>students/</td><td>JSON lines</td><td>once</td><td>3 duplicates · <code>"  JOHN.KHAN1@COMPANY.COM "</code> · 17 NULL e-mails · profile is a JSON <b>string</b></td><td>silver: <code>DISTINCT</code>, <code>lower(trim())</code>, <code>profile:city</code></td></tr>
<tr><td>student-updates/</td><td>JSON lines</td><td>once</td><td>20 changed + 5 new students</td><td>silver: <code>MERGE</code> (SCD 1)</td></tr>
<tr><td>enrollments/</td><td>JSON lines</td><td><b>1 file / day</b></td><td>per file: 1 duplicate, 1 negative price, 1 unknown student, 1 unknown course · <b>new column</b> on day 4</td><td>Auto Loader schema evolution · silver dedup + quarantine</td></tr>
<tr><td>syllabus/</td><td>text</td><td>once</td><td>unstructured</td><td>bronze: <code>read_files(format =&gt; 'text', wholeText =&gt; true)</code></td></tr>
<tr><td>reviews/</td><td>JSON lines</td><td>once</td><td>3 duplicates · ratings 0 / 6 / NULL · 2 unknown enrollments</td><td>challenge 14-L5</td></tr>
</table>
""" + callout("tip", "Bronze keeps <b>everything as delivered</b> (plus <code>_source_file</code>, <code>_ingested_at</code>). Problems are fixed — or quarantined with a reason — in <b>silver</b>, never silently dropped."))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · Architecture

# COMMAND ----------

# DBTITLE 1,Slide 4 · One catalog, one schema per layer
show("""
<div class="kicker">Slide 4 · Unity Catalog layout</div>
<div class="grid two">
 <div class="card"><h3>🗂️ The namespace you build in 14-L1</h3>
<pre style="font-size:13px;margin:0">skillwave                    catalog  (owner: you)
├── landing                  schema
│   ├── raw          volume  source files
│   └── checkpoints  volume  streaming state
├── bronze                   schema  raw tables
├── silver                   schema  clean tables + quarantine
├── gold                     schema  star schema, views, MV
├── sdp                      schema  tables of the pipeline (14-L4)
└── playground               schema  cheat-sheet experiments</pre></div>
 <div class="card orange"><h3>🔐 Why a schema per layer?</h3><ul>
 <li>grants are inherited: <code>SELECT ON SCHEMA gold</code> covers every current and future gold table</li>
 <li>analysts never see bronze/silver</li>
 <li>clear ownership: engineers own bronze/silver, BI reads gold</li>
 <li>dev/prod separation is done one level up: <b>one catalog per environment</b> (bundle variable <code>catalog</code>)</li></ul></div>
</div>
""")

# COMMAND ----------

# DBTITLE 1,Slide 5 · The data flow
show("""
<div class="kicker">Slide 5 · Medallion flow</div>
<div class="layer" style="background:#868e96">📁 <b>landing.raw</b> — files: CSV · JSON · text <small>(the outside world writes here)</small></div>
<div class="layer" style="background:#b08968">🥉 <b>bronze</b> — instructors (COPY INTO) · courses, students, student_updates, course_syllabus (CTAS read_files) · enrollments (Auto Loader) <small>+ _source_file, _ingested_at, _rescued_data</small></div>
<div class="layer" style="background:#adb5bd;color:#1d2433">🥈 <b>silver</b> — students (parsed, cleaned, MERGE SCD 1) · courses (CHECK constraint) · instructors · enrollments (insert-only MERGE) · enrollments_quarantine (dq_reason)</div>
<div class="layer" style="background:#f2c94c;color:#1d2433">🥇 <b>gold</b> — dim_course · dim_student (masked e-mail) · dim_date · fact_enrollments (liquid clustered, row filter demo) · v_course_performance · mv_daily_category_revenue · course_leaderboard</div>
<div class="layer" style="background:#1c7ed6">📊 <b>consumers</b> — dashboards, SQL warehouse, BI tools, the job that refreshes it all every morning</div>
""")

# COMMAND ----------

# DBTITLE 1,Slide 6 · Picking the ingestion tool
show("""
<div class="kicker">Slide 6 · Same lakehouse, four ingestion tools — on purpose</div>
<table class="tbl">
<tr><th>Source</th><th>Tool</th><th>Why this one</th></tr>
<tr><td>instructors (rare, small)</td><td><code>COPY INTO</code></td><td>SQL, <b>idempotent</b> (skips loaded files), thousands of files max — perfect for occasional drops</td></tr>
<tr><td>courses, students, updates (one-off snapshots)</td><td>CTAS + <code>read_files()</code></td><td>simplest; options for CSV; <code>_metadata</code>; one statement</td></tr>
<tr><td>enrollments (new file every day, forever)</td><td><b>Auto Loader</b> + <code>availableNow</code></td><td>discovers <b>only new files</b>, scales to millions, checkpoint, schema inference + <b>evolution</b></td></tr>
<tr><td>enrollments again, declaratively</td><td><b>streaming table</b> <code>FROM STREAM read_files(...)</code></td><td>the pipeline manages checkpoints, retries, DAG and data-quality metrics</td></tr>
<tr><td>a SaaS app / operational DB (not in this capstone)</td><td><b>Lakeflow Connect</b> managed connector</td><td>no code: connection (+ gateway for databases) → serverless pipeline → streaming tables, CDC</td></tr>
<tr><td>a one-time spreadsheet</td><td>UI upload (volume / create table)</td><td>manual, small files</td></tr>
</table>
""" + callout("exam", "Expect scenario questions: <i>“files arrive continuously, millions, schema changes”</i> → Auto Loader · <i>“SQL analyst, re-runnable, thousands of files”</i> → COPY INTO · <i>“Salesforce/SQL Server, minimal code”</i> → Lakeflow Connect."))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · Silver, gold, automation, security

# COMMAND ----------

# DBTITLE 1,Slide 7 · Silver rules
show("""
<div class="kicker">Slide 7 · Silver = clean, typed, deduplicated, validated</div>
<div class="grid">
 <div class="card"><h3>🧹 Clean</h3><code>lower(trim(email))</code>, <code>coalesce(category, 'Uncategorized')</code>, casts to DECIMAL / DATE / TIMESTAMP</div>
 <div class="card green"><h3>🔁 Deduplicate</h3><code>SELECT DISTINCT</code> for exact duplicates · <code>dropDuplicates(["enrollment_id"])</code> · insert-only <code>MERGE</code> across loads</div>
 <div class="card orange"><h3>🧾 Upsert (SCD 1)</h3><code>MERGE … WHEN MATCHED AND s.updated_at &gt; t.updated_at THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *</code></div>
 <div class="card red"><h3>🚧 Validate &amp; quarantine</h3>negative price · unknown student · unknown course → <code>silver.enrollments_quarantine</code> with <code>dq_reason</code></div>
 <div class="card purple"><h3>🛡️ Enforce</h3><code>ADD CONSTRAINT valid_price CHECK (list_price &gt;= 0)</code>, <code>SET NOT NULL</code></div>
 <div class="card teal"><h3>⏪ Recover</h3><code>DESCRIBE HISTORY</code>, <code>VERSION AS OF</code>, <code>RESTORE TABLE</code></div>
</div>
<p class="muted">Days 1–4 → silver: 584 valid enrollments, 12 in quarantine (4 per rule), revenue 42,105.50.</p>
""")

# COMMAND ----------

# DBTITLE 1,Slide 8 · Gold star schema
show("""
<div class="kicker">Slide 8 · Gold = business-ready</div>
<div class="flow">
 <div class="step"><b>dim_student</b>405 · e-mail masked</div><div class="arrow">─</div>
 <div class="step" style="border-color:#e8590c"><b>fact_enrollments</b>one row per enrollment · CLUSTER BY (enrolled_date, course_id)</div><div class="arrow">─</div>
 <div class="step"><b>dim_course</b>20 + instructor</div><div class="arrow">─</div>
 <div class="step"><b>dim_date</b>sequence + explode</div>
</div>
<table class="tbl">
<tr><th>Object</th><th>Kind</th><th>Why that kind</th></tr>
<tr><td>gold.v_course_performance</td><td>view</td><td>always current, cheap at this size, a single definition of "revenue" and "completion rate"</td></tr>
<tr><td>gold.mv_daily_category_revenue</td><td>materialized view</td><td>dashboard reads it many times a day → precompute, refresh after each load (incrementally)</td></tr>
<tr><td>gold.course_leaderboard</td><td>table (CTAS)</td><td>window functions + <code>QUALIFY</code>, snapshot published by the job</td></tr>
</table>
""" + callout("tip", "The job rewrites gold with <code>INSERT OVERWRITE</code> — it keeps grants, masks, clustering and history. <code>CREATE OR REPLACE</code> would redefine the table."))

# COMMAND ----------

# DBTITLE 1,Slide 9 · The daily job
show("""
<div class="kicker">Slide 9 · Lakeflow Jobs — “14 SkillWave Daily Refresh” (06:00 Asia/Riyadh)</div>
<pre style="font-size:13px;background:#f7f9fc;border:1px solid #e3e8ef;border-radius:10px;padding:12px">
                    ┌──▶ ingest_bronze ──▶ build_silver ──▶ check_quality ──true──▶ quarantine_alert
 land_day ──────────┤    (retries: 1)      (task values)      (If/else)
                    │                                          └──false──┐
                    └──▶ refresh_pipeline (pipeline task) ───────────────┴──▶ build_gold
</pre>
<div class="grid">
 <div class="card"><h3>🎛️ Job parameters</h3><code>catalog</code>, <code>days</code>, <code>max_quarantine</code> — pushed to every task as widgets</div>
 <div class="card green"><h3>📨 Task values</h3><code>dbutils.jobs.taskValues.set("quarantined", n)</code> → <code>{{tasks.build_silver.values.quarantined}}</code></div>
 <div class="card orange"><h3>🔀 If/else</h3><code>quarantined &gt; max_quarantine</code> → alert branch; gold task <b>Excluded</b> (run still succeeds)</div>
 <div class="card purple"><h3>🔁 Retry</h3>a new column stops Auto Loader once → the retry continues with the evolved schema</div>
</div>
""")

# COMMAND ----------

# DBTITLE 1,Slide 10 · Who may see what
show("""
<div class="kicker">Slide 10 · Access model</div>
<table class="tbl">
<tr><th>Principal</th><th>Privileges</th><th>Sees</th></tr>
<tr><td>you (owner)</td><td>owner of the catalog → everything</td><td>all data — but masks/filters still apply to you!</td></tr>
<tr><td><code>account users</code></td><td><code>USE CATALOG skillwave</code> · <code>USE SCHEMA</code> + <code>SELECT</code> on <b>gold</b></td><td>gold only; e-mails masked</td></tr>
<tr><td><code>skillwave_pii_readers</code></td><td>(group checked inside <code>gold.mask_email</code>)</td><td>full e-mails</td></tr>
<tr><td>regional managers</td><td>rows of <code>silver.region_access</code> → row filter <code>gold.rf_student_country</code></td><td>only their students' countries</td></tr>
<tr><td>the job (prod)</td><td>a <b>service principal</b> (bundle <code>run_as</code>)</td><td>what it needs to write bronze → gold</td></tr>
</table>
""" + callout("trap", "Unity Catalog has <b>no DENY</b>. A schema-level <code>SELECT</code> can't be removed with a table-level <code>REVOKE</code> — revoke where it was granted."))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · Coverage and plan

# COMMAND ----------

# DBTITLE 1,Slide 11 · Exam objectives covered
show("""
<div class="kicker">Slide 11 · Every exam domain is in the capstone</div>
<table class="tbl">
<tr><th>Domain (weight)</th><th>Where in the capstone</th></tr>
<tr><td>D1 Platform (~6 %)</td><td>L1 catalog/schemas/volumes · L2 Delta history, time travel, RESTORE · serverless everywhere</td></tr>
<tr><td>D2 Ingestion &amp; loading (~21 %)</td><td>L1 querying files · L2 COPY INTO, read_files, Auto Loader, schema evolution, unstructured text · L4 streaming tables</td></tr>
<tr><td>D3 Transformation &amp; modeling (~22 %)</td><td>L2 cleaning, dedup, MERGE, quarantine · L3 joins, set ops, PIVOT, windows, gold view/MV/table · L4 expectations</td></tr>
<tr><td>D4 Lakeflow Jobs (~16 %)</td><td>L4 DAG, parameters, task values, If/else, retries, pipeline task, schedule, run with different parameters</td></tr>
<tr><td>D5 CI/CD (~10 %)</td><td>L4 Git folder branch/commit/PR, job as YAML, bundle targets &amp; variables, CLI</td></tr>
<tr><td>D6 Troubleshooting &amp; optimization (~10 %)</td><td>L3 liquid clustering, OPTIMIZE, EXPLAIN, query profile · L4 run history, event log, system tables</td></tr>
<tr><td>D7 Governance &amp; security (~15 %)</td><td>L1 GRANT/inheritance · L3 column mask, row filter, information_schema · L5 mask + least privilege</td></tr>
</table>
""")

# COMMAND ----------

# DBTITLE 1,Slide 12 · Run order and expected numbers
show("""
<div class="kicker">Slide 12 · Plan your capstone (≈ 5 hours)</div>
<div class="flow">
 <div class="step"><b>14-L1</b>45 min</div><div class="arrow">→</div>
 <div class="step"><b>14-L2</b>60 min</div><div class="arrow">→</div>
 <div class="step"><b>14-L3</b>55 min</div><div class="arrow">→</div>
 <div class="step"><b>14-L4</b>70 min</div><div class="arrow">→</div>
 <div class="step"><b>14-L5</b>45 min</div><div class="arrow">→</div>
 <div class="step"><b>14-Q</b>90 min mock</div>
</div>
<table class="tbl">
<tr><th>Checkpoint</th><th>Expected</th></tr>
<tr><td>bronze after L2 (days 1–4)</td><td>instructors 10 · courses 21 · students 403 · updates 25 · syllabus 20 · enrollments 600</td></tr>
<tr><td>silver after L2</td><td>students 405 (128 pro) · courses 20 · enrollments 584 · quarantine 12 · revenue 42,105.50</td></tr>
<tr><td>gold after L3</td><td>dim_course 20 · dim_student 405 · dim_date 30 · fact 584 · leaderboard 16</td></tr>
<tr><td>after the 3 job runs of L4</td><td>silver 876 · quarantine 18 · fact 876 · revenue 62,671.40</td></tr>
</table>
""" + callout("tip", "Each lab starts with a <b>catch-up</b> cell: if you skipped a part, the reference implementation in <code>_14_prepare</code> builds what's missing."))
