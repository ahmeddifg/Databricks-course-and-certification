# Databricks notebook source
# MAGIC %md
# MAGIC # 🎓 Section 00 · Course Overview & Exam Blueprint
# MAGIC ### Databricks Certified Data Engineer Associate — May 2026 exam guide
# MAGIC
# MAGIC **In this presentation**
# MAGIC 1. The exam at a glance
# MAGIC 2. The 7 domains and their weights
# MAGIC 3. How this course maps to the exam
# MAGIC 4. The ShopWave dataset we use in every lab
# MAGIC 5. Your study plan & exam-day strategy
# MAGIC 6. Which environment to use
# MAGIC
# MAGIC > ▶️ **Run all** (any compute, takes a few seconds) to render the diagrams.

# COMMAND ----------

# MAGIC %run ../../Includes/_style

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · The exam at a glance

# COMMAND ----------

# DBTITLE 1,Slide · Exam facts
show("""
<div class="kicker">Slide 1 · Exam facts</div>
<h2>What you are signing up for</h2>
<div class="grid">
  <div class="card"><h3>📋 Format</h3>45 <b>scored</b> multiple-choice questions<br>
     <span class="muted">+ possibly a few unscored items you can't identify</span></div>
  <div class="card orange"><h3>⏱️ Time</h3>90 minutes<br><span class="muted">≈ 2 minutes per question</span></div>
  <div class="card green"><h3>💵 Fee</h3>USD 200 per attempt</div>
  <div class="card purple"><h3>🖥️ Delivery</h3>Online proctored or test center<br>No test aids allowed</div>
  <div class="card teal"><h3>📆 Validity</h3>2 years — recertify by passing the <i>current</i> exam version</div>
  <div class="card gray"><h3>🌐 Languages</h3>English, Japanese, Portuguese (BR), Korean</div>
</div>
""" + callout("tip", "There are no prerequisites, but Databricks expects <b>hands-on experience</b> with the tasks in the guide. That's why every section here has labs."))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · The 7 exam domains
# MAGIC
# MAGIC The **May 2026** guide organizes the exam into **7 domains**. Compared with older versions, **Lakeflow Jobs**, **CI/CD** and
# MAGIC **Troubleshooting/Optimization** now have their own domains, Delta Sharing no longer appears in the outline, and
# MAGIC Databricks Asset Bundles are now called **Declarative Automation Bundles**.

# COMMAND ----------

# DBTITLE 1,Slide · Domain weights
domains = [
    ("D3 · Data Transformation and Modeling", 22, "#1c7ed6", "07, 08, 09"),
    ("D2 · Data Ingestion and Loading", 21, "#2f9e44", "05, 06, 08"),
    ("D4 · Working with Lakeflow Jobs", 16, "#e8590c", "10"),
    ("D7 · Governance and Security", 15, "#7048e8", "04, 13"),
    ("D5 · Implementing CI/CD", 10, "#0c8599", "11"),
    ("D6 · Troubleshooting, Monitoring & Optimization", 10, "#e03131", "02, 12"),
    ("D1 · Databricks Intelligence Platform", 6, "#868e96", "01, 03, 04"),
]
rows = "".join(
    f"<tr><td>{name}</td><td style='width:45%'><span class='bar' style='width:{w * 3}%;background:{c}'></span> "
    f"<b>{w}%</b> <span class='muted'>≈ {round(45 * w / 100)} questions</span></td><td>{secs}</td></tr>"
    for name, w, c, secs in domains
)
show(f"""
<div class="kicker">Slide 2 · Where the points are</div>
<h2>Exam domains, ranked by weight</h2>
<table class="tbl"><tr><th>Domain</th><th>Weight</th><th>Course sections</th></tr>{rows}</table>
""" + callout("exam", "D2 + D3 alone are <b>43 %</b> of the exam (~19 questions). Ingestion and transformation skills must be automatic.")
    + callout("trap", "D1 is only 6 %, but its concepts (Delta Lake, Unity Catalog, compute choice) show up <i>inside</i> questions from every other domain."))

# COMMAND ----------

# MAGIC %md
# MAGIC ### What each domain tests (objectives from the guide)
# MAGIC
# MAGIC | Domain | Objectives you must be able to do |
# MAGIC |---|---|
# MAGIC | **D1 Platform (6 %)** | Explain the platform's core components (architecture, Delta Lake, Unity Catalog) · choose the right compute service and understand cost models |
# MAGIC | **D2 Ingestion & Loading (21 %)** | Batch vs streaming vs incremental patterns · `COPY INTO` · Auto Loader with schema enforcement/evolution · Lakeflow Connect · JDBC/ODBC & REST ingestion · choose the right ingestion method · semi-structured & unstructured data |
# MAGIC | **D3 Transformation & Modeling (22 %)** | Clean bronze→silver · joins & unions · manipulate columns, rows & nested structures · deduplicate & aggregate · Spark tuning parameters · build gold-layer objects · data-quality checks |
# MAGIC | **D4 Lakeflow Jobs (16 %)** | Control flow · tasks & dependencies (DAG) · schedules & trigger types · time-based vs data-driven triggers |
# MAGIC | **D5 CI/CD (10 %)** | Git folders workflow · Declarative Automation Bundles: variables, overrides, deployment · Databricks CLI |
# MAGIC | **D6 Troubleshooting (10 %)** | Job run history trends · pipeline health · Spark UI bottlenecks · liquid clustering & predictive optimization · cluster & memory failures |
# MAGIC | **D7 Governance & Security (15 %)** | Managed vs external tables · `GRANT` / `REVOKE` / `DENY` · column masks & row filters · Unity Catalog ABAC policies |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · How this course maps to the exam
# MAGIC
# MAGIC | # | Section | What you'll be able to do | Domain |
# MAGIC |---|---|---|---|
# MAGIC | 00 | Course Guide & Setup | Set up your workspace and the ShopWave data | — |
# MAGIC | 01 | Databricks Introduction & Platform | Explain the lakehouse, use notebooks & dbutils, choose compute | D1 |
# MAGIC | 02 | Spark Foundations | Reason about partitions, shuffles, lazy evaluation, `explain()` | D3, D6 |
# MAGIC | 03 | Delta Lake | Write, update, merge, time-travel and maintain Delta tables | D1 |
# MAGIC | 04 | Unity Catalog & Data Objects | Organise catalogs/schemas/tables/views/volumes; managed vs external | D1, D7 |
# MAGIC | 05 | Data Ingestion & Loading | Query files, CTAS, `COPY INTO`, Lakeflow Connect, JDBC/REST | D2 |
# MAGIC | 06 | Structured Streaming & Auto Loader | Build incremental, exactly-once ingestion | D2 |
# MAGIC | 07 | Data Transformation & Modeling | Clean, reshape, join, dedupe, aggregate, model the gold layer | D3 |
# MAGIC | 08 | Medallion → Declarative Pipelines | Build bronze/silver/gold by hand, then with Lakeflow Spark Declarative Pipelines | D2, D3 |
# MAGIC | 09 | Databricks SQL & BI Serving | Serve gold tables: SQL warehouses, queries, dashboards, alerts | D3 |
# MAGIC | 10 | Lakeflow Jobs | Orchestrate tasks, control flow, parameters, triggers, repairs | D4 |
# MAGIC | 11 | CI/CD | Git folders + Declarative Automation Bundles + CLI | D5 |
# MAGIC | 12 | Troubleshooting & Optimization | Read run history, Spark UI, query profile; tune layout & compute | D6 |
# MAGIC | 13 | Governance & Security | Privileges, masks, row filters, ABAC, lineage | D7 |
# MAGIC | 14 | Capstone & Mock Exams | End-to-end project + 2 full timed mock exams | all |

# COMMAND ----------

# DBTITLE 1,Slide · How a section works
show("""
<div class="kicker">Slide 3 · The learning loop</div>
<h2>Every section follows the same 4 steps</h2>
<div class="flow">
  <div class="step"><b>📊 presentation</b>Concepts, diagrams, exam traps</div><div class="arrow">➜</div>
  <div class="step"><b>🗂️ data</b>What data the section uses (auto-created)</div><div class="arrow">➜</div>
  <div class="step"><b>🧪 labs</b>Guided lab → challenge → solution</div><div class="arrow">➜</div>
  <div class="step"><b>📝 questions</b>Exam-style quiz with explanations</div>
</div>
<div class="grid">
  <div class="card orange"><h3>🎯 Exam focus</h3>Highlighted in every presentation — memorise these.</div>
  <div class="card red"><h3>⚠️ Exam traps</h3>The wrong answers that look right. Most points are lost here.</div>
  <div class="card gray"><h3>🕰️ Legacy</h3>Old names (DLT, Workflows, DABs, <code>hive_metastore</code>…) you must still recognise.</div>
  <div class="card green"><h3>✅ Best practice</h3>What a senior data engineer would do in production.</div>
</div>
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · The ShopWave dataset
# MAGIC
# MAGIC Every lab uses **one storyline**: *ShopWave*, an online retailer. You'll take its raw files all the way to governed,
# MAGIC dashboard-ready gold tables. The data is **generated inside your workspace** by `Includes/_setup` (deterministic → everyone gets the same numbers),
# MAGIC so no external storage or credentials are needed.

# COMMAND ----------

# DBTITLE 1,Slide · Dataset map
show("""
<div class="kicker">Slide 4 · Data model</div>
<h2>ShopWave raw data — <code>/Volumes/&lt;catalog&gt;/shopwave/raw/</code></h2>
<div class="grid">
  <div class="card"><h3>👤 customers-json/</h3>3 JSON files · 300 customers
    <ul><li><code>customer_id</code> C0001…</li><li><code>email</code> (some nulls!)</li>
    <li><code>profile</code> JSON <i>string</i>: name, gender, address{street, city, country}</li><li><code>updated</code> ISO timestamp</li></ul></div>
  <div class="card green"><h3>🛍️ orders-parquet/</h3>Parquet · 2,010 orders (Jan–Jun 2026)
    <ul><li><code>order_id</code>, <code>customer_id</code></li><li><code>order_timestamp</code> unix seconds</li>
    <li><code>quantity</code>, <code>total</code></li><li><code>items</code> ARRAY&lt;STRUCT&lt;product_id, quantity, subtotal&gt;&gt;</li></ul></div>
  <div class="card orange"><h3>📦 products-csv/</h3>2 CSV files · <code>;</code> delimiter · header
    <ul><li><code>product_id</code> P001…</li><li><code>title</code>, <code>brand</code></li><li><code>category</code>, <code>price</code></li></ul></div>
  <div class="card purple"><h3>🌊 orders-staging/ → orders-landing/</h3>10 JSON batches (July 2026)
    <ul><li>Same shape as orders</li><li><code>land_new_orders()</code> copies the next batch into <code>orders-landing/</code> to simulate files arriving</li></ul></div>
</div>
<div class="flow">
  <div class="step"><b>customers</b>customer_id</div><div class="arrow">⟵ 1 : N ⟶</div>
  <div class="step"><b>orders</b>order_id · customer_id · items[ ]</div><div class="arrow">⟵ N : M ⟶</div>
  <div class="step"><b>products</b>product_id</div>
</div>
""" + callout("info", "Deliberate data-quality problems are hidden in the data (null emails, cancelled orders with quantity 0, "
              "orders from an unknown customer <code>C9999</code>, exact duplicate rows). You'll find and fix them in sections 07–08."))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 · Study plan & exam strategy
# MAGIC
# MAGIC **Suggested 6-week plan (6–8 hours/week)**
# MAGIC
# MAGIC | Week | Sections | Milestone |
# MAGIC |---|---|---|
# MAGIC | 1 | 00, 01, 02 | Workspace ready · notebooks & Spark basics fluent |
# MAGIC | 2 | 03, 04 | Delta Lake & Unity Catalog quizzes ≥ 80 % |
# MAGIC | 3 | 05, 06 | Can ingest files in batch, incrementally, and as streams |
# MAGIC | 4 | 07, 08, 09 | ShopWave medallion pipeline running end-to-end |
# MAGIC | 5 | 10, 11, 12, 13 | Jobs, bundles, tuning, governance quizzes ≥ 80 % |
# MAGIC | 6 | 14 | 2 mock exams ≥ 80 % → book the exam |
# MAGIC
# MAGIC **Exam-day strategy**
# MAGIC * ⏱️ ~2 min per question. Flag anything taking > 3 min and come back to it.
# MAGIC * 🔍 Read the **last sentence first** — it tells you what's actually being asked (*"most cost-effective"*, *"least operational overhead"*, *"minimum privileges"*).
# MAGIC * ❌ Eliminate options that use **legacy** features when a modern one fits (e.g. DBFS mounts vs Unity Catalog volumes).
# MAGIC * 🧩 On code questions, check **syntax details**: `VERSION AS OF` vs `@v`, `outputMode`, `trigger(availableNow=True)`.
# MAGIC * ✅ No penalty for guessing → never leave a question blank.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6 · Which environment should you use?
# MAGIC
# MAGIC | Option | Cost | Good for | Limits you will hit in this course |
# MAGIC |---|---|---|---|
# MAGIC | **Databricks Free Edition** ✅ recommended | Free | All sections | Serverless compute only (no classic clusters or pools) · one 2X-Small SQL warehouse · no account console · limited concurrency. Labs clearly mark the few UI-only steps you can't do. |
# MAGIC | **Free trial** (AWS / Azure / GCP) | Free credits, then paid | Everything, including classic clusters & account console | Trial expires; you pay for cloud resources |
# MAGIC | **Your company workspace** | Company-paid | Everything | Needs permission to create a schema in a catalog (use `COURSE_CATALOG`) — ask your admin |
# MAGIC
# MAGIC > 🧪 **Next:** open `labs/00-L1 - Environment Setup` and get your workspace ready.
