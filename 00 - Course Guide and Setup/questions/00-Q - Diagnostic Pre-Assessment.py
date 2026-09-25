# Databricks notebook source
# MAGIC %md
# MAGIC # 📝 00-Q · Diagnostic Pre-Assessment
# MAGIC **Why:** find out what you already know *before* you start. Don't study for this — just answer honestly.
# MAGIC Your per-topic score at the end tells you which sections deserve extra time.
# MAGIC
# MAGIC * 16 questions · one from almost every exam objective area
# MAGIC * Click **Check answer** after each question, then **📊 Show my result** at the top.
# MAGIC * No score is saved — it's just for you. Retake it after Section 14 and compare!
# MAGIC
# MAGIC > ▶️ **Run all** — the quiz renders in a few seconds on any compute.

# COMMAND ----------

# MAGIC %run ../../Includes/_quiz

# COMMAND ----------

# DBTITLE 1,Question bank (collapse this cell - it contains the answers)
questions = [
    {"topic": "D1 Platform",
     "q": "Which Databricks component provides **centralized governance** — access control, auditing and lineage — for data and AI assets across all workspaces in a region?",
     "options": ["Delta Lake", "Unity Catalog", "Photon", "Lakeflow Jobs"],
     "answer": 1,
     "explanation": "Unity Catalog is the governance layer (metastore → catalog → schema → objects). Delta Lake is the storage format, Photon is the query engine, Lakeflow Jobs is orchestration."},
    {"topic": "D1 Platform",
     "q": "A team needs to run a **scheduled production job** every night with the **least operational overhead**. Which compute is the best fit?",
     "options": ["An all-purpose cluster shared by the team", "Serverless compute for jobs",
                 "A SQL warehouse", "A single-node all-purpose cluster with auto-termination disabled"],
     "answer": 1,
     "explanation": "Serverless jobs compute starts quickly, scales automatically and needs no configuration. All-purpose clusters are for interactive work and cost more per DBU; SQL warehouses run SQL workloads, not general notebooks/Python jobs."},
    {"topic": "D1 Delta Lake",
     "q": "Which Delta Lake feature lets you query a table **as it was yesterday**?",
     "options": ["VACUUM", "OPTIMIZE", "Time travel (`TIMESTAMP AS OF`)", "Z-ordering"],
     "answer": 2,
     "explanation": "Time travel reads an older table version using `VERSION AS OF n` / `TIMESTAMP AS OF ts`. VACUUM deletes old files (and can break time travel), OPTIMIZE compacts files, Z-order co-locates data."},
    {"topic": "D2 Ingestion",
     "q": "New JSON files land in cloud storage continuously — **millions of files per day** — and new columns sometimes appear. What is the recommended way to ingest them incrementally?",
     "options": ["`COPY INTO`", "Auto Loader (`cloudFiles`)", "`CREATE TABLE AS SELECT` every hour", "`INSERT OVERWRITE` every hour"],
     "answer": 1,
     "explanation": "Auto Loader scales to millions of files, tracks processed files in a checkpoint and supports schema inference/evolution. COPY INTO is fine for thousands of files; CTAS/INSERT OVERWRITE reprocess everything."},
    {"topic": "D2 Ingestion",
     "q": "Which SQL command **idempotently** loads only the files that have not been loaded before into an existing Delta table?",
     "options": ["`INSERT INTO t SELECT * FROM json.<path>`", "`COPY INTO`", "`MERGE INTO`", "`CREATE OR REPLACE TABLE ... AS SELECT`"],
     "answer": 1,
     "explanation": "COPY INTO remembers which files it already loaded and skips them on re-runs. INSERT INTO would duplicate data; MERGE needs a source dataset; CREATE OR REPLACE rewrites the table."},
    {"topic": "D3 Transformation",
     "q": "You must keep **only the most recent row per customer** (by `updated` timestamp), deterministically. Which approach is correct?",
     "options": ["`df.dropDuplicates(['customer_id'])`", "`df.distinct()`",
                 "`row_number()` over a window partitioned by `customer_id` ordered by `updated DESC`, then keep row 1",
                 "`GROUP BY customer_id` and `max(*)`"],
     "answer": 2,
     "explanation": "A window with row_number() lets you choose exactly which row survives. dropDuplicates keeps an arbitrary row per key; distinct removes only fully identical rows."},
    {"topic": "D3 Transformation",
     "q": "Column `profile` is a **STRING** containing JSON. Which Databricks SQL expression returns the nested country?",
     "code": 'profile = \'{"name":"Lina","address":{"city":"Osaka","country":"Japan"}}\'',
     "options": ["`profile.address.country`", "`profile:address:country`", "`profile['address']['country']`", "`get(profile, 'country')`"],
     "answer": 1,
     "explanation": "The colon syntax `col:path` extracts fields from a JSON *string*. Dot notation works on STRUCT columns (e.g. after `from_json`), not on strings."},
    {"topic": "D3 Pipelines",
     "q": "In a Lakeflow Spark Declarative Pipeline, what happens to rows that violate `CONSTRAINT valid_qty EXPECT (quantity > 0) ON VIOLATION DROP ROW`?",
     "options": ["The pipeline update fails", "The rows are written but flagged in the event log",
                 "The rows are dropped from the target and counted in data-quality metrics", "The rows are moved to a quarantine table automatically"],
     "answer": 2,
     "explanation": "DROP ROW removes invalid rows and records the counts in the pipeline event log. FAIL UPDATE stops the update; no ON VIOLATION clause (warn) keeps the rows."},
    {"topic": "D4 Jobs",
     "q": "Task B must run **only after task A succeeds**, in the same job. How do you configure this?",
     "options": ["Create two separate jobs with schedules 5 minutes apart", "Set task B's **Depends on** to task A",
                 "Call task A with `%run` from task B", "Add a `time.sleep()` at the start of task B"],
     "answer": 1,
     "explanation": "Lakeflow Jobs models tasks as a DAG: 'Depends on' creates the edge A → B, and by default B runs only if A succeeds (run-if condition: All succeeded)."},
    {"topic": "D4 Jobs",
     "q": "A job should start **whenever new files arrive** in a Unity Catalog volume, instead of on a fixed schedule. Which trigger type fits?",
     "options": ["Scheduled (cron) trigger", "File arrival trigger", "Continuous trigger", "Manual trigger"],
     "answer": 1,
     "explanation": "File arrival triggers are data-driven: the job runs when new files appear in the monitored storage location."},
    {"topic": "D5 CI/CD",
     "q": "Which tool lets you define jobs and pipelines as **YAML source files** and deploy the same project to dev and prod **targets** with `databricks bundle deploy -t prod`?",
     "options": ["Git folders", "Declarative Automation Bundles (formerly Databricks Asset Bundles)", "Databricks Connect", "Partner Connect"],
     "answer": 1,
     "explanation": "Bundles describe resources as code (databricks.yml) and deploy them per target via the Databricks CLI. Git folders only sync code with a Git provider."},
    {"topic": "D6 Optimization",
     "q": "Which data-layout technique is **incremental**, can have its keys **changed without rewriting existing data**, and replaces partitioning + Z-order for new tables?",
     "options": ["Hive-style partitioning", "Z-ordering", "Liquid clustering", "Bucketing"],
     "answer": 2,
     "explanation": "Liquid clustering (`CLUSTER BY`, or `CLUSTER BY AUTO` with predictive optimization) is incremental and flexible. Z-order is not incremental; changing partition columns requires a full rewrite."},
    {"topic": "D6 Troubleshooting",
     "q": "On classic compute, where do you look to find **data skew and disk spill** in a slow stage?",
     "options": ["The Spark UI (stages & tasks)", "Catalog Explorer", "The Git folder history", "The account console"],
     "answer": 0,
     "explanation": "The Spark UI shows per-stage task durations, shuffle and spill metrics. (On serverless, the Spark UI isn't available — use the query profile instead.)"},
    {"topic": "D7 Governance",
     "q": "You run `DROP TABLE` on a Unity Catalog **managed** table. What happens?",
     "options": ["Only the metadata is removed; the data files stay in storage",
                 "The metadata is removed and the underlying data files are deleted (recoverable with UNDROP for a limited time)",
                 "Nothing — managed tables cannot be dropped", "The table is converted to an external table"],
     "answer": 1,
     "explanation": "For managed tables Unity Catalog owns the files, so they are deleted too (UNDROP can recover it for 7 days). For external tables only the metadata is dropped."},
    {"topic": "D7 Governance",
     "q": "Group `analysts` must query table `sales.retail.orders`. Which **minimum** set of privileges is required?",
     "options": ["`SELECT` on the table only", "`USE CATALOG` on `sales`, `USE SCHEMA` on `sales.retail`, `SELECT` on the table",
                 "`ALL PRIVILEGES` on the catalog", "`MODIFY` on the table"],
     "answer": 1,
     "explanation": "In Unity Catalog you need USE CATALOG and USE SCHEMA on the parents to reach an object, plus SELECT on the table itself."},
    {"topic": "D7 Governance",
     "q": "Non-admin users must see `***` instead of real emails, **without creating a copy** of the table. What should you use?",
     "options": ["A separate table without the email column", "A column mask function applied with `ALTER TABLE ... ALTER COLUMN email SET MASK`",
                 "`REVOKE SELECT` on the table", "`VACUUM` the email column"],
     "answer": 1,
     "explanation": "Column masks (and row filters) apply fine-grained security on the same table at query time based on who is querying."},
]

render_quiz(questions, title="Diagnostic Pre-Assessment",
            meta="16 questions across all 7 domains · use the per-topic result to plan your study time")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 📌 How to use your result
# MAGIC | Weak topic | Spend extra time on |
# MAGIC |---|---|
# MAGIC | D1 Platform / Delta Lake | Sections 01, 03, 04 |
# MAGIC | D2 Ingestion | Sections 05, 06 |
# MAGIC | D3 Transformation / Pipelines | Sections 07, 08, 09 |
# MAGIC | D4 Jobs | Section 10 |
# MAGIC | D5 CI/CD | Section 11 |
# MAGIC | D6 Optimization / Troubleshooting | Sections 02, 12 |
# MAGIC | D7 Governance | Sections 04, 13 |

# COMMAND ----------

# DBTITLE 1,Optional - print the answer key
# print_answer_key(questions)
