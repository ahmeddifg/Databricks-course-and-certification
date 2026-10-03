# Databricks notebook source
# MAGIC %md
# MAGIC # 🏗️ Lab 14-L1 · Build the SkillWave Catalog from Scratch (Guided)
# MAGIC **Capstone part 1 of 4** · **Time:** ~45 min · **Compute:** Serverless notebook · **Works on Free Edition**
# MAGIC
# MAGIC Welcome to the capstone! Until now every lab lived in the ready-made `shopwave` schema. Now **you** are the data architect
# MAGIC of a new company — **SkillWave Academy**, an online learning platform — and you start from an **empty metastore**.
# MAGIC
# MAGIC **The business in one picture**
# MAGIC
# MAGIC ```
# MAGIC  INSTRUCTOR ──teaches──▶ COURSE ◀──enrolls in── STUDENT
# MAGIC      1                  N    1                N     1
# MAGIC                              │                │
# MAGIC                              └── ENROLLMENT ──┘   (one row per student × course: price paid, coupon, channel,
# MAGIC                                      │                                         status, progress %)
# MAGIC                                      └──▶ REVIEW  (rating 1-5 — the challenge lab)
# MAGIC ```
# MAGIC
# MAGIC | Source | Format | Arrives | Rows |
# MAGIC |---|---|---|---|
# MAGIC | `instructors/` | CSV (header) | once | 10 |
# MAGIC | `courses/` | CSV (header) | once | 21 lines = 20 courses + 1 duplicate; one course has **no category** |
# MAGIC | `students/` | JSON lines, `profile` is a **JSON string**, `interests` is an **array** | once | 403 = 400 students + 3 duplicates; messy & missing e-mails |
# MAGIC | `enrollments/` | JSON lines — **one file per day** (1–6 Sep 2026) | daily | 150 per file: 146 valid + 1 duplicate + 3 bad rows; **new column `referrer` from day 4** |
# MAGIC | `student-updates/` | JSON lines | once | 25 = 20 changed students + 5 new |
# MAGIC | `syllabus/` | plain **text** files (unstructured) | once | 20 |
# MAGIC | `reviews/` | JSON lines | once | 123 (challenge lab) |
# MAGIC
# MAGIC **Your target architecture** — one catalog, one **schema per medallion layer**:
# MAGIC
# MAGIC ```
# MAGIC skillwave                      ◀── catalog (you create it)
# MAGIC ├── landing                    ◀── schema for files
# MAGIC │   ├── raw          (volume)  ◀── the source files
# MAGIC │   └── checkpoints  (volume)  ◀── streaming checkpoints
# MAGIC ├── bronze                     ◀── raw tables (+ ingestion metadata)
# MAGIC ├── silver                     ◀── clean, typed, deduplicated tables
# MAGIC └── gold                       ◀── star schema, views, materialized view → BI
# MAGIC ```
# MAGIC
# MAGIC | Part | You will… | Exam objective |
# MAGIC |---|---|---|
# MAGIC | 1 | Create the **catalog** and inspect it | D1 Unity Catalog · D7 managed storage |
# MAGIC | 2 | Create one **schema** per layer, with comments and tags | D1 / D7 |
# MAGIC | 3 | Create **volumes** and land the source files | D2 unstructured data, volumes |
# MAGIC | 4 | **Query files directly** — CSV, JSON, text, binary — and `read_files()` | D2 |
# MAGIC | 5 | Ownership, **grants** and `information_schema` | D7 |
# MAGIC | 6 | ✅ Checks | |
# MAGIC
# MAGIC > 🔁 **Catalog name.** This lab uses the name **`skillwave`**. On **Free Edition** you can create catalogs. On a paid workspace
# MAGIC > you need the `CREATE CATALOG` privilege on the metastore (ask your admin). If you must use another name, set
# MAGIC > `CAPSTONE_CATALOG = "<name>"` in the next cell **and** use *Edit → Find and replace* `skillwave` → your name in this notebook.

# COMMAND ----------

# DBTITLE 1,(Optional) use a different catalog name
# CAPSTONE_CATALOG = "skillwave"     # uncomment + change ONLY if you cannot use the name skillwave

# COMMAND ----------

# MAGIC %run ./_14_prepare

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1 · Create the catalog
# MAGIC
# MAGIC A **catalog** is the top level of the Unity Catalog namespace `catalog.schema.object`. It's the unit you usually use to
# MAGIC separate **environments** (`dev` / `prod`), **business units** or **projects** — here, the whole SkillWave lakehouse.
# MAGIC
# MAGIC 🧪 Run the next cell. `IF NOT EXISTS` makes it re-runnable.

# COMMAND ----------

# DBTITLE 1,1.1 CREATE CATALOG
# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS skillwave
# MAGIC COMMENT 'SkillWave Academy - capstone lakehouse (Section 14)';

# COMMAND ----------

# MAGIC %md
# MAGIC > 💡 **Where are its files?** Without `MANAGED LOCATION`, managed tables of this catalog are stored in the **metastore's
# MAGIC > default storage** (Free Edition: Databricks-managed default storage). On paid workspaces an admin often requires
# MAGIC > `CREATE CATALOG x MANAGED LOCATION 's3://…/abfss://…'` so each catalog's data lives in its own bucket/container.

# COMMAND ----------

# DBTITLE 1,1.2 Look at it - DESCRIBE CATALOG EXTENDED
# MAGIC %sql
# MAGIC DESCRIBE CATALOG EXTENDED skillwave;

# COMMAND ----------

# MAGIC %md
# MAGIC Note the **Owner** — **you**, because you created it. Owners have **all privileges** on the object and can grant them to
# MAGIC others. A new catalog already contains two schemas: `default` and `information_schema` (read-only metadata views).

# COMMAND ----------

# DBTITLE 1,1.3 SHOW CATALOGS and switch to the new catalog
# MAGIC %sql
# MAGIC SHOW CATALOGS LIKE 'sk*';

# COMMAND ----------

# DBTITLE 1,1.4 USE CATALOG
# MAGIC %sql
# MAGIC USE CATALOG skillwave;
# MAGIC SELECT current_catalog() AS catalog_in_use;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2 · One schema per layer
# MAGIC A **schema** (a.k.a. database) groups tables, views, volumes, functions and models. Using one schema per layer makes
# MAGIC permissions simple: analysts get `SELECT` on **gold** only, engineers write to bronze/silver.

# COMMAND ----------

# DBTITLE 1,2.1 CREATE SCHEMA × 4
# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS landing COMMENT 'Volumes with source files and streaming checkpoints';
# MAGIC CREATE SCHEMA IF NOT EXISTS bronze  COMMENT 'Raw data as ingested + ingestion metadata (_source_file, _ingested_at)';
# MAGIC CREATE SCHEMA IF NOT EXISTS silver  COMMENT 'Clean, typed, deduplicated, validated data';
# MAGIC CREATE SCHEMA IF NOT EXISTS gold    COMMENT 'Business-level star schema, views and materialized views for BI';

# COMMAND ----------

# MAGIC %md
# MAGIC > 💡 Because of `USE CATALOG skillwave`, `landing` means `skillwave.landing`. In jobs, pipelines and shared code prefer
# MAGIC > **fully qualified** names — a notebook's current catalog is not guaranteed elsewhere.

# COMMAND ----------

# DBTITLE 1,2.2 Tags - make the layers discoverable
# MAGIC %sql
# MAGIC ALTER SCHEMA bronze SET TAGS ('layer' = 'bronze', 'project' = 'skillwave');
# MAGIC ALTER SCHEMA silver SET TAGS ('layer' = 'silver', 'project' = 'skillwave');
# MAGIC ALTER SCHEMA gold   SET TAGS ('layer' = 'gold',   'project' = 'skillwave');

# COMMAND ----------

# DBTITLE 1,2.3 SHOW SCHEMAS + DESCRIBE SCHEMA EXTENDED
# MAGIC %sql
# MAGIC SHOW SCHEMAS IN skillwave;

# COMMAND ----------

# DBTITLE 1,2.4 Details of one schema
# MAGIC %sql
# MAGIC DESCRIBE SCHEMA EXTENDED skillwave.gold;

# COMMAND ----------

# MAGIC %md
# MAGIC 🖱️ **Look in the UI.** Open **Catalog** (left sidebar) → `skillwave`. You'll see the four schemas, their comments, and on the
# MAGIC schema page the **Tags**. Keep this tab open — you'll watch it fill up during the capstone.
# MAGIC
# MAGIC > 🎯 **Exam:** schemas *can't be renamed*; `DROP SCHEMA x` fails if it contains objects unless you add `CASCADE`;
# MAGIC > `DROP CATALOG` needs `CASCADE` as soon as it contains any schema besides `information_schema` (even `default`).
# MAGIC
# MAGIC ## Part 3 · Volumes and the source files
# MAGIC Tables hold **tabular** data; **volumes** hold **files** of any kind (CSV, JSON, images, PDFs, text…) and are governed
# MAGIC by Unity Catalog like tables. A volume path is `/Volumes/<catalog>/<schema>/<volume>/<path>`.

# COMMAND ----------

# DBTITLE 1,3.1 CREATE VOLUME × 2
# MAGIC %sql
# MAGIC CREATE VOLUME IF NOT EXISTS landing.raw         COMMENT 'Source files delivered by the SkillWave applications';
# MAGIC CREATE VOLUME IF NOT EXISTS landing.checkpoints COMMENT 'Streaming checkpoints and Auto Loader schema locations';

# COMMAND ----------

# DBTITLE 1,3.2 SHOW VOLUMES
# MAGIC %sql
# MAGIC SHOW VOLUMES IN landing;

# COMMAND ----------

# MAGIC %md
# MAGIC These are **managed volumes** — Unity Catalog decides where the files live and deletes them when you drop the volume.
# MAGIC An **external volume** (`CREATE EXTERNAL VOLUME … LOCATION 's3://…'`) points at existing cloud storage through an
# MAGIC **external location**; dropping it removes only the metadata.
# MAGIC
# MAGIC 🧪 Now generate the SkillWave source files into `landing.raw` (deterministic: everybody gets identical data). It also
# MAGIC "delivers" the first daily enrollment file (1 Sep 2026).

# COMMAND ----------

# DBTITLE 1,3.3 Generate the source files
_ = generate_skillwave_data()

# COMMAND ----------

# DBTITLE 1,3.4 LIST the volume (SQL)
# MAGIC %sql
# MAGIC LIST '/Volumes/skillwave/landing/raw/';

# COMMAND ----------

# DBTITLE 1,3.5 The same with dbutils.fs
for f in dbutils.fs.ls(LANDING):
    print(f"{f.name:<20} {'(folder)' if f.name.endswith('/') else f.size}")
print("\nenrollment files landed so far:", landed_days())

# COMMAND ----------

# MAGIC %md
# MAGIC 🖱️ **Optional — the upload UI.** In Catalog Explorer open `skillwave → landing → raw` → **Upload to this volume** lets you
# MAGIC drop files from your laptop into a volume (≤ 5 GB per file). That's the "manual upload" ingestion option of the exam.
# MAGIC (`_staging/` holds the enrollment files that will "arrive" later — don't touch it.)
# MAGIC
# MAGIC ## Part 4 · Query files directly
# MAGIC Before building tables, an engineer **looks at the files**. Databricks SQL can query a file or folder directly with
# MAGIC `format.`path``, or with the `read_files()` table-valued function.

# COMMAND ----------

# DBTITLE 1,4.1 CSV the quick way - what goes wrong?
# MAGIC %sql
# MAGIC SELECT * FROM csv.`/Volumes/skillwave/landing/raw/courses/` LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC The columns are called `_c0, _c1, …` and the **header line is a data row**: the `csv.` shortcut can't take options.
# MAGIC Non-self-describing formats (CSV, text) need options → use `read_files()`:

# COMMAND ----------

# DBTITLE 1,4.2 CSV with read_files() and options
# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM read_files('/Volumes/skillwave/landing/raw/courses/',
# MAGIC                 format => 'csv',
# MAGIC                 header => true)
# MAGIC ORDER BY course_id;

# COMMAND ----------

# MAGIC %md
# MAGIC ✅ Real column names and **inferred types** (`list_price` becomes a DOUBLE, `duration_hours` an INT — you'll still cast
# MAGIC explicitly in silver, never trust inference blindly). Spot the data problems you
# MAGIC will fix in silver: **C118 has no category** and **C120 appears twice**. `read_files` also adds `_rescued_data` — values
# MAGIC that don't fit the schema land there instead of being lost.

# COMMAND ----------

# DBTITLE 1,4.3 JSON is self-describing - the shortcut works
# MAGIC %sql
# MAGIC SELECT * FROM json.`/Volumes/skillwave/landing/raw/students/` LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC `profile` is a **JSON string** (not a struct) and `interests` an **array**. You can already reach inside the string with
# MAGIC the `:` path syntax:

# COMMAND ----------

# DBTITLE 1,4.4 Inside a JSON string - the colon syntax
# MAGIC %sql
# MAGIC SELECT student_id,
# MAGIC        profile:first_name           AS first_name,
# MAGIC        profile:country              AS country,
# MAGIC        profile:birth_year::INT      AS birth_year,
# MAGIC        interests,
# MAGIC        size(interests)              AS n_interests,
# MAGIC        email
# MAGIC FROM json.`/Volumes/skillwave/landing/raw/students/`
# MAGIC LIMIT 8;

# COMMAND ----------

# DBTITLE 1,4.5 File metadata - which file did each row come from?
# MAGIC %sql
# MAGIC SELECT _metadata.file_name AS file, count(*) AS rows
# MAGIC FROM read_files('/Volumes/skillwave/landing/raw/students/', format => 'json')
# MAGIC GROUP BY ALL
# MAGIC ORDER BY file;

# COMMAND ----------

# MAGIC %md
# MAGIC `_metadata` is a hidden column available for every file source (`file_path`, `file_name`, `file_size`,
# MAGIC `file_modification_time`). 🕰️ The old `input_file_name` function is not supported on Unity Catalog / serverless.
# MAGIC
# MAGIC ### Unstructured data — the syllabus text files

# COMMAND ----------

# DBTITLE 1,4.6 Text files - one row per LINE
# MAGIC %sql
# MAGIC SELECT * FROM text.`/Volumes/skillwave/landing/raw/syllabus/C101.txt`;

# COMMAND ----------

# DBTITLE 1,4.7 Text files - one row per FILE (wholeText)
# MAGIC %sql
# MAGIC SELECT _metadata.file_name AS file, value AS syllabus
# MAGIC FROM read_files('/Volumes/skillwave/landing/raw/syllabus/', format => 'text', wholeText => true)
# MAGIC ORDER BY file
# MAGIC LIMIT 3;

# COMMAND ----------

# DBTITLE 1,4.8 binaryFile - path, size and the raw bytes
# MAGIC %sql
# MAGIC SELECT path, length, modificationTime
# MAGIC FROM binaryFile.`/Volumes/skillwave/landing/raw/syllabus/`
# MAGIC ORDER BY path
# MAGIC LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC `binaryFile` returns `path`, `modificationTime`, `length` and `content` (bytes) — the way to bring images, PDFs or audio
# MAGIC into a table.
# MAGIC
# MAGIC ### Peek at the daily enrollments

# COMMAND ----------

# DBTITLE 1,4.9 The first daily file
# MAGIC %sql
# MAGIC SELECT channel, status, count(*) AS rows, round(sum(price_paid), 2) AS revenue,
# MAGIC        count_if(price_paid < 0) AS negative_prices
# MAGIC FROM json.`/Volumes/skillwave/landing/raw/enrollments/`
# MAGIC GROUP BY ALL
# MAGIC ORDER BY rows DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC 150 rows on day 1 — with **one negative price** already visible. You'll deal with it in silver.
# MAGIC
# MAGIC ## Part 5 · Ownership, grants and metadata
# MAGIC You own everything you created. Let's let **all users** of the account *see* the gold layer (and nothing else) — the
# MAGIC least-privilege pattern: **`USE CATALOG` + `USE SCHEMA` + `SELECT`**.

# COMMAND ----------

# DBTITLE 1,5.1 GRANT read access on gold to everybody
# MAGIC %sql
# MAGIC GRANT USE CATALOG ON CATALOG skillwave TO `account users`;
# MAGIC GRANT USE SCHEMA, SELECT ON SCHEMA skillwave.gold TO `account users`;

# COMMAND ----------

# DBTITLE 1,5.2 SHOW GRANTS
# MAGIC %sql
# MAGIC SHOW GRANTS ON SCHEMA skillwave.gold;

# COMMAND ----------

# MAGIC %md
# MAGIC * `SELECT` granted on the **schema** is **inherited** by every current and **future** table/view in it — you won't need a
# MAGIC   grant per table.
# MAGIC * Without `USE CATALOG` + `USE SCHEMA` the `SELECT` would be useless (you can't "reach" the table).
# MAGIC * Unity Catalog has **no `DENY`** — to stop access you `REVOKE` (and you can only revoke at the level it was granted).

# COMMAND ----------

# DBTITLE 1,5.3 information_schema - metadata as SQL
# MAGIC %sql
# MAGIC SELECT schema_name, schema_owner, comment
# MAGIC FROM skillwave.information_schema.schemata
# MAGIC ORDER BY schema_name;

# COMMAND ----------

# DBTITLE 1,5.4 Volumes in information_schema
# MAGIC %sql
# MAGIC SELECT volume_schema, volume_name, volume_type, comment
# MAGIC FROM skillwave.information_schema.volumes;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 6 · ✅ Checks

# COMMAND ----------

# DBTITLE 1,Checks
_st = foundation_status(verbose=False)
_ok = [
    check("catalog skillwave exists", _st["catalog"]),
    check("schemas landing, bronze, silver, gold exist", all(_st[f"schema {s}"] for s in SCHEMAS14)),
    check("volumes landing.raw and landing.checkpoints exist", _st["volume landing.raw"] and _st["volume landing.checkpoints"]),
    check("source files generated (students, courses, instructors, syllabus, reviews)",
          all(path_exists(f"{LANDING}/{d}") for d in ("students", "courses", "instructors", "syllabus", "reviews"))),
    check("day 1 of the enrollments has landed", 1 in landed_days()),
]
try:
    _g = [{k.lower(): v for k, v in r.asDict().items()} for r in spark.sql(f"SHOW GRANTS ON SCHEMA `{CAT}`.gold").collect()]
    _ok.append(check("account users can USE SCHEMA + SELECT on gold",
                     {"USE SCHEMA", "SELECT"} <= {g["actiontype"] for g in _g if g["principal"] == "account users"}))
except Exception as e:
    print("grants not checked:", _first_line(e))
print("\n🎉 Foundation ready - continue with 14-L2." if all(_ok) else "\nFix the ❌ items above (re-run the cells of that part).")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🧠 What you practised
# MAGIC | Concept | Statement |
# MAGIC |---|---|
# MAGIC | Catalog → schema → object | `CREATE CATALOG` · `CREATE SCHEMA` · `USE CATALOG` · fully qualified names |
# MAGIC | Metadata | `DESCRIBE CATALOG/SCHEMA EXTENDED` · `SHOW SCHEMAS/VOLUMES` · `information_schema` · tags · comments |
# MAGIC | Files | managed volumes · `LIST` · `dbutils.fs.ls` · `/Volumes/…` paths |
# MAGIC | Querying files | `csv.`/`json.`/`text.`/`binaryFile.` shortcuts · `read_files()` with options · `_metadata` · `:` JSON paths |
# MAGIC | Access | ownership · `GRANT USE CATALOG / USE SCHEMA / SELECT` · inheritance · `SHOW GRANTS` |
# MAGIC
# MAGIC ➡️ Next: **14-L2 — Bronze and Silver**.
