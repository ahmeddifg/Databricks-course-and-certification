# Databricks notebook source
# MAGIC %md
# MAGIC # 🥉🥈 Lab 14-L2 · Bronze and Silver Layers (Guided)
# MAGIC **Capstone part 2 of 4** · **Time:** ~60 min · **Compute:** Serverless notebook · **Works on Free Edition**
# MAGIC
# MAGIC You ingest every SkillWave source into **bronze** with the right tool for each, then clean it into **silver**.
# MAGIC
# MAGIC | Part | You will… | Tool | Exam objective |
# MAGIC |---|---|---|---|
# MAGIC | 1 | Bronze for the one-off files | `COPY INTO` (idempotent) · CTAS + `read_files()` · text files | D2 |
# MAGIC | 2 | Bronze for the **daily** enrollment files | **Auto Loader** (`cloudFiles`, `availableNow`) | D2 incremental |
# MAGIC | 3 | A new column appears on day 4 | Auto Loader **schema evolution**, `_rescued_data` | D2 |
# MAGIC | 4 | Silver students & courses | JSON paths, cleaning, dedup, **MERGE (SCD 1)**, constraints | D3 |
# MAGIC | 5 | Silver enrollments + quarantine | PySpark: `dropDuplicates`, joins, `when`, **insert-only MERGE** | D3 quality |
# MAGIC | 6 | Oops! | `DESCRIBE HISTORY`, time travel, `RESTORE` | D1 Delta |
# MAGIC | 7 | ✅ Checks | | |
# MAGIC
# MAGIC ```
# MAGIC landing/raw/instructors  ──COPY INTO──────────▶ bronze.instructors      ──▶ silver.instructors
# MAGIC landing/raw/courses      ──CTAS read_files────▶ bronze.courses          ──▶ silver.courses      (dedup, default category, CHECK)
# MAGIC landing/raw/students     ──CTAS read_files────▶ bronze.students         ──▶ silver.students     (parse JSON, clean e-mail, dedup)
# MAGIC landing/raw/student-upd. ──CTAS read_files────▶ bronze.student_updates  ──MERGE (SCD 1)──▶ silver.students
# MAGIC landing/raw/syllabus     ──read_files text────▶ bronze.course_syllabus
# MAGIC landing/raw/enrollments  ──Auto Loader────────▶ bronze.enrollments      ──▶ silver.enrollments  + silver.enrollments_quarantine
# MAGIC    (one file per day)
# MAGIC ```

# COMMAND ----------

# MAGIC %run ./_14_prepare

# COMMAND ----------

# DBTITLE 1,Start clean (safe to re-run) - needs the foundation of 14-L1
require_foundation()                                   # catch-up: creates catalog/schemas/volumes/data if 14-L1 was skipped
stop_all_streams()
for _t in ("bronze.instructors", "bronze.courses", "bronze.students", "bronze.student_updates", "bronze.course_syllabus",
           "bronze.enrollments", "silver.students", "silver.courses", "silver.instructors", "silver.enrollments",
           "silver.enrollments_quarantine"):
    spark.sql(f"DROP TABLE IF EXISTS {fq(_t)}")
dbutils.fs.rm(f"{CHECKPOINTS}/bronze_enrollments", True)     # forget which files the stream has seen
reset_enrollments_landing()                                   # only day 1 has "arrived"
spark.sql(f"USE CATALOG `{CAT}`")


def expect_failure(sql_text: str):
    """Run a statement that SHOULD fail and show the error's first line."""
    try:
        spark.sql(sql_text)
        print("⚠️ no error - unexpected!")
    except Exception as e:
        print("❌ (expected) " + _first_line(e))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1 · Bronze for the one-off files
# MAGIC
# MAGIC ### 1a · Instructors with `COPY INTO`
# MAGIC `COPY INTO` loads files from a path into a Delta table and **remembers which files it already loaded** → re-running it
# MAGIC loads nothing new (idempotent). Perfect for files that arrive *occasionally*, in the thousands rather than millions.
# MAGIC
# MAGIC A table created **without columns** lets `COPY INTO` set the schema from the files (`mergeSchema` in both option groups).

# COMMAND ----------

# DBTITLE 1,1.1 Schemaless target + COPY INTO
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS bronze.instructors;
# MAGIC
# MAGIC COPY INTO bronze.instructors
# MAGIC FROM '/Volumes/skillwave/landing/raw/instructors/'
# MAGIC FILEFORMAT = CSV
# MAGIC FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'true', 'mergeSchema' = 'true')
# MAGIC COPY_OPTIONS ('mergeSchema' = 'true');

# COMMAND ----------

# MAGIC %md
# MAGIC The result shows `num_affected_rows = 10`, `num_inserted_rows = 10`. 🧪 **Run the cell above again** — now 0 rows: the file
# MAGIC was already loaded. (`COPY_OPTIONS ('force' = 'true')` would reload it.)

# COMMAND ----------

# DBTITLE 1,1.2 The table
# MAGIC %sql
# MAGIC SELECT * FROM bronze.instructors ORDER BY instructor_id;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1b · Courses, students and student updates with CTAS + `read_files()`
# MAGIC For a one-time snapshot, **CTAS** (`CREATE TABLE … AS SELECT`) over `read_files()` is the simplest. We add two
# MAGIC **ingestion metadata** columns — standard practice in bronze:

# COMMAND ----------

# DBTITLE 1,1.3 bronze.courses (CSV)
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE bronze.courses
# MAGIC COMMENT 'Raw course catalogue (CSV) as delivered'
# MAGIC AS
# MAGIC SELECT *,
# MAGIC        _metadata.file_name   AS _source_file,
# MAGIC        current_timestamp()   AS _ingested_at
# MAGIC FROM read_files('/Volumes/skillwave/landing/raw/courses/', format => 'csv', header => true);

# COMMAND ----------

# DBTITLE 1,1.4 bronze.students and bronze.student_updates (JSON)
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE bronze.students
# MAGIC COMMENT 'Raw students (JSON lines); profile is a JSON string'
# MAGIC AS
# MAGIC SELECT *, _metadata.file_name AS _source_file, current_timestamp() AS _ingested_at
# MAGIC FROM read_files('/Volumes/skillwave/landing/raw/students/', format => 'json');
# MAGIC
# MAGIC CREATE OR REPLACE TABLE bronze.student_updates
# MAGIC COMMENT 'Raw changes to students (plan upgrades, moves, new students)'
# MAGIC AS
# MAGIC SELECT *, _metadata.file_name AS _source_file, current_timestamp() AS _ingested_at
# MAGIC FROM read_files('/Volumes/skillwave/landing/raw/student-updates/', format => 'json');

# COMMAND ----------

# DBTITLE 1,1.5 bronze.course_syllabus (unstructured text, one row per file)
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE bronze.course_syllabus
# MAGIC COMMENT 'Syllabus text of every course - unstructured data in a table'
# MAGIC AS
# MAGIC SELECT replace(_metadata.file_name, '.txt', '') AS course_id,
# MAGIC        value                                    AS syllabus_text,
# MAGIC        _metadata.file_size                      AS file_size,
# MAGIC        current_timestamp()                      AS _ingested_at
# MAGIC FROM read_files('/Volumes/skillwave/landing/raw/syllabus/', format => 'text', wholeText => true);

# COMMAND ----------

# DBTITLE 1,1.6 Row counts so far
# MAGIC %sql
# MAGIC SELECT 'instructors' AS bronze_table, count(*) AS rows FROM bronze.instructors UNION ALL
# MAGIC SELECT 'courses',         count(*) FROM bronze.courses         UNION ALL
# MAGIC SELECT 'students',        count(*) FROM bronze.students        UNION ALL
# MAGIC SELECT 'student_updates', count(*) FROM bronze.student_updates UNION ALL
# MAGIC SELECT 'course_syllabus', count(*) FROM bronze.course_syllabus;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected: **10 · 21 · 403 · 25 · 20**. Bronze keeps the data **as delivered** — including the duplicate course and the
# MAGIC 3 duplicate students.
# MAGIC
# MAGIC ## Part 2 · The daily enrollments with Auto Loader
# MAGIC Enrollments arrive **every day**, forever. Re-reading the whole folder each day would get slower and slower. **Auto
# MAGIC Loader** (`cloudFiles`) is a Structured Streaming source that discovers **only new files**, remembers them in the
# MAGIC **checkpoint**, infers and **evolves** the schema. With `trigger(availableNow=True)` it processes everything new and
# MAGIC then **stops** — incremental *batch* processing, ideal for a scheduled job.

# COMMAND ----------

# DBTITLE 1,2.1 The bronze stream (read the comments!)
def ingest_enrollments():
    query = (spark.readStream
             .format("cloudFiles")                                          # Auto Loader
             .option("cloudFiles.format", "json")                           # format of the files
             .option("cloudFiles.schemaLocation",                           # where the inferred schema is tracked
                     f"{CHECKPOINTS}/bronze_enrollments/_schema")
             .option("cloudFiles.inferColumnTypes", "true")                 # JSON numbers -> numbers, not strings
             .load(ENROLL_LANDING)                                          # the folder that receives the daily files
             .select("*",
                     F.col("_metadata.file_name").alias("_source_file"),
                     F.current_timestamp().alias("_ingested_at"))
             .writeStream
             .option("checkpointLocation", f"{CHECKPOINTS}/bronze_enrollments")   # progress: which files are done
             .option("mergeSchema", "true")                                 # let new columns reach the table
             .trigger(availableNow=True)                                    # process what's there, then stop
             .toTable(fq("bronze.enrollments")))
    query.awaitTermination()                                                # wait until it stops
    print("bronze.enrollments rows:", spark.table(fq("bronze.enrollments")).count())


ingest_enrollments()

# COMMAND ----------

# MAGIC %md
# MAGIC **150 rows** — day 1. Now two more days "arrive". Run the **same** stream again:

# COMMAND ----------

# DBTITLE 1,2.2 Days 2 and 3 arrive - incremental processing
land_day(2)
ingest_enrollments()

# COMMAND ----------

# MAGIC %md
# MAGIC **450** — only the 2 new files were read (300 rows); day 1 was skipped because the checkpoint knows it.

# COMMAND ----------

# DBTITLE 1,2.3 Which files went into bronze? (and which batch)
# MAGIC %sql
# MAGIC SELECT _source_file, count(*) AS rows, min(_ingested_at) AS ingested_at
# MAGIC FROM bronze.enrollments
# MAGIC GROUP BY ALL
# MAGIC ORDER BY _source_file;

# COMMAND ----------

# DBTITLE 1,2.4 What does Auto Loader remember? cloud_files_state()
try:
    display(spark.sql(f"SELECT path, size, commit_time FROM cloud_files_state('{CHECKPOINTS}/bronze_enrollments')"))
except Exception as e:
    print("cloud_files_state not available here:", _first_line(e))

# COMMAND ----------

# MAGIC %md
# MAGIC > 🎯 **COPY INTO vs Auto Loader** — both are idempotent and incremental. **Auto Loader** scales to millions of files, runs as
# MAGIC > a stream (`availableNow` or continuous), tracks schema in `schemaLocation` and evolves it; **COPY INTO** is a SQL
# MAGIC > command for thousands of files, re-runnable, simplest for occasional loads.
# MAGIC
# MAGIC ## Part 3 · Day 4 brings a new column
# MAGIC The marketing team added a `referrer` field to the enrollment events from 4 September. Nobody told you. 😉

# COMMAND ----------

# DBTITLE 1,3.1 Day 4 arrives - run the stream (it is EXPECTED to stop with an error)
land_day(1)
try:
    ingest_enrollments()
    print("ℹ️ no error this time (the schema had already evolved in an earlier run)")
except Exception as e:
    print("❌ (expected) the stream stopped:", _first_line(e))

# COMMAND ----------

# MAGIC %md
# MAGIC With the default `cloudFiles.schemaEvolutionMode = addNewColumns`, Auto Loader **updates the schema** in the schema
# MAGIC location and **stops the stream** (`UnknownFieldException`) so nothing is silently mis-read. **Restart it** — in production
# MAGIC a job **retry** does this automatically:

# COMMAND ----------

# DBTITLE 1,3.2 Restart the stream - it continues with the new schema
ingest_enrollments()
display(spark.sql("SELECT _source_file, count(*) AS rows, count(referrer) AS with_referrer "
                  "FROM bronze.enrollments GROUP BY ALL ORDER BY 1"))

# COMMAND ----------

# MAGIC %md
# MAGIC **600 rows**, and `referrer` is filled only for day 4 (older rows have `NULL`). Schema evolution modes to know:
# MAGIC
# MAGIC | `cloudFiles.schemaEvolutionMode` | New column in the data… |
# MAGIC |---|---|
# MAGIC | `addNewColumns` *(default)* | stream fails once, schema updated, restart continues with the column |
# MAGIC | `rescue` | schema never changes; unexpected values go to the `_rescued_data` column |
# MAGIC | `failOnNewColumns` | stream fails until you change the schema yourself |
# MAGIC | `none` | new columns ignored (default when you **provide** a schema) |

# COMMAND ----------

# DBTITLE 1,3.3 The bronze schema - note referrer and _rescued_data
# MAGIC %sql
# MAGIC DESCRIBE TABLE bronze.enrollments;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 4 · Silver students and courses (SQL)
# MAGIC
# MAGIC ### 4a · Students: parse, clean, deduplicate
# MAGIC | Problem in bronze | Fix in silver |
# MAGIC |---|---|
# MAGIC | `profile` is a JSON **string** | extract fields with `profile:field::TYPE` |
# MAGIC | e-mails like `"  JOHN.KHAN1@COMPANY.COM "` | `lower(trim(email))` |
# MAGIC | 3 exact duplicate rows | `SELECT DISTINCT` |
# MAGIC | `signup_ts` is a string | `CAST(… AS TIMESTAMP)` |

# COMMAND ----------

# DBTITLE 1,4.1 silver.students
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE silver.students
# MAGIC COMMENT 'SkillWave students - cleaned, one row per student (SCD type 1)'
# MAGIC AS
# MAGIC SELECT student_id,
# MAGIC        lower(trim(email))          AS email,
# MAGIC        profile:first_name::STRING  AS first_name,
# MAGIC        profile:last_name::STRING   AS last_name,
# MAGIC        profile:country::STRING     AS country,
# MAGIC        profile:city::STRING        AS city,
# MAGIC        profile:birth_year::INT     AS birth_year,
# MAGIC        plan,
# MAGIC        interests,
# MAGIC        CAST(signup_ts AS TIMESTAMP) AS signup_ts,
# MAGIC        CAST(signup_ts AS TIMESTAMP) AS updated_at
# MAGIC FROM (SELECT DISTINCT student_id, email, profile, plan, interests, signup_ts FROM bronze.students);

# COMMAND ----------

# DBTITLE 1,4.2 Did it work? (400 students, clean e-mails)
# MAGIC %sql
# MAGIC SELECT count(*)                                     AS students,
# MAGIC        count(DISTINCT student_id)                   AS distinct_ids,
# MAGIC        count_if(email IS NULL)                      AS missing_email,
# MAGIC        count_if(email <> lower(trim(email)))        AS messy_email,
# MAGIC        count_if(plan = 'pro')                       AS pro_students
# MAGIC FROM silver.students;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected: **400 · 400 · 17 · 0 · 111**.
# MAGIC
# MAGIC ### 4b · Apply the student updates — MERGE = upsert (SCD type 1)
# MAGIC 20 students changed (plan upgrade or new city) and 5 are new. **SCD type 1** = overwrite the old values, keep no history.
# MAGIC `MERGE` updates the matches and inserts the rest **in one atomic transaction**. The extra condition
# MAGIC `s.updated_at > t.updated_at` makes it safe to re-run (and ignores out-of-date changes).

# COMMAND ----------

# DBTITLE 1,4.3 MERGE INTO silver.students
# MAGIC %sql
# MAGIC MERGE INTO silver.students AS t
# MAGIC USING (
# MAGIC   SELECT student_id,
# MAGIC          lower(trim(email))          AS email,
# MAGIC          profile:first_name::STRING  AS first_name,
# MAGIC          profile:last_name::STRING   AS last_name,
# MAGIC          profile:country::STRING     AS country,
# MAGIC          profile:city::STRING        AS city,
# MAGIC          profile:birth_year::INT     AS birth_year,
# MAGIC          plan,
# MAGIC          interests,
# MAGIC          CAST(signup_ts AS TIMESTAMP)  AS signup_ts,
# MAGIC          CAST(updated_ts AS TIMESTAMP) AS updated_at
# MAGIC   FROM bronze.student_updates
# MAGIC ) AS s
# MAGIC ON t.student_id = s.student_id
# MAGIC WHEN MATCHED AND s.updated_at > t.updated_at THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result: `num_affected_rows 25 · num_updated_rows 20 · num_inserted_rows 5`. 🧪 **Run it again** → 0 rows affected
# MAGIC (no change is newer any more). Now **405 students, 128 on the pro plan**.
# MAGIC
# MAGIC > 💡 **SCD type 2** would *keep* the old row (with `valid_from`/`valid_to`/`is_current`) and insert a new version. In
# MAGIC > declarative pipelines `AUTO CDC … STORED AS SCD TYPE 2` does it for you (Section 08).
# MAGIC
# MAGIC ### 4c · Courses: dedup, default values, constraints

# COMMAND ----------

# DBTITLE 1,4.4 silver.courses
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE silver.courses
# MAGIC COMMENT 'SkillWave courses - typed, deduplicated, category defaulted'
# MAGIC AS
# MAGIC SELECT DISTINCT
# MAGIC        course_id,
# MAGIC        title,
# MAGIC        coalesce(category, 'Uncategorized')    AS category,
# MAGIC        level,
# MAGIC        instructor_id,
# MAGIC        CAST(list_price AS DECIMAL(10,2))      AS list_price,
# MAGIC        CAST(duration_hours AS INT)            AS duration_hours,
# MAGIC        CAST(published_on AS DATE)             AS published_on
# MAGIC FROM bronze.courses;

# COMMAND ----------

# DBTITLE 1,4.5 Protect the table with constraints
# MAGIC %sql
# MAGIC ALTER TABLE silver.courses DROP CONSTRAINT IF EXISTS valid_price;
# MAGIC ALTER TABLE silver.courses ADD CONSTRAINT valid_price CHECK (list_price >= 0);
# MAGIC ALTER TABLE silver.courses ALTER COLUMN course_id SET NOT NULL;

# COMMAND ----------

# DBTITLE 1,4.6 Try to break the rules (both must FAIL)
expect_failure("INSERT INTO silver.courses VALUES ('C900', 'Free money', 'Business', 'Beginner', 'I01', -10, 1, current_date())")
expect_failure("INSERT INTO silver.courses VALUES (NULL, 'No id', 'Business', 'Beginner', 'I01', 10, 1, current_date())")
print("courses:", spark.table(fq("silver.courses")).count())

# COMMAND ----------

# MAGIC %md
# MAGIC A violating write fails **as a whole** (no partial insert) — that's **schema/constraint enforcement** + ACID. 20 courses,
# MAGIC `C118` is now `Uncategorized`.

# COMMAND ----------

# DBTITLE 1,4.7 silver.instructors
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE silver.instructors
# MAGIC COMMENT 'SkillWave instructors'
# MAGIC AS
# MAGIC SELECT instructor_id,
# MAGIC        concat_ws(' ', first_name, last_name)  AS full_name,
# MAGIC        lower(email)                           AS email,
# MAGIC        country,
# MAGIC        CAST(hire_date AS DATE)                AS hire_date,
# MAGIC        specialty
# MAGIC FROM bronze.instructors;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 5 · Silver enrollments with PySpark — validate, quarantine, MERGE
# MAGIC Rules for a **valid** enrollment: unique `enrollment_id`, `price_paid >= 0`, a **known** student and a **known** course.
# MAGIC Bad rows are not thrown away: they go to a **quarantine** table with the reason, so someone can fix them.

# COMMAND ----------

# DBTITLE 1,5.1 Build the valid and the quarantine DataFrames
bronze = spark.table(fq("bronze.enrollments"))
known_students = spark.table(fq("silver.students")).select("student_id", F.lit(True).alias("_known_student"))
known_courses = spark.table(fq("silver.courses")).select("course_id", F.lit(True).alias("_known_course"))

checked = (bronze
    .dropDuplicates(["enrollment_id"])                                   # 1 duplicate per file
    .select("enrollment_id", "student_id", "course_id",
            F.col("enrolled_at").cast("timestamp").alias("enrolled_at"),
            F.to_date(F.col("enrolled_at").cast("timestamp")).alias("enrolled_date"),
            F.col("price_paid").cast("decimal(10,2)").alias("price_paid"),
            F.col("coupon").cast("string").alias("coupon"), "channel", "status",
            F.col("progress_pct").cast("int").alias("progress_pct"),
            F.col("referrer").cast("string").alias("referrer"), "_source_file")
    .join(known_students, "student_id", "left")                          # left join: keep unknown students
    .join(known_courses, "course_id", "left")
    .withColumn("dq_reason",
                F.when(F.col("enrollment_id").isNull(), "missing_id")
                 .when(F.col("price_paid") < 0, "negative_price")
                 .when(F.col("_known_student").isNull(), "unknown_student")
                 .when(F.col("_known_course").isNull(), "unknown_course"))   # NULL = passed every rule
    .drop("_known_student", "_known_course"))

cols = ["enrollment_id", "student_id", "course_id", "enrolled_at", "enrolled_date", "price_paid", "coupon",
        "channel", "status", "progress_pct", "referrer", "_source_file"]
valid_df = checked.where("dq_reason IS NULL").select(*cols)
quarantine_df = checked.where("dq_reason IS NOT NULL").select(*cols, "dq_reason",
                                                               F.current_timestamp().alias("quarantined_at"))
print("valid:", valid_df.count(), "· quarantine:", quarantine_df.count())
display(quarantine_df.groupBy("dq_reason").count())

# COMMAND ----------

# MAGIC %md
# MAGIC Expected: **valid 584 · quarantine 12** (4 negative prices, 4 unknown students `S9999`, 4 unknown courses `C999`).
# MAGIC
# MAGIC Now write them with an **insert-only MERGE**: rows whose `enrollment_id` is already in silver are skipped → running it
# MAGIC again (or loading the same file twice) never creates duplicates. We create the tables first with an explicit schema.

# COMMAND ----------

# DBTITLE 1,5.2 Create the silver tables and MERGE (insert-only)
from delta.tables import DeltaTable

spark.sql(f"""CREATE TABLE IF NOT EXISTS {fq('silver.enrollments')} ({SILVER_ENROLL_DDL})
              COMMENT 'SkillWave enrollments - valid, deduplicated'""")
spark.sql(f"""CREATE TABLE IF NOT EXISTS {fq('silver.enrollments_quarantine')}
              ({SILVER_ENROLL_DDL}, dq_reason STRING, quarantined_at TIMESTAMP)
              COMMENT 'SkillWave enrollments that failed a quality rule'""")


def merge_insert_only(df, table):
    (DeltaTable.forName(spark, fq(table)).alias("t")
        .merge(df.alias("s"), "t.enrollment_id = s.enrollment_id")
        .whenNotMatchedInsertAll()
        .execute())


merge_insert_only(valid_df, "silver.enrollments")
merge_insert_only(quarantine_df, "silver.enrollments_quarantine")
print("silver.enrollments:", spark.table(fq("silver.enrollments")).count(),
      "· quarantine:", spark.table(fq("silver.enrollments_quarantine")).count())

# COMMAND ----------

# MAGIC %md
# MAGIC 🧪 **Run the cell above again.** Still **584 / 12** — the MERGE is idempotent. (`SILVER_ENROLL_DDL` is defined in
# MAGIC `_14_prepare`; print it if you're curious.)
# MAGIC
# MAGIC > 🎯 The same insert-only MERGE in SQL:
# MAGIC > ```sql
# MAGIC > MERGE INTO silver.enrollments t
# MAGIC > USING new_rows s ON t.enrollment_id = s.enrollment_id
# MAGIC > WHEN NOT MATCHED THEN INSERT *
# MAGIC > ```

# COMMAND ----------

# DBTITLE 1,5.3 Silver at a glance
# MAGIC %sql
# MAGIC SELECT enrolled_date,
# MAGIC        count(*)                       AS enrollments,
# MAGIC        round(sum(price_paid), 2)      AS revenue,
# MAGIC        count_if(coupon IS NOT NULL)   AS with_coupon,
# MAGIC        count(referrer)                AS with_referrer
# MAGIC FROM silver.enrollments
# MAGIC GROUP BY enrolled_date
# MAGIC ORDER BY enrolled_date;

# COMMAND ----------

# MAGIC %md
# MAGIC 146 per day, total revenue **42,105.50**.
# MAGIC
# MAGIC ## Part 6 · Oops! — history, time travel and RESTORE
# MAGIC A colleague wanted to remove the *partner* enrollments from a report… and ran the `DELETE` on the silver table. 😱

# COMMAND ----------

# DBTITLE 1,6.1 The accident
# MAGIC %sql
# MAGIC DELETE FROM silver.enrollments WHERE channel = 'partner';

# COMMAND ----------

# DBTITLE 1,6.2 Every change is a version - DESCRIBE HISTORY
# MAGIC %sql
# MAGIC DESCRIBE HISTORY silver.enrollments;

# COMMAND ----------

# DBTITLE 1,6.3 Time travel: compare the current version with the one before the DELETE
_hist = spark.sql(f"DESCRIBE HISTORY {fq('silver.enrollments')}").orderBy(F.desc("version")).collect()
_delete_version = _hist[0]["version"]
_before = _delete_version - 1
print("now                    :", spark.table(fq("silver.enrollments")).count())
print(f"VERSION AS OF {_before:<9}:",
      spark.sql(f"SELECT count(*) FROM {fq('silver.enrollments')} VERSION AS OF {_before}").first()[0])

# COMMAND ----------

# DBTITLE 1,6.4 RESTORE the table to the version before the accident
display(spark.sql(f"RESTORE TABLE {fq('silver.enrollments')} TO VERSION AS OF {_before}"))
print("after RESTORE:", spark.table(fq("silver.enrollments")).count())

# COMMAND ----------

# MAGIC %md
# MAGIC **526 → 584** again. `RESTORE` itself is a new version in the history (nothing is erased). Time travel works as long as
# MAGIC the old data files still exist — `VACUUM` (default retention **7 days**) removes them.

# COMMAND ----------

# DBTITLE 1,6.5 DESCRIBE DETAIL - the table's physical facts
# MAGIC %sql
# MAGIC DESCRIBE DETAIL silver.enrollments;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 7 · ✅ Checks

# COMMAND ----------

# DBTITLE 1,Checks
_bronze = {t: count(f"bronze.{t}") for t in ("instructors", "courses", "students", "student_updates",
                                               "course_syllabus", "enrollments")}
_ok = [
    check(f"bronze row counts {_bronze}",
          _bronze == {"instructors": 10, "courses": 21, "students": 403, "student_updates": 25,
                      "course_syllabus": 20, "enrollments": 600}),
    check("bronze.enrollments has the new column referrer",
          table_exists("bronze.enrollments") and "referrer" in spark.table(fq("bronze.enrollments")).columns),
    check("silver.students: 405 students, 128 pro, no messy e-mails",
          spark.sql(f"SELECT count(*), count_if(plan = 'pro'), count_if(email <> lower(trim(email))) "
                    f"FROM {fq('silver.students')}").first()[:] == (405, 128, 0)),
    check("silver.courses: 20 courses, 1 Uncategorized",
          spark.sql(f"SELECT count(*), count_if(category = 'Uncategorized') FROM {fq('silver.courses')}").first()[:] == (20, 1)),
    check("silver.courses has the CHECK constraint valid_price",
          "delta.constraints.valid_price" in {r["key"] for r in spark.sql(f"SHOW TBLPROPERTIES {fq('silver.courses')}").collect()}),
    check("silver.instructors: 10", count("silver.instructors") == 10),
    check("silver.enrollments: 584 rows, revenue 42,105.50",
          spark.sql(f"SELECT count(*), sum(price_paid) FROM {fq('silver.enrollments')}").first()[:] == (584, 42105.50)),
    check("quarantine: 12 rows (4 per reason)",
          sorted(r["count"] for r in spark.table(fq("silver.enrollments_quarantine")).groupBy("dq_reason").count().collect())
          == [4, 4, 4]),
]
print("\n🎉 Bronze and silver are ready - continue with 14-L3." if all(_ok) else "\nFix the ❌ items (re-run that part).")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🧠 What you practised
# MAGIC | Concept | Where |
# MAGIC |---|---|
# MAGIC | `COPY INTO` (schemaless target, idempotent) vs CTAS `read_files()` vs **Auto Loader** | Parts 1–2 |
# MAGIC | `availableNow` trigger, checkpoint, `schemaLocation`, `cloud_files_state` | Part 2 |
# MAGIC | Schema evolution `addNewColumns` → restart, `mergeSchema`, `_rescued_data` | Part 3 |
# MAGIC | JSON `:` paths, `lower/trim`, `DISTINCT`, casts, `coalesce` defaults | Part 4 |
# MAGIC | `MERGE` upsert (SCD 1) and **insert-only MERGE** for dedup, idempotent pipelines | Parts 4–5 |
# MAGIC | `CHECK` / `NOT NULL` constraints, quarantine pattern with reasons | Parts 4–5 |
# MAGIC | `DESCRIBE HISTORY`, `VERSION AS OF`, `RESTORE`, `DESCRIBE DETAIL` | Part 6 |
# MAGIC
# MAGIC ➡️ Next: **14-L3 — Gold layer, governance and optimization**.
