# Databricks notebook source
# MAGIC %md
# MAGIC # 📗 14-CS1 · SQL Cheat Sheet for the Data Engineer Associate exam (Runnable)
# MAGIC Every SQL statement you need for the exam — **one runnable cell per idea**, each starting with a short description:
# MAGIC
# MAGIC ```
# MAGIC -- WHAT : what the statement does
# MAGIC -- EXAM : what the exam likes to ask about it (traps, defaults, differences)
# MAGIC ```
# MAGIC
# MAGIC **How to use it** · *Run all* once (≈ 5 min on serverless) — every cell works on Free Edition — then keep it open as your
# MAGIC reference. Cells that can't run in a normal notebook (pipeline syntax, legacy features) are shown as 📖 **reference**
# MAGIC blocks in markdown. Everything is created in the schema **`skillwave.playground`** (prefix `cs_`) and can be dropped with
# MAGIC the last cell. Needs only the files of Lab 14-L1 (created automatically if missing).
# MAGIC
# MAGIC | # | Topic | # | Topic |
# MAGIC |---|---|---|---|
# MAGIC | A | Context & Unity Catalog objects | G | Joins, set operations, PIVOT |
# MAGIC | B | Querying files directly | H | Semi-structured: JSON, arrays, HOFs, VARIANT |
# MAGIC | C | Creating tables (managed, constraints, identity, clustering) | I | Views, functions, parameters & variables |
# MAGIC | D | Writing: INSERT, OVERWRITE, UPDATE, DELETE, MERGE, COPY INTO | J | Governance: GRANT, masks, row filters, metadata |
# MAGIC | E | Delta: history, time travel, RESTORE, OPTIMIZE, VACUUM, CLONE | K | Performance: EXPLAIN, statistics, hints |
# MAGIC | F | Transformations: functions, NULLs, aggregates, windows, dedup | L | 📖 Pipelines (SDP) SQL syntax · clean-up |

# COMMAND ----------

# MAGIC %run ./_14_prepare

# COMMAND ----------

# DBTITLE 1,0. Setup - foundation + playground schema + one widget for the parameter examples
require_foundation(verbose=False)
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{CAT}`.playground COMMENT 'Cheat-sheet experiments (safe to drop)'")
spark.sql(f"USE CATALOG `{CAT}`")
spark.sql("USE SCHEMA playground")
dbutils.widgets.text("min_price", "100", "min_price")             # used by the :min_price examples (section I)
dbutils.widgets.text("table_name", "cs_courses", "table_name")
print(f"✅ working in {CAT}.playground")

# COMMAND ----------

# MAGIC %md
# MAGIC ## A · Context and Unity Catalog objects

# COMMAND ----------

# DBTITLE 1,A1. Where am I? current_catalog / current_schema / current_user
# MAGIC %sql
# MAGIC -- WHAT : the session's current catalog and schema (what 1- and 2-part names resolve to) and your identity
# MAGIC -- EXAM : 3-level namespace catalog.schema.object · USE CATALOG / USE SCHEMA change the defaults for the session
# MAGIC SELECT current_catalog() AS catalog, current_schema() AS schema, current_user() AS me, current_version().dbsql_version AS dbsql;

# COMMAND ----------

# DBTITLE 1,A2. USE CATALOG / USE SCHEMA (a.k.a. USE DATABASE)
# MAGIC %sql
# MAGIC -- WHAT : set the defaults · afterwards "cs_courses" means skillwave.playground.cs_courses
# MAGIC -- EXAM : SCHEMA and DATABASE are synonyms · prefer fully-qualified names in jobs, pipelines and shared code
# MAGIC USE CATALOG skillwave;
# MAGIC USE SCHEMA playground;

# COMMAND ----------

# DBTITLE 1,A3. CREATE SCHEMA with comment and properties
# MAGIC %sql
# MAGIC -- WHAT : create a schema (= database) if it does not exist · IF NOT EXISTS makes it idempotent
# MAGIC -- EXAM : MANAGED LOCATION 's3://...' puts the schema's managed tables in your own storage · without it they go to the
# MAGIC --        catalog's (or metastore's) storage. CREATE CATALOG x [MANAGED LOCATION '...'] works the same way one level up.
# MAGIC CREATE SCHEMA IF NOT EXISTS skillwave.playground
# MAGIC COMMENT 'Cheat-sheet experiments (safe to drop)'
# MAGIC WITH DBPROPERTIES ('owner_team' = 'data-eng');

# COMMAND ----------

# DBTITLE 1,A4. SHOW - list objects
# MAGIC %sql
# MAGIC -- WHAT : SHOW CATALOGS | SCHEMAS | TABLES | VIEWS | VOLUMES | FUNCTIONS, with optional IN <parent> and LIKE pattern
# MAGIC -- EXAM : SHOW TABLES shows tables AND views (isTemporary column for temp views)
# MAGIC SHOW SCHEMAS IN skillwave;

# COMMAND ----------

# DBTITLE 1,A5. DESCRIBE ... EXTENDED - metadata of a schema
# MAGIC %sql
# MAGIC -- WHAT : owner, location, comment, properties of a catalog/schema/table/volume/function
# MAGIC -- EXAM : DESCRIBE TABLE EXTENDED shows Type (MANAGED/EXTERNAL), Location, Provider (delta), Owner
# MAGIC DESCRIBE SCHEMA EXTENDED skillwave.playground;

# COMMAND ----------

# DBTITLE 1,A6. Volumes - create, list files
# MAGIC %sql
# MAGIC -- WHAT : a VOLUME stores FILES (any format) governed by Unity Catalog · path /Volumes/<catalog>/<schema>/<volume>/...
# MAGIC -- EXAM : managed volume = UC manages the storage (DROP deletes files) · external volume = LOCATION in an external location
# MAGIC --        (DROP removes metadata only). Volumes replace DBFS mounts (/mnt) and the DBFS root (legacy).
# MAGIC CREATE VOLUME IF NOT EXISTS skillwave.playground.files COMMENT 'scratch files';
# MAGIC LIST '/Volumes/skillwave/landing/raw/';

# COMMAND ----------

# MAGIC %md
# MAGIC ## B · Querying files directly

# COMMAND ----------

# DBTITLE 1,B1. format.`path` shortcuts - self-describing formats
# MAGIC %sql
# MAGIC -- WHAT : query a file, a folder or a glob directly: json.`...`, parquet.`...`, delta.`...`, csv.`...`, text.`...`, binaryFile.`...`
# MAGIC -- EXAM : works well for self-describing formats (JSON, Parquet, Delta) · CSV via csv.`...` gets _c0, _c1... and NO options
# MAGIC SELECT student_id, email, plan FROM json.`/Volumes/skillwave/landing/raw/students/students_01.json` LIMIT 3;

# COMMAND ----------

# DBTITLE 1,B2. read_files() - formats + options + schema hints
# MAGIC %sql
# MAGIC -- WHAT : table-valued function that reads files WITH options · infers the schema · adds _rescued_data for bad values
# MAGIC -- EXAM : needed for non-self-describing formats (CSV with header/delimiter) · schemaHints fixes types of some columns ·
# MAGIC --        the same function works in streaming tables: FROM STREAM read_files(...)
# MAGIC SELECT course_id, title, list_price, typeof(list_price) AS type_of_price
# MAGIC FROM read_files('/Volumes/skillwave/landing/raw/courses/',
# MAGIC                 format => 'csv', header => true, sep => ',',
# MAGIC                 schemaHints => 'list_price DECIMAL(10,2)')
# MAGIC LIMIT 3;

# COMMAND ----------

# DBTITLE 1,B3. The _metadata column - file name, path, size, modification time
# MAGIC %sql
# MAGIC -- WHAT : hidden column of every file-based source · select it explicitly
# MAGIC -- EXAM : replaces input_file_name (not supported on Unity Catalog shared/serverless compute)
# MAGIC SELECT _metadata.file_name, _metadata.file_size, _metadata.file_modification_time, count(*) AS rows
# MAGIC FROM read_files('/Volumes/skillwave/landing/raw/students/', format => 'json')
# MAGIC GROUP BY ALL;

# COMMAND ----------

# DBTITLE 1,B4. Text and binary files (unstructured data)
# MAGIC %sql
# MAGIC -- WHAT : text = one row per line (or per file with wholeText) · binaryFile = path, modificationTime, length, content (bytes)
# MAGIC -- EXAM : how to ingest images/PDFs/audio into a table: binaryFile (+ the files usually stay in a volume)
# MAGIC SELECT path, length FROM binaryFile.`/Volumes/skillwave/landing/raw/syllabus/` ORDER BY path LIMIT 3;

# COMMAND ----------

# MAGIC %md
# MAGIC 📖 **Reference — older patterns you must recognise**
# MAGIC
# MAGIC ```sql
# MAGIC -- Temp view with options, then CTAS (pre-read_files pattern for CSV)
# MAGIC CREATE OR REPLACE TEMP VIEW courses_csv (course_id STRING, title STRING, list_price DOUBLE)
# MAGIC USING CSV OPTIONS (path = '/Volumes/skillwave/landing/raw/courses/', header = 'true', delimiter = ',');
# MAGIC CREATE TABLE courses AS SELECT * FROM courses_csv;
# MAGIC
# MAGIC -- External NON-Delta table over files (needs an external location; no time travel, no ACID; cache -> REFRESH TABLE)
# MAGIC CREATE TABLE ext_courses (course_id STRING, title STRING) USING CSV
# MAGIC OPTIONS (header = 'true') LOCATION 's3://bucket/courses/';
# MAGIC REFRESH TABLE ext_courses;
# MAGIC
# MAGIC -- JDBC table (legacy) - today use a Unity Catalog connection / Lakehouse Federation
# MAGIC CREATE TABLE pg_users USING JDBC OPTIONS (url 'jdbc:postgresql://host:5432/db', dbtable 'users', user '...', password '...');
# MAGIC ```
# MAGIC
# MAGIC ## C · Creating tables

# COMMAND ----------

# DBTITLE 1,C1. CTAS - CREATE TABLE AS SELECT (schema comes from the query)
# MAGIC %sql
# MAGIC -- WHAT : creates a managed Delta table and fills it in one statement
# MAGIC -- EXAM : CTAS cannot declare column types/constraints manually (the SELECT decides) - cast in the SELECT · supports COMMENT,
# MAGIC --        TBLPROPERTIES, CLUSTER BY / PARTITIONED BY, LOCATION (external)
# MAGIC CREATE OR REPLACE TABLE cs_courses
# MAGIC COMMENT 'Courses from the CSV file'
# MAGIC TBLPROPERTIES ('quality' = 'bronze')
# MAGIC AS
# MAGIC SELECT course_id, title, category, level, instructor_id,
# MAGIC        CAST(list_price AS DECIMAL(10,2)) AS list_price, CAST(duration_hours AS INT) AS duration_hours
# MAGIC FROM read_files('/Volumes/skillwave/landing/raw/courses/', format => 'csv', header => true);

# COMMAND ----------

# DBTITLE 1,C2. CREATE TABLE with an explicit schema: NOT NULL, identity, generated column, liquid clustering
# MAGIC %sql
# MAGIC -- WHAT : the full DDL form - you control types, nullability, comments, keys
# MAGIC -- EXAM : IDENTITY and GENERATED columns can only be defined at CREATE time · CLUSTER BY = liquid clustering
# MAGIC --        (can't be combined with PARTITIONED BY / ZORDER) · Delta is the default format (no USING DELTA needed)
# MAGIC CREATE OR REPLACE TABLE cs_enrollments (
# MAGIC   id            BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC   enrollment_id STRING NOT NULL COMMENT 'business key',
# MAGIC   student_id    STRING,
# MAGIC   course_id     STRING,
# MAGIC   enrolled_at   TIMESTAMP,
# MAGIC   enrolled_date DATE GENERATED ALWAYS AS (CAST(enrolled_at AS DATE)),
# MAGIC   price_paid    DECIMAL(10,2),
# MAGIC   channel       STRING
# MAGIC )
# MAGIC COMMENT 'Enrollments - cheat-sheet copy'
# MAGIC CLUSTER BY (course_id);

# COMMAND ----------

# DBTITLE 1,C3. Fill it - the identity and generated columns are computed for you
# MAGIC %sql
# MAGIC -- WHAT : INSERT ... SELECT with a column list · omitted identity/generated columns are filled automatically
# MAGIC -- EXAM : you may NOT insert explicit values into a GENERATED ALWAYS AS IDENTITY column
# MAGIC INSERT INTO cs_enrollments (enrollment_id, student_id, course_id, enrolled_at, price_paid, channel)
# MAGIC SELECT enrollment_id, student_id, course_id, CAST(enrolled_at AS TIMESTAMP), CAST(price_paid AS DECIMAL(10,2)), channel
# MAGIC FROM read_files('/Volumes/skillwave/landing/raw/_staging/enrollments/enrollments_2026-09-01.json', format => 'json');
# MAGIC
# MAGIC SELECT id, enrollment_id, enrolled_at, enrolled_date FROM cs_enrollments ORDER BY id LIMIT 3;

# COMMAND ----------

# DBTITLE 1,C4. CHECK constraints - added with ALTER TABLE
# MAGIC %sql
# MAGIC -- WHAT : a rule every existing AND future row must satisfy · violating writes fail as a whole (atomic)
# MAGIC -- EXAM : ADD CONSTRAINT fails if existing rows violate it · NOT NULL via ALTER COLUMN ... SET NOT NULL ·
# MAGIC --        PRIMARY KEY / FOREIGN KEY are informational only (not enforced)
# MAGIC ALTER TABLE cs_courses DROP CONSTRAINT IF EXISTS positive_price;
# MAGIC ALTER TABLE cs_courses ADD CONSTRAINT positive_price CHECK (list_price >= 0);
# MAGIC ALTER TABLE cs_courses ALTER COLUMN course_id SET NOT NULL;
# MAGIC SHOW TBLPROPERTIES cs_courses;

# COMMAND ----------

# DBTITLE 1,C5. A violating INSERT fails - nothing is written
# WHAT : demonstrates constraint enforcement (CHECK and NOT NULL)
# EXAM : the error is "CHECK constraint ... violated"; the whole INSERT is rolled back
for stmt in ("INSERT INTO cs_courses VALUES ('C900', 'Bad', 'X', 'Beginner', 'I01', -1, 1)",
             "INSERT INTO cs_courses VALUES (NULL, 'Bad', 'X', 'Beginner', 'I01', 1, 1)"):
    try:
        spark.sql(stmt)
    except Exception as e:
        print("❌ expected:", _first_line(e)[:150])

# COMMAND ----------

# DBTITLE 1,C6. CREATE OR REPLACE vs IF NOT EXISTS vs plain CREATE
# MAGIC %sql
# MAGIC -- WHAT : OR REPLACE = atomic swap of definition+data (history kept) · IF NOT EXISTS = no-op when present · plain = error if present
# MAGIC -- EXAM : CREATE OR REPLACE TABLE keeps the table's history (time travel to before the replace works) ·
# MAGIC --        DROP + CREATE loses it. CREATE OR REPLACE can change the schema · INSERT OVERWRITE cannot.
# MAGIC CREATE TABLE IF NOT EXISTS cs_courses AS SELECT 1 AS ignored;          -- nothing happens: the table exists
# MAGIC SELECT count(*) AS still_the_courses FROM cs_courses;

# COMMAND ----------

# MAGIC %md
# MAGIC 📖 **Managed vs external tables**
# MAGIC
# MAGIC | | Managed | External |
# MAGIC |---|---|---|
# MAGIC | Create | `CREATE TABLE t (...)` / CTAS | `CREATE TABLE t (...) LOCATION 's3://…/abfss://…'` (path inside an **external location**) |
# MAGIC | Who manages files | Unity Catalog (catalog/schema/metastore storage) | you |
# MAGIC | `DROP TABLE` | data deleted (managed tables can be `UNDROP`-ed for 7 days) | only metadata deleted, files stay |
# MAGIC | Formats | Delta (and Iceberg) | Delta, Parquet, CSV, JSON, … |
# MAGIC | Convert | `ALTER TABLE t SET MANAGED` (external → managed) | managed → external: CTAS / DEEP CLONE with `LOCATION` |
# MAGIC | Features | predictive optimization, liquid `CLUSTER BY AUTO`, faster metadata | no automatic optimization |
# MAGIC
# MAGIC ## D · Writing data

# COMMAND ----------

# DBTITLE 1,D1. INSERT INTO - append rows
# MAGIC %sql
# MAGIC -- WHAT : append the result of VALUES or a SELECT
# MAGIC -- EXAM : INSERT INTO appends -> running it twice creates duplicates (not idempotent) · column order matters unless BY NAME
# MAGIC INSERT INTO cs_courses (course_id, title, category, level, instructor_id, list_price, duration_hours)
# MAGIC VALUES ('C900', 'Cheat Sheet Bootcamp', 'Data Engineering', 'Beginner', 'I01', 10.00, 2);

# COMMAND ----------

# DBTITLE 1,D2. INSERT OVERWRITE - replace all the data, keep the table
# MAGIC %sql
# MAGIC -- WHAT : atomically replaces the rows · the table definition (schema, comments, grants, masks, clustering) stays
# MAGIC -- EXAM : INSERT OVERWRITE fails if the SELECT's schema differs (it can't change the schema) - CREATE OR REPLACE can.
# MAGIC --        Old data stays reachable by time travel.
# MAGIC INSERT OVERWRITE cs_courses
# MAGIC SELECT DISTINCT course_id, title, coalesce(category, 'Uncategorized'), level, instructor_id,
# MAGIC        CAST(list_price AS DECIMAL(10,2)), CAST(duration_hours AS INT)
# MAGIC FROM read_files('/Volumes/skillwave/landing/raw/courses/', format => 'csv', header => true);

# COMMAND ----------

# DBTITLE 1,D3. INSERT ... REPLACE WHERE - overwrite only a slice
# MAGIC %sql
# MAGIC -- WHAT : deletes the rows matching the predicate and inserts the new ones, atomically (selective overwrite)
# MAGIC -- EXAM : handy to re-load one day / one category idempotently · every inserted row must match the predicate
# MAGIC INSERT INTO cs_courses
# MAGIC REPLACE WHERE category = 'AI'
# MAGIC SELECT course_id, title, category, level, instructor_id,
# MAGIC        CAST(list_price AS DECIMAL(10,2)), CAST(duration_hours AS INT)
# MAGIC FROM read_files('/Volumes/skillwave/landing/raw/courses/', format => 'csv', header => true)
# MAGIC WHERE category = 'AI';

# COMMAND ----------

# DBTITLE 1,D4. UPDATE and DELETE
# MAGIC %sql
# MAGIC -- WHAT : row-level changes · Delta rewrites only the affected files (or uses deletion vectors)
# MAGIC -- EXAM : supported on Delta tables, NOT on non-Delta external tables (CSV/JSON/Parquet) or views
# MAGIC UPDATE cs_courses SET list_price = CAST(list_price * 0.9 AS DECIMAL(10,2)) WHERE category = 'AI';
# MAGIC DELETE FROM cs_enrollments WHERE price_paid < 0;

# COMMAND ----------

# DBTITLE 1,D5. MERGE - upsert (update matches, insert the rest)
# MAGIC %sql
# MAGIC -- WHAT : one atomic statement: WHEN MATCHED [AND cond] THEN UPDATE/DELETE, WHEN NOT MATCHED THEN INSERT,
# MAGIC --        WHEN NOT MATCHED BY SOURCE THEN UPDATE/DELETE
# MAGIC -- EXAM : SCD type 1 = MERGE upsert · MERGE fails if several source rows match the same target row (dedup the source first) ·
# MAGIC --        UPDATE SET * / INSERT * need the same column names
# MAGIC MERGE INTO cs_courses AS t
# MAGIC USING (SELECT 'C101' AS course_id, 'Lakehouse Fundamentals (2nd ed.)' AS title, 'Data Engineering' AS category,
# MAGIC               'Beginner' AS level, 'I01' AS instructor_id, CAST(55 AS DECIMAL(10,2)) AS list_price, 7 AS duration_hours
# MAGIC        UNION ALL
# MAGIC        SELECT 'C121', 'Unity Catalog in a Day', 'Cloud', 'Beginner', 'I04', CAST(25 AS DECIMAL(10,2)), 3) AS s
# MAGIC ON t.course_id = s.course_id
# MAGIC WHEN MATCHED THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *;

# COMMAND ----------

# DBTITLE 1,D6. Insert-only MERGE - deduplicate on write
# MAGIC %sql
# MAGIC -- WHAT : insert only rows whose key is not yet in the target -> re-loading the same data adds nothing
# MAGIC -- EXAM : the classic "avoid duplicates when re-processing" answer · idempotent
# MAGIC --        (here it inserts just 1 row: the negative-price enrollment that D4 deleted - run it again: 0)
# MAGIC MERGE INTO cs_enrollments AS t
# MAGIC USING (SELECT DISTINCT enrollment_id, student_id, course_id, CAST(enrolled_at AS TIMESTAMP) AS enrolled_at,
# MAGIC               CAST(price_paid AS DECIMAL(10,2)) AS price_paid, channel
# MAGIC        FROM read_files('/Volumes/skillwave/landing/raw/_staging/enrollments/enrollments_2026-09-01.json', format => 'json')) AS s
# MAGIC ON t.enrollment_id = s.enrollment_id
# MAGIC WHEN NOT MATCHED THEN INSERT (enrollment_id, student_id, course_id, enrolled_at, price_paid, channel)
# MAGIC                       VALUES (s.enrollment_id, s.student_id, s.course_id, s.enrolled_at, s.price_paid, s.channel);

# COMMAND ----------

# DBTITLE 1,D7. COPY INTO - idempotent incremental file loading in SQL
# MAGIC %sql
# MAGIC -- WHAT : loads files from a path into a Delta table · already-loaded files are skipped (tracked in the table)
# MAGIC -- EXAM : re-runnable · COPY_OPTIONS ('force' = 'true') reloads · schemaless target + mergeSchema lets it create the schema ·
# MAGIC --        good for thousands of files - for millions / streaming use Auto Loader
# MAGIC CREATE TABLE IF NOT EXISTS cs_instructors;
# MAGIC COPY INTO cs_instructors
# MAGIC FROM '/Volumes/skillwave/landing/raw/instructors/'
# MAGIC FILEFORMAT = CSV
# MAGIC FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'true', 'mergeSchema' = 'true')
# MAGIC COPY_OPTIONS ('mergeSchema' = 'true');

# COMMAND ----------

# MAGIC %md
# MAGIC ## E · Delta Lake features

# COMMAND ----------

# DBTITLE 1,E1. DESCRIBE HISTORY - every commit is a version
# MAGIC %sql
# MAGIC -- WHAT : version, timestamp, user, operation (WRITE, MERGE, UPDATE, OPTIMIZE...), operationMetrics
# MAGIC -- EXAM : the history lives in the _delta_log (JSON commits + checkpoints every N commits)
# MAGIC DESCRIBE HISTORY cs_courses;

# COMMAND ----------

# DBTITLE 1,E2. DESCRIBE DETAIL - physical facts
# MAGIC %sql
# MAGIC -- WHAT : format, location, numFiles, sizeInBytes, partitionColumns, clusteringColumns, properties, minReader/WriterVersion
# MAGIC -- EXAM : the quickest way to see "how many files / how big / clustered by what"
# MAGIC DESCRIBE DETAIL cs_enrollments;

# COMMAND ----------

# DBTITLE 1,E3. Time travel - VERSION AS OF / TIMESTAMP AS OF / @v
# MAGIC %sql
# MAGIC -- WHAT : query an older version of the table
# MAGIC -- EXAM : works only while the old data files exist (VACUUM removes them after the retention, default 7 days)
# MAGIC SELECT 'v0' AS version, count(*) AS rows FROM cs_courses VERSION AS OF 0
# MAGIC UNION ALL
# MAGIC SELECT 'now', count(*) FROM cs_courses;

# COMMAND ----------

# DBTITLE 1,E4. RESTORE - roll the table back (dynamic version number)
# WHAT : make an older version current again; RESTORE is itself a new commit (history is never rewritten)
# EXAM : RESTORE TABLE t TO VERSION AS OF n | TO TIMESTAMP AS OF 'yyyy-MM-dd HH:mm:ss'
#        Here: back to just before the latest UPDATE (D4) -> the UPDATE and the later MERGE (D5) are undone
v_before_update = spark.sql("DESCRIBE HISTORY cs_courses").where("operation = 'UPDATE'").agg(F.max("version")).first()[0] - 1
print("restoring cs_courses to version", v_before_update)
display(spark.sql(f"RESTORE TABLE cs_courses TO VERSION AS OF {v_before_update}"))

# COMMAND ----------

# DBTITLE 1,E5. OPTIMIZE - compact small files (and cluster)
# MAGIC %sql
# MAGIC -- WHAT : rewrites many small files into fewer large ones · on a liquid-clustered table it also clusters the data
# MAGIC -- EXAM : OPTIMIZE t ZORDER BY (col) for non-clustered tables (Z-order is NOT incremental and can't be used with
# MAGIC --        liquid clustering) · predictive optimization can run OPTIMIZE/VACUUM/ANALYZE automatically on UC managed tables
# MAGIC OPTIMIZE cs_enrollments;

# COMMAND ----------

# DBTITLE 1,E6. Liquid clustering - change keys anytime, or let Databricks choose
# MAGIC %sql
# MAGIC -- WHAT : ALTER TABLE ... CLUSTER BY (cols) | CLUSTER BY AUTO | CLUSTER BY NONE
# MAGIC -- EXAM : better than partitioning for most tables (no small-file problem, keys changeable, incremental) ·
# MAGIC --        partitioning only for very large tables with a low-cardinality column (e.g. date), each partition >= 1 GB
# MAGIC ALTER TABLE cs_enrollments CLUSTER BY (course_id, channel);
# MAGIC DESCRIBE DETAIL cs_enrollments;

# COMMAND ----------

# DBTITLE 1,E7. VACUUM - delete files no longer referenced (DRY RUN first)
# MAGIC %sql
# MAGIC -- WHAT : removes data files older than the retention that the current version doesn't use
# MAGIC -- EXAM : default retention 7 days (168 h) · after VACUUM you can't time travel beyond the retention ·
# MAGIC --        RETAIN 0 HOURS needs spark.databricks.delta.retentionDurationCheck.enabled = false (not allowed on serverless)
# MAGIC VACUUM cs_courses RETAIN 168 HOURS DRY RUN;

# COMMAND ----------

# DBTITLE 1,E8. CLONE - deep vs shallow
# MAGIC %sql
# MAGIC -- WHAT : DEEP CLONE copies metadata AND data files (independent copy, incremental re-sync) ·
# MAGIC --        SHALLOW CLONE copies only metadata and points to the source's files (fast, for tests)
# MAGIC -- EXAM : a shallow clone breaks if VACUUM on the source removes files it points to · changes to a clone never affect the source
# MAGIC CREATE OR REPLACE TABLE cs_courses_backup DEEP CLONE cs_courses;
# MAGIC DROP TABLE IF EXISTS cs_courses_test;
# MAGIC DROP TABLE IF EXISTS cs_courses_sandbox;
# MAGIC CREATE TABLE cs_courses_test SHALLOW CLONE cs_courses;
# MAGIC SELECT (SELECT count(*) FROM cs_courses_backup) AS deep_rows, (SELECT count(*) FROM cs_courses_test) AS shallow_rows;

# COMMAND ----------

# DBTITLE 1,E9. Schema changes - ADD COLUMNS, comments, rename table
# MAGIC %sql
# MAGIC -- WHAT : evolve a table's schema explicitly
# MAGIC -- EXAM : renaming/dropping columns needs column mapping ('delta.columnMapping.mode' = 'name') ·
# MAGIC --        on write, .option("mergeSchema", "true") adds new columns automatically (schema evolution)
# MAGIC ALTER TABLE cs_courses_test ADD COLUMNS (language STRING COMMENT 'course language');
# MAGIC ALTER TABLE cs_courses_test ALTER COLUMN title COMMENT 'Course title shown in the app';
# MAGIC COMMENT ON TABLE cs_courses_test IS 'Shallow clone used to test schema changes';
# MAGIC ALTER TABLE cs_courses_test RENAME TO cs_courses_sandbox;

# COMMAND ----------

# DBTITLE 1,E10. DROP TABLE and UNDROP TABLE
# MAGIC %sql
# MAGIC -- WHAT : DROP a managed table and bring it back (Unity Catalog keeps dropped managed tables for 7 days)
# MAGIC -- EXAM : SHOW TABLES DROPPED lists recoverable tables · UNDROP restores the most recent drop of that name
# MAGIC DROP TABLE cs_courses_sandbox;
# MAGIC UNDROP TABLE cs_courses_sandbox;
# MAGIC SHOW TABLES IN playground LIKE 'cs_courses*';

# COMMAND ----------

# MAGIC %md
# MAGIC ## F · Transformations

# COMMAND ----------

# DBTITLE 1,F1. CASE, coalesce, casting (CAST, ::, try_cast)
# MAGIC %sql
# MAGIC -- WHAT : conditional logic, defaults for NULL, type conversion
# MAGIC -- EXAM : with ANSI mode (default on serverless) an invalid CAST raises an error -> try_cast returns NULL instead
# MAGIC SELECT course_id, list_price,
# MAGIC        CASE WHEN list_price < 50 THEN 'budget' WHEN list_price < 120 THEN 'standard' ELSE 'premium' END AS tier,
# MAGIC        coalesce(category, 'Uncategorized')  AS category,
# MAGIC        list_price::INT                      AS price_int,
# MAGIC        try_cast('12.5 EUR' AS DOUBLE)       AS bad_number_becomes_null
# MAGIC FROM cs_courses
# MAGIC ORDER BY list_price DESC
# MAGIC LIMIT 5;

# COMMAND ----------

# DBTITLE 1,F2. String functions
# MAGIC %sql
# MAGIC -- WHAT : the ones that show up in cleaning questions
# MAGIC -- EXAM : regexp_extract/regexp_replace, split + [index] (0-based) / element_at (1-based), concat_ws skips NULLs
# MAGIC SELECT lower(trim('  JOHN.KHAN1@COMPANY.COM '))           AS clean_email,
# MAGIC        split('john.khan1@company.com', '@')[1]            AS domain,
# MAGIC        element_at(split('a-b-c', '-'), 1)                 AS first_part,
# MAGIC        regexp_replace('C-101', '-', '')                   AS no_dash,
# MAGIC        concat_ws(' ', 'Amira', NULL, 'Haddad')            AS full_name,
# MAGIC        initcap('lakehouse fundamentals')                  AS title_case,
# MAGIC        lpad('7', 3, '0')                                  AS padded,
# MAGIC        length('SkillWave')                                AS len;

# COMMAND ----------

# DBTITLE 1,F3. Date and time functions
# MAGIC %sql
# MAGIC -- WHAT : convert, extract, add, truncate, format
# MAGIC -- EXAM : from_unixtime(seconds) -> string · to_timestamp/to_date with format patterns · date_trunc for grouping
# MAGIC SELECT to_timestamp('2026-09-04T17:30:06Z')                        AS ts,
# MAGIC        to_date('04/09/2026', 'dd/MM/yyyy')                         AS d,
# MAGIC        date_format(DATE'2026-09-04', 'EEEE')                       AS day_name,
# MAGIC        date_add(DATE'2026-09-04', 7)                               AS next_week,
# MAGIC        datediff(DATE'2026-09-30', DATE'2026-09-04')                AS days_left,
# MAGIC        date_trunc('MONTH', TIMESTAMP'2026-09-04 17:30:06')         AS month_start,
# MAGIC        from_unixtime(1788000000)                                   AS from_epoch,
# MAGIC        unix_timestamp(TIMESTAMP'2026-09-04 00:00:00')              AS to_epoch,
# MAGIC        current_timestamp()                                         AS now;

# COMMAND ----------

# DBTITLE 1,F4. NULL semantics
# MAGIC %sql
# MAGIC -- WHAT : count(*) counts rows, count(col) skips NULLs · NULL = NULL is NULL (not true) · <=> is NULL-safe equality
# MAGIC -- EXAM : a classic trap in joins (NULL keys never match with =) and in WHERE col <> 'x' (drops NULL rows)
# MAGIC SELECT count(*)                       AS all_rows,
# MAGIC        count(email)                   AS with_email,
# MAGIC        count_if(email IS NULL)        AS missing_email,
# MAGIC        count(DISTINCT plan)           AS plans,
# MAGIC        NULL = NULL                    AS null_eq,
# MAGIC        NULL <=> NULL                  AS null_safe_eq
# MAGIC FROM json.`/Volumes/skillwave/landing/raw/students/`;

# COMMAND ----------

# DBTITLE 1,F5. Aggregates: GROUP BY, HAVING, GROUP BY ALL, FILTER, count_if, approx_count_distinct
# MAGIC %sql
# MAGIC -- WHAT : summarise groups · HAVING filters groups (WHERE filters rows before grouping)
# MAGIC -- EXAM : GROUP BY ALL = group by every non-aggregated column · approx_count_distinct is fast on huge data
# MAGIC SELECT channel,
# MAGIC        count(*)                                         AS enrollments,
# MAGIC        round(sum(price_paid), 2)                        AS revenue,
# MAGIC        round(avg(price_paid), 2)                        AS avg_price,
# MAGIC        count(*) FILTER (WHERE price_paid >= 100)        AS premium,
# MAGIC        count_if(course_id = 'C101')                     AS c101,
# MAGIC        approx_count_distinct(student_id)                AS approx_students
# MAGIC FROM cs_enrollments
# MAGIC GROUP BY ALL
# MAGIC HAVING count(*) > 10
# MAGIC ORDER BY revenue DESC;

# COMMAND ----------

# DBTITLE 1,F6. ROLLUP / CUBE / GROUPING SETS - subtotals
# MAGIC %sql
# MAGIC -- WHAT : several grouping levels in one query · NULL in a grouping column = "all"
# MAGIC -- EXAM : ROLLUP(a, b) = (a,b), (a), () · CUBE(a, b) adds (b) too
# MAGIC SELECT category, level, count(*) AS courses
# MAGIC FROM cs_courses
# MAGIC GROUP BY ROLLUP (category, level)
# MAGIC ORDER BY category NULLS LAST, level NULLS LAST;

# COMMAND ----------

# DBTITLE 1,F7. Window functions - rank, running totals, lag/lead, QUALIFY
# MAGIC %sql
# MAGIC -- WHAT : a value computed over a "window" of related rows WITHOUT collapsing them (unlike GROUP BY)
# MAGIC -- EXAM : row_number (1,2,3), rank (1,1,3), dense_rank (1,1,2) · QUALIFY filters on window results
# MAGIC SELECT category, title, list_price,
# MAGIC        row_number() OVER (PARTITION BY category ORDER BY list_price DESC) AS rn,
# MAGIC        rank()       OVER (PARTITION BY category ORDER BY list_price DESC) AS rnk,
# MAGIC        dense_rank() OVER (PARTITION BY category ORDER BY list_price DESC) AS drnk,
# MAGIC        sum(list_price) OVER (PARTITION BY category ORDER BY list_price DESC
# MAGIC                              ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_total,
# MAGIC        lag(list_price)  OVER (PARTITION BY category ORDER BY list_price DESC) AS previous_price
# MAGIC FROM cs_courses
# MAGIC QUALIFY rn <= 2
# MAGIC ORDER BY category, rn;

# COMMAND ----------

# DBTITLE 1,F8. Deduplication - DISTINCT vs "keep the latest row per key"
# MAGIC %sql
# MAGIC -- WHAT : DISTINCT removes exact duplicates · row_number() keeps one row per key using an ordering
# MAGIC -- EXAM : for "latest record per id" use row_number() OVER (PARTITION BY id ORDER BY ts DESC) = 1
# MAGIC SELECT student_id, plan, updated_ts
# MAGIC FROM json.`/Volumes/skillwave/landing/raw/student-updates/`
# MAGIC QUALIFY row_number() OVER (PARTITION BY student_id ORDER BY updated_ts DESC) = 1
# MAGIC LIMIT 5;

# COMMAND ----------

# DBTITLE 1,F9. CTEs and subqueries
# MAGIC %sql
# MAGIC -- WHAT : WITH name AS (...) makes long queries readable · scalar / IN / EXISTS subqueries
# MAGIC -- EXAM : a CTE exists only for that statement (not a view, nothing stored)
# MAGIC WITH revenue AS (
# MAGIC   SELECT course_id, sum(price_paid) AS revenue FROM cs_enrollments GROUP BY course_id
# MAGIC )
# MAGIC SELECT c.title, r.revenue
# MAGIC FROM revenue r JOIN cs_courses c ON r.course_id = c.course_id
# MAGIC WHERE r.revenue > (SELECT avg(revenue) FROM revenue)
# MAGIC ORDER BY r.revenue DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## G · Joins, set operations, PIVOT

# COMMAND ----------

# DBTITLE 1,G1. Join types side by side
# MAGIC %sql
# MAGIC -- WHAT : INNER (matches) · LEFT/RIGHT (all of one side) · FULL (all of both) · LEFT SEMI (left rows WITH a match, left
# MAGIC --        columns only) · LEFT ANTI (left rows WITHOUT a match) · CROSS (every combination)
# MAGIC -- EXAM : anti join = "find orphans / unknown keys" · semi join = "exists" filter · cross join = rows_a * rows_b
# MAGIC SELECT
# MAGIC   (SELECT count(*) FROM cs_enrollments e JOIN cs_courses c ON e.course_id = c.course_id)            AS inner_join,
# MAGIC   (SELECT count(*) FROM cs_enrollments e LEFT JOIN cs_courses c ON e.course_id = c.course_id)       AS left_join,
# MAGIC   (SELECT count(*) FROM cs_enrollments e LEFT SEMI JOIN cs_courses c ON e.course_id = c.course_id)  AS semi_join,
# MAGIC   (SELECT count(*) FROM cs_enrollments e LEFT ANTI JOIN cs_courses c ON e.course_id = c.course_id)  AS anti_join_orphans,
# MAGIC   (SELECT count(*) FROM cs_courses CROSS JOIN (SELECT explode(array('web', 'mobile')) AS ch))       AS cross_join;

# COMMAND ----------

# DBTITLE 1,G2. Join hints - BROADCAST a small table
# MAGIC %sql
# MAGIC -- WHAT : asks Spark to copy the small table to every executor -> no shuffle of the big table
# MAGIC -- EXAM : automatic below spark.sql.autoBroadcastJoinThreshold (AQE can also switch at runtime) · hint = BROADCAST(alias)
# MAGIC SELECT /*+ BROADCAST(c) */ c.category, count(*) AS enrollments
# MAGIC FROM cs_enrollments e JOIN cs_courses c ON e.course_id = c.course_id
# MAGIC GROUP BY c.category;

# COMMAND ----------

# DBTITLE 1,G3. UNION / UNION ALL / INTERSECT / EXCEPT
# MAGIC %sql
# MAGIC -- WHAT : combine query results by POSITION (same number of columns, compatible types)
# MAGIC -- EXAM : UNION, INTERSECT, EXCEPT (= MINUS) remove duplicates · UNION ALL keeps them
# MAGIC SELECT 'union'     AS op, count(*) AS n FROM (SELECT course_id FROM cs_courses UNION     SELECT course_id FROM cs_enrollments)
# MAGIC UNION ALL
# MAGIC SELECT 'union all',        count(*)     FROM (SELECT course_id FROM cs_courses UNION ALL SELECT course_id FROM cs_enrollments)
# MAGIC UNION ALL
# MAGIC SELECT 'intersect',        count(*)     FROM (SELECT course_id FROM cs_courses INTERSECT SELECT course_id FROM cs_enrollments)
# MAGIC UNION ALL
# MAGIC SELECT 'except',           count(*)     FROM (SELECT course_id FROM cs_courses EXCEPT    SELECT course_id FROM cs_enrollments);

# COMMAND ----------

# DBTITLE 1,G4. PIVOT - rows to columns
# MAGIC %sql
# MAGIC -- WHAT : turn the distinct values of one column into columns with an aggregate
# MAGIC -- EXAM : the IN list must be literal values · PIVOT is NOT supported inside declarative pipelines
# MAGIC SELECT *
# MAGIC FROM (SELECT c.category, e.channel, e.price_paid FROM cs_enrollments e JOIN cs_courses c ON e.course_id = c.course_id)
# MAGIC PIVOT (sum(price_paid) FOR channel IN ('web' AS web, 'mobile' AS mobile, 'partner' AS partner))
# MAGIC ORDER BY category;

# COMMAND ----------

# DBTITLE 1,G5. UNPIVOT - columns to rows
# MAGIC %sql
# MAGIC -- WHAT : the reverse of PIVOT
# MAGIC SELECT * FROM (SELECT 'AI' AS category, 10 AS web, 4 AS mobile, 1 AS partner)
# MAGIC UNPIVOT (enrollments FOR channel IN (web, mobile, partner));

# COMMAND ----------

# MAGIC %md
# MAGIC ## H · Semi-structured data: JSON strings, structs, arrays, higher-order functions, VARIANT

# COMMAND ----------

# DBTITLE 1,H1. JSON string -> fields with the colon syntax
# MAGIC %sql
# MAGIC -- WHAT : col:field:subfield extracts from a JSON STRING · ::TYPE casts the result
# MAGIC -- EXAM : works on STRING (and VARIANT) columns · returns a string -> cast for numbers
# MAGIC SELECT profile:first_name AS first_name, profile:country AS country, profile:birth_year::INT AS birth_year
# MAGIC FROM json.`/Volumes/skillwave/landing/raw/students/students_01.json`
# MAGIC LIMIT 3;

# COMMAND ----------

# DBTITLE 1,H2. JSON string -> STRUCT with from_json + schema_of_json, then struct.* and to_json
# MAGIC %sql
# MAGIC -- WHAT : parse once into a typed struct · access with dot notation or expand all fields with .*
# MAGIC -- EXAM : schema_of_json('<sample json>') derives the schema from one example
# MAGIC WITH parsed AS (
# MAGIC   SELECT student_id,
# MAGIC          from_json(profile, schema_of_json('{"first_name":"x","last_name":"x","country":"x","city":"x","birth_year":1}')) AS p
# MAGIC   FROM json.`/Volumes/skillwave/landing/raw/students/students_01.json`
# MAGIC )
# MAGIC SELECT student_id, p.city, p.*, to_json(p) AS back_to_json FROM parsed LIMIT 3;

# COMMAND ----------

# DBTITLE 1,H3. Arrays: explode, explode_outer, size, array_contains, collect_set/collect_list, flatten, array_distinct
# MAGIC %sql
# MAGIC -- WHAT : explode = one row per element · collect_* = the reverse (aggregate rows into an array)
# MAGIC -- EXAM : explode drops rows with empty/NULL arrays, explode_outer keeps them · collect_set removes duplicates
# MAGIC SELECT interest,
# MAGIC        count(*)                                      AS students,
# MAGIC        size(collect_set(profile:country))            AS countries
# MAGIC FROM (SELECT explode(interests) AS interest, profile
# MAGIC       FROM json.`/Volumes/skillwave/landing/raw/students/`)
# MAGIC GROUP BY interest
# MAGIC ORDER BY students DESC;

# COMMAND ----------

# DBTITLE 1,H4. More array functions
# MAGIC %sql
# MAGIC -- WHAT : array_contains, array_distinct, flatten, sort_array, array_join, element_at, posexplode
# MAGIC SELECT array_contains(array('sql', 'ai'), 'ai')                          AS has_ai,
# MAGIC        array_distinct(array('sql', 'sql', 'ai'))                          AS distinct_items,
# MAGIC        flatten(array(array('sql'), array('ai', 'bi')))                    AS flat,
# MAGIC        sort_array(array('spark', 'ai', 'sql'))                            AS sorted,
# MAGIC        array_join(array('a', 'b', 'c'), '|')                              AS joined,
# MAGIC        element_at(array('x', 'y', 'z'), -1)                               AS last_item;

# COMMAND ----------

# DBTITLE 1,H5. Higher-order functions: transform, filter, exists, aggregate
# MAGIC %sql
# MAGIC -- WHAT : apply a lambda (x -> ...) to every element of an array without exploding it
# MAGIC -- EXAM : TRANSFORM = map, FILTER = keep matching, EXISTS = any match, AGGREGATE = fold/reduce
# MAGIC SELECT interests,
# MAGIC        transform(interests, x -> upper(x))                        AS upper_case,
# MAGIC        filter(interests, x -> x <> 'sql')                          AS without_sql,
# MAGIC        exists(interests, x -> x = 'ai')                            AS likes_ai,
# MAGIC        aggregate(interests, 0, (acc, x) -> acc + length(x))        AS total_chars
# MAGIC FROM json.`/Volumes/skillwave/landing/raw/students/students_01.json`
# MAGIC LIMIT 5;

# COMMAND ----------

# DBTITLE 1,H6. VARIANT - flexible semi-structured type
# MAGIC %sql
# MAGIC -- WHAT : parse_json() stores JSON in an efficient binary VARIANT · query with : paths and :: casts
# MAGIC -- EXAM : better than JSON strings for changing schemas · VARIANT can't be compared/sorted without a cast
# MAGIC SELECT v:profile.city::STRING AS city, v:interests[0]::STRING AS first_interest, schema_of_variant(v) AS schema
# MAGIC FROM (SELECT parse_json('{"profile":{"city":"Riyadh"},"interests":["sql","ai"]}') AS v);

# COMMAND ----------

# MAGIC %md
# MAGIC ## I · Views, functions, parameters and variables

# COMMAND ----------

# DBTITLE 1,I1. Views - stored view and temp view
# MAGIC %sql
# MAGIC -- WHAT : a VIEW stores a query (no data) · TEMP VIEW lives only in this session (not in the catalog)
# MAGIC -- EXAM : stored view = UC object, can be granted · temp view = session-scoped · global temp view = cluster-scoped
# MAGIC --        (global_temp schema, not supported on serverless) · a view is recomputed on every query
# MAGIC CREATE OR REPLACE VIEW cs_v_premium_courses AS
# MAGIC SELECT course_id, title, list_price FROM cs_courses WHERE list_price >= 100;
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW tv_ai_courses AS
# MAGIC SELECT * FROM cs_courses WHERE category = 'AI';
# MAGIC
# MAGIC SHOW VIEWS IN playground;

# COMMAND ----------

# MAGIC %md
# MAGIC 📖 **Materialized view (MV)** — `CREATE [OR REPLACE] MATERIALIZED VIEW gold.mv AS SELECT …` stores the result and is
# MAGIC updated by `REFRESH MATERIALIZED VIEW gold.mv` (or `SCHEDULE EVERY 1 HOUR` / `TRIGGER ON UPDATE`), incrementally when
# MAGIC possible. Runs on serverless pipelines behind the scenes. (Created for real in Lab 14-L3.)

# COMMAND ----------

# DBTITLE 1,I2. SQL UDF - CREATE FUNCTION
# MAGIC %sql
# MAGIC -- WHAT : a reusable SQL expression stored in Unity Catalog (catalog.schema.function)
# MAGIC -- EXAM : SQL UDFs are optimised like built-ins (preferred over Python UDFs) · DESCRIBE FUNCTION EXTENDED shows the body ·
# MAGIC --        callers need EXECUTE on the function
# MAGIC CREATE OR REPLACE FUNCTION cs_discounted(price DECIMAL(10,2), pct DOUBLE)
# MAGIC RETURNS DECIMAL(10,2)
# MAGIC COMMENT 'Price after a percentage discount'
# MAGIC RETURN CAST(price * (1 - pct) AS DECIMAL(10,2));
# MAGIC
# MAGIC SELECT title, list_price, cs_discounted(list_price, 0.2) AS with_summer20 FROM cs_courses LIMIT 3;

# COMMAND ----------

# DBTITLE 1,I3. DESCRIBE FUNCTION EXTENDED / Python UDF in SQL
# MAGIC %sql
# MAGIC -- WHAT : inspect a function · Python UDFs can also be created in SQL (LANGUAGE PYTHON) in Unity Catalog
# MAGIC -- EXAM : Python UDFs serialize rows to Python -> slower · use for logic SQL can't express
# MAGIC DESCRIBE FUNCTION EXTENDED cs_discounted;

# COMMAND ----------

# DBTITLE 1,I4. Named parameter markers (:name) - here filled from a widget
# MAGIC %sql
# MAGIC -- WHAT : :min_price is replaced by a parameter value (widget in notebooks, parameter in the SQL editor/dashboards/jobs)
# MAGIC -- EXAM : parameters avoid SQL injection · legacy notebook syntax was ${min_price} / $min_price
# MAGIC SELECT title, list_price FROM cs_courses
# MAGIC WHERE list_price >= CAST(:min_price AS DECIMAL(10,2))
# MAGIC ORDER BY list_price;

# COMMAND ----------

# DBTITLE 1,I5. IDENTIFIER() - a parameter as a table/column name
# MAGIC %sql
# MAGIC -- WHAT : turns a string (parameter or variable) into an object name safely
# MAGIC -- EXAM : you can't write FROM :table_name directly - wrap it: FROM IDENTIFIER(:table_name)
# MAGIC SELECT count(*) AS rows_in_parameter_table FROM IDENTIFIER(:table_name);

# COMMAND ----------

# DBTITLE 1,I6. Session variables - DECLARE / SET VAR
# MAGIC %sql
# MAGIC -- WHAT : typed variables for the session (SQL scripting light)
# MAGIC DECLARE OR REPLACE VARIABLE min_hours INT DEFAULT 8;
# MAGIC SET VAR min_hours = 10;
# MAGIC SELECT title, duration_hours FROM cs_courses WHERE duration_hours >= min_hours ORDER BY duration_hours DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## J · Governance

# COMMAND ----------

# DBTITLE 1,J1. GRANT, SHOW GRANTS, REVOKE
# MAGIC %sql
# MAGIC -- WHAT : give/remove privileges to users, groups or service principals
# MAGIC -- EXAM : to read a table you need USE CATALOG + USE SCHEMA + SELECT · grants on a catalog/schema are INHERITED by
# MAGIC --        current and future children · Unity Catalog has NO DENY (only hive_metastore table ACLs had DENY) ·
# MAGIC --        MODIFY = insert/update/delete · ALL PRIVILEGES does not include MANAGE
# MAGIC GRANT USE SCHEMA, SELECT ON SCHEMA skillwave.playground TO `account users`;
# MAGIC SHOW GRANTS ON SCHEMA skillwave.playground;

# COMMAND ----------

# DBTITLE 1,J2. Revoke again (and grant on a single object)
# MAGIC %sql
# MAGIC REVOKE USE SCHEMA, SELECT ON SCHEMA skillwave.playground FROM `account users`;
# MAGIC GRANT SELECT ON VIEW cs_v_premium_courses TO `account users`;
# MAGIC SHOW GRANTS `account users` ON VIEW cs_v_premium_courses;

# COMMAND ----------

# MAGIC %md
# MAGIC 📖 More privilege syntax: `GRANT CREATE TABLE, CREATE VOLUME ON SCHEMA s TO g` · `GRANT READ VOLUME, WRITE VOLUME ON VOLUME v TO g` ·
# MAGIC `GRANT EXECUTE ON FUNCTION f TO g` · `GRANT MODIFY ON TABLE t TO g` · `GRANT BROWSE ON CATALOG c TO g` (discover, no data) ·
# MAGIC `ALTER TABLE t OWNER TO \`data_team\`` (owner = full control; prefer a **group** as owner) ·
# MAGIC `GRANT CREATE EXTERNAL TABLE ON EXTERNAL LOCATION loc TO g`.

# COMMAND ----------

# DBTITLE 1,J3. Column mask
# MAGIC %sql
# MAGIC -- WHAT : a SQL UDF (first parameter = the column) attached to a column · every query sees the function's result
# MAGIC -- EXAM : applies to everyone incl. owners (exemptions are written inside the function) · one mask per column
# MAGIC CREATE OR REPLACE FUNCTION cs_mask_price(price DECIMAL(10,2)) RETURNS DECIMAL(10,2)
# MAGIC RETURN CASE WHEN is_account_group_member('finance') THEN price ELSE NULL END;
# MAGIC
# MAGIC ALTER TABLE cs_courses_backup ALTER COLUMN list_price SET MASK cs_mask_price;
# MAGIC SELECT course_id, list_price FROM cs_courses_backup LIMIT 3;

# COMMAND ----------

# DBTITLE 1,J4. Row filter
# MAGIC %sql
# MAGIC -- WHAT : a BOOLEAN SQL UDF attached to the table · rows where it returns FALSE/NULL are invisible
# MAGIC -- EXAM : one row filter per table · ON (cols) passes column values to the function · protected tables can't be
# MAGIC --        time-travelled or cloned by readers subject to the filter
# MAGIC CREATE OR REPLACE FUNCTION cs_only_ai(category STRING) RETURNS BOOLEAN
# MAGIC RETURN is_account_group_member('admins') OR category = 'AI';
# MAGIC
# MAGIC ALTER TABLE cs_courses_backup SET ROW FILTER cs_only_ai ON (category);
# MAGIC SELECT category, count(*) AS visible_courses FROM cs_courses_backup GROUP BY category;

# COMMAND ----------

# DBTITLE 1,J5. Remove them again
# MAGIC %sql
# MAGIC -- WHAT : DROP ROW FILTER / DROP MASK
# MAGIC ALTER TABLE cs_courses_backup DROP ROW FILTER;
# MAGIC ALTER TABLE cs_courses_backup ALTER COLUMN list_price DROP MASK;
# MAGIC SELECT count(*) AS all_visible_again FROM cs_courses_backup;

# COMMAND ----------

# DBTITLE 1,J6. Dynamic view - the older way to filter rows / mask columns
# MAGIC %sql
# MAGIC -- WHAT : a view using current_user() / is_account_group_member() in CASE and WHERE
# MAGIC -- EXAM : protects only people who read THROUGH the view (base-table readers bypass it) · is_member() = legacy workspace groups
# MAGIC CREATE OR REPLACE VIEW cs_v_courses_secure AS
# MAGIC SELECT course_id, title,
# MAGIC        CASE WHEN is_account_group_member('finance') THEN list_price END AS list_price
# MAGIC FROM cs_courses
# MAGIC WHERE is_account_group_member('admins') OR category <> 'Business';
# MAGIC
# MAGIC SELECT count(*) AS visible_rows, count(list_price) AS visible_prices FROM cs_v_courses_secure;

# COMMAND ----------

# MAGIC %md
# MAGIC 📖 **ABAC policy** (attribute-based, needs governed tags; set at catalog/schema level, applies to current & future tables):
# MAGIC ```sql
# MAGIC CREATE POLICY mask_pii ON SCHEMA skillwave.gold
# MAGIC COLUMN MASK gold.mask_email
# MAGIC TO `account users` EXCEPT `skillwave_pii_readers`
# MAGIC FOR TABLES
# MAGIC MATCH COLUMNS has_tag_value('pii', 'email') AS e
# MAGIC ON COLUMN e;
# MAGIC
# MAGIC CREATE POLICY only_my_region ON CATALOG skillwave
# MAGIC ROW FILTER gold.rf_student_country
# MAGIC TO `account users`
# MAGIC FOR TABLES
# MAGIC MATCH COLUMNS has_tag('geo') AS c
# MAGIC USING COLUMNS (c);
# MAGIC
# MAGIC SHOW POLICIES ON SCHEMA skillwave.gold;   DROP POLICY mask_pii ON SCHEMA skillwave.gold;
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,J7. information_schema - metadata with SQL
# MAGIC %sql
# MAGIC -- WHAT : read-only views per catalog: schemata, tables, columns, views, volumes, table_privileges, row_filters, column_masks...
# MAGIC -- EXAM : system.information_schema covers all catalogs · system tables (system.access.audit, system.access.table_lineage,
# MAGIC --        system.lakeflow.*, system.query.history, system.billing.usage) hold audit, lineage, jobs, queries, costs
# MAGIC SELECT table_name, table_type, data_source_format, comment
# MAGIC FROM skillwave.information_schema.tables
# MAGIC WHERE table_schema = 'playground'
# MAGIC ORDER BY table_name;

# COMMAND ----------

# MAGIC %md
# MAGIC ## K · Performance

# COMMAND ----------

# DBTITLE 1,K1. EXPLAIN - the physical plan
# MAGIC %sql
# MAGIC -- WHAT : shows how Spark will run the query: scans (with pushed filters), joins (Broadcast/SortMerge/ShuffledHash), exchanges (= shuffles)
# MAGIC -- EXAM : "Exchange" = shuffle (expensive) · look at the query profile for runtime metrics (time, spill, skew, files pruned)
# MAGIC EXPLAIN FORMATTED
# MAGIC SELECT c.category, sum(e.price_paid) FROM cs_enrollments e JOIN cs_courses c ON e.course_id = c.course_id GROUP BY c.category;

# COMMAND ----------

# DBTITLE 1,K2. ANALYZE TABLE - statistics for the optimizer
# MAGIC %sql
# MAGIC -- WHAT : collects table/column statistics used by the cost-based optimizer (join order, broadcast decisions)
# MAGIC -- EXAM : predictive optimization can run ANALYZE automatically on UC managed tables
# MAGIC ANALYZE TABLE cs_enrollments COMPUTE STATISTICS FOR ALL COLUMNS;
# MAGIC DESCRIBE TABLE EXTENDED cs_enrollments course_id;

# COMMAND ----------

# MAGIC %md
# MAGIC 📖 **Caching** — `CACHE TABLE t` / `UNCACHE TABLE t` (Spark cache, classic compute only) vs the **disk cache** (automatic on
# MAGIC SSD-backed workers, also on SQL warehouses). The **result cache** of SQL warehouses returns identical query results
# MAGIC instantly while the data hasn't changed.
# MAGIC
# MAGIC ## L · 📖 Lakeflow Spark Declarative Pipelines — SQL syntax
# MAGIC These run **inside a pipeline** (source files of a pipeline), not as normal notebook cells:
# MAGIC
# MAGIC ```sql
# MAGIC -- Streaming table: incremental, each input row processed once (append sources)
# MAGIC CREATE OR REFRESH STREAMING TABLE bronze_orders
# MAGIC COMMENT 'raw orders'
# MAGIC AS SELECT *, _metadata.file_path AS source_file
# MAGIC FROM STREAM read_files('${landing_path}/orders', format => 'json');
# MAGIC
# MAGIC -- Expectations: warn (default) / drop / fail
# MAGIC CREATE OR REFRESH STREAMING TABLE silver_orders (
# MAGIC   CONSTRAINT valid_id    EXPECT (order_id IS NOT NULL) ON VIOLATION FAIL UPDATE,
# MAGIC   CONSTRAINT valid_price EXPECT (price >= 0)           ON VIOLATION DROP ROW,
# MAGIC   CONSTRAINT has_email   EXPECT (email IS NOT NULL)                       -- warn only
# MAGIC )
# MAGIC AS SELECT * FROM STREAM(bronze_orders);
# MAGIC
# MAGIC -- Materialized view: recomputed (incrementally when possible) on each update
# MAGIC CREATE OR REFRESH MATERIALIZED VIEW gold_daily AS
# MAGIC SELECT to_date(order_ts) AS day, sum(price) AS revenue FROM silver_orders GROUP BY 1;
# MAGIC
# MAGIC -- CDC (SCD 1 / SCD 2) - replaces APPLY CHANGES INTO
# MAGIC CREATE OR REFRESH STREAMING TABLE customers;
# MAGIC CREATE FLOW customers_cdc AS AUTO CDC INTO customers
# MAGIC FROM STREAM(customers_cdc_feed)
# MAGIC KEYS (customer_id)
# MAGIC APPLY AS DELETE WHEN operation = 'DELETE'
# MAGIC SEQUENCE BY event_ts
# MAGIC COLUMNS * EXCEPT (operation, event_ts)
# MAGIC STORED AS SCD TYPE 2;
# MAGIC
# MAGIC -- Temporary view (only inside the pipeline)
# MAGIC CREATE TEMPORARY VIEW v_valid AS SELECT * FROM silver_orders WHERE price > 0;
# MAGIC
# MAGIC -- Event log (pipeline owner)
# MAGIC SELECT * FROM event_log(TABLE(catalog.schema.silver_orders)) WHERE event_type = 'flow_progress';
# MAGIC ```
# MAGIC 🕰️ Legacy (DLT) syntax to **recognise**: `CREATE OR REFRESH LIVE TABLE`, `STREAMING LIVE TABLE`, `LIVE.table`, `STREAM(LIVE.t)`,
# MAGIC `cloud_files('path', 'json')`, `APPLY CHANGES INTO`.
# MAGIC
# MAGIC ## 🧹 Clean-up

# COMMAND ----------

# DBTITLE 1,Drop the playground (optional)
DROP_PLAYGROUND = False             # set True to remove everything this cheat sheet created
if DROP_PLAYGROUND:
    spark.sql(f"DROP SCHEMA IF EXISTS `{CAT}`.playground CASCADE")
    dbutils.widgets.removeAll()
    print("🧹 playground dropped")
else:
    print("Kept skillwave.playground - set DROP_PLAYGROUND = True and re-run to remove it.")
