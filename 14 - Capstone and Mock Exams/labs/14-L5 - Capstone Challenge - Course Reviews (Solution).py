# Databricks notebook source
# MAGIC %md
# MAGIC # 🏆 Lab 14-L5 · Capstone Challenge — Course Reviews — ✅ SOLUTION
# MAGIC > Try the challenge yourself first! Every TODO is solved below; the checks are unchanged.
# MAGIC
# MAGIC **Time:** ~45 min · **Compute:** Serverless notebook · **Works on Free Edition** · *Do 14-L1 … 14-L3 first (14-L4 optional)*
# MAGIC
# MAGIC The product team launches **course reviews**. One JSON file with **123 rows** is waiting in
# MAGIC `/Volumes/skillwave/landing/raw/reviews/`. Build the feature end-to-end — this time **without step-by-step code**. Use
# MAGIC everything you practised; the cheat sheets (`14-CS1`, `14-CS2`) are allowed. 😉
# MAGIC
# MAGIC | Field | Notes |
# MAGIC |---|---|
# MAGIC | `review_id` | unique id — but the file contains **3 exact duplicate rows** |
# MAGIC | `enrollment_id` | must exist in `silver.enrollments` (2 reviews point to enrollments that don't exist) |
# MAGIC | `rating` | must be an integer **1–5** (3 rows break this: `0`, `6`, `NULL`) |
# MAGIC | `comment`, `reviewed_at` (ISO string), `reviewer_email` (PII!) | |
# MAGIC
# MAGIC | Task | Build | Expected |
# MAGIC |---|---|---|
# MAGIC | 1 | `bronze.reviews` — the raw file + `_source_file` and `_ingested_at` | 123 rows |
# MAGIC | 2 | `silver.reviews` (valid, deduplicated, with `course_id` and `student_id` from the enrollment, `rating` INT, `reviewed_at` TIMESTAMP, clean e-mail) and `silver.reviews_quarantine` (+ `dq_reason` = `invalid_rating` / `unknown_enrollment`) | 115 · 5 |
# MAGIC | 3 | `gold.course_ratings`: per course `course_id, title, category, n_reviews, avg_rating` (2 decimals), `pct_5_star`, `rank_in_category` (best average = 1) — only courses with **≥ 3 reviews** | 17 rows |
# MAGIC | 4 | Mask `silver.reviews.reviewer_email` with the function `gold.mask_email` from 14-L3 | |
# MAGIC | 5–8 | Four exam-style decisions about this feature | |
# MAGIC
# MAGIC Run the **checks** at the end — all must be ✅. The solution is in `14-L5 - Capstone Challenge - Course Reviews (Solution)`.

# COMMAND ----------

# MAGIC %run ./_14_prepare

# COMMAND ----------

# DBTITLE 1,Start clean (safe to re-run)
require_gold()
for _t in ("bronze.reviews", "silver.reviews", "silver.reviews_quarantine", "gold.course_ratings"):
    spark.sql(f"DROP TABLE IF EXISTS {fq(_t)}")
spark.sql("""CREATE FUNCTION IF NOT EXISTS gold.mask_email(email STRING)
             RETURNS STRING
             RETURN CASE WHEN is_account_group_member('skillwave_pii_readers') THEN email
                         ELSE regexp_replace(email, '^(.)[^@]*', '$1***') END""")
spark.sql(f"USE CATALOG `{CAT}`")
print("ready - silver.enrollments:", count("silver.enrollments"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 1 · Bronze
# MAGIC Load the reviews file into `bronze.reviews` with the two metadata columns. Any method is fine (CTAS + `read_files`,
# MAGIC `COPY INTO`, Auto Loader).

# COMMAND ----------

# DBTITLE 1,Task 1 · SOLUTION
# MAGIC %sql
# MAGIC -- One-off file -> CTAS over read_files() is the simplest (COPY INTO would be the choice for recurring files)
# MAGIC CREATE OR REPLACE TABLE bronze.reviews
# MAGIC COMMENT 'Raw course reviews as delivered'
# MAGIC AS
# MAGIC SELECT *,
# MAGIC        _metadata.file_name AS _source_file,
# MAGIC        current_timestamp() AS _ingested_at
# MAGIC FROM read_files('/Volumes/skillwave/landing/raw/reviews/', format => 'json');

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 2 · Silver + quarantine
# MAGIC Rules, in this order: rating NULL or not between 1 and 5 → `invalid_rating`; enrollment not found in
# MAGIC `silver.enrollments` → `unknown_enrollment`. Remove the exact duplicates first. Hint: a `LEFT JOIN` to
# MAGIC `silver.enrollments` gives you `course_id`/`student_id` **and** tells you which enrollments are unknown.

# COMMAND ----------

# DBTITLE 1,Task 2 · SOLUTION
# MAGIC %sql
# MAGIC -- 1) dedup  2) enrich + validate in ONE pass (LEFT JOIN keeps unknown enrollments so we can flag them)
# MAGIC CREATE OR REPLACE TEMP VIEW reviews_checked AS
# MAGIC SELECT r.review_id,
# MAGIC        r.enrollment_id,
# MAGIC        e.course_id,
# MAGIC        e.student_id,
# MAGIC        CAST(r.rating AS INT)                 AS rating,
# MAGIC        r.comment,
# MAGIC        CAST(r.reviewed_at AS TIMESTAMP)      AS reviewed_at,
# MAGIC        lower(trim(r.reviewer_email))         AS reviewer_email,
# MAGIC        CASE WHEN r.rating IS NULL OR r.rating NOT BETWEEN 1 AND 5 THEN 'invalid_rating'
# MAGIC             WHEN e.enrollment_id IS NULL                           THEN 'unknown_enrollment'
# MAGIC        END                                   AS dq_reason
# MAGIC FROM (SELECT DISTINCT review_id, enrollment_id, rating, comment, reviewed_at, reviewer_email
# MAGIC       FROM bronze.reviews) r
# MAGIC LEFT JOIN silver.enrollments e ON r.enrollment_id = e.enrollment_id;
# MAGIC
# MAGIC CREATE OR REPLACE TABLE silver.reviews
# MAGIC COMMENT 'Valid course reviews (one row per review)'
# MAGIC AS SELECT * EXCEPT (dq_reason) FROM reviews_checked WHERE dq_reason IS NULL;
# MAGIC
# MAGIC CREATE OR REPLACE TABLE silver.reviews_quarantine
# MAGIC COMMENT 'Reviews that failed a quality rule'
# MAGIC AS SELECT *, current_timestamp() AS quarantined_at FROM reviews_checked WHERE dq_reason IS NOT NULL;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 3 · Gold
# MAGIC `gold.course_ratings` — join to `gold.dim_course`, aggregate per course, keep courses with at least 3 reviews and rank
# MAGIC them inside their category (rank 1 = highest average rating).

# COMMAND ----------

# DBTITLE 1,Task 3 · SOLUTION
# MAGIC %sql
# MAGIC -- GROUP BY + HAVING first, then the window function ranks the aggregated rows
# MAGIC CREATE OR REPLACE TABLE gold.course_ratings
# MAGIC COMMENT 'Average rating per course (courses with at least 3 reviews)'
# MAGIC AS
# MAGIC SELECT c.course_id,
# MAGIC        c.title,
# MAGIC        c.category,
# MAGIC        count(*)                                              AS n_reviews,
# MAGIC        round(avg(r.rating), 2)                               AS avg_rating,
# MAGIC        round(100 * count_if(r.rating = 5) / count(*), 1)     AS pct_5_star,
# MAGIC        rank() OVER (PARTITION BY c.category ORDER BY avg(r.rating) DESC) AS rank_in_category
# MAGIC FROM silver.reviews r
# MAGIC JOIN gold.dim_course c ON r.course_id = c.course_id
# MAGIC GROUP BY c.course_id, c.title, c.category
# MAGIC HAVING count(*) >= 3;
# MAGIC
# MAGIC SELECT * FROM gold.course_ratings ORDER BY category, rank_in_category;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 4 · Protect the reviewer's e-mail

# COMMAND ----------

# DBTITLE 1,Task 4 · SOLUTION
# MAGIC %sql
# MAGIC ALTER TABLE silver.reviews ALTER COLUMN reviewer_email SET MASK gold.mask_email;
# MAGIC
# MAGIC SELECT review_id, rating, reviewer_email FROM silver.reviews ORDER BY review_id LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tasks 5–8 · Decisions (exam style)
# MAGIC **Task 5.** From next year reviews arrive as **one small file per day** in the same folder; a SQL analyst will schedule a
# MAGIC re-runnable load and must never load a file twice. Simplest correct choice?
# MAGIC A) `CREATE OR REPLACE TABLE … AS SELECT * FROM read_files(…)` every day · B) `COPY INTO bronze.reviews FROM …` ·
# MAGIC C) ``INSERT INTO bronze.reviews SELECT * FROM json.`…` `` · D) A Python `spark.read.json(…).write.mode("append")`
# MAGIC
# MAGIC **Task 6.** `gold.course_ratings` feeds a dashboard opened ~1,000 times a day; reviews change once a day. Which object
# MAGIC avoids recomputing the aggregation on every dashboard load while staying easy to keep fresh?
# MAGIC A) A standard view · B) A temporary view · C) A materialized view refreshed after the daily load · D) A global temp view
# MAGIC
# MAGIC **Task 7.** You move the silver logic to a declarative pipeline. Bad ratings must be **removed** from the table but
# MAGIC **counted** in the pipeline's data-quality metrics. Which definition?
# MAGIC A) `CONSTRAINT valid_rating EXPECT (rating BETWEEN 1 AND 5) ON VIOLATION DROP ROW` ·
# MAGIC B) `CONSTRAINT valid_rating EXPECT (rating BETWEEN 1 AND 5)` · C) `CONSTRAINT valid_rating EXPECT (rating BETWEEN 1 AND 5) ON VIOLATION FAIL UPDATE` ·
# MAGIC D) `WHERE rating BETWEEN 1 AND 5` in the query
# MAGIC
# MAGIC **Task 8.** Marketing analysts (group `marketing`) must query `gold.course_ratings` but **nothing in silver**. Gold
# MAGIC already has no grants for them. Minimal set of privileges?
# MAGIC A) `SELECT` on `gold.course_ratings` only · B) `ALL PRIVILEGES` on `skillwave.gold` · C) `USE SCHEMA` + `SELECT` on
# MAGIC `skillwave.silver` and `skillwave.gold` · D) `USE CATALOG` on `skillwave`, `USE SCHEMA` on `skillwave.gold`, `SELECT` on
# MAGIC `gold.course_ratings`

# COMMAND ----------

# DBTITLE 1,Your answers
answer_task5 = "B"   # COPY INTO: idempotent, re-runnable, SQL, fine for thousands of files
answer_task6 = "C"   # materialized view: precomputed, refreshed after the load
answer_task7 = "A"   # EXPECT ... ON VIOLATION DROP ROW: dropped AND counted in the metrics
answer_task8 = "D"   # USE CATALOG + USE SCHEMA + SELECT - least privilege

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Checks

# COMMAND ----------

# DBTITLE 1,Checks
def _q(sql_text):
    return spark.sql(sql_text).first()


_ok = []
_ok.append(check("Task 1 · bronze.reviews has 123 rows with _source_file and _ingested_at",
                 table_exists("bronze.reviews") and count("bronze.reviews") == 123
                 and {"_source_file", "_ingested_at"} <= set(spark.table(fq("bronze.reviews")).columns)))
if table_exists("silver.reviews") and table_exists("silver.reviews_quarantine"):
    _cols = set(spark.table(fq("silver.reviews")).columns)
    _ok.append(check("Task 2 · silver.reviews: 115 unique reviews with course_id and student_id",
                     _q(f"SELECT count(*), count(DISTINCT review_id) FROM {fq('silver.reviews')}")[:] == (115, 115)
                     and {"course_id", "student_id", "rating", "reviewed_at", "reviewer_email"} <= _cols))
    _ok.append(check("Task 2 · ratings are INT 1-5 and reviewed_at is a TIMESTAMP",
                     dict(spark.table(fq("silver.reviews")).dtypes).get("rating") == "int"
                     and dict(spark.table(fq("silver.reviews")).dtypes).get("reviewed_at") == "timestamp"
                     and _q(f"SELECT count_if(rating NOT BETWEEN 1 AND 5) FROM {fq('silver.reviews')}")[0] == 0))
    _reasons = {r["dq_reason"]: r["count"] for r in
                spark.table(fq("silver.reviews_quarantine")).groupBy("dq_reason").count().collect()}
    _ok.append(check(f"Task 2 · quarantine {_reasons} = 3 invalid_rating + 2 unknown_enrollment",
                     _reasons == {"invalid_rating": 3, "unknown_enrollment": 2}))
else:
    _ok.append(check("Task 2 · silver.reviews and silver.reviews_quarantine exist", False))
if table_exists("gold.course_ratings"):
    _top = {r["category"]: (r["title"], float(r["avg_rating"])) for r in
            spark.sql(f"SELECT * FROM {fq('gold.course_ratings')} WHERE rank_in_category = 1").collect()}
    _ok.append(check("Task 3 · gold.course_ratings has 17 courses (>= 3 reviews each)",
                     _q(f"SELECT count(*), min(n_reviews) FROM {fq('gold.course_ratings')}")[:] == (17, 3)))
    _ok.append(check(f"Task 3 · best course per category {_top}",
                     _top == {"AI": ("Building RAG Apps", 5.0), "Business": ("Data Strategy for Leaders", 4.4),
                              "Cloud": ("Cloud Storage Basics", 4.0), "Data Engineering": ("Lakehouse Fundamentals", 4.44),
                              "Data Science": ("Feature Engineering", 4.8)}))
else:
    _ok.append(check("Task 3 · gold.course_ratings exists", False))
try:
    _m = spark.sql(f"SELECT column_name, mask_name FROM `{CAT}`.information_schema.column_masks "
                   "WHERE table_schema = 'silver' AND table_name = 'reviews'").collect()
    _ok.append(check("Task 4 · reviewer_email is masked with gold.mask_email",
                     len(_m) == 1 and _m[0][0] == "reviewer_email" and _m[0][1].endswith("mask_email")))
except Exception as e:
    print("Task 4 not checked:", _first_line(e))
_answers = {"5": answer_task5, "6": answer_task6, "7": answer_task7, "8": answer_task8}
_key = {"5": "B", "6": "C", "7": "A", "8": "D"}
for _n in _answers:
    _ok.append(check(f"Task {_n} · answer {_answers[_n]}", (_answers[_n] or "").strip().upper() == _key[_n]))
print(f"\n{sum(_ok)} / {len(_ok)} checks passed" + (" - 🏆 Challenge complete!" if all(_ok) else ""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 💬 Why those answers?
# MAGIC | Task | Answer | Why the others are wrong |
# MAGIC |---|---|---|
# MAGIC | 5 | **B** `COPY INTO` | A reloads everything every day (slow, and a "replace" loses history); C and D load every file **again** on each run → duplicates. COPY INTO remembers loaded files (idempotent). Auto Loader would also be right, but it's Python/streaming — the analyst wants SQL. |
# MAGIC | 6 | **C** materialized view | A standard view recomputes on every read; temp/global temp views live only in a session and can't serve a dashboard. |
# MAGIC | 7 | **A** `ON VIOLATION DROP ROW` | B only **warns** (row kept, counted); C **fails** the update; D silently filters — nothing appears in the data-quality metrics. |
# MAGIC | 8 | **D** `USE CATALOG` + `USE SCHEMA` + `SELECT` | A: without `USE CATALOG`/`USE SCHEMA` the table can't be reached; B grants far too much (and modify rights); C exposes silver. |
# MAGIC
# MAGIC **Also worth noticing**
# MAGIC * The 3 duplicates were removed **before** validation — otherwise a duplicated bad row would be quarantined twice.
# MAGIC * `rating NOT BETWEEN 1 AND 5` is **NULL** when `rating` is NULL, so the `IS NULL` test must come first (three-valued logic).
# MAGIC * `HAVING` filters groups; the window function runs **after** it — so the ranking only compares courses with ≥ 3 reviews.
# MAGIC * The mask applies to `silver.reviews` for **everyone** (owners too) — `reviews_checked` was a **temp view**, gone with the session.
