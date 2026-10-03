# Databricks notebook source
# MAGIC %md
# MAGIC # 🥇 Lab 14-L3 · Gold Layer, Optimization and Governance (Guided)
# MAGIC **Capstone part 3 of 4** · **Time:** ~55 min · **Compute:** Serverless notebook · **Works on Free Edition**
# MAGIC
# MAGIC Silver is clean — now make it **useful for the business**: a **star schema** that any BI tool understands, a few
# MAGIC ready-made business objects, then make it **fast** and **safe**.
# MAGIC
# MAGIC **The star schema you'll build**
# MAGIC
# MAGIC ```
# MAGIC                       gold.dim_date (30 days of Sep 2026)
# MAGIC                                   │ date_key
# MAGIC  gold.dim_student ── student_id ─ gold.fact_enrollments ─ course_id ── gold.dim_course
# MAGIC  (405, e-mail MASKED)             (584 rows, ROW FILTER     (20 + instructor name)
# MAGIC                                    by student country)
# MAGIC             ▼                               ▼                               ▼
# MAGIC    gold.v_course_performance (view) · gold.mv_daily_category_revenue (materialized view) · gold.course_leaderboard
# MAGIC ```
# MAGIC
# MAGIC | Part | You will… | Exam objective |
# MAGIC |---|---|---|
# MAGIC | 1 | Dimensions and the fact table (CTAS) | D3 gold layer, joins |
# MAGIC | 2 | A view, a **materialized view** and a leaderboard (window functions) | D3 gold objects |
# MAGIC | 3 | Answer business questions: PIVOT, set operations, reconciliation checks | D3 |
# MAGIC | 4 | **Liquid clustering**, OPTIMIZE, query plans | D6 optimization · D3 tuning |
# MAGIC | 5 | **Column mask** and **row filter** | D7 |
# MAGIC | 6 | ✅ Checks | |

# COMMAND ----------

# MAGIC %run ./_14_prepare

# COMMAND ----------

# DBTITLE 1,Start clean (safe to re-run) - needs silver from 14-L2
require_silver()                                      # catch-up: builds bronze + silver if 14-L2 was skipped
for _stmt in ("DROP MATERIALIZED VIEW IF EXISTS gold.mv_daily_category_revenue",
              "DROP VIEW IF EXISTS gold.v_course_performance",
              "DROP TABLE IF EXISTS gold.course_leaderboard",
              "DROP TABLE IF EXISTS gold.fact_enrollments", "DROP TABLE IF EXISTS gold.dim_student",
              "DROP TABLE IF EXISTS gold.dim_course", "DROP TABLE IF EXISTS gold.dim_date",
              "DROP TABLE IF EXISTS silver.region_access",
              "DROP FUNCTION IF EXISTS gold.mask_email", "DROP FUNCTION IF EXISTS gold.rf_student_country"):
    try:
        spark.sql(_stmt)
    except Exception as e:
        print("note:", _first_line(e))
spark.sql(f"USE CATALOG `{CAT}`")
print("silver.enrollments:", count("silver.enrollments"), "rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1 · Dimensions and fact
# MAGIC A **star schema** = one **fact** table (events/measures: one row per enrollment, with `price_paid`, `progress_pct`) surrounded by
# MAGIC **dimension** tables (descriptive attributes to filter and group by: course, student, date). BI tools and humans find it
# MAGIC easy, and joins on small dimensions are cheap (**broadcast**).

# COMMAND ----------

# DBTITLE 1,1.1 gold.dim_course - course + instructor (LEFT JOIN: keep courses without instructor)
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE gold.dim_course
# MAGIC COMMENT 'Course dimension with the instructor denormalised in'
# MAGIC AS
# MAGIC SELECT c.course_id, c.title, c.category, c.level, c.list_price, c.duration_hours,
# MAGIC        c.published_on, i.instructor_id, i.full_name AS instructor_name, i.country AS instructor_country
# MAGIC FROM silver.courses c
# MAGIC LEFT JOIN silver.instructors i ON c.instructor_id = i.instructor_id;

# COMMAND ----------

# DBTITLE 1,1.2 gold.dim_student - derived attributes (CASE)
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE gold.dim_student
# MAGIC COMMENT 'Student dimension (SCD 1) - e-mail is masked for non PII readers'
# MAGIC AS
# MAGIC SELECT student_id,
# MAGIC        concat_ws(' ', first_name, last_name)               AS full_name,
# MAGIC        email, country, city, plan, birth_year,
# MAGIC        CASE WHEN 2026 - birth_year < 25 THEN '18-24'
# MAGIC             WHEN 2026 - birth_year < 35 THEN '25-34'
# MAGIC             WHEN 2026 - birth_year < 45 THEN '35-44'
# MAGIC             ELSE '45+' END                                  AS age_band,
# MAGIC        CAST(signup_ts AS DATE)                              AS signup_date
# MAGIC FROM silver.students;

# COMMAND ----------

# DBTITLE 1,1.3 gold.dim_date - generate rows with sequence() + explode()
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE gold.dim_date
# MAGIC COMMENT 'Calendar of September 2026 (Saudi weekend = Friday + Saturday)'
# MAGIC AS
# MAGIC SELECT d                                                    AS date_key,
# MAGIC        year(d) AS year, month(d) AS month, day(d) AS day,
# MAGIC        date_format(d, 'EEEE')                               AS day_name,
# MAGIC        dayofweek(d) IN (6, 7)                               AS is_weekend_ksa
# MAGIC FROM (SELECT explode(sequence(DATE'2026-09-01', DATE'2026-09-30', INTERVAL 1 DAY)) AS d);

# COMMAND ----------

# DBTITLE 1,1.4 gold.fact_enrollments
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE gold.fact_enrollments
# MAGIC COMMENT 'One row per valid enrollment - the grain of the star schema'
# MAGIC AS
# MAGIC SELECT e.enrollment_id, e.enrolled_date, e.student_id, e.course_id,
# MAGIC        s.country                                            AS student_country,
# MAGIC        e.channel, e.coupon, e.status, e.progress_pct, e.price_paid,
# MAGIC        e.status = 'completed'                               AS is_completed
# MAGIC FROM silver.enrollments e
# MAGIC JOIN silver.students s ON e.student_id = s.student_id;

# COMMAND ----------

# DBTITLE 1,1.5 Sizes
# MAGIC %sql
# MAGIC SELECT 'dim_course' AS gold_table, count(*) AS rows FROM gold.dim_course  UNION ALL
# MAGIC SELECT 'dim_student',              count(*)         FROM gold.dim_student UNION ALL
# MAGIC SELECT 'dim_date',                 count(*)         FROM gold.dim_date    UNION ALL
# MAGIC SELECT 'fact_enrollments',         count(*)         FROM gold.fact_enrollments;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected: **20 · 405 · 30 · 584**. (`student_country` is copied into the fact on purpose — the row filter in Part 5 uses it.)
# MAGIC
# MAGIC ## Part 2 · Business objects: view, materialized view, leaderboard
# MAGIC
# MAGIC | Object | Stores data? | Fresh? | Use it for |
# MAGIC |---|---|---|---|
# MAGIC | **View** | no — the query runs every time | always current | light logic, security layers, small data |
# MAGIC | **Materialized view** | yes — precomputed result | as of the last **refresh** (incremental when possible) | expensive aggregations read often (dashboards) |
# MAGIC | **Table** (CTAS) | yes | as of the last write | results you control fully |
# MAGIC | **Streaming table** | yes | appended incrementally | append-only sources (pipelines) |

# COMMAND ----------

# DBTITLE 1,2.1 VIEW gold.v_course_performance (GROUP BY ALL)
# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW gold.v_course_performance
# MAGIC COMMENT 'One row per course: enrollments, revenue, progress and completion'
# MAGIC AS
# MAGIC SELECT c.course_id, c.title, c.category, c.level, c.instructor_name,
# MAGIC        count(*)                                                     AS enrollments,
# MAGIC        round(sum(f.price_paid), 2)                                  AS revenue,
# MAGIC        round(avg(f.progress_pct), 1)                                AS avg_progress_pct,
# MAGIC        round(100 * avg(CASE WHEN f.is_completed THEN 1 ELSE 0 END), 1) AS completion_rate_pct
# MAGIC FROM gold.fact_enrollments f
# MAGIC JOIN gold.dim_course c ON f.course_id = c.course_id
# MAGIC GROUP BY ALL;

# COMMAND ----------

# DBTITLE 1,2.2 Query the view
# MAGIC %sql
# MAGIC SELECT * FROM gold.v_course_performance ORDER BY revenue DESC LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC Top course: **Delta Lake Deep Dive — 52 enrollments, 4,352.10**.
# MAGIC
# MAGIC Next a **materialized view**. On serverless it is created and refreshed by a small managed **pipeline** behind the
# MAGIC scenes, so this cell can take a minute.

# COMMAND ----------

# DBTITLE 1,2.3 MATERIALIZED VIEW gold.mv_daily_category_revenue
# MAGIC %sql
# MAGIC CREATE OR REPLACE MATERIALIZED VIEW gold.mv_daily_category_revenue
# MAGIC COMMENT 'Daily revenue per course category - refreshed by the job'
# MAGIC AS
# MAGIC SELECT f.enrolled_date, c.category,
# MAGIC        count(*)                     AS enrollments,
# MAGIC        round(sum(f.price_paid), 2)  AS revenue
# MAGIC FROM gold.fact_enrollments f
# MAGIC JOIN gold.dim_course c ON f.course_id = c.course_id
# MAGIC GROUP BY f.enrolled_date, c.category;

# COMMAND ----------

# DBTITLE 1,2.4 Read it like a table
# MAGIC %sql
# MAGIC SELECT category, sum(enrollments) AS enrollments, sum(revenue) AS revenue
# MAGIC FROM gold.mv_daily_category_revenue
# MAGIC GROUP BY category
# MAGIC ORDER BY revenue DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC **Data Engineering 13,606.90** leads, then AI 10,962.40. When new enrollments arrive, the MV does **not** change until
# MAGIC `REFRESH MATERIALIZED VIEW gold.mv_daily_category_revenue` runs (manually, on a `SCHEDULE`, or from a job — 14-L4).

# COMMAND ----------

# DBTITLE 1,2.5 Leaderboard - window functions + QUALIFY
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE gold.course_leaderboard
# MAGIC COMMENT 'Top 3 courses by revenue in each category'
# MAGIC AS
# MAGIC SELECT category, title, enrollments, revenue,
# MAGIC        dense_rank() OVER (PARTITION BY category ORDER BY revenue DESC)        AS rank_in_category,
# MAGIC        round(100 * revenue / sum(revenue) OVER (PARTITION BY category), 1)    AS pct_of_category
# MAGIC FROM gold.v_course_performance
# MAGIC QUALIFY rank_in_category <= 3;
# MAGIC
# MAGIC SELECT * FROM gold.course_leaderboard ORDER BY category, rank_in_category;

# COMMAND ----------

# MAGIC %md
# MAGIC `QUALIFY` filters on a window function's result (like `HAVING` does for aggregates). **16 rows**: 3 per category, but
# MAGIC *Uncategorized* has only one course. `sum(revenue) OVER (PARTITION BY category)` keeps every row and adds the category
# MAGIC total — that's the difference between a **window** and a **GROUP BY**.
# MAGIC
# MAGIC ## Part 3 · Answer business questions

# COMMAND ----------

# DBTITLE 1,3.1 PIVOT - revenue per category and channel
# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM (
# MAGIC   SELECT c.category, f.channel, f.price_paid
# MAGIC   FROM gold.fact_enrollments f
# MAGIC   JOIN gold.dim_course c ON f.course_id = c.course_id
# MAGIC )
# MAGIC PIVOT (round(sum(price_paid), 2) FOR channel IN ('web' AS web, 'mobile' AS mobile, 'partner' AS partner))
# MAGIC ORDER BY category;

# COMMAND ----------

# DBTITLE 1,3.2 Set operations - AI learners who never took a Data Engineering course
# MAGIC %sql
# MAGIC SELECT count(*) AS ai_but_not_de_students
# MAGIC FROM (
# MAGIC   SELECT f.student_id FROM gold.fact_enrollments f JOIN gold.dim_course c ON f.course_id = c.course_id
# MAGIC   WHERE c.category = 'AI'
# MAGIC   EXCEPT
# MAGIC   SELECT f.student_id FROM gold.fact_enrollments f JOIN gold.dim_course c ON f.course_id = c.course_id
# MAGIC   WHERE c.category = 'Data Engineering'
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC `EXCEPT` (= `MINUS`) and `INTERSECT` return **distinct** rows; `UNION` removes duplicates, `UNION ALL` keeps them.

# COMMAND ----------

# DBTITLE 1,3.3 Coupons - which code costs the most? (FILTER clause + conditional aggregates)
# MAGIC %sql
# MAGIC SELECT coalesce(f.coupon, '(none)')                                   AS coupon,
# MAGIC        count(*)                                                       AS enrollments,
# MAGIC        round(sum(f.price_paid), 2)                                    AS revenue,
# MAGIC        round(sum(c.list_price - f.price_paid), 2)                     AS discount_given,
# MAGIC        count(*) FILTER (WHERE f.channel = 'mobile')                   AS via_mobile
# MAGIC FROM gold.fact_enrollments f
# MAGIC JOIN gold.dim_course c ON f.course_id = c.course_id
# MAGIC GROUP BY ALL
# MAGIC ORDER BY discount_given DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reconciliation — trust, but verify
# MAGIC Before publishing gold, check it against silver. These queries must all return **0**:

# COMMAND ----------

# DBTITLE 1,3.4 Data quality checks on gold
# MAGIC %sql
# MAGIC SELECT 'fact rows missing vs silver' AS rule,
# MAGIC        (SELECT count(*) FROM silver.enrollments) - (SELECT count(*) FROM gold.fact_enrollments) AS violations
# MAGIC UNION ALL
# MAGIC SELECT 'orphan course_id (anti join)',
# MAGIC        (SELECT count(*) FROM gold.fact_enrollments f LEFT ANTI JOIN gold.dim_course c ON f.course_id = c.course_id)
# MAGIC UNION ALL
# MAGIC SELECT 'orphan date (anti join)',
# MAGIC        (SELECT count(*) FROM gold.fact_enrollments f LEFT ANTI JOIN gold.dim_date d ON f.enrolled_date = d.date_key)
# MAGIC UNION ALL
# MAGIC SELECT 'duplicate enrollment_id',
# MAGIC        (SELECT count(*) - count(DISTINCT enrollment_id) FROM gold.fact_enrollments)
# MAGIC UNION ALL
# MAGIC SELECT 'negative price',
# MAGIC        (SELECT count_if(price_paid < 0) FROM gold.fact_enrollments);

# COMMAND ----------

# MAGIC %md
# MAGIC `LEFT ANTI JOIN` returns the left rows that have **no** match — the classic "find orphans" join (`LEFT SEMI JOIN` returns
# MAGIC those that **do** match, without adding columns).
# MAGIC
# MAGIC ## Part 4 · Make it fast — liquid clustering and query plans
# MAGIC Dashboards filter the fact table by **date** and **course**. **Liquid clustering** co-locates rows with similar values of
# MAGIC the clustering keys in the same files, so queries skip files (data skipping). Unlike partitioning + Z-order it is
# MAGIC **incremental**, keys can be **changed** any time, and it works well for low and high cardinality.

# COMMAND ----------

# DBTITLE 1,4.1 Turn on liquid clustering and OPTIMIZE
# MAGIC %sql
# MAGIC ALTER TABLE gold.fact_enrollments CLUSTER BY (enrolled_date, course_id);
# MAGIC OPTIMIZE gold.fact_enrollments;

# COMMAND ----------

# DBTITLE 1,4.2 DESCRIBE DETAIL - clusteringColumns, numFiles, sizeInBytes
# MAGIC %sql
# MAGIC DESCRIBE DETAIL gold.fact_enrollments;

# COMMAND ----------

# MAGIC %md
# MAGIC > 💡 `CLUSTER BY AUTO` lets Databricks **choose** (and change) the keys from your query history — with **predictive
# MAGIC > optimization** (on by default for new Unity Catalog managed tables in most accounts) running `OPTIMIZE`/`VACUUM`/`ANALYZE`
# MAGIC > for you. ⚠️ Liquid clustering **can't be combined** with `PARTITIONED BY` or `ZORDER BY` on the same table.

# COMMAND ----------

# DBTITLE 1,4.3 How will Spark join? EXPLAIN
# MAGIC %sql
# MAGIC EXPLAIN FORMATTED
# MAGIC SELECT c.category, sum(f.price_paid) AS revenue
# MAGIC FROM gold.fact_enrollments f
# MAGIC JOIN gold.dim_course c ON f.course_id = c.course_id
# MAGIC GROUP BY c.category;

# COMMAND ----------

# MAGIC %md
# MAGIC Look for **`BroadcastHashJoin`** (with Photon: `PhotonBroadcastHashJoin`): the 20-row `dim_course` is **broadcast** to every
# MAGIC executor, so the big side is never **shuffled**. Spark does that automatically below
# MAGIC `spark.sql.autoBroadcastJoinThreshold` (adaptive query execution also converts joins at runtime).
# MAGIC
# MAGIC 🖱️ **Query profile.** Run the query without `EXPLAIN` in the **SQL editor** (or click *See performance* under a cell's result
# MAGIC on serverless) → **query profile**: time per operator, rows, bytes read, **files pruned**, spill. That's where you diagnose
# MAGIC skew, shuffles and spill — Section 12 / exam domain 6.

# COMMAND ----------

# DBTITLE 1,4.4 Tuning knobs you can read (serverless lets you set only a few)
for k in ("spark.sql.shuffle.partitions", "spark.sql.autoBroadcastJoinThreshold", "spark.sql.adaptive.enabled",
          "spark.sql.ansi.enabled"):
    try:
        print(f"{k:<40} {spark.conf.get(k)}")
    except Exception as e:
        print(f"{k:<40} (not readable here: {_first_line(e)[:60]})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 5 · Make it safe — column mask and row filter
# MAGIC You already granted `SELECT` on the **gold** schema to `account users` (14-L1). Two rules from the compliance team:
# MAGIC
# MAGIC 1. **E-mail addresses** are PII: only members of `skillwave_pii_readers` see them in full.
# MAGIC 2. **Regional managers** see only the enrollments of students from **their** countries (from a mapping table);
# MAGIC    members of `skillwave_admins` see everything.

# COMMAND ----------

# DBTITLE 1,5.1 The mask function and the mask
# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION gold.mask_email(email STRING)
# MAGIC RETURNS STRING
# MAGIC COMMENT 'Shows the full e-mail only to skillwave_pii_readers'
# MAGIC RETURN CASE WHEN is_account_group_member('skillwave_pii_readers') THEN email
# MAGIC             ELSE regexp_replace(email, '^(.)[^@]*', '$1***') END;
# MAGIC
# MAGIC ALTER TABLE gold.dim_student ALTER COLUMN email SET MASK gold.mask_email;

# COMMAND ----------

# DBTITLE 1,5.2 You (the owner!) now see masked e-mails
# MAGIC %sql
# MAGIC SELECT student_id, full_name, email, country FROM gold.dim_student ORDER BY student_id LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC Masks apply to **every** reader — owners included — unless the function says otherwise. Now the row filter, driven by a
# MAGIC **mapping table** kept in **silver** (analysts can't read or change it):

# COMMAND ----------

# DBTITLE 1,5.3 Mapping table + row filter function + attach
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE silver.region_access (user_email STRING, country STRING)
# MAGIC COMMENT 'Which user may see which student country in gold.fact_enrollments';
# MAGIC INSERT INTO silver.region_access VALUES
# MAGIC   ('manager.ksa@skillwave.academy', 'Saudi Arabia'),
# MAGIC   ('manager.egypt@skillwave.academy', 'Egypt');
# MAGIC
# MAGIC CREATE OR REPLACE FUNCTION gold.rf_student_country(p_country STRING)
# MAGIC RETURNS BOOLEAN
# MAGIC COMMENT 'Admins see all rows; others only the countries mapped to them in silver.region_access'
# MAGIC RETURN is_account_group_member('skillwave_admins')
# MAGIC     OR EXISTS (SELECT 1 FROM silver.region_access a
# MAGIC                WHERE a.user_email = current_user() AND a.country = p_country);
# MAGIC
# MAGIC ALTER TABLE gold.fact_enrollments SET ROW FILTER gold.rf_student_country ON (student_country);

# COMMAND ----------

# DBTITLE 1,5.4 What do you see now? Give yourself Saudi Arabia, then Egypt
def visible():
    return spark.table(fq("gold.fact_enrollments")).count()


print("rows visible with no mapping       :", visible())
spark.sql(f"INSERT INTO {fq('silver.region_access')} VALUES (current_user(), 'Saudi Arabia')")
print("rows visible with Saudi Arabia     :", visible())
spark.sql(f"INSERT INTO {fq('silver.region_access')} VALUES (current_user(), 'Egypt')")
print("rows visible with SA + Egypt       :", visible())

# COMMAND ----------

# MAGIC %md
# MAGIC Expected (days 1–4): **0 → 65 → 139**. Same table, same query — the **data** in the mapping table decides. And the view
# MAGIC `gold.v_course_performance` built on top now also shows only your countries: filters travel with the table.

# COMMAND ----------

# DBTITLE 1,5.5 Where are the protections recorded?
# MAGIC %sql
# MAGIC SELECT table_name, filter_name, target_columns FROM skillwave.information_schema.row_filters
# MAGIC UNION ALL
# MAGIC SELECT table_name, mask_name, column_name FROM skillwave.information_schema.column_masks;

# COMMAND ----------

# MAGIC %md
# MAGIC For the rest of the capstone (the job in 14-L4 rebuilds and aggregates the fact table) remove the row filter. The e-mail
# MAGIC mask stays — the job writes `dim_student` with `INSERT OVERWRITE`, which keeps masks, grants and clustering (a
# MAGIC `CREATE OR REPLACE` would rebuild the table definition).

# COMMAND ----------

# DBTITLE 1,5.6 Remove the row filter (keep the mask)
# MAGIC %sql
# MAGIC ALTER TABLE gold.fact_enrollments DROP ROW FILTER;
# MAGIC SELECT count(*) AS visible_again FROM gold.fact_enrollments;

# COMMAND ----------

# MAGIC %md
# MAGIC > 🎯 **Exam map** — *dynamic view* (`CASE WHEN is_account_group_member(...)` inside a view: protects only readers of the view) ·
# MAGIC > *row filter / column mask* (SQL UDF attached to the table: protects every query) · *ABAC* (`CREATE POLICY … ROW FILTER |
# MAGIC > COLUMN MASK … MATCH COLUMNS has_tag_value(...)` on a catalog/schema: one policy for all tagged tables, current and future).
# MAGIC
# MAGIC ## Part 6 · ✅ Checks

# COMMAND ----------

# DBTITLE 1,Checks
_ok = [
    check("dimensions 20 / 405 / 30", (count("gold.dim_course"), count("gold.dim_student"), count("gold.dim_date")) == (20, 405, 30)),
    check("fact_enrollments has as many rows as silver.enrollments",
          count("gold.fact_enrollments") == count("silver.enrollments") > 0),
    check("view gold.v_course_performance: 20 courses",
          table_exists("gold.v_course_performance") and spark.table(fq("gold.v_course_performance")).count() == 20),
    check("leaderboard: 16 rows, max rank 3",
          table_exists("gold.course_leaderboard") and
          spark.sql(f"SELECT count(*), max(rank_in_category) FROM {fq('gold.course_leaderboard')}").first()[:] == (16, 3)),
]
try:
    _ok.append(check("materialized view exists and matches the fact revenue",
                     spark.sql(f"SELECT round(sum(revenue), 2) FROM {fq('gold.mv_daily_category_revenue')}").first()[0]
                     == spark.sql(f"SELECT round(sum(price_paid), 2) FROM {fq('gold.fact_enrollments')}").first()[0]))
except Exception as e:
    print("❌ materialized view:", _first_line(e))
    _ok.append(False)
_detail = spark.sql(f"DESCRIBE DETAIL {fq('gold.fact_enrollments')}").first().asDict()
_ok.append(check(f"fact clustered by {_detail.get('clusteringColumns')}",
                 list(_detail.get("clusteringColumns") or []) == ["enrolled_date", "course_id"]))
try:
    _masks = spark.sql(f"SELECT column_name FROM `{CAT}`.information_schema.column_masks "
                       "WHERE table_schema = 'gold' AND table_name = 'dim_student'").collect()
    _ok.append(check("e-mail of gold.dim_student is masked", [r[0] for r in _masks] == ["email"]))
    _rf = spark.sql(f"SELECT count(*) FROM `{CAT}`.information_schema.row_filters WHERE table_schema = 'gold'").first()[0]
    _ok.append(check("row filter removed again from gold.fact_enrollments", _rf == 0))
except Exception as e:
    print("protections not checked:", _first_line(e))
print("\n🎉 Gold is ready - continue with 14-L4." if all(_ok) else "\nFix the ❌ items (re-run that part).")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🧠 What you practised
# MAGIC | Concept | Where |
# MAGIC |---|---|
# MAGIC | Star schema: facts vs dimensions, LEFT vs INNER join, `sequence` + `explode` date dimension | Part 1 |
# MAGIC | View vs materialized view vs table, `GROUP BY ALL`, windows, `QUALIFY`, `dense_rank` | Part 2 |
# MAGIC | `PIVOT`, `EXCEPT`, `FILTER (WHERE …)`, `LEFT ANTI JOIN` reconciliation | Part 3 |
# MAGIC | Liquid clustering, `OPTIMIZE`, `DESCRIBE DETAIL`, broadcast joins, `EXPLAIN`, query profile | Part 4 |
# MAGIC | Column mask, row filter + mapping table, `information_schema`, `INSERT OVERWRITE` keeps protections | Part 5 |
# MAGIC
# MAGIC ➡️ Next: **14-L4 — Automate it: pipeline, job, CI/CD and monitoring**.
