# Databricks notebook source
# MAGIC %md
# MAGIC # 🗂️ 14-D · Data used in Section 14 — SkillWave Academy
# MAGIC
# MAGIC The capstone uses its **own catalog** (`skillwave`, or `CAPSTONE_CATALOG`), not the ShopWave schema. All source files are
# MAGIC generated **deterministically** by `labs/_14_prepare` (`generate_skillwave_data()`) into the volume
# MAGIC `skillwave.landing.raw` — everyone gets identical data, so the numbers in the labs match yours.
# MAGIC
# MAGIC ### Source files (`/Volumes/skillwave/landing/raw/`)
# MAGIC | Folder | Format | Rows | Planted problems |
# MAGIC |---|---|---|---|
# MAGIC | `instructors/instructors.csv` | CSV, header | 10 | — |
# MAGIC | `courses/courses_2026.csv` | CSV, header | 21 | C120 twice · C118 without category |
# MAGIC | `students/students_01.json`, `_02.json` | JSON lines | 200 + 203 | 3 exact duplicates · 17 NULL and 18 messy e-mails · `profile` is a JSON string · `interests` array |
# MAGIC | `student-updates/updates_2026-09-10.json` | JSON lines | 25 | 20 changed students (15 plan upgrades free→pro, 5 moved city) + 5 new (S0401–S0405) |
# MAGIC | `_staging/enrollments/enrollments_2026-09-0{1..6}.json` | JSON lines | 150 each | 1 duplicate, 1 negative price, 1 unknown student `S9999`, 1 unknown course `C999` per file · column `referrer` from day 4 |
# MAGIC | `enrollments/` | — | — | where the daily files "arrive" (`land_day()`); day 1 lands with the generator |
# MAGIC | `syllabus/C101.txt … C120.txt` | text | 20 files | unstructured |
# MAGIC | `reviews/reviews_2026-09.json` | JSON lines | 123 | 3 duplicates · ratings 0, 6, NULL · 2 unknown enrollments (challenge) |
# MAGIC
# MAGIC ### Tables the labs build
# MAGIC | Lab | Objects |
# MAGIC |---|---|
# MAGIC | 14-L1 | catalog `skillwave`, schemas `landing`, `bronze`, `silver`, `gold`, volumes `landing.raw`, `landing.checkpoints`, grants on gold |
# MAGIC | 14-L2 | `bronze.{instructors, courses, students, student_updates, course_syllabus, enrollments}` · `silver.{students, courses, instructors, enrollments, enrollments_quarantine}` |
# MAGIC | 14-L3 | `gold.{dim_course, dim_student, dim_date, fact_enrollments, v_course_performance, mv_daily_category_revenue, course_leaderboard}` · `silver.region_access` · functions `gold.mask_email`, `gold.rf_student_country` |
# MAGIC | 14-L4 | schema `sdp` + pipeline tables `sdp_bronze_enrollments`, `sdp_silver_enrollments`, `sdp_gold_daily_revenue` · job **14 SkillWave Daily Refresh** · pipeline **14 SkillWave SDP pipeline** |
# MAGIC | 14-L5 | `bronze.reviews`, `silver.reviews`, `silver.reviews_quarantine`, `gold.course_ratings` |
# MAGIC | 14-CS1 / CS2 | schema `playground` (`cs_*`, `py_*` tables, volume `playground.files`) |
# MAGIC
# MAGIC ### Key figures
# MAGIC | After | Figure |
# MAGIC |---|---|
# MAGIC | 14-L2 (days 1–4) | bronze enrollments **600** · silver **584** · quarantine **12** · revenue **42,105.50** · students **405** (128 pro) |
# MAGIC | 14-L3 | dims 20 / 405 / 30 · fact 584 · Saudi Arabia 65 + Egypt 74 = 139 rows through the row filter · top course *Delta Lake Deep Dive* 4,352.10 |
# MAGIC | 14-L4 pipeline (days 1–4 → 5 → 6) | bronze 600 → 750 → 900 · silver 592 → 740 → 888 · gold 588 → 735 → 882 |
# MAGIC | 14-L4 job runs | run 1: silver 730 / gold 730 · run 2 (`max_quarantine = 2`): silver 876 / gold 730 · run 3: gold 876, revenue 62,671.40 |
# MAGIC | 14-L5 | reviews 123 → 115 valid + 5 quarantined · 17 rated courses |

# COMMAND ----------

# MAGIC %run ../labs/_14_prepare

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔍 Status and preview

# COMMAND ----------

# DBTITLE 1,What exists right now?
_st = foundation_status()
print("\nlanded enrollment days:", landed_days() if _st["volume landing.raw"] else "-")
if _st["catalog"]:
    _rows = []
    for _t in ("bronze.enrollments", "silver.students", "silver.enrollments", "silver.enrollments_quarantine",
               "gold.fact_enrollments", "gold.dim_student", "sdp.sdp_bronze_enrollments", "silver.reviews",
               "gold.course_ratings"):
        _rows.append((_t, count(_t) if table_exists(_t) else None))
    display(spark.createDataFrame(_rows, "table STRING, row_count LONG"))

# COMMAND ----------

# DBTITLE 1,Create the data now (only if you skipped 14-L1)
CREATE_FOUNDATION = False            # set True to create catalog, schemas, volumes and files in one go
if CREATE_FOUNDATION:
    require_foundation()

# COMMAND ----------

# DBTITLE 1,Peek at one daily enrollment file
if foundation_status(verbose=False)["source files"]:
    display(spark.read.json(f"{ENROLL_STAGING}/enrollments_2026-09-04.json").limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧹 Reset Section 14
# MAGIC `reset_capstone(confirm="YES")` deletes the capstone **job** and **pipeline** and runs `DROP CATALOG skillwave CASCADE`
# MAGIC (all schemas, tables, volumes and files). Use `drop_catalog=False` to keep the catalog and drop only its schemas.

# COMMAND ----------

# DBTITLE 1,Reset (optional)
# reset_capstone(confirm="YES")
