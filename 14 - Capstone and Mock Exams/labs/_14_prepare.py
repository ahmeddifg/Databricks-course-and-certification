# Databricks notebook source
# MAGIC %md
# MAGIC # 🔧 _14_prepare — SkillWave Academy capstone: configuration, data generator and helpers
# MAGIC Included by every Section 14 lab, the cheat sheets **and** the job-task notebooks:
# MAGIC
# MAGIC ```
# MAGIC %run ./_14_prepare          (task notebooks: %run ../_14_prepare)
# MAGIC ```
# MAGIC
# MAGIC Unlike Sections 01–13 this capstone does **not** use the ShopWave `Includes/_setup`: you build a brand-new
# MAGIC Unity Catalog **catalog** from scratch. This notebook only defines configuration and functions — it never creates
# MAGIC anything by itself (Lab 14-L1 does that step by step; later labs call `require_foundation()` to catch up if needed).
# MAGIC
# MAGIC **The story — SkillWave Academy** is an online learning platform: instructors publish courses, students enroll (one JSON
# MAGIC file of enrollments arrives every day), students rate courses. You build its lakehouse:
# MAGIC
# MAGIC ```
# MAGIC /Volumes/skillwave/landing/raw/  ──▶  skillwave.bronze  ──▶  skillwave.silver  ──▶  skillwave.gold  ──▶ dashboards / BI
# MAGIC    instructors · courses · students        raw copies          clean, typed,          star schema, views,
# MAGIC    enrollments (daily) · syllabus           + metadata          deduplicated            materialized view
# MAGIC    student-updates · reviews
# MAGIC ```
# MAGIC
# MAGIC | Helper | What it does |
# MAGIC |---|---|
# MAGIC | `CAT`, `LANDING`, `CHECKPOINTS` | catalog name (`CAPSTONE_CATALOG`, default `skillwave`) and the two volume paths |
# MAGIC | `generate_skillwave_data()` | writes all source files into the landing volume (deterministic, skips existing files) |
# MAGIC | `land_day(n)` · `land_until(day)` · `landed_days()` | simulate the daily enrollment files arriving (6 days: 1–6 Sep 2026) |
# MAGIC | `foundation_status()` · `require_foundation()` | show / build the catalog, schemas, volumes and data (catch-up) |
# MAGIC | `run_autoloader()` · `stop_all_streams()` | the bronze Auto Loader stream (availableNow) |
# MAGIC | `build_bronze()` · `build_silver()` · `build_gold()` | reference implementations of Labs 14-L2 / 14-L3 (used by the job and for catch-up) |
# MAGIC | `task_*` | the logic of the job-task notebooks in `labs/tasks/` |
# MAGIC | Jobs & pipeline API | `create_or_update_capstone_pipeline()`, `capstone_job_spec()`, `create_or_replace_job(spec)`, `job_summary(name)`, `run_job_now(name, params)`, `last_run(name)` |
# MAGIC | `check(label, cond)` · `reset_capstone(confirm="YES")` | ✅/❌ checks · drop everything |

# COMMAND ----------

# DBTITLE 1,Configuration
import json
import random
import time
import datetime as _dt

from pyspark.sql import functions as F
from pyspark.sql.window import Window

CAPSTONE_VERSION = "1.0.0"
CAT = globals().get("CAPSTONE_CATALOG") or "skillwave"       # set CAPSTONE_CATALOG before the %run to change it
SCHEMAS14 = ("landing", "bronze", "silver", "gold")
LANDING = f"/Volumes/{CAT}/landing/raw"                       # source files (the "outside world")
CHECKPOINTS = f"/Volumes/{CAT}/landing/checkpoints"           # streaming checkpoints + Auto Loader schemas
ENROLL_STAGING = f"{LANDING}/_staging/enrollments"            # files waiting to "arrive" (one per day)
ENROLL_LANDING = f"{LANDING}/enrollments"                     # where the daily files arrive
DAYS14 = 6                                                    # 2026-09-01 ... 2026-09-06
JOB14 = "14 SkillWave Daily Refresh"
PIPELINE14 = "14 SkillWave SDP pipeline"


def fq(name: str) -> str:
    """Fully qualified name: fq('gold.dim_course') -> 'skillwave.gold.dim_course'."""
    return f"{CAT}.{name}"


def _first_line(e) -> str:
    return (str(e).strip().splitlines() or [repr(e)])[0][:300]


def check(label: str, condition) -> bool:
    print(("✅ " if condition else "❌ ") + label)
    return bool(condition)


def path_exists(path: str) -> bool:
    try:
        dbutils.fs.ls(path)
        return True
    except Exception as e:
        msg = str(e).lower()
        if any(k in msg for k in ("not found", "notfound", "does not exist", "no such file")):
            return False
        raise


def _put(path: str, text: str):
    dbutils.fs.mkdirs(path.rsplit("/", 1)[0])
    dbutils.fs.put(path, text, True)


def _jsonl(rows) -> str:
    return "\n".join(json.dumps(r, ensure_ascii=False) for r in rows) + "\n"

# COMMAND ----------

# DBTITLE 1,Source data model (deterministic)
_INSTRUCTORS14 = [  # id, first, last, country, hire date, specialty
    ("I01", "Amira", "Haddad", "Saudi Arabia", "2023-02-01", "Data Engineering"),
    ("I02", "Daniel", "Okafor", "United Kingdom", "2022-09-15", "Data Engineering"),
    ("I03", "Mei", "Tanaka", "Japan", "2024-01-08", "Data Science"),
    ("I04", "Omar", "Farouk", "Egypt", "2023-06-20", "Cloud"),
    ("I05", "Sofia", "Rossi", "Germany", "2021-11-03", "AI"),
    ("I06", "Ravi", "Iyer", "India", "2024-04-11", "AI"),
    ("I07", "Hana", "Saleh", "United Arab Emirates", "2022-03-29", "Business"),
    ("I08", "Lucas", "Silva", "Brazil", "2023-10-02", "Data Science"),
    ("I09", "Grace", "Miller", "United States", "2021-05-17", "Cloud"),
    ("I10", "Yusuf", "Kaya", "Saudi Arabia", "2025-01-12", "Business"),
]
_COURSES14 = [  # id, title, category, level, instructor, list price, hours, published
    ("C101", "Lakehouse Fundamentals", "Data Engineering", "Beginner", "I01", 49.0, 6, "2025-01-15"),
    ("C102", "Delta Lake Deep Dive", "Data Engineering", "Intermediate", "I02", 89.0, 10, "2025-02-10"),
    ("C103", "Streaming with Spark", "Data Engineering", "Advanced", "I02", 149.0, 14, "2025-03-05"),
    ("C104", "Declarative Pipelines in Practice", "Data Engineering", "Intermediate", "I01", 99.0, 9, "2025-04-22"),
    ("C105", "Orchestrating with Lakeflow Jobs", "Data Engineering", "Intermediate", "I01", 79.0, 7, "2025-06-01"),
    ("C106", "Python for Data Analysis", "Data Science", "Beginner", "I03", 39.0, 12, "2025-01-20"),
    ("C107", "Statistics Essentials", "Data Science", "Beginner", "I08", 35.0, 8, "2025-02-14"),
    ("C108", "Machine Learning with Spark", "Data Science", "Advanced", "I03", 159.0, 18, "2025-05-09"),
    ("C109", "Feature Engineering", "Data Science", "Intermediate", "I08", 85.0, 9, "2025-07-30"),
    ("C110", "Cloud Storage Basics", "Cloud", "Beginner", "I09", 29.0, 4, "2025-01-05"),
    ("C111", "Data Security and Governance", "Cloud", "Intermediate", "I04", 95.0, 8, "2025-03-18"),
    ("C112", "Cost Optimization in the Cloud", "Cloud", "Advanced", "I09", 129.0, 6, "2025-08-12"),
    ("C113", "Intro to Generative AI", "AI", "Beginner", "I05", 45.0, 5, "2025-02-01"),
    ("C114", "Prompt Engineering", "AI", "Beginner", "I06", 39.0, 4, "2025-03-25"),
    ("C115", "Building RAG Apps", "AI", "Advanced", "I05", 179.0, 12, "2025-06-16"),
    ("C116", "AI Agents Workshop", "AI", "Advanced", "I06", 199.0, 10, "2025-09-01"),
    ("C117", "SQL for Business Analysts", "Business", "Beginner", "I07", 45.0, 8, "2025-01-28"),
    ("C118", "Dashboard Storytelling", None, "Beginner", "I07", 39.0, 5, "2025-04-04"),   # missing category
    ("C119", "Data Product Management", "Business", "Intermediate", "I10", 89.0, 7, "2025-07-07"),
    ("C120", "Data Strategy for Leaders", "Business", "Intermediate", "I10", 119.0, 6, "2025-10-20"),
]
_PLACES14 = {
    "Saudi Arabia": ["Riyadh", "Jeddah", "Dammam"], "Egypt": ["Cairo", "Alexandria"],
    "United Arab Emirates": ["Dubai", "Abu Dhabi"], "India": ["Bengaluru", "Mumbai", "Delhi"],
    "United States": ["New York", "Austin", "Seattle"], "United Kingdom": ["London", "Manchester"],
    "Germany": ["Berlin", "Munich"], "Brazil": ["Sao Paulo", "Recife"],
}
_FIRST14 = ["Adam", "Sara", "Omar", "Lina", "John", "Maya", "Ali", "Sofia", "Lucas", "Noor", "Emma", "Yusuf",
            "Hana", "Carlos", "Aisha", "Liam", "Mei", "Ivan", "Fatima", "Noah", "Zara", "Kenji", "Leila", "Mateo"]
_LAST14 = ["Smith", "Haddad", "Garcia", "Chen", "Muller", "Rossi", "Khan", "Silva", "Nakamura", "Dubois",
           "Ali", "Novak", "Kim", "Hansen", "Costa", "Saleh", "Brown", "Lopez"]
_DOMAINS14 = ["gmail.com", "outlook.com", "uni.edu", "company.com"]
_INTERESTS14 = ["sql", "python", "spark", "ai", "bi", "cloud", "ml"]
_COUPONS14 = {"WELCOME10": 0.10, "SUMMER20": 0.20, "VIP50": 0.50}
_REFERRERS14 = ["google", "youtube", "newsletter", "linkedin"]
_MODULES14 = {
    "Data Engineering": ["Architecture overview", "Ingesting data", "Transforming with SQL and Python",
                         "Quality and testing", "Orchestration and deployment"],
    "Data Science": ["Exploring data", "Visualising distributions", "Modelling basics", "Evaluating models"],
    "Cloud": ["Storage and networking", "Identity and access", "Monitoring and cost"],
    "AI": ["How large language models work", "Prompting patterns", "Retrieval and tools", "Evaluation and safety"],
    "Business": ["Asking the right questions", "Metrics that matter", "Telling the story", "Driving decisions"],
    None: ["Getting started", "Core techniques", "Putting it together"],
}


def _iso(d: _dt.datetime) -> str:
    return d.strftime("%Y-%m-%dT%H:%M:%SZ")


def _make_students14(rng, n=400):
    rows = []
    for i in range(1, n + 1):
        first, last = rng.choice(_FIRST14), rng.choice(_LAST14)
        country = rng.choice(list(_PLACES14))
        email = f"{first}.{last}{i}@{rng.choice(_DOMAINS14)}".lower()
        r = rng.random()
        if r < 0.04:
            email = None                                    # missing e-mail
        elif r < 0.09:
            email = f"  {email.upper()} "                   # messy e-mail -> lower(trim())
        profile = {"first_name": first, "last_name": last, "country": country,
                   "city": rng.choice(_PLACES14[country]), "birth_year": rng.randint(1975, 2006)}
        signup = _dt.datetime(2025, 1, 1) + _dt.timedelta(days=rng.randint(0, 600), seconds=rng.randint(0, 86399))
        rows.append({"student_id": f"S{i:04d}", "email": email, "profile": json.dumps(profile),
                     "plan": "pro" if rng.random() < 0.3 else "free",
                     "interests": rng.sample(_INTERESTS14, rng.randint(1, 3)), "signup_ts": _iso(signup)})
    return rows


def _make_student_updates14(rng, students):
    updates = []
    for s in rng.sample(students, 20):
        u = dict(s)
        prof = json.loads(s["profile"])
        if s["plan"] == "free":
            u["plan"] = "pro"                                  # upgrade
        else:
            prof["city"] = [c for c in _PLACES14[prof["country"]] if c != prof["city"]][0]   # moved city
        u["profile"] = json.dumps(prof)
        u["updated_ts"] = _iso(_dt.datetime(2026, 9, 10, 8) + _dt.timedelta(minutes=rng.randint(0, 600)))
        updates.append(u)
    new = _make_students14(random.Random(99), 405)[400:]       # S0401 ... S0405
    for u in new:
        u["updated_ts"] = _iso(_dt.datetime(2026, 9, 10, 9))
    return updates + new


def _make_enrollments14(day: int, students):
    rng = random.Random(1000 + day)
    base = _dt.datetime(2026, 9, day)
    popular = [c[0] for c in _COURSES14] + ["C101", "C101", "C113", "C113", "C114", "C106", "C117", "C102"]
    price = {c[0]: c[5] for c in _COURSES14}
    rows = []
    for k in range(1, 150):                                   # 146 valid + 3 bad
        course = rng.choice(popular)
        coupon = rng.choice(list(_COUPONS14)) if rng.random() < 0.2 else None
        status = rng.choices(["active", "completed", "refunded"], [70, 25, 5])[0]
        progress = 100 if status == "completed" else (rng.randint(0, 20) if status == "refunded" else rng.randint(0, 95))
        r = {"enrollment_id": f"E{day}{k:04d}", "student_id": rng.choice(students)["student_id"],
             "course_id": course,
             "enrolled_at": _iso(base + _dt.timedelta(seconds=rng.randint(0, 86399))),
             "price_paid": round(price[course] * (1 - _COUPONS14.get(coupon, 0)), 2),
             "coupon": coupon,
             "channel": rng.choices(["web", "mobile", "partner"], [55, 35, 10])[0],
             "status": status, "progress_pct": progress}
        if day >= 4:                                          # NEW column from day 4 -> schema evolution
            r["referrer"] = rng.choice(_REFERRERS14 + [None])
        if k == 147:
            r["student_id"] = "S9999"                        # unknown student
        elif k == 148:
            r["price_paid"] = -r["price_paid"]                # negative price
        elif k == 149:
            r["course_id"] = "C999"                           # unknown course
        rows.append(r)
    rows.append(dict(rows[rng.randint(0, 145)]))              # exact duplicate of a valid row
    rng.shuffle(rows)
    return rows


def _make_reviews14(rng, enrollments, emails):
    templates = {1: "Not what I expected.", 2: "Too fast for me.", 3: "OK, some good parts.",
                 4: "Very useful, clear examples.", 5: "Excellent course, highly recommended!"}
    valid = [e for e in enrollments if e["student_id"] != "S9999" and e["price_paid"] >= 0
             and e["course_id"] != "C999"]
    seen, unique = set(), []
    for e in valid:
        if e["enrollment_id"] not in seen:
            seen.add(e["enrollment_id"])
            unique.append(e)
    rows = []
    for n, e in enumerate(rng.sample(unique, 115), 1):
        rating = rng.choices([1, 2, 3, 4, 5], [5, 8, 17, 35, 35])[0]
        when = _dt.datetime.strptime(e["enrolled_at"], "%Y-%m-%dT%H:%M:%SZ") + _dt.timedelta(days=rng.randint(1, 9))
        rows.append({"review_id": f"R{n:04d}", "enrollment_id": e["enrollment_id"], "rating": rating,
                     "comment": templates[rating], "reviewed_at": _iso(when),
                     "reviewer_email": emails.get(e["student_id"])})
    bad = [("R0116", unique[0]["enrollment_id"], 0), ("R0117", unique[1]["enrollment_id"], 6),
           ("R0118", unique[2]["enrollment_id"], None), ("R0119", "E99001", 5), ("R0120", "E99002", 4)]
    for rid, eid, rating in bad:
        rows.append({"review_id": rid, "enrollment_id": eid, "rating": rating, "comment": "n/a",
                     "reviewed_at": "2026-09-12T10:00:00Z", "reviewer_email": None})
    rows += [dict(r) for r in rng.sample(rows[:115], 3)]      # 3 exact duplicates
    rng.shuffle(rows)
    return rows


def skillwave_source_data() -> dict:
    """All source records as Python objects (no I/O) - also used by checks to compute expected numbers."""
    rng = random.Random(1414)
    students = _make_students14(rng)
    students_with_dups = students + [dict(s) for s in rng.sample(students, 3)]
    updates = _make_student_updates14(rng, students)
    days = {d: _make_enrollments14(d, students) for d in range(1, DAYS14 + 1)}
    emails = {s["student_id"]: (s["email"] or "").strip().lower() or None for s in students}
    reviews = _make_reviews14(rng, days[1] + days[2] + days[3], emails)
    return {"students": students_with_dups, "updates": updates, "days": days, "reviews": reviews}

# COMMAND ----------

# DBTITLE 1,Write the files into the landing volume
def _csv(rows, header) -> str:
    def cell(v):
        return "" if v is None else str(v)
    return "\n".join([",".join(header)] + [",".join(cell(v) for v in r) for r in rows]) + "\n"


def generate_skillwave_data(verbose: bool = True) -> list:
    """Write all SkillWave source files into /Volumes/<CAT>/landing/raw (skips files that exist) and land day 1."""
    if not path_exists(f"/Volumes/{CAT}/landing/raw"):
        raise RuntimeError(f"Volume {CAT}.landing.raw does not exist yet - create it first (Lab 14-L1 Part 3) "
                           "or call require_foundation().")
    data = skillwave_source_data()
    files = {
        "instructors/instructors.csv": lambda: _csv(
            [(i, f, l, f"{f}.{l}@skillwave.academy".lower(), c, h, s) for i, f, l, c, h, s in _INSTRUCTORS14],
            ["instructor_id", "first_name", "last_name", "email", "country", "hire_date", "specialty"]),
        "courses/courses_2026.csv": lambda: _csv(
            _COURSES14 + [_COURSES14[-1]],                    # C120 appears twice (exact duplicate line)
            ["course_id", "title", "category", "level", "instructor_id", "list_price", "duration_hours",
             "published_on"]),
        "students/students_01.json": lambda: _jsonl(data["students"][:200]),
        "students/students_02.json": lambda: _jsonl(data["students"][200:]),
        "student-updates/updates_2026-09-10.json": lambda: _jsonl(data["updates"]),
        "reviews/reviews_2026-09.json": lambda: _jsonl(data["reviews"]),
    }
    for d, rows in data["days"].items():
        files[f"_staging/enrollments/enrollments_2026-09-{d:02d}.json"] = (lambda rows=rows: _jsonl(rows))
    for cid, title, category, level, instr, _p, hours, _pub in _COURSES14:
        modules = _MODULES14[category]
        text = (f"# {title}\nCourse ID: {cid}\nLevel: {level}\nInstructor: {instr}\nDuration: {hours} hours\n\n"
                "## Modules\n" + "\n".join(f"{n}. {m}" for n, m in enumerate(modules, 1)) + "\n")
        files[f"syllabus/{cid}.txt"] = (lambda text=text: text)
    written = []
    for rel, make in files.items():
        target = f"{LANDING}/{rel}"
        if not path_exists(target):
            _put(target, make())
            written.append(rel)
    if not landed_days():
        land_day(1, verbose=False)
        written.append("enrollments/enrollments_2026-09-01.json (landed)")
    if verbose:
        print(f"✅ SkillWave source data ready in {LANDING}" + (f" - wrote {len(written)} files" if written else
                                                                 " (nothing to do, all files exist)"))
    return written

# COMMAND ----------

# DBTITLE 1,The daily enrollment files: land_day()
def landed_days() -> list:
    """Days (1..6) whose enrollment file is already in the landing folder."""
    if not path_exists(ENROLL_LANDING):
        return []
    return sorted(int(f.name[-7:-5]) for f in dbutils.fs.ls(ENROLL_LANDING) if f.name.endswith(".json"))


def land_day(n: int = 1, verbose: bool = True) -> list:
    """Simulate the next n daily enrollment files arriving (copy from _staging into enrollments/)."""
    done = set(landed_days())
    todo = [d for d in range(1, DAYS14 + 1) if d not in done][:max(0, int(n))]
    for d in todo:
        name = f"enrollments_2026-09-{d:02d}.json"
        dbutils.fs.cp(f"{ENROLL_STAGING}/{name}", f"{ENROLL_LANDING}/{name}")
        if verbose:
            print(f"📥 landed {name}")
    if verbose and not todo:
        print("No more files to land - all 6 days are already in the landing folder.")
    return todo


def land_until(day: int, verbose: bool = True) -> list:
    """Make sure days 1..day are landed (re-run safe)."""
    missing = [d for d in range(1, day + 1) if d not in landed_days()]
    return land_day(len(missing), verbose) if missing else []


def reset_enrollments_landing():
    """Remove the landed enrollment files and land day 1 again."""
    if path_exists(ENROLL_LANDING):
        dbutils.fs.rm(ENROLL_LANDING, True)
    land_day(1)

# COMMAND ----------

# DBTITLE 1,Foundation: catalog, schemas, volumes (catch-up for learners who skip 14-L1)
def _catalog_exists() -> bool:
    return CAT in [r[0] for r in spark.sql("SHOW CATALOGS").collect()]


def foundation_status(verbose: bool = True) -> dict:
    st = {"catalog": _catalog_exists()}
    for s in SCHEMAS14:
        st[f"schema {s}"] = st["catalog"] and spark.sql(f"SHOW SCHEMAS IN `{CAT}` LIKE '{s}'").count() > 0
    for v in ("raw", "checkpoints"):
        st[f"volume landing.{v}"] = st["schema landing"] and path_exists(f"/Volumes/{CAT}/landing/{v}")
    st["source files"] = st["volume landing.raw"] and path_exists(f"{LANDING}/students")
    if verbose:
        for k, v in st.items():
            print(("✅ " if v else "⬜ ") + k)
    return st


def require_foundation(verbose: bool = True):
    """Create whatever is missing of Lab 14-L1 (catalog, schemas, volumes, data), then USE the catalog."""
    st = foundation_status(verbose=False)
    if not st["catalog"]:
        print(f"🛠️ catalog {CAT} is missing - creating it (Lab 14-L1 Part 1 does this step by step)")
        try:
            spark.sql(f"CREATE CATALOG IF NOT EXISTS `{CAT}` COMMENT 'SkillWave Academy - capstone lakehouse'")
        except Exception as e:
            raise RuntimeError(
                f"Could not create catalog '{CAT}': {_first_line(e)}\n"
                "On a paid workspace you need CREATE CATALOG on the metastore (and maybe a MANAGED LOCATION). "
                "Alternative: set CAPSTONE_CATALOG = '<a catalog you own>' in a cell BEFORE the %run cell.") from e
    for s in SCHEMAS14:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{CAT}`.`{s}`")
    for v in ("raw", "checkpoints"):
        spark.sql(f"CREATE VOLUME IF NOT EXISTS `{CAT}`.`landing`.`{v}`")
    spark.sql(f"USE CATALOG `{CAT}`")
    generate_skillwave_data(verbose=verbose)


def use_capstone():
    """USE CATALOG <CAT> so SQL cells can use two-level names like bronze.students."""
    spark.sql(f"USE CATALOG `{CAT}`")
    spark.sql("USE SCHEMA gold")


def table_exists(name: str) -> bool:
    try:
        return spark.catalog.tableExists(fq(name))
    except Exception:
        return False


def count(name: str) -> int:
    return spark.table(fq(name)).count() if table_exists(name) else -1

# COMMAND ----------

# DBTITLE 1,Bronze (reference implementation of Lab 14-L2 Parts 1-3)
def _bronze_sql() -> list:
    return [
        "CREATE TABLE IF NOT EXISTS bronze.instructors",
        f"""COPY INTO bronze.instructors
            FROM '{LANDING}/instructors/'
            FILEFORMAT = CSV
            FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'true', 'mergeSchema' = 'true')
            COPY_OPTIONS ('mergeSchema' = 'true')""",
        f"""CREATE OR REPLACE TABLE bronze.courses AS
            SELECT *, _metadata.file_name AS _source_file, current_timestamp() AS _ingested_at
            FROM read_files('{LANDING}/courses/', format => 'csv', header => true)""",
        f"""CREATE OR REPLACE TABLE bronze.students AS
            SELECT *, _metadata.file_name AS _source_file, current_timestamp() AS _ingested_at
            FROM read_files('{LANDING}/students/', format => 'json')""",
        f"""CREATE OR REPLACE TABLE bronze.student_updates AS
            SELECT *, _metadata.file_name AS _source_file, current_timestamp() AS _ingested_at
            FROM read_files('{LANDING}/student-updates/', format => 'json')""",
        f"""CREATE OR REPLACE TABLE bronze.course_syllabus AS
            SELECT replace(_metadata.file_name, '.txt', '') AS course_id, value AS syllabus_text,
                   _metadata.file_size AS file_size, current_timestamp() AS _ingested_at
            FROM read_files('{LANDING}/syllabus/', format => 'text', wholeText => true)""",
    ]


def _is_schema_change(e) -> bool:
    msg = str(e)
    return any(k in msg for k in ("UnknownFieldException", "UNKNOWN_FIELD_EXCEPTION", "NEW_FIELDS_IN_RECORD",
                                  "new fields", "schema has changed"))


def run_autoloader(verbose: bool = True) -> int:
    """Bronze enrollments with Auto Loader (availableNow). A schema change stops the stream once (addNewColumns):
    we simply start it again - exactly what a job retry does. Returns the number of rows in bronze.enrollments."""
    for attempt in (1, 2, 3):
        try:
            q = (spark.readStream.format("cloudFiles")
                 .option("cloudFiles.format", "json")
                 .option("cloudFiles.schemaLocation", f"{CHECKPOINTS}/bronze_enrollments/_schema")
                 .option("cloudFiles.inferColumnTypes", "true")
                 .load(ENROLL_LANDING)
                 .select("*", F.col("_metadata.file_name").alias("_source_file"),
                         F.current_timestamp().alias("_ingested_at"))
                 .writeStream
                 .option("checkpointLocation", f"{CHECKPOINTS}/bronze_enrollments")
                 .option("mergeSchema", "true")
                 .trigger(availableNow=True)
                 .toTable(fq("bronze.enrollments")))
            q.awaitTermination()
            break
        except Exception as e:
            if _is_schema_change(e) and attempt < 3:
                if verbose:
                    print(f"🔁 schema evolved (new column) - restarting the stream (attempt {attempt + 1})")
                continue
            raise
    n = spark.table(fq("bronze.enrollments")).count()
    if verbose:
        print(f"bronze.enrollments: {n} rows")
    return n


def stop_all_streams():
    for q in spark.streams.active:
        q.stop()
        print("⏹️ stopped stream", q.name or q.id)


def build_bronze(verbose: bool = True):
    spark.sql(f"USE CATALOG `{CAT}`")
    for stmt in _bronze_sql():
        spark.sql(stmt)
    run_autoloader(verbose)
    if verbose:
        for t in ("instructors", "courses", "students", "student_updates", "course_syllabus", "enrollments"):
            print(f"  bronze.{t:<16} {count('bronze.' + t)}")

# COMMAND ----------

# DBTITLE 1,Silver (reference implementation of Lab 14-L2 Parts 4-6)
STUDENT_COLS_SQL = """student_id,
       lower(trim(email))          AS email,
       profile:first_name::STRING  AS first_name,
       profile:last_name::STRING   AS last_name,
       profile:country::STRING     AS country,
       profile:city::STRING        AS city,
       profile:birth_year::INT     AS birth_year,
       plan,
       interests,
       CAST(signup_ts AS TIMESTAMP) AS signup_ts"""

SILVER_STUDENTS_SQL = f"""CREATE OR REPLACE TABLE silver.students
COMMENT 'SkillWave students - cleaned, one row per student (SCD type 1)'
AS
SELECT {STUDENT_COLS_SQL},
       CAST(signup_ts AS TIMESTAMP) AS updated_at
FROM (SELECT DISTINCT student_id, email, profile, plan, interests, signup_ts FROM bronze.students)"""

MERGE_STUDENTS_SQL = f"""MERGE INTO silver.students AS t
USING (
  SELECT {STUDENT_COLS_SQL},
         CAST(updated_ts AS TIMESTAMP) AS updated_at
  FROM bronze.student_updates
) AS s
ON t.student_id = s.student_id
WHEN MATCHED AND s.updated_at > t.updated_at THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *"""

SILVER_COURSES_SQL = """CREATE OR REPLACE TABLE silver.courses
COMMENT 'SkillWave courses - typed, deduplicated, category defaulted'
AS
SELECT DISTINCT
       course_id,
       title,
       coalesce(category, 'Uncategorized')    AS category,
       level,
       instructor_id,
       CAST(list_price AS DECIMAL(10,2))      AS list_price,
       CAST(duration_hours AS INT)            AS duration_hours,
       CAST(published_on AS DATE)             AS published_on
FROM bronze.courses"""

SILVER_INSTRUCTORS_SQL = """CREATE OR REPLACE TABLE silver.instructors
COMMENT 'SkillWave instructors'
AS
SELECT instructor_id,
       concat_ws(' ', first_name, last_name)  AS full_name,
       lower(email)                           AS email,
       country,
       CAST(hire_date AS DATE)                AS hire_date,
       specialty
FROM bronze.instructors"""

SILVER_ENROLL_DDL = """enrollment_id STRING NOT NULL, student_id STRING, course_id STRING, enrolled_at TIMESTAMP,
    enrolled_date DATE, price_paid DECIMAL(10,2), coupon STRING, channel STRING, status STRING, progress_pct INT,
    referrer STRING, _source_file STRING"""


def silver_enrollments_frames():
    """(valid_df, quarantine_df) from bronze.enrollments - the logic of Lab 14-L2 Part 6 (PySpark)."""
    bronze = spark.table(fq("bronze.enrollments"))
    referrer = F.col("referrer") if "referrer" in bronze.columns else F.lit(None).cast("string")
    students = spark.table(fq("silver.students")).select("student_id", F.lit(True).alias("_known_student"))
    courses = spark.table(fq("silver.courses")).select("course_id", F.lit(True).alias("_known_course"))
    checked = (bronze
               .dropDuplicates(["enrollment_id"])
               .select("enrollment_id", "student_id", "course_id",
                       F.col("enrolled_at").cast("timestamp").alias("enrolled_at"),
                       F.to_date(F.col("enrolled_at").cast("timestamp")).alias("enrolled_date"),
                       F.col("price_paid").cast("decimal(10,2)").alias("price_paid"),
                       F.col("coupon").cast("string").alias("coupon"), "channel", "status",
                       F.col("progress_pct").cast("int").alias("progress_pct"),
                       referrer.cast("string").alias("referrer"), "_source_file")
               .join(students, "student_id", "left")
               .join(courses, "course_id", "left")
               .withColumn("dq_reason",
                           F.when(F.col("enrollment_id").isNull(), "missing_id")
                            .when(F.col("price_paid") < 0, "negative_price")
                            .when(F.col("_known_student").isNull(), "unknown_student")
                            .when(F.col("_known_course").isNull(), "unknown_course"))
               .drop("_known_student", "_known_course"))
    cols = ["enrollment_id", "student_id", "course_id", "enrolled_at", "enrolled_date", "price_paid", "coupon",
            "channel", "status", "progress_pct", "referrer", "_source_file"]
    valid = checked.where("dq_reason IS NULL").select(*cols)
    quarantine = checked.where("dq_reason IS NOT NULL").select(*cols, "dq_reason",
                                                               F.current_timestamp().alias("quarantined_at"))
    return valid, quarantine


def merge_silver_enrollments() -> dict:
    """Insert-only MERGE (idempotent) of new valid rows into silver.enrollments and bad rows into the quarantine."""
    from delta.tables import DeltaTable
    spark.sql(f"CREATE TABLE IF NOT EXISTS {fq('silver.enrollments')} ({SILVER_ENROLL_DDL}) "
              "COMMENT 'SkillWave enrollments - valid, deduplicated'")
    spark.sql(f"CREATE TABLE IF NOT EXISTS {fq('silver.enrollments_quarantine')} ({SILVER_ENROLL_DDL}, "
              "dq_reason STRING, quarantined_at TIMESTAMP) COMMENT 'SkillWave enrollments that failed a quality rule'")
    before = {t: spark.table(fq(t)).count() for t in ("silver.enrollments", "silver.enrollments_quarantine")}
    valid, quarantine = silver_enrollments_frames()
    (DeltaTable.forName(spark, fq("silver.enrollments")).alias("t")
        .merge(valid.alias("s"), "t.enrollment_id = s.enrollment_id")
        .whenNotMatchedInsertAll()
        .execute())
    (DeltaTable.forName(spark, fq("silver.enrollments_quarantine")).alias("t")
        .merge(quarantine.alias("s"), "t.enrollment_id = s.enrollment_id")
        .whenNotMatchedInsertAll()
        .execute())
    after = {t: spark.table(fq(t)).count() for t in before}
    return {"new_valid": after["silver.enrollments"] - before["silver.enrollments"],
            "new_quarantined": after["silver.enrollments_quarantine"] - before["silver.enrollments_quarantine"],
            "valid_total": after["silver.enrollments"], "quarantine_total": after["silver.enrollments_quarantine"]}


def build_silver(verbose: bool = True) -> dict:
    spark.sql(f"USE CATALOG `{CAT}`")
    if not table_exists("silver.students"):
        spark.sql(SILVER_STUDENTS_SQL)
    spark.sql(MERGE_STUDENTS_SQL)
    if not table_exists("silver.courses"):
        spark.sql(SILVER_COURSES_SQL)
        spark.sql("ALTER TABLE silver.courses ADD CONSTRAINT valid_price CHECK (list_price >= 0)")
    if not table_exists("silver.instructors"):
        spark.sql(SILVER_INSTRUCTORS_SQL)
    res = merge_silver_enrollments()
    if verbose:
        print(f"silver: students {count('silver.students')} · courses {count('silver.courses')} · "
              f"instructors {count('silver.instructors')} · enrollments {res['valid_total']} "
              f"(+{res['new_valid']}) · quarantine {res['quarantine_total']} (+{res['new_quarantined']})")
    return res

# COMMAND ----------

# DBTITLE 1,Gold (reference implementation of Lab 14-L3)
GOLD_SELECTS = {
    "gold.dim_course": """SELECT c.course_id, c.title, c.category, c.level, c.list_price, c.duration_hours,
       c.published_on, i.instructor_id, i.full_name AS instructor_name, i.country AS instructor_country
FROM silver.courses c
LEFT JOIN silver.instructors i ON c.instructor_id = i.instructor_id""",
    "gold.dim_student": """SELECT student_id,
       concat_ws(' ', first_name, last_name)               AS full_name,
       email, country, city, plan, birth_year,
       CASE WHEN 2026 - birth_year < 25 THEN '18-24'
            WHEN 2026 - birth_year < 35 THEN '25-34'
            WHEN 2026 - birth_year < 45 THEN '35-44'
            ELSE '45+' END                                  AS age_band,
       CAST(signup_ts AS DATE)                              AS signup_date
FROM silver.students""",
    "gold.dim_date": """SELECT d                                                    AS date_key,
       year(d) AS year, month(d) AS month, day(d) AS day,
       date_format(d, 'EEEE')                               AS day_name,
       dayofweek(d) IN (6, 7)                               AS is_weekend_ksa
FROM (SELECT explode(sequence(DATE'2026-09-01', DATE'2026-09-30', INTERVAL 1 DAY)) AS d)""",
    "gold.fact_enrollments": """SELECT e.enrollment_id, e.enrolled_date, e.student_id, e.course_id,
       s.country                                            AS student_country,
       e.channel, e.coupon, e.status, e.progress_pct, e.price_paid,
       e.status = 'completed'                               AS is_completed
FROM silver.enrollments e
JOIN silver.students s ON e.student_id = s.student_id""",
}

GOLD_VIEW_SQL = """CREATE OR REPLACE VIEW gold.v_course_performance
COMMENT 'One row per course: enrollments, revenue, progress and completion'
AS
SELECT c.course_id, c.title, c.category, c.level, c.instructor_name,
       count(*)                                                     AS enrollments,
       round(sum(f.price_paid), 2)                                  AS revenue,
       round(avg(f.progress_pct), 1)                                AS avg_progress_pct,
       round(100 * avg(CASE WHEN f.is_completed THEN 1 ELSE 0 END), 1) AS completion_rate_pct
FROM gold.fact_enrollments f
JOIN gold.dim_course c ON f.course_id = c.course_id
GROUP BY ALL"""

GOLD_MV_SQL = """CREATE OR REPLACE MATERIALIZED VIEW gold.mv_daily_category_revenue
COMMENT 'Daily revenue per course category - refreshed by the job'
AS
SELECT f.enrolled_date, c.category,
       count(*)                     AS enrollments,
       round(sum(f.price_paid), 2)  AS revenue
FROM gold.fact_enrollments f
JOIN gold.dim_course c ON f.course_id = c.course_id
GROUP BY f.enrolled_date, c.category"""

GOLD_LEADERBOARD_SQL = """CREATE OR REPLACE TABLE gold.course_leaderboard
COMMENT 'Top 3 courses by revenue in each category'
AS
SELECT category, title, enrollments, revenue,
       dense_rank() OVER (PARTITION BY category ORDER BY revenue DESC)        AS rank_in_category,
       round(100 * revenue / sum(revenue) OVER (PARTITION BY category), 1)    AS pct_of_category
FROM gold.v_course_performance
QUALIFY rank_in_category <= 3"""


def build_gold(verbose: bool = True, refresh_mv: bool = True):
    """First run: CREATE OR REPLACE. Later runs: INSERT OVERWRITE - keeps grants, masks, clustering and history."""
    spark.sql(f"USE CATALOG `{CAT}`")
    for name, select in GOLD_SELECTS.items():
        if table_exists(name):
            spark.sql(f"INSERT OVERWRITE {name}\n{select}")
        else:
            spark.sql(f"CREATE OR REPLACE TABLE {name} AS\n{select}")
    spark.sql(GOLD_VIEW_SQL)
    spark.sql(GOLD_LEADERBOARD_SQL)
    if refresh_mv:
        try:
            if table_exists("gold.mv_daily_category_revenue"):
                spark.sql("REFRESH MATERIALIZED VIEW gold.mv_daily_category_revenue")
            else:
                spark.sql(GOLD_MV_SQL)
        except Exception as e:
            print("note: materialized view not refreshed -", _first_line(e))
    if verbose:
        print("gold: " + " · ".join(f"{n.split('.')[1]} {count(n)}" for n in GOLD_SELECTS))

# COMMAND ----------

# DBTITLE 1,Catch-up helpers for later labs
def require_silver(verbose: bool = True):
    """Build bronze + silver if Lab 14-L2 was skipped (lands days 1-4 like the lab does)."""
    require_foundation(verbose=False)
    if not (table_exists("silver.enrollments") and table_exists("silver.students") and table_exists("silver.courses")):
        print("🛠️ silver tables missing - building bronze and silver with the reference code (Lab 14-L2)")
        land_until(4, verbose=False)
        build_bronze(verbose)
        build_silver(verbose)
    use_capstone()


def require_gold(verbose: bool = True):
    """Build gold if Lab 14-L3 was skipped."""
    require_silver(verbose=False)
    if not table_exists("gold.fact_enrollments"):
        print("🛠️ gold tables missing - building them with the reference code (Lab 14-L3)")
        build_gold(verbose)
    use_capstone()

# COMMAND ----------

# DBTITLE 1,Job-task logic (the notebooks in labs/tasks/ call these)
def set_task_value(key: str, value):
    try:
        dbutils.jobs.taskValues.set(key=key, value=value)
    except Exception:
        pass                                                   # interactive run: no job context
    print(f"task value {key} = {value}")


def task_land_day(days="1") -> dict:
    landed = land_day(int(days or 1))
    return {"landed_days": landed, "all_landed": landed_days()}


def task_ingest_bronze() -> dict:
    require_foundation(verbose=False)
    for stmt in _bronze_sql():
        spark.sql(stmt)
    return {"bronze_enrollments": run_autoloader()}


def task_build_silver() -> dict:
    res = build_silver()
    set_task_value("quarantined", res["new_quarantined"])
    set_task_value("new_valid", res["new_valid"])
    return res


def task_build_gold() -> dict:
    build_gold()
    return {"fact_enrollments": count("gold.fact_enrollments")}


def task_quarantine_alert() -> dict:
    df = spark.table(fq("silver.enrollments_quarantine")).groupBy("dq_reason").count()
    rows = {r["dq_reason"]: r["count"] for r in df.collect()}
    print("⚠️ too many rows in quarantine - gold was NOT refreshed. Quarantine by reason:", rows)
    return {"quarantine_by_reason": rows}

# COMMAND ----------

# DBTITLE 1,Jobs & pipelines REST API helpers (Databricks SDK)
_ws14 = None


def _ws():
    global _ws14
    if _ws14 is None:
        from databricks.sdk import WorkspaceClient
        _ws14 = WorkspaceClient()
    return _ws14


def _api(method: str, path: str, body=None, query=None):
    return _ws().api_client.do(method, path, query=query, body=body) or {}


def _host() -> str:
    return _ws().config.host.rstrip("/")


def _me() -> str:
    return spark.sql("SELECT current_user()").first()[0]


def _notebook_path() -> str:
    try:
        return dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    except Exception:
        return dbutils.notebook.getContext().notebookPath


def _section_root() -> str:
    """Workspace path of the '14 - Capstone and Mock Exams' folder (override with SECTION14_ROOT)."""
    if globals().get("SECTION14_ROOT"):
        return SECTION14_ROOT.rstrip("/")
    nb = _notebook_path()
    cut = max(nb.rfind("/labs/"), nb.rfind("/data/"), nb.rfind("/questions/"), nb.rfind("/presentation/"))
    if cut < 0:
        raise RuntimeError("Set SECTION14_ROOT = '/Workspace/<path to>/14 - Capstone and Mock Exams' before the %run.")
    root = nb[:cut]
    return root if root.startswith("/Workspace") else "/Workspace" + root


def task_path(name: str) -> str:
    return f"{_section_root()}/labs/tasks/{name}"


def show_task_paths():
    for n in ("land_day", "ingest_bronze", "build_silver", "build_gold", "quarantine_alert"):
        print(f"{n:<17} {task_path(n)}")


def _paged(path: str, key: str, query=None, max_pages: int = 20) -> list:
    q, out = dict(query or {}), []
    for _ in range(max_pages):
        res = _api("GET", path, query=q)
        out += res.get(key) or []
        if not res.get("next_page_token"):
            break
        q["page_token"] = res["next_page_token"]
    return out


def find_pipeline14():
    res = _api("GET", "/api/2.0/pipelines", query={"filter": f"name LIKE '{PIPELINE14}'", "max_results": 100})
    mine = [s for s in res.get("statuses", []) if s.get("name") == PIPELINE14
            and s.get("creator_user_name") in (_me(), None)]
    return mine[0]["pipeline_id"] if mine else None


def create_or_update_capstone_pipeline() -> str:
    """Plan B for Lab 14-L4 Part 1: the serverless SQL pipeline in labs/pipelines/skillwave_sdp."""
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{CAT}`.`sdp` COMMENT 'Tables published by the SkillWave pipeline'")
    root = f"{_section_root()}/labs/pipelines/skillwave_sdp"
    spec = {"name": PIPELINE14, "catalog": CAT, "schema": "sdp", "serverless": True, "continuous": False,
            "development": True, "channel": "CURRENT", "root_path": root,
            "libraries": [{"glob": {"include": f"{root}/transformations/**"}}],
            "configuration": {"landing_path": LANDING}}
    pid = find_pipeline14()
    if pid:
        _api("PUT", f"/api/2.0/pipelines/{pid}", body=dict(spec, id=pid))
        print(f"🔁 pipeline '{PIPELINE14}' updated ({pid})")
        return pid
    pid = _api("POST", "/api/2.0/pipelines", body=spec)["pipeline_id"]
    print(f"🆕 pipeline '{PIPELINE14}' created ({pid})")
    return pid


def pipeline_link():
    pid = find_pipeline14()
    if pid:
        displayHTML(f'<a href="{_host()}/pipelines/{pid}" target="_blank">🔗 Open pipeline “{PIPELINE14}”</a>')
    else:
        print(f"No pipeline called '{PIPELINE14}' yet.")


def run_pipeline14(full_refresh: bool = False, timeout_min: int = 20) -> str:
    pid = find_pipeline14()
    if not pid:
        raise RuntimeError("Create the pipeline first.")
    uid = _api("POST", f"/api/2.0/pipelines/{pid}/updates", body={"full_refresh": full_refresh})["update_id"]
    print(f"🚀 pipeline update {uid} started")
    t0, state = time.time(), None
    while time.time() - t0 < timeout_min * 60:
        state = (_api("GET", f"/api/2.0/pipelines/{pid}/updates/{uid}").get("update") or {}).get("state")
        if state in ("COMPLETED", "FAILED", "CANCELED"):
            break
        time.sleep(10)
    print("pipeline update:", state)
    return state


def find_job(name: str = JOB14):
    me = _me()
    jobs = [j for j in _paged("/api/2.2/jobs/list", "jobs", {"name": name, "limit": 100})
            if (j.get("settings") or {}).get("name") == name and j.get("creator_user_name") in (me, None)]
    if not jobs:
        return None
    jobs.sort(key=lambda j: j.get("created_time", 0), reverse=True)
    return _api("GET", "/api/2.2/jobs/get", query={"job_id": jobs[0]["job_id"]})


def job_link(name: str = JOB14):
    j = find_job(name)
    if j:
        displayHTML(f'<a href="{_host()}/jobs/{j["job_id"]}" target="_blank">🔗 Open job “{name}”</a>')
    else:
        print(f"No job called '{name}' yet.")


def _task_type(t: dict) -> str:
    for key, label in (("notebook_task", "notebook"), ("pipeline_task", "pipeline"), ("condition_task", "if/else"),
                       ("sql_task", "sql"), ("for_each_task", "for each"), ("run_job_task", "run job")):
        if key in t:
            return label
    return "other"


def job_summary(name: str = JOB14, verbose: bool = True):
    """Tasks, types, dependencies, retries, parameters and schedule of the job - or None."""
    j = find_job(name)
    if not j:
        if verbose:
            print(f"❌ no job called '{name}' (check the exact name)")
        return None
    s = j.get("settings") or {}
    tasks = {}
    for t in s.get("tasks") or []:
        tasks[t["task_key"]] = {
            "type": _task_type(t),
            "depends_on": [(d["task_key"], d.get("outcome")) for d in t.get("depends_on") or []],
            "retries": t.get("max_retries", 0),
            "run_if": t.get("run_if", "ALL_SUCCESS"),
            "condition": t.get("condition_task"),
            "notebook": (t.get("notebook_task") or {}).get("notebook_path", ""),
            "pipeline_id": (t.get("pipeline_task") or {}).get("pipeline_id")}
    summary = {"job_id": j["job_id"], "tasks": tasks,
               "parameters": {p["name"]: p.get("default") for p in s.get("parameters") or []},
               "schedule": s.get("schedule"), "trigger": s.get("trigger")}
    if verbose:
        print(f"🧩 job '{name}' ({j['job_id']}) · parameters {summary['parameters']}")
        for k, t in tasks.items():
            deps = ", ".join(f"{d}{'=' + o if o else ''}" for d, o in t["depends_on"]) or "-"
            extra = f" · retries {t['retries']}" if t["retries"] else ""
            extra += f" · condition {t['condition']}" if t["condition"] else ""
            print(f"   • {k:<17} {t['type']:<9} ← {deps}{extra}")
        if summary["schedule"]:
            print(f"   schedule: {summary['schedule']}")
    return summary


def _task_state(t: dict) -> str:
    st = t.get("state") or {}
    return st.get("result_state") or st.get("life_cycle_state") or (t.get("status") or {}).get("state") or "?"


def run_summary(run_id: int, verbose: bool = True) -> dict:
    r = _api("GET", "/api/2.2/jobs/runs/get", query={"run_id": run_id})
    tasks = {t["task_key"]: {"state": _task_state(t), "outcome": (t.get("condition_task") or {}).get("outcome")}
             for t in r.get("tasks") or []}
    out = {"run_id": run_id, "state": _task_state(r), "tasks": tasks,
           "parameters": {p["name"]: p.get("value", p.get("default")) for p in r.get("job_parameters") or []}}
    if verbose:
        print(f"▶️ run {run_id}: {out['state']} · parameters {out['parameters']}")
        for k, t in tasks.items():
            print(f"   • {k:<17} {t['state']}" + (f" → outcome {t['outcome']}" if t["outcome"] else ""))
    return out


def wait_for_run(run_id: int, timeout_min: int = 40) -> dict:
    t0, last = time.time(), None
    while time.time() - t0 < timeout_min * 60:
        lc = (_api("GET", "/api/2.2/jobs/runs/get", query={"run_id": run_id}).get("state") or {}).get("life_cycle_state")
        if lc != last:
            print(f"   {time.strftime('%H:%M:%S')}  {lc}")
            last = lc
        if lc in ("TERMINATED", "INTERNAL_ERROR", "SKIPPED"):
            break
        time.sleep(10)
    return run_summary(run_id)


def run_job_now(name: str = JOB14, params: dict = None, wait: bool = True) -> dict:
    """Like 'Run now' / 'Run now with different parameters'."""
    j = find_job(name)
    if not j:
        raise RuntimeError(f"No job called '{name}' - create it first.")
    body = {"job_id": j["job_id"]}
    if params:
        body["job_parameters"] = {k: str(v) for k, v in params.items()}
    run_id = _api("POST", "/api/2.2/jobs/run-now", body=body)["run_id"]
    print(f"🚀 started '{name}' run {run_id}" + (f" with {params}" if params else ""))
    return wait_for_run(run_id) if wait else {"run_id": run_id}


def last_run(name: str = JOB14, verbose: bool = True):
    j = find_job(name)
    if not j:
        return None
    for r in _api("GET", "/api/2.2/jobs/runs/list", query={"job_id": j["job_id"], "limit": 10}).get("runs", []):
        if (r.get("state") or {}).get("life_cycle_state") in ("TERMINATED", "INTERNAL_ERROR", "SKIPPED"):
            return run_summary(r["run_id"], verbose)
    return None


def create_or_replace_job(spec: dict) -> int:
    j = find_job(spec["name"])
    if j:
        _api("POST", "/api/2.2/jobs/reset", body={"job_id": j["job_id"], "new_settings": spec})
        print(f"🔁 job '{spec['name']}' ({j['job_id']}) replaced")
        return j["job_id"]
    job_id = _api("POST", "/api/2.2/jobs/create", body=spec)["job_id"]
    print(f"🆕 job '{spec['name']}' created ({job_id})")
    return job_id


def nb_task(key: str, depends=(), **extra) -> dict:
    """A notebook task (serverless). depends: 'task' or ('task', 'true'/'false') for If/else branches."""
    t = {"task_key": key, "notebook_task": {"notebook_path": task_path(key), "source": "WORKSPACE"}}
    if depends:
        t["depends_on"] = [{"task_key": d} if isinstance(d, str) else {"task_key": d[0], "outcome": d[1]}
                           for d in depends]
    t.update(extra)
    return t


def capstone_job_spec() -> dict:
    """The finished job of Lab 14-L4 Part 2, as Jobs API 2.2 settings (Plan B / solution)."""
    pid = find_pipeline14()
    tasks = [
        nb_task("land_day"),
        nb_task("ingest_bronze", depends=["land_day"], max_retries=1, min_retry_interval_millis=10000),
        nb_task("build_silver", depends=["ingest_bronze"]),
        {"task_key": "check_quality", "depends_on": [{"task_key": "build_silver"}],
         "condition_task": {"op": "GREATER_THAN", "left": "{{tasks.build_silver.values.quarantined}}",
                            "right": "{{job.parameters.max_quarantine}}"}},
        nb_task("quarantine_alert", depends=[("check_quality", "true")]),
    ]
    gold_deps = [("check_quality", "false")]
    if pid:
        tasks.append({"task_key": "refresh_pipeline", "depends_on": [{"task_key": "land_day"}],
                      "pipeline_task": {"pipeline_id": pid}})
        gold_deps.append("refresh_pipeline")
    tasks.append(nb_task("build_gold", depends=gold_deps))
    return {"name": JOB14, "max_concurrent_runs": 1, "queue": {"enabled": True},
            "tags": {"project": "skillwave", "section": "14"},
            "parameters": [{"name": "catalog", "default": CAT}, {"name": "days", "default": "1"},
                           {"name": "max_quarantine", "default": "5"}],
            "schedule": {"quartz_cron_expression": "0 0 6 * * ?", "timezone_id": "Asia/Riyadh",
                         "pause_status": "PAUSED"},
            "tasks": tasks}


def pause_capstone_job():
    j = find_job()
    if j and (j["settings"].get("schedule") or {}).get("pause_status") == "UNPAUSED":
        _api("POST", "/api/2.2/jobs/update", body={"job_id": j["job_id"], "new_settings": {
            "schedule": dict(j["settings"]["schedule"], pause_status="PAUSED")}})
        print("⏸️ schedule paused")

# COMMAND ----------

# DBTITLE 1,Reset the capstone
def reset_capstone(confirm: str = "", drop_catalog: bool = True):
    """Delete the capstone job + pipeline and DROP CATALOG <CAT> CASCADE (or only its schemas)."""
    if confirm != "YES":
        print("Nothing done. To really drop everything call: reset_capstone(confirm='YES')")
        return
    stop_all_streams()
    for finder, kind in ((find_job, "jobs"), (find_pipeline14, "pipelines")):
        try:
            found = finder()
            if found and kind == "jobs":
                _api("POST", "/api/2.2/jobs/delete", body={"job_id": found["job_id"]})
                print("🗑️ job", JOB14)
            elif found:
                _api("DELETE", f"/api/2.0/pipelines/{found}")
                print("🗑️ pipeline", PIPELINE14)
        except Exception as e:
            print("note:", _first_line(e))
    if drop_catalog:
        spark.sql(f"DROP CATALOG IF EXISTS `{CAT}` CASCADE")
        print(f"🗑️ catalog {CAT} dropped")
    else:
        for s in ("gold", "silver", "bronze", "sdp", "playground", "landing"):
            spark.sql(f"DROP SCHEMA IF EXISTS `{CAT}`.`{s}` CASCADE")
        print(f"🗑️ schemas of {CAT} dropped")

# COMMAND ----------

# DBTITLE 1,Loaded
print(f"🎓 SkillWave capstone helpers loaded · catalog = {CAT} · landing = {LANDING}")
