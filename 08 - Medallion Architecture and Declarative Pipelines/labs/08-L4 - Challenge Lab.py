# Databricks notebook source
# MAGIC %md
# MAGIC # 🏆 Lab 08-L4 · Challenge Lab — Build a Declarative Pipeline
# MAGIC **Time:** ~60 min · **Compute:** Serverless notebook + a serverless pipeline · **Story:** ShopWave's mobile app sends
# MAGIC **product ratings** as JSON files. Build a medallion pipeline with Lakeflow Spark Declarative Pipelines: bronze (all
# MAGIC ratings) → silver (valid ratings, with data-quality rules) → gold (rating per product).
# MAGIC
# MAGIC You write the **pipeline code** in the SQL file `labs/pipelines/ratings_challenge/transformations/ratings.sql` (open it in
# MAGIC the pipeline editor) and answer a few questions in this notebook. Replace every `None` / `# TODO`, then run each **✅ Check**.
# MAGIC
# MAGIC | Task | Skill |
# MAGIC |---|---|
# MAGIC | 1 | Bronze **streaming table** with `STREAM read_files` + file metadata |
# MAGIC | 2 | Silver streaming table with **expectations** (drop / warn) |
# MAGIC | 3 | Gold **materialized view** with a join and deduplication |
# MAGIC | 4 | Read **data-quality metrics** from the event log |
# MAGIC | 5 | A **new column** arrives: schema evolution in bronze, extend silver |
# MAGIC | 6–9 | 🧠 Declarative pipeline concepts |
# MAGIC
# MAGIC > 🆓 Free Edition: make sure the 08-L2 / 08-L3 pipelines are not running while you run this one.

# COMMAND ----------

# MAGIC %run ../../Includes/_setup

# COMMAND ----------

# MAGIC %run ./_08_prepare

# COMMAND ----------

# DBTITLE 1,Setup for the challenge (run me - don't edit) - resets the ratings feed and your pipeline
delete_pipeline("L4")                       # your CODE lives in the repo file, not in the pipeline - nothing is lost
for _t in ("ch08_ratings_bronze", "ch08_ratings_silver", "ch08_product_ratings"):
    for _stmt in ("DROP TABLE IF EXISTS", "DROP MATERIALIZED VIEW IF EXISTS"):
        try:
            spark.sql(f"{_stmt} {_t}")
            break
        except Exception:
            pass
reset_feed("ratings")
_ = land_until("ratings", "ratings_02.json")          # ratings_01.json + ratings_02.json
print("pipeline source file to edit:", pipeline_source("ratings_challenge") + "/transformations/ratings.sql")

answer_task4 = answer_task6 = answer_task7 = answer_task8 = answer_task9 = None
state_run1 = state_run5 = None
_m = {}
_PRODUCTS = (spark.read.option("header", True).option("sep", ";").csv(f"{dataset_path}/products-csv")
                  .select("product_id", "title", "category"))


def check(label, condition):
    print(("✅ " if condition else "❌ ") + label)
    return bool(condition)


def _exists(t):
    return spark.catalog.tableExists(t)


def _expected():
    """What the tables should contain for the files landed so far - computed from the raw files."""
    raw = spark.read.json(landing("ratings"))
    valid = raw.where("rating BETWEEN 1 AND 5 AND product_id IS NOT NULL")
    known = valid.select("rating_id", "product_id", "rating").distinct().join(_PRODUCTS.select("product_id"), "product_id")
    return raw.count(), valid.count(), known

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tasks 1–3 · Write the pipeline (in the SQL file)
# MAGIC Open `labs/pipelines/ratings_challenge/transformations/ratings.sql` — the TODO comments describe each dataset in detail:
# MAGIC
# MAGIC | Task | Dataset | Type | Must have |
# MAGIC |---|---|---|---|
# MAGIC | 1 | `ch08_ratings_bronze` | streaming table | every source column + `source_file`; `rating` as INT; reads `${dataset_path}/lab08/ratings` |
# MAGIC | 2 | `ch08_ratings_silver` | streaming table | expectations `valid_rating` (1–5, **drop**), `has_product` (not null, **drop**), `has_comment` (not empty, **warn**); `rated_at` as TIMESTAMP |
# MAGIC | 3 | `ch08_product_ratings` | materialized view | per known product: `product_id`, `title`, `category`, `ratings` (distinct `rating_id`), `avg_rating` (2 decimals) |
# MAGIC
# MAGIC Then create the pipeline: name **`08-L4 Ratings challenge`**, default catalog/schema = the course schema, root folder
# MAGIC `ratings_challenge`, configuration `dataset_path`. 🖱️ UI (as in 08-L2) — or run the next cell. Use **Dry run** in the editor
# MAGIC to validate your code before a real run.

# COMMAND ----------

# DBTITLE 1,Values + create the pipeline if needed
for k, v in [("Pipeline name", PIPELINES08["L4"]["name"]), ("Default catalog", catalog_name),
             ("Default schema", schema_name), ("Root folder", pipeline_source("ratings_challenge")),
             ("Configuration", f"dataset_path = {dataset_path}")]:
    print(f"{k:<16}: {v}")
if not find_pipeline("L4"):
    create_or_update_pipeline("L4")
pipeline_link("L4")

# COMMAND ----------

# DBTITLE 1,Run your pipeline (or click "Run pipeline" in the editor) - rerun after every fix
state_run1 = run_pipeline("L4")

# COMMAND ----------

# DBTITLE 1,✅ Check 1-3
_n_raw, _n_valid, _known = _expected()
if _exists("ch08_ratings_bronze"):
    _b = spark.table("ch08_ratings_bronze")
    check(f"Task 1 · bronze holds every landed row ({_n_raw})", _b.count() == _n_raw)
    check("Task 1 · source_file column + rating is an INT",
          "source_file" in _b.columns and dict(_b.dtypes).get("rating") == "int")
else:
    check("Task 1 · ch08_ratings_bronze exists", False)
if _exists("ch08_ratings_silver"):
    _s = spark.table("ch08_ratings_silver")
    check(f"Task 2 · silver holds the {_n_valid} valid rows (invalid ratings and missing products dropped)",
          _s.count() == _n_valid)
    check("Task 2 · empty comments were only warned (kept)", _s.where("comment = ''").count() > 0)
    check("Task 2 · rated_at is a timestamp", dict(_s.dtypes).get("rated_at") == "timestamp")
else:
    check("Task 2 · ch08_ratings_silver exists", False)
if _exists("ch08_product_ratings"):
    _g = spark.table("ch08_product_ratings")
    _exp = {r["product_id"]: (r["n"], r["avg"]) for r in _known.groupBy("product_id")
            .agg(F.count("*").alias("n"), F.round(F.avg("rating"), 2).alias("avg")).collect()}
    _got = {r["product_id"]: (r["ratings"], float(r["avg_rating"])) for r in _g.collect()}
    check("Task 3 · one row per known product (no P999)", set(_got) == set(_exp))
    check("Task 3 · ratings counted once and averages correct",
          all(p in _got and _got[p][0] == n and abs(_got[p][1] - a) < 0.011 for p, (n, a) in _exp.items()))
else:
    check("Task 3 · ch08_product_ratings exists", False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 4 · How many ratings did `valid_rating` drop?
# MAGIC Query the pipeline's **event log** (you may use the helper `expectation_metrics("<table>")` or write the query yourself
# MAGIC with `event_log(TABLE(...))`) and store the number of **failed (dropped) records** of the expectation `valid_rating` in
# MAGIC `answer_task4` (an int).

# COMMAND ----------

# DBTITLE 1,Task 4
answer_task4 = None   # TODO

# COMMAND ----------

# DBTITLE 1,✅ Check 4
try:
    _m = {r["expectation"]: r["failed_records"] for r in expectation_metrics("ch08_ratings_silver").collect()}
except Exception as e:
    _m = {}
    print("event log not readable:", _first_line(e))
check("Task 4 · dropped by valid_rating matches the event log", answer_task4 is not None and answer_task4 == _m.get("valid_rating"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 5 · A new column arrives
# MAGIC The app now sends a new field **`helpful_votes`** (file `ratings_03.json`).
# MAGIC
# MAGIC 1. Run the next cell to land the file.
# MAGIC 2. Run the pipeline. Bronze picks up the new column automatically (schema evolution). *If the update stops with a message
# MAGIC    about a schema change, simply run it again — the new schema has already been recorded.*
# MAGIC 3. Edit **silver**: add `helpful_votes` to its column list (older rows will have `NULL`). Run the pipeline again.
# MAGIC 4. Store the state of your final run in `state_run5`.

# COMMAND ----------

# DBTITLE 1,📦 ratings_03.json arrives (safe to re-run)
_ = land_until("ratings", "ratings_03.json")

# COMMAND ----------

# DBTITLE 1,Task 5
# TODO: run the pipeline (again after editing silver) and keep the final state
state_run5 = None

# COMMAND ----------

# DBTITLE 1,✅ Check 5
_n_raw, _n_valid, _known = _expected()
check("Task 5 · last update completed", state_run5 == "COMPLETED")
check(f"Task 5 · bronze now has {_n_raw} rows and the column helpful_votes",
      _exists("ch08_ratings_bronze") and spark.table("ch08_ratings_bronze").count() == _n_raw
      and "helpful_votes" in spark.table("ch08_ratings_bronze").columns)
check("Task 5 · silver exposes helpful_votes (filled for the new file)",
      _exists("ch08_ratings_silver") and "helpful_votes" in spark.table("ch08_ratings_silver").columns
      and spark.table("ch08_ratings_silver").where("helpful_votes IS NOT NULL").count() > 0)
check(f"Task 5 · gold counts {_known.count()} distinct ratings",
      _exists("ch08_product_ratings") and spark.table("ch08_product_ratings").agg(F.sum("ratings")).first()[0] == _known.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 6 · 🧠 Gold dataset type
# MAGIC The gold table must always reflect the **current** content of silver — including rows that were corrected or deleted in
# MAGIC silver after they were first loaded. Which dataset type fits best?
# MAGIC
# MAGIC * **A** — A streaming table reading silver with `STREAM(...)`
# MAGIC * **B** — A temporary view
# MAGIC * **C** — A materialized view
# MAGIC * **D** — A streaming table with `skipChangeCommits`

# COMMAND ----------

# DBTITLE 1,Task 6
answer_task6 = None   # "A", "B", "C" or "D"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 7 · 🧠 Expectation actions
# MAGIC Records with a missing `email` must be **kept** in the target table, but the team wants to **know how many** there are.
# MAGIC Which definition is correct?
# MAGIC
# MAGIC * **A** — `CONSTRAINT has_email EXPECT (email IS NOT NULL)`
# MAGIC * **B** — `CONSTRAINT has_email EXPECT (email IS NOT NULL) ON VIOLATION DROP ROW`
# MAGIC * **C** — `CONSTRAINT has_email EXPECT (email IS NOT NULL) ON VIOLATION FAIL UPDATE`
# MAGIC * **D** — `CONSTRAINT has_email CHECK (email IS NOT NULL)`

# COMMAND ----------

# DBTITLE 1,Task 7
answer_task7 = None   # "A", "B", "C" or "D"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 8 · 🧠 Out-of-order CDC events
# MAGIC A CDC feed delivers updates out of order. With `AUTO CDC`, what guarantees that an older update never overwrites a newer
# MAGIC one in an SCD type 1 target?
# MAGIC
# MAGIC * **A** — `KEYS (customer_id)`
# MAGIC * **B** — `SEQUENCE BY` a column that orders the events (e.g. a sequence number or timestamp)
# MAGIC * **C** — `APPLY AS DELETE WHEN`
# MAGIC * **D** — Processing the files in alphabetical order

# COMMAND ----------

# DBTITLE 1,Task 8
answer_task8 = None   # "A", "B", "C" or "D"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 9 · 🧠 Full refresh
# MAGIC A streaming table ingests from Kafka, whose topic keeps messages for **7 days**. The table holds a year of data. What
# MAGIC happens on a **full refresh**, and how do you prevent it?
# MAGIC
# MAGIC * **A** — Nothing changes; full refresh only recomputes materialized views
# MAGIC * **B** — The table is truncated and rebuilt from Kafka → everything older than 7 days is lost; set the table property `pipelines.reset.allowed = false`
# MAGIC * **C** — The pipeline fails because Kafka can't be re-read; no action needed
# MAGIC * **D** — Only the checkpoint is reset; the existing rows are kept and new ones appended

# COMMAND ----------

# DBTITLE 1,Task 9
answer_task9 = None   # "A", "B", "C" or "D"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🏁 Final score
# MAGIC Run every **✅ Check** cell first.

# COMMAND ----------

# DBTITLE 1,Score
_n_raw, _n_valid, _known = _expected()
_final = {
    "Task 1": _exists("ch08_ratings_bronze") and spark.table("ch08_ratings_bronze").count() == _n_raw,
    "Task 2": _exists("ch08_ratings_silver") and spark.table("ch08_ratings_silver").count() == _n_valid,
    "Task 3": _exists("ch08_product_ratings")
              and spark.table("ch08_product_ratings").agg(F.sum("ratings")).first()[0] == _known.count(),
    "Task 4": answer_task4 is not None and answer_task4 == _m.get("valid_rating"),
    "Task 5": state_run5 == "COMPLETED" and _exists("ch08_ratings_silver")
              and "helpful_votes" in spark.table("ch08_ratings_silver").columns,
    "Task 6": answer_task6 == "C",
    "Task 7": answer_task7 == "A",
    "Task 8": answer_task8 == "B",
    "Task 9": answer_task9 == "B",
}
for k, v in _final.items():
    print(("✅ " if v else "❌ ") + k)
print(f"\nScore: {sum(_final.values())} / {len(_final)}")
