# Databricks notebook source
# MAGIC %md
# MAGIC # 🧪 Lab 08-L3 · Python Pipelines, AUTO CDC and the Event Log (Guided)
# MAGIC **Time:** ~60 min · **Compute:** Serverless notebook + a **serverless pipeline** · **Works on Free Edition**
# MAGIC
# MAGIC ShopWave's CRM publishes **change events** for customers: `INSERT`, `UPDATE`, `DELETE`, each with a `sequence_num`.
# MAGIC Events can arrive **late** (out of order) and the feed sometimes contains garbage. You'll run a **Python** pipeline that turns
# MAGIC the feed into an **SCD type 1** table (current state) and an **SCD type 2** table (full history) with `AUTO CDC` — no
# MAGIC `MERGE` written by hand — and then dig into the **event log**.
# MAGIC
# MAGIC | Part | You will… |
# MAGIC |---|---|
# MAGIC | 1 | Inspect the CDC feed |
# MAGIC | 2 | Read the Python pipeline (`pyspark.pipelines as dp`) |
# MAGIC | 3 | 🖱️ Create and run the pipeline → initial load |
# MAGIC | 4 | Updates, deletes and invalid events |
# MAGIC | 5 | **Late** events: why `SEQUENCE BY` matters |
# MAGIC | 6 | Point-in-time queries on SCD type 2 |
# MAGIC | 7 | The **event log**: data quality, update history, user actions, refresh techniques |
# MAGIC | 8 | 🕰️ Legacy syntax you must recognise |
# MAGIC | 9 | ✅ Automatic checks |

# COMMAND ----------

# MAGIC %run ../../Includes/_setup

# COMMAND ----------

# MAGIC %run ./_08_prepare

# COMMAND ----------

# MAGIC %md
# MAGIC ### Start clean
# MAGIC ⚠️ Deletes the `08-L3 ShopWave customers CDC (Python)` pipeline of an earlier attempt (and its tables), empties
# MAGIC `lab08/customers-cdc/` and delivers the first file.

# COMMAND ----------

# DBTITLE 1,Start clean (safe to re-run)
delete_pipeline("L3")
for t in ("cdc_customers_raw", "cdc_customers_scd1", "cdc_customers_scd2", "cdc_customers_per_country"):
    for stmt in ("DROP TABLE IF EXISTS", "DROP MATERIALIZED VIEW IF EXISTS"):
        try:
            spark.sql(f"{stmt} {t}")
            break
        except Exception:
            pass
reset_feed("cdc")
_ = land_until("cdc", "customers_cdc_01.json")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1 · The change feed

# COMMAND ----------

# DBTITLE 1,All three files of the feed (staging) - operations per file
_feed = (spark.read.json(f"{dataset_path}/customers-cdc-staging")
              .select("*", F.col("_metadata.file_name").alias("file")))
display(_feed.groupBy("file").pivot("operation").count().orderBy("file"))
display(_feed.where("customer_id = 'C0018'").orderBy("sequence_num")
             .select("file", "customer_id", "operation", "sequence_num", "email", "city", "country"))

# COMMAND ----------

# MAGIC %md
# MAGIC Look at customer **C0018**: inserted (seq 18), updated in file 02 (seq **1020**, back in Riyadh) — and file 03 brings an
# MAGIC update with seq **1015** (a move to Osaka) that **happened before** 1020 but **arrived after** it. A naive "last file wins"
# MAGIC `MERGE` would now say Osaka. `AUTO CDC … SEQUENCE BY sequence_num` gets it right.
# MAGIC
# MAGIC ## Part 2 · The Python source

# COMMAND ----------

# DBTITLE 1,Print the pipeline source file
_src = pipeline_source("shopwave_cdc_python")
print(open(f"{_src}/transformations/customers_cdc.py").read())

# COMMAND ----------

# MAGIC %md
# MAGIC | Python API (`from pyspark import pipelines as dp`) | Creates | SQL equivalent |
# MAGIC |---|---|---|
# MAGIC | `@dp.table` returning a **streaming** DataFrame | streaming table | `CREATE OR REFRESH STREAMING TABLE … AS SELECT … FROM STREAM …` |
# MAGIC | `@dp.table` / **`@dp.materialized_view`** returning a **batch** DataFrame | materialized view | `CREATE OR REFRESH MATERIALIZED VIEW` |
# MAGIC | `@dp.temporary_view` | temporary view (pipeline-only) | `CREATE TEMPORARY VIEW` |
# MAGIC | `@dp.expect` / `expect_or_drop` / `expect_or_fail` / `expect_all*` | expectations | `CONSTRAINT … EXPECT … [ON VIOLATION …]` |
# MAGIC | `dp.create_streaming_table()` + `dp.create_auto_cdc_flow()` | CDC target + flow | `CREATE OR REFRESH STREAMING TABLE t;` + `CREATE FLOW … AS AUTO CDC INTO t …` |
# MAGIC | `spark.read.table("x")` / `spark.readStream.table("x")` | read another dataset | `FROM x` / `FROM STREAM(x)` |
# MAGIC | `spark.conf.get("dataset_path")` | configuration value | `${dataset_path}` |
# MAGIC
# MAGIC The same CDC logic in SQL:
# MAGIC
# MAGIC ```sql
# MAGIC CREATE OR REFRESH STREAMING TABLE cdc_customers_scd2;
# MAGIC
# MAGIC CREATE FLOW customers_scd2_flow AS AUTO CDC INTO cdc_customers_scd2
# MAGIC FROM STREAM(cdc_customers_clean)
# MAGIC KEYS (customer_id)
# MAGIC APPLY AS DELETE WHEN operation = 'DELETE'
# MAGIC SEQUENCE BY sequence_num
# MAGIC COLUMNS * EXCEPT (operation, sequence_num, source_file)
# MAGIC STORED AS SCD TYPE 2
# MAGIC TRACK HISTORY ON * EXCEPT (change_ts);
# MAGIC ```
# MAGIC
# MAGIC > ⚠️ A pipeline source file only **defines** datasets. `collect()`, `count()`, `display()`, `toPandas()`, `save()`,
# MAGIC > `saveAsTable()`, `start()` or `toTable()` inside a dataset function are not allowed — the pipeline decides when to run it.
# MAGIC
# MAGIC ## Part 3 · 🖱️ Create and run the pipeline
# MAGIC Same steps as in 08-L2, with these values:

# COMMAND ----------

# DBTITLE 1,Values for the pipeline settings
for k, v in [("Pipeline name", PIPELINES08["L3"]["name"]), ("Default catalog", catalog_name),
             ("Default schema", schema_name), ("Root folder", pipeline_source("shopwave_cdc_python")),
             ("Source code path", pipeline_source("shopwave_cdc_python") + "/transformations"),
             ("Configuration key", "dataset_path"), ("Configuration value", dataset_path)]:
    print(f"{k:<20}: {v}")

# COMMAND ----------

# MAGIC %md
# MAGIC 🖱️ **New** → **ETL pipeline** → name → default catalog/schema → **Add existing assets** (root = `shopwave_cdc_python`,
# MAGIC source = its `transformations` folder) → **Settings** → **Configuration** → `dataset_path` → **Save**.
# MAGIC Free Edition: make sure the 08-L2 pipeline is **not running**.
# MAGIC
# MAGIC …or run the next cell (creates it only if it doesn't exist yet).

# COMMAND ----------

# DBTITLE 1,Create the pipeline if needed + link
if not find_pipeline("L3"):
    create_or_update_pipeline("L3")
pipeline_link("L3")

# COMMAND ----------

# DBTITLE 1,Run 1 - the initial load (300 INSERT events)
state_1 = run_pipeline("L3")
scd1_1, scd2_1 = table_count("cdc_customers_scd1"), table_count("cdc_customers_scd2")
print(f"SCD1 rows: {scd1_1} | SCD2 rows: {scd2_1}")
display(spark.table("cdc_customers_scd2").orderBy("customer_id").limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC Both tables hold **300** rows. SCD2 has two extra columns: **`__START_AT`** (the `sequence_num` that opened the version) and
# MAGIC **`__END_AT`** (`NULL` = current version). Their type is the type of the `SEQUENCE BY` column.
# MAGIC
# MAGIC ## Part 4 · Updates, deletes and invalid events
# MAGIC File 02: 30 `UPDATE`s, 3 `DELETE`s (C0080, C0275, C0287), 5 new customers and 2 events with the invalid operation `MERGE`.

# COMMAND ----------

# DBTITLE 1,Land file 02 and run
_ = land_until("cdc", "customers_cdc_02.json")
state_2 = run_pipeline("L3")
scd1_2, scd2_2 = table_count("cdc_customers_scd1"), table_count("cdc_customers_scd2")
print(f"SCD1 rows: {scd1_2} (300 + 5 new - 3 deleted) | SCD2 rows: {scd2_2} (305 inserts + 30 updates)")

# COMMAND ----------

# DBTITLE 1,A deleted customer in SCD1 vs SCD2
print("SCD1:", spark.table("cdc_customers_scd1").where("customer_id = 'C0080'").count(), "row(s)")
display(spark.table("cdc_customers_scd2").where("customer_id = 'C0080'")
             .select("customer_id", "email", "city", "__START_AT", "__END_AT"))

# COMMAND ----------

# MAGIC %md
# MAGIC * **SCD1** — the `DELETE` removed the row.
# MAGIC * **SCD2** — the history stays; the delete **closed** the current version (`__END_AT` = the delete's sequence number).
# MAGIC * The 2 `MERGE` events never reached `AUTO CDC`: `expect_all_or_drop` on the temporary view dropped them (Part 7 shows
# MAGIC   the metrics).
# MAGIC
# MAGIC ## Part 5 · Late events

# COMMAND ----------

# DBTITLE 1,Land file 03 (16 updates + 4 late ones) and run
_ = land_until("cdc", "customers_cdc_03.json")
state_3 = run_pipeline("L3")
scd1_3, scd2_3 = table_count("cdc_customers_scd1"), table_count("cdc_customers_scd2")
print(f"SCD1 rows: {scd1_3} | SCD2 rows: {scd2_3} (335 + 16 + 4 late versions)")

# COMMAND ----------

# DBTITLE 1,C0018 - current state and history
display(spark.table("cdc_customers_scd1").where("customer_id = 'C0018'").select("customer_id", "email", "city", "country"))
display(spark.table("cdc_customers_scd2").where("customer_id = 'C0018'")
             .select("customer_id", "email", "city", "country", "__START_AT", "__END_AT").orderBy("__START_AT"))
c0018_scd1 = spark.table("cdc_customers_scd1").where("customer_id = 'C0018'").first()
c0018_versions = [r["city"] for r in spark.table("cdc_customers_scd2").where("customer_id = 'C0018'")
                                          .orderBy("__START_AT").collect()]

# COMMAND ----------

# MAGIC %md
# MAGIC * **SCD1** still says **Riyadh** (seq 1020): the late event (seq 1015) is older than what is already applied → ignored.
# MAGIC * **SCD2** slots the late event **into the history** where it belongs: Riyadh [18, 1015) → Osaka [1015, 1020) → Riyadh [1020, ∞).
# MAGIC
# MAGIC Order is decided by **`SEQUENCE BY`**, never by arrival time. The sequencing column must be sortable and **not NULL**
# MAGIC (that's why the view drops events without a `sequence_num`).
# MAGIC
# MAGIC ## Part 6 · Point-in-time queries on SCD type 2

# COMMAND ----------

# DBTITLE 1,Where did each customer live at sequence 1017?
display(spark.sql("""
    SELECT customer_id, city, country, __START_AT, __END_AT
    FROM cdc_customers_scd2
    WHERE __START_AT <= 1017 AND (__END_AT > 1017 OR __END_AT IS NULL)
      AND customer_id IN ('C0018', 'C0080', 'C0301')
    ORDER BY customer_id"""))

# COMMAND ----------

# MAGIC %md
# MAGIC C0018 was in **Osaka** at 1017; C0080 still existed (deleted later); C0301 didn't exist yet (inserted in file 02 with a
# MAGIC higher sequence number). With timestamps as the sequence column this becomes "as of date X" reporting.
# MAGIC
# MAGIC ## Part 7 · The event log
# MAGIC Every pipeline writes an **event log** (a Delta table, hidden by default). Read it with `event_log(TABLE(<any of its
# MAGIC tables>))` or `event_log('<pipeline id>')`, or publish it to a Unity Catalog table in the pipeline settings.

# COMMAND ----------

# DBTITLE 1,1 · Data quality - drops and warnings
dq = expectation_metrics("cdc_customers_scd1")
display(dq)

# COMMAND ----------

# MAGIC %md
# MAGIC * `valid_operation` → the 2 `MERGE` events (a temporary view is evaluated by **every flow that reads it** — here two
# MAGIC   AUTO CDC flows — so you may see each drop counted per flow).
# MAGIC * `has_email` → customers without an e-mail were **kept** (warn), just counted.
# MAGIC * `has_key` (**fail**) → 0 failed; had it failed, the update would have stopped and no metrics would be written for it.

# COMMAND ----------

# DBTITLE 1,2 · Update history - one row per update and state change
display(spark.sql("""
    SELECT timestamp, origin.update_id, details:update_progress.state::string AS state
    FROM event_log(TABLE(cdc_customers_scd1))
    WHERE event_type = 'update_progress'
    ORDER BY timestamp"""))

# COMMAND ----------

# DBTITLE 1,3 · Audit - who did what (user_action events)
display(spark.sql("""
    SELECT timestamp, details:user_action.action::string AS action, details:user_action.user_name::string AS user_name
    FROM event_log(TABLE(cdc_customers_scd1))
    WHERE event_type = 'user_action'
    ORDER BY timestamp"""))

# COMMAND ----------

# DBTITLE 1,4 · How was the materialized view refreshed? (planning_information)
try:
    display(spark.sql("""
        SELECT timestamp, origin.flow_name, details:planning_information.technique_information AS techniques
        FROM event_log(TABLE(cdc_customers_scd1))
        WHERE event_type = 'planning_information'
        ORDER BY timestamp DESC"""))
except Exception as e:
    print("planning_information not available here:", _first_line(e))

# COMMAND ----------

# MAGIC %md
# MAGIC For materialized views Databricks decides per update whether an **incremental** refresh is cheaper than a **full
# MAGIC recompute** (serverless pipelines) — `planning_information` shows the choice (e.g. `ROW_BASED`, `PARTITION_OVERWRITE`,
# MAGIC `COMPLETE_RECOMPUTE`).
# MAGIC
# MAGIC | Event type | Tells you |
# MAGIC |---|---|
# MAGIC | `create_update`, `update_progress` | an update started; its states (`INITIALIZING`, `RUNNING`, `COMPLETED`, `FAILED` …) |
# MAGIC | `flow_progress` | per flow: status, rows written, **data-quality metrics**, backlog |
# MAGIC | `flow_definition` | lineage: input and output datasets, schema |
# MAGIC | `planning_information` | MV refresh technique |
# MAGIC | `user_action` | audit: who created, edited, started, stopped the pipeline |
# MAGIC | `cluster_resources`, `autoscale` | compute (classic / enhanced autoscaling) |
# MAGIC
# MAGIC ## Part 8 · 🕰️ Legacy syntax you must recognise
# MAGIC Exam questions (and older code) still use the Delta Live Tables names. They still run, but write new code with the new API.
# MAGIC
# MAGIC | Legacy (DLT) | Today (Lakeflow SDP) |
# MAGIC |---|---|
# MAGIC | `import dlt` · `@dlt.table` · `@dlt.view` | `from pyspark import pipelines as dp` · `@dp.table` / `@dp.materialized_view` · `@dp.temporary_view` |
# MAGIC | `dlt.read("x")` / `dlt.read_stream("x")` · `LIVE.x` | `spark.read.table("x")` / `spark.readStream.table("x")` · `x` |
# MAGIC | `CREATE LIVE TABLE` · `CREATE STREAMING LIVE TABLE` | `CREATE OR REFRESH MATERIALIZED VIEW` · `CREATE OR REFRESH STREAMING TABLE` |
# MAGIC | `FROM cloud_files('/path', 'json')` | `FROM STREAM read_files('/path', format => 'json')` |
# MAGIC | `APPLY CHANGES INTO LIVE.t FROM STREAM(LIVE.s) …` · `dlt.apply_changes()` | `AUTO CDC INTO t FROM STREAM(s) …` · `dp.create_auto_cdc_flow()` |
# MAGIC | `dlt.create_streaming_live_table()` / `create_target_table()` | `dp.create_streaming_table()` |
# MAGIC
# MAGIC ## Part 9 · ✅ Automatic checks

# COMMAND ----------

# DBTITLE 1,Check your work
from pyspark.sql import Window
_valid = (spark.read.json(landing("cdc"))
               .where("operation IN ('INSERT', 'UPDATE', 'DELETE') AND customer_id IS NOT NULL AND sequence_num IS NOT NULL"))
_latest = (_valid.withColumn("_rn", F.row_number().over(Window.partitionBy("customer_id").orderBy(F.desc("sequence_num"))))
                 .where("_rn = 1"))
_expected_scd1 = _latest.where("operation <> 'DELETE'").count()
_expected_scd2 = _valid.where("operation <> 'DELETE'").count()
_scd2 = spark.table("cdc_customers_scd2")
_merge_applied = (spark.table("cdc_customers_scd1")                  # a MERGE event's e-mail must not show up
                       .join(spark.read.json(landing("cdc")).where("operation = 'MERGE'").select("customer_id", "email"),
                             ["customer_id", "email"]).count())
_checks = {
    "all three updates completed": (state_1, state_2, state_3) == ("COMPLETED",) * 3,
    "initial load: 300 / 300": (scd1_1, scd2_1) == (300, 300),
    "file 02: SCD1 302, SCD2 335": (scd1_2, scd2_2) == (302, 335),
    f"SCD1 = latest valid event per key, deletes removed ({_expected_scd1})": scd1_3 == _expected_scd1 == 302,
    f"SCD2 = one version per INSERT/UPDATE ({_expected_scd2})": scd2_3 == _expected_scd2 == 355,
    "SCD2 has exactly one current version per live customer": _scd2.where("__END_AT IS NULL").count() == 302,
    "deleted customers are gone from SCD1": spark.table("cdc_customers_scd1")
                                             .where("customer_id IN ('C0080', 'C0275', 'C0287')").count() == 0,
    "the MERGE events were not applied": _merge_applied == 0,
    "late event ignored by SCD1 (C0018 in Riyadh)": c0018_scd1 is not None and c0018_scd1["city"] == "Riyadh",
    "late event slotted into SCD2 history (Riyadh -> Osaka -> Riyadh)": c0018_versions == ["Riyadh", "Osaka", "Riyadh"],
    "per-country MV adds up to the current customers": spark.table("cdc_customers_per_country")
                                                         .agg(F.sum("customers")).first()[0] == 302,
}
for name, ok in _checks.items():
    print(("✅ " if ok else "❌ ") + name)
print("\n🎉 Lab complete - next: 08-L4 · Challenge lab")
