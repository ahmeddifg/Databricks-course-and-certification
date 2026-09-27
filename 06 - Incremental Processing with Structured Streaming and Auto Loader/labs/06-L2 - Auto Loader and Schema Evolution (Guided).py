# Databricks notebook source
# MAGIC %md
# MAGIC # 🧪 Lab 06-L2 · Auto Loader & Schema Evolution (Guided)
# MAGIC **Time:** ~50 min · **Compute:** Serverless (Free Edition) — `trigger(availableNow=True)` everywhere
# MAGIC
# MAGIC The mobile app team sends **app events** as JSON files. Their schema is not stable: a **`coupon`** field appears in
# MAGIC batch 3, and batch 4 sends some amounts as text. Auto Loader (`cloudFiles`) will infer the schema, track it, evolve it,
# MAGIC and **rescue** what doesn't fit — without losing a single row.
# MAGIC
# MAGIC | Part | You will… |
# MAGIC |---|---|
# MAGIC | 1 | Start an Auto Loader stream and look at the **inferred schema** (strings by default!) |
# MAGIC | 2 | Load a bronze table with `inferColumnTypes`, `schemaHints` and file metadata |
# MAGIC | 3 | Look at the **schema location** (`_schemas`) |
# MAGIC | 4 | Watch **schema evolution** (`addNewColumns`): the stream stops once, then continues with the new column |
# MAGIC | 5 | See **rescued data** for values with the wrong type |
# MAGIC | 6 | Compare the other evolution modes: `rescue`, `failOnNewColumns`, `none` |
# MAGIC | 7 | Control batch size (`maxFilesPerTrigger`) and existing files (`includeExistingFiles`) |
# MAGIC | 8 | File detection modes and the SQL flavour of Auto Loader |
# MAGIC | 9 | ✅ Automatic checks |

# COMMAND ----------

# MAGIC %run ../../Includes/_setup

# COMMAND ----------

# MAGIC %run ./_06_prepare

# COMMAND ----------

# DBTITLE 1,Start clean (safe to re-run)
stop_all_streams()
for t in ("lab06_events_bronze", "lab06_events_rescue", "lab06_events_batches", "lab06_events_new_only"):
    spark.sql(f"DROP TABLE IF EXISTS {t}")
for c in ("events_peek", "events_bronze", "events_rescue", "events_batches", "events_new_only"):
    if path_exists(checkpoint(c)):
        dbutils.fs.rm(checkpoint(c), True)
reset_events_landing()                                    # events-landing/ holds events_01.json only
events_landing = f"{dataset_path}/events-landing"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1 · The minimal Auto Loader stream
# MAGIC Auto Loader is a streaming **source** named `cloudFiles`. Two options are required for schema inference:
# MAGIC `cloudFiles.format` (the file format) and `cloudFiles.schemaLocation` (where it stores the inferred schema).

# COMMAND ----------

# DBTITLE 1,Minimal Auto Loader reader - look at the schema it inferred
peek = (spark.readStream.format("cloudFiles")
             .option("cloudFiles.format", "json")
             .option("cloudFiles.schemaLocation", checkpoint("events_peek"))
             .load(events_landing))
peek.printSchema()
peek_types = dict(peek.dtypes)

# COMMAND ----------

# MAGIC %md
# MAGIC Two surprises:
# MAGIC 1. **Every column is a `string`** — for JSON, CSV and XML Auto Loader infers **strings** by default (safe for bronze: nothing
# MAGIC    can fail to parse). Set `cloudFiles.inferColumnTypes = true` to get numbers, booleans, etc.
# MAGIC 2. There is an extra **`_rescued_data`** column — Auto Loader adds it automatically when it infers the schema.
# MAGIC
# MAGIC Inference samples the first **50 GB or 1,000 files** (whichever comes first). The result is saved in the schema location,
# MAGIC so the next start doesn't infer again.
# MAGIC
# MAGIC ## Part 2 · Bronze table with typed columns

# COMMAND ----------

# DBTITLE 1,Auto Loader -> Delta bronze table
def events_reader(mode="addNewColumns", schema_location=None):
    return (spark.readStream.format("cloudFiles")
                 .option("cloudFiles.format", "json")
                 .option("cloudFiles.schemaLocation", schema_location or checkpoint("events_bronze"))
                 .option("cloudFiles.inferColumnTypes", "true")
                 .option("cloudFiles.schemaHints", "event_time TIMESTAMP")   # force what inference might get wrong
                 .option("cloudFiles.schemaEvolutionMode", mode)            # addNewColumns is the default
                 .load(events_landing)
                 .select("*",
                         F.col("_metadata.file_name").alias("source_file"),
                         F.current_timestamp().alias("ingested_at")))


def bronze_events():
    """Build the stream definition. Re-building it before each run makes sure a restart picks up the latest schema."""
    return (events_reader().writeStream
            .option("checkpointLocation", checkpoint("events_bronze"))  # the schema location may live inside the checkpoint
            .option("mergeSchema", "true"))                             # let the TABLE evolve too (see Part 4)


run_stream(bronze_events(), "lab06_events_bronze")
rows_b1 = spark.table("lab06_events_bronze").count()
print("rows:", rows_b1)
print({c: t for c, t in spark.table("lab06_events_bronze").dtypes})

# COMMAND ----------

# MAGIC %md
# MAGIC `amount` is now a `double` and `event_time` a `timestamp` (thanks to the hint).
# MAGIC
# MAGIC To compare evolution modes later, start a **second** stream on the same folder — identical, except
# MAGIC `schemaEvolutionMode = "rescue"`, and with its **own** schema location and checkpoint.

# COMMAND ----------

# DBTITLE 1,A twin stream in rescue mode (we'll compare in Part 6)
def rescue_writer():
    return (events_reader(mode="rescue", schema_location=checkpoint("events_rescue")).writeStream
            .option("checkpointLocation", checkpoint("events_rescue")))


run_stream(rescue_writer(), "lab06_events_rescue")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 3 · The schema location

# COMMAND ----------

# DBTITLE 1,_schemas: Auto Loader's schema history
schemas_dir = f"{checkpoint('events_bronze')}/_schemas"
for f in dbutils.fs.ls(schemas_dir):
    print(f.name, f.size, "bytes")
print("\nversion 0 (first lines):\n", dbutils.fs.head(f"{schemas_dir}/0", 400))

# COMMAND ----------

# MAGIC %md
# MAGIC Each schema change creates a new numbered file. The stream always starts from the **latest** schema version.
# MAGIC
# MAGIC ## Part 4 · Schema evolution with `addNewColumns` (the default)

# COMMAND ----------

# DBTITLE 1,Batch 2: same schema -> business as usual
land_until("events", "events_02.json")     # re-running this cell lands nothing new
run_stream(bronze_events(), "lab06_events_bronze")
rows_b2 = spark.table("lab06_events_bronze").count()
print("rows:", rows_b2)

# COMMAND ----------

# DBTITLE 1,Batch 3 brings a NEW field `coupon` -> the stream stops ONCE
land_until("events", "events_03.json")
evolution_failed = globals().get("evolution_failed", False)   # keeps the result if you re-run this cell
try:
    run_stream(bronze_events(), "lab06_events_bronze")
except Exception as e:
    evolution_failed = True
    print("🛑 Stream stopped (expected):", (str(e).strip().splitlines() or [repr(e)])[0][:220])
print("schema versions now:", sorted(f.name for f in dbutils.fs.ls(schemas_dir)))

# COMMAND ----------

# DBTITLE 1,Meanwhile, the rescue-mode twin does NOT stop
rescue_failed = False
try:
    run_stream(rescue_writer(), "lab06_events_rescue")
except Exception as e:
    rescue_failed = True
    print("unexpected:", (str(e).strip().splitlines() or [repr(e)])[0][:200])

# COMMAND ----------

# MAGIC %md
# MAGIC That's by design: Auto Loader detected `coupon`, **added it to the schema in the schema location**, and stopped with an
# MAGIC `UnknownFieldException` so that no data is silently processed with the old schema. **Just restart** the stream (run the
# MAGIC cell that defines and starts it again) — in production a **Lakeflow Job with retries** does this automatically.

# COMMAND ----------

# DBTITLE 1,Restart -> the new column is picked up
run_stream(bronze_events(), "lab06_events_bronze")
rows_b3 = spark.table("lab06_events_bronze").count()
display(spark.sql("SELECT source_file, count(*) AS rows, count(coupon) AS with_coupon "
                  "FROM lab06_events_bronze GROUP BY source_file ORDER BY source_file"))

# COMMAND ----------

# MAGIC %md
# MAGIC > ⚠️ Two different "evolutions" are involved: Auto Loader evolves the **stream's schema**; the Delta **table** accepts the
# MAGIC > new column only because the writer has `.option("mergeSchema", "true")`. Without it the restart would fail on the sink.
# MAGIC
# MAGIC ## Part 5 · Rescued data
# MAGIC Batch 4 sends three purchases with `"amount": "12.50 EUR"` — text in a `double` column.

# COMMAND ----------

# DBTITLE 1,Batch 4: wrong types don't fail the stream, they are rescued
land_until("events", "events_04.json")
run_stream(bronze_events(), "lab06_events_bronze")
rows_b4 = spark.table("lab06_events_bronze").count()
rescued = spark.table("lab06_events_bronze").where("_rescued_data IS NOT NULL")
rescued_rows = rescued.count()
display(rescued.select("event_id", "event_type", "amount", "_rescued_data", "source_file"))
run_stream(rescue_writer(), "lab06_events_rescue")                          # the twin catches up as well

# COMMAND ----------

# MAGIC %md
# MAGIC The rows are **kept**: `amount` is `NULL` and the original text sits in `_rescued_data` as JSON (with `_file_path`).
# MAGIC Silver logic can repair (`"12.50 EUR"` → 12.50) or quarantine these rows.
# MAGIC
# MAGIC ## Part 6 · The other schema evolution modes
# MAGIC
# MAGIC | `cloudFiles.schemaEvolutionMode` | New column appears → | Stream fails? |
# MAGIC |---|---|---|
# MAGIC | **`addNewColumns`** (default when Auto Loader infers the schema) | added to the schema | ✅ once, then restart continues |
# MAGIC | `addNewColumnsWithTypeWidening` | added; compatible types widened (e.g. int → long) | ✅ once |
# MAGIC | **`rescue`** | **not** added — its values go to `_rescued_data` | ❌ never fails on schema changes |
# MAGIC | **`failOnNewColumns`** | not added | ✅ fails until you update the schema (or hints) yourself |
# MAGIC | `none` | ignored (not rescued unless you set `rescuedDataColumn`) | ❌ (default when **you** provide the schema) |

# COMMAND ----------

# DBTITLE 1,The rescue-mode twin: schema frozen, coupon values rescued
rescue_df = spark.table("lab06_events_rescue")
rescue_has_coupon_col = "coupon" in rescue_df.columns
coupon_rescued = rescue_df.where("_rescued_data LIKE '%coupon%'").count()
print("rows:", rescue_df.count(), "| coupon column?", rescue_has_coupon_col,
      "| rows with coupon inside _rescued_data:", coupon_rescued)
display(rescue_df.where("_rescued_data IS NOT NULL").select("event_id", "source_file", "_rescued_data").limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC The twin never stopped and has **no** `coupon` column — the new field lives in `_rescued_data`, so nothing is lost and you
# MAGIC can promote it to a real column later. `rescue` is handy when the table schema must stay stable; `addNewColumns` (+ job
# MAGIC retries) is the usual choice for bronze tables that should follow the source.
# MAGIC
# MAGIC ## Part 7 · Batch size and existing files
# MAGIC With `availableNow`, Auto Loader still splits the work into **micro-batches**. `cloudFiles.maxFilesPerTrigger` (default 1000;
# MAGIC on DBR 18.0+ tuned dynamically unless you set it) and `cloudFiles.maxBytesPerTrigger` control their size.

# COMMAND ----------

# DBTITLE 1,maxFilesPerTrigger = 1 -> one micro-batch per file
batches_writer = (spark.readStream.format("cloudFiles")
                       .option("cloudFiles.format", "json")
                       .option("cloudFiles.schemaLocation", checkpoint("events_batches"))
                       .option("cloudFiles.maxFilesPerTrigger", 1)
                       .load(events_landing)
                       .writeStream.option("checkpointLocation", checkpoint("events_batches"))
                       .option("mergeSchema", "true"))
q_batches = run_stream(batches_writer, "lab06_events_batches")
n_data_batches = data_batches(q_batches)
print("micro-batches that read data:", n_data_batches)

# COMMAND ----------

# DBTITLE 1,includeExistingFiles = false -> only files that arrive AFTER the first start
new_only_writer = (spark.readStream.format("cloudFiles")
                        .option("cloudFiles.format", "json")
                        .option("cloudFiles.schemaLocation", checkpoint("events_new_only"))
                        .option("cloudFiles.includeExistingFiles", "false")
                        .load(events_landing)
                        .writeStream.option("checkpointLocation", checkpoint("events_new_only")))
new_only_rows, new_only_error = 0, False
try:
    run_stream(new_only_writer, "lab06_events_new_only")
    new_only_rows = spark.table("lab06_events_new_only").count() if spark.catalog.tableExists("lab06_events_new_only") else 0
except Exception as e:
    new_only_error = True
    print("ℹ️", (str(e).strip().splitlines() or [repr(e)])[0][:200])
print("rows loaded with includeExistingFiles=false:", new_only_rows, "(the 4 files were already there)")

# COMMAND ----------

# MAGIC %md
# MAGIC * `cloudFiles.includeExistingFiles` (default **true**) is evaluated only on the **first** start of a stream.
# MAGIC * Other useful options: `cloudFiles.schemaHints`, `cloudFiles.partitionColumns`, `pathGlobFilter`,
# MAGIC   `cloudFiles.cleanSource` (move or delete source files after processing), `cloudFiles.allowOverwrites`.
# MAGIC
# MAGIC ## Part 8 · How Auto Loader finds new files
# MAGIC
# MAGIC | Mode | How | When |
# MAGIC |---|---|---|
# MAGIC | **Directory listing** (default) | lists the whole input path each micro-batch (the old *incremental listing* option is deprecated) | simple, no setup — fine for small/medium folders; these labs use it |
# MAGIC | **File notification** — **with file events** (recommended for most workloads) | uses **file events** enabled on the external location (`cloudFiles.useManagedFileEvents`, DBR 14.3+) | more performant and scalable; no extra cloud permissions |
# MAGIC | File notification (legacy) | Auto Loader creates cloud queues/notifications (`cloudFiles.useNotifications`) | older setups; needs cloud permissions |
# MAGIC
# MAGIC **Unity Catalog permissions** an Auto Loader pipeline needs: `READ VOLUME` on the source volume (or `READ FILES` on the
# MAGIC external location), `WRITE VOLUME` where the checkpoint / schema location lives, and `USE CATALOG` + `USE SCHEMA` +
# MAGIC `CREATE TABLE` (first run) / `MODIFY` on the target table.
# MAGIC
# MAGIC Both give **exactly-once** processing: discovered files are tracked in the checkpoint (a RocksDB key-value store) — not by
# MAGIC listing the target table like `COPY INTO`. That's why Auto Loader scales to **millions** of files.
# MAGIC
# MAGIC **SQL flavour** — in a Lakeflow pipeline (Section 08) or Databricks SQL, a **streaming table** uses Auto Loader under the hood:
# MAGIC
# MAGIC ```sql
# MAGIC CREATE OR REFRESH STREAMING TABLE bronze_events
# MAGIC AS SELECT *, _metadata.file_name AS source_file
# MAGIC    FROM STREAM read_files('/Volumes/workspace/shopwave/raw/events-landing', format => 'json');
# MAGIC ```
# MAGIC
# MAGIC > 🕰️ Legacy DLT syntax you may still see: `cloud_files('/path', 'json')` = today's `STREAM read_files('/path', format => 'json')`.
# MAGIC
# MAGIC ## Part 9 · ✅ Automatic checks

# COMMAND ----------

# DBTITLE 1,Check your work
_bronze = spark.table("lab06_events_bronze")
_checks = {
    "default inference: every data column is a string": all(t == "string" for c, t in peek_types.items()
                                                             if c not in ("_rescued_data",)),
    "inferColumnTypes: amount is a double": dict(_bronze.dtypes).get("amount") == "double",
    "schemaHints: event_time is a timestamp": dict(_bronze.dtypes).get("event_time") == "timestamp",
    "batches 1-2 loaded (100 rows)": rows_b2 == 100,
    "new column stopped the stream once": evolution_failed,
    "after restart: coupon column + 150 rows": "coupon" in _bronze.columns and rows_b3 == 150,
    "batch 4 loaded (200 rows) with 3 rescued amounts": rows_b4 == 200 and rescued_rows == 3,
    f"maxFilesPerTrigger=1 -> several micro-batches with data (expected 4, got {n_data_batches})": n_data_batches >= 2,
    "includeExistingFiles=false skipped the existing files": new_only_rows == 0 and not new_only_error,
    "rescue mode never stopped, kept the schema, rescued coupon": (not rescue_failed and not rescue_has_coupon_col
                                                                   and coupon_rescued > 0),
}
for name, ok in _checks.items():
    print(("✅ " if ok else "❌ ") + name)
stop_all_streams()
print("\n🎉 Lab complete - next: 06-L3 · Windows, Watermarks & foreachBatch")
