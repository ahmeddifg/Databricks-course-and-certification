# Databricks notebook source
# MAGIC %md
# MAGIC # 🏆 Lab 07-L4 · Challenge Lab — Data Transformation & Modeling — ✅ SOLUTION
# MAGIC > Try the challenge yourself first! This notebook contains the reference answers and runs end-to-end.
# MAGIC
# MAGIC **Time:** ~60 min · **Compute:** Serverless · **Story:** ShopWave's logistics partner sends a daily **shipments**
# MAGIC export (`shipments-messy/`). It is as messy as the payments feed: carriers spelled three ways, two date formats, costs
# MAGIC like `"USD 9.99"` or `"N/A"`, re-sent records and exact duplicates. Take it from raw file to a **gold** table the
# MAGIC operations team can use.
# MAGIC
# MAGIC Write the code yourself (PySpark or SQL — your choice). Replace every `None` / `# TODO`, then run each **✅ Check**.
# MAGIC Everything you need was shown in 07-L1 … 07-L3.
# MAGIC
# MAGIC | Task | Skill |
# MAGIC |---|---|
# MAGIC | 1 | Bronze table from CSV + profiling (`count`, distinct ids) |
# MAGIC | 2 | Silver: standardise strings, parse **two date formats**, `try_cast`, keep the **latest** row per key |
# MAGIC | 3 | Data quality: **quarantine** rows that break the rules, with the reasons |
# MAGIC | 4 | Gold: **join** three tables and **aggregate** |
# MAGIC | 5 | **Window function**: the fastest delivery per carrier |
# MAGIC | 6–8 | 🧠 Concepts: unions, gold objects, SCD |

# COMMAND ----------

# MAGIC %run ../../Includes/_setup

# COMMAND ----------

# MAGIC %run ./_07_prepare

# COMMAND ----------

# DBTITLE 1,Setup for the challenge (run me - don't edit) - resets your challenge tables
for _t in ("lab07_ch_bronze_shipments", "lab07_ch_silver_shipments", "lab07_ch_shipments_valid",
           "lab07_ch_shipments_quarantine", "lab07_ch_gold_carrier_country"):
    spark.sql(f"DROP TABLE IF EXISTS {_t}")
build_silver_tables()                                   # lab07_silver_orders / lab07_silver_customers are needed in Task 4
SHIPMENTS_PATH = f"{dataset_path}/shipments-messy"
print("SHIPMENTS_PATH =", SHIPMENTS_PATH)
print(dbutils.fs.head(f"{SHIPMENTS_PATH}/shipments_export.csv", 400))

raw_rows = raw_distinct_ids = fastest = None
answer_task6 = answer_task7 = answer_task8 = None


def check(label, condition):
    print(("✅ " if condition else "❌ ") + label)


def _exists(t):
    return spark.catalog.tableExists(t)


def _count(t):
    return spark.table(t).count() if _exists(t) else -1


def _ref_silver():
    """Reference cleaning used by the checks."""
    b = spark.read.option("header", True).csv(SHIPMENTS_PATH)
    shipped = F.coalesce(F.expr("try_to_timestamp(shipped_at, 'yyyy-MM-dd HH:mm:ss')"),
                         F.expr("try_to_timestamp(shipped_at, 'dd/MM/yyyy HH:mm')"))
    s = b.select("shipment_id", "order_id", F.upper(F.trim("carrier")).alias("carrier"), shipped.alias("shipped_at"),
                 F.to_timestamp("delivered_at").alias("delivered_at"),
                 F.expr("try_cast(regexp_replace(trim(cost), '[^0-9.-]', '') AS DECIMAL(10,2))").alias("cost"),
                 F.to_timestamp("updated_at").alias("updated_at"))
    return latest_per_key(s, "shipment_id", "updated_at")


_hours = (F.unix_timestamp("delivered_at") - F.unix_timestamp("shipped_at")) / 3600
_ref_valid = _ref_silver().where("cost IS NOT NULL AND NOT coalesce(delivered_at < shipped_at, false)")
_cols4 = ["carrier", "country", "shipments", "delivered", "avg_delivery_hours"]
_ref4 = (_ref_valid.join(spark.table("lab07_silver_orders").select("order_id", "customer_id"), "order_id")
                   .join(spark.table("lab07_silver_customers").select("customer_id", "country"), "customer_id")
                   .groupBy("carrier", "country")
                   .agg(F.count("*").alias("shipments"), F.count("delivered_at").alias("delivered"),
                        F.round(F.avg(_hours), 1).alias("avg_delivery_hours")))
_ref5 = {r["carrier"]: r["shipment_id"] for r in
         _ref_valid.where("delivered_at IS NOT NULL")
                   .withColumn("rn", F.row_number().over(Window.partitionBy("carrier").orderBy(_hours.asc(), "shipment_id")))
                   .where("rn = 1").collect()}


def _task4_ok():
    if not _exists("lab07_ch_gold_carrier_country"):
        return False
    g = spark.table("lab07_ch_gold_carrier_country")
    if not set(_cols4) <= set(g.columns) or g.count() != _ref4.count():
        return False
    return g.select(*[F.col(c).cast("string") for c in _cols4]).exceptAll(
        _ref4.select(*[F.col(c).cast("string") for c in _cols4])).count() == 0

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 1 · Bronze + profile
# MAGIC Read every CSV file in `SHIPMENTS_PATH` **with the header and without type inference** (all columns stay strings), add a
# MAGIC column **`source_file`** with the file name, and save it as **`lab07_ch_bronze_shipments`**.
# MAGIC
# MAGIC Then store the number of rows in `raw_rows` and the number of **distinct** `shipment_id` values in `raw_distinct_ids`.
# MAGIC
# MAGIC > Hints · PySpark: `spark.read.option("header", True).csv(...)` · SQL: `read_files(..., format => 'csv', header => true,
# MAGIC > inferColumnTypes => false)` · file name: `_metadata.file_name` (`input_file_name()` is not available on Unity Catalog).
# MAGIC > Extra ingestion columns starting with `_` (e.g. `_ingested_at`) are allowed.

# COMMAND ----------

# DBTITLE 1,Task 1 · SOLUTION
(spark.read.option("header", True).csv(SHIPMENTS_PATH)          # no inferSchema -> every column is a STRING
      .withColumn("source_file", F.col("_metadata.file_name"))
      .write.mode("overwrite").saveAsTable("lab07_ch_bronze_shipments"))

bronze_shipments = spark.table("lab07_ch_bronze_shipments")
raw_rows = bronze_shipments.count()
raw_distinct_ids = bronze_shipments.select(F.count_distinct("shipment_id")).first()[0]
print(raw_rows, raw_distinct_ids)

# COMMAND ----------

# DBTITLE 1,✅ Check 1
check("bronze table has 318 rows", _count("lab07_ch_bronze_shipments") == 318)
check("every business column is a STRING (no type inference)", _exists("lab07_ch_bronze_shipments") and all(
    t == "string" for c, t in spark.table("lab07_ch_bronze_shipments").dtypes if c != "source_file" and not c.startswith("_")))
check("source_file column present", _exists("lab07_ch_bronze_shipments") and "source_file" in spark.table("lab07_ch_bronze_shipments").columns)
check("raw_rows = 318 and raw_distinct_ids = 300", (raw_rows, raw_distinct_ids) == (318, 300))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 2 · Silver: clean and deduplicate
# MAGIC Create **`lab07_ch_silver_shipments`** from the bronze table with exactly these columns:
# MAGIC
# MAGIC | Column | Rule |
# MAGIC |---|---|
# MAGIC | `shipment_id`, `order_id` | as is |
# MAGIC | `carrier` | trimmed, UPPER case → only `DHL`, `ARAMEX`, `FEDEX` |
# MAGIC | `shipped_at` | TIMESTAMP — the feed uses `yyyy-MM-dd HH:mm:ss` **and** `dd/MM/yyyy HH:mm` |
# MAGIC | `delivered_at` | TIMESTAMP (NULL = still in transit) |
# MAGIC | `cost` | `DECIMAL(10,2)`; values like `" 7.00"` or `"USD 9.99"` must become numbers, `"N/A"` → NULL |
# MAGIC | `updated_at` | TIMESTAMP |
# MAGIC
# MAGIC Keep **one row per `shipment_id`: the one with the latest `updated_at`** (re-sent records carry the delivery date).

# COMMAND ----------

# DBTITLE 1,Task 2 · SOLUTION
shipped_clean = F.coalesce(F.expr("try_to_timestamp(shipped_at, 'yyyy-MM-dd HH:mm:ss')"),
                           F.expr("try_to_timestamp(shipped_at, 'dd/MM/yyyy HH:mm')"))
cost_clean = F.expr("try_cast(regexp_replace(trim(cost), '[^0-9.-]', '') AS DECIMAL(10,2))")

cleaned = spark.table("lab07_ch_bronze_shipments").select(
    "shipment_id", "order_id",
    F.upper(F.trim("carrier")).alias("carrier"),
    shipped_clean.alias("shipped_at"),
    F.to_timestamp("delivered_at").alias("delivered_at"),
    cost_clean.alias("cost"),
    F.to_timestamp("updated_at").alias("updated_at"))

latest = Window.partitionBy("shipment_id").orderBy(F.col("updated_at").desc())
(cleaned.withColumn("rn", F.row_number().over(latest))
        .where("rn = 1").drop("rn")
        .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("lab07_ch_silver_shipments"))
# dropDuplicates(["shipment_id"]) would keep an ARBITRARY version - some shipments would lose their delivery date.

# COMMAND ----------

# DBTITLE 1,✅ Check 2
_s = spark.table("lab07_ch_silver_shipments") if _exists("lab07_ch_silver_shipments") else None
check("300 rows, one per shipment_id", _s is not None and _s.count() == 300 and _s.select("shipment_id").distinct().count() == 300)
check("carriers standardised to DHL / ARAMEX / FEDEX", _s is not None and sorted(
    r[0] for r in _s.select("carrier").distinct().collect()) == ["ARAMEX", "DHL", "FEDEX"])
check("shipped_at parsed for every row (both formats)", _s is not None and dict(_s.dtypes).get("shipped_at") == "timestamp"
      and _s.where("shipped_at IS NULL").count() == 0)
check("cost is DECIMAL(10,2) with 4 NULLs (N/A)", _s is not None and dict(_s.dtypes).get("cost") == "decimal(10,2)"
      and _s.where("cost IS NULL").count() == 4)
check("latest version kept: 20 shipments still in transit", _s is not None and _s.where("delivered_at IS NULL").count() == 20)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 3 · Data quality: quarantine
# MAGIC Two rules — a row breaks a rule when:
# MAGIC * **`cost_missing`**: `cost` is NULL
# MAGIC * **`delivered_before_shipped`**: `delivered_at` is earlier than `shipped_at`
# MAGIC
# MAGIC Split the silver rows into **`lab07_ch_shipments_valid`** (no rule broken) and **`lab07_ch_shipments_quarantine`**
# MAGIC (at least one rule broken) with an extra column **`dq_errors`**: an array with the **names** of the broken rules.
# MAGIC In-transit shipments (NULL `delivered_at`) are valid.

# COMMAND ----------

# DBTITLE 1,Task 3 · SOLUTION
ship_rules = {
    "cost_missing": F.col("cost").isNull(),
    "delivered_before_shipped": F.col("delivered_at") < F.col("shipped_at"),   # NULL (in transit) -> not flagged
}
tagged = spark.table("lab07_ch_silver_shipments").withColumn(
    "dq_errors", F.array_compact(F.array(*[F.when(c, F.lit(n)) for n, c in ship_rules.items()])))
(tagged.where(F.size("dq_errors") == 0).drop("dq_errors")
       .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("lab07_ch_shipments_valid"))
(tagged.where(F.size("dq_errors") > 0)
       .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("lab07_ch_shipments_quarantine"))

# COMMAND ----------

# DBTITLE 1,✅ Check 3
check("291 valid + 9 quarantined = 300", (_count("lab07_ch_shipments_valid"), _count("lab07_ch_shipments_quarantine")) == (291, 9))
_q = spark.table("lab07_ch_shipments_quarantine") if _exists("lab07_ch_shipments_quarantine") else None
check("dq_errors names the rules (4 cost_missing, 5 delivered_before_shipped)", _q is not None and "dq_errors" in _q.columns
      and _q.where("array_contains(dq_errors, 'cost_missing')").count() == 4
      and _q.where("array_contains(dq_errors, 'delivered_before_shipped')").count() == 5)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 4 · Gold: delivery performance per carrier and country
# MAGIC Build **`lab07_ch_gold_carrier_country`** from the **valid** shipments, joined (inner joins) to `lab07_silver_orders`
# MAGIC (to get `customer_id`) and `lab07_silver_customers` (to get `country`). One row per `carrier`, `country` with:
# MAGIC
# MAGIC | Column | Definition |
# MAGIC |---|---|
# MAGIC | `shipments` | number of shipments |
# MAGIC | `delivered` | number of shipments with a `delivered_at` |
# MAGIC | `avg_delivery_hours` | average of `(unix_timestamp(delivered_at) - unix_timestamp(shipped_at)) / 3600` over delivered shipments, **rounded to 1 decimal** |

# COMMAND ----------

# DBTITLE 1,Task 4 · SOLUTION
delivery_hours = (F.unix_timestamp("delivered_at") - F.unix_timestamp("shipped_at")) / 3600
gold = (spark.table("lab07_ch_shipments_valid")
             .join(spark.table("lab07_silver_orders").select("order_id", "customer_id"), "order_id")
             .join(spark.table("lab07_silver_customers").select("customer_id", "country"), "customer_id")
             .groupBy("carrier", "country")
             .agg(F.count("*").alias("shipments"),
                  F.count("delivered_at").alias("delivered"),              # count(col) skips NULLs
                  F.round(F.avg(delivery_hours), 1).alias("avg_delivery_hours")))   # avg skips NULLs too
gold.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("lab07_ch_gold_carrier_country")
display(spark.table("lab07_ch_gold_carrier_country").orderBy("carrier", "country"))

# COMMAND ----------

# DBTITLE 1,✅ Check 4
check(f"gold table matches the expected {_ref4.count()} carrier/country rows", _task4_ok())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 5 · Fastest delivery per carrier
# MAGIC Using a **window function** on `lab07_ch_shipments_valid`, find for each carrier the delivered shipment with the
# MAGIC **shortest** delivery time (`delivered_at - shipped_at`; on a tie, the smaller `shipment_id`). Store the result as a
# MAGIC Python dict in `fastest`, e.g.
# MAGIC `{"DHL": "SHP00042", "ARAMEX": "…", "FEDEX": "…"}`.

# COMMAND ----------

# DBTITLE 1,Task 5 · SOLUTION
by_speed = Window.partitionBy("carrier").orderBy(
    (F.unix_timestamp("delivered_at") - F.unix_timestamp("shipped_at")).asc(), F.col("shipment_id"))   # tie-break
fastest_rows = (spark.table("lab07_ch_shipments_valid")
                     .where("delivered_at IS NOT NULL")
                     .withColumn("rn", F.row_number().over(by_speed))
                     .where("rn = 1")
                     .collect())
fastest = {r["carrier"]: r["shipment_id"] for r in fastest_rows}
print(fastest)

# COMMAND ----------

# DBTITLE 1,✅ Check 5
check("fastest shipment per carrier", fastest == _ref5)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 6 · 🧠 Unions
# MAGIC `a` has columns `(shipment_id STRING, carrier STRING)`; `b` has the **same columns in the opposite order**
# MAGIC `(carrier STRING, shipment_id STRING)`. What does `a.union(b)` return?
# MAGIC
# MAGIC * **A** — an error, because the column order differs
# MAGIC * **B** — the rows stacked, matching columns **by name**, duplicates removed
# MAGIC * **C** — the rows stacked **by position**: `b`'s carriers end up in the `shipment_id` column, duplicates kept
# MAGIC * **D** — only the rows present in both DataFrames

# COMMAND ----------

# DBTITLE 1,Task 6 · SOLUTION
answer_task6 = "C"
# union() = UNION ALL by POSITION. Both columns are strings, so there is no error - the values are silently mixed.
# Use a.unionByName(b) to match by name (and .distinct() if duplicates must go).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 7 · 🧠 Gold objects
# MAGIC Dashboards query "revenue per country and month" many times an hour. The source tables change a few times a day, and
# MAGIC the aggregation is expensive. The team wants the result **precomputed** and kept up to date **incrementally where
# MAGIC possible**, with as little custom code as possible. Which gold object fits best?
# MAGIC
# MAGIC * **A** — a view
# MAGIC * **B** — a materialized view
# MAGIC * **C** — a temporary view created by the dashboard
# MAGIC * **D** — a Python UDF called by the dashboard

# COMMAND ----------

# DBTITLE 1,Task 7 · SOLUTION
answer_task7 = "B"
# A materialized view stores the precomputed result and refreshes (incrementally when possible) on a schedule or
# REFRESH. A view recomputes the expensive query on every dashboard load.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 8 · 🧠 Slowly changing dimensions
# MAGIC Finance must report every shipment under the **country the customer lived in when the order was placed**, even after
# MAGIC customers move. How should `dim_customer` be maintained?
# MAGIC
# MAGIC * **A** — SCD type 1: overwrite the row when the customer moves
# MAGIC * **B** — `dropDuplicates(["customer_id"])` on every load
# MAGIC * **C** — `INSERT OVERWRITE` of the latest customer export every night
# MAGIC * **D** — SCD type 2: close the old row (`valid_to`, `is_current = false`) and insert a new version

# COMMAND ----------

# DBTITLE 1,Task 8 · SOLUTION
answer_task8 = "D"
# SCD type 2 keeps every version with its validity period -> a point-in-time join finds the country at order time.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🏁 Final score

# COMMAND ----------

# DBTITLE 1,Score
_final = {
    "Task 1": _count("lab07_ch_bronze_shipments") == 318 and (raw_rows, raw_distinct_ids) == (318, 300),
    "Task 2": _exists("lab07_ch_silver_shipments") and spark.table("lab07_ch_silver_shipments").count() == 300
              and spark.table("lab07_ch_silver_shipments").where("delivered_at IS NULL").count() == 20,
    "Task 3": (_count("lab07_ch_shipments_valid"), _count("lab07_ch_shipments_quarantine")) == (291, 9),
    "Task 4": _task4_ok(),
    "Task 5": fastest == _ref5,
    "Task 6": answer_task6 == "C",
    "Task 7": answer_task7 == "B",
    "Task 8": answer_task8 == "D",
}
for k, v in _final.items():
    print(("✅ " if v else "❌ ") + k)
print(f"\nScore: {sum(_final.values())} / {len(_final)}")
