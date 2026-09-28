# Databricks notebook source
# MAGIC %md
# MAGIC # 🧪 Lab 07-L3 · Gold Modeling, SCD and Data Quality (Guided)
# MAGIC **Time:** ~70 min · **Compute:** Serverless (Free Edition) or any Unity Catalog compute
# MAGIC
# MAGIC Silver is clean and typed — but is it **correct**? And how should the business consume it? In this lab you apply
# MAGIC **data quality rules** (quarantine, Delta constraints), model a small **star schema** with a **slowly changing
# MAGIC dimension** (SCD type 1 and type 2), and publish **gold objects** — table, view, materialized view — for BI teams.
# MAGIC
# MAGIC | Part | You will… |
# MAGIC |---|---|
# MAGIC | 1 | Write **data quality rules**, build a DQ report and **quarantine** failing rows |
# MAGIC | 2 | Enforce rules in the table itself with **Delta constraints** (`CHECK`, `NOT NULL`) |
# MAGIC | 3 | Build a **star schema**: date and product dimensions, a sales **fact** table |
# MAGIC | 4 | Track customer changes with **SCD type 1** (overwrite) and **SCD type 2** (history) using `MERGE` |
# MAGIC | 5 | Publish **gold objects**: table, view, materialized view — and know when to use a streaming table |
# MAGIC | 6 | **UDFs**: SQL UDF vs Python UDF vs built-in functions |
# MAGIC | 7 | ✅ Automatic checks |

# COMMAND ----------

# MAGIC %run ../../Includes/_setup

# COMMAND ----------

# MAGIC %run ./_07_prepare

# COMMAND ----------

# DBTITLE 1,Start clean (safe to re-run)
build_silver_tables()                                   # silver tables from 07-L1 (rebuilt if you skipped it)
for t in ("lab07_payments_valid", "lab07_payments_quarantine", "lab07_dim_date", "lab07_dim_product", "lab07_fact_sales",
          "lab07_dim_customer_scd1", "lab07_dim_customer_scd2", "lab07_gold_daily_sales", "lab07_gold_sales_wide"):
    spark.sql(f"DROP TABLE IF EXISTS {t}")
spark.sql("DROP VIEW IF EXISTS lab07_gold_customer_360_v")
try:
    spark.sql("DROP MATERIALIZED VIEW IF EXISTS lab07_gold_country_revenue_mv")
except Exception:
    pass                                                # materialized views not supported on this compute
spark.sql("DROP FUNCTION IF EXISTS lab07_clean_method")
print("silver payments:", spark.table("lab07_silver_payments").count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1 · Data quality rules and quarantine
# MAGIC A **rule** is a condition every good row must satisfy. What to do with a row that breaks it is a business decision:
# MAGIC
# MAGIC | Strategy | When | Example |
# MAGIC |---|---|---|
# MAGIC | **Fix** | the correct value is known | NULL currency → `USD` (07-L1) |
# MAGIC | **Drop** | the row is useless and nobody needs to know | a payment without `payment_id` |
# MAGIC | **Quarantine** | the row may be fixable — keep it **aside** with the reason | negative amount, unknown order |
# MAGIC | **Flag / warn** | keep the row, but mark or count it | `dq_errors` column, metrics |
# MAGIC | **Fail** | bad data must stop the pipeline | duplicate primary keys in a finance table |

# COMMAND ----------

# DBTITLE 1,Define the rules (a failed rule = the condition is TRUE)
payments = spark.table("lab07_silver_payments")
known_orders = spark.table("lab07_silver_orders").select("order_id", F.lit(True).alias("_order_exists"))
checked = payments.join(known_orders, "order_id", "left")          # left join: keep payments without an order

rules = {
    "amount_missing":  F.col("amount").isNull(),
    "amount_negative": F.col("amount") < 0,
    "paid_at_missing": F.col("paid_at").isNull(),
    "method_missing":  F.col("method").isNull(),
    "unknown_order":   F.col("_order_exists").isNull(),
    "status_invalid":  ~F.col("status").isin("SUCCESS", "FAILED", "REFUNDED"),
}

# COMMAND ----------

# DBTITLE 1,Data quality report: failures per rule
dq_report = checked.agg(*[F.sum(F.when(cond, 1).otherwise(0)).alias(name) for name, cond in rules.items()])
display(dq_report)
dq_counts = dq_report.first().asDict()
print(dq_counts)

# COMMAND ----------

# MAGIC %md
# MAGIC You should see **3** missing amounts, **2** negative amounts, **7** missing `paid_at`, **7** missing methods and **3**
# MAGIC payments for orders that don't exist. `status_invalid` = 0 is a **passing** rule — worth keeping: it will catch a new
# MAGIC status value the day it appears.
# MAGIC
# MAGIC Now tag every row with the **list of rules it breaks**. `array_compact` removes the NULLs that `when` produces for
# MAGIC rules that pass, so a clean row gets an **empty** array:

# COMMAND ----------

# DBTITLE 1,Tag rows and split: valid vs quarantine
tagged = checked.withColumn(
    "dq_errors",
    F.array_compact(F.array(*[F.when(cond, F.lit(name)) for name, cond in rules.items()]))).drop("_order_exists")

valid = tagged.where(F.size("dq_errors") == 0).drop("dq_errors")
quarantine = tagged.where(F.size("dq_errors") > 0).withColumn("quarantined_at", F.current_timestamp())

valid.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("lab07_payments_valid")
quarantine.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("lab07_payments_quarantine")
n_valid, n_quarantine = spark.table("lab07_payments_valid").count(), spark.table("lab07_payments_quarantine").count()
print(f"valid: {n_valid} | quarantine: {n_quarantine} | total: {n_valid + n_quarantine} (silver: {payments.count()})")
display(spark.table("lab07_payments_quarantine").select("payment_id", "order_id", "amount", "paid_at", "method", "dq_errors"))

# COMMAND ----------

# MAGIC %md
# MAGIC > 🎯 A good quarantine keeps **every** row somewhere: `valid + quarantine = input`. Nothing silently disappears, and the
# MAGIC > `dq_errors` array tells the data owner exactly what to fix. Lakeflow pipelines do this declaratively with
# MAGIC > **expectations** (`EXPECT … ON VIOLATION DROP ROW / FAIL UPDATE`, or just warn) — Section 08.
# MAGIC
# MAGIC ## Part 2 · Delta constraints: the table refuses bad data
# MAGIC Rules in your code protect **your** pipeline. **Constraints** protect the **table** from every writer:
# MAGIC
# MAGIC | Constraint | Syntax | Checked |
# MAGIC |---|---|---|
# MAGIC | `NOT NULL` | `ALTER TABLE t ALTER COLUMN c SET NOT NULL` (or in `CREATE TABLE`) | on every write |
# MAGIC | `CHECK` | `ALTER TABLE t ADD CONSTRAINT name CHECK (condition)` | on every write — and against **existing rows** when added |
# MAGIC
# MAGIC A write that violates a constraint **fails as a whole** (the transaction is rolled back — no partial writes).

# COMMAND ----------

# DBTITLE 1,Add constraints to the valid table
run_sql("ALTER TABLE lab07_payments_valid DROP CONSTRAINT IF EXISTS amount_non_negative")   # re-run safe
run_sql("ALTER TABLE lab07_payments_valid ALTER COLUMN payment_id SET NOT NULL")
run_sql("ALTER TABLE lab07_payments_valid ADD CONSTRAINT amount_non_negative CHECK (amount >= 0)")
display(run_sql("SHOW TBLPROPERTIES lab07_payments_valid").where("key LIKE 'delta.constraints%'"))   # CHECK constraints
spark.table("lab07_payments_valid").select("payment_id").printSchema()      # NOT NULL shows as nullable = false

# COMMAND ----------

# DBTITLE 1,A bad insert is rejected
bad_insert_rejected = False
try:
    run_sql("""
        INSERT INTO lab07_payments_valid (order_id, payment_id, method, amount, currency, paid_at, status, updated_at)
        VALUES ('O000001', 'PAY99999', 'card', -10.00, 'USD', current_timestamp(), 'SUCCESS', current_timestamp())""")
except Exception as e:
    bad_insert_rejected = True
    print("🚫 Expected:", (str(e).strip().splitlines() or [repr(e)])[0][:220])

# COMMAND ----------

# DBTITLE 1,A constraint can't be added while existing rows break it
existing_rows_block = False
try:
    run_sql("ALTER TABLE lab07_silver_payments ADD CONSTRAINT amount_non_negative CHECK (amount >= 0)")
except Exception as e:
    existing_rows_block = True
    print("🚫 Expected (silver still has 2 negative amounts):", (str(e).strip().splitlines() or [repr(e)])[0][:200])

# COMMAND ----------

# MAGIC %md
# MAGIC > ℹ️ Other constraint types in Unity Catalog — `PRIMARY KEY` / `FOREIGN KEY` — are **informational** (not enforced).
# MAGIC > They document the model for BI tools; the optimizer uses them only when declared with `RELY`.
# MAGIC
# MAGIC ## Part 3 · A star schema for sales
# MAGIC A **star schema** puts the measurable events in a **fact** table (one row per order line: quantity, amount) surrounded by
# MAGIC **dimensions** that describe them (date, product, customer). BI tools and humans understand it at a glance, and joins
# MAGIC follow simple keys.

# COMMAND ----------

# DBTITLE 1,dim_date: one row per day, generated with sequence() + explode()
dim_date = (spark.sql("SELECT explode(sequence(DATE'2026-01-01', DATE'2026-06-30', INTERVAL 1 DAY)) AS date")
                 .select(F.date_format("date", "yyyyMMdd").cast("int").alias("date_key"), "date",
                         F.year("date").alias("year"), F.month("date").alias("month"),
                         F.date_format("date", "yyyy-MM").alias("year_month"),
                         F.date_format("date", "E").alias("day_name"),
                         F.dayofweek("date").isin(1, 7).alias("is_weekend")))
dim_date.write.mode("overwrite").saveAsTable("lab07_dim_date")
print("dim_date days:", spark.table("lab07_dim_date").count())

# COMMAND ----------

# DBTITLE 1,dim_product and the sales fact table
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE lab07_dim_product AS
# MAGIC SELECT product_id, title, brand, category, price
# MAGIC FROM lab07_silver_products;
# MAGIC
# MAGIC CREATE OR REPLACE TABLE lab07_fact_sales AS
# MAGIC SELECT i.order_id,
# MAGIC        i.line_no,
# MAGIC        CAST(date_format(o.order_date, 'yyyyMMdd') AS INT) AS date_key,
# MAGIC        o.order_ts,
# MAGIC        o.customer_id,
# MAGIC        i.product_id,
# MAGIC        i.quantity,
# MAGIC        i.subtotal                                         AS amount
# MAGIC FROM lab07_silver_order_items i
# MAGIC JOIN lab07_silver_orders o ON i.order_id = o.order_id

# COMMAND ----------

# DBTITLE 1,Query the star: revenue per category and weekday vs weekend
star = (spark.table("lab07_fact_sales")
             .join(spark.table("lab07_dim_date"), "date_key")
             .join(spark.table("lab07_dim_product"), "product_id")
             .groupBy("category", "is_weekend")
             .agg(F.round(F.sum("amount"), 0).alias("revenue"), F.count_distinct("order_id").alias("orders")))
display(star.orderBy("category", "is_weekend"))
n_fact = spark.table("lab07_fact_sales").count()
print("fact rows:", n_fact)

# COMMAND ----------

# MAGIC %md
# MAGIC > 💡 The fact table still contains the `C9999` orders (their customer is unknown). Star schemas usually give every
# MAGIC > dimension an **"unknown" member** (e.g. `customer_id = 'UNKNOWN'`) so facts are never dropped by an inner join. A
# MAGIC > **denormalized** ("one big table") gold table is the other common style: all attributes pre-joined — simpler for BI, more
# MAGIC > storage, harder to keep consistent. You'll build one in Part 5.
# MAGIC
# MAGIC ## Part 4 · Slowly changing dimensions: SCD type 1 vs type 2
# MAGIC Customers move and change e-mail addresses. Two ways to reflect that in `dim_customer`:
# MAGIC
# MAGIC | | **SCD type 1** | **SCD type 2** |
# MAGIC |---|---|---|
# MAGIC | On a change | **overwrite** the row | **close** the current row (`valid_to`, `is_current = false`) and **insert** a new version |
# MAGIC | History | lost | kept — one row per version |
# MAGIC | Rows per customer | 1 | 1 + number of changes |
# MAGIC | Use when | corrections, attributes nobody analyses over time | reports must use the value **valid at the time** (country at order time) |
# MAGIC
# MAGIC Two batches of changes arrive as CSV files in `customer-updates/`: **2026-04-01** (25 rows) and **2026-06-01**
# MAGIC (27 rows: changes, 5 **new** customers and 2 rows that change **nothing**).

# COMMAND ----------

# DBTITLE 1,Initial load of both dimensions from silver
base = spark.table("lab07_silver_customers").select("customer_id", "first_name", "last_name", "email", "city", "country")

(base.withColumn("updated_at", F.lit("2026-01-01 00:00:00").cast("timestamp"))
     .write.mode("overwrite").saveAsTable("lab07_dim_customer_scd1"))

(base.withColumn("valid_from", F.lit("2026-01-01 00:00:00").cast("timestamp"))
     .withColumn("valid_to", F.lit(None).cast("timestamp"))
     .withColumn("is_current", F.lit(True))
     .write.mode("overwrite").saveAsTable("lab07_dim_customer_scd2"))
print("scd1:", spark.table("lab07_dim_customer_scd1").count(), "| scd2:", spark.table("lab07_dim_customer_scd2").count())
print("⚠️ Run Part 4 in order. To redo any step, re-run from THIS cell - it resets both dimensions.")


def load_updates(file_name):
    """Read one update batch into the temp view `customer_updates` (typed)."""
    df = (spark.read.option("header", True).csv(f"{dataset_path}/customer-updates/{file_name}")
               .withColumn("updated_at", F.to_timestamp("updated_at")))
    df.createOrReplaceTempView("customer_updates")
    return df

# COMMAND ----------

# DBTITLE 1,Batch 1 (2026-04-01): which rows really changed? Beware of NULLs
updates1 = load_updates("updates_2026_04_01.csv")
compare = run_sql("""
    SELECT count_if(d.email <> u.email OR d.city <> u.city OR d.country <> u.country)               AS changed_with_neq,
           count_if(NOT (d.email <=> u.email) OR NOT (d.city <=> u.city) OR NOT (d.country <=> u.country)) AS changed_null_safe
    FROM customer_updates u JOIN lab07_silver_customers d ON u.customer_id = d.customer_id""").first()
print(compare)

# COMMAND ----------

# MAGIC %md
# MAGIC One customer had **no e-mail** (NULL) and now has one. `NULL <> 'x'` is **NULL** (not TRUE), so the `<>` version
# MAGIC **misses** that change. The **null-safe** equality operator `<=>` treats NULLs as comparable values
# MAGIC (`NULL <=> NULL` is TRUE, `NULL <=> 'x'` is FALSE) — always use it (or `IS DISTINCT FROM`) to detect changes.
# MAGIC
# MAGIC ### SCD type 1 — one `MERGE`, overwrite in place

# COMMAND ----------

# DBTITLE 1,SCD1 MERGE (batch 1)
load_updates("updates_2026_04_01.csv")                  # make sure the view holds batch 1
SCD1_MERGE = """
MERGE INTO lab07_dim_customer_scd1 AS t
USING customer_updates AS s
ON t.customer_id = s.customer_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *"""
run_sql(SCD1_MERGE)
scd1_after_b1 = spark.table("lab07_dim_customer_scd1").count()
print("scd1 rows:", scd1_after_b1)

# COMMAND ----------

# MAGIC %md
# MAGIC ### SCD type 2 — the "merge key" trick
# MAGIC For a changed customer, one `MERGE` must do **two** things: **update** the current row (close it) **and insert** the new
# MAGIC version. A MERGE can only do one action per source row, so we feed each changed customer **twice**:
# MAGIC
# MAGIC * with `merge_key = customer_id` → **matches** the current row → `UPDATE` closes it;
# MAGIC * with `merge_key = NULL` → **never matches** → `INSERT` the new version.
# MAGIC
# MAGIC New customers appear only once (no current row → `INSERT`); unchanged customers match but fail the change condition → nothing happens.

# COMMAND ----------

# DBTITLE 1,SCD2 MERGE (batch 1)
load_updates("updates_2026_04_01.csv")                  # make sure the view holds batch 1
CHANGED = "NOT (t.email <=> s.email) OR NOT (t.city <=> s.city) OR NOT (t.country <=> s.country)"
SCD2_MERGE = f"""
MERGE INTO lab07_dim_customer_scd2 AS t
USING (
  SELECT u.customer_id AS merge_key, u.* FROM customer_updates u
  UNION ALL
  SELECT NULL AS merge_key, u.*
  FROM customer_updates u
  JOIN lab07_dim_customer_scd2 d ON u.customer_id = d.customer_id AND d.is_current
  WHERE NOT (d.email <=> u.email) OR NOT (d.city <=> u.city) OR NOT (d.country <=> u.country)
) AS s
ON t.customer_id = s.merge_key AND t.is_current
WHEN MATCHED AND ({CHANGED}) THEN
  UPDATE SET is_current = false, valid_to = s.updated_at
WHEN NOT MATCHED THEN
  INSERT (customer_id, first_name, last_name, email, city, country, valid_from, valid_to, is_current)
  VALUES (s.customer_id, s.first_name, s.last_name, s.email, s.city, s.country, s.updated_at, NULL, true)"""
run_sql(SCD2_MERGE)
scd2 = spark.table("lab07_dim_customer_scd2")
scd2_after_b1 = (scd2.count(), scd2.where("is_current").count())
print("scd2 rows / current rows:", scd2_after_b1)

# COMMAND ----------

# DBTITLE 1,Batch 2 (2026-06-01): changes, new customers, no-ops - same two MERGEs
updates2 = load_updates("updates_2026_06_01.csv")
run_sql(SCD1_MERGE)
run_sql(SCD2_MERGE)
scd1_after_b2 = spark.table("lab07_dim_customer_scd1").count()
scd2 = spark.table("lab07_dim_customer_scd2")
scd2_after_b2 = (scd2.count(), scd2.where("is_current").count())
print("scd1 rows:", scd1_after_b2, "| scd2 rows / current rows:", scd2_after_b2)

# COMMAND ----------

# DBTITLE 1,History of a customer who moved twice
moved_twice = (scd2.groupBy("customer_id").count().where("count = 3").orderBy("customer_id").first()["customer_id"])
display(scd2.where(F.col("customer_id") == moved_twice).orderBy("valid_from"))
display(spark.table("lab07_dim_customer_scd1").where(F.col("customer_id") == moved_twice))

# COMMAND ----------

# MAGIC %md
# MAGIC SCD1 only knows where the customer lives **now**; SCD2 knows every address and **when** it was valid.
# MAGIC
# MAGIC ### Using SCD2: the country **at the time of the order** (point-in-time join)

# COMMAND ----------

# DBTITLE 1,Point-in-time join: fact -> the dimension version valid at order time
f = spark.table("lab07_fact_sales").alias("f")
d = spark.table("lab07_dim_customer_scd2").alias("d")
as_of = f.join(d, (F.col("f.customer_id") == F.col("d.customer_id"))
                  & (F.col("f.order_ts") >= F.col("d.valid_from"))
                  & (F.col("d.valid_to").isNull() | (F.col("f.order_ts") < F.col("d.valid_to"))))
current = spark.table("lab07_dim_customer_scd2").where("is_current").select("customer_id", F.col("country").alias("current_country"))
compare_country = (as_of.select("f.order_id", "f.line_no", "f.customer_id", F.col("d.country").alias("country_at_order"))
                        .join(current, "customer_id"))
lines_other_country = compare_country.where("country_at_order <> current_country").count()
print("fact lines of known customers:", as_of.count(), "| booked in a country the customer has since left:", lines_other_country)

# COMMAND ----------

# MAGIC %md
# MAGIC > 🎯 Exam wording: "keep a full history of changes to an attribute" → **SCD type 2**; "only the latest value matters" →
# MAGIC > **SCD type 1**. In Lakeflow pipelines, `AUTO CDC … STORED AS SCD TYPE 2` (formerly `APPLY CHANGES INTO`) builds
# MAGIC > both types **without** hand-written MERGEs — Section 08.
# MAGIC
# MAGIC ## Part 5 · Gold objects for BI and analytics
# MAGIC Which Unity Catalog object should a gold dataset be?
# MAGIC
# MAGIC | Object | Stores data? | Freshness | Best for |
# MAGIC |---|---|---|---|
# MAGIC | **Table** (Delta) | ✅ | when **your job** rewrites it | full control, complex logic, any writer |
# MAGIC | **View** | ❌ — just a saved query | always current (computed at query time) | light logic, security (hide columns/rows), no storage |
# MAGIC | **Materialized view** | ✅ precomputed | `REFRESH` / schedule — **incremental** when possible | expensive aggregations queried often by BI |
# MAGIC | **Streaming table** | ✅ | each refresh processes **only new** source rows | append-heavy ingestion (bronze/silver), low latency |

# COMMAND ----------

# DBTITLE 1,Gold TABLE: daily sales (rebuilt by your job)
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE lab07_gold_daily_sales
# MAGIC COMMENT 'Gold: revenue per day and category - rebuilt by the Section 07 job'
# MAGIC AS
# MAGIC SELECT d.date, d.year_month, p.category,
# MAGIC        count(DISTINCT f.order_id)   AS orders,
# MAGIC        sum(f.quantity)              AS units,
# MAGIC        round(sum(f.amount), 2)      AS revenue
# MAGIC FROM lab07_fact_sales f
# MAGIC JOIN lab07_dim_date    d ON f.date_key = d.date_key
# MAGIC JOIN lab07_dim_product p ON f.product_id = p.product_id
# MAGIC GROUP BY d.date, d.year_month, p.category

# COMMAND ----------

# DBTITLE 1,Gold VIEW: customer 360 (always current, no storage)
# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW lab07_gold_customer_360_v
# MAGIC COMMENT 'Gold: current customer attributes + lifetime value'
# MAGIC AS
# MAGIC SELECT c.customer_id, c.first_name, c.last_name, c.country, c.city,
# MAGIC        count(DISTINCT f.order_id)  AS orders,
# MAGIC        round(sum(f.amount), 2)     AS lifetime_value,
# MAGIC        max(f.order_ts)             AS last_order_ts
# MAGIC FROM lab07_dim_customer_scd2 c
# MAGIC LEFT JOIN lab07_fact_sales f ON f.customer_id = c.customer_id
# MAGIC WHERE c.is_current
# MAGIC GROUP BY c.customer_id, c.first_name, c.last_name, c.country, c.city

# COMMAND ----------

# MAGIC %md
# MAGIC > ⏳ Creating a materialized view starts a small **serverless pipeline** behind the scenes — the next cell can take a
# MAGIC > minute or two. On Free Edition only one pipeline of a type can run at a time.

# COMMAND ----------

# DBTITLE 1,Gold MATERIALIZED VIEW: revenue per country (precomputed, refreshable)
mv_ok = False
try:
    run_sql("""
        CREATE OR REPLACE MATERIALIZED VIEW lab07_gold_country_revenue_mv
        COMMENT 'Gold: revenue per current customer country and month'
        AS
        SELECT c.country, d.year_month, round(sum(f.amount), 2) AS revenue
        FROM lab07_fact_sales f
        JOIN lab07_dim_date d ON f.date_key = d.date_key
        JOIN lab07_dim_customer_scd2 c ON f.customer_id = c.customer_id AND c.is_current
        GROUP BY c.country, d.year_month""")
    # CREATE already computed it. A schedule (or you) keeps it fresh with:  REFRESH MATERIALIZED VIEW lab07_gold_country_revenue_mv
    mv_ok = True
    display(spark.table("lab07_gold_country_revenue_mv").orderBy("country", "year_month").limit(10))
except Exception as e:
    print("🚫 Materialized view not available on this compute:", (str(e).strip().splitlines() or [repr(e)])[0][:200])
    print("   Run the same statement in the SQL editor on a SQL warehouse.")

# COMMAND ----------

# MAGIC %md
# MAGIC A **streaming table** is created the same way but reads a **stream** — each refresh processes only new rows. It is the
# MAGIC typical object for ingestion (you'll build them in Section 08):
# MAGIC
# MAGIC ```sql
# MAGIC CREATE OR REFRESH STREAMING TABLE payments_bronze
# MAGIC AS SELECT * FROM STREAM read_files('/Volumes/<catalog>/shopwave/raw/payments-messy', format => 'csv', header => true);
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Denormalized "one big table" for BI (a gold table too)
wide = (spark.table("lab07_fact_sales")
             .join(spark.table("lab07_dim_date").select("date_key", "date", "year_month", "is_weekend"), "date_key")
             .join(spark.table("lab07_dim_product").select("product_id", "title", "category", "brand"), "product_id")
             .join(spark.table("lab07_dim_customer_scd2").where("is_current")
                        .select("customer_id", "country", "city"), "customer_id", "left"))
wide.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("lab07_gold_sales_wide")
print("wide table rows:", spark.table("lab07_gold_sales_wide").count(), "| columns:", len(spark.table("lab07_gold_sales_wide").columns))

# COMMAND ----------

# MAGIC %md
# MAGIC > 🔐 Publishing gold for a team is also about **access**: ``GRANT SELECT ON TABLE lab07_gold_daily_sales TO `analysts` ``,
# MAGIC > or grant on the whole schema. Views let you expose only some columns/rows (Section 13).
# MAGIC
# MAGIC ## Part 6 · User-defined functions
# MAGIC Built-in functions are always the first choice (optimised, run in Photon). When logic must be **reused**, write a UDF:
# MAGIC
# MAGIC | Kind | Defined with | Runs | Speed |
# MAGIC |---|---|---|---|
# MAGIC | **SQL UDF** | `CREATE FUNCTION f(x T) RETURNS T RETURN <SQL expression>` | **inlined** into the query plan like built-ins | ⚡ fast |
# MAGIC | **Python UDF** | `@F.udf("string")` / `spark.udf.register` · UC: `CREATE FUNCTION … LANGUAGE PYTHON` | row by row in a Python worker | 🐢 slowest |
# MAGIC | **pandas UDF** | `@F.pandas_udf("string")` | batches of rows as pandas Series (Arrow) | faster than Python UDFs |
# MAGIC
# MAGIC A SQL UDF created with `CREATE FUNCTION` is a **Unity Catalog object** (`catalog.schema.function`) — governed with
# MAGIC `GRANT EXECUTE`, reusable in every notebook, query and dashboard.

# COMMAND ----------

# DBTITLE 1,SQL UDF in Unity Catalog
# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION lab07_clean_method(m STRING)
# MAGIC RETURNS STRING
# MAGIC COMMENT 'Standardise a payment method: " Apple Pay" -> apple_pay'
# MAGIC RETURN regexp_replace(lower(trim(m)), '[ -]+', '_');
# MAGIC
# MAGIC SELECT method AS raw_method, lab07_clean_method(method) AS clean_method, count(*) AS n
# MAGIC FROM lab07_bronze_payments
# MAGIC GROUP BY ALL
# MAGIC ORDER BY clean_method, raw_method

# COMMAND ----------

# DBTITLE 1,DESCRIBE FUNCTION EXTENDED + the same logic as a Python UDF
display(spark.sql("DESCRIBE FUNCTION EXTENDED lab07_clean_method"))


@F.udf("string")
def clean_method_py(m):
    import re
    return None if m is None else re.sub(r"[ -]+", "_", m.strip().lower())


bronze_pay = spark.table("lab07_bronze_payments")
cmp_udf = bronze_pay.select(F.expr("lab07_clean_method(method)").alias("sql_udf"),
                            clean_method_py("method").alias("python_udf"),
                            F.regexp_replace(F.lower(F.trim("method")), "[ -]+", "_").alias("builtin"))
udf_disagreements = cmp_udf.where("NOT (sql_udf <=> python_udf) OR NOT (sql_udf <=> builtin)").count()
print("rows where the three versions disagree:", udf_disagreements)

# COMMAND ----------

# MAGIC %md
# MAGIC > 🎯 Exam angle: prefer **built-in functions**; if you need reuse, a **SQL UDF** is optimised like built-ins; **Python
# MAGIC > UDFs** move every row to Python (serialisation cost) — use **pandas UDFs** if you must. `DESCRIBE FUNCTION EXTENDED`
# MAGIC > shows a function's body, and `DROP FUNCTION` removes it.
# MAGIC
# MAGIC ## Part 7 · ✅ Automatic checks

# COMMAND ----------

# DBTITLE 1,Check your work
_checks = {
    "DQ rules found problems (unknown orders = 3, negative amounts = 2) and a passing rule (status_invalid = 0)":
        dq_counts["unknown_order"] == 3 and dq_counts["amount_negative"] == 2 and dq_counts["status_invalid"] == 0,
    "quarantine keeps every row: 381 valid + 22 quarantined = 403": (n_valid, n_quarantine) == (381, 22)
                                                                   and n_valid + n_quarantine == payments.count(),
    "every quarantined row says why": spark.table("lab07_payments_quarantine").where("size(dq_errors) = 0").count() == 0,
    "CHECK constraint rejected a negative amount": bad_insert_rejected,
    "a constraint can't be added while rows violate it": existing_rows_block,
    "dim_date has 181 days; fact has one row per order line": spark.table("lab07_dim_date").count() == 181
                                                            and n_fact == spark.table("lab07_silver_order_items").count(),
    "null-safe comparison found 25 changes, <> only 24": (compare["changed_with_neq"], compare["changed_null_safe"]) == (24, 25),
    "SCD1: 300 -> 300 -> 305 rows (overwritten in place + 5 new)": (scd1_after_b1, scd1_after_b2) == (300, 305),
    "SCD2: (325, 300) after batch 1, (350, 305) after batch 2": (scd2_after_b1, scd2_after_b2) == ((325, 300), (350, 305)),
    "SCD2: every customer has exactly one current row": scd2.groupBy("customer_id").agg(F.sum(F.col("is_current").cast("int")).alias("c"))
                                                              .where("c <> 1").count() == 0,
    "point-in-time join finds lines booked in a former country": lines_other_country > 0,
    "gold table and view exist": spark.catalog.tableExists("lab07_gold_daily_sales") and spark.catalog.tableExists("lab07_gold_customer_360_v"),
    "customer 360 view: one row per current customer (305)": spark.table("lab07_gold_customer_360_v").count() == 305,
    "SQL UDF, Python UDF and built-in agree": udf_disagreements == 0,
}
for name, ok in _checks.items():
    print(("✅ " if ok else "❌ ") + name)
print("ℹ️ materialized view:", "created and refreshed" if mv_ok else "not available on this compute")
print("\n🎉 Lab complete - next: 07-L4 · Challenge Lab")
