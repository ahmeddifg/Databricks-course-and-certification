# Databricks notebook source
# MAGIC %md
# MAGIC # 🧪 Lab 07-L1 · Cleaning Bronze into Silver (Guided)
# MAGIC **Time:** ~60 min · **Compute:** Serverless (Free Edition) or any Unity Catalog compute
# MAGIC
# MAGIC Bronze tables hold data **exactly as it arrived**: JSON strings, numbers stored as text, three spellings of the same
# MAGIC payment method, two date formats, duplicates. In this lab you profile bronze data, fix it column by column with
# MAGIC **PySpark** (and the SQL equivalents), remove duplicates the right way, and write clean **silver** tables.
# MAGIC
# MAGIC | Part | You will… |
# MAGIC |---|---|
# MAGIC | 1 | **Profile** bronze tables: `describe()`, `summary()`, null counts, `count_distinct` vs `approx_count_distinct` |
# MAGIC | 2 | Reshape **columns**: parse a JSON string, flatten a struct, split, rename, drop, cast |
# MAGIC | 3 | **Standardise** messy values: trim / case / regex, `try_cast`, two date formats, fill & drop nulls |
# MAGIC | 4 | **Deduplicate**: `distinct()` vs `dropDuplicates(subset)` vs "latest row per key" with `row_number()` |
# MAGIC | 5 | **Explode** arrays: `explode` vs `explode_outer` vs `posexplode` |
# MAGIC | 6 | **Write** silver tables: `saveAsTable` modes and the SQL `CREATE OR REPLACE TABLE … AS SELECT` |
# MAGIC | 7 | ✅ Automatic checks |
# MAGIC
# MAGIC > Every step is shown in **PySpark** and — where it matters for the exam — in **SQL**. Both compile to the same plan.

# COMMAND ----------

# MAGIC %run ../../Includes/_setup

# COMMAND ----------

# MAGIC %run ./_07_prepare

# COMMAND ----------

# DBTITLE 1,Start clean (safe to re-run)
for t in ("lab07_silver_customers", "lab07_silver_products", "lab07_silver_orders", "lab07_silver_order_items",
          "lab07_silver_payments"):
    spark.sql(f"DROP TABLE IF EXISTS {t}")
print("bronze tables:", [r["tableName"] for r in spark.sql("SHOW TABLES LIKE 'lab07_bronze*'").collect()])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1 · Profile the bronze data (EDA)
# MAGIC Never clean data you haven't looked at. Three quick tools:
# MAGIC
# MAGIC | Tool | Gives you |
# MAGIC |---|---|
# MAGIC | `df.describe()` | count, mean, stddev, min, max (numeric **and** string columns) |
# MAGIC | `df.summary()` | the same **plus percentiles** (25 %, 50 %, 75 %) — or pick: `summary("count", "min", "max")` |
# MAGIC | `null_counts(df)` (helper) | NULLs per column: `sum(col.isNull().cast("int"))` for every column |

# COMMAND ----------

# DBTITLE 1,Row counts and schemas
bronze_orders = spark.table("lab07_bronze_orders")
bronze_payments = spark.table("lab07_bronze_payments")
n_bronze_orders, n_bronze_payments = bronze_orders.count(), bronze_payments.count()
print("orders:", n_bronze_orders, "| payments:", n_bronze_payments)
bronze_payments.printSchema()          # CSV read without type inference -> every business column is a STRING

# COMMAND ----------

# DBTITLE 1,describe() vs summary()
display(bronze_orders.select("quantity", "total").describe())
display(bronze_orders.select("quantity", "total").summary())          # + 25% / 50% / 75%

# COMMAND ----------

# DBTITLE 1,NULLs per column
display(null_counts(bronze_payments.drop("_source_file", "_ingested_at")))

# COMMAND ----------

# MAGIC %md
# MAGIC **Exact vs approximate distinct counts.** `count_distinct` (= `countDistinct`) is exact but needs to remember every
# MAGIC value; **`approx_count_distinct`** uses the HyperLogLog++ sketch: tiny memory, fast on billions of rows, default maximum
# MAGIC relative standard deviation **5 %** (`rsd` argument, default 0.05) — an estimate, not a guarantee. Same in SQL: `count(DISTINCT x)` vs `approx_count_distinct(x)`.

# COMMAND ----------

# DBTITLE 1,Aggregates for profiling: count, count_distinct, approx_count_distinct, mean
profile = bronze_orders.agg(
    F.count("*").alias("rows"),
    F.count_distinct("order_id").alias("distinct_orders"),
    F.approx_count_distinct("customer_id").alias("approx_customers"),
    F.count_distinct("customer_id").alias("exact_customers"),
    F.round(F.mean("total"), 2).alias("mean_total"),
    F.sum(F.when(F.col("quantity") == 0, 1).otherwise(0)).alias("cancelled"))
display(profile)
profile_row = profile.first()

# COMMAND ----------

# MAGIC %md
# MAGIC > 🎯 What the profile tells us about `lab07_bronze_orders`: **2010 rows but 2000 distinct order ids** (duplicates!),
# MAGIC > 20 orders with quantity 0 (cancelled). `lab07_bronze_payments`: every business column is a string; `payment_id`,
# MAGIC > `method`, `currency` and `paid_at` contain NULLs, and `amount` hides bad values as **text** (`'N/A'`, `'USD 12.50'`)
# MAGIC > that only become NULL after cleaning. That is our cleaning to-do list.
# MAGIC
# MAGIC ## Part 2 · Column operations — the customers table
# MAGIC `profile` is a JSON **string**. Two ways to get at it:
# MAGIC
# MAGIC | Way | SQL | PySpark |
# MAGIC |---|---|---|
# MAGIC | **Path extraction** (Databricks SQL, on the fly) | `profile:address:country` | `F.get_json_object("profile", "$.address.country")` |
# MAGIC | **Parse into a struct** (typed, reusable) | `from_json(profile, '<schema>')` | `F.from_json("profile", schema)` |
# MAGIC
# MAGIC `schema_of_json` derives the schema from a **sample** value, so you don't have to type it.

# COMMAND ----------

# DBTITLE 1,JSON paths in SQL (Databricks syntax)
# MAGIC %sql
# MAGIC SELECT customer_id,
# MAGIC        profile:first_name                AS first_name,
# MAGIC        profile:address:country           AS country,
# MAGIC        profile:address:city              AS city
# MAGIC FROM lab07_bronze_customers
# MAGIC LIMIT 5

# COMMAND ----------

# DBTITLE 1,schema_of_json + from_json: turn the string into a struct
sample = spark.table("lab07_bronze_customers").select("profile").first()[0]
print("sample value:", sample)
profile_schema = spark.range(1).select(F.schema_of_json(F.lit(sample))).first()[0]
print("schema_of_json ->", profile_schema)

customers_parsed = (spark.table("lab07_bronze_customers")
                         .withColumn("p", F.from_json("profile", profile_schema)))
customers_parsed.select("customer_id", "p").printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Now the struct can be **flattened** with `p.*` (all fields) or `p.address.city` (one nested field), and the other column
# MAGIC operations follow:
# MAGIC
# MAGIC | Operation | PySpark | SQL |
# MAGIC |---|---|---|
# MAGIC | pick / compute columns | `select("a", F.col("b").alias("c"))` | `SELECT a, b AS c` |
# MAGIC | add / replace a column | `withColumn("x", expr)` · several: `withColumns({...})` | `SELECT *, expr AS x` |
# MAGIC | rename | `withColumnRenamed("old", "new")` · several: `withColumnsRenamed({...})` | `SELECT old AS new` |
# MAGIC | drop | `drop("a", "b")` | `SELECT * EXCEPT (a, b)` |
# MAGIC | split a string | `F.split("email", "@").getItem(1)` | `split(email, '@')[1]` · `split_part(email, '@', 2)` |
# MAGIC | change a type | `F.col("x").cast("decimal(10,2)")` | `CAST(x AS DECIMAL(10,2))` · `x::decimal(10,2)` |

# COMMAND ----------

# DBTITLE 1,Flatten, split, rename, drop, cast
customers_silver = (customers_parsed
    .select("customer_id", "email", "updated", "p.*")                  # struct.* -> first_name, last_name, gender, address
    .withColumn("street", F.col("address.street"))
    .withColumn("city", F.col("address.city"))
    .withColumn("country", F.col("address.country"))
    .drop("address")
    .withColumn("email", F.lower(F.trim("email")))
    .withColumn("email_domain", F.split("email", "@").getItem(1))    # index 1 = the part after '@'
    .withColumnRenamed("updated", "updated_raw")
    .withColumn("updated_at", F.to_timestamp("updated_raw"))
    .drop("updated_raw")
    .select("customer_id", "first_name", "last_name", "gender", "email", "email_domain",
            "street", "city", "country", "updated_at"))
customers_silver.printSchema()
display(customers_silver.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC > 💡 `F.split(...)` returns an **array**; `getItem(1)` (or `[1]`) takes the 2nd element — arrays are **0-based** in
# MAGIC > `getItem` / `[ ]`, but **1-based** in `element_at(array, 1)` and `split_part(str, '@', 2)`. A missing element gives
# MAGIC > NULL (a NULL email → NULL domain).
# MAGIC
# MAGIC ## Part 3 · Standardise the messy payments
# MAGIC Let's look at what "the same value" looks like in bronze:

# COMMAND ----------

# DBTITLE 1,How many spellings of each value?
for c in ("method", "status", "currency"):
    values = [r[0] for r in bronze_payments.select(c).distinct().orderBy(c).collect()]
    print(f"{c:<9} {len(values):>2} distinct: {values}")
display(bronze_payments.select("amount", "paid_at").where("amount NOT RLIKE '^[0-9.]+$' OR paid_at LIKE '%/%'").limit(8))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Strings: trim, case, regular expressions
# MAGIC `regexp_replace(lower(trim(method)), '[ -]+', '_')` turns `" Apple Pay"`, `"apple-pay"` and `"APPLE PAY"` into
# MAGIC `apple_pay`. `upper(trim(status))` fixes `success` / `Success`.
# MAGIC
# MAGIC ### Numbers: `try_cast`
# MAGIC `" 12.50 "`, `"USD 12.50"` and `"N/A"` are all text. Remove everything that isn't a digit, `.` or `-`, then
# MAGIC **`try_cast`** — it returns **NULL** when the value can't be converted, where `cast` **fails the query** under ANSI mode.
# MAGIC
# MAGIC ### Timestamps: two formats
# MAGIC `to_timestamp(col, fmt)` parses **one** format. For several formats, try each and keep the first that works:
# MAGIC `coalesce(try_to_timestamp(c, fmt1), try_to_timestamp(c, fmt2))`.

# COMMAND ----------

# DBTITLE 1,Why "try_" matters: ANSI mode
print("spark.sql.ansi.enabled =", spark.conf.get("spark.sql.ansi.enabled"))
ansi_error_seen = False
try:
    bronze_payments.select(F.to_timestamp("paid_at", "yyyy-MM-dd HH:mm:ss").alias("t")).where("t IS NULL").count()
    print("No error: with ANSI off, unparseable values silently become NULL")
except Exception as e:
    ansi_error_seen = True
    print("🚫 Expected with ANSI on:", (str(e).strip().splitlines() or [repr(e)])[0][:200])

# COMMAND ----------

# MAGIC %md
# MAGIC > 🎯 **ANSI mode** is **on** by default on serverless (and in Databricks SQL): invalid casts, overflows and unparseable
# MAGIC > dates **raise errors** instead of silently returning NULL. Use `try_cast`, `try_to_timestamp`, `try_divide`… when bad
# MAGIC > values are expected and you want NULL instead.

# COMMAND ----------

# DBTITLE 1,Standardise every column
method_clean = F.regexp_replace(F.lower(F.trim("method")), "[ -]+", "_")
amount_clean = F.expr("try_cast(regexp_replace(trim(amount), '[^0-9.-]', '') AS DECIMAL(10,2))")
paid_at_clean = F.coalesce(F.expr("try_to_timestamp(paid_at, 'yyyy-MM-dd HH:mm:ss')"),
                           F.expr("try_to_timestamp(paid_at, 'dd/MM/yyyy HH:mm')"))

payments_std = (bronze_payments
    .select("payment_id", "order_id",
            method_clean.alias("method"),
            amount_clean.alias("amount"),
            F.upper(F.trim("currency")).alias("currency"),
            paid_at_clean.alias("paid_at"),
            F.upper(F.trim("status")).alias("status"),
            F.to_timestamp("updated_at").alias("updated_at")))

display(payments_std.groupBy("method").count().orderBy("method"))
display(null_counts(payments_std))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Missing values: `fillna` / `dropna` (a.k.a. `df.na.fill` / `df.na.drop`)
# MAGIC
# MAGIC | Call | Effect |
# MAGIC |---|---|
# MAGIC | `df.fillna({"currency": "USD"})` | replace NULLs **per column** (the value's type must match the column's) |
# MAGIC | `df.fillna(0)` | replace NULLs in **all numeric** columns |
# MAGIC | `df.dropna(subset=["payment_id"])` | drop rows where **any** listed column is NULL (`how="all"` → only if all are NULL; `thresh=n` → keep rows with ≥ n non-nulls) |
# MAGIC | `df.na.replace("usd", "USD", subset=["currency"])` | replace specific **values** |
# MAGIC | SQL | `coalesce(currency, 'USD')` · `WHERE payment_id IS NOT NULL` · `nvl`, `ifnull` |

# COMMAND ----------

# DBTITLE 1,Fill and drop NULLs
payments_nn = (payments_std
    .dropna(subset=["payment_id"])            # a payment without its key is useless -> drop (3 rows)
    .fillna({"currency": "USD"}))             # business rule: the feed is USD-only
print("rows before:", payments_std.count(), "| after dropna:", payments_nn.count())
display(payments_nn.groupBy("currency").count())

# COMMAND ----------

# MAGIC %md
# MAGIC We **don't** drop rows with a NULL `amount`, `paid_at` or `method` here: they are data-quality problems to **flag or
# MAGIC quarantine** (07-L3), not to silently delete. A derived label makes them easy to find:

# COMMAND ----------

# DBTITLE 1,Derive a label with when / otherwise
payments_labelled = payments_nn.withColumn(
    "amount_check",
    F.when(F.col("amount").isNull(), "missing")
     .when(F.col("amount") < 0, "negative")
     .otherwise("ok"))
display(payments_labelled.groupBy("amount_check").count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 4 · Deduplication — three tools, three meanings
# MAGIC
# MAGIC | Tool | Removes | Which row survives? |
# MAGIC |---|---|---|
# MAGIC | `df.distinct()` · `df.dropDuplicates()` · SQL `SELECT DISTINCT` | rows identical in **every** column | any copy (they're identical) |
# MAGIC | `df.dropDuplicates(["payment_id"])` | rows with the same **key** | **an arbitrary one** ⚠️ |
# MAGIC | `row_number()` over `partitionBy(key).orderBy(updated_at desc)` → keep `= 1` | rows with the same key | the **latest** one — deterministic ✅ |

# COMMAND ----------

# DBTITLE 1,1. Exact duplicates
n_std = payments_nn.count()
n_distinct = payments_nn.distinct().count()
n_keys = payments_nn.select("payment_id").distinct().count()
print(f"rows: {n_std} | distinct rows: {n_distinct} | distinct payment_id: {n_keys}")

# COMMAND ----------

# MAGIC %md
# MAGIC 10 rows were exact copies — but there are still **more rows than payment ids**: 15 payments were **re-sent later with
# MAGIC a new status** (e.g. `SUCCESS` → `REFUNDED`). Those rows differ, so `distinct()` keeps both versions.

# COMMAND ----------

# DBTITLE 1,2. dropDuplicates(subset) keeps an ARBITRARY version
by_key = payments_nn.dropDuplicates(["payment_id"])
resent_ids = [r[0] for r in payments_nn.groupBy("payment_id").agg(F.count_distinct("status").alias("n"))
                                       .where("n > 1").collect()]
print("payments with 2 different versions:", len(resent_ids))
display(by_key.where(F.col("payment_id").isin(resent_ids)).groupBy("status").count())

# COMMAND ----------

# MAGIC %md
# MAGIC Some of the re-sent payments may still show their **old** status — `dropDuplicates` gives no guarantee which row it keeps.
# MAGIC To keep the **latest** version, rank the rows of each key by `updated_at` and keep rank 1:

# COMMAND ----------

# DBTITLE 1,3. Latest row per key with a window function
from pyspark.sql import Window

latest_first = Window.partitionBy("payment_id").orderBy(F.col("updated_at").desc())
payments_silver = (payments_nn
    .withColumn("rn", F.row_number().over(latest_first))
    .where("rn = 1")
    .drop("rn"))
n_payments_silver = payments_silver.count()
refunded_resent = payments_silver.where(F.col("payment_id").isin(resent_ids) & (F.col("status") == "REFUNDED")).count()
print("silver payments:", n_payments_silver, "| re-sent payments now REFUNDED:", refunded_resent, "of", len(resent_ids))

# COMMAND ----------

# MAGIC %md
# MAGIC The same in **SQL** — a window function in a subquery (Databricks also offers the shortcut
# MAGIC `QUALIFY row_number() OVER (…) = 1`):

# COMMAND ----------

# DBTITLE 1,Latest row per key in SQL
# MAGIC %sql
# MAGIC SELECT upper(trim(status)) AS status, count(*) AS payments
# MAGIC FROM (
# MAGIC   SELECT *, row_number() OVER (PARTITION BY payment_id ORDER BY to_timestamp(updated_at) DESC) AS rn
# MAGIC   FROM lab07_bronze_payments
# MAGIC   WHERE payment_id IS NOT NULL
# MAGIC ) AS ranked
# MAGIC WHERE rn = 1
# MAGIC GROUP BY upper(trim(status))
# MAGIC ORDER BY payments DESC

# COMMAND ----------

# MAGIC %md
# MAGIC > 🎯 Exam pattern: "keep only the most recent record per customer/order" → **`row_number()` over a window
# MAGIC > partitioned by the key, ordered by the timestamp DESC, filter `= 1`**. Use `rank()` only if ties should all be kept.
# MAGIC
# MAGIC ## Part 5 · Orders: duplicates, types and exploding arrays
# MAGIC `items` is an **array of structs** (one element per product). To analyse products we need **one row per element**.
# MAGIC
# MAGIC | Function | Rows per order | Empty / NULL array |
# MAGIC |---|---|---|
# MAGIC | `explode(items)` | one per element | **row disappears** |
# MAGIC | `explode_outer(items)` | one per element | **kept** with NULL item |
# MAGIC | `posexplode(items)` | one per element + its position (`pos`, 0-based) | row disappears |

# COMMAND ----------

# DBTITLE 1,Clean orders
orders_clean = (bronze_orders
    .drop("_source_file", "_ingested_at")     # ingestion metadata differs between copies -> drop before deduplicating
    .dropDuplicates()                         # the 10 exact duplicates
    .select("order_id", "customer_id",
            F.timestamp_seconds("order_timestamp").alias("order_ts"),   # epoch seconds -> TIMESTAMP
            F.to_date(F.timestamp_seconds("order_timestamp")).alias("order_date"),
            "quantity",
            F.col("total").cast("decimal(10,2)").alias("total"),
            (F.col("quantity") == 0).alias("is_cancelled"),
            "items"))
n_orders_silver = orders_clean.count()
print("orders after dropDuplicates():", n_orders_silver)

# COMMAND ----------

# DBTITLE 1,explode vs explode_outer vs posexplode
n_explode = orders_clean.select("order_id", F.explode("items").alias("item")).count()
n_explode_outer = orders_clean.select("order_id", F.explode_outer("items").alias("item")).count()
print(f"explode: {n_explode} rows | explode_outer: {n_explode_outer} rows "
      f"(+{n_explode_outer - n_explode} cancelled orders with an empty items array)")

order_items_silver = (orders_clean
    .select("order_id", F.posexplode("items").alias("pos", "item"))
    .select("order_id",
            (F.col("pos") + 1).alias("line_no"),
            "item.product_id", "item.quantity",
            F.col("item.subtotal").cast("decimal(10,2)").alias("subtotal")))
display(order_items_silver.orderBy("order_id", "line_no").limit(6))

# COMMAND ----------

# MAGIC %md
# MAGIC In SQL: `SELECT order_id, explode(items) AS item FROM …` or the `LATERAL VIEW explode(items) t AS item` form. The
# MAGIC reverse operation — rebuilding an array per group — is `collect_list` / `collect_set` (07-L2).
# MAGIC
# MAGIC ## Part 6 · Write the silver tables
# MAGIC `df.write.saveAsTable(name)` creates a **managed Delta table** in the current catalog.schema. The **mode** decides what
# MAGIC happens when the table already exists:
# MAGIC
# MAGIC | `.mode(...)` | Existing table | SQL equivalent |
# MAGIC |---|---|---|
# MAGIC | `"errorifexists"` (**default**) | **error** | `CREATE TABLE t AS SELECT …` |
# MAGIC | `"overwrite"` | replace the data (same schema) | `INSERT OVERWRITE` |
# MAGIC | `"overwrite"` + `.option("overwriteSchema", "true")` | replace data **and** schema | `CREATE OR REPLACE TABLE … AS` |
# MAGIC | `"append"` | add rows | `INSERT INTO` |
# MAGIC | `"ignore"` | do nothing | `CREATE TABLE IF NOT EXISTS … AS` |

# COMMAND ----------

# DBTITLE 1,saveAsTable - four silver tables
writes = {"lab07_silver_customers": customers_silver,
          "lab07_silver_orders": orders_clean.drop("items"),
          "lab07_silver_order_items": order_items_silver,
          "lab07_silver_payments": payments_silver}
for name, df in writes.items():
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(name)
    print(f"✅ {name}: {spark.table(name).count()} rows")

# COMMAND ----------

# DBTITLE 1,The default mode refuses to overwrite
default_mode_error = False
try:
    payments_silver.write.saveAsTable("lab07_silver_payments")          # no mode -> errorifexists
except Exception as e:
    default_mode_error = True
    print("🚫 Expected:", (str(e).strip().splitlines() or [repr(e)])[0][:160])

# COMMAND ----------

# MAGIC %md
# MAGIC The products table in **SQL**: cast the string columns while creating the table (CTAS):

# COMMAND ----------

# DBTITLE 1,CREATE OR REPLACE TABLE ... AS SELECT (products)
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE lab07_silver_products AS
# MAGIC SELECT trim(product_id)               AS product_id,
# MAGIC        trim(title)                    AS title,
# MAGIC        trim(brand)                    AS brand,
# MAGIC        trim(category)                 AS category,
# MAGIC        CAST(price AS DECIMAL(10,2))   AS price
# MAGIC FROM lab07_bronze_products;
# MAGIC
# MAGIC DESCRIBE TABLE lab07_silver_products

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 7 · ✅ Automatic checks

# COMMAND ----------

# DBTITLE 1,Check your work
_pay = spark.table("lab07_silver_payments")
_checks = {
    "bronze profile: 2010 orders, 2000 distinct ids, 20 cancelled": (n_bronze_orders, profile_row["distinct_orders"],
                                                                     profile_row["cancelled"]) == (2010, 2000, 20),
    "approx_count_distinct is close to the exact count (within 5 %)":
        abs(profile_row["approx_customers"] - profile_row["exact_customers"]) <= 0.05 * profile_row["exact_customers"],
    "customers: 300 rows, city/country from the JSON profile, email_domain split out":
        spark.table("lab07_silver_customers").count() == 300
        and spark.table("lab07_silver_customers").where("country IS NULL OR city IS NULL").count() == 0
        and "email_domain" in spark.table("lab07_silver_customers").columns,
    "payments: 4 standard methods (+ NULL), currency never NULL": sorted(
        r[0] for r in _pay.select("method").distinct().collect() if r[0]) == ["apple_pay", "bank_transfer", "card", "paypal"]
        and _pay.where("currency IS NULL").count() == 0,
    "payments: amount DECIMAL and paid_at TIMESTAMP": dict(_pay.dtypes)["amount"] == "decimal(10,2)"
                                                      and dict(_pay.dtypes)["paid_at"] == "timestamp",
    "dedup: 431 -> 428 (no key) -> 418 (distinct) -> 403 (latest per key)":
        (n_bronze_payments, n_std, n_distinct, n_payments_silver) == (431, 428, 418, 403),
    "all 15 re-sent payments keep their LATEST status (REFUNDED)": len(resent_ids) == 15 and refunded_resent == 15,
    "orders: 2000 rows; explode 5004 vs explode_outer 5024": (n_orders_silver, n_explode, n_explode_outer) == (2000, 5004, 5024),
    "order items table has line numbers starting at 1": spark.table("lab07_silver_order_items").agg(F.min("line_no")).first()[0] == 1,
    "products: 36 rows with a DECIMAL price": spark.table("lab07_silver_products").count() == 36
                                             and dict(spark.table("lab07_silver_products").dtypes)["price"] == "decimal(10,2)",
    "saveAsTable without a mode refused to overwrite": default_mode_error,
}
for name, ok in _checks.items():
    print(("✅ " if ok else "❌ ") + name)
print("ℹ️ ANSI error on an unparseable timestamp:", "seen" if ansi_error_seen else "not raised (ANSI off on this compute)")
print("\n🎉 Lab complete - next: 07-L2 · Joins, Unions, Aggregations and Windows")
