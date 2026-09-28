# Databricks notebook source
# MAGIC %md
# MAGIC # 🧪 Lab 07-L2 · Joins, Unions, Aggregations and Windows (Guided)
# MAGIC **Time:** ~60 min · **Compute:** Serverless (Free Edition) or any Unity Catalog compute
# MAGIC
# MAGIC Silver tables are clean — now we **combine** and **summarise** them the way gold tables and BI reports need.
# MAGIC
# MAGIC | Part | You will… |
# MAGIC |---|---|
# MAGIC | 1 | Run every **join type** and predict its row count (inner, left, right, full, semi, anti, cross) |
# MAGIC | 2 | Join on **multiple keys** and use a **cross join** to build a complete grid |
# MAGIC | 3 | **Broadcast** joins and the **tuning parameters** the exam lists — change one, **re-measure** |
# MAGIC | 4 | Stack DataFrames: `union` vs `unionByName` vs SQL `UNION` / `UNION ALL` / `INTERSECT` / `EXCEPT` |
# MAGIC | 5 | **Aggregate**: `groupBy().agg()`, `approx_count_distinct`, `collect_set`, **pivot** |
# MAGIC | 6 | **Window functions**: `row_number` / `rank` / `dense_rank`, `lag` / `lead`, running totals |
# MAGIC | 7 | **Arrays & higher-order functions**: `transform`, `filter`, `exists`, `aggregate`, `array_distinct`, `flatten` |
# MAGIC | 8 | ✅ Automatic checks |

# COMMAND ----------

# MAGIC %run ../../Includes/_setup

# COMMAND ----------

# MAGIC %run ./_07_prepare

# COMMAND ----------

# DBTITLE 1,Silver tables from 07-L1 (rebuilt here if you skipped it)
build_silver_tables()
orders = spark.table("lab07_silver_orders")
customers = spark.table("lab07_silver_customers")
products = spark.table("lab07_silver_products")
order_items = spark.table("lab07_silver_order_items")
print(f"orders {orders.count()} | customers {customers.count()} | products {products.count()} | items {order_items.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1 · Join types — predict, then count
# MAGIC We join the **March 2026** orders with the customers. `march` has **343** orders; **2** of them belong to customer
# MAGIC **`C9999`**, who does not exist in `customers` (300 rows), and **92** customers placed no order in March.
# MAGIC Before running the next cell, guess each count.
# MAGIC
# MAGIC | `how=` | SQL | Keeps |
# MAGIC |---|---|---|
# MAGIC | `"inner"` (default) | `JOIN` / `INNER JOIN` | matching rows only |
# MAGIC | `"left"` | `LEFT [OUTER] JOIN` | all left rows (+ NULLs when no match) |
# MAGIC | `"right"` | `RIGHT [OUTER] JOIN` | all right rows |
# MAGIC | `"full"` | `FULL [OUTER] JOIN` | everything from both sides |
# MAGIC | `"left_semi"` | `LEFT SEMI JOIN` | left rows that **have** a match — left columns only, never duplicated |
# MAGIC | `"left_anti"` | `LEFT ANTI JOIN` | left rows with **no** match (≈ `NOT EXISTS`) |
# MAGIC | `crossJoin()` | `CROSS JOIN` | every combination (n × m) |

# COMMAND ----------

# DBTITLE 1,Count every join type
march = orders.where("order_date BETWEEN '2026-03-01' AND '2026-03-31'")
join_counts = {how: march.join(customers, "customer_id", how).count()
               for how in ("inner", "left", "right", "full", "left_semi", "left_anti")}
customers_without_orders = customers.join(march, "customer_id", "left_anti").count()
customers_with_orders = customers.join(march, "customer_id", "left_semi").count()
print(f"march orders: {march.count()}")
for how, n in join_counts.items():
    print(f"march {how:<10} customers -> {n:>4} rows")
print("customers LEFT ANTI march (no order in March):", customers_without_orders)
print("customers LEFT SEMI march (ordered in March): ", customers_with_orders)

# COMMAND ----------

# MAGIC %md
# MAGIC > 🎯 Read it like the exam: **left = inner + unmatched left rows** (343 = 341 + 2), **right = inner + customers with
# MAGIC > no March order** (433 = 341 + 92), **full = inner + both kinds of unmatched rows** (435). `left_anti` on orders = the
# MAGIC > 2 `C9999` orders — the classic "find orphans" query. `march LEFT SEMI customers` = the 341 orders that have a customer
# MAGIC > (order columns only); `customers LEFT SEMI march` returns each customer who ordered **once** (208), even if they placed
# MAGIC > 5 orders — an inner join would repeat them.

# COMMAND ----------

# DBTITLE 1,The same in SQL (semi / anti joins)
# MAGIC %sql
# MAGIC SELECT 'orders (all months) without a known customer' AS question, count(*) AS n
# MAGIC FROM lab07_silver_orders o LEFT ANTI JOIN lab07_silver_customers c ON o.customer_id = c.customer_id
# MAGIC UNION ALL
# MAGIC SELECT 'customers with at least one order (all months)', count(*)
# MAGIC FROM lab07_silver_customers c LEFT SEMI JOIN lab07_silver_orders o ON o.customer_id = c.customer_id

# COMMAND ----------

# MAGIC %md
# MAGIC Over **all** months you should see **5** orphan orders (all of `C9999`'s) and **300** customers with at least one order.
# MAGIC
# MAGIC ### Join keys: string vs expression
# MAGIC `orders.join(customers, "customer_id")` (or `on=["a", "b"]`) keeps **one** `customer_id` column — like SQL `USING`.
# MAGIC A join **expression** `orders.customer_id == customers.customer_id` keeps **both**, and a later reference to
# MAGIC `customer_id` becomes **ambiguous**:

# COMMAND ----------

# DBTITLE 1,Ambiguous column after an expression join
ambiguous_error = False
joined = march.join(customers, march.customer_id == customers.customer_id)
try:
    joined.select("customer_id").count()
except Exception as e:
    ambiguous_error = True
    print("🚫 Expected:", (str(e).strip().splitlines() or [repr(e)])[0][:200])

fixed = (march.alias("o").join(customers.alias("c"), F.col("o.customer_id") == F.col("c.customer_id"))
               .select("o.order_id", "o.customer_id", "c.country"))
print("with aliases:", fixed.columns)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2 · Multiple keys and a cross join
# MAGIC Finance keeps monthly **revenue targets per country** (`lab07_country_targets`: country, month, target_revenue). To compare
# MAGIC actuals with targets we must join on **both** keys. Some targets are missing, and some country-months may have no sales —
# MAGIC a **cross join** of all countries × all months gives a complete grid to start from.

# COMMAND ----------

# DBTITLE 1,Actual revenue per country and month
revenue_cm = (orders.where("NOT is_cancelled")
                    .join(customers.select("customer_id", "country"), "customer_id")      # inner: drops C9999
                    .withColumn("month", F.date_format("order_ts", "yyyy-MM"))
                    .groupBy("country", "month")
                    .agg(F.sum("total").alias("revenue")))
print("country-month combinations with sales:", revenue_cm.count())

# COMMAND ----------

# DBTITLE 1,Cross join -> complete grid, then two multi-key left joins
countries = customers.select("country").distinct()
months = spark.createDataFrame([(f"2026-{m:02d}",) for m in range(1, 7)], "month STRING")
grid = countries.crossJoin(months)                       # 10 countries x 6 months
targets = spark.table("lab07_country_targets")

attainment = (grid
    .join(revenue_cm, ["country", "month"], "left")      # multiple keys: a list of column names
    .join(targets, ["country", "month"], "left")
    .withColumn("revenue", F.coalesce("revenue", F.lit(0)))
    .withColumn("attainment_pct", F.round(F.col("revenue") / F.col("target_revenue") * 100, 1)))
n_grid, n_missing_targets = grid.count(), attainment.where("target_revenue IS NULL").count()
print(f"grid rows: {n_grid} | rows without a target: {n_missing_targets}")
display(attainment.orderBy("country", "month"))

# COMMAND ----------

# MAGIC %md
# MAGIC > 💡 With **different column names** on each side use an expression with `&`:
# MAGIC > `a.join(b, (a.country == b.cntry) & (a.month == b.period), "left")`. In SQL: `ON a.country = b.cntry AND a.month = b.period`.
# MAGIC > A **cross join** is rarely what you want on big tables (n × m rows) — here it's 10 × 6 = 60, on purpose.
# MAGIC
# MAGIC ## Part 3 · Broadcast joins and tuning parameters
# MAGIC When one side is small, Spark can **broadcast** it (send a full copy to every executor) and avoid shuffling the big
# MAGIC side. Spark does it automatically below `spark.sql.autoBroadcastJoinThreshold`; you can force it with a **hint**
# MAGIC (details in 02-L2).

# COMMAND ----------

# DBTITLE 1,Force a broadcast join
bcast = orders.join(F.broadcast(customers), "customer_id")
bcast_strategy = join_strategy(plan_text(bcast))
print("strategy with broadcast():", bcast_strategy)

# COMMAND ----------

# DBTITLE 1,The same hint in SQL
# MAGIC %sql
# MAGIC SELECT /*+ BROADCAST(c) */ c.country, count(*) AS orders
# MAGIC FROM lab07_silver_orders o JOIN lab07_silver_customers c ON o.customer_id = c.customer_id
# MAGIC GROUP BY c.country
# MAGIC ORDER BY orders DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ### The tuning parameters on the exam
# MAGIC
# MAGIC | Parameter | Controls | Default (Apache Spark) | Serverless |
# MAGIC |---|---|---|---|
# MAGIC | `spark.sql.shuffle.partitions` | partitions **after a shuffle** (joins, groupBy) in DataFrame/SQL | 200 | settable (default `auto`: AQE picks the number) |
# MAGIC | `spark.default.parallelism` | default partitions for **RDD** operations (`parallelize`, RDD shuffles) | total executor cores | managed — not settable |
# MAGIC | `spark.executor.memory` / `spark.driver.memory` | JVM heap of each executor / of the driver — set when the **cluster starts** | 1g | managed — not settable |
# MAGIC | `spark.sql.autoBroadcastJoinThreshold` | max size of a table that is **broadcast automatically** (`-1` = never) | 10 MB | managed — use **hints** instead |
# MAGIC
# MAGIC The method is always the same: **measure → change one parameter → re-measure** (same query, same data) — and check the
# MAGIC query profile (serverless) or the Spark UI (classic compute) to see *why* it changed. On classic compute you set cluster-level values (memory, parallelism)
# MAGIC in the cluster's **Spark config**; `spark.conf.set(...)` only works for runtime SQL settings like shuffle partitions.

# COMMAND ----------

# DBTITLE 1,Read the current values (some are hidden on serverless)
for key in ("spark.sql.shuffle.partitions", "spark.sql.autoBroadcastJoinThreshold", "spark.default.parallelism",
            "spark.executor.memory", "spark.driver.memory"):
    try:
        print(f"{key:<40} = {spark.conf.get(key)}")
    except Exception as e:
        print(f"{key:<40} ⛔ not available here ({(str(e).strip().splitlines() or [repr(e)])[0][:70]})")

# COMMAND ----------

# DBTITLE 1,Measure -> change spark.sql.shuffle.partitions -> re-measure
big = (spark.range(3_000_000)
            .withColumn("key", F.col("id") % 5000)
            .groupBy("key").agg(F.sum("id").alias("total"), F.count("*").alias("n")))

try:                                            # the noop sink runs the whole query but writes nothing
    spark.range(1).write.format("noop").mode("overwrite").save()
    use_noop = True
except Exception:
    use_noop = False                            # not available here -> collect() the small (5000-row) result instead
print("timing method:", "noop sink" if use_noop else "collect()")


def execute(df):
    if use_noop:
        df.write.format("noop").mode("overwrite").save()
    else:
        df.collect()


original_shuffle = spark.conf.get("spark.sql.shuffle.partitions")
timings = {}
for value in (original_shuffle, "1", "400"):
    spark.conf.set("spark.sql.shuffle.partitions", value)
    _, timings[value] = time_it(f"shuffle.partitions = {value}", lambda: execute(big))
spark.conf.set("spark.sql.shuffle.partitions", original_shuffle)          # always restore
print("restored:", spark.conf.get("spark.sql.shuffle.partitions"))

# COMMAND ----------

# MAGIC %md
# MAGIC > 📏 Timings vary run to run (caching, cluster load) — run the cell twice before drawing conclusions. On small data the
# MAGIC > differences are tiny; too **few** partitions hurt when data is large (huge tasks, spill), too **many** hurt when data is
# MAGIC > small (scheduling overhead). That is why serverless defaults to `auto` and lets **AQE** coalesce partitions at runtime.

# COMMAND ----------

# DBTITLE 1,Can we change the broadcast threshold here?
broadcast_conf_settable = False
try:
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")          # -1 disables automatic broadcasts
    broadcast_conf_settable = True
    print("Settable (classic compute). Strategy without auto-broadcast:",
          join_strategy(plan_text(orders.join(customers, "customer_id"))))
except Exception as e:
    print("⛔ Not settable on this compute:", (str(e).strip().splitlines() or [repr(e)])[0][:150])
finally:
    if broadcast_conf_settable:
        spark.conf.unset("spark.sql.autoBroadcastJoinThreshold")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 4 · Stacking rows: unions and set operations
# MAGIC Two payment extracts overlap: January–February and February–March.
# MAGIC
# MAGIC | PySpark | Matches columns by | Duplicates | SQL |
# MAGIC |---|---|---|---|
# MAGIC | `a.union(b)` (= `unionAll`) | **position** | **kept** | `UNION ALL` |
# MAGIC | `a.union(b).distinct()` | position | removed | `UNION` (= `UNION DISTINCT`) |
# MAGIC | `a.unionByName(b)` | **name** | kept | — |
# MAGIC | `a.unionByName(b, allowMissingColumns=True)` | name; missing columns → NULL | kept | — |
# MAGIC | `a.intersect(b)` / `a.exceptAll(b)` / `a.subtract(b)` | position | — | `INTERSECT` / `EXCEPT ALL` / `EXCEPT` (= `MINUS`) |

# COMMAND ----------

# DBTITLE 1,union keeps duplicates, distinct() removes them
payments = spark.table("lab07_silver_payments").select("payment_id", "method", "amount", "paid_at")
jan_feb = payments.where("month(paid_at) IN (1, 2)")
feb_mar = payments.where("month(paid_at) IN (2, 3)")
n_union_all = jan_feb.union(feb_mar).count()
n_union_distinct = jan_feb.union(feb_mar).distinct().count()
n_intersect = jan_feb.intersect(feb_mar).count()
print(f"jan_feb {jan_feb.count()} + feb_mar {feb_mar.count()} -> union {n_union_all} | "
      f"union + distinct {n_union_distinct} | intersect (February) {n_intersect}")

# COMMAND ----------

# DBTITLE 1,union matches by POSITION - unionByName by NAME
reordered = feb_mar.select("method", "payment_id", "amount", "paid_at")      # same columns, different order
mixed = jan_feb.union(reordered)                                             # no error: both columns are strings!
print("rows where 'payment_id' now holds a method:", mixed.where("payment_id NOT LIKE 'PAY%'").count())

by_name = jan_feb.unionByName(reordered)
print("with unionByName:", by_name.where("payment_id NOT LIKE 'PAY%'").count())

with_extra = feb_mar.withColumn("source", F.lit("march_export"))
stacked = jan_feb.unionByName(with_extra, allowMissingColumns=True)
print("allowMissingColumns ->", stacked.columns, "| NULL sources:", stacked.where("source IS NULL").count())

# COMMAND ----------

# MAGIC %md
# MAGIC > ⚠️ Classic exam trap: DataFrame **`union()` behaves like SQL `UNION ALL`** (keeps duplicates) and matches columns **by
# MAGIC > position**. If the types happen to match, swapped columns are mixed **silently**. Use `unionByName`.

# COMMAND ----------

# DBTITLE 1,UNION vs UNION ALL vs INTERSECT vs EXCEPT in SQL
# MAGIC %sql
# MAGIC WITH jf AS (SELECT payment_id FROM lab07_silver_payments WHERE month(paid_at) IN (1, 2)),
# MAGIC      fm AS (SELECT payment_id FROM lab07_silver_payments WHERE month(paid_at) IN (2, 3))
# MAGIC SELECT 'UNION ALL' AS op, count(*) AS n FROM (SELECT * FROM jf UNION ALL SELECT * FROM fm)
# MAGIC UNION ALL SELECT 'UNION',     count(*) FROM (SELECT * FROM jf UNION     SELECT * FROM fm)
# MAGIC UNION ALL SELECT 'INTERSECT', count(*) FROM (SELECT * FROM jf INTERSECT SELECT * FROM fm)
# MAGIC UNION ALL SELECT 'EXCEPT',    count(*) FROM (SELECT * FROM jf EXCEPT    SELECT * FROM fm)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 5 · Aggregations and pivot
# MAGIC `groupBy(...).agg(...)` takes any number of aggregate expressions. Revenue per product **category**:

# COMMAND ----------

# DBTITLE 1,groupBy + many aggregates
items_enriched = (order_items.join(products, "product_id")
                             .join(orders.select("order_id", "customer_id", "order_ts"), "order_id"))
category_stats = (items_enriched.groupBy("category")
    .agg(F.count("*").alias("lines"),
         F.sum("quantity").alias("units"),
         F.round(F.sum("subtotal"), 2).alias("revenue"),
         F.round(F.avg("subtotal"), 2).alias("avg_line"),             # avg = mean
         F.min("subtotal").alias("min_line"),
         F.max("subtotal").alias("max_line"),
         F.count_distinct("customer_id").alias("customers"),
         F.approx_count_distinct("customer_id").alias("customers_approx"),
         F.collect_set("brand").alias("brands"))                      # distinct values as an array
    .orderBy(F.desc("revenue")))
display(category_stats)

# COMMAND ----------

# MAGIC %md
# MAGIC **Pivot** turns the distinct values of one column into **columns**. Listing the values (`pivot("month", [...])`) saves
# MAGIC Spark a job to discover them and fixes the column order.

# COMMAND ----------

# DBTITLE 1,Pivot: category x month revenue
month_list = [f"2026-{m:02d}" for m in range(1, 7)]
pivoted = (items_enriched.withColumn("month", F.date_format("order_ts", "yyyy-MM"))
                         .groupBy("category")
                         .pivot("month", month_list)
                         .agg(F.round(F.sum("subtotal"), 0)))
display(pivoted.orderBy("category"))
print("pivot columns:", pivoted.columns)

# COMMAND ----------

# DBTITLE 1,PIVOT in SQL
# MAGIC %sql
# MAGIC SELECT * FROM (
# MAGIC   SELECT p.category, date_format(o.order_ts, 'yyyy-MM') AS month, i.subtotal
# MAGIC   FROM lab07_silver_order_items i
# MAGIC   JOIN lab07_silver_products p ON i.product_id = p.product_id
# MAGIC   JOIN lab07_silver_orders   o ON i.order_id = o.order_id
# MAGIC )
# MAGIC PIVOT (round(sum(subtotal), 0) FOR month IN ('2026-01', '2026-02', '2026-03', '2026-04', '2026-05', '2026-06'))
# MAGIC ORDER BY category

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 6 · Window functions
# MAGIC An aggregate **collapses** rows; a **window function** computes over a group of rows but **keeps every row**.
# MAGIC A window = `partitionBy` (the groups) + `orderBy` (order inside the group) + optionally a **frame** (`rowsBetween`).
# MAGIC
# MAGIC | Function | Ties (same value) | Example result for 100, 90, 90, 80 |
# MAGIC |---|---|---|
# MAGIC | `row_number()` | always unique | 1, 2, 3, 4 |
# MAGIC | `rank()` | same rank, then a **gap** | 1, 2, 2, 4 |
# MAGIC | `dense_rank()` | same rank, **no gap** | 1, 2, 2, 3 |
# MAGIC | `lag(col, 1)` / `lead(col, 1)` | value of the previous / next row | — |

# COMMAND ----------

# DBTITLE 1,Rank products by units sold within their category
units = (items_enriched.groupBy("category", "product_id", "title").agg(F.sum("quantity").alias("units")))
by_units = Window.partitionBy("category").orderBy(F.desc("units"))
ranked = (units.withColumn("row_number", F.row_number().over(by_units))
               .withColumn("rank", F.rank().over(by_units))
               .withColumn("dense_rank", F.dense_rank().over(by_units)))
display(ranked.orderBy("category", "row_number"))
top_per_category = ranked.where("row_number = 1")
print("best seller per category:", top_per_category.count(), "rows")

# COMMAND ----------

# DBTITLE 1,lag / lead: days between a customer's orders
per_customer = Window.partitionBy("customer_id").orderBy("order_ts")
gaps = (orders.where("NOT is_cancelled")
              .select("customer_id", "order_id", "order_ts", "total")
              .withColumn("prev_order_ts", F.lag("order_ts", 1).over(per_customer))
              .withColumn("next_order_id", F.lead("order_id", 1).over(per_customer))
              .withColumn("days_since_prev", F.datediff("order_ts", "prev_order_ts")))
display(gaps.where("customer_id = 'C0001'"))

# COMMAND ----------

# DBTITLE 1,Running total and share of total per customer
running = Window.partitionBy("customer_id").orderBy("order_ts").rowsBetween(Window.unboundedPreceding, Window.currentRow)
whole = Window.partitionBy("customer_id")                         # no orderBy -> the whole partition
ltv = (orders.where("NOT is_cancelled")
             .select("customer_id", "order_id", "order_ts", "total")
             .withColumn("running_total", F.sum("total").over(running))
             .withColumn("customer_total", F.sum("total").over(whole))
             .withColumn("share_pct", F.round(F.col("total") / F.col("customer_total") * 100, 1)))
display(ltv.where("customer_id = 'C0001'").orderBy("order_ts"))

# COMMAND ----------

# DBTITLE 1,Window functions in SQL
# MAGIC %sql
# MAGIC SELECT customer_id, order_id, order_ts, total,
# MAGIC        row_number() OVER (PARTITION BY customer_id ORDER BY order_ts)                        AS order_no,
# MAGIC        sum(total)   OVER (PARTITION BY customer_id ORDER BY order_ts
# MAGIC                           ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)                  AS running_total,
# MAGIC        lag(order_ts) OVER (PARTITION BY customer_id ORDER BY order_ts)                       AS prev_order_ts
# MAGIC FROM lab07_silver_orders
# MAGIC WHERE customer_id = 'C0001' AND NOT is_cancelled
# MAGIC ORDER BY order_ts

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 7 · Arrays and higher-order functions
# MAGIC Arrays are common in semi-structured data (our `items`). You can **build** arrays with aggregates and **transform** them
# MAGIC without exploding, using **higher-order functions** (HOFs) that take a lambda `x -> …`.
# MAGIC
# MAGIC | Function | Does |
# MAGIC |---|---|
# MAGIC | `collect_list(c)` / `collect_set(c)` | aggregate values into an array (with / without duplicates) |
# MAGIC | `array_distinct`, `array_contains`, `size`, `sort_array`, `flatten` (array of arrays → array) | array helpers |
# MAGIC | `transform(arr, x -> …)` | map every element |
# MAGIC | `filter(arr, x -> …)` | keep matching elements |
# MAGIC | `exists(arr, x -> …)` | true if any element matches |
# MAGIC | `aggregate(arr, start, (acc, x) -> …)` | fold the array into one value (also `reduce`) |

# COMMAND ----------

# DBTITLE 1,Build arrays per customer: collect_list vs collect_set, flatten, array_distinct
cats = items_enriched.groupBy("customer_id").agg(
    F.collect_list("category").alias("categories_list"),
    F.collect_set("category").alias("categories_set"))
cats = cats.withColumn("n_list", F.size("categories_list")).withColumn("n_set", F.size("categories_set"))
display(cats.orderBy("customer_id").limit(5))

baskets = items_enriched.groupBy("customer_id", "order_id").agg(F.collect_list("product_id").alias("products"))
per_customer_products = (baskets.groupBy("customer_id")
                                .agg(F.collect_list("products").alias("nested"))          # array<array<string>>
                                .withColumn("all_products", F.flatten("nested"))
                                .withColumn("distinct_products", F.array_distinct("all_products")))
display(per_customer_products.select("customer_id", "nested", "all_products", "distinct_products")
                             .orderBy("customer_id").limit(3))

# COMMAND ----------

# DBTITLE 1,Higher-order functions on the items array (PySpark)
orders_with_items = silver_orders_df()                  # reference silver orders incl. the items array
orders_with_items.createOrReplaceTempView("orders_with_items_v")
hof = orders_with_items.select(
    "order_id", "total",
    F.transform("items", lambda i: i["product_id"]).alias("product_ids"),
    F.filter("items", lambda i: i["quantity"] >= 2).alias("multi_unit_items"),
    F.exists("items", lambda i: i["product_id"] == "P001").alias("has_p001"),
    F.round(F.aggregate("items", F.lit(0.0), lambda acc, i: acc + i["subtotal"]), 2).alias("items_sum"))
display(hof.limit(5))
hof_mismatch = hof.where(F.abs(F.col("items_sum") - F.col("total")) > 0.01).count()
print("orders whose total != sum of their item subtotals:", hof_mismatch)

# COMMAND ----------

# DBTITLE 1,The same HOFs in SQL (lambda syntax: x -> expression)
# MAGIC %sql
# MAGIC SELECT order_id,
# MAGIC        transform(items, i -> i.product_id)                        AS product_ids,
# MAGIC        filter(items, i -> i.quantity >= 2)                        AS multi_unit_items,
# MAGIC        exists(items, i -> i.product_id = 'P001')                  AS has_p001,
# MAGIC        round(aggregate(items, 0D, (acc, i) -> acc + i.subtotal), 2) AS items_sum,
# MAGIC        size(items)                                                AS n_items
# MAGIC FROM orders_with_items_v
# MAGIC LIMIT 5

# COMMAND ----------

# MAGIC %md
# MAGIC > 💡 HOFs keep one row per order — no `explode` + `groupBy` round trip. `explode` is still the right tool when you need
# MAGIC > **one row per element** (e.g. a fact table of order lines).
# MAGIC
# MAGIC ## Part 8 · ✅ Automatic checks

# COMMAND ----------

# DBTITLE 1,Check your work
_checks = {
    "March: inner 341 / left 343 / left_anti 2 (the C9999 orders)":
        (join_counts["inner"], join_counts["left"], join_counts["left_anti"]) == (341, 343, 2),
    "right 433 = inner + 92 customers without a March order": join_counts["right"] == 433 and customers_without_orders == 92,
    "full 435 = inner + 2 orphan orders + 92 customers": join_counts["full"] == 435,
    "semi joins: 341 orders with a customer, 208 customers with an order (each once)":
        join_counts["left_semi"] == 341 and customers_with_orders == 208 == 300 - customers_without_orders,
    "expression join made customer_id ambiguous": ambiguous_error,
    "cross join grid = 10 x 6 = 60, 4 country-months without a target": (n_grid, n_missing_targets) == (60, 4),
    "broadcast() hint produced a broadcast join": bcast_strategy == "broadcast",
    "shuffle partitions restored after the experiment": spark.conf.get("spark.sql.shuffle.partitions") == original_shuffle,
    "union kept duplicates; union + distinct = union - intersect": n_union_distinct == n_union_all - n_intersect,
    "union by position mixed columns; unionByName did not": mixed.where("payment_id NOT LIKE 'PAY%'").count() > 0
                                                            and by_name.where("payment_id NOT LIKE 'PAY%'").count() == 0,
    "category stats for all 6 categories, pivot has 7 columns": category_stats.count() == 6 and len(pivoted.columns) == 7,
    "one best seller per category (row_number = 1)": top_per_category.count() == 6,
    "running total of a customer's last order = customer total":
        ltv.withColumn("rn", F.row_number().over(Window.partitionBy("customer_id").orderBy(F.desc("order_ts"))))
           .where("rn = 1 AND running_total <> customer_total").count() == 0,
    "collect_set never larger than collect_list": cats.where("n_set > n_list").count() == 0,
    "aggregate() over items reproduces every order total": hof_mismatch == 0,
}
for name, ok in _checks.items():
    print(("✅ " if ok else "❌ ") + name)
print("ℹ️ autoBroadcastJoinThreshold", "is settable here (classic)" if broadcast_conf_settable else "is managed by serverless")
print("\n🎉 Lab complete - next: 07-L3 · Gold Modeling, SCD and Data Quality")
