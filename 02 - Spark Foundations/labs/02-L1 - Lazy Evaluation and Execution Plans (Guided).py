# Databricks notebook source
# MAGIC %md
# MAGIC # 🧪 Lab 02-L1 · Lazy Evaluation & Execution Plans (Guided)
# MAGIC **Time:** ~40 min · **Compute:** Serverless (everything also works on classic compute)
# MAGIC
# MAGIC | Part | You will… |
# MAGIC |---|---|
# MAGIC | 1 | Prove that transformations are lazy and actions trigger work |
# MAGIC | 2 | See that DataFrame code and SQL produce the **same plan** |
# MAGIC | 3 | Watch Catalyst **push filters down** and **prune columns** |
# MAGIC | 4 | Find the **shuffles** (narrow vs wide) in plans |
# MAGIC | 5 | Open the **query profile** (serverless) / **Spark UI** (classic) |
# MAGIC | 6 | ✅ Automatic checks |

# COMMAND ----------

# MAGIC %run ../../Includes/_setup

# COMMAND ----------

# MAGIC %run ./_02_prepare

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1 · Transformations are lazy, actions are eager
# MAGIC
# MAGIC We start from **5 billion** rows. If Spark ran each transformation immediately, the next cell would take a very long time…

# COMMAND ----------

# DBTITLE 1,Build a huge pipeline - watch the timer
import time
from pyspark.sql import functions as F

t0 = time.time()
huge = spark.range(5_000_000_000)                                   # 5 billion ids
doubled = huge.withColumn("x", F.col("id") * 2)                     # transformation
multiples_of_7 = doubled.where("x % 7 = 0")                          # transformation
projected = multiples_of_7.select("id", "x", (F.col("x") / 7).alias("x_div_7"))  # transformation
print(f"4 transformations defined in {time.time() - t0:.3f} s — nothing has been computed yet 💤")

# COMMAND ----------

# MAGIC %md
# MAGIC Now an **action**. `show(5)` only needs 5 rows, and because Spark optimizes the *whole* plan before running it,
# MAGIC it stops as soon as it has them.

# COMMAND ----------

# DBTITLE 1,Action: show(5)
t0 = time.time()
projected.show(5)
print(f"Action finished in {time.time() - t0:.2f} s ⚡")

# COMMAND ----------

# MAGIC %md
# MAGIC > ⚠️ Don't run `projected.count()` — that action really must read all 5 billion rows!
# MAGIC >
# MAGIC > 🎯 **Remember:** transformations return a **new DataFrame** and build a plan; **actions** (`show`, `count`, `collect`, `take`, `write`, `display`) execute it.

# COMMAND ----------

# MAGIC %md
# MAGIC ### DataFrames are immutable

# COMMAND ----------

# DBTITLE 1,Immutability gotcha
orders = spark.table("lab02_orders")
orders.withColumn("total_with_vat", F.round(F.col("total") * 1.15, 2))   # result not assigned!
print("Has total_with_vat?", "total_with_vat" in orders.columns)          # False

orders_vat = orders.withColumn("total_with_vat", F.round(F.col("total") * 1.15, 2))
print("Has total_with_vat now?", "total_with_vat" in orders_vat.columns)  # True

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2 · Python and SQL compile to the same plan
# MAGIC
# MAGIC We write the same question — *revenue per country for orders above 100* — in PySpark and in SQL, then compare physical plans.

# COMMAND ----------

# DBTITLE 1,The DataFrame version
customers = spark.table("lab02_customers")
revenue_df = (orders.where(F.col("total") > 100)
                    .join(customers, "customer_id")
                    .groupBy("country")
                    .agg(F.round(F.sum("total"), 2).alias("revenue")))
display(revenue_df.orderBy(F.desc("revenue")))

# COMMAND ----------

# DBTITLE 1,The SQL version
# MAGIC %sql
# MAGIC SELECT c.country, round(sum(o.total), 2) AS revenue
# MAGIC FROM lab02_orders o
# MAGIC JOIN lab02_customers c ON o.customer_id = c.customer_id
# MAGIC WHERE o.total > 100
# MAGIC GROUP BY c.country
# MAGIC ORDER BY revenue DESC

# COMMAND ----------

# DBTITLE 1,Compare the physical plans
df_plan = plan_text(revenue_df)
sql_plan = plan_text("""
    SELECT c.country, round(sum(o.total), 2) AS revenue
    FROM lab02_orders o JOIN lab02_customers c ON o.customer_id = c.customer_id
    WHERE o.total > 100 GROUP BY c.country""")

print("=== DataFrame plan ===\n", df_plan)
print("\n=== SQL plan ===\n", sql_plan)
print("Same join strategy:", join_strategy(df_plan), "vs", join_strategy(sql_plan))
print("Both shuffle:", has_shuffle(df_plan), "/", has_shuffle(sql_plan))

# COMMAND ----------

# MAGIC %md
# MAGIC The operator names and ids may differ slightly, but the **shape is identical**: same scans, same pushed filter, same join
# MAGIC strategy, same aggregation. ✅ There's no performance reason to prefer SQL over the DataFrame API or vice versa.
# MAGIC
# MAGIC > 💡 You can also call `revenue_df.explain()` / `revenue_df.explain("formatted")` — they print the plan directly.

# COMMAND ----------

# DBTITLE 1,explain() directly
revenue_df.explain()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 3 · Catalyst at work: predicate pushdown & column pruning
# MAGIC
# MAGIC We ask for just two columns of big orders. Look at the **scan** node at the bottom of the plan:
# MAGIC * only `order_id` and `total` are read (**column pruning** — see `ReadSchema` / the output columns),
# MAGIC * the filter on `total` is handed to the scan (**pushed filters / data filters**) so files and row groups that can't match are skipped.
# MAGIC
# MAGIC *(On serverless the scan is a Photon operator and the labels can differ — e.g. `PhotonScan … RequiredDataFilters` — but the idea is the same.)*

# COMMAND ----------

# DBTITLE 1,Pushdown & pruning in the plan
pushdown_plan = plan_text("SELECT order_id, total FROM lab02_orders WHERE total > 500", mode="FORMATTED")
print(pushdown_plan)

# COMMAND ----------

# MAGIC %md
# MAGIC The `EXTENDED` mode shows **all four planning phases** — parsed, analyzed, optimized logical, and physical:

# COMMAND ----------

# DBTITLE 1,The four Catalyst phases
print(plan_text("SELECT order_id, total * (1 + 0.15) AS gross FROM lab02_orders WHERE total > 500", mode="EXTENDED"))

# COMMAND ----------

# MAGIC %md
# MAGIC In the **Optimized Logical Plan** notice **constant folding**: `(1 + 0.15)` has been pre-computed to `1.15`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 4 · Narrow vs wide — find the shuffles
# MAGIC
# MAGIC For each query we print whether the physical plan contains a **shuffle** (`Exchange`, ignoring broadcast exchanges).
# MAGIC **Predict the answer before running the cell!**

# COMMAND ----------

# DBTITLE 1,Which queries shuffle?
queries = {
    "A · filter + select":            "SELECT order_id, total FROM lab02_orders WHERE quantity > 2",
    "B · withColumn-style expression": "SELECT *, total * 1.15 AS gross FROM lab02_orders",
    "C · GROUP BY":                    "SELECT customer_id, sum(total) FROM lab02_orders GROUP BY customer_id",
    "D · ORDER BY":                    "SELECT * FROM lab02_orders ORDER BY total DESC",
    "E · DISTINCT":                    "SELECT DISTINCT customer_id FROM lab02_orders",
    "F · UNION ALL":                   "SELECT order_id FROM lab02_orders UNION ALL SELECT order_id FROM lab02_orders",
}
shuffle_results = {}
for name, q in queries.items():
    shuffle_results[name] = has_shuffle(plan_text(q))
    print(f"{name:<34} → {'🔀 SHUFFLE (wide)' if shuffle_results[name] else '➡️ no shuffle (narrow)'}")

# COMMAND ----------

# MAGIC %md
# MAGIC | Query | Why |
# MAGIC |---|---|
# MAGIC | A, B, F | Each output row depends on one input partition → **narrow**, pipelined in one stage |
# MAGIC | C | Rows with the same `customer_id` must meet → `Exchange hashpartitioning(customer_id)` |
# MAGIC | D | A **global** sort needs all data ordered across partitions → `Exchange rangepartitioning` |
# MAGIC | E | DISTINCT is an aggregation on all columns → hash shuffle |
# MAGIC
# MAGIC Look at the GROUP BY plan in detail — note the **two-step aggregation** around the shuffle (partial `HashAggregate` → `Exchange` → final `HashAggregate`):

# COMMAND ----------

# DBTITLE 1,Two-phase aggregation
print(plan_text(queries["C · GROUP BY"]))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 5 · 🖱️ Query profile & Spark UI
# MAGIC
# MAGIC **On serverless (query profile):**
# MAGIC 1. Run the next cell.
# MAGIC 2. Under its output, click **See performance** (or the ⏱️ / **Query profile** link) → open the query.
# MAGIC 3. Explore the operator **graph**: rows produced per operator, time spent, bytes read, whether a shuffle (Exchange) happened, spill.
# MAGIC
# MAGIC **On classic compute (Spark UI):**
# MAGIC 1. Click the **Spark Jobs** expander under the cell output → **View** next to a job (or Compute ▸ your cluster ▸ **Spark UI**).
# MAGIC 2. **Jobs** tab: one job per action · **Stages** tab: a new stage after every shuffle, one task per partition ·
# MAGIC    **SQL / DataFrame** tab: the executed plan with metrics (the **final** AQE plan).
# MAGIC 3. In a stage, compare task durations: one much longer task = **skew**; "Spill (memory/disk)" > 0 = partitions too big.

# COMMAND ----------

# DBTITLE 1,Run something worth profiling
display(spark.sql("""
    WITH order_items AS (
        SELECT customer_id, explode(items) AS i FROM lab02_orders
    )
    SELECT c.country, p.category, round(sum(oi.i.subtotal), 2) AS revenue
    FROM order_items oi
    JOIN lab02_customers c ON oi.customer_id  = c.customer_id
    JOIN lab02_products  p ON oi.i.product_id = p.product_id
    GROUP BY c.country, p.category
    ORDER BY revenue DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 6 · ✅ Automatic checks

# COMMAND ----------

# DBTITLE 1,Check your understanding
_checks = {
    "Filter/select query is narrow (A)": shuffle_results["A · filter + select"] is False,
    "GROUP BY query shuffles (C)": shuffle_results["C · GROUP BY"] is True,
    "ORDER BY query shuffles (D)": shuffle_results["D · ORDER BY"] is True,
    "UNION ALL is narrow (F)": shuffle_results["F · UNION ALL"] is False,
    "DataFrame & SQL versions use the same join strategy": join_strategy(df_plan) == join_strategy(sql_plan),
    "orders_vat has the new column": "total_with_vat" in orders_vat.columns,
}
for name, ok in _checks.items():
    print(("✅ " if ok else "❌ ") + name)
print("\n🎉 Lab complete - next: 02-L2 · Partitions, Shuffles & Joins")
