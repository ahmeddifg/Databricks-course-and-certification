# Databricks notebook source
# MAGIC %md
# MAGIC # 🏆 Lab 02-L3 · Challenge Lab — Spark Foundations — ✅ SOLUTION
# MAGIC > Try the challenge yourself first! This notebook contains the reference answers and runs end-to-end.
# MAGIC
# MAGIC **Time:** ~45 min · **Compute:** Serverless
# MAGIC
# MAGIC Replace every `None` / `# TODO`, then run the **✅ Check** cell under each task.
# MAGIC Helpers from `_02_prepare` are available: `plan_text()`, `has_shuffle()`, `join_strategy()`, `partition_sizes()`, `num_partitions()`.
# MAGIC
# MAGIC | Task | Skill |
# MAGIC |---|---|
# MAGIC | 1 | Transformations vs actions |
# MAGIC | 2 | Build a lazy DataFrame pipeline (explode + join + aggregate) |
# MAGIC | 3 | Read plans: which queries shuffle? |
# MAGIC | 4 | Force a broadcast join |
# MAGIC | 5 | `repartition` vs `coalesce` |
# MAGIC | 6 | Shuffle partitions in the plan |
# MAGIC | 7 | Driver vs executor |

# COMMAND ----------

# MAGIC %run ../../Includes/_setup

# COMMAND ----------

# MAGIC %run ./_02_prepare

# COMMAND ----------

# DBTITLE 1,Check helper (run me - don't edit)
from pyspark.sql import functions as F


def check(label, condition):
    print(("✅ " if condition else "❌ ") + label)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 1 · Transformations vs actions
# MAGIC From this list, put **only the actions** into the set `my_actions`:
# MAGIC
# MAGIC `select`, `count`, `filter`, `show`, `groupBy`, `collect`, `withColumn`, `take`, `join`, `write`, `orderBy`, `first`

# COMMAND ----------

# DBTITLE 1,Task 1 · SOLUTION
my_actions = {"count", "show", "collect", "take", "write", "first"}
# select, filter, groupBy, withColumn, join, orderBy are lazy transformations.

# COMMAND ----------

# DBTITLE 1,✅ Check 1
check("Correct set of actions", my_actions == {"count", "show", "collect", "take", "write", "first"})

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 2 · A lazy pipeline: revenue per product category
# MAGIC Using the **DataFrame API** (no SQL), build `category_revenue` with columns **`category`** and **`revenue`**:
# MAGIC 1. From `lab02_orders`, **explode** the `items` array into one row per item (`F.explode`).
# MAGIC 2. **Join** the items with `lab02_products` on `product_id`.
# MAGIC 3. **Sum** the item `subtotal` per `category`, rounded to 2 decimals, as `revenue`.
# MAGIC
# MAGIC Don't call any action in this cell — just define the DataFrame.

# COMMAND ----------

# DBTITLE 1,Task 2 · SOLUTION
items = (spark.table("lab02_orders")
         .select(F.explode("items").alias("item"))
         .select("item.product_id", "item.subtotal"))

category_revenue = (items.join(spark.table("lab02_products"), "product_id")
                         .groupBy("category")
                         .agg(F.round(F.sum("subtotal"), 2).alias("revenue")))
# Nothing has run yet - this is only a plan. The check cell's collect() is the action.

# COMMAND ----------

# DBTITLE 1,✅ Check 2
_expected2 = {r["category"]: r["revenue"] for r in spark.sql("""
    SELECT p.category, round(sum(oi.i.subtotal), 2) AS revenue
    FROM (SELECT explode(items) AS i FROM lab02_orders) oi
    JOIN lab02_products p ON oi.i.product_id = p.product_id
    GROUP BY p.category""").collect()}
_got2 = {} if category_revenue is None else {r["category"]: r["revenue"] for r in category_revenue.collect()}
check("category_revenue has columns category, revenue",
      category_revenue is not None and set(category_revenue.columns) == {"category", "revenue"})
check("revenue per category is correct",
      _got2.keys() == _expected2.keys() and all(abs(_got2[k] - _expected2[k]) < 0.01 for k in _expected2))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 3 · Which queries shuffle?
# MAGIC **Without running them**, predict for each query whether its physical plan contains a shuffle (`True`) or not (`False`).
# MAGIC Then run the check — it uses `has_shuffle(plan_text(query))` to verify.
# MAGIC
# MAGIC | Id | Query |
# MAGIC |---|---|
# MAGIC | Q1 | `SELECT customer_id, total FROM lab02_orders WHERE total BETWEEN 50 AND 60` |
# MAGIC | Q2 | `SELECT country, count(*) FROM lab02_customers GROUP BY country` |
# MAGIC | Q3 | `SELECT *, upper(title) AS title_uc FROM lab02_products` |
# MAGIC | Q4 | `SELECT * FROM lab02_products ORDER BY price DESC` |
# MAGIC | Q5 | `SELECT DISTINCT category FROM lab02_products` |

# COMMAND ----------

# DBTITLE 1,Task 3 · SOLUTION
shuffle_predictions = {
    "Q1": False,  # filter + projection: narrow
    "Q2": True,   # GROUP BY: hash shuffle by country
    "Q3": False,  # column expression: narrow
    "Q4": True,   # global ORDER BY: range-partitioning shuffle
    "Q5": True,   # DISTINCT = aggregation: hash shuffle
}

# COMMAND ----------

# DBTITLE 1,✅ Check 3
_q3 = {
    "Q1": "SELECT customer_id, total FROM lab02_orders WHERE total BETWEEN 50 AND 60",
    "Q2": "SELECT country, count(*) FROM lab02_customers GROUP BY country",
    "Q3": "SELECT *, upper(title) AS title_uc FROM lab02_products",
    "Q4": "SELECT * FROM lab02_products ORDER BY price DESC",
    "Q5": "SELECT DISTINCT category FROM lab02_products",
}
for qid, q in _q3.items():
    actual = has_shuffle(plan_text(q))
    check(f"{qid}: predicted {shuffle_predictions[qid]}, actual {actual}", shuffle_predictions[qid] == actual)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 4 · Force a broadcast join
# MAGIC Create `bcast_df`: join `lab02_orders` with `lab02_customers` on `customer_id`, **forcing a broadcast** of the customers side.
# MAGIC Then store its physical plan (use `plan_text`) in `bcast_plan`.

# COMMAND ----------

# DBTITLE 1,Task 4 · SOLUTION
from pyspark.sql.functions import broadcast

bcast_df = spark.table("lab02_orders").join(broadcast(spark.table("lab02_customers")), "customer_id")
bcast_plan = plan_text(bcast_df)
print(bcast_plan)

# COMMAND ----------

# DBTITLE 1,✅ Check 4
check("bcast_plan uses a broadcast hash join", bcast_plan is not None and join_strategy(bcast_plan) == "broadcast")
check("the join returns 2,010 rows minus orders from unknown customers",
      bcast_df is not None and bcast_df.count() == spark.sql(
          "SELECT count(*) FROM lab02_orders o JOIN lab02_customers c USING (customer_id)").first()[0])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 5 · `repartition` vs `coalesce`
# MAGIC 1. `df6` = `lab02_orders` split into **exactly 6** balanced partitions.
# MAGIC 2. `df3` = `df6` reduced to **3** partitions **without a full shuffle**.

# COMMAND ----------

# DBTITLE 1,Task 5 · SOLUTION
df6 = spark.table("lab02_orders").repartition(6)   # full shuffle, balanced
df3 = df6.coalesce(3)                               # merge partitions, no extra shuffle
display(partition_sizes(df3))

# COMMAND ----------

# DBTITLE 1,✅ Check 5
check("df6 has 6 partitions", df6 is not None and num_partitions(df6) == 6)
check("df3 has 3 partitions", df3 is not None and num_partitions(df3) == 3)
check("df3 plan has only ONE shuffle exchange (from df6)",
      df3 is not None and sum(1 for l in plan_text(df3).splitlines()
                              if "Exchange" in l and "Broadcast" not in l and "Sink" not in l) == 1)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 6 · Shuffle partitions in the plan
# MAGIC 1. Save the current value of `spark.sql.shuffle.partitions` in `original_sp`.
# MAGIC 2. Set it to **16**.
# MAGIC 3. Build `country_counts` = number of customers per `country` from `lab02_customers`, and store its plan in `country_plan`.
# MAGIC 4. Restore the original value.

# COMMAND ----------

# DBTITLE 1,Task 6 · SOLUTION
original_sp = spark.conf.get("spark.sql.shuffle.partitions")
spark.conf.set("spark.sql.shuffle.partitions", "16")

country_counts = spark.table("lab02_customers").groupBy("country").count()
country_plan = plan_text(country_counts)          # plan is fixed now, with 16 shuffle partitions
print(country_plan)

spark.conf.set("spark.sql.shuffle.partitions", original_sp)

# COMMAND ----------

# DBTITLE 1,✅ Check 6
_exchange_lines = [] if country_plan is None else [l for l in country_plan.splitlines() if "hashpartitioning" in l]
check("plan shuffles by country into 16 partitions", any("16)" in l for l in _exchange_lines))
check("setting restored", original_sp is not None and spark.conf.get("spark.sql.shuffle.partitions") == original_sp)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 7 · Driver vs executor
# MAGIC A notebook runs `rows = spark.table("big_events").collect()` on a 500 GB table and fails with an **out-of-memory error**.
# MAGIC Where does the error happen and what's the best fix?
# MAGIC
# MAGIC * **A** — On the executors; add more workers
# MAGIC * **B** — On the driver; keep the processing distributed and write the result to a table (or `limit()` before collecting)
# MAGIC * **C** — In the control plane; restart the workspace
# MAGIC * **D** — On the SQL warehouse; increase its size

# COMMAND ----------

# DBTITLE 1,Task 7 · SOLUTION
answer_task7 = "B"
# collect() pulls every row to the DRIVER -> driver OOM. Keep work distributed; write results to a table.

# COMMAND ----------

# DBTITLE 1,✅ Check 7
check("Task 7 answer", answer_task7 == "B")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🏁 Final score

# COMMAND ----------

# DBTITLE 1,Score
_final = {
    "Task 1": my_actions == {"count", "show", "collect", "take", "write", "first"},
    "Task 2": category_revenue is not None and set(category_revenue.columns) == {"category", "revenue"},
    "Task 3": all(shuffle_predictions[q] == has_shuffle(plan_text(sql)) for q, sql in _q3.items()),
    "Task 4": bcast_plan is not None and join_strategy(bcast_plan) == "broadcast",
    "Task 5": df6 is not None and df3 is not None and num_partitions(df3) == 3,
    "Task 6": country_plan is not None and any("16)" in l for l in _exchange_lines),
    "Task 7": answer_task7 == "B",
}
for k, v in _final.items():
    print(("✅ " if v else "❌ ") + k)
print(f"\nScore: {sum(_final.values())} / {len(_final)}")
