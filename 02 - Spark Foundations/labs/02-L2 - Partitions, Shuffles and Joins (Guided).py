# Databricks notebook source
# MAGIC %md
# MAGIC # 🧪 Lab 02-L2 · Partitions, Shuffles & Joins (Guided)
# MAGIC **Time:** ~45 min · **Compute:** Serverless (Part 6 behaves differently on classic compute — that's the point)
# MAGIC
# MAGIC | Part | You will… |
# MAGIC |---|---|
# MAGIC | 1 | Count partitions and rows per partition — without `df.rdd` |
# MAGIC | 2 | Compare `repartition()` and `coalesce()` |
# MAGIC | 3 | Control shuffle partitions with `spark.sql.shuffle.partitions` |
# MAGIC | 4 | Compare **broadcast** and **shuffle** joins using hints |
# MAGIC | 5 | Create **data skew**, see it, and fix it with **salting** |
# MAGIC | 6 | Try caching (classic compute only) |
# MAGIC | 7 | ✅ Automatic checks |

# COMMAND ----------

# MAGIC %run ../../Includes/_setup

# COMMAND ----------

# MAGIC %run ./_02_prepare

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1 · How many partitions?
# MAGIC
# MAGIC `df.rdd.getNumPartitions()` is the classic answer, but **RDDs aren't available on serverless**. The portable way is the
# MAGIC built-in function **`spark_partition_id()`**, which tells you which partition each row lives in.

# COMMAND ----------

# DBTITLE 1,Rows per partition of a table scan
from pyspark.sql import functions as F

orders = spark.table("lab02_orders")
display(partition_sizes(orders))
print("Non-empty partitions:", num_partitions(orders))

# COMMAND ----------

# MAGIC %md
# MAGIC A small Delta table is usually read in **one or a few partitions** (roughly one per file / 128 MB split).
# MAGIC For demos we'll use a bigger generated DataFrame:

# COMMAND ----------

# DBTITLE 1,A 2-million-row demo DataFrame
events = (spark.range(2_000_000)
          .withColumn("country_id", (F.col("id") % 10).cast("int"))
          .withColumn("amount", F.round(F.rand(seed=42) * 100, 2)))
print("Partitions of events:", num_partitions(events))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2 · `repartition()` vs `coalesce()`

# COMMAND ----------

# DBTITLE 1,repartition(8): full shuffle, balanced
events_8 = events.repartition(8)
display(partition_sizes(events_8))
print("Partitions:", num_partitions(events_8), "| shuffle in plan:", has_shuffle(plan_text(events_8)))

# COMMAND ----------

# DBTITLE 1,coalesce(2): merge without a full shuffle
events_2 = events_8.coalesce(2)
display(partition_sizes(events_2))
print("Partitions:", num_partitions(events_2))
print("Plan of coalesce step:\n", plan_text(events_2))

# COMMAND ----------

# MAGIC %md
# MAGIC Notice in the last plan: the `Exchange RoundRobinPartitioning(8)` comes from `repartition(8)`; `coalesce(2)` adds only a
# MAGIC **`Coalesce 2`** node — **no extra exchange**.

# COMMAND ----------

# DBTITLE 1,coalesce cannot increase partitions
events_up = events_8.coalesce(16)
print("coalesce(16) on 8 partitions ->", num_partitions(events_up), "partitions (coalesce can only decrease!)")
print("repartition(16) on 8 partitions ->", num_partitions(events_8.repartition(16)), "partitions")

# COMMAND ----------

# MAGIC %md
# MAGIC **Repartition by a column** puts all rows with the same key into the same partition (useful before writing, or for key-based work):

# COMMAND ----------

# DBTITLE 1,repartition by column
by_country = events.repartition(4, "country_id")
display(by_country.groupBy(F.spark_partition_id().alias("partition"))
                  .agg(F.collect_set("country_id").alias("country_ids"), F.count("*").alias("rows"))
                  .orderBy("partition"))

# COMMAND ----------

# MAGIC %md
# MAGIC > 🎯 `repartition(n)` → **full shuffle**, can go **up or down**, evenly balanced.
# MAGIC > `coalesce(n)` → **no full shuffle**, can only go **down**, partitions may be uneven. Typical use: fewer output files after a filter.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 3 · Shuffle partitions
# MAGIC After a shuffle (groupBy, join…) the number of partitions comes from **`spark.sql.shuffle.partitions`** (default **200**) —
# MAGIC and **AQE** then **coalesces** small partitions automatically. This is one of the few Spark settings you may change on serverless.

# COMMAND ----------

# DBTITLE 1,Current setting
original_shuffle_partitions = spark.conf.get("spark.sql.shuffle.partitions")
print("spark.sql.shuffle.partitions =", original_shuffle_partitions)

# COMMAND ----------

# DBTITLE 1,Change it and look at the result of a groupBy
spark.conf.set("spark.sql.shuffle.partitions", "8")
per_country = events.groupBy("country_id").agg(F.sum("amount").alias("amount"))
print("Plan shows the requested shuffle partitions:")
print([l.strip() for l in plan_text(per_country).splitlines() if "Exchange" in l])
print("Non-empty partitions after the groupBy (AQE may merge them):", num_partitions(per_country))

# COMMAND ----------

# DBTITLE 1,Reset to the original value
spark.conf.set("spark.sql.shuffle.partitions", original_shuffle_partitions)
print("spark.sql.shuffle.partitions =", spark.conf.get("spark.sql.shuffle.partitions"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 4 · Broadcast vs shuffle joins
# MAGIC
# MAGIC `lab02_customers` is tiny (300 rows), so Spark picks a **broadcast hash join** automatically.
# MAGIC We can also **force** strategies with **hints**.

# COMMAND ----------

# DBTITLE 1,1. Let Spark choose
customers = spark.table("lab02_customers")
auto_join = orders.join(customers, "customer_id")
print("Spark chose:", join_strategy(plan_text(auto_join)))

# COMMAND ----------

# DBTITLE 1,2. Force a broadcast with broadcast()
from pyspark.sql.functions import broadcast

bcast_join = orders.join(broadcast(customers), "customer_id")
bcast_plan = plan_text(bcast_join)
print("Strategy:", join_strategy(bcast_plan), "| big side shuffled?", has_shuffle(bcast_plan))

# COMMAND ----------

# DBTITLE 1,3. Force a shuffle join with a MERGE hint
shuffle_join = orders.join(customers.hint("merge"), "customer_id")
shuffle_plan = plan_text(shuffle_join)
print("Strategy:", join_strategy(shuffle_plan), "| shuffle in plan?", has_shuffle(shuffle_plan))
print(shuffle_plan)

# COMMAND ----------

# MAGIC %md
# MAGIC The same hints in SQL:

# COMMAND ----------

# DBTITLE 1,SQL join hints
# MAGIC %sql
# MAGIC SELECT /*+ BROADCAST(c) */ c.country, count(*) AS orders
# MAGIC FROM lab02_orders o JOIN lab02_customers c ON o.customer_id = c.customer_id
# MAGIC GROUP BY c.country
# MAGIC ORDER BY orders DESC

# COMMAND ----------

# MAGIC %md
# MAGIC | | Broadcast hash join | Shuffle (sort-merge / shuffled hash) join |
# MAGIC |---|---|---|
# MAGIC | Plan shows | `BroadcastExchange` + `BroadcastHashJoin` | `Exchange hashpartitioning` on **both** sides + `SortMergeJoin` / `ShuffledHashJoin` |
# MAGIC | Best when | one side small | both sides large |
# MAGIC | Hint | `broadcast(df)` · `/*+ BROADCAST(t) */` | `df.hint("merge")` · `/*+ MERGE(t) */` · `/*+ SHUFFLE_HASH(t) */` |
# MAGIC
# MAGIC > 💡 On serverless you can't change `spark.sql.autoBroadcastJoinThreshold`, but **hints always work** — and AQE may still switch
# MAGIC > to a broadcast at runtime when it discovers a side is small.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 5 · Data skew — see it, then fix it with salting
# MAGIC
# MAGIC We create a dataset where **90 %** of rows have the same key (`key = 0`) — like one mega-customer or a default value.

# COMMAND ----------

# DBTITLE 1,Create skewed data
skewed = (spark.range(2_000_000)
          .withColumn("key", F.when(F.rand(seed=7) < 0.9, F.lit(0)).otherwise((F.col("id") % 50) + 1))
          .withColumn("amount", F.lit(1)))

skewed_by_key = skewed.repartition(8, "key")          # hash partitioning by key -> hot key lands in ONE partition
display(partition_sizes(skewed_by_key))

# COMMAND ----------

# MAGIC %md
# MAGIC One partition holds ~1.8 million rows while the others hold a few thousand: in a real job, the task for that partition is the
# MAGIC **straggler** that keeps the whole stage waiting (you'd see it in the Spark UI as one long task, often with spill).
# MAGIC
# MAGIC **Salting:** add a random "salt" to the key so the hot key is spread over many partitions, aggregate per (key, salt),
# MAGIC then aggregate again per key.

# COMMAND ----------

# DBTITLE 1,Salted repartition is balanced
SALT_BUCKETS = 8
salted = skewed.withColumn("salt", (F.rand(seed=11) * SALT_BUCKETS).cast("int"))
salted_by_key = salted.repartition(8, "key", "salt")
display(partition_sizes(salted_by_key))

# COMMAND ----------

# DBTITLE 1,Two-step aggregation gives the same answer
direct = skewed.groupBy("key").agg(F.sum("amount").alias("total"))
two_step = (salted.groupBy("key", "salt").agg(F.sum("amount").alias("partial"))
                  .groupBy("key").agg(F.sum("partial").alias("total")))

same = direct.orderBy("key").collect() == two_step.orderBy("key").collect()
print("Same result with and without salting:", same)

# COMMAND ----------

# MAGIC %md
# MAGIC > ✅ On Databricks, **AQE skew-join handling** automatically splits skewed partitions in **joins** — try it first.
# MAGIC > Salting is the manual technique for skewed aggregations/joins AQE can't fix. Other fixes: filter or handle hot keys separately,
# MAGIC > broadcast the small side.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 6 · Caching (classic compute only)
# MAGIC Caching is **lazy** (stored on the first action) and **not supported on serverless**. Run the cell to see how your compute behaves.

# COMMAND ----------

# DBTITLE 1,Try to cache
try:
    cached = orders.where("total > 100").cache()     # lazy: nothing stored yet
    cached.count()                                   # first action materializes the cache
    cached.count()                                   # served from cache
    print("✅ Cached. Storage level:", cached.storageLevel)
    cached.unpersist()                               # always free the memory
    print("Unpersisted.")
except Exception as e:
    print("🚫 Caching not available on this compute (expected on serverless):", str(e).splitlines()[0][:150])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 7 · ✅ Automatic checks

# COMMAND ----------

# DBTITLE 1,Check your work
_checks = {
    "repartition(8) gives 8 partitions": num_partitions(events_8) == 8,
    "coalesce(2) gives 2 partitions": num_partitions(events_2) == 2,
    "coalesce cannot increase partitions": num_partitions(events_up) == 8,
    "broadcast() produced a broadcast join": join_strategy(bcast_plan) == "broadcast",
    "merge hint produced a shuffle join": join_strategy(shuffle_plan) == "shuffle",
    "salting preserved the aggregation result": same,
    "shuffle partitions reset": spark.conf.get("spark.sql.shuffle.partitions") == original_shuffle_partitions,
}
for name, ok in _checks.items():
    print(("✅ " if ok else "❌ ") + name)
print("\n🎉 Lab complete - next: 02-L3 · Challenge Lab")
