# Databricks notebook source
# MAGIC %md
# MAGIC # 🧪 Lab 12-L2 · Skew, Shuffle and Spill on a Real Dataset (Guided)
# MAGIC **Time:** ~45 min · **Compute:** Serverless notebook (classic compute optional) · **Works on Free Edition**
# MAGIC
# MAGIC `perf_events` has **3,000,000** clickstream events. **30 %** of them come from one customer, **`C0007`** — a marketplace bot.
# MAGIC The `transform_clicks` regression you found in 12-L1 is exactly this kind of data. Now go *inside* the query: stages, tasks,
# MAGIC shuffles — and fix the skew.
# MAGIC
# MAGIC | Part | You will… |
# MAGIC |---|---|
# MAGIC | 1 | Read a plan: where are the **shuffles** (stage boundaries)? Open the **query profile** |
# MAGIC | 2 | **See the skew**: records per task, Spark-UI style (Min / 25th / Median / 75th / Max) |
# MAGIC | 3 | Why a skewed `sum` is harmless and a skewed **join** is not (partial aggregation) |
# MAGIC | 4 | **Shuffle**: broadcast join vs shuffle join — plans and profiles |
# MAGIC | 5 | Fix skew with **salting** (join and two-stage aggregation) — same result, balanced tasks |
# MAGIC | 6 | **Spill**: where it shows up and how to provoke / fix it (+ `system.query.history`) |
# MAGIC | 7 | **Caching** on serverless vs classic |
# MAGIC
# MAGIC > 🖥️ **Serverless has no Spark UI.** You'll use the **query profile** (*See performance* under a cell) and the helper
# MAGIC > `stage_summary(df, n)`, which prints the same *Summary Metrics* table the Spark UI shows for a stage — computed from the
# MAGIC > real number of records in each partition. If you have a classic cluster (paid / trial workspace), the optional steps show
# MAGIC > where to find each metric in the real Spark UI.

# COMMAND ----------

# MAGIC %run ../../Includes/_setup

# COMMAND ----------

# MAGIC %run ./_12_prepare

# COMMAND ----------

# DBTITLE 1,Lab settings
events = spark.table("perf_events")
customers = spark.table("perf_customers")
N_TASKS = 16                       # we repartition explicitly, so every "stage" below has 16 tasks
print(f"perf_events: {events.count():,} rows · hot customer {HOT_CUSTOMER}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1 · Plans, stages and the query profile
# MAGIC A **shuffle** (an `Exchange` in the plan) is a stage boundary. Count them before you run anything.

# COMMAND ----------

# DBTITLE 1,How many shuffles does a group-by need?
by_type = events.groupBy("event_type").agg(F.count(F.lit(1)).alias("events"), F.sum("amount").alias("revenue"))
_ops = plan_ops(by_type)
display(by_type)

# COMMAND ----------

# MAGIC %md
# MAGIC 🖱️ **Query profile** (serverless): under the cell above click **See performance** → pick the query → **See query profile**.
# MAGIC 1. **Details** panel: *Wall-clock duration* vs *Total task time* (many cores working in parallel → task time ≫ wall time).
# MAGIC 2. The graph: **Scan perf_events** → **Aggregate** (partial, before the shuffle) → **Shuffle** → **Aggregate** (final).
# MAGIC    Click the Shuffle node: rows and bytes that crossed the network — only a few rows per task, thanks to the partial
# MAGIC    aggregation.
# MAGIC 3. **Top operators**: which step took the most time.
# MAGIC
# MAGIC 🖥️ *Classic cluster (optional)*: the **Spark Jobs** link under the cell → the job → **2 stages**: stage 1 reads the files
# MAGIC (one task per file split) and writes the shuffle; stage 2 reads the shuffle. The **DAG visualization** shows the
# MAGIC `Exchange` between them.

# COMMAND ----------

# DBTITLE 1,✅ Check Part 1
check("the group-by plan contains at least one shuffle (Exchange)", _ops["shuffles"] >= 1)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2 · See the skew
# MAGIC Which customers produce the most events?

# COMMAND ----------

# DBTITLE 1,Top keys
# MAGIC %sql
# MAGIC SELECT customer_id, count(*) AS events, round(100 * count(*) / sum(count(*)) OVER (), 2) AS pct
# MAGIC FROM perf_events GROUP BY customer_id ORDER BY events DESC LIMIT 5

# COMMAND ----------

# MAGIC %md
# MAGIC Any operation that **shuffles by `customer_id`** (join, group-by, window `PARTITION BY customer_id`) sends **all** rows of a
# MAGIC key to the **same task**. `repartition(16, "customer_id")` does exactly that shuffle — look at the records each of the 16
# MAGIC tasks would get:

# COMMAND ----------

# DBTITLE 1,Records per task when shuffling by customer_id - and by event_id
_skewed = stage_summary(events.repartition(N_TASKS, "customer_id"), N_TASKS, "Shuffle by customer_id")
_even = stage_summary(events.repartition(N_TASKS, "event_id"), N_TASKS, "Shuffle by event_id (unique key)")

# COMMAND ----------

# MAGIC %md
# MAGIC Read it like the Spark UI's **Summary Metrics**: when shuffling by `customer_id` one task gets **≈ a third** of all rows
# MAGIC — Max is several times the 75th percentile. That task alone decides how long the stage takes; the other 15 cores wait
# MAGIC (in the Spark UI event timeline: one long bar). Shuffling by a unique key is balanced.

# COMMAND ----------

# DBTITLE 1,✅ Check Part 2
check("shuffling by customer_id is skewed (Max > 1.5 x 75th percentile)", _skewed["skewed"])
check("shuffling by event_id is balanced", not _even["skewed"])
check("the biggest customer_id task holds more than 25 % of all rows", _skewed["max_share"] > 25)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 3 · A skewed `sum` is fine — a skewed join is not
# MAGIC Spark aggregates **before** the shuffle (partial aggregation): each task first sums *its* rows per customer, so only one
# MAGIC row per customer and task is shuffled. The hot key costs nothing extra.

# COMMAND ----------

# DBTITLE 1,Revenue per customer: look at the plan
rev = events.where("event_type = 'purchase'").groupBy("customer_id").agg(F.sum("amount").alias("revenue"))
print(plan_text(rev).split("== Physical Plan ==")[-1][:1500])

# COMMAND ----------

# MAGIC %md
# MAGIC You see **two** aggregates: a partial one (`HashAggregate … partial_sum` / Photon *grouping aggregate*) **below** the
# MAGIC `Exchange` and a final one above it. Skew hurts when rows **can't** be pre-combined:
# MAGIC
# MAGIC | Operation | Pre-aggregated before the shuffle? | Hurt by a hot key? |
# MAGIC |---|---|---|
# MAGIC | `sum`, `count`, `min`, `max`, `avg` per key | ✅ yes | hardly |
# MAGIC | **join** on the key (both sides shuffled) | ❌ every row moves | ✅ **yes** |
# MAGIC | window `PARTITION BY key` (rank, lag, running total) | ❌ | ✅ yes |
# MAGIC | `collect_list`, `count(DISTINCT …)`, percentiles per key | partly | ✅ often |
# MAGIC
# MAGIC ## Part 4 · Shuffle: broadcast join vs shuffle join
# MAGIC `perf_customers` is tiny (300 rows): **broadcasting** it means the 3M events are **not shuffled at all**. Force the other
# MAGIC strategy with a hint to compare.

# COMMAND ----------

# DBTITLE 1,Two plans for the same join
joined_bc = (events.join(F.broadcast(customers), "customer_id")
                   .groupBy("country").agg(F.sum("amount").alias("revenue")))
joined_sh = (events.join(customers.hint("shuffle_merge"), "customer_id")
                   .groupBy("country").agg(F.sum("amount").alias("revenue")))
print("BROADCAST:"); _bc = plan_ops(joined_bc)
print("SHUFFLE  :"); _sh = plan_ops(joined_sh)

# COMMAND ----------

# DBTITLE 1,Run both (wall time is only indicative on a shared serverless pool)
_t_bc = time_it("broadcast join", joined_bc)
_t_sh = time_it("shuffle join  ", joined_sh)

# COMMAND ----------

# MAGIC %md
# MAGIC 🖱️ Open the **query profile** of each `time_it` run (*See performance* under the cell): in the shuffle join, the events
# MAGIC side goes through a **Shuffle** node (all 3M rows, partitioned by `customer_id` → the skewed partition from Part 2); in the
# MAGIC broadcast join the events are joined where they are read and only the small per-country result is shuffled.
# MAGIC
# MAGIC > 🧠 On serverless the hint `shuffle_merge` may be executed by **Photon** as a *shuffled hash join* — still a shuffle of both
# MAGIC > sides. Without hints, **AQE** would broadcast `perf_customers` anyway: it is far below the broadcast threshold.

# COMMAND ----------

# DBTITLE 1,✅ Check Part 4
check("the broadcast plan has a broadcast and no shuffle of the 3M events (fewer exchanges)",
      _bc["broadcasts"] >= 1 and _bc["shuffles"] < _sh["shuffles"])
check("the hinted plan shuffles both join sides", _sh["shuffles"] >= 2 and _sh["broadcasts"] == 0)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 5 · Fix skew with salting
# MAGIC Suppose `customers` were **big** (say 50 M profiles) — broadcast is no option and the join is skewed. **Salting** splits the
# MAGIC hot key: add a salt `0..N-1` to every event, replicate each customer row `N` times (one per salt), join on
# MAGIC `(customer_id, salt)`.

# COMMAND ----------

# DBTITLE 1,Salted join - records per task
SALTS = 16
events_s = events.withColumn("salt", F.pmod(F.col("event_id"), F.lit(SALTS)))           # deterministic 0..15
customers_s = customers.crossJoin(spark.range(SALTS).withColumnRenamed("id", "salt"))   # 300 x 16 rows
_salted = stage_summary(events_s.repartition(N_TASKS, "customer_id", "salt"), N_TASKS, "Shuffle by (customer_id, salt)")
salted_join = (events_s.join(customers_s.hint("shuffle_merge"), ["customer_id", "salt"])
                       .groupBy("country").agg(F.sum("amount").alias("revenue")))

# COMMAND ----------

# DBTITLE 1,Same answer?
_a = {r["country"]: r["revenue"] for r in joined_bc.collect()}
_b = {r["country"]: r["revenue"] for r in salted_join.collect()}
print("countries:", len(_a), "· identical results:", _a == _b)

# COMMAND ----------

# MAGIC %md
# MAGIC The hot key is now spread over 16 tasks. The same trick works for a **skewed aggregation that can't be pre-combined**,
# MAGIC in two steps — for example the number of **distinct products** per customer:

# COMMAND ----------

# DBTITLE 1,Two-stage (salted) aggregation
step1 = (events_s.groupBy("customer_id", "salt")
                 .agg(F.collect_set("product_id").alias("products")))                   # hot key split 16 ways
step2 = (step1.groupBy("customer_id")
              .agg(F.size(F.array_distinct(F.flatten(F.collect_list("products")))).cast("long").alias("distinct_products")))
direct = events.groupBy("customer_id").agg(F.countDistinct("product_id").alias("distinct_products"))
_same = step2.exceptAll(direct).count() == 0 and direct.exceptAll(step2).count() == 0
print("two-stage result == direct result:", _same)
display(step2.orderBy("customer_id").limit(5))

# COMMAND ----------

# DBTITLE 1,✅ Check Part 5
check("salting cut the biggest task's share by more than half", _salted["max_share"] < _skewed["max_share"] / 2)
check("the salted join returns exactly the same revenue per country", _a == _b and len(_a) > 0)
check("the two-stage aggregation returns the same result", _same)

# COMMAND ----------

# MAGIC %md
# MAGIC > 🧠 For **joins**, try the cheap fixes first: **AQE skew join** is on by default and splits skewed partitions of a
# MAGIC > sort-merge join automatically; **broadcast** when one side is small. Salting is for when those don't apply — skewed
# MAGIC > aggregations, windows, or two big skewed tables.
# MAGIC
# MAGIC ## Part 6 · Spill
# MAGIC **Spill** = a task's data doesn't fit in execution memory and is written to local disk. Where you see it:
# MAGIC
# MAGIC | Tool | Where |
# MAGIC |---|---|
# MAGIC | Spark UI (classic) | stage page → **Spill (Memory)** / **Spill (Disk)** columns in the Summary Metrics and the task table (they only appear when there is spill) |
# MAGIC | Query profile | operator details → **spilled** bytes / *Spill to disk* |
# MAGIC | `system.query.history` | column `spilled_local_bytes` per statement |
# MAGIC
# MAGIC On serverless, memory is managed for you and these small queries won't spill. The classic way to **provoke** spill is to put
# MAGIC lots of data into very few tasks — exactly what skew does. *Classic cluster (optional)*:
# MAGIC
# MAGIC ```python
# MAGIC spark.conf.set("spark.sql.shuffle.partitions", 2)      # 2 huge reduce tasks
# MAGIC spark.conf.set("spark.sql.adaptive.enabled", False)    # stop AQE from re-planning
# MAGIC events.repartition(2).sortWithinPartitions("customer_id", "event_ts", "product_id").write.format("noop").mode("overwrite").save()
# MAGIC # Spark UI → longest stage → Spill (Memory) / Spill (Disk) > 0 on a small cluster
# MAGIC # Fix: more partitions (or AQE back on), fix skew, fewer columns, memory-optimized workers
# MAGIC ```
# MAGIC
# MAGIC Your own statements on serverless are in **`system.query.history`** (it can lag a few minutes; needs access to the `system`
# MAGIC catalog):

# COMMAND ----------

# DBTITLE 1,My recent statements: duration, shuffle and spill
try:
    display(spark.sql(f"""
        SELECT start_time, total_duration_ms, read_rows, read_files, pruned_files,
               shuffle_read_bytes, spilled_local_bytes, left(statement_text, 80) AS statement
        FROM system.query.history
        WHERE executed_by = current_user() AND start_time >= current_timestamp() - INTERVAL 2 HOURS
        ORDER BY start_time DESC LIMIT 20"""))
except Exception as e:
    print("system.query.history not available:", _first_line(e))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 7 · Caching
# MAGIC The **disk cache** works automatically on serverless (repeated reads of the same Delta files are served from local SSD).
# MAGIC The **Spark cache** (`df.cache()`, `persist()`) is a classic-compute feature:

# COMMAND ----------

# DBTITLE 1,df.cache() on serverless
try:
    _c = events.where("event_type = 'purchase'").cache()
    print("cached rows:", _c.count())
    _c.unpersist()
    print("✅ Spark cache works here (classic compute) - remember to unpersist()")
except Exception as e:
    print("⛔ Spark cache not available here:", _first_line(e))
    print("   → on serverless, rely on the automatic disk cache, or write the intermediate result to a table")

# COMMAND ----------

# MAGIC %md
# MAGIC | Question | Answer |
# MAGIC |---|---|
# MAGIC | `df.cache()` storage level for a DataFrame | `MEMORY_AND_DISK` |
# MAGIC | When is the cache filled? | lazily, by the **next action** |
# MAGIC | How to free it | `df.unpersist()` · `UNCACHE TABLE t` · `spark.catalog.clearCache()` |
# MAGIC | What the disk cache stores | copies of remote **Parquet/Delta files** on local SSD — automatic, always consistent |
# MAGIC
# MAGIC ## ✅ Summary
# MAGIC | Symptom | You measured | Fix you applied |
# MAGIC |---|---|---|
# MAGIC | **Skew** | one task with ~⅓ of the rows (Max ≫ 75th percentile) | **salting** (join and two-stage aggregation); broadcast; AQE skew join |
# MAGIC | **Shuffle** | `Exchange` count in the plan, Shuffle node in the profile | **broadcast** the small table (fewer exchanges) |
# MAGIC | **Spill** | Spill columns / `spilled_local_bytes` | more partitions, fix skew, more memory |
# MAGIC | **Cache** | Spark cache unavailable on serverless | disk cache (automatic) or materialize to a table |
