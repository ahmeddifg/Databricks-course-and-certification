# Databricks notebook source
# MAGIC %md
# MAGIC # 🔗 07-P2 · Combining and Aggregating Data
# MAGIC **Section 07 — Data Transformation & Modeling** · exam domain **D3 (22 %)** · objectives: *combine DataFrames (inner,
# MAGIC left, broadcast, multiple keys, cross join, union, union all)* · *aggregations* · *basic tuning parameters
# MAGIC (`spark.sql.shuffle.partitions`, `spark.default.parallelism`, executor/driver memory, `spark.sql.autoBroadcastJoinThreshold`)
# MAGIC and re-measuring performance*
# MAGIC
# MAGIC | After this presentation you can… |
# MAGIC |---|
# MAGIC | Predict the rows returned by every **join type**, including **semi**, **anti** and **cross** joins |
# MAGIC | Join on **multiple keys** and avoid **ambiguous columns** and **row explosions** |
# MAGIC | Explain **broadcast joins** and the **tuning parameters** the exam lists — and how to measure their effect |
# MAGIC | Choose between `union`, `unionByName`, `UNION`, `UNION ALL`, `INTERSECT`, `EXCEPT` |
# MAGIC | Aggregate with `groupBy().agg()`, **pivot**, `collect_list` / `collect_set` |
# MAGIC | Use **window functions** (ranking, `lag`/`lead`, running totals) and **higher-order functions** on arrays |
# MAGIC
# MAGIC > ▶️ **Run all** to render the diagrams (a few seconds, any compute).

# COMMAND ----------

# MAGIC %run ../../Includes/_style

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · Join types

# COMMAND ----------

# DBTITLE 1,Slide · Join types
show("""
<div class="kicker">Slide 1 · Joins</div>
<h2>orders (left) ⋈ customers (right) on customer_id</h2>
<table class="tbl">
<tr><th>Join</th><th>PySpark <code>how=</code></th><th>Result</th><th>Typical use</th></tr>
<tr><td><b>inner</b></td><td><code>"inner"</code> (default)</td><td>only matching pairs</td><td>enrich with a mandatory dimension</td></tr>
<tr><td><b>left outer</b></td><td><code>"left"</code></td><td>all orders; customer columns NULL when no match</td><td>enrich without losing facts</td></tr>
<tr><td><b>right outer</b></td><td><code>"right"</code></td><td>all customers; order columns NULL when no match</td><td>rarely — swap sides and use left</td></tr>
<tr><td><b>full outer</b></td><td><code>"full"</code></td><td>everything from both sides</td><td>reconciliation of two sources</td></tr>
<tr><td><b>left semi</b></td><td><code>"left_semi"</code></td><td>left rows that <b>have</b> a match — left columns only, no duplicates</td><td>filter: “customers who ordered” (≈ <code>EXISTS</code>)</td></tr>
<tr><td><b>left anti</b></td><td><code>"left_anti"</code></td><td>left rows with <b>no</b> match</td><td>orphans, “not yet processed” (≈ <code>NOT EXISTS</code>)</td></tr>
<tr><td><b>cross</b></td><td><code>crossJoin(df)</code></td><td>every combination: n × m rows</td><td>build a complete grid (all countries × all months)</td></tr>
</table>
""" + callout("exam", "Count logic: <b>left = inner + unmatched left</b>, <b>right = inner + unmatched right</b>, "
              "<b>full = inner + both unmatched</b>, <b>semi + anti = left row count</b>.")
  + callout("trap", "A join key that is <b>not unique on both sides</b> multiplies rows (many-to-many). A customers table with a "
            "duplicated customer_id doubles that customer's orders after the join — deduplicate dimensions first."))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · Join syntax and pitfalls
# MAGIC
# MAGIC ```python
# MAGIC # 1. Column name(s) -> ONE key column in the result (like SQL USING)
# MAGIC orders.join(customers, "customer_id")                         # inner
# MAGIC orders.join(customers, "customer_id", "left")
# MAGIC revenue.join(targets, ["country", "month"], "left")           # multiple keys, same names
# MAGIC
# MAGIC # 2. Expression -> BOTH key columns kept; needed when names differ
# MAGIC a.join(b, (a.country == b.cntry) & (a.month == b.period), "left")
# MAGIC
# MAGIC # 3. Aliases avoid ambiguity
# MAGIC orders.alias("o").join(customers.alias("c"), F.col("o.customer_id") == F.col("c.customer_id")).select("o.*", "c.country")
# MAGIC ```
# MAGIC
# MAGIC | Pitfall | Symptom | Fix |
# MAGIC |---|---|---|
# MAGIC | expression join on same-named columns | `[AMBIGUOUS_REFERENCE] customer_id` on a later `select` | join on the **name** (`"customer_id"`), or use aliases / `drop(b.customer_id)` |
# MAGIC | NULL keys | NULL never equals NULL → rows don't match | clean keys, or `a.k.eqNullSafe(b.k)` / `<=>` |
# MAGIC | duplicated keys | row counts grow unexpectedly | check uniqueness: `groupBy(key).count().where("count > 1")` |
# MAGIC | filtering the right side of a left join in `WHERE` | left join behaves like inner | put the condition in the `ON` clause |
# MAGIC
# MAGIC ## 3 · Broadcast joins and tuning parameters

# COMMAND ----------

# DBTITLE 1,Slide · Broadcast join
show("""
<div class="kicker">Slide 3 · Join strategies</div>
<h2>Small table? Send it everywhere instead of shuffling the big one</h2>
<div class="grid two">
 <div class="card green"><h3>📡 Broadcast hash join</h3><ul>
  <li>the small side is copied to <b>every executor</b></li>
  <li>the big side is <b>not shuffled</b> → fast</li>
  <li>automatic below <code>spark.sql.autoBroadcastJoinThreshold</code> (10 MB in Apache Spark; <code>-1</code> disables)</li>
  <li>force: <code>F.broadcast(df)</code> · SQL <code>/*+ BROADCAST(t) */</code></li></ul></div>
 <div class="card orange"><h3>🔀 Shuffle join (sort-merge / shuffled hash)</h3><ul>
  <li>both sides <b>shuffled</b> by the key</li>
  <li>for two large tables</li>
  <li>number of shuffle partitions = <code>spark.sql.shuffle.partitions</code></li>
  <li>AQE can switch to broadcast at runtime when a side turns out small</li></ul></div>
</div>
""" + callout("trap", "Broadcasting a table that is too large puts it in the memory of the driver and every executor → "
              "out-of-memory errors. Broadcast only genuinely small sides (dimensions, lookups)."))

# COMMAND ----------

# DBTITLE 1,Slide · Tuning parameters
show("""
<div class="kicker">Slide 3b · The tuning parameters on the exam</div>
<h2>Change one thing, then re-measure</h2>
<table class="tbl">
<tr><th>Parameter</th><th>What it controls</th><th>Default</th><th>Set where</th></tr>
<tr><td><code>spark.sql.shuffle.partitions</code></td><td>partitions after a shuffle in DataFrame/SQL (joins, groupBy)</td><td>200 (serverless: <code>auto</code>)</td><td><code>spark.conf.set</code> at runtime · also on serverless</td></tr>
<tr><td><code>spark.default.parallelism</code></td><td>default partitions for <b>RDD</b> operations</td><td>total cores</td><td>cluster Spark config</td></tr>
<tr><td><code>spark.executor.memory</code></td><td>heap per executor (bigger partitions, joins, caching)</td><td>depends on node type</td><td>cluster Spark config — before start</td></tr>
<tr><td><code>spark.driver.memory</code></td><td>heap of the driver (<code>collect()</code>, broadcast building, many files)</td><td>depends on node type</td><td>cluster Spark config — before start</td></tr>
<tr><td><code>spark.sql.autoBroadcastJoinThreshold</code></td><td>max size for automatic broadcast</td><td>10 MB</td><td>runtime (classic) · hints on serverless</td></tr>
</table>
<div class="flow">
 <div class="step"><b>1 · Measure</b>run the query, note duration, look at the query profile (serverless) / Spark UI (classic): spill? skew? shuffle size?</div><div class="arrow">➜</div>
 <div class="step"><b>2 · Change one</b>e.g. shuffle partitions 200 → 64, or broadcast a small side</div><div class="arrow">➜</div>
 <div class="step"><b>3 · Re-measure</b>same query, same data — keep the change only if it helps</div>
</div>
""" + callout("info", "<b>Serverless</b> manages memory, parallelism and the broadcast threshold for you — only a few settings "
              "(e.g. <code>spark.sql.shuffle.partitions</code>, <code>spark.sql.ansi.enabled</code>, <code>spark.sql.session.timeZone</code>) "
              "can be set. Setting others raises an error. Tune via hints, data layout and query design instead."))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · Stacking rows: unions and set operations

# COMMAND ----------

# DBTITLE 1,Slide · Unions
show("""
<div class="kicker">Slide 4 · union, unionByName, UNION, UNION ALL</div>
<h2>DataFrame <code>union</code> = SQL <code>UNION ALL</code>, matched by <u>position</u></h2>
<table class="tbl">
<tr><th>PySpark</th><th>Columns matched by</th><th>Duplicates</th><th>SQL</th></tr>
<tr><td><code>a.union(b)</code> (alias <code>unionAll</code>)</td><td><b>position</b></td><td>kept</td><td><code>UNION ALL</code></td></tr>
<tr><td><code>a.union(b).distinct()</code></td><td>position</td><td>removed</td><td><code>UNION</code> (= <code>UNION DISTINCT</code>)</td></tr>
<tr><td><code>a.unionByName(b)</code></td><td><b>name</b></td><td>kept</td><td>—</td></tr>
<tr><td><code>a.unionByName(b, allowMissingColumns=True)</code></td><td>name; missing → NULL</td><td>kept</td><td>—</td></tr>
<tr><td><code>a.intersect(b)</code> · <code>intersectAll</code></td><td>position</td><td>rows in both</td><td><code>INTERSECT</code> · <code>INTERSECT ALL</code></td></tr>
<tr><td><code>a.subtract(b)</code> · <code>a.exceptAll(b)</code></td><td>position</td><td>rows in a, not in b</td><td><code>EXCEPT</code> (= <code>MINUS</code>) · <code>EXCEPT ALL</code></td></tr>
</table>
""" + callout("trap", "All set operations need the <b>same number of columns</b> with compatible types. If two string columns are "
              "swapped, <code>union</code> does <b>not</b> fail — values are mixed silently. Use <code>unionByName</code>."))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 · Aggregations
# MAGIC
# MAGIC ```python
# MAGIC (items.groupBy("category")
# MAGIC       .agg(F.count("*").alias("lines"),                 # rows
# MAGIC            F.count("discount").alias("with_discount"),  # non-NULL values only
# MAGIC            F.sum("amount").alias("revenue"),
# MAGIC            F.avg("amount").alias("avg_line"),            # = F.mean
# MAGIC            F.min("amount"), F.max("amount"),
# MAGIC            F.count_distinct("customer_id").alias("customers"),
# MAGIC            F.approx_count_distinct("customer_id").alias("customers_approx"),
# MAGIC            F.collect_set("brand").alias("brands")))      # array of distinct values
# MAGIC ```
# MAGIC
# MAGIC | Need | PySpark | SQL |
# MAGIC |---|---|---|
# MAGIC | aggregate the whole table | `df.agg(F.sum("x"))` | `SELECT sum(x) FROM t` |
# MAGIC | filter **after** aggregating | `.agg(...).where("revenue > 1000")` | `HAVING sum(x) > 1000` |
# MAGIC | values → columns | `.groupBy("category").pivot("month", ["2026-01", …]).agg(F.sum("amount"))` | `PIVOT (sum(amount) FOR month IN ('2026-01', …))` |
# MAGIC | rows → array | `F.collect_list("x")` (duplicates) / `F.collect_set("x")` (distinct) | same names |
# MAGIC | subtotals | `df.rollup("country", "city")` / `df.cube(...)` | `GROUP BY ROLLUP (…)` / `CUBE (…)` |
# MAGIC
# MAGIC > 💡 Give `pivot` the list of values: Spark then doesn't need an extra job to find them, and the column order is fixed.
# MAGIC
# MAGIC ## 6 · Window functions

# COMMAND ----------

# DBTITLE 1,Slide · Window functions
show("""
<div class="kicker">Slide 6 · Window functions</div>
<h2>Compute across related rows — <u>without</u> collapsing them</h2>
<div class="grid two">
 <div class="card"><h3>Anatomy</h3>
 <code>w = Window.partitionBy("customer_id").orderBy("order_ts")</code><br>
 <code>F.row_number().over(w)</code><br><br>
 SQL: <code>row_number() OVER (PARTITION BY customer_id ORDER BY order_ts)</code><br><br>
 Frame: <code>.rowsBetween(Window.unboundedPreceding, Window.currentRow)</code> = running total</div>
 <div class="card green"><h3>Ranking — values 100, 90, 90, 80</h3>
 <code>row_number()</code> → 1, 2, 3, 4<br><code>rank()</code> → 1, 2, 2, 4 (gap)<br><code>dense_rank()</code> → 1, 2, 2, 3 (no gap)<br>
 <code>ntile(4)</code>, <code>percent_rank()</code> → buckets / percentiles</div>
</div>
<table class="tbl">
<tr><th>Function</th><th>Use</th></tr>
<tr><td><code>lag(c, 1)</code> / <code>lead(c, 1)</code></td><td>previous / next row's value: time between orders, change vs last month</td></tr>
<tr><td><code>sum/avg/count(c).over(w)</code></td><td>running totals, moving averages, share of the group total (no <code>orderBy</code> = whole partition)</td></tr>
<tr><td><code>first/last(c).over(w)</code></td><td>first/last value in the window</td></tr>
</table>
""" + callout("exam", "Top-N per group and “latest record per key” are window questions: <code>row_number()</code> (exactly one per group) "
              "vs <code>rank()</code>/<code>dense_rank()</code> (ties share a rank).")
  + callout("info", "With an <code>orderBy</code> and no explicit frame, aggregate windows default to <i>RANGE BETWEEN UNBOUNDED PRECEDING "
            "AND CURRENT ROW</i> — rows with the same order value are included together. Use <code>rowsBetween</code> for strict row-by-row running totals."))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7 · Arrays and higher-order functions
# MAGIC
# MAGIC | Function | Example | Result |
# MAGIC |---|---|---|
# MAGIC | `transform(arr, x -> …)` | `transform(items, i -> i.product_id)` | array of product ids |
# MAGIC | `filter(arr, x -> …)` | `filter(items, i -> i.quantity >= 2)` | only matching elements |
# MAGIC | `exists(arr, x -> …)` | `exists(items, i -> i.product_id = 'P001')` | true / false |
# MAGIC | `aggregate(arr, start, (acc, x) -> …)` | `aggregate(items, 0D, (acc, i) -> acc + i.subtotal)` | one value (order total) |
# MAGIC | `size`, `array_contains`, `array_distinct`, `sort_array`, `flatten`, `array_union` | `flatten(collect_list(products))` | array helpers |
# MAGIC
# MAGIC PySpark uses Python lambdas: `F.transform("items", lambda i: i["product_id"])`, `F.filter(...)`, `F.exists(...)`,
# MAGIC `F.aggregate(...)`. HOFs work **inside** each row — no `explode` + `groupBy` round trip.
# MAGIC
# MAGIC ## ✅ Key takeaways
# MAGIC 1. Join types: inner, left, right, full, **left_semi** (exists), **left_anti** (not exists — orphans), **cross** (n × m grid).
# MAGIC 2. Join on column **names** (`"k"` or `["k1", "k2"]`) to get one key column; expressions keep both → ambiguity. NULL keys never match; duplicate keys multiply rows.
# MAGIC 3. **Broadcast** the small side (`F.broadcast`, `/*+ BROADCAST */`, auto below `autoBroadcastJoinThreshold`); shuffle joins for two large sides.
# MAGIC 4. Tuning: `spark.sql.shuffle.partitions`, `spark.default.parallelism` (RDDs), executor/driver **memory** (cluster config), broadcast threshold — **measure, change one, re-measure**. Serverless manages most of them.
# MAGIC 5. `union` = `UNION ALL` **by position**; `unionByName` by name (+ `allowMissingColumns`); SQL `UNION` removes duplicates; `INTERSECT`, `EXCEPT`.
# MAGIC 6. `groupBy().agg()` with `count`, `count_distinct`, `approx_count_distinct`, `avg`/`mean`, `collect_list`/`collect_set`; **pivot** turns values into columns.
# MAGIC 7. Windows: `partitionBy` + `orderBy` (+ frame); `row_number` / `rank` / `dense_rank`, `lag` / `lead`, running totals.
# MAGIC 8. HOFs `transform`, `filter`, `exists`, `aggregate` process arrays in place.
# MAGIC
# MAGIC ➡️ Next: **07-P3 · Modeling Gold: Star Schemas, SCD and Data Quality**
