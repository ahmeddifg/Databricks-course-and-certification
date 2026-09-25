# Databricks notebook source
# MAGIC %md
# MAGIC # 📝 02-Q · Exam Questions — Spark Foundations
# MAGIC **27 exam-style questions** on Spark architecture (02-P1), execution & plans (02-P2) and DataFrames/Spark SQL (02-P3).
# MAGIC These concepts appear inside **D3 Transformation** and **D6 Troubleshooting & Optimization** questions.
# MAGIC
# MAGIC * Click **Check answer** for feedback; **📊 Show my result** for your per-topic score. Target **≥ 80 %**.
# MAGIC * **Choose 2** questions need exactly two answers.

# COMMAND ----------

# MAGIC %run ../../Includes/_quiz

# COMMAND ----------

# DBTITLE 1,Question bank (collapse this cell - it contains the answers)
questions = [
    # ---------------- Architecture ----------------
    {"topic": "Architecture",
     "q": "Which component **builds the execution plan, splits it into tasks and schedules them**?",
     "options": ["An executor", "The driver", "The cluster manager", "The Unity Catalog metastore"],
     "answer": 1,
     "explanation": "The driver runs the SparkSession, turns code into logical/physical plans, creates jobs/stages/tasks and schedules them. Executors run the tasks; the cluster manager allocates resources."},
    {"topic": "Architecture",
     "q": "How many **tasks** does Spark create for a stage that processes a DataFrame with 48 partitions?",
     "options": ["1", "One per executor", "48", "One per core in the cluster"],
     "answer": 2,
     "explanation": "One task per partition. Cores only limit how many of those tasks run at the same time."},
    {"topic": "Architecture",
     "q": "A cluster has **5 workers with 8 cores each**. A stage has 400 partitions. What is the **maximum number of tasks running simultaneously**?",
     "options": ["5", "8", "40", "400"],
     "answer": 2,
     "explanation": "Each core is a task slot: 5 × 8 = 40 slots. The 400 tasks run in 10 waves."},
    {"topic": "Architecture",
     "q": "What creates a **new stage** within a Spark job?",
     "options": ["Every call to `withColumn`", "A shuffle caused by a wide transformation", "Every new notebook cell", "Each executor"],
     "answer": 1,
     "explanation": "Narrow transformations are pipelined in one stage; each shuffle (groupBy, join, orderBy, distinct, repartition) starts a new stage."},
    {"topic": "Architecture",
     "q": "`rows = spark.table(\"events\").collect()` on a 300 GB table fails with an out-of-memory error. Where, and why?",
     "options": ["On the executors, because the table is too large to read",
                 "On the driver, because collect() brings every row back to the driver process",
                 "In the control plane, because notebooks are limited to 1 GB",
                 "On the SQL warehouse, because it is too small"],
     "answer": 1,
     "explanation": "collect()/toPandas() move all data to the driver. Keep processing distributed, write results to a table, or limit() first."},
    {"topic": "Architecture",
     "q": "On **serverless** compute, `df.rdd.getNumPartitions()` fails. Why?",
     "options": ["Serverless DataFrames have no partitions", "Serverless uses Spark Connect, which only supports the DataFrame/SQL APIs — RDD APIs aren't available",
                 "You must enable Photon first", "The table must be cached first"],
     "answer": 1,
     "explanation": "Serverless (and Standard access mode) use Spark Connect: no sparkContext, no RDDs. Use spark_partition_id(), explain() or the query profile instead."},

    # ---------------- Execution & plans ----------------
    {"topic": "Execution & plans",
     "q": "Which of the following is an **action**?",
     "options": ["`df.filter(\"total > 100\")`", "`df.groupBy(\"country\")`", "`df.count()`", "`df.withColumn(\"x\", F.lit(1))`"],
     "answer": 2,
     "explanation": "count() triggers a job and returns a value. filter, groupBy and withColumn are lazy transformations."},
    {"topic": "Execution & plans",
     "q": "Which **two** transformations are **wide** (cause a shuffle)?",
     "options": ["`filter`", "`groupBy().agg()`", "`withColumn`", "`orderBy`"],
     "answer": [1, 3],
     "explanation": "groupBy/agg needs rows with the same key together (hash shuffle); a global orderBy needs a range shuffle. filter and withColumn are narrow."},
    {"topic": "Execution & plans",
     "q": "What is the main benefit of **lazy evaluation**?",
     "options": ["Each line runs immediately so errors are found sooner",
                 "Spark sees the whole pipeline before running it and can optimize it (pushdown, pruning, pipelining, join choice)",
                 "Data is automatically cached in memory", "It removes the need for a driver"],
     "answer": 1,
     "explanation": "Deferring execution lets Catalyst optimize the entire plan at once."},
    {"topic": "Execution & plans",
     "q": "What does this code print?",
     "code": "df = spark.createDataFrame([(1, 10.0)], \"id INT, total DOUBLE\")\ndf.withColumn(\"vat\", df.total * 0.15)\nprint(df.columns)",
     "options": ["`['id', 'total', 'vat']`", "`['id', 'total']`", "An error: withColumn needs an action", "`['vat']`"],
     "answer": 1,
     "explanation": "DataFrames are immutable. withColumn returns a NEW DataFrame that wasn't assigned, so df is unchanged."},
    {"topic": "Execution & plans",
     "q": "A physical plan contains `Exchange hashpartitioning(customer_id#12, 200)`. What does it indicate?",
     "options": ["A broadcast of the customers table", "A shuffle that redistributes rows by customer_id into 200 partitions",
                 "A filter pushed down to the files", "A cache of 200 rows"],
     "answer": 1,
     "explanation": "Exchange = shuffle. hashpartitioning(key, n) means rows are redistributed by key into n (spark.sql.shuffle.partitions) partitions — typical for groupBy or joins on that key."},
    {"topic": "Execution & plans",
     "q": "A join's plan shows `BroadcastExchange` followed by `BroadcastHashJoin`. What does it mean?",
     "options": ["Both tables were shuffled by the join key", "The smaller table was copied to every executor, so the larger table wasn't shuffled",
                 "The join was executed on the driver only", "The join result was cached"],
     "answer": 1,
     "explanation": "A broadcast hash join ships the small side to all executors; each executor joins its partitions of the big side locally — no shuffle of the big table."},
    {"topic": "Execution & plans",
     "q": "In a plan you see `PushedFilters: [IsNotNull(total), GreaterThan(total,500.0)]` on the scan. What optimization is this?",
     "options": ["Constant folding", "Predicate pushdown", "Broadcast join", "Adaptive coalescing"],
     "answer": 1,
     "explanation": "Filters are pushed to the data source so it can skip files/row groups that can't match (data skipping)."},
    {"topic": "Execution & plans",
     "q": "A colleague rewrites a PySpark DataFrame transformation in Spark SQL \"to make it faster\". What happens to performance?",
     "options": ["SQL is always faster", "DataFrame code is always faster",
                 "It stays the same: both are compiled by Catalyst into the same optimized plan", "SQL can't use Photon"],
     "answer": 2,
     "explanation": "SQL and the DataFrame API are two front-ends to the same optimizer and engine."},
    {"topic": "Execution & plans",
     "q": "Which **two** things can **Adaptive Query Execution (AQE)** do at runtime?",
     "options": ["Coalesce many small shuffle partitions into fewer ones", "Create indexes on Delta tables",
                 "Switch a sort-merge join to a broadcast join when one side turns out to be small", "Cache every table that is read twice"],
     "answer": [0, 2],
     "explanation": "AQE uses runtime shuffle statistics to coalesce partitions, change join strategies and split skewed partitions. It doesn't create indexes or cache data."},
    {"topic": "Execution & plans",
     "q": "Which statement about **Photon** is correct?",
     "options": ["It is a Python library you install with %pip",
                 "It is Databricks' vectorized C++ query engine that accelerates SQL/DataFrame operations and is always on for serverless",
                 "It speeds up Python UDFs the most", "It replaces the Catalyst optimizer"],
     "answer": 1,
     "explanation": "Photon executes the physical plan's operators natively. Catalyst still plans the query; Python UDFs aren't accelerated."},
    {"topic": "Execution & plans",
     "q": "A pipeline uses a Python UDF to compute `lower(trim(email))`. What is the best change for performance?",
     "options": ["Increase the cluster size", "Replace the UDF with the built-in functions `F.lower(F.trim(\"email\"))`",
                 "Cache the DataFrame before the UDF", "Convert the DataFrame to pandas"],
     "answer": 1,
     "explanation": "Built-in functions run inside the engine (and Photon); Python UDFs serialize rows to a Python process."},
    {"topic": "Execution & plans",
     "q": "What is the default value of `spark.sql.shuffle.partitions` in Apache Spark?",
     "options": ["8", "64", "200", "Equal to the number of cores"],
     "answer": 2,
     "explanation": "200. On Databricks, AQE coalesces small shuffle partitions automatically, and serverless tunes shuffles for you."},
    {"topic": "Execution & plans",
     "q": "In the Spark UI, one task of a stage runs for 25 minutes while the other 199 finish in 30 seconds. What is the most likely cause?",
     "options": ["Too many executors", "Data skew — one partition holds far more rows than the others",
                 "Photon is disabled", "The driver is too small"],
     "answer": 1,
     "explanation": "A straggler task signals skew. Fixes: AQE skew-join handling, salting the key, handling hot keys separately, broadcasting the small side."},

    # ---------------- DataFrames & Spark SQL ----------------
    {"topic": "DataFrames & SQL",
     "q": "After a filter, a DataFrame has 200 small partitions. You want **10 output files** with the **least data movement**. What do you use?",
     "options": ["`df.repartition(10)`", "`df.coalesce(10)`", "`df.repartition(200)`", "`spark.conf.set(\"spark.sql.shuffle.partitions\", 10)`"],
     "answer": 1,
     "explanation": "coalesce(10) merges existing partitions without a full shuffle. repartition(10) would also work but shuffles everything."},
    {"topic": "DataFrames & SQL",
     "q": "A DataFrame has 4 large partitions and you need **64** partitions for more parallelism. What do you use?",
     "options": ["`df.coalesce(64)`", "`df.repartition(64)`", "`df.limit(64)`", "`df.cache()`"],
     "answer": 1,
     "explanation": "Only repartition can increase the number of partitions (full shuffle). coalesce can only decrease."},
    {"topic": "DataFrames & SQL",
     "q": "`df1` has columns `(id, name)` and `df2` has `(name, id)`. Which call stacks them correctly?",
     "options": ["`df1.union(df2)`", "`df1.unionByName(df2)`", "`df1.join(df2)`", "`df1.intersect(df2)`"],
     "answer": 1,
     "explanation": "union() matches columns by POSITION (it would put names into id). unionByName() matches by column name."},
    {"topic": "DataFrames & SQL",
     "q": "Why should production pipelines usually **declare a schema** instead of using `inferSchema`?",
     "options": ["Inference is not supported for CSV", "Inference needs an extra pass over the data and can guess wrong types",
                 "Declared schemas disable Photon", "Inferred schemas can't be saved to Delta"],
     "answer": 1,
     "explanation": "Schema inference reads data an extra time and may mistype columns (e.g. codes with leading zeros). Declaring the schema is faster and safer."},
    {"topic": "DataFrames & SQL",
     "q": "`df.write.saveAsTable(\"sales\")` runs a second time and the table already exists. What happens with the **default** save mode?",
     "options": ["Rows are appended", "The table is overwritten", "It raises an error", "Nothing happens"],
     "answer": 2,
     "explanation": "The default mode is errorifexists. Use mode(\"append\"), mode(\"overwrite\") or mode(\"ignore\") explicitly."},
    {"topic": "DataFrames & SQL",
     "q": "Which **two** formats are **self-describing** (store their own schema)?",
     "options": ["CSV", "Parquet", "JSON", "TXT"],
     "answer": [1, 2],
     "explanation": "Parquet stores a typed schema in the file; JSON records carry field names. CSV/TXT need a header/delimiter/schema from you."},
    {"topic": "DataFrames & SQL",
     "q": "Which approach matches the **lakehouse / medallion** way of working?",
     "options": ["ETL: transform in a staging server, load only clean data", "ELT: load raw data into the lakehouse (bronze), then transform it with Spark into silver and gold",
                 "Load only aggregated data", "Transform on the analyst's laptop"],
     "answer": 1,
     "explanation": "ELT keeps raw data for reprocessing and uses the lakehouse's distributed compute to transform it."},
    {"topic": "DataFrames & SQL",
     "q": "Which statement about `df.cache()` is correct?",
     "options": ["It immediately stores the DataFrame in memory",
                 "It is lazy — data is stored on the first action — and it is not supported on serverless compute",
                 "It writes the DataFrame to a Delta table", "It is required before every join"],
     "answer": 1,
     "explanation": "cache()/persist() mark the DataFrame; the first action materializes it. Use it only for reused results and unpersist() afterwards. Serverless doesn't support the cache APIs."},
]

render_quiz(questions, title="Section 02 · Spark Foundations",
            meta="27 questions · architecture, execution plans, DataFrames & Spark SQL · target ≥ 80 %")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 📌 If you scored below 80 %
# MAGIC | Weak topic | Review |
# MAGIC |---|---|
# MAGIC | Architecture | `presentation/02-P1 - Spark Architecture` |
# MAGIC | Execution & plans | `presentation/02-P2 - How Spark Executes Your Code` + `labs/02-L1` |
# MAGIC | DataFrames & SQL | `presentation/02-P3 - DataFrames and Spark SQL Essentials` + `labs/02-L2` |

# COMMAND ----------

# DBTITLE 1,Optional - print the answer key
# print_answer_key(questions)
