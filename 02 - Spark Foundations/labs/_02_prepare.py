# Databricks notebook source
# MAGIC %md
# MAGIC # 🔧 _02_prepare — Section 02 tables & plan helpers
# MAGIC Included by every Section 02 lab with `%run ./_02_prepare` (after `%run ../../Includes/_setup`).
# MAGIC
# MAGIC Creates (only if missing) three Delta tables in the course schema:
# MAGIC
# MAGIC | Table | Rows | Columns |
# MAGIC |---|---|---|
# MAGIC | `lab02_orders` | 2,010 | `order_id, order_ts, customer_id, quantity, total, items` |
# MAGIC | `lab02_customers` | 300 | `customer_id, first_name, last_name, city, country, email` |
# MAGIC | `lab02_products` | 36 | `product_id, title, brand, category, price` |
# MAGIC
# MAGIC and defines helpers:
# MAGIC * `plan_text(query_or_df, mode="SIMPLE")` → the physical plan as a string (works on serverless)
# MAGIC * `has_shuffle(plan)` / `join_strategy(plan)` → read a plan for you
# MAGIC * `partition_sizes(df)` → rows per partition (uses `spark_partition_id()`; works without `df.rdd`)

# COMMAND ----------

# DBTITLE 1,Create the Section 02 tables (idempotent)
from pyspark.sql import functions as F


def _prepare_lab02_tables(force: bool = False):
    created = []
    if force or not spark.catalog.tableExists("lab02_orders"):
        (spark.read.parquet(f"{dataset_path}/orders-parquet")
              .withColumn("order_ts", F.timestamp_seconds("order_timestamp"))
              .select("order_id", "order_ts", "customer_id", "quantity", "total", "items")
              .write.mode("overwrite").saveAsTable("lab02_orders"))
        created.append("lab02_orders")
    if force or not spark.catalog.tableExists("lab02_customers"):
        (spark.read.json(f"{dataset_path}/customers-json")
              .select("customer_id",
                      F.get_json_object("profile", "$.first_name").alias("first_name"),
                      F.get_json_object("profile", "$.last_name").alias("last_name"),
                      F.get_json_object("profile", "$.address.city").alias("city"),
                      F.get_json_object("profile", "$.address.country").alias("country"),
                      "email")
              .write.mode("overwrite").saveAsTable("lab02_customers"))
        created.append("lab02_customers")
    if force or not spark.catalog.tableExists("lab02_products"):
        (spark.read.option("header", "true").option("delimiter", ";").option("inferSchema", "true")
              .csv(f"{dataset_path}/products-csv")
              .write.mode("overwrite").saveAsTable("lab02_products"))
        created.append("lab02_products")
    return created


_created02 = _prepare_lab02_tables()
print("📦 Section 02 tables ready" + (f" (created: {', '.join(_created02)})" if _created02 else ""))

# COMMAND ----------

# DBTITLE 1,Plan & partition helpers
def plan_text(query_or_df, mode: str = "SIMPLE") -> str:
    """Physical plan of a SQL string or a DataFrame, as text. mode: SIMPLE | EXTENDED | FORMATTED | COST.
    Uses SQL EXPLAIN, so it works on serverless (Spark Connect) and classic compute alike."""
    if isinstance(query_or_df, str):
        query = query_or_df
    else:
        query_or_df.createOrReplaceTempView("_lab02_plan_v")
        query = "SELECT * FROM _lab02_plan_v"
    keyword = "" if mode.upper() == "SIMPLE" else mode.upper()
    return spark.sql(f"EXPLAIN {keyword} {query}").first()[0]


def has_shuffle(plan: str) -> bool:
    """True if the plan contains a shuffle Exchange (broadcast exchanges don't count)."""
    for line in plan.splitlines():
        if "Exchange" in line and "Broadcast" not in line and "Reused" not in line:
            return True
    return False


def join_strategy(plan: str) -> str:
    """'broadcast', 'shuffle' or 'none' - works for classic and Photon operator names."""
    if "BroadcastHashJoin" in plan or "BroadcastNestedLoopJoin" in plan:
        return "broadcast"
    if "SortMergeJoin" in plan or "ShuffledHashJoin" in plan:
        return "shuffle"
    return "none"


def partition_sizes(df):
    """Rows per (non-empty) partition of df, computed with spark_partition_id()."""
    return (df.groupBy(F.spark_partition_id().alias("partition")).count()
              .withColumnRenamed("count", "rows").orderBy("partition"))


def num_partitions(df) -> int:
    """Number of NON-EMPTY partitions (df.rdd.getNumPartitions() isn't available on serverless)."""
    return df.select(F.spark_partition_id().alias("p")).distinct().count()


print("🔎 Helpers ready: plan_text(), has_shuffle(), join_strategy(), partition_sizes(), num_partitions()")
