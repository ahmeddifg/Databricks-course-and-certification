# =====================================================================================
# 08-L3 · ShopWave customers CDC pipeline (Python)
# Lakeflow Spark Declarative Pipelines source file (a plain .py FILE, not a notebook).
# Functions only DEFINE datasets - the pipeline decides when and how to run them.
# Never call collect(), count(), display(), write or start() here.
# =====================================================================================
from pyspark import pipelines as dp            # legacy: import dlt
from pyspark.sql import functions as F

# Pipeline Configuration key -> Spark conf (set in Settings > Configuration)
DATASET_PATH = spark.conf.get("dataset_path")
CDC_PATH = f"{DATASET_PATH}/lab08/customers-cdc"


# ---------------------------------------------------------------- BRONZE
# @dp.table + a STREAMING DataFrame  ->  streaming table
@dp.table(
    name="cdc_customers_raw",
    comment="Raw customer change events (INSERT / UPDATE / DELETE) loaded with Auto Loader",
)
def cdc_customers_raw():
    return (
        spark.readStream.format("cloudFiles")            # Auto Loader - no checkpoint / schemaLocation needed here
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(CDC_PATH)
        .select("*", F.col("_metadata.file_name").alias("source_file"))
    )


# ---------------------------------------------------------------- SILVER (input of AUTO CDC)
# Temporary view: not published to the catalog, but it can carry expectations.
VALID_EVENT = {
    "valid_operation": "operation IN ('INSERT', 'UPDATE', 'DELETE')",
    "has_sequence": "sequence_num IS NOT NULL",
}


@dp.temporary_view(name="cdc_customers_clean")
@dp.expect_all_or_drop(VALID_EVENT)                      # drop events that AUTO CDC could not apply safely
@dp.expect_or_fail("has_key", "customer_id IS NOT NULL")  # a change without a key is a broken feed -> stop
@dp.expect("has_email", "email IS NOT NULL")               # warn only: keep the row, count it
def cdc_customers_clean():
    return (
        spark.readStream.table("cdc_customers_raw")      # another dataset of this pipeline - referenced by name
        .withColumn("change_ts", F.to_timestamp("change_ts"))
        .drop("_rescued_data")
    )


# ---------------------------------------------------------------- SCD TYPE 1: current state only
dp.create_streaming_table(
    name="cdc_customers_scd1",
    comment="Current customers - updates overwrite, deletes remove (SCD type 1)",
)

dp.create_auto_cdc_flow(                                  # legacy name: dlt.apply_changes(...)
    target="cdc_customers_scd1",
    source="cdc_customers_clean",
    keys=["customer_id"],
    sequence_by=F.col("sequence_num"),                   # decides which change wins - not the arrival order
    apply_as_deletes=F.expr("operation = 'DELETE'"),
    except_column_list=["operation", "sequence_num", "source_file"],
    stored_as_scd_type=1,
)


# ---------------------------------------------------------------- SCD TYPE 2: full history
dp.create_streaming_table(
    name="cdc_customers_scd2",
    comment="Customer history - one row per version with __START_AT / __END_AT (SCD type 2)",
)

dp.create_auto_cdc_flow(
    target="cdc_customers_scd2",
    source="cdc_customers_clean",
    keys=["customer_id"],
    sequence_by=F.col("sequence_num"),
    apply_as_deletes=F.expr("operation = 'DELETE'"),
    except_column_list=["operation", "sequence_num", "source_file"],
    stored_as_scd_type=2,
    track_history_except_column_list=["change_ts"],      # a change of change_ts alone would not open a new version
)


# ---------------------------------------------------------------- GOLD
# @dp.materialized_view + a BATCH DataFrame -> materialized view
@dp.materialized_view(
    name="cdc_customers_per_country",
    comment="Current number of customers per country",
)
def cdc_customers_per_country():
    return (
        spark.read.table("cdc_customers_scd1")
        .groupBy("country")
        .agg(F.count("*").alias("customers"))
    )
