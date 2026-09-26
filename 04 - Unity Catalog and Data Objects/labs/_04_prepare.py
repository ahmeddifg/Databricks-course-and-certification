# Databricks notebook source
# MAGIC %md
# MAGIC # 🔧 _04_prepare — Section 04 lab catalog & helpers
# MAGIC Included by the Section 04 labs with `%run ./_04_prepare` (after `%run ../../Includes/_setup`). Nothing is created until you call a helper.
# MAGIC
# MAGIC * **`LAB_CATALOG_NAME`** — your own lab catalog, unique per user: `lab04_<your-user-name>` (catalog names are shared by the whole metastore).
# MAGIC * **`ensure_lab_catalog()`** — creates that catalog if you're allowed to (Free Edition: yes) and sets the names below.
# MAGIC   If you can't create catalogs (some company workspaces), it falls back to the course catalog and uses schemas named `lab04_bronze`, … instead.
# MAGIC * **`BRONZE`, `SILVER`, `GOLD`** — the three lab schemas as `catalog.schema` strings, e.g. `lab04_ahmed.bronze`.
# MAGIC * **`build_lab_tables()`** — creates `silver.customers`, `silver.products` and `silver.orders` if missing (called by 04-L2 and 04-D, so they also work if you skipped 04-L1).
# MAGIC * **`run_sql(stmt)`** — prints the SQL statement it runs (so you always see the real three-level names), then runs it.
# MAGIC * **`table_type(full_name)`** — `MANAGED`, `EXTERNAL`, `VIEW`, `MATERIALIZED_VIEW`… read from `information_schema.tables`.
# MAGIC * **`restore_course_context()`**, **`drop_lab_catalog()`** — go back to `<course catalog>.shopwave` / remove everything Section 04 created.

# COMMAND ----------

# DBTITLE 1,Lab catalog name + helpers
import re
import textwrap
from pyspark.sql import functions as F

_user = spark.sql("SELECT current_user()").first()[0]
LAB_CATALOG_NAME = "lab04_" + (re.sub(r"[^a-z0-9]+", "_", _user.split("@")[0].lower()).strip("_") or "learner")

# Filled in by ensure_lab_catalog()
LAB_CATALOG, SCHEMA_PREFIX, CATALOG_CREATED = None, None, None
BRONZE = SILVER = GOLD = None


def run_sql(stmt: str):
    """Print a SQL statement, run it and return the DataFrame."""
    stmt = textwrap.dedent(stmt).strip()
    print("▶ " + stmt.replace("\n", "\n  "))
    return spark.sql(stmt)


def _set_names(catalog, prefix, created):
    global LAB_CATALOG, SCHEMA_PREFIX, CATALOG_CREATED, BRONZE, SILVER, GOLD
    LAB_CATALOG, SCHEMA_PREFIX, CATALOG_CREATED = catalog, prefix, created
    BRONZE, SILVER, GOLD = (f"{catalog}.{prefix}{layer}" for layer in ("bronze", "silver", "gold"))


def ensure_lab_catalog(verbose: bool = True):
    """Create (if needed) the per-user lab catalog. Idempotent. Returns the catalog to use."""
    try:
        stmt = (f"CREATE CATALOG IF NOT EXISTS {LAB_CATALOG_NAME} "
                "COMMENT 'Section 04 lab catalog - safe to drop'")
        if verbose:
            run_sql(stmt)
        else:
            spark.sql(stmt)
        _set_names(LAB_CATALOG_NAME, "", True)
    except Exception as e:
        if verbose or CATALOG_CREATED is None:
            print("ℹ️ You can't create catalogs in this workspace, so the labs use schemas named lab04_* in the "
                  f"course catalog `{catalog_name}` instead.\n   ", str(e).strip().splitlines()[0][:160])
        _set_names(catalog_name, "lab04_", False)
    if verbose:
        print(f"\nLAB_CATALOG = {LAB_CATALOG}\nBRONZE = {BRONZE} | SILVER = {SILVER} | GOLD = {GOLD}")
    return LAB_CATALOG


def ensure_lab_schemas():
    """Create the three lab schemas (used by 04-L2/04-L3 if you skipped 04-L1)."""
    ensure_lab_catalog(verbose=False)
    for full, layer in ((BRONZE, "raw"), (SILVER, "cleaned"), (GOLD, "business-ready")):
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {full} COMMENT 'Section 04 {layer} layer'")


def build_lab_tables():
    """Create the silver tables used by 04-L2/04-L3 if they are missing (04-L1 builds them step by step)."""
    ensure_lab_schemas()
    built = []
    if not spark.catalog.tableExists(f"{SILVER}.customers"):
        spark.sql(f"""
            CREATE TABLE {SILVER}.customers
            COMMENT 'Cleaned customers - one row per customer_id (PII: email)'
            AS SELECT customer_id, email,
                      profile:first_name::string AS first_name, profile:last_name::string AS last_name,
                      profile:address:city::string AS city, profile:address:country::string AS country,
                      to_timestamp(updated) AS updated_at
               FROM read_files('{dataset_path}/customers-json', format => 'json')""")
        built.append("silver.customers")
    if not spark.catalog.tableExists(f"{SILVER}.products"):
        spark.sql(f"""
            CREATE TABLE {SILVER}.products
            AS SELECT product_id, title, brand, category, CAST(price AS DECIMAL(10,2)) AS price
               FROM read_files('{dataset_path}/products-csv', format => 'csv', header => true, delimiter => ';')""")
        built.append("silver.products")
    if not spark.catalog.tableExists(f"{SILVER}.orders"):
        (spark.read.parquet(f"{dataset_path}/orders-parquet")
              .dropDuplicates(["order_id"])
              .where("quantity > 0")
              .select("order_id", "customer_id",
                      F.timestamp_seconds("order_timestamp").alias("order_ts"), "quantity", "total")
              .write.saveAsTable(f"{SILVER}.orders"))
        built.append("silver.orders")
    print("✅ Lab tables ready" + (f" (built: {', '.join(built)})" if built else " (already there)"))


def table_type(full_name: str):
    """MANAGED / EXTERNAL / VIEW / MATERIALIZED_VIEW ... for catalog.schema.table (None if it doesn't exist)."""
    cat, sch, tbl = full_name.split(".")
    row = (spark.table(f"{cat}.information_schema.tables")
                .where((F.col("table_schema") == sch) & (F.col("table_name") == tbl))
                .select("table_type").first())
    return None if row is None else row[0]


def restore_course_context():
    """Go back to the course catalog/schema (the labs switch context with USE)."""
    spark.sql(f"USE CATALOG `{catalog_name}`")
    spark.sql(f"USE SCHEMA `{schema_name}`")
    print(f"Current context restored to {catalog_name}.{schema_name}")


def drop_lab_catalog():
    """Remove everything Section 04 created (its catalog, or its lab04_* schemas in fallback mode)."""
    restore_course_context()
    if LAB_CATALOG_NAME in [r[0] for r in spark.sql("SHOW CATALOGS").collect()]:
        _set_names(LAB_CATALOG_NAME, "", True)
    else:
        _set_names(catalog_name, "lab04_", False)
    if CATALOG_CREATED:
        run_sql(f"DROP CATALOG IF EXISTS {LAB_CATALOG} CASCADE")
    else:
        for s in [r[0] for r in spark.sql(f"SHOW SCHEMAS IN `{catalog_name}`").collect()]:
            if s.startswith("lab04_"):
                run_sql(f"DROP SCHEMA IF EXISTS `{catalog_name}`.`{s}` CASCADE")
    print("🧹 Section 04 objects removed")


print(f"🔧 Section 04 helpers ready - your lab catalog will be `{LAB_CATALOG_NAME}`")
