# Databricks notebook source
# MAGIC %md
# MAGIC # 🧪 Lab 04-L2 · Views, Volumes & Functions (Guided)
# MAGIC **Time:** ~50 min · **Compute:** Serverless (Free Edition) · Uses the lab catalog from 04-L1
# MAGIC (it is rebuilt automatically if you skipped 04-L1).
# MAGIC
# MAGIC | Part | You will… |
# MAGIC |---|---|
# MAGIC | 1 | Create a **stored view** and prove it holds **no data** |
# MAGIC | 2 | Create **temporary views** (SQL and DataFrame) |
# MAGIC | 3 | Prove a temp view lives only in **your Spark session** |
# MAGIC | 4 | Try a **global temporary view** (classic compute only) |
# MAGIC | 5 | Create and refresh a **materialized view** |
# MAGIC | 6 | Create a **volume**, put files in it, list and query them |
# MAGIC | 7 | Create **SQL** and **Python** user-defined functions in Unity Catalog |
# MAGIC | 8 | Find everything in `information_schema` — ✅ automatic checks |

# COMMAND ----------

# MAGIC %run ../../Includes/_setup

# COMMAND ----------

# MAGIC %run ./_04_prepare

# COMMAND ----------

# DBTITLE 1,Lab catalog, schemas and silver tables (idempotent)
ensure_lab_catalog()
build_lab_tables()
run_sql(f"USE CATALOG {LAB_CATALOG}")
run_sql(f"USE SCHEMA {SCHEMA_PREFIX}gold")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1 · Stored views
# MAGIC A **view** is a **saved query** registered in Unity Catalog (`catalog.schema.view`). It stores **no data**: every time you
# MAGIC query it, its query runs against the current data of the underlying tables.

# COMMAND ----------

# DBTITLE 1,CREATE VIEW
run_sql(f"""
    CREATE OR REPLACE VIEW {GOLD}.v_customers_per_country
    COMMENT 'Customers per country - always up to date'
    AS SELECT country, count(*) AS customers
       FROM {SILVER}.customers
       GROUP BY country""")

run_sql(f"""
    CREATE OR REPLACE VIEW {GOLD}.v_order_details
    AS SELECT o.order_id, o.order_ts, o.total, c.customer_id, c.country
       FROM {SILVER}.orders o
       JOIN {SILVER}.customers c ON o.customer_id = c.customer_id""")

display(spark.table(f"{GOLD}.v_customers_per_country").orderBy(F.desc("customers")))

# COMMAND ----------

# MAGIC %md
# MAGIC **Proof that a view stores no data:** add a customer to the table — the view shows it immediately, without any refresh.

# COMMAND ----------

# DBTITLE 1,Change the table, query the view again
def customers_in(country):
    r = spark.table(f"{GOLD}.v_customers_per_country").where(F.col("country") == country).first()
    return 0 if r is None else r["customers"]


spark.sql(f"DELETE FROM {SILVER}.customers WHERE customer_id = 'C9001'")   # in case an earlier run was interrupted
before = customers_in("Atlantis")
run_sql(f"""INSERT INTO {SILVER}.customers (customer_id, email, first_name, last_name, city, country, updated_at)
            VALUES ('C9001', 'test@example.com', 'Test', 'User', 'Poseidonia', 'Atlantis', current_timestamp())""")
after = customers_in("Atlantis")
print(f"Customers in Atlantis via the view: before = {before}, after INSERT into the table = {after}")

run_sql(f"DELETE FROM {SILVER}.customers WHERE customer_id = 'C9001'")      # undo the test row
view_is_live = after == before + 1

# COMMAND ----------

# DBTITLE 1,DESCRIBE EXTENDED on a view: Type = VIEW, View Text = the query
# MAGIC %sql
# MAGIC DESCRIBE EXTENDED v_customers_per_country

# COMMAND ----------

# MAGIC %md
# MAGIC In the output look for **Type: VIEW**, **View Text** (the stored query) and **View Catalog and Namespace** — the catalog and
# MAGIC schema that were current when the view was created. Unqualified names inside the view are resolved against them, not against
# MAGIC the caller's current schema.

# COMMAND ----------

# DBTITLE 1,SHOW VIEWS (current schema)
# MAGIC %sql
# MAGIC SHOW VIEWS

# COMMAND ----------

# MAGIC %md
# MAGIC > 🎯 A view runs with the **view owner's** rights on the underlying tables: users need `SELECT` on the **view** only
# MAGIC > (plus `USE CATALOG`/`USE SCHEMA`), not on the tables. That's how views are used to share a **subset** of data (Section 13).
# MAGIC
# MAGIC ## Part 2 · Temporary views
# MAGIC A **temporary view** is **not** stored in Unity Catalog. It exists only in the current **Spark session** and has a
# MAGIC one-part name (no catalog, no schema). Great for intermediate steps; gone when the session ends.

# COMMAND ----------

# DBTITLE 1,CREATE TEMP VIEW (SQL)
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW tv_big_spenders AS
# MAGIC SELECT customer_id, count(*) AS orders, round(sum(total), 2) AS spent
# MAGIC FROM v_order_details
# MAGIC GROUP BY customer_id
# MAGIC HAVING sum(total) > 1000

# COMMAND ----------

# DBTITLE 1,createOrReplaceTempView (Python) - the same kind of object
(spark.table(f"{SILVER}.products")
      .where("category = 'Electronics'")
      .createOrReplaceTempView("tv_electronics"))

display(spark.sql("SELECT * FROM tv_electronics ORDER BY price DESC"))

# COMMAND ----------

# DBTITLE 1,SHOW VIEWS: temp views have isTemporary = true and no namespace
# MAGIC %sql
# MAGIC SHOW VIEWS

# COMMAND ----------

# MAGIC %md
# MAGIC Temp views are never stored in Unity Catalog, so they don't appear in `information_schema` or Catalog Explorer — we check that in Part 8.
# MAGIC
# MAGIC ## Part 3 · Temp views live in one Spark session
# MAGIC Events that end a Spark session — and with it, all its temp views:
# MAGIC
# MAGIC | Event | Temp views survive? |
# MAGIC |---|---|
# MAGIC | Running more cells in the same notebook | ✅ yes |
# MAGIC | Another notebook (each notebook has its **own** session) | ❌ no |
# MAGIC | `dbutils.notebook.run()` child notebook (separate run) | ❌ no |
# MAGIC | `%run ./other` (runs **inside** the caller's session) | ✅ yes |
# MAGIC | Detach & re-attach / restart compute / notebook idle long enough that the session expires | ❌ no |
# MAGIC | A new job run | ❌ no |
# MAGIC
# MAGIC Let's prove the `dbutils.notebook.run()` row with a child notebook that tries to read both views:

# COMMAND ----------

# DBTITLE 1,dbutils.notebook.run the child
import json

child_result = None
try:
    child_result = json.loads(dbutils.notebook.run("./_04_child", 300, {
        "temp_view": "tv_big_spenders",
        "stored_view": f"{GOLD}.v_customers_per_country"}))
    print("Child can see the TEMP view   :", child_result["temp_view_visible"])     # False
    print("Child can see the STORED view :", child_result["stored_view_visible"])   # True
except Exception as e:
    print("🚫 Could not run the child notebook here:", str(e).strip().splitlines()[0][:160])
    print("   Try it by hand: open a NEW notebook and run  SELECT * FROM tv_big_spenders  -> TABLE_OR_VIEW_NOT_FOUND")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 4 · Global temporary views (legacy, classic compute only)
# MAGIC A **global temp view** is registered in the special schema **`global_temp`** and is shared by **all notebooks attached to
# MAGIC the same classic cluster** until that cluster restarts. It is **not supported on serverless compute** (Databricks recommends
# MAGIC temp views or real tables/views instead), so expect this cell to fail on Free Edition.

# COMMAND ----------

# DBTITLE 1,Try a global temp view
global_temp_ok = False
try:
    spark.sql("""CREATE OR REPLACE GLOBAL TEMP VIEW gtv_countries AS
                 SELECT DISTINCT country FROM v_order_details""")
    display(spark.sql("SELECT * FROM global_temp.gtv_countries"))      # note the global_temp. prefix
    display(spark.sql("SHOW VIEWS IN global_temp"))
    display(spark.sql("SHOW TABLES IN global_temp"))                      # also lists them (isTemporary = true)
    spark.sql("DROP VIEW global_temp.gtv_countries")
    global_temp_ok = True
except Exception as e:
    print("🚫 Global temp views are not available on this compute (expected on serverless):",
          str(e).strip().splitlines()[0][:140])

# COMMAND ----------

# MAGIC %md
# MAGIC | | Stored view | Temp view | Global temp view |
# MAGIC |---|---|---|---|
# MAGIC | Registered in | Unity Catalog (`catalog.schema`) | the Spark session | `global_temp` schema of the cluster |
# MAGIC | Name used | `cat.sch.v` / `sch.v` / `v` | `v` | `global_temp.v` |
# MAGIC | Visible to | anyone with privileges, any compute | this notebook/session only | notebooks on the same cluster |
# MAGIC | Dropped | explicitly (`DROP VIEW`) | when the session ends | when the cluster restarts |
# MAGIC | Serverless | ✅ | ✅ | ❌ |
# MAGIC
# MAGIC ## Part 5 · Materialized views
# MAGIC A **materialized view (MV)** stores the **precomputed result** of its query as a table in Unity Catalog and is **refreshed**
# MAGIC (incrementally when possible) — on demand, on a schedule, or when its sources change. Queries against it are fast because they
# MAGIC read the stored result. Behind the scenes Databricks creates a **serverless pipeline** to maintain it.
# MAGIC
# MAGIC ⏱️ The first `CREATE` takes **1–3 minutes** (the pipeline starts). If MVs aren't available on your compute, the cell tells you and moves on.
# MAGIC Free Edition allows only **one active pipeline per type** at a time — if another pipeline is running, wait for it or run the same
# MAGIC `CREATE MATERIALIZED VIEW` later from the **SQL editor**.

# COMMAND ----------

# DBTITLE 1,CREATE MATERIALIZED VIEW + REFRESH
mv_name = f"{GOLD}.mv_revenue_by_country"
mv_ok = False
try:
    run_sql(f"""
        CREATE OR REPLACE MATERIALIZED VIEW {mv_name}
        COMMENT 'Revenue per country - refreshed on demand'
        AS SELECT c.country, count(*) AS orders, round(sum(o.total), 2) AS revenue
           FROM {SILVER}.orders o
           JOIN {SILVER}.customers c ON o.customer_id = c.customer_id
           GROUP BY c.country""")
    display(spark.table(mv_name).orderBy(F.desc("revenue")))
    run_sql(f"REFRESH MATERIALIZED VIEW {mv_name}")        # brings it up to date with the source tables
    print("table_type in information_schema:", table_type(mv_name))
    mv_ok = True
except Exception as e:
    print("🚫 Materialized view not available here:", str(e).strip().splitlines()[0][:180])

# COMMAND ----------

# MAGIC %md
# MAGIC | | View | Materialized view | Table |
# MAGIC |---|---|---|---|
# MAGIC | Stores data | ❌ query runs every time | ✅ precomputed result | ✅ |
# MAGIC | Freshness | always current | as of the **last refresh** | whatever you wrote |
# MAGIC | Refresh | — | `REFRESH MATERIALIZED VIEW`, `SCHEDULE …`, `TRIGGER ON UPDATE` | your code |
# MAGIC | You can `INSERT`/`UPDATE` it | ❌ | ❌ (only its query defines it) | ✅ |
# MAGIC | Good for | simple logic, security layers | expensive aggregations queried often (BI) | everything else |
# MAGIC
# MAGIC > In the UI: Catalog Explorer ▸ your gold schema ▸ `mv_revenue_by_country` ▸ **Overview / Details** shows the refresh
# MAGIC > history and the pipeline that maintains it. Materialized views are also a core object in **Lakeflow Declarative Pipelines** (Section 08).
# MAGIC
# MAGIC ## Part 6 · Volumes — governed storage for **files**
# MAGIC Tables hold rows; **volumes** hold **files** of any format (CSV, JSON, images, PDFs, ML models, libraries…) under a
# MAGIC Unity Catalog name — `catalog.schema.volume` — with a path **`/Volumes/<catalog>/<schema>/<volume>/…`**.
# MAGIC A **managed** volume lives in the schema's managed storage; an **external** volume points at an external location.

# COMMAND ----------

# DBTITLE 1,CREATE VOLUME (managed)
bronze_schema = f"{SCHEMA_PREFIX}bronze"
run_sql(f"CREATE VOLUME IF NOT EXISTS {BRONZE}.landing COMMENT 'Files dropped by partner systems'")
landing = f"/Volumes/{LAB_CATALOG}/{bronze_schema}/landing"
print("Volume path:", landing)

# COMMAND ----------

# DBTITLE 1,Write files into the volume
stores_csv = "\n".join([
    "store_id,city,country,opened",
    "S01,Riyadh,Saudi Arabia,2021-03-01",
    "S02,Jeddah,Saudi Arabia,2022-06-15",
    "S03,Dubai,United Arab Emirates,2023-01-10",
    "S04,Cairo,Egypt,2023-09-01",
    "S05,London,United Kingdom,2024-02-20",
])
dbutils.fs.put(f"{landing}/stores/stores_2026.csv", stores_csv, True)
dbutils.fs.put(f"{landing}/readme.txt", "Stores master data - owner: retail-ops team", True)

# COMMAND ----------

# DBTITLE 1,LIST (SQL) and dbutils.fs.ls (Python)
display(run_sql(f"LIST '{landing}'"))
print([f.name for f in dbutils.fs.ls(f"{landing}/stores")])

# COMMAND ----------

# DBTITLE 1,Volumes are real paths: plain Python can read them too
with open(f"{landing}/readme.txt") as fh:
    print(fh.read())

# COMMAND ----------

# DBTITLE 1,Query the files with read_files() and load a managed table
display(run_sql(f"SELECT * FROM read_files('{landing}/stores', format => 'csv', header => true)"))

run_sql(f"""
    CREATE OR REPLACE TABLE {BRONZE}.stores AS
    SELECT *, _metadata.file_path AS source_file
    FROM read_files('{landing}/stores', format => 'csv', header => true)""")
print("bronze.stores rows:", spark.table(f"{BRONZE}.stores").count())

# COMMAND ----------

# DBTITLE 1,DESCRIBE VOLUME + SHOW VOLUMES
display(run_sql(f"DESCRIBE VOLUME {BRONZE}.landing"))
display(run_sql(f"SHOW VOLUMES IN {BRONZE}"))

# COMMAND ----------

# MAGIC %md
# MAGIC > 🎯 **Exam:** volumes replace the legacy **DBFS root / mounts** (`dbfs:/mnt/...`, `/FileStore`) for files — governed by
# MAGIC > Unity Catalog privileges **`READ VOLUME`** / **`WRITE VOLUME`** (and `CREATE VOLUME` on the schema).
# MAGIC > You **can't** create a *table* inside a volume path — tables go to managed storage or an external location.
# MAGIC >
# MAGIC > 🧭 **UI:** Catalog Explorer ▸ your catalog ▸ bronze ▸ **Volumes ▸ landing** — you can **upload**, download and delete files
# MAGIC > there, or *Create table* from a file. The notebook **Catalog** pane can also copy a volume path for you.
# MAGIC
# MAGIC ## Part 7 · Functions in Unity Catalog
# MAGIC A **user-defined function (UDF)** registered with `CREATE FUNCTION` is a securable object (`catalog.schema.function`):
# MAGIC reusable by anyone with **`EXECUTE`** on it, from SQL and Python, on any compute that supports it.
# MAGIC
# MAGIC ### 7a · SQL UDF

# COMMAND ----------

# DBTITLE 1,CREATE FUNCTION ... RETURN (SQL)
run_sql(f"""
    CREATE OR REPLACE FUNCTION {SILVER}.mask_email(email STRING)
    RETURNS STRING
    COMMENT 'Keeps the first letter and the domain: a***@example.com'
    RETURN CASE WHEN email IS NULL THEN NULL
                ELSE concat(left(email, 1), '***@', substring_index(email, '@', -1)) END""")

display(run_sql(f"""
    SELECT customer_id, email, {SILVER}.mask_email(email) AS masked
    FROM {SILVER}.customers
    ORDER BY customer_id
    LIMIT 5"""))

# COMMAND ----------

# DBTITLE 1,DESCRIBE FUNCTION EXTENDED
display(run_sql(f"DESCRIBE FUNCTION EXTENDED {SILVER}.mask_email"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7b · Python UDF (in Unity Catalog)
# MAGIC The body between `$$ … $$` is Python; Databricks runs it in a sandbox. Supported on serverless, SQL warehouses and recent
# MAGIC runtimes — the cell reports it if your compute can't.

# COMMAND ----------

# DBTITLE 1,CREATE FUNCTION ... LANGUAGE PYTHON
py_udf_ok = False
try:
    run_sql(f"""
    CREATE OR REPLACE FUNCTION {SILVER}.email_domain(email STRING)
    RETURNS STRING
    LANGUAGE PYTHON
    COMMENT 'Lower-case domain part of an e-mail'
    AS $$
    if email is None or "@" not in email:
        return None
    return email.split("@")[1].lower()
    $$""")
    display(run_sql(f"""
        SELECT {SILVER}.email_domain(email) AS domain, count(*) AS customers
        FROM {SILVER}.customers GROUP BY 1 ORDER BY customers DESC"""))
    py_udf_ok = True
except Exception as e:
    print("🚫 Python UDFs in Unity Catalog not available here:", str(e).strip().splitlines()[0][:160])

# COMMAND ----------

# DBTITLE 1,SHOW USER FUNCTIONS
display(run_sql(f"SHOW USER FUNCTIONS IN {SILVER}"))

# COMMAND ----------

# MAGIC %md
# MAGIC > 💡 `spark.udf.register("f", python_function)` (Section 07) creates a **temporary**, session-scoped function — like a
# MAGIC > temp view, it isn't stored in Unity Catalog. `CREATE FUNCTION catalog.schema.f` is **permanent** and governed.
# MAGIC
# MAGIC ## Part 8 · Find everything in `information_schema` + ✅ checks

# COMMAND ----------

# DBTITLE 1,Tables, views and MVs in the lab schemas
lab_schemas = [f"{SCHEMA_PREFIX}{s}" for s in ("bronze", "silver", "gold")]
in_list = "', '".join(lab_schemas)
display(run_sql(f"""
    SELECT table_schema, table_name, table_type
    FROM {LAB_CATALOG}.information_schema.tables
    WHERE table_schema IN ('{in_list}')
    ORDER BY table_type, table_schema, table_name"""))

# COMMAND ----------

# DBTITLE 1,Views, volumes and functions (routines)
display(run_sql(f"SELECT table_schema, table_name, view_definition FROM {LAB_CATALOG}.information_schema.views "
                f"WHERE table_schema IN ('{in_list}')"))
display(run_sql(f"SELECT volume_schema, volume_name, volume_type, storage_location FROM {LAB_CATALOG}.information_schema.volumes "
                f"WHERE volume_schema IN ('{in_list}')"))
display(run_sql(f"SELECT routine_schema, routine_name, routine_body, data_type FROM {LAB_CATALOG}.information_schema.routines "
                f"WHERE routine_schema IN ('{in_list}')"))

# COMMAND ----------

# DBTITLE 1,Check your work
_temp_names = [r["viewName"] for r in spark.sql("SHOW VIEWS").where("isTemporary").collect()]
_is_names = [r["table_name"] for r in spark.table(f"{LAB_CATALOG}.information_schema.tables")
                                            .where(F.col("table_schema").isin(lab_schemas)).collect()]
_checks = {
    "v_customers_per_country is a VIEW": table_type(f"{GOLD}.v_customers_per_country") == "VIEW",
    "the view reflected the new row immediately": view_is_live,
    "temp views exist in this session": {"tv_big_spenders", "tv_electronics"} <= set(_temp_names),
    "landing volume has the stores file": any(f.name == "stores_2026.csv" for f in dbutils.fs.ls(f"{landing}/stores")),
    "bronze.stores has 5 rows": spark.table(f"{BRONZE}.stores").count() == 5,
    "mask_email works": spark.sql(f"SELECT {SILVER}.mask_email('sara@shop.com')").first()[0] == "s***@shop.com",
}
for name, ok in _checks.items():
    print(("✅ " if ok else "❌ ") + name)
print("ℹ️ tv_big_spenders in information_schema?", "tv_big_spenders" in _is_names, "(always False - temp views are not in UC)")
print("\nOptional features on this compute:")
for name, ok in {"child notebook saw temp view = False": (child_result or {}).get("temp_view_visible") is False,
                 "global temp view": global_temp_ok, "materialized view": mv_ok, "Python UDF": py_udf_ok}.items():
    print(("   ✅ " if ok else "   ➖ ") + name)

restore_course_context()
print("\n🎉 Lab complete - next: 04-L3 · Challenge Lab")
