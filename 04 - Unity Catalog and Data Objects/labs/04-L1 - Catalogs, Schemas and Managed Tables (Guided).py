# Databricks notebook source
# MAGIC %md
# MAGIC # 🧪 Lab 04-L1 · Catalogs, Schemas & Managed Tables (Guided)
# MAGIC **Time:** ~45 min · **Compute:** Serverless (Free Edition). A catalog on **default storage** can only be used from serverless compute.
# MAGIC
# MAGIC You'll build a small **medallion-style catalog** for ShopWave and see how Unity Catalog organizes, describes and protects data.
# MAGIC
# MAGIC | Part | You will… |
# MAGIC |---|---|
# MAGIC | 1 | Find out where you are: `current_catalog()`, `SHOW CATALOGS`, `SHOW SCHEMAS` |
# MAGIC | 2 | Create **your own catalog** and describe it |
# MAGIC | 3 | Create `bronze` / `silver` / `gold` **schemas** with comments |
# MAGIC | 4 | Create **managed tables** with three-level names — and switch context with `USE` |
# MAGIC | 5 | Inspect tables: `DESCRIBE EXTENDED`, `DESCRIBE DETAIL`, comments |
# MAGIC | 6 | Query the **`information_schema`** |
# MAGIC | 7 | 🧭 Explore it all in **Catalog Explorer** (UI) |
# MAGIC | 8 | `DROP TABLE` → `UNDROP TABLE` |
# MAGIC | 9 | `DROP SCHEMA` — `RESTRICT` vs `CASCADE` |
# MAGIC | 10 | 💳 Optional: an **external table** (needs cloud storage — not possible on Free Edition) |
# MAGIC | 11 | ✅ Automatic checks |
# MAGIC
# MAGIC > Every Python cell prints the **exact SQL** it runs (via `run_sql`) so you can copy it into a `%sql` cell or the SQL editor.

# COMMAND ----------

# MAGIC %run ../../Includes/_setup

# COMMAND ----------

# MAGIC %run ./_04_prepare

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1 · Where am I?
# MAGIC Every query runs in a **current catalog** and **current schema**. `Includes/_setup` set them to the course catalog
# MAGIC (`workspace` on Free Edition) and schema `shopwave`.

# COMMAND ----------

# DBTITLE 1,Current context
# MAGIC %sql
# MAGIC SELECT current_catalog() AS current_catalog, current_schema() AS current_schema, current_user() AS me

# COMMAND ----------

# DBTITLE 1,Catalogs you can see
# MAGIC %sql
# MAGIC SHOW CATALOGS

# COMMAND ----------

# MAGIC %md
# MAGIC Typical catalogs in a workspace:
# MAGIC
# MAGIC | Catalog | What it is |
# MAGIC |---|---|
# MAGIC | `workspace` | The **workspace catalog** created automatically for new (and Free Edition) workspaces — the default catalog |
# MAGIC | `system` | Read-only **system tables** (billing, audit, lineage, `information_schema` for all catalogs…) |
# MAGIC | `samples` | Read-only sample datasets (e.g. `samples.nyctaxi.trips`) |
# MAGIC | `hive_metastore` | The **legacy** per-workspace Hive metastore, shown as a catalog (only in workspaces that have one) |

# COMMAND ----------

# DBTITLE 1,Schemas in the course catalog
# MAGIC %sql
# MAGIC SHOW SCHEMAS

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2 · Create your own catalog
# MAGIC A **catalog** is the top-level container under the metastore — usually one per environment (`dev`, `prod`), business
# MAGIC unit or data product. Creating one needs the **`CREATE CATALOG`** privilege on the metastore (you have it on Free Edition).
# MAGIC
# MAGIC On Free Edition (serverless workspaces with **default storage**) you don't give a storage location: Databricks manages it.
# MAGIC In other workspaces you may have to add `MANAGED LOCATION 's3://…'` (an external location's path) — see 04-P2.

# COMMAND ----------

# DBTITLE 1,CREATE CATALOG
ensure_lab_catalog()

# COMMAND ----------

# DBTITLE 1,DESCRIBE CATALOG EXTENDED
if CATALOG_CREATED:
    display(run_sql(f"DESCRIBE CATALOG EXTENDED {LAB_CATALOG}"))
else:
    print("(fallback mode - you are using the course catalog, skip this cell)")

# COMMAND ----------

# MAGIC %md
# MAGIC Look at **Owner** (you — the creator owns the object), **Comment** and the storage fields. A new catalog already contains a
# MAGIC schema called **`information_schema`** (read-only metadata views) and one called **`default`**.

# COMMAND ----------

# DBTITLE 1,Schemas inside the new catalog
display(run_sql(f"SHOW SCHEMAS IN {LAB_CATALOG}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 3 · Schemas for the medallion layers
# MAGIC A **schema** (also called a **database** — `CREATE DATABASE` is a synonym) groups tables, views, volumes, functions and models.

# COMMAND ----------

# DBTITLE 1,CREATE SCHEMA ... COMMENT
for full, text in ((BRONZE, "Raw data as ingested - append only"),
                   (SILVER, "Cleaned and conformed data"),
                   (GOLD, "Business-level aggregates for BI")):
    run_sql(f"CREATE SCHEMA IF NOT EXISTS {full} COMMENT '{text}'")

display(run_sql(f"SHOW SCHEMAS IN {LAB_CATALOG} LIKE '{SCHEMA_PREFIX}bronze|{SCHEMA_PREFIX}silver|{SCHEMA_PREFIX}gold'"))

# COMMAND ----------

# DBTITLE 1,DESCRIBE SCHEMA EXTENDED
display(run_sql(f"DESCRIBE SCHEMA EXTENDED {SILVER}"))

# COMMAND ----------

# MAGIC %md
# MAGIC > 🎯 `IF NOT EXISTS` makes a `CREATE` idempotent (no error if it's already there). Without it, creating an existing
# MAGIC > schema fails with `SCHEMA_ALREADY_EXISTS`.
# MAGIC
# MAGIC ## Part 4 · Managed tables with three-level names
# MAGIC A table's full name is **`catalog.schema.table`**. A fully-qualified name works from **any** current context.
# MAGIC
# MAGIC We load the raw files from the course **volume** with `read_files()` (more in Section 05). Because there is **no `LOCATION`**,
# MAGIC Unity Catalog decides where the files go → a **managed** table (Delta by default).

# COMMAND ----------

# DBTITLE 1,Bronze: customers and products from files
run_sql(f"""
    CREATE OR REPLACE TABLE {BRONZE}.customers
    COMMENT 'Customers as delivered by the CRM export (JSON)'
    AS SELECT * FROM read_files('{dataset_path}/customers-json', format => 'json')""")

run_sql(f"""
    CREATE OR REPLACE TABLE {BRONZE}.products
    COMMENT 'Product catalog export (CSV, ; delimited)'
    AS SELECT * FROM read_files('{dataset_path}/products-csv', format => 'csv', header => true, delimiter => ';')""")

display(spark.table(f"{BRONZE}.customers").limit(5))

# COMMAND ----------

# DBTITLE 1,Silver: clean the customers (reads bronze, writes silver)
run_sql(f"""
    CREATE OR REPLACE TABLE {SILVER}.customers
    COMMENT 'One row per customer, profile JSON flattened'
    AS SELECT customer_id,
              email,
              profile:first_name::string        AS first_name,
              profile:last_name::string         AS last_name,
              profile:address:city::string      AS city,
              profile:address:country::string   AS country,
              to_timestamp(updated)             AS updated_at
       FROM {BRONZE}.customers""")

run_sql(f"""
    CREATE OR REPLACE TABLE {SILVER}.products
    AS SELECT product_id, title, brand, category, CAST(price AS DECIMAL(10,2)) AS price
       FROM {BRONZE}.products""")

display(spark.table(f"{SILVER}.customers").limit(5))

# COMMAND ----------

# DBTITLE 1,Gold: an aggregate
run_sql(f"""
    CREATE OR REPLACE TABLE {GOLD}.customers_by_country
    COMMENT 'Number of customers per country'
    AS SELECT country, count(*) AS customers, count(email) AS customers_with_email
       FROM {SILVER}.customers
       GROUP BY country""")
display(spark.table(f"{GOLD}.customers_by_country").orderBy(F.desc("customers")))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Switch context with `USE`
# MAGIC Typing the catalog every time is tedious. `USE CATALOG` + `USE SCHEMA` change the **current** catalog/schema for the rest
# MAGIC of this **session**, so shorter names resolve against them:
# MAGIC
# MAGIC | You write | Resolves to |
# MAGIC |---|---|
# MAGIC | `customers` | `<current catalog>.<current schema>.customers` |
# MAGIC | `silver.customers` | `<current catalog>.silver.customers` |
# MAGIC | `lab04_x.silver.customers` | exactly that |

# COMMAND ----------

# DBTITLE 1,USE CATALOG / USE SCHEMA
run_sql(f"USE CATALOG {LAB_CATALOG}")
run_sql(f"USE SCHEMA {SCHEMA_PREFIX}silver")

# COMMAND ----------

# DBTITLE 1,Short names now resolve to your silver schema
# MAGIC %sql
# MAGIC SELECT current_catalog(), current_schema(), count(*) AS customers
# MAGIC FROM customers

# COMMAND ----------

# MAGIC %md
# MAGIC > ⚠️ `USE SCHEMA silver` alone would look for `silver` in the **current catalog** — that's why we ran `USE CATALOG` first.
# MAGIC > A three-level name always wins, whatever the context.
# MAGIC
# MAGIC ## Part 5 · Inspect a managed table

# COMMAND ----------

# DBTITLE 1,DESCRIBE EXTENDED (look at Type, Location, Provider, Owner)
# MAGIC %sql
# MAGIC DESCRIBE EXTENDED customers

# COMMAND ----------

# MAGIC %md
# MAGIC In the **Detailed Table Information** rows find:
# MAGIC
# MAGIC * **Type: `MANAGED`** — Unity Catalog owns the files
# MAGIC * **Location** — a path inside the catalog/schema's **managed storage** (you never choose it; on Free Edition you can't even browse it)
# MAGIC * **Provider: `delta`** — managed tables are Delta (or Iceberg if you ask for it)
# MAGIC * **Owner** — you

# COMMAND ----------

# DBTITLE 1,DESCRIBE DETAIL (one row: format, location, numFiles, sizeInBytes...)
# MAGIC %sql
# MAGIC DESCRIBE DETAIL customers

# COMMAND ----------

# MAGIC %md
# MAGIC ### Add documentation
# MAGIC Comments show up in Catalog Explorer, in `DESCRIBE`, in `information_schema` and help AI features (Genie, AI-generated docs) understand your data.

# COMMAND ----------

# DBTITLE 1,COMMENT ON TABLE + column comment
run_sql(f"COMMENT ON TABLE {SILVER}.customers IS 'Cleaned customers - one row per customer_id (PII: email)'")
run_sql(f"ALTER TABLE {SILVER}.customers ALTER COLUMN email COMMENT 'Customer e-mail, may be NULL'")
display(spark.sql(f"DESCRIBE TABLE {SILVER}.customers").where("col_name = 'email'"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 6 · The `information_schema`
# MAGIC Every catalog has a read-only **`information_schema`** (ANSI standard views) listing the objects **you are allowed to see**.
# MAGIC `system.information_schema` covers **all** catalogs.
# MAGIC
# MAGIC | View | One row per… |
# MAGIC |---|---|
# MAGIC | `schemata` | schema |
# MAGIC | `tables` | table / view (`table_type`: `MANAGED`, `EXTERNAL`, `VIEW`, `MATERIALIZED_VIEW`, `STREAMING_TABLE`, `FOREIGN`…) |
# MAGIC | `columns` | column |
# MAGIC | `views`, `volumes`, `routines` (functions) | view, volume, function |
# MAGIC | `table_privileges`, `schema_privileges`, … | grant |

# COMMAND ----------

# DBTITLE 1,Tables in your three lab schemas
lab_schemas = [f"{SCHEMA_PREFIX}{s}" for s in ("bronze", "silver", "gold")]
display(run_sql(f"""
    SELECT table_schema, table_name, table_type, data_source_format, table_owner, comment
    FROM {LAB_CATALOG}.information_schema.tables
    WHERE table_schema IN ('{"', '".join(lab_schemas)}')
    ORDER BY table_schema, table_name"""))

# COMMAND ----------

# DBTITLE 1,Columns of silver.customers
display(run_sql(f"""
    SELECT ordinal_position, column_name, full_data_type, is_nullable, comment
    FROM {LAB_CATALOG}.information_schema.columns
    WHERE table_schema = '{SCHEMA_PREFIX}silver' AND table_name = 'customers'
    ORDER BY ordinal_position"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 7 · 🧭 Catalog Explorer (UI)
# MAGIC Everything you just did with SQL is also visible in the UI.
# MAGIC
# MAGIC 1. In the left sidebar click **Catalog** (📚). The panel lists the catalogs you can see.
# MAGIC 2. Type your catalog name (printed in Part 2) in the search box, or expand it in the tree: **catalog ▸ schemas ▸ tables**.
# MAGIC 3. Click **`silver` ▸ `customers`** and look at the tabs:
# MAGIC    * **Overview** — columns, types and the **comments** you added (you can also add/edit comments here, or let AI suggest them)
# MAGIC    * **Sample Data** — a preview (needs running compute)
# MAGIC    * **Details** — type **Managed**, storage location, format, owner
# MAGIC    * **Permissions** — who can do what (Section 13)
# MAGIC    * **History** — the Delta history (`DESCRIBE HISTORY`)
# MAGIC    * **Lineage** — `bronze.customers → silver.customers → gold.customers_by_country`, captured automatically by Unity Catalog
# MAGIC 4. Click the catalog name itself: its **Details** show the owner and storage; **Workspaces** shows which workspaces can use it (workspace-catalog binding).
# MAGIC    *(Just created your catalog and don't see it? Click the ⟳ refresh icon at the top of the Catalog panel.)*
# MAGIC 5. Back in this notebook, the **Catalog** pane is also available on the left (📚 icon inside the notebook) — drag a table name into a cell to insert its full name.
# MAGIC
# MAGIC > 💡 Lineage needs a few minutes to appear after the tables are created.
# MAGIC
# MAGIC ## Part 8 · `DROP TABLE` and `UNDROP TABLE`
# MAGIC Dropping a **managed** table removes it from the catalog; Unity Catalog keeps the data for a **recovery period (7 days by
# MAGIC default)** and then deletes the files. Inside that window **`UNDROP TABLE`** brings it back.
# MAGIC The recovery period can be set per catalog or schema: 0 (off) or 7–30 days, e.g. `ALTER SCHEMA … SET RETAIN DROPPED TO 14 DAYS`.

# COMMAND ----------

# DBTITLE 1,Drop the gold table
gold_table = f"{GOLD}.customers_by_country"
if spark.catalog.tableExists(gold_table):                  # safe to re-run
    rows_before = spark.table(gold_table).count()
    run_sql(f"DROP TABLE {gold_table}")
else:
    print("Already dropped - continue with the next cell.")
print("Exists after DROP:", spark.catalog.tableExists(gold_table))

# COMMAND ----------

# DBTITLE 1,SHOW TABLES DROPPED + UNDROP TABLE
undrop_ok = False
try:
    display(run_sql(f"SHOW TABLES DROPPED IN {GOLD}"))
    run_sql(f"UNDROP TABLE {gold_table}")
    undrop_ok = spark.table(gold_table).count() == globals().get("rows_before", -1)
    print("✅ Restored with UNDROP:", undrop_ok)
except Exception as e:
    print("🚫 UNDROP not available here:", str(e).strip().splitlines()[0][:160])
    print("   Rebuilding the table so the rest of the lab works...")
    run_sql(f"""CREATE TABLE {gold_table} COMMENT 'Number of customers per country'
                AS SELECT country, count(*) AS customers, count(email) AS customers_with_email
                   FROM {SILVER}.customers GROUP BY country""")

# COMMAND ----------

# MAGIC %md
# MAGIC > 🎯 **Exam:** `DROP TABLE` on a **managed** table ⇒ metadata **and** data go (data after the recovery window).
# MAGIC > On an **external** table ⇒ only the metadata goes; the files stay in your storage. `UNDROP TABLE name` restores the **most recently**
# MAGIC > dropped table with that name; to pick an older one use `UNDROP TABLE WITH ID '<tableId>'` (the id is in `SHOW TABLES DROPPED`).
# MAGIC
# MAGIC ## Part 9 · `DROP SCHEMA` — `RESTRICT` (default) vs `CASCADE`

# COMMAND ----------

# DBTITLE 1,A scratch schema with one table
scratch = f"{LAB_CATALOG}.{SCHEMA_PREFIX}scratch"
run_sql(f"CREATE SCHEMA IF NOT EXISTS {scratch}")
run_sql(f"CREATE OR REPLACE TABLE {scratch}.tmp AS SELECT 1 AS id")

# COMMAND ----------

# DBTITLE 1,DROP SCHEMA without CASCADE fails when the schema is not empty
restrict_failed = False
try:
    run_sql(f"DROP SCHEMA {scratch}")          # same as ... RESTRICT
except Exception as e:
    restrict_failed = True
    print("🚫 Expected error:", str(e).strip().splitlines()[0][:160])

# COMMAND ----------

# DBTITLE 1,CASCADE drops the schema and everything in it
run_sql(f"DROP SCHEMA {scratch} CASCADE")
print("Scratch schema still exists?", spark.catalog.databaseExists(scratch))

# COMMAND ----------

# MAGIC %md
# MAGIC The same rules apply one level up: `DROP CATALOG x` fails while it still has any schema other than `information_schema` —
# MAGIC **including the auto-created `default` schema** — so in practice you use `DROP CATALOG x CASCADE`, which removes everything. **Don't drop your lab catalog yet** — 04-L2 and 04-L3 use it.
# MAGIC
# MAGIC ## Part 10 · 💳 Optional — an external table (needs your own cloud storage)
# MAGIC Free Edition has no external locations, so **skip this part there**. In a workspace where an admin created an
# MAGIC **external location** and granted you `CREATE EXTERNAL TABLE` (and `READ FILES`/`WRITE FILES`) on it, paste a
# MAGIC folder URL inside it into the **`external_location_url`** widget at the top (e.g. `s3://my-bucket/landing/lab04`) and run the cells.

# COMMAND ----------

# DBTITLE 1,Widget (leave empty to skip)
dbutils.widgets.text("external_location_url", "", "External location URL (optional)")
ext_url = dbutils.widgets.get("external_location_url").strip().rstrip("/")
print("External table demo:", "ENABLED -> " + ext_url if ext_url else "skipped (widget empty)")

# COMMAND ----------

# DBTITLE 1,CREATE TABLE ... LOCATION -> EXTERNAL
ext_ok = None
if ext_url:
    ext_table, ext_path = f"{BRONZE}.products_ext", f"{ext_url}/products_ext"
    run_sql(f"CREATE OR REPLACE TABLE {ext_table} LOCATION '{ext_path}' AS SELECT * FROM {SILVER}.products")
    print("table_type:", table_type(ext_table))                      # EXTERNAL
    run_sql(f"DROP TABLE {ext_table}")                                 # removes only the metadata
    files_left = dbutils.fs.ls(ext_path)                               # ... the Delta files are still there
    print("Files still in storage after DROP:", len(files_left))
    ext_ok = len(files_left) > 0
    dbutils.fs.rm(ext_path, True)                                      # clean up the files ourselves
else:
    print("Skipped.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 11 · ✅ Automatic checks
# MAGIC Your current context is still your lab catalog — the last cell switches back to the course schema.

# COMMAND ----------

# DBTITLE 1,Check your work
_checks = {
    "lab catalog exists": LAB_CATALOG in [r[0] for r in spark.sql("SHOW CATALOGS").collect()],
    "bronze / silver / gold schemas exist": all(spark.catalog.databaseExists(s) for s in (BRONZE, SILVER, GOLD)),
    "bronze.customers has 300 rows": spark.table(f"{BRONZE}.customers").count() == 300,
    "bronze.customers is a MANAGED table": table_type(f"{BRONZE}.customers") == "MANAGED",
    "silver.customers has a country column": "country" in spark.table(f"{SILVER}.customers").columns,
    "silver.products has 36 products": spark.table(f"{SILVER}.products").count() == 36,
    "silver.customers has a table comment": (spark.table(f"{LAB_CATALOG}.information_schema.tables")
                                             .where(f"table_schema = '{SCHEMA_PREFIX}silver' AND table_name = 'customers'")
                                             .select("comment").first()[0] or "").startswith("Cleaned"),
    "gold table exists again (UNDROP, or rebuilt if UNDROP isn't available)": spark.catalog.tableExists(gold_table),
    "DROP SCHEMA without CASCADE failed on a non-empty schema": restrict_failed,
    "scratch schema dropped with CASCADE": not spark.catalog.databaseExists(scratch),
}
for name, ok in _checks.items():
    print(("✅ " if ok else "❌ ") + name)
print("ℹ️ UNDROP worked" if undrop_ok else "ℹ️ UNDROP was not available - table was rebuilt instead")
if ext_ok is not None:
    print(("✅ " if ext_ok else "❌ ") + "external table files survived DROP TABLE")

restore_course_context()
print("\n🎉 Lab complete - next: 04-L2 · Views, Volumes and Functions (your lab catalog stays for the next labs)")
