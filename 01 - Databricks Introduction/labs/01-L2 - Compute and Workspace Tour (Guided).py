# Databricks notebook source
# MAGIC %md
# MAGIC # 🧪 Lab 01-L2 · Compute & Workspace Tour (Guided)
# MAGIC **Time:** ~35 min · **Compute:** Serverless · Parts marked 💳 need a **paid/trial workspace** (skip them on Free Edition)
# MAGIC
# MAGIC | Part | What you do | Exam objective |
# MAGIC |---|---|---|
# MAGIC | 1 | 🖱️ Tour the workspace sidebar & Catalog Explorer | 1.1 |
# MAGIC | 2 | 🧪 Inspect the compute this notebook runs on | 1.2 |
# MAGIC | 3 | 🧪 Hit the serverless limitations on purpose | 1.2 |
# MAGIC | 4 | 🖱️ 💳 Create classic all-purpose compute | 1.2 |
# MAGIC | 5 | 🖱️ Start a SQL warehouse and run a query | 1.2 |
# MAGIC | 6 | 🧪 Look at what you spend (`system.billing.usage`) | 1.2 cost models |
# MAGIC | 7 | 🧠 Compute decision drill | 1.2 |

# COMMAND ----------

# MAGIC %run ../../Includes/_setup

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1 · 🖱️ Workspace tour (10 min)
# MAGIC Tick each item as you go:
# MAGIC
# MAGIC - [ ] **Workspace** → open **Home**. Find this course folder. Right-click a notebook → see *Clone, Rename, Move, Export, Share (Permissions)*.
# MAGIC - [ ] **Workspace** → **Shared** folder: content visible to everyone in the workspace.
# MAGIC - [ ] **Recents**: the last objects you opened.
# MAGIC - [ ] **Catalog** → expand your catalog → **`shopwave`**. Open the **`lab01_customers`** table (if you did Lab 01-L1) → tabs **Overview** (columns), **Sample data**, **Details**, **Permissions**, **History**, **Lineage**.
# MAGIC - [ ] **Catalog** → **`samples`** catalog (if present): read-only sample datasets such as `nyctaxi`, `tpch`, `bakehouse`.
# MAGIC - [ ] **Jobs & Pipelines**: where Lakeflow Jobs and pipelines live (Sections 08, 10).
# MAGIC - [ ] **Compute**: tabs for *All-purpose compute*, *Job compute*, *SQL warehouses*, *Pools*, *Policies* (some are hidden on Free Edition).
# MAGIC - [ ] **SQL Editor**, **Dashboards**, **Genie**, **Alerts**, **Query History**: the Databricks SQL tools (Section 09).
# MAGIC - [ ] In this notebook: **File ▸ Version history** — every save is kept.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2 · 🧪 Inspect your compute
# MAGIC
# MAGIC `current_version()` returns the Databricks Runtime / SQL version of the compute you are attached to.

# COMMAND ----------

# DBTITLE 1,Which runtime am I on?
# MAGIC %sql
# MAGIC SELECT current_version() AS version_info

# COMMAND ----------

# DBTITLE 1,Spark session type
from pyspark.sql import SparkSession

session_type = type(spark).__module__
print("Spark version :", spark.version)
print("Session class :", session_type)
if "connect" in session_type:
    print("➡️ This is a Spark Connect session - used by serverless and by Standard-access-mode compute.")
else:
    print("➡️ This is a classic Spark session (e.g. Dedicated access mode compute).")

# COMMAND ----------

# MAGIC %md
# MAGIC > 🖱️ Click the **compute selector** (top right) → with **Serverless** selected, open the **Environment** side panel (🧩 icon on the right)
# MAGIC > to see the **environment version** and where you could add Python dependencies. Serverless has no Spark UI —
# MAGIC > for a query's details open **"See performance"** under a cell's output (the **query profile**).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 3 · 🧪 Hit the serverless limitations on purpose
# MAGIC Each cell below *tries* something that behaves differently on serverless vs classic compute and reports what happened.
# MAGIC Nothing breaks — errors are caught and printed. Knowing these explains several exam answers.

# COMMAND ----------

# DBTITLE 1,Helper to try an action
def try_it(label, fn):
    try:
        result = fn()
        print(f"✅ {label}: worked" + (f" -> {result}" if result is not None else ""))
    except Exception as e:
        first_line = str(e).strip().splitlines()[0][:180]
        print(f"🚫 {label}: NOT supported here -> {first_line}")

# COMMAND ----------

# DBTITLE 1,1. DataFrame caching
df = spark.table("lab01_customers") if spark.catalog.tableExists("lab01_customers") else spark.range(10)
try_it("df.cache()", lambda: df.cache().count())

# COMMAND ----------

# DBTITLE 1,2. Global temporary view
try_it("createOrReplaceGlobalTempView", lambda: spark.range(3).createOrReplaceGlobalTempView("lab02_global_v"))

# COMMAND ----------

# DBTITLE 1,3. Spark configurations
try_it("set spark.sql.shuffle.partitions (supported on serverless)",
       lambda: (spark.conf.set("spark.sql.shuffle.partitions", "64"), spark.conf.get("spark.sql.shuffle.partitions"))[1])
try_it("set spark.sql.autoBroadcastJoinThreshold",
       lambda: (spark.conf.set("spark.sql.autoBroadcastJoinThreshold", str(20 * 1024 * 1024)),
                spark.conf.get("spark.sql.autoBroadcastJoinThreshold"))[1])

# COMMAND ----------

# DBTITLE 1,4. RDD / SparkContext API
try_it("spark.sparkContext", lambda: spark.sparkContext.defaultParallelism)

# COMMAND ----------

# MAGIC %md
# MAGIC **What you should see on serverless:** caching 🚫, global temp views 🚫, `spark.sql.shuffle.partitions` ✅,
# MAGIC most other configs 🚫 or ignored, `sparkContext` 🚫. On **Dedicated** classic compute everything works.
# MAGIC
# MAGIC | If you need… | …on serverless do this instead |
# MAGIC |---|---|
# MAGIC | a reusable intermediate result | write it to a table (or a temporary view that is recomputed) |
# MAGIC | a view shared by several notebooks | a real `VIEW` in Unity Catalog |
# MAGIC | low-level RDD code | rewrite with the DataFrame API |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 4 · 🖱️ 💳 Create classic all-purpose compute *(paid / trial workspaces only)*
# MAGIC Free Edition users: read the steps — the settings are exam material — then skip to Part 5.
# MAGIC
# MAGIC 1. Sidebar **Compute** → **All-purpose compute** tab → **Create compute**.
# MAGIC 2. Configure:
# MAGIC
# MAGIC | Setting | Value | Why |
# MAGIC |---|---|---|
# MAGIC | Name | `Demo Compute - <your name>` | |
# MAGIC | Policy | `Unrestricted` (or `Personal Compute`) | Policies limit what users can configure |
# MAGIC | Machine learning | off | ML runtime only for ML libraries |
# MAGIC | Databricks Runtime | the newest version marked **LTS** | Long-term support = stable, 3 years of fixes |
# MAGIC | Photon acceleration | ✅ on | Faster SQL/DataFrame work |
# MAGIC | Node type | a small **general purpose** instance (≈ 4 cores) | |
# MAGIC | Single node | ✅ (for learning) | Driver only → cheapest |
# MAGIC | Access mode | **Auto** → Standard | Unity Catalog multi-user isolation |
# MAGIC | Terminate after | **30** minutes of inactivity | ⚠️ avoid paying for idle compute |
# MAGIC | Tags | `project = de-associate-course` | Cost attribution in `system.billing.usage` |
# MAGIC
# MAGIC 3. Click **Create compute**. Note it takes **several minutes** to start (serverless: seconds).
# MAGIC 4. Open the compute → tabs **Configuration**, **Libraries**, **Event log**, **Spark UI**, **Driver logs**, **Metrics**.
# MAGIC 5. Attach this notebook to it (compute selector) and re-run **Part 3** — see which actions now succeed.
# MAGIC 6. **Terminate** it when you are done (or let auto-termination do it).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 5 · 🖱️ SQL warehouse
# MAGIC 1. Sidebar **SQL Warehouses** (or **Compute ▸ SQL warehouses**). Free Edition has one **2X-Small** warehouse (e.g. *Serverless Starter Warehouse*).
# MAGIC 2. Open it → look at **Type** (Serverless / Pro / Classic), **Cluster size**, **Auto stop**, **Scaling (min/max clusters)**.
# MAGIC 3. Click **Start** if it's stopped.
# MAGIC 4. Open **SQL Editor**, select the warehouse, and run:
# MAGIC
# MAGIC ```sql
# MAGIC SELECT profile:address:country AS country, count(*) AS customers
# MAGIC FROM shopwave.lab01_customers
# MAGIC GROUP BY 1 ORDER BY 2 DESC;
# MAGIC ```
# MAGIC *(If your catalog isn't the default one, prefix with it, e.g. `workspace.shopwave.lab01_customers`.)*
# MAGIC
# MAGIC 5. Open **Query History** and find your query — note its duration and the warehouse that ran it.
# MAGIC
# MAGIC > 🎯 A notebook can also attach to a **SQL warehouse** — but then only **SQL** cells run. Python needs Spark compute.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 6 · 🧪 What am I spending?
# MAGIC Billing data is exposed in the **system table** `system.billing.usage` (one row per usage record, in **DBUs**).
# MAGIC Access requires the system schema to be enabled and `SELECT` granted — on some workspaces you'll get a permission message; that's expected.

# COMMAND ----------

# DBTITLE 1,Last 7 days of usage by product
try:
    usage = spark.sql("""
        SELECT usage_date,
               billing_origin_product,
               sku_name,
               round(sum(usage_quantity), 3) AS dbus
        FROM system.billing.usage
        WHERE usage_date >= date_sub(current_date(), 7)
        GROUP BY ALL
        ORDER BY usage_date DESC, dbus DESC
    """)
    display(usage)
except Exception as e:
    print("ℹ️ system.billing.usage isn't accessible for you here:", str(e).splitlines()[0][:200])
    print("   That's normal on some workspaces. Just remember: system.billing.usage = DBU consumption records.")

# COMMAND ----------

# MAGIC %md
# MAGIC > 🎯 **Exam focus:** DBU consumption lives in **`system.billing.usage`**; list prices in **`system.billing.list_prices`**.
# MAGIC > **Tags** you put on compute appear in the `custom_tags` column → cost per team/project.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 7 · 🧠 Compute decision drill
# MAGIC For each scenario, decide before scrolling to the answers.
# MAGIC
# MAGIC | # | Scenario |
# MAGIC |---|---|
# MAGIC | 1 | A data scientist explores data interactively in Python and doesn't want to configure anything. |
# MAGIC | 2 | A nightly production ETL job must run with the least operational overhead. |
# MAGIC | 3 | An ETL job uses a custom JAR library and an init script. |
# MAGIC | 4 | 40 analysts query gold tables from Power BI all day. |
# MAGIC | 5 | A team writes R code. |
# MAGIC | 6 | A large, fault-tolerant batch job on classic compute must be as cheap as possible. |
# MAGIC | 7 | Many short jobs on classic compute spend most of their time waiting for VMs to start. |

# COMMAND ----------

# MAGIC %md
# MAGIC ### ✅ Answers
# MAGIC | # | Answer | Reason |
# MAGIC |---|---|---|
# MAGIC | 1 | **Serverless compute for notebooks** | seconds to start, no configuration |
# MAGIC | 2 | **Serverless compute for jobs** (or job compute) | automated, no cluster management, cheaper than all-purpose |
# MAGIC | 3 | **Job compute (classic)** | JARs & init scripts aren't supported on serverless |
# MAGIC | 4 | **Serverless SQL warehouse** | built for concurrent SQL/BI, scales out with more clusters |
# MAGIC | 5 | **Classic all-purpose compute, Dedicated access mode** | R isn't available on serverless or Standard mode |
# MAGIC | 6 | **Job compute with spot workers** (on-demand driver) | spot VMs are discounted; the job tolerates lost workers |
# MAGIC | 7 | **Instance pool** | idle pre-warmed VMs cut start-up time |
# MAGIC
# MAGIC 🎉 Lab complete — next: **01-L3 · Challenge Lab**
