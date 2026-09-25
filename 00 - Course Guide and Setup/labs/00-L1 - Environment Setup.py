# Databricks notebook source
# MAGIC %md
# MAGIC # 🧪 Lab 00-L1 · Environment Setup
# MAGIC **Goal:** get a working Databricks workspace, import the course, and create the ShopWave data.
# MAGIC **Time:** ~15 minutes · **Compute:** Serverless
# MAGIC
# MAGIC | Part | What you do |
# MAGIC |---|---|
# MAGIC | 1 | 🖱️ Get a workspace (Free Edition) |
# MAGIC | 2 | 🖱️ Import the course |
# MAGIC | 3 | 🖱️ Attach serverless compute |
# MAGIC | 4 | 🧪 Run the course setup |
# MAGIC | 5 | 🧪 Verify what was created |
# MAGIC | 6 | 🖱️ Explore it in Catalog Explorer |
# MAGIC | 7 | ✅ Automatic checks |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1 · 🖱️ Get a workspace
# MAGIC
# MAGIC Skip this part if you already have a Unity Catalog–enabled workspace.
# MAGIC
# MAGIC 1. Go to **https://www.databricks.com/learn/free-edition** and click **Sign up**.
# MAGIC 2. Sign up with **email** (one-time code), **Google** or **Microsoft**.
# MAGIC 3. Your workspace opens automatically. Bookmark its URL (`https://<something>.cloud.databricks.com`).
# MAGIC
# MAGIC > ℹ️ Free Edition gives you **serverless compute**, a **SQL warehouse**, **Unity Catalog** with a catalog called **`workspace`**,
# MAGIC > Jobs and Lakeflow pipelines — everything this course needs. It does **not** allow classic clusters, pools, or the account console;
# MAGIC > labs that show those steps mark them as *"paid workspace only"*.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2 · 🖱️ Import the course
# MAGIC
# MAGIC 1. In the left sidebar click **Workspace**, then open **Home** (your user folder).
# MAGIC 2. Click the **⋮** (kebab) menu at the top right of the folder view → **Import**.
# MAGIC 3. Choose **File**, drop the course **`.zip`** file, click **Import**.
# MAGIC 4. A folder **`Databricks course and certification`** appears, with `Includes`, `README - Start Here`, and one folder per section.
# MAGIC
# MAGIC > ⚠️ **Do not rename or move** the `Includes` folder or the section folders. Every lab finds the setup code with the relative path
# MAGIC > `%run ../../Includes/_setup`.
# MAGIC
# MAGIC *(Alternative for Git users: **Workspace → Create → Git folder** and clone your own repository containing these files.)*

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 3 · 🖱️ Attach serverless compute
# MAGIC
# MAGIC 1. At the top right of this notebook, open the **compute selector** (it may say *Connect*).
# MAGIC 2. Choose **Serverless**. It starts in a few seconds — no configuration needed.
# MAGIC 3. Check that the cell below runs.

# COMMAND ----------

# DBTITLE 1,🧪 Your first cell
print("Hello from ShopWave! 👋  Python is running on:", "serverless / Spark", spark.version)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 4 · 🧪 Run the course setup
# MAGIC
# MAGIC The next cell *includes* the setup notebook. `%run` executes another notebook **inside this notebook's session**,
# MAGIC so every variable and function it defines (`dataset_path`, `land_new_orders()` …) becomes available here.
# MAGIC
# MAGIC The first run generates the data (≈ 30–60 s). Later runs only verify it (a few seconds).

# COMMAND ----------

# MAGIC %run ../../Includes/_setup

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 5 · 🧪 Verify what was created
# MAGIC
# MAGIC The setup ran `USE CATALOG` and `USE SCHEMA`, so SQL cells can now use **short names**.

# COMMAND ----------

# DBTITLE 1,Where am I?
# MAGIC %sql
# MAGIC SELECT current_catalog() AS catalog, current_schema() AS schema, current_user() AS me

# COMMAND ----------

# DBTITLE 1,Volumes in the course schema
# MAGIC %sql
# MAGIC SHOW VOLUMES

# COMMAND ----------

# DBTITLE 1,Details of the raw volume
# MAGIC %sql
# MAGIC DESCRIBE VOLUME raw

# COMMAND ----------

# DBTITLE 1,Files in the raw volume
display(dbutils.fs.ls(dataset_path))

# COMMAND ----------

# MAGIC %md
# MAGIC You should see 5 folders: `customers-json/`, `orders-landing/`, `orders-parquet/`, `orders-staging/`, `products-csv/`.
# MAGIC
# MAGIC > 🎯 **Exam focus — Volumes:** a **volume** is a Unity Catalog object that governs **non-tabular files**.
# MAGIC > Its path is always `/Volumes/<catalog>/<schema>/<volume>/<path>`. It replaces the old DBFS `/mnt` mount points (🕰️ legacy).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 6 · 🖱️ Explore it in Catalog Explorer
# MAGIC
# MAGIC 1. In the left sidebar click **Catalog**.
# MAGIC 2. Expand your catalog (Free Edition: **`workspace`**) → **`shopwave`**.
# MAGIC 3. Open **Volumes → `raw`** and browse the folders. Click a file in `customers-json/` to preview it.
# MAGIC 4. Look at the **Details** and **Permissions** tabs of the `shopwave` schema. You are its **owner**.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 7 · ✅ Automatic checks
# MAGIC Run the cell — every line should show ✅.

# COMMAND ----------

# DBTITLE 1,Check your environment
_checks = {
    "Current catalog is the course catalog": spark.sql("SELECT current_catalog()").first()[0] == catalog_name,
    "Current schema is the course schema": spark.sql("SELECT current_schema()").first()[0] == schema_name,
}
for _v in ("raw", "checkpoints", "files"):
    _checks[f"Volume '{_v}' exists"] = path_exists(f"{volume_root}/{_v}")
for _d in ("customers-json", "products-csv", "orders-parquet", "orders-staging", "orders-landing"):
    _checks[f"Dataset '{_d}' generated"] = path_exists(f"{dataset_path}/{_d}")
_checks["Customers = 300"] = spark.read.json(f"{dataset_path}/customers-json").count() == 300

for name, ok in _checks.items():
    print(("✅ " if ok else "❌ ") + name)
print("\n🎉 Environment ready - continue with Section 01!" if all(_checks.values())
      else "\n⚠️ Something is missing - see the troubleshooting table below.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🛟 Troubleshooting
# MAGIC
# MAGIC | Symptom | Fix |
# MAGIC |---|---|
# MAGIC | `Notebook not found: ../../Includes/_setup` | The folder structure changed. Re-import the zip without moving folders. |
# MAGIC | `Could not create schema/volumes in catalog …` | You can't create schemas in your default catalog. Add a cell **above** the `%run` with `COURSE_CATALOG = "<a catalog you own>"`. |
# MAGIC | `Your default catalog is the legacy Hive metastore …` | Your workspace defaults to `hive_metastore`. Set `COURSE_CATALOG` as above to a Unity Catalog catalog. |
# MAGIC | Setup is slow the first time | Normal — the data is generated once. Later runs take seconds. |
# MAGIC | You want a clean start | Run `reset_course(confirm="YES")` in a new cell, then re-run the `%run` cell. |
# MAGIC
# MAGIC ### 🧹 Optional: reset everything
# MAGIC Uncomment and run **only** if you want to delete all course tables and files.

# COMMAND ----------

# DBTITLE 1,Reset (optional)
# reset_course(confirm="YES")
