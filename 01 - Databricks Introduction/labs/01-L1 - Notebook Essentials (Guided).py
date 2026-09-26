# Databricks notebook source
# MAGIC %md
# MAGIC # 🧪 Lab 01-L1 · Notebook Essentials (Guided)
# MAGIC **Time:** ~40 min · **Compute:** Serverless (or any Unity Catalog compute) · **Default language of this notebook:** Python
# MAGIC
# MAGIC | Part | Skill | Exam relevance |
# MAGIC |---|---|---|
# MAGIC | 1 | Run the course setup with `%run` | notebook modularity |
# MAGIC | 2 | Python, SQL and Markdown cells, magic commands | everyday skill |
# MAGIC | 3 | `display()` vs `.show()`, visualizations | everyday skill |
# MAGIC | 4 | `dbutils.fs` with Unity Catalog volumes | files & volumes (D1, D2) |
# MAGIC | 5 | Widgets — parameterised notebooks | jobs parameters (D4) |
# MAGIC | 6 | Python ⇄ SQL interoperability | D3 |
# MAGIC | 7 | `%run` vs `dbutils.notebook.run()` | 🎯 classic exam question |
# MAGIC | 8 | Automatic checks | |
# MAGIC
# MAGIC > 🧪 Run each cell **one by one** (`Shift + Enter`) and read the explanation above it. Don't just *Run all*.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1 · Include the course setup with `%run`
# MAGIC `%run` must be **alone in its cell**, and the path is **relative to this notebook**.
# MAGIC It executes `Includes/_setup` **in this notebook's context**, so its variables (`dataset_path`, `files_path` …) and
# MAGIC functions (`land_new_orders()` …) become available here.

# COMMAND ----------

# MAGIC %run ../../Includes/_setup

# COMMAND ----------

# DBTITLE 1,Variables created by the setup
print("dataset_path =", dataset_path)
print("files_path   =", files_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2 · Python, SQL and Markdown cells
# MAGIC
# MAGIC This notebook's default language is **Python**, so a cell without a magic command runs Python.

# COMMAND ----------

# DBTITLE 1,A Python cell
cities = ["Riyadh", "Paris", "Tokyo"]
for c in cities:
    print(f"ShopWave now delivers to {c} 🚚")

# COMMAND ----------

# MAGIC %md
# MAGIC A cell that starts with **`%sql`** runs Spark SQL, whatever the notebook's default language is.

# COMMAND ----------

# DBTITLE 1,A SQL cell (magic command %sql)
# MAGIC %sql
# MAGIC SELECT 'Hello from SQL' AS greeting, current_date() AS today, current_user() AS me

# COMMAND ----------

# MAGIC %md
# MAGIC ### ✍️ Markdown cells
# MAGIC This text is a **Markdown** cell (`%md`). Double-click it to see the source. Markdown supports:
# MAGIC
# MAGIC # Heading 1
# MAGIC ## Heading 2
# MAGIC **bold**, *italic*, `inline code`, [links](https://docs.databricks.com), and lists:
# MAGIC 1. ordered item
# MAGIC 1. another one (numbers are auto-generated)
# MAGIC * bullet
# MAGIC * bullet
# MAGIC
# MAGIC | tables | work | too |
# MAGIC |---|---|---|
# MAGIC | ✅ | 📊 | 🚀 |
# MAGIC
# MAGIC > 🧪 **Try it:** add a new cell below (hover between cells → **+ Code**), type `%md`, write a heading and a bullet list, and run it.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 3 · Create a practice table and look at data
# MAGIC
# MAGIC We read the customer JSON files and save them as a table called **`lab01_customers`**.
# MAGIC *(Reading files and creating tables are covered in depth in Sections 03 and 05 — here we just need something to query.)*

# COMMAND ----------

# DBTITLE 1,Create the practice table
customers_df = spark.read.json(f"{dataset_path}/customers-json")
customers_df.write.mode("overwrite").saveAsTable("lab01_customers")
print("Rows written:", spark.table("lab01_customers").count())

# COMMAND ----------

# MAGIC %md
# MAGIC ### `display()` vs `.show()`

# COMMAND ----------

# DBTITLE 1,display(): interactive table
display(spark.table("lab01_customers"))

# COMMAND ----------

# DBTITLE 1,.show(): plain text
spark.table("lab01_customers").show(5, truncate=40)

# COMMAND ----------

# MAGIC %md
# MAGIC The `profile` column is a **JSON string**. Databricks SQL can reach inside it with the **colon syntax** `profile:address:country`
# MAGIC (details in Section 07).

# COMMAND ----------

# DBTITLE 1,Customers per country
# MAGIC %sql
# MAGIC SELECT profile:address:country AS country,
# MAGIC        count(*)                AS customers
# MAGIC FROM lab01_customers
# MAGIC GROUP BY 1
# MAGIC ORDER BY customers DESC

# COMMAND ----------

# MAGIC %md
# MAGIC > 🖱️ **Try a visualization:** in the result above click **+** → **Visualization** → type **Bar**, X = `country`, Y = `customers` → **Save**.
# MAGIC > Also try **+** → **Data profile** to get summary statistics for every column.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 4 · `dbutils.fs` — files in Unity Catalog volumes
# MAGIC
# MAGIC `dbutils` is available in every notebook. Start with the built-in help:

# COMMAND ----------

# DBTITLE 1,dbutils help
dbutils.fs.help()

# COMMAND ----------

# DBTITLE 1,List the raw volume
display(dbutils.fs.ls(dataset_path))

# COMMAND ----------

# DBTITLE 1,List one dataset folder
display(dbutils.fs.ls(f"{dataset_path}/customers-json"))

# COMMAND ----------

# DBTITLE 1,Peek at the beginning of a file
print(dbutils.fs.head(f"{dataset_path}/customers-json/export_001.json", 400))

# COMMAND ----------

# MAGIC %md
# MAGIC ### The `%fs` shortcut
# MAGIC `%fs <command> <path>` is shorthand for `dbutils.fs.<command>(path)`. It needs a **literal path** (no Python variables),
# MAGIC so the next cell prints the exact command for *your* workspace.
# MAGIC
# MAGIC > 🧪 Copy the printed line into a **new cell** and run it.

# COMMAND ----------

# DBTITLE 1,Your %fs command
print(f"%fs ls {dataset_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create, copy, move and delete files in your sandbox volume
# MAGIC The `files` volume is your playground. We'll create a folder, write a file, copy it, move it and clean up.

# COMMAND ----------

# DBTITLE 1,mkdirs + put
sandbox = f"{files_path}/lab01_sandbox"
dbutils.fs.mkdirs(sandbox)
dbutils.fs.put(f"{sandbox}/hello.txt", "Hello, volumes! 👋\nLine 2\n", True)   # True = overwrite
print(dbutils.fs.head(f"{sandbox}/hello.txt"))

# COMMAND ----------

# DBTITLE 1,cp + mv + ls
dbutils.fs.mkdirs(f"{sandbox}/copy")
dbutils.fs.cp(f"{sandbox}/hello.txt", f"{sandbox}/copy/hello_copy.txt")
dbutils.fs.mv(f"{sandbox}/copy/hello_copy.txt", f"{sandbox}/moved.txt")
display(dbutils.fs.ls(sandbox))

# COMMAND ----------

# DBTITLE 1,rm (recursive)
dbutils.fs.rm(sandbox, True)   # True = recursive
print("Sandbox exists after rm?", path_exists(sandbox))

# COMMAND ----------

# MAGIC %md
# MAGIC > 🎯 **Exam focus:** `dbutils.fs` and `%fs` work with **`/Volumes/<catalog>/<schema>/<volume>/…`** paths.
# MAGIC > 🕰️ Older material uses `dbfs:/FileStore/…` or `dbfs:/mnt/…` — recognise them, but prefer volumes.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 5 · Widgets — parameterise your notebook
# MAGIC Widgets appear at the **top** of the notebook. When a notebook runs as a **job task**, job parameters with the same
# MAGIC name override the widget values.

# COMMAND ----------

# DBTITLE 1,Create widgets
countries = [r.country for r in spark.sql(
    "SELECT DISTINCT profile:address:country AS country FROM lab01_customers ORDER BY 1").collect()]
dbutils.widgets.dropdown("country", "Saudi Arabia", countries, "Country")
dbutils.widgets.text("min_customers", "5", "Min customers per city")
print("Widgets created - look at the top of the notebook ⬆️")

# COMMAND ----------

# DBTITLE 1,Read widget values in Python
country = dbutils.widgets.get("country")
min_customers = int(dbutils.widgets.get("min_customers"))   # widgets always return STRINGS
print(f"country={country!r}  min_customers={min_customers}")

# COMMAND ----------

# MAGIC %md
# MAGIC In **SQL**, refer to a widget with a **named parameter marker** `:widget_name`.
# MAGIC > 🧪 Change the **country** widget at the top, then re-run the cell below.

# COMMAND ----------

# DBTITLE 1,Use widgets in SQL with :name
# MAGIC %sql
# MAGIC SELECT profile:address:city AS city, count(*) AS customers
# MAGIC FROM lab01_customers
# MAGIC WHERE profile:address:country = :country
# MAGIC GROUP BY 1
# MAGIC ORDER BY customers DESC

# COMMAND ----------

# MAGIC %md
# MAGIC The same query from Python — pass values with `args=` (**parameterised**, safe from SQL injection):

# COMMAND ----------

# DBTITLE 1,Parameterised spark.sql()
display(spark.sql("""
    SELECT profile:address:city AS city, count(*) AS customers
    FROM lab01_customers
    WHERE profile:address:country = :country
    GROUP BY 1
    HAVING count(*) >= :min_customers
""", args={"country": country, "min_customers": min_customers}))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 6 · Python ⇄ SQL
# MAGIC
# MAGIC **Python → SQL:** register a DataFrame as a **temporary view**.

# COMMAND ----------

# DBTITLE 1,Python DataFrame -> temp view
orders_df = spark.read.parquet(f"{dataset_path}/orders-parquet")
orders_df.createOrReplaceTempView("lab01_orders_v")
print("Temp view lab01_orders_v created (visible only in THIS notebook's session)")

# COMMAND ----------

# DBTITLE 1,Query the temp view from SQL
# MAGIC %sql
# MAGIC SELECT date_format(from_unixtime(order_timestamp), 'yyyy-MM') AS month,
# MAGIC        count(*)                                          AS orders,
# MAGIC        round(sum(total), 2)                              AS revenue
# MAGIC FROM lab01_orders_v
# MAGIC GROUP BY 1
# MAGIC ORDER BY 1

# COMMAND ----------

# MAGIC %md
# MAGIC **SQL → Python:** use `spark.sql()` — or the special variable **`_sqldf`**, which holds the result of the last `%sql` cell.

# COMMAND ----------

# DBTITLE 1,SQL result back in Python
monthly_df = spark.sql("""
    SELECT date_format(from_unixtime(order_timestamp), 'yyyy-MM') AS month, round(sum(total), 2) AS revenue
    FROM lab01_orders_v GROUP BY 1 ORDER BY 1""")
best = monthly_df.orderBy("revenue", ascending=False).first()
print(f"Best month: {best.month} with revenue {best.revenue:,.2f}")

try:
    print("_sqldf from the previous %sql cell has", _sqldf.count(), "rows")
except NameError:
    print("_sqldf is only set after a %sql cell has run in this session")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 7 · 🎯 `%run` vs `dbutils.notebook.run()`
# MAGIC
# MAGIC This folder contains two small notebooks:
# MAGIC * **`_01_helpers`** — defines a variable and a function (to be **included** with `%run`)
# MAGIC * **`_01_child`** — takes parameters and **returns** a value with `dbutils.notebook.exit()` (to be **called** with `dbutils.notebook.run()`)
# MAGIC
# MAGIC ### A) `%run` → same context, shares variables & functions

# COMMAND ----------

# MAGIC %run ./_01_helpers

# COMMAND ----------

# DBTITLE 1,Use what %run brought in
print(helpers_message)
print("150 SAR in USD =", sar_to_usd(150))

# COMMAND ----------

# MAGIC %md
# MAGIC ### B) `dbutils.notebook.run()` → separate run, parameters in, string out
# MAGIC The child runs **in its own context** (it can't see our variables), receives **arguments** as widget values,
# MAGIC and sends back a **string**. We pass it our catalog, schema and country.

# COMMAND ----------

# DBTITLE 1,Call a child notebook with parameters
import json

try:
    result = dbutils.notebook.run(
        "./_01_child",                       # path (can be computed at runtime!)
        300,                                 # timeout in seconds
        {"catalog": catalog_name, "schema": schema_name, "country": country},
    )
    print("Raw return value:", result, type(result))
    print("Parsed:", json.loads(result))
except Exception as e:
    print("dbutils.notebook.run is not available on this compute:", str(e)[:200])

# COMMAND ----------

# MAGIC %md
# MAGIC | | `%run` | `dbutils.notebook.run()` |
# MAGIC |---|---|---|
# MAGIC | Context | same | new, separate |
# MAGIC | Sees caller's variables | ✅ | ❌ |
# MAGIC | Parameters | ❌ | ✅ via `arguments` → widgets |
# MAGIC | Return value | ❌ | ✅ string from `dbutils.notebook.exit()` |
# MAGIC | Path can be a variable | ❌ | ✅ |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 8 · ✅ Automatic checks

# COMMAND ----------

# DBTITLE 1,Check your work
def _widget_exists(name):
    try:
        dbutils.widgets.get(name)
        return True
    except Exception:
        return False


_checks = {
    "Table lab01_customers has 300 rows": spark.table("lab01_customers").count() == 300,
    "Widget 'country' exists": _widget_exists("country"),
    "Temp view lab01_orders_v exists": spark.catalog.tableExists("lab01_orders_v"),
    "Function sar_to_usd came from %run": callable(globals().get("sar_to_usd")),
    "Sandbox folder was cleaned up": not path_exists(f"{files_path}/lab01_sandbox"),
}
for name, ok in _checks.items():
    print(("✅ " if ok else "❌ ") + name)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🧹 Clean up widgets
# MAGIC Widgets stay on the notebook until removed.

# COMMAND ----------

# DBTITLE 1,Remove widgets
dbutils.widgets.removeAll()
print("Widgets removed. 🎉 Lab complete - next: 01-L2 · Compute & Workspace Tour")