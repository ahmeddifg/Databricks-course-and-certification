# Databricks notebook source
# MAGIC %md
# MAGIC # 🏆 Lab 01-L3 · Challenge Lab
# MAGIC **Time:** ~40 min · **Compute:** Serverless
# MAGIC
# MAGIC No step-by-step instructions this time — only the goal. Replace every `# TODO` / `None` and run the
# MAGIC **✅ Check** cell after each task. Stuck? Look back at **01-L1** / **01-L2**, then at **01-L3 - Challenge Lab (Solution)**.
# MAGIC
# MAGIC | Task | Skill |
# MAGIC |---|---|
# MAGIC | 1 | Rename the notebook & attach compute (UI) |
# MAGIC | 2 | Python cell |
# MAGIC | 3 | Read files → table → SQL |
# MAGIC | 4 | Markdown |
# MAGIC | 5 | `%run` with your own helper notebook |
# MAGIC | 6 | `dbutils.fs` |
# MAGIC | 7 | Widgets + parameterised SQL |
# MAGIC | 8 | Python ⇄ SQL |
# MAGIC | 9 | Compute choice |

# COMMAND ----------

# MAGIC %run ../../Includes/_setup

# COMMAND ----------

# DBTITLE 1,Check helper (run me - don't edit)
def check(label, condition):
    print(("✅ " if condition else "❌ ") + label)


def widget_value(name):
    """Return a widget's value, or None if the widget doesn't exist yet."""
    try:
        return dbutils.widgets.get(name)
    except Exception:
        return None

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 1 · 🖱️ Rename & attach
# MAGIC 1. Clone this notebook (**File ▸ Clone**) and name the clone **`01-L3 - My Challenge`** — work in the clone.
# MAGIC 2. Make sure it is attached to **Serverless** compute.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 2 · Python
# MAGIC Assign to `total_customers` the number of customer records in the JSON files under `f"{dataset_path}/customers-json"`.

# COMMAND ----------

# DBTITLE 1,Task 2
# TODO
df_customers=spark.read.json(f"{dataset_path}/customers-json")
total_customers = df_customers.count()

# COMMAND ----------

# DBTITLE 1,✅ Check 2
check("total_customers is 300", total_customers == 300)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 3 · Files → table → SQL
# MAGIC 1. Read the **products** CSV files in `f"{dataset_path}/products-csv"` — delimiter `;`, header row, infer the schema.
# MAGIC 2. Save them as a table named **`lab01_products`** (overwrite if it exists).
# MAGIC 3. In a **SQL cell** (create it below), list each `category` with its number of products and average price (rounded to 2 decimals), most expensive category first.

# COMMAND ----------

# DBTITLE 1,Task 3.1 + 3.2
# TODO: read the CSV files and save them as table lab01_products
products_df = spark.read.csv(f"{dataset_path}/products-csv", header=True, inferSchema=True, sep=";")
products_df.write.mode("overwrite").saveAsTable("lab01_products") 

# COMMAND ----------

# MAGIC %md
# MAGIC *(Task 3.3 — add your `%sql` cell here.)*

# COMMAND ----------

# DBTITLE 1,✅ Check 3
exists = spark.catalog.tableExists("lab01_products")
check("table lab01_products exists", exists)
check("lab01_products has 36 rows", exists and spark.table("lab01_products").count() == 36)
check("price column is numeric", exists and dict(spark.table("lab01_products").dtypes).get("price") in ("double", "decimal(10,2)", "float"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 4 · Markdown
# MAGIC Add a Markdown cell below this one containing:
# MAGIC * a level-2 heading **ShopWave categories**
# MAGIC * a bullet list of the **6** product categories
# MAGIC * the word **lakehouse** in bold

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 5 · Your own helper notebook with `%run`
# MAGIC 1. In **this folder**, create a new Python notebook named **`my_helper`**.
# MAGIC 2. In it, define:
# MAGIC    * a variable `my_country` with your country's name
# MAGIC    * a function `add_vat(amount, rate=0.15)` returning `amount * (1 + rate)` **rounded to 2 decimals**
# MAGIC 3. Back here, add a cell containing only `%run ./my_helper` and run it.

# COMMAND ----------

# DBTITLE 1,✅ Check 5
check("my_country is defined", isinstance(globals().get("my_country"), str))
check("add_vat(100) == 115.0", callable(globals().get("add_vat")) and add_vat(100) == 115.0)
check("add_vat(80, 0.05) == 84.0", callable(globals().get("add_vat")) and add_vat(80, 0.05) == 84.0)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 6 · `dbutils.fs`
# MAGIC 1. Create the folder `f"{files_path}/challenge"` and write a file **`notes.txt`** inside it containing the text `ShopWave rocks`.
# MAGIC 2. Assign to `n_order_files` the number of **`.parquet`** files in `f"{dataset_path}/orders-parquet"` (ignore `_SUCCESS`, `_committed…` etc.).

# COMMAND ----------

# DBTITLE 1,Task 6
# TODO
n_order_files = None

# COMMAND ----------

# DBTITLE 1,✅ Check 6
_notes = f"{files_path}/challenge/notes.txt"
check("notes.txt exists", path_exists(_notes))
check("notes.txt contains 'ShopWave rocks'", path_exists(_notes) and "ShopWave rocks" in dbutils.fs.head(_notes))
check("n_order_files is correct",
      n_order_files == len([f for f in dbutils.fs.ls(f"{dataset_path}/orders-parquet") if f.name.endswith(".parquet")]))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 7 · Widgets
# MAGIC 1. Create a **dropdown** widget named **`category`** whose choices are the distinct categories in `lab01_products`, default **`Books`**.
# MAGIC 2. Read it into the Python variable `selected_category`.
# MAGIC 3. Using **`spark.sql` with `args=`** (no f-strings!), assign to `n_selected` the number of products in the selected category.

# COMMAND ----------

# DBTITLE 1,Task 7
# TODO
selected_category = None
n_selected = None

# COMMAND ----------

# DBTITLE 1,✅ Check 7
check("selected_category matches the widget", selected_category is not None and selected_category == widget_value("category"))
check("n_selected is correct", n_selected is not None and n_selected ==
      spark.table("lab01_products").where(f"category = '{selected_category}'").count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 8 · Python ⇄ SQL
# MAGIC 1. Register the orders Parquet data (`f"{dataset_path}/orders-parquet"`) as a temporary view **`lab01_orders_v`**.
# MAGIC 2. With SQL, compute the total revenue (`sum(total)`) of orders placed in **June 2026**
# MAGIC    (`order_timestamp` is Unix seconds → use `from_unixtime`), and store it in the Python variable `june_revenue` (a float).

# COMMAND ----------

# DBTITLE 1,Task 8
# TODO
june_revenue = None

# COMMAND ----------

# DBTITLE 1,✅ Check 8
from pyspark.sql import functions as F

_expected = (spark.read.parquet(f"{dataset_path}/orders-parquet")
             .where(F.date_format(F.from_unixtime("order_timestamp"), "yyyy-MM") == "2026-06")
             .agg(F.sum("total")).first()[0])
check("temp view lab01_orders_v exists", spark.catalog.tableExists("lab01_orders_v"))
check("june_revenue is correct", june_revenue is not None and abs(float(june_revenue) - _expected) < 0.01)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 9 · Compute choice
# MAGIC A team needs to run a **nightly production pipeline** written in Python. They want the **lowest operational overhead**
# MAGIC and to **stop paying for idle compute**. Which option is best?
# MAGIC
# MAGIC * **A** — An all-purpose cluster with auto-termination disabled
# MAGIC * **B** — Serverless compute for jobs
# MAGIC * **C** — A Pro SQL warehouse
# MAGIC * **D** — A single-node all-purpose cluster started manually each night

# COMMAND ----------

# DBTITLE 1,Task 9
answer_task9 = None   # "A", "B", "C" or "D"

# COMMAND ----------

# DBTITLE 1,✅ Check 9
check("Task 9 answer", answer_task9 == "B")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🏁 Final score

# COMMAND ----------

# DBTITLE 1,Score
_final = {
    "Task 2": total_customers == 300,
    "Task 3": spark.catalog.tableExists("lab01_products") and spark.table("lab01_products").count() == 36,
    "Task 5": callable(globals().get("add_vat")),
    "Task 6": path_exists(f"{files_path}/challenge/notes.txt") and n_order_files is not None,
    "Task 7": n_selected is not None,
    "Task 8": june_revenue is not None,
    "Task 9": answer_task9 == "B",
}
for k, v in _final.items():
    print(("✅ " if v else "❌ ") + k)
print(f"\nScore: {sum(_final.values())} / {len(_final)} automatically checked tasks (Tasks 1 & 4 are checked by you)")