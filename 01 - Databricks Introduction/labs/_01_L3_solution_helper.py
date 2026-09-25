# Databricks notebook source
# MAGIC %md
# MAGIC # 🔧 _01_L3_solution_helper — reference answer for Challenge Task 5
# MAGIC Included by `01-L3 - Challenge Lab (Solution)` with `%run ./_01_L3_solution_helper`.

# COMMAND ----------

# DBTITLE 1,Helper definitions
my_country = "Saudi Arabia"


def add_vat(amount, rate=0.15):
    """Return the amount including VAT, rounded to 2 decimals."""
    return round(amount * (1 + rate), 2)
