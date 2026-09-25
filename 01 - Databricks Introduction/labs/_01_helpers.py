# Databricks notebook source
# MAGIC %md
# MAGIC # 🔧 _01_helpers — included by Lab 01-L1 with `%run ./_01_helpers`
# MAGIC Everything defined here becomes available in the calling notebook, because `%run` executes this code
# MAGIC **in the caller's execution context**. Don't run this notebook on its own — it's meant to be included.

# COMMAND ----------

# DBTITLE 1,Shared variable and function
helpers_message = "👋 Hello from _01_helpers - this variable was defined by %run"

SAR_PER_USD = 3.75


def sar_to_usd(amount_sar: float) -> float:
    """Convert Saudi riyals to US dollars (fixed peg rate)."""
    return round(amount_sar / SAR_PER_USD, 2)
