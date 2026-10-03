# Databricks notebook source
# MAGIC %md
# MAGIC # 🗂️ 13-D · Data used in Section 13
# MAGIC
# MAGIC Section 13 secures **personal data**, so `labs/_13_prepare` builds a few small tables with PII from the ShopWave base data.
# MAGIC All names, e-mails, phone and card numbers are **generated** (card numbers are in the `4000 00…` test range).
# MAGIC
# MAGIC | Table | Rows | Columns | Built from |
# MAGIC |---|---|---|---|
# MAGIC | `sec_customers` | 300 | `customer_id`, `first_name`, `last_name`, **`email`** (19 NULL), **`phone`** (country dial code), `country`, `city`, `loyalty_tier`, **`card_number`** | `customers-json` + deterministic fake digits |
# MAGIC | `sec_orders` | 1,980 | `order_id`, `order_date`, `customer_id`, **`country`**, `total` | `orders-parquet` (dedup, no cancelled orders), country of the customer (`Unknown` for `C9999`) |
# MAGIC | `sec_region_access` | 4 + yours | `user_email`, `country` | the **mapping table** for row-level security; `grant_region()` / `revoke_region()` add/remove **your** rows |
# MAGIC | `ch13_employees` | 40 | `employee_id`, `full_name`, `email`, `department`, `country`, **`salary`** | generated: 4 departments × 5 countries × 2 |
# MAGIC | `ch13_dept_access` | 2 + yours | `user_email`, `department` | mapping table of the challenge |
# MAGIC
# MAGIC | Key figure | Value |
# MAGIC |---|---|
# MAGIC | Orders · revenue — Saudi Arabia | **197** · 110,438.74 |
# MAGIC | Orders — Egypt · Saudi Arabia + Egypt | 222 · **419** |
# MAGIC | Customers — Saudi Arabia · Egypt | **30** · 34 |
# MAGIC | Employees per department | 10 (Engineering, Finance, HR, Sales) |
# MAGIC
# MAGIC **Objects the labs create**
# MAGIC
# MAGIC | Lab | Objects |
# MAGIC |---|---|
# MAGIC | 13-L1 | grants to `G13` (your `COURSE_GROUP` or `account users`), `sec_new_products`, function `sec_fn_tier` |
# MAGIC | 13-L2 | view `sec_v_customers`, functions `sec_country_filter`, `sec_mask_email`, `sec_mask_card`, `sec_mask_phone` (row filter / masks removed at the end) |
# MAGIC | 13-L3 | governed tags `sw_pii`, `sw_geo` (account level), column tags, policies `sw_mask_pii`, `sw_geo_rows` (dropped at the end), `sec_leads`, `sec_gold_country_sales`, function `sec_abac_mask` |
# MAGIC | 13-L4 | functions `ch13_dept_filter`, `ch13_mask_salary`, view `ch13_v_directory` |

# COMMAND ----------

# MAGIC %run ../../Includes/_setup

# COMMAND ----------

# MAGIC %run ../labs/_13_prepare

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔍 Preview

# COMMAND ----------

# DBTITLE 1,Tables and row counts (as YOU see them - protections apply)
_rows = []
for _t in ("sec_customers", "sec_orders", "sec_region_access", "ch13_employees", "ch13_dept_access",
           "sec_v_customers", "sec_leads", "sec_gold_country_sales", "ch13_v_directory"):
    _rows.append((_t, spark.table(_t).count() if spark.catalog.tableExists(_t) else None))
display(spark.createDataFrame(_rows, "object STRING, rows_visible_to_you LONG"))
print("your regions:", my_regions() or "none")

# COMMAND ----------

# DBTITLE 1,Protections currently in place (row filters, masks, policies, grants to G13)
for _view in ("row_filters", "column_masks"):
    try:
        display(spark.sql(f"SELECT * FROM `{catalog_name}`.information_schema.{_view} WHERE table_schema = '{schema_name}'"))
    except Exception as e:
        print(f"information_schema.{_view}:", _first_line(e))
print("ABAC policies on the schema:", _policy_names() or "none")
try:
    display(table_privileges("sec_"))
except Exception as e:
    print("information_schema.table_privileges:", _first_line(e))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧹 Reset Section 13
# MAGIC `reset_lab13(confirm="YES")` drops the policies, row filters, masks, views, functions and `sec_*` / `ch13_*` tables and
# MAGIC revokes the schema-level grants given to `G13`. Governed tags are account-level objects and are **kept** (delete them in
# MAGIC *Catalog → Govern → Governed tags* if you want).

# COMMAND ----------

# DBTITLE 1,Reset (optional)
# reset_lab13(confirm="YES")
