# Databricks notebook source
# MAGIC %md
# MAGIC # 🧪 Lab 13-L2 · Row Filters, Column Masks and Dynamic Views (Guided)
# MAGIC **Time:** ~50 min · **Compute:** Serverless notebook (or a SQL warehouse) · **Works on Free Edition**
# MAGIC
# MAGIC Regional managers may only see the orders and customers of **their** countries, and nobody outside the PII team may see
# MAGIC full e-mail addresses or card numbers. You'll implement both rules twice — first as a **dynamic view**, then directly on
# MAGIC the tables with a **row filter** and **column masks** — driven by a **mapping table**, so you can change *your own* access
# MAGIC and watch the result change.
# MAGIC
# MAGIC | Part | You will… |
# MAGIC |---|---|
# MAGIC | 1 | Look at the data and the mapping table `sec_region_access` |
# MAGIC | 2 | A **dynamic view** that filters rows and masks columns |
# MAGIC | 3 | A **row filter** on `sec_orders` (mapping table + admin group) |
# MAGIC | 4 | **Column masks** on `sec_customers` (e-mail, card number, phone with `USING COLUMNS`) |
# MAGIC | 5 | Inspect: `DESCRIBE`, `information_schema` |
# MAGIC | 6 | Limits: time travel and clones |
# MAGIC | 7 | ✅ Checks and clean-up |

# COMMAND ----------

# MAGIC %run ../../Includes/_setup

# COMMAND ----------

# MAGIC %run ./_13_prepare

# COMMAND ----------

# DBTITLE 1,Start clean (safe to re-run)
drop_lab13_policies()                                      # in case 13-L3 left ABAC policies behind
for _t in ("sec_orders", "sec_customers"):
    _drop_security(_t)
for _stmt in ("DROP VIEW IF EXISTS sec_v_customers",):
    spark.sql(_stmt)
revoke_region()                                            # you start with NO region


def check(label, condition):
    print(("✅ " if condition else "❌ ") + label)
    return bool(condition)


def expected_orders(countries) -> int:
    """Orders of the given countries, computed from the raw FILES (not through the protected table)."""
    cust = spark.table("sec_customers").select("customer_id", "country")
    return (spark.read.schema(ORDER_SCHEMA).parquet(f"{dataset_path}/orders-parquet")
                 .dropDuplicates(["order_id"]).where("quantity > 0")
                 .join(cust, "customer_id").where(F.col("country").isin(list(countries) or ["-"])).count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1 · The data and the mapping table

# COMMAND ----------

# DBTITLE 1,PII in sec_customers
# MAGIC %sql
# MAGIC SELECT customer_id, first_name, email, phone, country, card_number
# MAGIC FROM sec_customers
# MAGIC ORDER BY customer_id
# MAGIC LIMIT 5

# COMMAND ----------

# DBTITLE 1,The mapping table - who may see which country
# MAGIC %sql
# MAGIC SELECT * FROM sec_region_access ORDER BY user_email, country

# COMMAND ----------

# MAGIC %md
# MAGIC Four fictitious managers — and **you** have no row yet (`my_regions()` is empty). In real life an admin team maintains
# MAGIC this table (and only they can modify it).
# MAGIC
# MAGIC ## Part 2 · A dynamic view
# MAGIC A view whose result depends on **who** runs it: `current_user()` for the row filter (through the mapping table),
# MAGIC `is_account_group_member()` for exemptions and column masking.

# COMMAND ----------

# DBTITLE 1,CREATE the dynamic view
# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW sec_v_customers
# MAGIC COMMENT 'Section 13: customers of the regions of the current user, PII masked unless in shopwave_pii_readers'
# MAGIC AS
# MAGIC SELECT customer_id, first_name, last_name, city, country, loyalty_tier,
# MAGIC        CASE WHEN is_account_group_member('shopwave_pii_readers') THEN email
# MAGIC             ELSE regexp_replace(email, '^(.)[^@]*', '$1***') END            AS email,
# MAGIC        concat('**** ', right(card_number, 4))                               AS card_last4
# MAGIC FROM sec_customers
# MAGIC WHERE is_account_group_member('shopwave_admins')
# MAGIC    OR country IN (SELECT country FROM sec_region_access WHERE user_email = current_user())

# COMMAND ----------

# DBTITLE 1,Query it - then give yourself a region
print("rows before:", spark.table("sec_v_customers").count())
grant_region("Saudi Arabia")
print("rows after :", spark.table("sec_v_customers").count())
display(spark.table("sec_v_customers").orderBy("customer_id").limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC **0 rows → 30 rows**: same view, same query — the **data** of the mapping table changed your access. E-mails show as
# MAGIC `a***@shopwave.org` and cards as `**** 1233` because you're not in `shopwave_pii_readers`.
# MAGIC
# MAGIC Share the **view**, not the table:

# COMMAND ----------

# DBTITLE 1,Grant the view to the group
try_sql(f"GRANT USE SCHEMA ON SCHEMA `{catalog_name}`.`{schema_name}` TO `{G13}`")
try_sql(f"GRANT SELECT ON VIEW sec_v_customers TO `{G13}`")
print("rows of the BASE table you can still read:", spark.table("sec_customers").count(), "(all of them - you own it)")

# COMMAND ----------

# MAGIC %md
# MAGIC > ⚠️ The weak spot of a dynamic view: it protects only readers who go **through the view**. You (owner), and anybody with
# MAGIC > `SELECT` on `sec_customers`, still see everything. To protect the **table itself**, attach the rules to the table.
# MAGIC
# MAGIC ## Part 3 · A row filter on `sec_orders`
# MAGIC A row filter is a SQL **function returning BOOLEAN**, attached to the table. It receives the values of the columns you
# MAGIC name in `ON (...)`; rows where it returns `FALSE` disappear — for **every** query, user and tool.

# COMMAND ----------

# DBTITLE 1,The filter function
# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION sec_country_filter(p_country STRING)
# MAGIC RETURNS BOOLEAN
# MAGIC COMMENT 'Section 13 row filter: admins see all rows, others only the countries of their mapping rows'
# MAGIC RETURN is_account_group_member('shopwave_admins')
# MAGIC     OR EXISTS (SELECT 1 FROM sec_region_access a
# MAGIC                WHERE a.user_email = current_user() AND a.country = p_country)

# COMMAND ----------

# DBTITLE 1,Attach it to the table
# MAGIC %sql
# MAGIC ALTER TABLE sec_orders SET ROW FILTER sec_country_filter ON (country)

# COMMAND ----------

# DBTITLE 1,What do you see now?
display(spark.sql("SELECT country, count(*) AS orders, round(sum(total), 2) AS revenue FROM sec_orders GROUP BY country"))
print("my regions:", my_regions(), "· expected rows:", expected_orders(my_regions()))

# COMMAND ----------

# MAGIC %md
# MAGIC Expected: only **Saudi Arabia — 197 orders, 110,438.74**. You are the **owner** of `sec_orders` and still see only your
# MAGIC region: row filters have no owner exemption — exemptions are written **inside** the function (here: the group
# MAGIC `shopwave_admins`).
# MAGIC
# MAGIC Give yourself Egypt, look again, take it away:

# COMMAND ----------

# DBTITLE 1,Change the mapping - change the rows
grant_region("Egypt")
print("rows:", spark.table("sec_orders").count(), "(expected", expected_orders(my_regions()), ")")
revoke_region("Egypt")
print("rows:", spark.table("sec_orders").count(), "(expected", expected_orders(my_regions()), ")")

# COMMAND ----------

# MAGIC %md
# MAGIC **197 → 419 → 197.** Adding a region for a manager is an `INSERT` into the mapping table — no code change, no new view.
# MAGIC
# MAGIC ## Part 4 · Column masks on `sec_customers`
# MAGIC A column mask is a SQL function whose **first parameter is the column value**; it returns the value to show. Extra
# MAGIC columns can be passed with `USING COLUMNS`.
# MAGIC
# MAGIC | Column | Rule |
# MAGIC |---|---|
# MAGIC | `email` | full value for `shopwave_pii_readers`, otherwise `a***@domain` |
# MAGIC | `card_number` | always only the last 4 digits (PCI) |
# MAGIC | `phone` | full value only for customers in **your** regions, otherwise `+*** *** ***` — needs the row's `country` → `USING COLUMNS (country)` |

# COMMAND ----------

# DBTITLE 1,Mask functions
# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION sec_mask_email(email STRING) RETURNS STRING
# MAGIC RETURN CASE WHEN is_account_group_member('shopwave_pii_readers') THEN email
# MAGIC             ELSE regexp_replace(email, '^(.)[^@]*', '$1***') END;
# MAGIC
# MAGIC CREATE OR REPLACE FUNCTION sec_mask_card(card STRING) RETURNS STRING
# MAGIC RETURN concat('**** **** **** ', right(card, 4));
# MAGIC
# MAGIC CREATE OR REPLACE FUNCTION sec_mask_phone(phone STRING, p_country STRING) RETURNS STRING
# MAGIC RETURN CASE WHEN EXISTS (SELECT 1 FROM sec_region_access a
# MAGIC                          WHERE a.user_email = current_user() AND a.country = p_country)
# MAGIC             THEN phone ELSE '+*** *** ***' END;

# COMMAND ----------

# DBTITLE 1,Attach the masks
# MAGIC %sql
# MAGIC ALTER TABLE sec_customers ALTER COLUMN email SET MASK sec_mask_email;
# MAGIC ALTER TABLE sec_customers ALTER COLUMN card_number SET MASK sec_mask_card;
# MAGIC ALTER TABLE sec_customers ALTER COLUMN phone SET MASK sec_mask_phone USING COLUMNS (country);

# COMMAND ----------

# DBTITLE 1,The table now - all 300 rows, masked values
# MAGIC %sql
# MAGIC SELECT customer_id, email, phone, country, card_number
# MAGIC FROM sec_customers
# MAGIC WHERE country IN ('Saudi Arabia', 'Egypt')
# MAGIC ORDER BY country DESC, customer_id
# MAGIC LIMIT 8

# COMMAND ----------

# MAGIC %md
# MAGIC All rows are still there (no row filter on customers), but: e-mails masked, cards `**** **** **** 1233`, phones visible for
# MAGIC **Saudi Arabia** (your region) and `+*** *** ***` for Egypt. Note that the **view** of Part 2 now reads masked values from
# MAGIC the table — masks apply wherever the table is read.
# MAGIC
# MAGIC > 💡 Masks can't change the column **type** (the result must be castable to it) — return `NULL` or a masked string of the
# MAGIC > same type. One mask per column, one row filter per table.
# MAGIC
# MAGIC ## Part 5 · Inspect the protections

# COMMAND ----------

# DBTITLE 1,DESCRIBE TABLE EXTENDED and information_schema
_rows = spark.sql("DESCRIBE TABLE EXTENDED sec_orders").collect()
print("\n".join(f"{r['col_name']}: {r['data_type']}" for r in _rows
                if any(k in (r["col_name"] or "").lower() for k in ("row filter", "mask", "owner", "type"))) or
      "(look for the row filter in the table details in Catalog Explorer)")
for _view in ("row_filters", "column_masks"):
    try:
        display(spark.sql(f"SELECT * FROM `{catalog_name}`.information_schema.{_view} "
                          f"WHERE table_schema = '{schema_name}' AND table_name LIKE 'sec_%'"))
    except Exception as e:
        print(f"information_schema.{_view}:", _first_line(e))

# COMMAND ----------

# MAGIC %md
# MAGIC In **Catalog Explorer** the table's **Overview** marks the filtered table and the masked columns (shield icons), with the
# MAGIC function names.
# MAGIC
# MAGIC ## Part 6 · Limits worth knowing for the exam

# COMMAND ----------

# DBTITLE 1,Time travel and clones on a protected table
_v = spark.sql("DESCRIBE HISTORY sec_orders").agg(F.min("version")).first()[0]
try_sql(f"SELECT count(*) FROM sec_orders VERSION AS OF {_v}")
try_sql("CREATE OR REPLACE TABLE sec_orders_copy SHALLOW CLONE sec_orders")

# COMMAND ----------

# MAGIC %md
# MAGIC ⛔ Both are refused: time travel and (deep or shallow) clones are not supported on tables with row filters or column masks
# MAGIC — otherwise they would be an easy bypass. Other limits: a filter/mask function can't read another protected table, views
# MAGIC can't carry row filters (use a dynamic view), and the protections apply on serverless, SQL warehouses and standard
# MAGIC compute (dedicated clusters delegate the filtering to serverless).
# MAGIC
# MAGIC ## Part 7 · ✅ Checks and clean-up

# COMMAND ----------

# DBTITLE 1,✅ Checks
check("you have exactly one region: Saudi Arabia", my_regions() == ["Saudi Arabia"])
check("sec_v_customers returns the 30 Saudi customers", spark.table("sec_v_customers").count() == 30)
check("the view masks e-mails (no full local part visible)",
      spark.table("sec_v_customers").where("email IS NOT NULL AND email NOT LIKE '_***@%'").count() == 0)
check("G13 can SELECT the view", any({k.lower(): v for k, v in r.asDict().items()}["principal"] == G13
                                     for r in show_grants("VIEW", "sec_v_customers").collect()))
check("the row filter limits sec_orders to your region (197 rows)",
      spark.table("sec_orders").count() == expected_orders(my_regions()) == 197)
_c = spark.table("sec_customers")
check("sec_customers still has all 300 rows", _c.count() == 300)
check("every e-mail is masked", _c.where("email IS NOT NULL AND email NOT LIKE '_***@%'").count() == 0)
check("every card shows only the last 4 digits", _c.where("card_number NOT LIKE '**** **** **** ____'").count() == 0)
check("phones visible only for your region",
      _c.where("country = 'Saudi Arabia' AND phone LIKE '+966%'").count() == 30
      and _c.where("country <> 'Saudi Arabia' AND phone <> '+*** *** ***'").count() == 0)

# COMMAND ----------

# MAGIC %md
# MAGIC **Clean-up.** The next lab protects the same tables **centrally with ABAC policies** — don't mix table-level filters/masks
# MAGIC and policies on the same table, so remove the table-level rules now (`DROP ROW FILTER`, `DROP MASK`). The functions and
# MAGIC the view stay.

# COMMAND ----------

# DBTITLE 1,Remove the row filter and the masks
# MAGIC %sql
# MAGIC ALTER TABLE sec_orders DROP ROW FILTER;
# MAGIC ALTER TABLE sec_customers ALTER COLUMN email DROP MASK;
# MAGIC ALTER TABLE sec_customers ALTER COLUMN card_number DROP MASK;
# MAGIC ALTER TABLE sec_customers ALTER COLUMN phone DROP MASK;

# COMMAND ----------

# DBTITLE 1,Back to normal
print("sec_orders rows:", spark.table("sec_orders").count(), "· first e-mail:",
      spark.table("sec_customers").where("email IS NOT NULL").orderBy("customer_id").first()["email"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ What you practised
# MAGIC * **Dynamic view**: `current_user()` + mapping table filters rows, `is_account_group_member()` masks columns; grant the
# MAGIC   **view**, keep the base table private.
# MAGIC * **Row filter**: `CREATE FUNCTION … RETURNS BOOLEAN` + `ALTER TABLE … SET ROW FILTER f ON (col)` — applies to everyone,
# MAGIC   owner included; exemptions live in the function.
# MAGIC * **Column masks**: `ALTER TABLE … ALTER COLUMN c SET MASK f [USING COLUMNS (…)]`; `DROP ROW FILTER` / `DROP MASK`.
# MAGIC * A **mapping table** makes access a matter of data; time travel and clones are blocked on protected tables.
# MAGIC
# MAGIC ➡️ Next: **13-L3 · ABAC Policies, Lineage and Audit**
