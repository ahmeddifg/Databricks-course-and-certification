# Databricks notebook source
# MAGIC %md
# MAGIC # 🧪 Lab 13-L3 · ABAC Policies, Lineage and Audit (Guided)
# MAGIC **Time:** ~50 min · **Compute:** Serverless notebook (ABAC needs serverless or DBR 16.4+) · **Works on Free Edition** (if
# MAGIC you may create governed tags — the lab tells you)
# MAGIC
# MAGIC In 13-L2 you attached rules table by table. ShopWave has hundreds of tables with e-mails and countries — you need rules
# MAGIC that follow the **data classification** instead: **tag** the sensitive columns once, write **one policy** per rule on the
# MAGIC schema, and every matching table — including tables created tomorrow — is protected. Then you look at the other half of
# MAGIC governance: **lineage** and **audit**.
# MAGIC
# MAGIC | Part | You will… |
# MAGIC |---|---|
# MAGIC | 1 | Create two **governed tags**: `sw_pii` and `sw_geo` |
# MAGIC | 2 | **Tag** the sensitive columns (and see a governed tag refuse a wrong value) |
# MAGIC | 3 | A **column mask policy** on the schema |
# MAGIC | 4 | A **new table** is protected the moment its column is tagged |
# MAGIC | 5 | A **row filter policy** driven by the mapping table |
# MAGIC | 6 | `EXCEPT`, `SHOW POLICIES`, `DESCRIBE POLICY` |
# MAGIC | 7 | **Lineage** — and why derived tables need protection too |
# MAGIC | 8 | **Audit** with system tables |
# MAGIC | 9 | ✅ Checks and clean-up (drop the policies!) |

# COMMAND ----------

# MAGIC %run ../../Includes/_setup

# COMMAND ----------

# MAGIC %run ./_13_prepare

# COMMAND ----------

# DBTITLE 1,Start clean (safe to re-run)
drop_lab13_policies()
for _t in ("sec_orders", "sec_customers", "sec_leads"):
    if spark.catalog.tableExists(_t):
        _drop_security(_t)
spark.sql("DROP TABLE IF EXISTS sec_leads")
spark.sql("DROP TABLE IF EXISTS sec_gold_country_sales")
spark.sql(f"""CREATE OR REPLACE FUNCTION sec_country_filter(p_country STRING) RETURNS BOOLEAN
              RETURN is_account_group_member('shopwave_admins')
                  OR EXISTS (SELECT 1 FROM `{catalog_name}`.`{schema_name}`.sec_region_access a
                             WHERE a.user_email = current_user() AND a.country = p_country)""")
spark.sql("""CREATE OR REPLACE FUNCTION sec_abac_mask(v STRING) RETURNS STRING
             COMMENT 'Section 13 ABAC mask: first character + ****'
             RETURN CASE WHEN v IS NULL THEN NULL ELSE concat(left(v, 1), '****') END""")
revoke_region()
grant_region("Saudi Arabia")
SCHEMA_FQ = f"`{catalog_name}`.`{schema_name}`"
FN = lambda name: f"`{catalog_name}`.`{schema_name}`.{name}"       # policies reference functions by full name


def check(label, condition):
    print(("✅ " if condition else "❌ ") + label)
    return bool(condition)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1 · Governed tags
# MAGIC ABAC policies match **governed tags** — account-level tag keys with a list of **allowed values** and control over who may
# MAGIC assign them (free-text tags can't drive policies). Create them with the cell (it uses the *tag policies* REST API) — or in
# MAGIC the UI: **Catalog** → **Govern** (⚙️ / *Governed tags*) → **Create governed tag** → key + allowed values.
# MAGIC
# MAGIC | Governed tag | Allowed values | Meaning |
# MAGIC |---|---|---|
# MAGIC | `sw_pii` | `email`, `phone`, `card_number` | the column contains personal data of that kind |
# MAGIC | `sw_geo` | `country` | the column holds the country used for regional access |

# COMMAND ----------

# DBTITLE 1,Create the governed tags
TAGS_OK = (create_governed_tag("sw_pii", ["email", "phone", "card_number"], "Section 13: personal data")
           & create_governed_tag("sw_geo", ["country"], "Section 13: regional access column"))
print("\nTAGS_OK =", TAGS_OK)

# COMMAND ----------

# MAGIC %md
# MAGIC > ⛔ **`TAGS_OK = False`?** Creating governed tags needs an account-level permission. Try the UI path above; if it is not
# MAGIC > offered, ask an admin — or read Parts 2–6 (the expected results are written below each cell) and continue at Part 7.
# MAGIC
# MAGIC ## Part 2 · Tag the sensitive columns

# COMMAND ----------

# DBTITLE 1,Assign tags to columns
# MAGIC %sql
# MAGIC ALTER TABLE sec_customers ALTER COLUMN email       SET TAGS ('sw_pii' = 'email');
# MAGIC ALTER TABLE sec_customers ALTER COLUMN phone       SET TAGS ('sw_pii' = 'phone');
# MAGIC ALTER TABLE sec_customers ALTER COLUMN card_number SET TAGS ('sw_pii' = 'card_number');
# MAGIC ALTER TABLE sec_customers ALTER COLUMN country     SET TAGS ('sw_geo' = 'country');
# MAGIC ALTER TABLE sec_orders    ALTER COLUMN country     SET TAGS ('sw_geo' = 'country');

# COMMAND ----------

# DBTITLE 1,The tags as data - and a value the governed tag refuses
display(spark.sql(f"""
    SELECT table_name, column_name, tag_name, tag_value
    FROM `{catalog_name}`.information_schema.column_tags
    WHERE schema_name = '{schema_name}' AND tag_name IN ('sw_pii', 'sw_geo')
    ORDER BY table_name, column_name"""))
try_sql("ALTER TABLE sec_customers ALTER COLUMN loyalty_tier SET TAGS ('sw_pii' = 'loyalty')")

# COMMAND ----------

# MAGIC %md
# MAGIC The last statement is refused: `loyalty` is not an allowed value of the governed tag `sw_pii`. That consistency is what
# MAGIC makes tags reliable enough to drive security. (Assigning tags needs `APPLY TAG` on the object + `ASSIGN` on the governed tag.)
# MAGIC
# MAGIC ## Part 3 · One column-mask policy for the whole schema
# MAGIC Policy = **what** (a mask function) + **where** (schema scope, columns matched by tag) + **for whom** (`TO … EXCEPT …`).

# COMMAND ----------

# DBTITLE 1,CREATE POLICY - column mask
try_sql(f"""
CREATE OR REPLACE POLICY sw_mask_pii
ON SCHEMA {SCHEMA_FQ}
COLUMN MASK {FN('sec_abac_mask')}
TO `account users`
FOR TABLES
MATCH COLUMNS has_tag('sw_pii') AS pii
ON COLUMN pii""")

# COMMAND ----------

# DBTITLE 1,sec_customers through the policy
# MAGIC %sql
# MAGIC SELECT customer_id, email, phone, card_number, country
# MAGIC FROM sec_customers
# MAGIC ORDER BY customer_id
# MAGIC LIMIT 5

# COMMAND ----------

# MAGIC %md
# MAGIC Expected: `e****`, `+****`, `4****` — all three tagged columns are masked by **one** policy; `country` (tagged `sw_geo`, not
# MAGIC `sw_pii`) is untouched. No `ALTER TABLE … SET MASK` anywhere.
# MAGIC
# MAGIC ## Part 4 · New table, protected immediately

# COMMAND ----------

# DBTITLE 1,A new table with an e-mail column
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE sec_leads (lead_id STRING, email STRING, country STRING)
# MAGIC COMMENT 'Section 13: marketing leads - created AFTER the policy';
# MAGIC INSERT INTO sec_leads VALUES
# MAGIC   ('L1', 'nora.saleh@example.com', 'Saudi Arabia'),
# MAGIC   ('L2', 'tom.brown@example.com',  'United Kingdom'),
# MAGIC   ('L3', 'mei.tanaka@example.com', 'Japan');
# MAGIC ALTER TABLE sec_leads ALTER COLUMN email   SET TAGS ('sw_pii' = 'email');
# MAGIC ALTER TABLE sec_leads ALTER COLUMN country SET TAGS ('sw_geo' = 'country');
# MAGIC SELECT * FROM sec_leads ORDER BY lead_id;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected: `n****`, `t****`, `m****`. The data steward only **classified** the column; the policy did the rest. That is the
# MAGIC point of ABAC: security scales with tagging, not with ALTER TABLE statements per table.
# MAGIC
# MAGIC ## Part 5 · A row-filter policy driven by the mapping table
# MAGIC Re-use the 13-L2 filter function (`sec_country_filter`) — but attach it **by policy** to every table that has a column
# MAGIC tagged `sw_geo = country`.

# COMMAND ----------

# DBTITLE 1,CREATE POLICY - row filter
try_sql(f"""
CREATE OR REPLACE POLICY sw_geo_rows
ON SCHEMA {SCHEMA_FQ}
ROW FILTER {FN('sec_country_filter')}
TO `account users`
FOR TABLES
MATCH COLUMNS has_tag_value('sw_geo', 'country') AS geo
USING COLUMNS (geo)""")
for _t in ("sec_orders", "sec_customers", "sec_leads"):
    print(f"{_t:<14} rows visible: {spark.table(_t).count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC Expected with your region **Saudi Arabia**: `sec_orders` **197** · `sec_customers` **30** · `sec_leads` **1** — three
# MAGIC tables, one policy, the column found by its tag in each table (`USING COLUMNS (geo)` passes it to the function).
# MAGIC
# MAGIC ## Part 6 · Exemptions and inspection
# MAGIC The PII team must see real values. Exempt them with `EXCEPT` — here we exempt **you**, so you can see the effect:

# COMMAND ----------

# DBTITLE 1,CREATE OR REPLACE the mask policy with EXCEPT
try_sql(f"""
CREATE OR REPLACE POLICY sw_mask_pii
ON SCHEMA {SCHEMA_FQ}
COLUMN MASK {FN('sec_abac_mask')}
TO `account users`
EXCEPT `{me()}`
FOR TABLES
MATCH COLUMNS has_tag('sw_pii') AS pii
ON COLUMN pii""")
display(spark.sql("SELECT customer_id, email, phone FROM sec_customers ORDER BY customer_id LIMIT 3"))

# COMMAND ----------

# MAGIC %md
# MAGIC Real e-mails again (but still only 30 rows: the **row** policy doesn't exempt you). In production you'd write
# MAGIC ``EXCEPT `shopwave_pii_readers` `` — a group, never a person.

# COMMAND ----------

# DBTITLE 1,SHOW POLICIES, DESCRIBE POLICY, information_schema
try:
    display(show_policies())
except Exception as e:
    print("SHOW POLICIES:", _first_line(e))
try_sql(f"DESCRIBE POLICY sw_geo_rows ON SCHEMA {SCHEMA_FQ}", show_result=True)
try:
    display(spark.sql(f"SELECT * FROM `{catalog_name}`.information_schema.abac_policy_definitions"))
except Exception as e:
    print("information_schema.abac_policy_definitions:", _first_line(e))

# COMMAND ----------

# MAGIC %md
# MAGIC 🖱️ Also look in the UI: **Catalog** → your schema → **Policies** tab (policies can be created there too, with a form), and
# MAGIC open `sec_customers` → the masked columns show the policy that applies.
# MAGIC
# MAGIC ## Part 7 · Lineage — and protecting derived data
# MAGIC Build a gold table from the protected tables:

# COMMAND ----------

# DBTITLE 1,A derived gold table
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE sec_gold_country_sales
# MAGIC COMMENT 'Section 13: revenue per country - built from sec_orders'
# MAGIC AS SELECT country, count(*) AS orders, round(sum(total), 2) AS revenue
# MAGIC    FROM sec_orders
# MAGIC    GROUP BY country;
# MAGIC SELECT * FROM sec_gold_country_sales;

# COMMAND ----------

# MAGIC %md
# MAGIC Only **Saudi Arabia** is in the gold table: the CTAS ran **as you**, through the row policy. The gold table is a new
# MAGIC object: it has **no** tags and **no** policy — whoever can read it sees what you saw. Protect derived tables too (tags,
# MAGIC grants), and run production builds as a **service principal** that is exempt from the filters.
# MAGIC
# MAGIC 🖱️ **Lineage:** Catalog Explorer → `sec_gold_country_sales` → **Lineage** tab → **See lineage graph**: upstream
# MAGIC `sec_orders`, the notebook that wrote it; click a column for **column-level lineage**. Then the same as data (system tables
# MAGIC can lag a few minutes and need access to the `system` catalog):

# COMMAND ----------

# DBTITLE 1,system.access.table_lineage
try:
    display(spark.sql(f"""
        SELECT event_time, source_table_full_name, target_table_full_name, entity_type
        FROM system.access.table_lineage
        WHERE target_table_full_name = '{catalog_name}.{schema_name}.sec_gold_country_sales'
        ORDER BY event_time DESC LIMIT 10"""))
except Exception as e:
    print("system.access.table_lineage not available here:", _first_line(e))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 8 · Audit — who changed access?

# COMMAND ----------

# DBTITLE 1,system.access.audit - your Unity Catalog permission changes today
try:
    display(spark.sql(f"""
        SELECT event_time, action_name, request_params
        FROM system.access.audit
        WHERE service_name = 'unityCatalog'
          AND user_identity.email = '{me()}'
          AND action_name IN ('updatePermissions', 'createPolicy', 'updatePolicy', 'deletePolicy')
          AND event_date >= current_date() - INTERVAL 1 DAY
        ORDER BY event_time DESC LIMIT 20"""))
except Exception as e:
    print("system.access.audit not available here:", _first_line(e))

# COMMAND ----------

# MAGIC %md
# MAGIC Every GRANT/REVOKE (13-L1, UI or SQL) is an `updatePermissions` event with the securable and the changes in
# MAGIC `request_params` — the trail an auditor asks for.
# MAGIC
# MAGIC ## Part 9 · ✅ Checks and clean-up

# COMMAND ----------

# DBTITLE 1,✅ Checks
_pols = _policy_names()
if not TAGS_OK:
    print("⚠️ governed tags not available - ABAC checks skipped")
else:
    check("policies sw_mask_pii and sw_geo_rows exist on the schema", {"sw_mask_pii", "sw_geo_rows"} <= set(_pols))
    check("row policy: sec_orders shows only Saudi Arabia (197 rows)",
          spark.table("sec_orders").count() == 197
          and [r[0] for r in spark.sql("SELECT DISTINCT country FROM sec_orders").collect()] == ["Saudi Arabia"])
    check("row policy also covers sec_customers (30) and the new sec_leads (1)",
          spark.table("sec_customers").count() == 30 and spark.table("sec_leads").count() == 1)
    check("you are exempt from the mask policy (real e-mails visible)",
          spark.table("sec_customers").where("email LIKE '%****'").count() == 0)
    check("the gold table only contains what you were allowed to see",
          [r[0] for r in spark.table("sec_gold_country_sales").select("country").collect()] == ["Saudi Arabia"])
check("sec_leads e-mail column is tagged sw_pii = email", spark.sql(f"""
      SELECT count(*) FROM `{catalog_name}`.information_schema.column_tags
      WHERE schema_name = '{schema_name}' AND table_name = 'sec_leads' AND column_name = 'email'
        AND tag_name = 'sw_pii' AND tag_value = 'email'""").first()[0] == 1)

# COMMAND ----------

# MAGIC %md
# MAGIC **Clean-up — important.** The policies live on your **course schema**: leave them and other sections that read tagged
# MAGIC columns would suddenly see filtered/masked data. Drop them (tags and the sec_ objects can stay):

# COMMAND ----------

# DBTITLE 1,Drop the lab policies
drop_lab13_policies()
print("policies left:", _policy_names() or "none")
print("sec_orders rows:", spark.table("sec_orders").count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ What you practised
# MAGIC * **Governed tags** (allowed values, controlled assignment) and column tags (`SET TAGS`, `information_schema.column_tags`).
# MAGIC * **ABAC policies**: `CREATE POLICY … ON SCHEMA … COLUMN MASK f … TO … [EXCEPT …] FOR TABLES MATCH COLUMNS has_tag(…) AS c
# MAGIC   ON COLUMN c` and `… ROW FILTER f … MATCH COLUMNS has_tag_value(…) AS g USING COLUMNS (g)`; tables created later are covered.
# MAGIC * `SHOW POLICIES`, `DESCRIBE POLICY`, `DROP POLICY`, the **Policies** tab.
# MAGIC * **Lineage** (UI + `system.access.table_lineage`) and **audit** (`system.access.audit`); derived tables don't inherit
# MAGIC   protections.
# MAGIC
# MAGIC ➡️ Next: **13-L4 · Challenge Lab**
