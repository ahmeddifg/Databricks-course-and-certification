# Databricks notebook source
# MAGIC %md
# MAGIC # 🧪 Lab 13-L1 · Privileges, Ownership and Inheritance (Guided)
# MAGIC **Time:** ~50 min · **Compute:** Serverless notebook · **Works on Free Edition**
# MAGIC
# MAGIC ShopWave's analysts need access to the order data — **only** what they need. You'll grant and revoke privileges in SQL and
# MAGIC in the UI, read them back three ways (`SHOW GRANTS`, `information_schema`, Catalog Explorer), see **inheritance** (and its
# MAGIC trap), transfer **ownership**, and look at managed vs external tables from a governance angle.
# MAGIC
# MAGIC | Part | You will… |
# MAGIC |---|---|
# MAGIC | 1 | Find out who you are and who owns what |
# MAGIC | 2 | Grant read access to **one** table — the three privileges it takes |
# MAGIC | 3 | **Inheritance**: grant on the schema, a new table, and the REVOKE trap |
# MAGIC | 4 | Least privilege for four requests (create, volume, function, modify) |
# MAGIC | 5 | `ALL PRIVILEGES`, `MANAGE` and **ownership** |
# MAGIC | 6 | 🖱️ The same in **Catalog Explorer** |
# MAGIC | 7 | **DENY**? Not in Unity Catalog |
# MAGIC | 8 | Managed vs external, DROP and UNDROP |
# MAGIC | 9 | ✅ Checks and clean-up |
# MAGIC
# MAGIC > 👥 **The group.** The labs grant to `G13` — your `COURSE_GROUP` if you set it before the `%run` (e.g. a group
# MAGIC > `shopwave_analysts` you created in *Settings → Identity and access → Groups*), otherwise the built-in group
# MAGIC > **`account users`**. On Free Edition you are the only user, so you can't *log in as* the analyst — you verify access by
# MAGIC > reading the grants, exactly as an administrator does.

# COMMAND ----------

# MAGIC %run ../../Includes/_setup

# COMMAND ----------

# MAGIC %run ./_13_prepare

# COMMAND ----------

# DBTITLE 1,Start clean (safe to re-run)
for _stmt in ("DROP TABLE IF EXISTS sec_new_products", "DROP FUNCTION IF EXISTS sec_fn_tier"):
    spark.sql(_stmt)
for _priv in ("SELECT", "MODIFY", "CREATE TABLE", "USE SCHEMA", "EXECUTE", "READ VOLUME"):
    for _obj in (f"SCHEMA {schema_name}", "TABLE sec_orders", "TABLE sec_customers"):
        try:
            spark.sql(f"REVOKE {_priv} ON {_obj} FROM `{G13}`")
        except Exception:
            pass
print(f"me: {me()} · group used for grants: G13 = `{G13}` · schema: {catalog_name}.{schema_name}")


def check(label, condition):
    print(("✅ " if condition else "❌ ") + label)
    return bool(condition)


def grants_of(kind: str, name: str, principal: str = G13) -> set:
    """Privileges of `principal` on an object (direct + inherited) as a set like {'SELECT', 'USE SCHEMA'}."""
    rows = [{k.lower(): v for k, v in r.asDict().items()} for r in show_grants(kind, name).collect()]
    return {r["actiontype"].replace("_", " ").upper() for r in rows if r["principal"] == principal}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1 · Who am I, who owns what?

# COMMAND ----------

# DBTITLE 1,Identity functions
# MAGIC %sql
# MAGIC SELECT current_user()                              AS me,
# MAGIC        is_account_group_member('account users')    AS in_account_users,   -- everybody is
# MAGIC        is_account_group_member('shopwave_admins')  AS in_shopwave_admins  -- false unless you created + joined it

# COMMAND ----------

# DBTITLE 1,Owners and types of the course objects
for _kind, _name in (("CATALOG", catalog_name), ("SCHEMA", f"{catalog_name}.{schema_name}")):
    try:
        _info = {r[0]: r[1] for r in spark.sql(f"DESCRIBE {_kind} EXTENDED {_name}").collect()}
        print(f"{_kind:<7} {_name:<28} owner: {_info.get('Owner')}")
    except Exception as e:
        print(f"{_kind:<7} {_name:<28} ⛔ {_first_line(e)}")
_t = {r["col_name"]: r["data_type"] for r in spark.sql("DESCRIBE TABLE EXTENDED sec_orders").collect()}
print(f"TABLE   sec_orders                   owner: {_t.get('Owner')} · type: {_t.get('Type')}")

# COMMAND ----------

# MAGIC %md
# MAGIC You created the schema `shopwave` (in `_setup`), so you **own** it and everything you created in it. The catalog
# MAGIC (`workspace` on Free Edition) was created by Databricks — you may or may not be allowed to grant on it.
# MAGIC
# MAGIC ## Part 2 · Read access to ONE table
# MAGIC An analyst must query `sec_orders`. Reading a table takes **three** privileges at three levels: **USE CATALOG** on the
# MAGIC catalog, **USE SCHEMA** on the schema, **SELECT** on the table.

# COMMAND ----------

# DBTITLE 1,GRANT the three privileges
try_sql(f"GRANT USE CATALOG ON CATALOG `{catalog_name}` TO `{G13}`")      # may be refused if you don't manage the catalog
try_sql(f"GRANT USE SCHEMA ON SCHEMA `{catalog_name}`.`{schema_name}` TO `{G13}`")
try_sql(f"GRANT SELECT ON TABLE sec_orders TO `{G13}`")

# COMMAND ----------

# MAGIC %md
# MAGIC > ⛔ If `USE CATALOG` was refused: you are not the owner of the catalog and don't have `MANAGE` on it. On Free Edition all
# MAGIC > workspace users usually have `USE CATALOG` on `workspace` already — an admin did that part for you. In a company, the
# MAGIC > catalog owner (often a platform team group) grants it.

# COMMAND ----------

# DBTITLE 1,SHOW GRANTS on the table - direct AND inherited privileges
# MAGIC %sql
# MAGIC SHOW GRANTS ON TABLE sec_orders

# COMMAND ----------

# DBTITLE 1,One principal, one object - and the same as data
display(show_grants("SCHEMA", f"{catalog_name}.{schema_name}", G13))
display(table_privileges("sec_"))

# COMMAND ----------

# MAGIC %md
# MAGIC `SHOW GRANTS` columns: `principal`, `actionType`, `objectType`, `objectKey` — **objectType** tells you *where* the grant
# MAGIC was made (on the table itself, or inherited from the schema/catalog). `information_schema.table_privileges` has the same
# MAGIC information as a queryable view, with an `inherited_from` column.
# MAGIC
# MAGIC ## Part 3 · Inheritance — and the REVOKE trap
# MAGIC Granting `SELECT` table by table doesn't scale. Grant it **once on the schema**: every table and view of the schema —
# MAGIC including tables created **later** — inherits it.

# COMMAND ----------

# DBTITLE 1,GRANT SELECT on the schema, then create a NEW table
try_sql(f"GRANT SELECT ON SCHEMA `{catalog_name}`.`{schema_name}` TO `{G13}`")
spark.sql(f"""CREATE OR REPLACE TABLE sec_new_products COMMENT 'Section 13: created AFTER the schema grant'
              AS SELECT product_id, title, category, price
                 FROM read_files('{dataset_path}/products-csv', format => 'csv', header => true, sep => ';')""")
display(spark.sql(f"""
    SELECT table_name, privilege_type, inherited_from
    FROM `{catalog_name}`.information_schema.table_privileges
    WHERE table_schema = '{schema_name}' AND grantee = '{G13}' AND table_name IN ('sec_orders', 'sec_new_products')
    ORDER BY table_name, inherited_from"""))

# COMMAND ----------

# MAGIC %md
# MAGIC `sec_new_products` didn't exist when you granted — it still shows `SELECT` **inherited from the schema**. `sec_orders` now
# MAGIC has `SELECT` twice: once direct (Part 2) and once inherited.
# MAGIC
# MAGIC **The trap:** an auditor asks you to remove the analysts' access to `sec_orders`. You revoke on the table…

# COMMAND ----------

# DBTITLE 1,REVOKE on the table - does the analyst lose access?
try_sql(f"REVOKE SELECT ON TABLE sec_orders FROM `{G13}`")
print("privileges of G13 on sec_orders now:", grants_of("TABLE", "sec_orders"))

# COMMAND ----------

# MAGIC %md
# MAGIC `SELECT` is **still there** — inherited from the schema. A `REVOKE` only removes a grant made **on that object**. To remove
# MAGIC it you revoke on the schema (and then grant the other tables individually), or you move the sensitive table to a schema
# MAGIC the group can't read. Revoke the schema-level `SELECT` now:

# COMMAND ----------

# DBTITLE 1,REVOKE on the schema
try_sql(f"REVOKE SELECT ON SCHEMA `{catalog_name}`.`{schema_name}` FROM `{G13}`")
print("privileges of G13 on sec_orders now:", grants_of("TABLE", "sec_orders") or "none (except USE ... on parents)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 4 · Least privilege — four requests
# MAGIC For each request, the cell grants the **minimum**. Read the statement before you run it: could anything smaller work?
# MAGIC
# MAGIC | # | Request | Minimum (besides USE CATALOG / USE SCHEMA) |
# MAGIC |---|---|---|
# MAGIC | a | “The analysts must create their own tables in `shopwave`.” | `CREATE TABLE` on the schema |
# MAGIC | b | “They must read the raw files in the `raw` volume.” | `READ VOLUME` on the volume |
# MAGIC | c | “They must call our function `sec_fn_tier`.” | `EXECUTE` on the function |
# MAGIC | d | “The data engineers' group must correct rows in `sec_orders`.” | `MODIFY` on the table (+ `SELECT` to read what they change) |

# COMMAND ----------

# DBTITLE 1,Grant the four requests
spark.sql("""CREATE OR REPLACE FUNCTION sec_fn_tier(total DOUBLE) RETURNS STRING
             COMMENT 'Section 13: order size tier'
             RETURN CASE WHEN total >= 1000 THEN 'large' WHEN total >= 300 THEN 'medium' ELSE 'small' END""")
try_sql(f"GRANT CREATE TABLE ON SCHEMA `{catalog_name}`.`{schema_name}` TO `{G13}`")                 # a
try_sql(f"GRANT READ VOLUME ON VOLUME `{catalog_name}`.`{schema_name}`.raw TO `{G13}`")              # b
try_sql(f"GRANT EXECUTE ON FUNCTION `{catalog_name}`.`{schema_name}`.sec_fn_tier TO `{G13}`")       # c
try_sql(f"GRANT SELECT, MODIFY ON TABLE sec_orders TO `{G13}`")                                    # d
display(show_grants("SCHEMA", f"{catalog_name}.{schema_name}", G13))

# COMMAND ----------

# MAGIC %md
# MAGIC > 🧠 `CREATE TABLE` on a schema also allows creating **views**. Writing files into a volume would need `WRITE VOLUME`;
# MAGIC > reading a table never needs anything on the volume. And none of these grants gives `SELECT` on other tables.
# MAGIC
# MAGIC ## Part 5 · ALL PRIVILEGES, MANAGE and ownership

# COMMAND ----------

# DBTITLE 1,ALL PRIVILEGES and MANAGE
try_sql(f"GRANT ALL PRIVILEGES ON TABLE sec_new_products TO `{G13}`")
try_sql(f"GRANT MANAGE ON TABLE sec_new_products TO `{G13}`")
display(show_grants("TABLE", "sec_new_products", G13))

# COMMAND ----------

# MAGIC %md
# MAGIC `SHOW GRANTS` lists **`ALL PRIVILEGES`**, not SELECT/MODIFY/… one by one — it stands for every applicable privilege, also
# MAGIC future ones. **`MANAGE`** is separate (ALL PRIVILEGES doesn't include it): it lets the group manage grants, transfer
# MAGIC ownership and drop the table — without implying data access.
# MAGIC
# MAGIC Now **ownership**: the owner has every privilege and can grant. Make the group the owner (best practice for production
# MAGIC objects), look, and take it back.

# COMMAND ----------

# DBTITLE 1,Transfer ownership and back
try_sql(f"ALTER TABLE sec_new_products OWNER TO `{G13}`")
print("owner:", {r["col_name"]: r["data_type"] for r in spark.sql("DESCRIBE TABLE EXTENDED sec_new_products").collect()}.get("Owner"))
try_sql(f"ALTER TABLE sec_new_products OWNER TO `{me()}`")      # you can: you own the parent schema
print("owner:", {r["col_name"]: r["data_type"] for r in spark.sql("DESCRIBE TABLE EXTENDED sec_new_products").collect()}.get("Owner"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 6 · 🖱️ The same in Catalog Explorer
# MAGIC 1. Left sidebar → **Catalog** → your catalog → `shopwave` → **`sec_customers`** → **Permissions** tab.
# MAGIC 2. **Grant** → *Principals*: `G13` (the group printed at the top) → *Privileges*: **SELECT** (look at the *privilege
# MAGIC    presets* such as *Data Reader*) → **Confirm**.
# MAGIC 3. Open the **schema** `shopwave` → **Permissions**: the grants of Part 4 are listed there. Select the `CREATE TABLE` row →
# MAGIC    **Revoke** → confirm.
# MAGIC 4. Run the check below. UI and SQL write the **same** grants (and the same audit events).

# COMMAND ----------

# DBTITLE 1,✅ Check Part 6
check("SELECT on sec_customers for G13 (granted in the UI)", "SELECT" in grants_of("TABLE", "sec_customers"))
check("CREATE TABLE on the schema revoked in the UI",
      "CREATE TABLE" not in grants_of("SCHEMA", f"{catalog_name}.{schema_name}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 7 · DENY? Not in Unity Catalog
# MAGIC The exam guide mentions **GRANT, REVOKE and DENY**. Try it:

# COMMAND ----------

# DBTITLE 1,DENY on a Unity Catalog table
try_sql(f"DENY SELECT ON TABLE sec_orders TO `{G13}`")

# COMMAND ----------

# MAGIC %md
# MAGIC ⛔ `DENY` exists only for **legacy hive_metastore table ACLs** (where it beats any grant, and is undone with `REVOKE`).
# MAGIC Unity Catalog is **allow-only**: no access until granted; you remove access with **REVOKE** where it was granted. The only
# MAGIC “deny” in UC is the **ABAC DENY policy** (Beta), which can deny just `MANAGE ACCESS CONTROL` on tagged objects.
# MAGIC
# MAGIC ```sql
# MAGIC -- 🕰️ legacy (hive_metastore, table ACLs enabled) — recognise it
# MAGIC GRANT USAGE, SELECT ON DATABASE hive_metastore.hr_db TO `hr_team`;
# MAGIC DENY  SELECT ON TABLE hive_metastore.hr_db.salaries TO `interns`;
# MAGIC GRANT SELECT ON ANY FILE TO `etl_admins`;
# MAGIC ```
# MAGIC
# MAGIC ## Part 8 · Managed vs external, DROP and UNDROP

# COMMAND ----------

# DBTITLE 1,What kind of table is sec_orders?
_d = {r["col_name"]: r["data_type"] for r in spark.sql("DESCRIBE TABLE EXTENDED sec_orders").collect()}
print("Type    :", _d.get("Type"))
print("Location:", _d.get("Location"))
try:
    display(spark.sql("SHOW EXTERNAL LOCATIONS"))
except Exception as e:
    print("external locations:", _first_line(e))

# COMMAND ----------

# MAGIC %md
# MAGIC `MANAGED` — the files live in the managed storage Unity Catalog chose (a path you never use directly). External tables need
# MAGIC an **external location** (storage credential + path) and `CREATE EXTERNAL TABLE` on it; on Free Edition there is usually
# MAGIC none, so external tables are presentation-only (13-P3). Drop a managed table and get it back:

# COMMAND ----------

# DBTITLE 1,DROP and UNDROP a managed table
spark.sql("DROP TABLE sec_new_products")
display(spark.sql("SHOW TABLES DROPPED"))
try_sql("UNDROP TABLE sec_new_products")
print("back:", spark.catalog.tableExists("sec_new_products"), "· rows:",
      spark.table("sec_new_products").count() if spark.catalog.tableExists("sec_new_products") else None)

# COMMAND ----------

# MAGIC %md
# MAGIC > 🔗 **Have an external location?** (paid workspace) — set `EXT = 'abfss://…/shopwave/sec_ext'` (a path inside a location
# MAGIC > where you have `CREATE EXTERNAL TABLE`) and try: `CREATE TABLE sec_orders_ext LOCATION '{EXT}' AS SELECT * FROM
# MAGIC > sec_orders` → `DESCRIBE TABLE EXTENDED` (Type EXTERNAL) → `ALTER TABLE sec_orders_ext SET MANAGED` (Type MANAGED) →
# MAGIC > `ALTER TABLE sec_orders_ext UNSET MANAGED` (back to EXTERNAL, within 14 days) → `DROP TABLE sec_orders_ext` (the files
# MAGIC > stay at `EXT`).
# MAGIC
# MAGIC ## Part 9 · ✅ Checks and clean-up

# COMMAND ----------

# DBTITLE 1,✅ Checks
_sch = grants_of("SCHEMA", f"{catalog_name}.{schema_name}")
check("G13 has USE SCHEMA on the course schema", "USE SCHEMA" in _sch)
check("the schema-level SELECT was revoked (no inherited SELECT left)", "SELECT" not in _sch)
check("G13 can SELECT and MODIFY sec_orders (direct grants, Part 4d)",
      {"SELECT", "MODIFY"} <= grants_of("TABLE", "sec_orders"))
check("READ VOLUME on the raw volume", "READ VOLUME" in grants_of("VOLUME", f"{catalog_name}.{schema_name}.raw"))
check("EXECUTE on sec_fn_tier", "EXECUTE" in grants_of("FUNCTION", f"{catalog_name}.{schema_name}.sec_fn_tier"))
check("ALL PRIVILEGES on sec_new_products", "ALL PRIVILEGES" in grants_of("TABLE", "sec_new_products"))
check("sec_new_products is back (UNDROP) and owned by you",
      spark.catalog.tableExists("sec_new_products")
      and {r["col_name"]: r["data_type"] for r in spark.sql("DESCRIBE TABLE EXTENDED sec_new_products").collect()}.get("Owner") == me())
check("SELECT on sec_customers (UI grant, Part 6)", "SELECT" in grants_of("TABLE", "sec_customers"))

# COMMAND ----------

# MAGIC %md
# MAGIC Clean-up: revoke what this lab granted (the next labs start from a clean state). Look at the statements — `REVOKE` mirrors
# MAGIC `GRANT` with `FROM` instead of `TO`.

# COMMAND ----------

# DBTITLE 1,Clean-up - revoke the lab's grants
for _stmt in (f"REVOKE SELECT, MODIFY ON TABLE sec_orders FROM `{G13}`",
              f"REVOKE SELECT ON TABLE sec_customers FROM `{G13}`",
              f"REVOKE ALL PRIVILEGES ON TABLE sec_new_products FROM `{G13}`",
              f"REVOKE MANAGE ON TABLE sec_new_products FROM `{G13}`",
              f"REVOKE READ VOLUME ON VOLUME `{catalog_name}`.`{schema_name}`.raw FROM `{G13}`",
              f"REVOKE EXECUTE ON FUNCTION `{catalog_name}`.`{schema_name}`.sec_fn_tier FROM `{G13}`",
              f"REVOKE CREATE TABLE ON SCHEMA `{catalog_name}`.`{schema_name}` FROM `{G13}`"):
    try_sql(_stmt)
print("\nremaining grants of G13 on the schema:", grants_of("SCHEMA", f"{catalog_name}.{schema_name}") or "none")

# COMMAND ----------

# MAGIC %md
# MAGIC `USE SCHEMA` stays — the next labs grant `SELECT` on **views** to the same group, and a view is useless without `USE
# MAGIC SCHEMA`.
# MAGIC
# MAGIC ## ✅ What you practised
# MAGIC * Reading a table = **USE CATALOG + USE SCHEMA + SELECT**; least privilege for create (`CREATE TABLE`), files (`READ
# MAGIC   VOLUME`), functions (`EXECUTE`), changes (`MODIFY`).
# MAGIC * `GRANT … TO`, `REVOKE … FROM`, `SHOW GRANTS [principal] ON …` and `information_schema.table_privileges`
# MAGIC   (`inherited_from`) — and the **Permissions** tab in Catalog Explorer.
# MAGIC * **Inheritance** to current and future objects — and why a table-level REVOKE can't remove a schema-level grant.
# MAGIC * `ALL PRIVILEGES` vs `MANAGE` vs **ownership** (`ALTER … OWNER TO`).
# MAGIC * **DENY** is legacy hive_metastore only. Managed tables: DROP removes data, **UNDROP** brings it back.
# MAGIC
# MAGIC ➡️ Next: **13-L2 · Row Filters, Column Masks and Dynamic Views**
