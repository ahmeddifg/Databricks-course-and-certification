# Databricks notebook source
# MAGIC %md
# MAGIC # 🔧 _13_prepare — Section 13 tables & governance helpers
# MAGIC Included by every Section 13 lab with `%run ./_13_prepare` (after `%run ../../Includes/_setup`). Idempotent: it only builds
# MAGIC what is missing.
# MAGIC
# MAGIC **The story.** ShopWave's customer and order data contain **PII** (e-mail, phone, card number) and are used by **regional
# MAGIC managers** who may only see **their** countries. Section 13 secures it with Unity Catalog: privileges, row filters, column
# MAGIC masks, dynamic views and ABAC policies.
# MAGIC
# MAGIC | Object | Rows | Content |
# MAGIC |---|---|---|
# MAGIC | `sec_customers` | 300 | `customer_id`, `first_name`, `last_name`, **`email`**, **`phone`**, `country`, `city`, `loyalty_tier`, **`card_number`** (fake test numbers) |
# MAGIC | `sec_orders` | 1,980 | `order_id`, `order_date`, `customer_id`, **`country`**, `total` (valid orders Jan–Jun 2026) |
# MAGIC | `sec_region_access` | 4 + yours | the **mapping table**: `user_email` → `country` a user may see (the managers below + rows you add for yourself) |
# MAGIC | `ch13_employees` · `ch13_dept_access` | 40 · 2 | the 13-L4 challenge data (HR) |
# MAGIC
# MAGIC **Who is “the group”?** Grants need a principal. The labs use **`G13`** = the group in `COURSE_GROUP` if you set it before
# MAGIC the `%run`, else the built-in group **`account users`** (every user of the account — on Free Edition that's only you).
# MAGIC Create your own group (*Settings → Identity and access → Groups*) and set `COURSE_GROUP = "shopwave_analysts"` to make the
# MAGIC labs feel like a real team.
# MAGIC
# MAGIC **Helpers:** `me()`, `sec_table(name)`, `q(stmt)` (run SQL, return a DataFrame), `try_sql(stmt)` (run SQL and print the
# MAGIC first line of the error instead of failing), `show_grants(kind, name)`, `table_privileges(prefix)`, `my_regions()`,
# MAGIC `grant_region(country)`, `revoke_region(country)`, governed tags: `create_governed_tag(key, values)`, `governed_tag(key)`,
# MAGIC policies: `show_policies()`, `drop_lab13_policies()` · `reset_lab13(confirm="YES")`.

# COMMAND ----------

# DBTITLE 1,Section 13 configuration and helpers
import json
import hashlib
from pyspark.sql import functions as F

G13 = globals().get("COURSE_GROUP") or "account users"     # the principal the labs grant to


def me() -> str:
    return spark.sql("SELECT current_user()").first()[0]


def sec_table(name: str) -> str:
    """Fully qualified (three-level) name of a Section 13 object."""
    return f"{catalog_name}.{schema_name}.{name}"


def q(stmt: str):
    """Run one SQL statement and return the DataFrame (display(q(...)) to look at it)."""
    return spark.sql(stmt)


def _first_line(e) -> str:
    return (str(e).strip().splitlines() or [repr(e)])[0][:300]


def try_sql(stmt: str, show_result: bool = False) -> bool:
    """Run SQL; on error print the first line of the message (used to SEE what is not allowed). Returns True if it ran."""
    try:
        df = spark.sql(stmt)
        if show_result:
            display(df)
        print("✅ ran:", " ".join(stmt.split())[:110])
        return True
    except Exception as e:
        print("⛔ error:", _first_line(e))
        return False


def show_grants(kind: str, name: str, principal: str = None):
    """SHOW GRANTS [principal] ON <kind> <name> - all privileges that affect the object (direct AND inherited)."""
    who = f"`{principal}` " if principal else ""
    return spark.sql(f"SHOW GRANTS {who}ON {kind} {name}")


def table_privileges(prefix: str = "sec_"):
    """information_schema.table_privileges of this schema's tables starting with `prefix` (incl. inherited_from)."""
    return spark.sql(f"""
        SELECT grantee, table_name, privilege_type, inherited_from
        FROM `{catalog_name}`.information_schema.table_privileges
        WHERE table_schema = '{schema_name}' AND table_name LIKE '{prefix}%'
        ORDER BY table_name, grantee, privilege_type""")

# COMMAND ----------

# DBTITLE 1,Section 13 tables (built on the first run only)
_REGION_SEED13 = [("manager.sa@shopwave.example", "Saudi Arabia"), ("manager.eg@shopwave.example", "Egypt"),
                  ("manager.eu@shopwave.example", "France"), ("manager.eu@shopwave.example", "Germany")]
_DIAL13 = {"Saudi Arabia": "+966", "United Arab Emirates": "+971", "Egypt": "+20", "United States": "+1",
           "United Kingdom": "+44", "France": "+33", "Germany": "+49", "India": "+91", "Japan": "+81", "Brazil": "+55"}


def _fake_digits(seed: str, n: int) -> str:
    h = hashlib.sha256(seed.encode()).hexdigest()
    return "".join(str(int(c, 16) % 10) for c in h)[:n]


def _build_sec_customers():
    rows = []
    raw = (spark.read.json(f"{dataset_path}/customers-json")
                .select("customer_id", "email",
                        F.get_json_object("profile", "$.first_name").alias("first_name"),
                        F.get_json_object("profile", "$.last_name").alias("last_name"),
                        F.get_json_object("profile", "$.address.city").alias("city"),
                        F.get_json_object("profile", "$.address.country").alias("country"))
                .orderBy("customer_id").collect())
    for r in raw:
        cid = r["customer_id"]
        d = _fake_digits(cid, 20)
        phone = f"{_DIAL13.get(r['country'], '+0')} 5{d[0:2]} {d[2:5]} {d[5:8]}"
        card = f"4000 00{d[8:10]} {d[10:14]} {d[14:18]}"                # 4000 00xx … = test-card range, not real
        tier = ("Gold", "Silver", "Bronze", "Bronze")[int(cid[1:]) % 4]
        rows.append((cid, r["first_name"], r["last_name"], r["email"], phone, r["country"], r["city"], tier, card))
    schema = ("customer_id STRING, first_name STRING, last_name STRING, email STRING, phone STRING, country STRING, "
              "city STRING, loyalty_tier STRING, card_number STRING")
    spark.createDataFrame(rows, schema).write.mode("overwrite").saveAsTable("sec_customers")
    spark.sql("COMMENT ON TABLE sec_customers IS 'Section 13: customers with PII (email, phone, card_number)'")


def _build_sec_orders():
    cust = spark.table("sec_customers").select("customer_id", "country")
    (spark.read.schema(ORDER_SCHEMA).parquet(f"{dataset_path}/orders-parquet")
          .dropDuplicates(["order_id"]).where("quantity > 0")
          .join(cust, "customer_id", "left")
          .select("order_id", F.to_date(F.timestamp_seconds("order_timestamp")).alias("order_date"), "customer_id",
                  F.coalesce("country", F.lit("Unknown")).alias("country"), "total")
          .write.mode("overwrite").saveAsTable("sec_orders"))
    spark.sql("COMMENT ON TABLE sec_orders IS 'Section 13: valid orders Jan-Jun 2026 with the customer country'")


def _build_region_access():
    spark.createDataFrame(_REGION_SEED13, "user_email STRING, country STRING").write.mode("overwrite") \
         .saveAsTable("sec_region_access")
    spark.sql("COMMENT ON TABLE sec_region_access IS 'Section 13 mapping table: which user may see which country'")


_DEPTS13 = ["Engineering", "Finance", "HR", "Sales"]
_COUNTRY13 = ["Saudi Arabia", "Egypt", "France", "Germany", "United States"]


def _build_ch13():
    rows = []
    for i in range(1, 41):
        d = _fake_digits(f"E{i}", 6)
        first, last = _FIRST[i % len(_FIRST)], _LAST[(i * 7) % len(_LAST)]
        dept = _DEPTS13[i % 4]
        salary = 4000 + int(d[:4]) % 9000 + (3000 if dept == "Engineering" else 0)
        rows.append((f"E{i:03d}", f"{first} {last}", f"{first}.{last}.{i}@shopwave.example".lower(), dept,
                     _COUNTRY13[i % 5], float(salary)))
    spark.createDataFrame(rows, "employee_id STRING, full_name STRING, email STRING, department STRING, "
                                "country STRING, salary DOUBLE").write.mode("overwrite").saveAsTable("ch13_employees")
    spark.createDataFrame([("hr.lead@shopwave.example", "HR"), ("cfo@shopwave.example", "Finance")],
                          "user_email STRING, department STRING").write.mode("overwrite").saveAsTable("ch13_dept_access")


def build_lab13(force: bool = False) -> list:
    built = []
    for name, fn in (("sec_customers", _build_sec_customers), ("sec_orders", _build_sec_orders),
                     ("sec_region_access", _build_region_access), ("ch13_employees", _build_ch13)):
        if force or not spark.catalog.tableExists(name):
            fn()
            built.append(name)
    return built


_built13 = build_lab13()
print("✅ Section 13 tables " + ("built: " + ", ".join(_built13) if _built13 else "ready")
      + f" · principal for grants: G13 = `{G13}`")

# COMMAND ----------

# DBTITLE 1,Mapping-table helpers (row-level security without a second user)
def my_regions() -> list:
    """Countries YOU may see according to sec_region_access."""
    return [r[0] for r in spark.sql(f"SELECT country FROM sec_region_access WHERE user_email = '{me()}' ORDER BY 1").collect()]


def grant_region(country: str):
    """Add (me, country) to the mapping table - like an admin giving you a region."""
    if country not in my_regions():
        spark.createDataFrame([(me(), country)], "user_email STRING, country STRING").write.mode("append") \
             .saveAsTable("sec_region_access")
    print(f"🗺️ {me()} may now see: {my_regions()}")


def revoke_region(country: str = None):
    """Remove one (or all, if country is None) of YOUR rows from the mapping table."""
    keep = [(r["user_email"], r["country"]) for r in spark.table("sec_region_access").collect()
            if not (r["user_email"] == me() and (country is None or r["country"] == country))]
    spark.createDataFrame(keep, "user_email STRING, country STRING").write.mode("overwrite") \
         .saveAsTable("sec_region_access")
    print(f"🗺️ {me()} may now see: {my_regions() or 'nothing'}")

# COMMAND ----------

# DBTITLE 1,Governed tags (tag policies API) and ABAC policy helpers
_ws13 = None


def _api(method: str, path: str, body=None, query=None):
    global _ws13
    if _ws13 is None:
        from databricks.sdk import WorkspaceClient
        _ws13 = WorkspaceClient()
    return _ws13.api_client.do(method, path, query=query, body=body) or {}


def governed_tag(key: str):
    """The governed tag (tag policy) `key`, or None."""
    try:
        return _api("GET", f"/api/2.1/tag-policies/{key}")
    except Exception:
        return None


def create_governed_tag(key: str, values: list, description: str = "") -> bool:
    """Create (or extend) a governed tag with allowed values - what Catalog > Govern > Governed tags does in the UI."""
    existing = governed_tag(key)
    try:
        if existing:
            have = {v["name"] for v in existing.get("values") or []}
            missing = [v for v in values if v not in have]
            if missing:
                _api("PATCH", f"/api/2.1/tag-policies/{key}", query={"update_mask": "values"},
                     body={"tag_key": key, "values": [{"name": v} for v in sorted(have | set(values))]})
            print(f"🏷️ governed tag '{key}' exists - allowed values {sorted(have | set(values))}")
            return True
        _api("POST", "/api/2.1/tag-policies",
             body={"tag_key": key, "description": description, "values": [{"name": v} for v in values]})
        print(f"🏷️ governed tag '{key}' created - allowed values {values}")
        return True
    except Exception as e:
        print(f"⛔ could not create the governed tag '{key}':", _first_line(e))
        print("   → create it in the UI (Catalog → Govern → Governed tags → Create governed tag) or ask an account admin.")
        return False


def show_policies(securable: str = None):
    """SHOW POLICIES ON SCHEMA <course schema> (ABAC row filter / column mask policies)."""
    return spark.sql(f"SHOW POLICIES ON SCHEMA {securable or f'{catalog_name}.{schema_name}'}")


def _policy_names() -> list:
    try:
        rows = show_policies().collect()
    except Exception:
        return []
    out = []
    for r in rows:
        d = r.asDict()
        name = d.get("name") or d.get("policy_name") or list(d.values())[0]
        on = (d.get("on_securable_fullname") or d.get("securable_fullname") or "")
        if str(name).startswith(("sw_", "ch13_")) and (not on or on.endswith(f"{schema_name}")):
            out.append(name)
    return out


def drop_lab13_policies():
    for name in _policy_names():
        try_sql(f"DROP POLICY {name} ON SCHEMA {catalog_name}.{schema_name}")

# COMMAND ----------

# DBTITLE 1,Reset Section 13
def _drop_security(table: str):
    for stmt in (f"ALTER TABLE {table} DROP ROW FILTER",):
        try:
            spark.sql(stmt)
        except Exception:
            pass
    try:
        for c in spark.table(table).columns:
            try:
                spark.sql(f"ALTER TABLE {table} ALTER COLUMN {c} DROP MASK")
            except Exception:
                pass
    except Exception:
        pass


def reset_lab13(confirm: str = ""):
    """Drop the Section 13 policies, filters, masks, views, functions and tables, and revoke what the labs granted."""
    if confirm != "YES":
        print("Nothing done. To really reset Section 13 call: reset_lab13(confirm='YES')")
        return
    drop_lab13_policies()
    for r in spark.sql("SHOW TABLES").collect():
        n = r["tableName"]
        if r["isTemporary"] or not n.startswith(("sec_", "ch13_")):
            continue
        _drop_security(n)
        for stmt in ("DROP VIEW IF EXISTS", "DROP TABLE IF EXISTS"):
            try:
                spark.sql(f"{stmt} {n}")
                break
            except Exception:
                pass
    for r in spark.sql("SHOW USER FUNCTIONS").collect():
        fn = r[0].split(".")[-1]
        if fn.startswith(("sec_", "ch13_")):
            try_sql(f"DROP FUNCTION IF EXISTS {fn}")
    for priv in ("USE SCHEMA", "SELECT", "MODIFY", "CREATE TABLE", "READ VOLUME", "EXECUTE"):
        try:
            spark.sql(f"REVOKE {priv} ON SCHEMA {schema_name} FROM `{G13}`")
        except Exception:
            pass
    print("✅ Section 13 reset - re-run %run ./_13_prepare to rebuild the tables")


print("🔧 Section 13 helpers loaded")
