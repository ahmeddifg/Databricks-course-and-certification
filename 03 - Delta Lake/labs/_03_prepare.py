# Databricks notebook source
# MAGIC %md
# MAGIC # 🔧 _03_prepare — Section 03 data & Delta helpers
# MAGIC Included by every Section 03 lab with `%run ./_03_prepare` (after `%run ../../Includes/_setup`).
# MAGIC
# MAGIC **Creates (only if missing) two small source datasets** in the raw volume:
# MAGIC
# MAGIC | Folder | Content | Used for |
# MAGIC |---|---|---|
# MAGIC | `raw/customers-updates-json/` | 40 changed existing customers (new email/city, newer `updated`) + 20 new customers `C0301–C0320` | `MERGE` upserts |
# MAGIC | `raw/products-new-csv/` | 6 products: 3 already in the catalog (`P005, P012, P020`) + 3 new (`P037–P039`) | insert-only `MERGE` (dedup) |
# MAGIC
# MAGIC **Helpers:** `history(table)`, `latest_version(table)`, `table_detail(table)`, `num_files(table)`,
# MAGIC `last_operation_metrics(table, operation)`, `reset_lab03()`.

# COMMAND ----------

# DBTITLE 1,Generate Section 03 source files (idempotent)
import json
import random
import datetime as _dt
from pyspark.sql import functions as F


def _gen_customer_updates():
    base = {r["customer_id"]: r.asDict()
            for r in spark.read.json(f"{dataset_path}/customers-json").collect()}
    rng = random.Random(303)
    changed_ids = sorted(rng.sample(sorted(base), 40))
    rows = []
    when = _dt.datetime(2026, 8, 15, 9, 0, 0)
    for i, cid in enumerate(changed_ids):
        rec = dict(base[cid])
        profile = json.loads(rec["profile"])
        if i % 2 == 0:  # half of them moved to another city (same country)
            profile["address"]["city"] = profile["address"]["city"] + " North"
        first, last = profile["first_name"].lower(), profile["last_name"].lower()
        rec["email"] = f"{first}.{last}.{cid.lower()}@shopwave.org"
        rec["profile"] = json.dumps(profile)
        rec["updated"] = (when + _dt.timedelta(minutes=i)).strftime("%Y-%m-%dT%H:%M:%S.000Z")
        rows.append(rec)
    for n in range(301, 321):  # brand-new customers
        profile = {"first_name": f"New{n}", "last_name": "Customer", "gender": rng.choice(["Male", "Female"]),
                   "address": {"street": f"{n} Market St", "city": "Riyadh", "country": "Saudi Arabia"}}
        rows.append({"customer_id": f"C{n:04d}", "email": f"new{n}@shopwave.org", "profile": json.dumps(profile),
                     "updated": (when + _dt.timedelta(hours=2, minutes=n)).strftime("%Y-%m-%dT%H:%M:%S.000Z")})
    return rows


def _prepare_lab03_sources():
    created = []
    target = f"{dataset_path}/customers-updates-json"
    if not path_exists(target):
        text = "\n".join(json.dumps(r, ensure_ascii=False) for r in _gen_customer_updates()) + "\n"
        dbutils.fs.mkdirs(target)
        dbutils.fs.put(f"{target}/updates_001.json", text, True)
        created.append("customers-updates-json")
    target = f"{dataset_path}/products-new-csv"
    if not path_exists(target):
        lines = ["product_id;title;brand;category;price",
                 "P005;Mechanical Keyboard;Voltix;Electronics;129.99",   # already exists
                 "P012;Blender;Casa;Home & Kitchen;89.5",                # already exists
                 "P020;Yoga Mat;Stride;Sports;35.0",                     # already exists
                 "P037;Smart Doorbell;Nexa;Electronics;149.0",           # new
                 "P038;Pour-Over Coffee Set;Brewly;Home & Kitchen;39.9", # new
                 "P039;The Delta Lake Handbook;DataPub;Books;44.0"]      # new
        dbutils.fs.mkdirs(target)
        dbutils.fs.put(f"{target}/new_products.csv", "\n".join(lines) + "\n", True)
        created.append("products-new-csv")
    return created


_created03 = _prepare_lab03_sources()
print("📦 Section 03 source files ready" + (f" (created: {', '.join(_created03)})" if _created03 else ""))

# COMMAND ----------

# DBTITLE 1,Delta helpers
def history(table: str):
    """DESCRIBE HISTORY with the most useful columns, oldest version first."""
    return (spark.sql(f"DESCRIBE HISTORY {table}")
                 .select("version", "timestamp", "operation", "operationParameters", "operationMetrics")
                 .orderBy("version"))


def latest_version(table: str) -> int:
    return spark.sql(f"DESCRIBE HISTORY {table} LIMIT 1").first()["version"]


def table_detail(table: str):
    """DESCRIBE DETAIL as a Row: format, location, numFiles, sizeInBytes, properties, ..."""
    return spark.sql(f"DESCRIBE DETAIL {table}").first()


def num_files(table: str) -> int:
    return table_detail(table)["numFiles"]


def last_operation_metrics(table: str, operation: str) -> dict:
    """operationMetrics of the most recent commit whose operation == `operation` (e.g. 'MERGE', 'OPTIMIZE')."""
    row = (spark.sql(f"DESCRIBE HISTORY {table}")
                .where(F.col("operation") == operation)
                .orderBy(F.desc("version")).first())
    return {} if row is None else dict(row["operationMetrics"] or {})


def reset_lab03():
    """Drop every lab03_* table so the labs can start from scratch."""
    for t in [r["tableName"] for r in spark.sql("SHOW TABLES LIKE 'lab03*'").collect()]:
        spark.sql(f"DROP TABLE IF EXISTS {t}")
        print("dropped", t)


print("🔎 Helpers ready: history(), latest_version(), table_detail(), num_files(), last_operation_metrics(), reset_lab03()")
