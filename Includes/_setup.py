# Databricks notebook source
# MAGIC %md
# MAGIC # 🛠️ Course Setup — ShopWave Lakehouse
# MAGIC
# MAGIC > **You never need to open or edit this notebook.** Every lab includes it with
# MAGIC > `%run ../../Includes/_setup` (or `%run ./Includes/_setup` from the course root).
# MAGIC
# MAGIC What it does — it is **idempotent**, so running it many times is safe:
# MAGIC
# MAGIC | Step | Action |
# MAGIC |---|---|
# MAGIC | 1 | Picks a Unity Catalog **catalog** → your default catalog (`workspace` on Free Edition) |
# MAGIC | 2 | Creates schema **`shopwave`** + volumes **`raw`**, **`checkpoints`**, **`files`** |
# MAGIC | 3 | Generates the ShopWave dataset into `/Volumes/<catalog>/shopwave/raw/` (first run only) |
# MAGIC | 4 | Runs `USE CATALOG` / `USE SCHEMA` so SQL cells can use short table names |
# MAGIC | 5 | Defines helpers: `course_info()`, `land_new_orders()`, `reset_orders_landing()`, `reset_course()` |
# MAGIC
# MAGIC **Use a different catalog or schema?** In your notebook, add a Python cell **before** the `%run` cell:
# MAGIC ```python
# MAGIC COURSE_CATALOG = "my_catalog"   # optional
# MAGIC COURSE_SCHEMA  = "shopwave"     # optional
# MAGIC ```
# MAGIC
# MAGIC Works on **serverless** compute (recommended, and the only option on Free Edition) and on classic compute with Unity Catalog.

# COMMAND ----------

# DBTITLE 1,Imports & configuration
import json
import random
import datetime as _dt

from pyspark.sql import types as T

SETUP_VERSION = "1.0.0"
COURSE_SCHEMA = globals().get("COURSE_SCHEMA") or "shopwave"
_VOLUMES = ("raw", "checkpoints", "files")

# COMMAND ----------

# DBTITLE 1,Resolve catalog, create schema & volumes
def _resolve_catalog():
    """Use COURSE_CATALOG if the learner set it, else the current (default) catalog."""
    requested = globals().get("COURSE_CATALOG")
    if requested:
        return requested
    current = spark.sql("SELECT current_catalog()").first()[0]
    if current not in ("hive_metastore", "spark_catalog"):
        return current
    # Old workspaces may default to the legacy Hive metastore -> find a Unity Catalog catalog.
    available = [row[0] for row in spark.sql("SHOW CATALOGS").collect()]
    for candidate in ("workspace", "main"):
        if candidate in available:
            return candidate
    raise RuntimeError(
        "Your default catalog is the legacy Hive metastore and no 'workspace'/'main' catalog was found.\n"
        "Set a Unity Catalog catalog you can write to, in a cell BEFORE the %run cell, e.g.:\n"
        "    COURSE_CATALOG = 'my_catalog'\n"
        f"Catalogs visible to you: {available}"
    )


catalog_name = _resolve_catalog()
schema_name = COURSE_SCHEMA

try:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog_name}`.`{schema_name}` "
              "COMMENT 'ShopWave - Databricks Data Engineer Associate course'")
    for _v in _VOLUMES:
        spark.sql(f"CREATE VOLUME IF NOT EXISTS `{catalog_name}`.`{schema_name}`.`{_v}`")
except Exception as e:
    raise RuntimeError(
        f"Could not create schema/volumes in catalog '{catalog_name}'. You need USE CATALOG + CREATE SCHEMA "
        f"privileges there (on Free Edition the 'workspace' catalog works out of the box).\n"
        f"Set COURSE_CATALOG = '<a catalog you own>' before the %run cell.\nOriginal error: {e}"
    ) from e

spark.sql(f"USE CATALOG `{catalog_name}`")
spark.sql(f"USE SCHEMA `{schema_name}`")

volume_root = f"/Volumes/{catalog_name}/{schema_name}"
dataset_path = f"{volume_root}/raw"            # source files (read-only in labs)
checkpoint_path = f"{volume_root}/checkpoints"  # streaming checkpoints / schema locations
files_path = f"{volume_root}/files"             # scratch space for your own experiments

# COMMAND ----------

# DBTITLE 1,File-system helpers
def path_exists(path: str) -> bool:
    """True if a file/dir exists (works for /Volumes paths)."""
    try:
        dbutils.fs.ls(path)
        return True
    except Exception as e:  # message text differs between compute types
        msg = str(e).lower()
        if any(k in msg for k in ("not found", "notfound", "does not exist", "no such file")):
            return False
        raise


def _write_text(path: str, text: str):
    dbutils.fs.mkdirs(path.rsplit("/", 1)[0])
    dbutils.fs.put(path, text, True)  # overwrite=True


def _jsonl(rows):
    return "\n".join(json.dumps(r, ensure_ascii=False) for r in rows) + "\n"

# COMMAND ----------

# DBTITLE 1,ShopWave data generator (deterministic)
_FIRST = ["Adam", "Sarah", "Omar", "Lina", "John", "Maya", "Ali", "Sofia", "Lucas", "Noor", "Emma",
          "Yusuf", "Hana", "Carlos", "Aisha", "Liam", "Mei", "Ivan", "Fatima", "Noah", "Zara", "Kenji",
          "Leila", "Mateo", "Chloe", "Tariq", "Elena", "Samir", "Grace", "Ravi"]
_LAST = ["Smith", "Haddad", "Garcia", "Chen", "Muller", "Rossi", "Khan", "Silva", "Nakamura", "Dubois",
         "Ali", "Novak", "Kim", "Hansen", "Costa", "Saleh", "Brown", "Ivanova", "Lopez", "Tanaka"]
_COUNTRIES = {
    "Saudi Arabia": ["Riyadh", "Jeddah", "Dammam"],
    "United Arab Emirates": ["Dubai", "Abu Dhabi"],
    "Egypt": ["Cairo", "Alexandria"],
    "United States": ["New York", "Austin", "Seattle"],
    "United Kingdom": ["London", "Manchester"],
    "France": ["Paris", "Lyon"],
    "Germany": ["Berlin", "Munich"],
    "India": ["Bengaluru", "Mumbai"],
    "Japan": ["Tokyo", "Osaka"],
    "Brazil": ["Sao Paulo", "Rio de Janeiro"],
}
_STREETS = ["King Fahd Rd", "Main St", "Olaya St", "Baker St", "Rue de Rivoli", "Sunset Blvd",
            "Tahrir Sq", "Market St", "Park Ave", "Nile Corniche"]
_DOMAINS = ["gmail.com", "outlook.com", "shopwave.org", "university.edu", "company.com", "mail.net"]
_CATALOG = {  # category -> (brand list, product names, price range)
    "Electronics": (["Voltix", "Nexa", "Aurora"], ["Wireless Earbuds", "Smart Watch", "USB-C Hub",
                    "4K Monitor", "Mechanical Keyboard", "Noise-Cancelling Headphones"], (19, 499)),
    "Home & Kitchen": (["Casa", "Brewly"], ["Espresso Machine", "Air Fryer", "Chef Knife Set",
                       "Cast Iron Pan", "Smart Kettle", "Blender"], (15, 320)),
    "Books": (["Lakeside Press", "DataPub"], ["Spark in Action", "Designing Data Pipelines",
              "The Lakehouse Book", "SQL for Analysts", "Streaming Systems 101", "Clean Data"], (12, 65)),
    "Sports": (["Stride", "Peakline"], ["Running Shoes", "Yoga Mat", "Adjustable Dumbbells",
               "Cycling Helmet", "Hiking Backpack", "Fitness Tracker"], (10, 260)),
    "Beauty": (["Glow", "Pure"], ["Face Serum", "Sunscreen SPF50", "Hair Dryer", "Perfume Oud",
               "Lip Balm Set", "Beard Trimmer"], (6, 150)),
    "Toys": (["Blocko", "FunLab"], ["Building Blocks Set", "RC Car", "Puzzle 1000pc", "Science Kit",
             "Plush Camel", "Board Game"], (8, 120)),
}


def _ts(d: _dt.datetime) -> str:
    return d.strftime("%Y-%m-%dT%H:%M:%S.000Z")


def generate_customers(rng, n=300):
    base = _dt.datetime(2024, 1, 1)
    rows = []
    for i in range(1, n + 1):
        first, last = rng.choice(_FIRST), rng.choice(_LAST)
        country = rng.choice(list(_COUNTRIES))
        profile = {
            "first_name": first,
            "last_name": last,
            "gender": rng.choice(["Male", "Female"]),
            "address": {"street": f"{rng.randint(1, 250)} {rng.choice(_STREETS)}",
                        "city": rng.choice(_COUNTRIES[country]),
                        "country": country},
        }
        email = f"{first}.{last}{i}@{rng.choice(_DOMAINS)}".lower()
        if rng.random() < 0.05:           # ~5% missing emails -> null handling lessons
            email = None
        updated = base + _dt.timedelta(days=rng.randint(0, 800), seconds=rng.randint(0, 86399))
        rows.append({"customer_id": f"C{i:04d}", "email": email,
                     "profile": json.dumps(profile),   # JSON *string* -> teaches `profile:address:country`
                     "updated": _ts(updated)})
    return rows


def generate_products(rng):
    rows, pid = [], 1
    for category, (brands, names, (lo, hi)) in _CATALOG.items():
        for name in names:
            rows.append({"product_id": f"P{pid:03d}", "title": name, "brand": rng.choice(brands),
                         "category": category, "price": round(rng.uniform(lo, hi), 2)})
            pid += 1
    return rows


def _make_order(rng, order_no, customers, products, when):
    items = []
    for p in rng.sample(products, rng.randint(1, 4)):
        q = rng.randint(1, 3)
        items.append({"product_id": p["product_id"], "quantity": q, "subtotal": round(q * p["price"], 2)})
    return {"order_id": f"O{order_no:06d}",
            "order_timestamp": int(when.replace(tzinfo=_dt.timezone.utc).timestamp()),
            "customer_id": rng.choice(customers)["customer_id"],
            "quantity": sum(i["quantity"] for i in items),
            "total": round(sum(i["subtotal"] for i in items), 2),
            "items": items}


def generate_orders(rng, customers, products, n=2000, start_no=1,
                    start=_dt.datetime(2026, 1, 1), days=181):
    rows = []
    for k in range(n):
        when = start + _dt.timedelta(days=rng.randint(0, days - 1), seconds=rng.randint(0, 86399))
        rows.append(_make_order(rng, start_no + k, customers, products, when))
    # Deliberate data-quality issues (used in later sections)
    for r in rng.sample(rows, max(1, n // 100)):          # ~1% cancelled: quantity 0, no items
        r.update(quantity=0, total=0.0, items=[])
    for r in rng.sample(rows, max(1, n // 400)):          # unknown customer -> anti-join lessons
        r["customer_id"] = "C9999"
    rows.extend(dict(r) for r in rng.sample(rows, max(1, n // 200)))  # exact duplicates
    return rows


ORDER_SCHEMA = T.StructType([
    T.StructField("order_id", T.StringType()),
    T.StructField("order_timestamp", T.LongType()),
    T.StructField("customer_id", T.StringType()),
    T.StructField("quantity", T.IntegerType()),
    T.StructField("total", T.DoubleType()),
    T.StructField("items", T.ArrayType(T.StructType([
        T.StructField("product_id", T.StringType()),
        T.StructField("quantity", T.IntegerType()),
        T.StructField("subtotal", T.DoubleType()),
    ]))),
])

# COMMAND ----------

# DBTITLE 1,Write datasets to the raw volume (skips anything that already exists)
def _ensure(name, writer):
    target = f"{dataset_path}/{name}"
    if path_exists(target):
        return False
    print(f"  • generating {name} ...")
    writer(target)
    return True


def build_datasets():
    rng = random.Random(2026)  # fixed seed -> everyone gets identical data
    customers = generate_customers(rng)
    products = generate_products(rng)
    history = generate_orders(rng, customers, products)
    stream_rng = random.Random(7)
    stream_batches = [
        generate_orders(stream_rng, customers, products, n=120, start_no=100001 + b * 1000,
                        start=_dt.datetime(2026, 7, 1) + _dt.timedelta(days=b), days=1)
        for b in range(10)
    ]
    created = []

    def w_customers(t):
        for part in range(3):  # 3 files -> teaches wildcard / directory reads
            _write_text(f"{t}/export_{part + 1:03d}.json", _jsonl(customers[part::3]))

    def w_products(t):
        header = "product_id;title;brand;category;price"
        half = len(products) // 2
        for idx, chunk in enumerate((products[:half], products[half:])):
            lines = [header] + [f"{p['product_id']};{p['title']};{p['brand']};{p['category']};{p['price']}"
                                for p in chunk]
            _write_text(f"{t}/export_{idx + 1:03d}.csv", "\n".join(lines) + "\n")

    def w_orders_parquet(t):
        rows = [(o["order_id"], o["order_timestamp"], o["customer_id"], o["quantity"], o["total"],
                 [(i["product_id"], i["quantity"], i["subtotal"]) for i in o["items"]]) for o in history]
        spark.createDataFrame(rows, ORDER_SCHEMA).repartition(4).write.mode("overwrite").parquet(t)

    def w_orders_staging(t):
        for b, batch in enumerate(stream_batches, 1):
            _write_text(f"{t}/{b:02d}.json", _jsonl(batch))

    for name, fn in [("customers-json", w_customers), ("products-csv", w_products),
                     ("orders-parquet", w_orders_parquet), ("orders-staging", w_orders_staging)]:
        if _ensure(name, fn):
            created.append(name)

    if not path_exists(f"{dataset_path}/orders-landing"):
        land_new_orders(1, verbose=False)   # first file lands so streams have data to start with
        created.append("orders-landing")

    welcome = f"{files_path}/welcome.txt"
    if not path_exists(welcome):
        _write_text(welcome, "Welcome to the ShopWave lakehouse!\n"
                             "This file lives in a Unity Catalog VOLUME, not in DBFS.\n"
                             f"Path: {welcome}\n")
    return created

# COMMAND ----------

# DBTITLE 1,Helper functions used by the labs
def land_new_orders(n: int = 1, all: bool = False, verbose: bool = True) -> int:
    """Simulate new files arriving: copy the next staged JSON file(s) into orders-landing/."""
    staging, landing = f"{dataset_path}/orders-staging", f"{dataset_path}/orders-landing"
    staged = sorted(f.name for f in dbutils.fs.ls(staging) if f.name.endswith(".json"))
    landed = {f.name for f in dbutils.fs.ls(landing)} if path_exists(landing) else set()
    pending = [f for f in staged if f not in landed]
    if not pending:
        print("No more files to land - all 10 batches are already in orders-landing/. "
              "Call reset_orders_landing() to start over.")
        return 0
    todo = pending if all else pending[:max(1, n)]
    for name in todo:
        dbutils.fs.cp(f"{staging}/{name}", f"{landing}/{name}")
        if verbose:
            print(f"Landed {name} -> {landing}/")
    return len(todo)


def reset_orders_landing():
    """Empty orders-landing/ and land batch 01 again (use before re-running streaming labs)."""
    landing = f"{dataset_path}/orders-landing"
    if path_exists(landing):
        dbutils.fs.rm(landing, True)
    land_new_orders(1)


def reset_course(confirm: str = ""):
    """Drop the whole course schema (tables, views, volumes, files). Pass confirm='YES'."""
    if confirm != "YES":
        print("Nothing done. To really drop everything call: reset_course(confirm='YES')")
        return
    spark.sql(f"DROP SCHEMA IF EXISTS `{catalog_name}`.`{schema_name}` CASCADE")
    print(f"Dropped {catalog_name}.{schema_name}. Re-run the %run cell to rebuild it.")


def course_info():
    rows = [("catalog", catalog_name), ("schema", schema_name), ("dataset_path", dataset_path),
            ("checkpoint_path", checkpoint_path), ("files_path", files_path),
            ("current user", spark.sql("SELECT current_user()").first()[0]),
            ("setup version", SETUP_VERSION)]
    width = max(len(k) for k, _ in rows)
    print("\n".join(f"{k:<{width}} : {v}" for k, v in rows))

# COMMAND ----------

# DBTITLE 1,Run
_created = build_datasets()
print("✅ ShopWave course environment ready" + (f" (generated: {', '.join(_created)})" if _created else ""))
course_info()
