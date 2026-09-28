# Databricks notebook source
# MAGIC %md
# MAGIC # 🔧 _07_prepare — Section 07 bronze tables, messy sources & helpers
# MAGIC Included by every Section 07 lab with `%run ./_07_prepare` (after `%run ../../Includes/_setup`). Idempotent: it only
# MAGIC creates what is missing.
# MAGIC
# MAGIC **New raw sources** (generated once into the `raw` volume, deterministic):
# MAGIC
# MAGIC | Folder | Content | Used for |
# MAGIC |---|---|---|
# MAGIC | `payments-messy/` | 2 CSV exports, **431 rows**: mixed-case/space methods, amounts like `" 12.50 "`, `"USD 12.50"`, `"N/A"`, two date formats, empty currencies, **10 exact duplicates**, **15 re-sent status updates**, 3 rows without `payment_id`, 2 negative amounts, 3 payments for unknown orders | 07-L1 cleaning & dedup, 07-L3 quality |
# MAGIC | `customer-updates/` | 2 CSV batches of customer changes (2026-04-01: 25 rows, 2026-06-01: 27 rows incl. 5 new customers and 2 rows that change nothing) | 07-L3 SCD type 1 & 2 |
# MAGIC | `shipments-messy/` | 1 CSV, **318 rows** of shipments with the same kinds of problems | 07-L4 challenge |
# MAGIC
# MAGIC **Bronze tables** (`lab07_bronze_*`): `customers` (raw JSON, `profile` is a JSON **string**), `products` (CSV, all
# MAGIC **strings**), `orders` (Parquet, `items` array), `payments` (CSV, all strings). Plus `lab07_country_targets` (monthly revenue
# MAGIC targets per country, for multi-key joins).
# MAGIC
# MAGIC **Helpers:** `run_sql(stmt)`, `null_counts(df)`, `plan_text(df)`, `join_strategy(plan)`, `time_it(label, fn)`,
# MAGIC `build_silver_tables(force=False)` (reference silver tables for 07-L2/L3/L4 if you skipped 07-L1), `reset_lab07()`.

# COMMAND ----------

# DBTITLE 1,Generate the Section 07 source files (first run only)
import csv
import io
import json
import random
import textwrap
import time
import datetime as _dt
from pyspark.sql import functions as F
from pyspark.sql import Window

_ISO = "%Y-%m-%d %H:%M:%S"
_EU = "%d/%m/%Y %H:%M"


def _csv_text(header, rows):
    buf = io.StringIO()
    w = csv.writer(buf, lineterminator="\n")
    w.writerow(header)
    for r in rows:
        w.writerow(["" if r.get(h) is None else r.get(h) for h in header])
    return buf.getvalue()


def _order_facts():
    """(order_id, order_timestamp, total, quantity) of the distinct historical orders, sorted -> deterministic."""
    rows = (spark.read.parquet(f"{dataset_path}/orders-parquet")
                 .select("order_id", "order_timestamp", "total", "quantity").distinct().collect())
    return sorted((r["order_id"], r["order_timestamp"], r["total"], r["quantity"]) for r in rows)


def _variant(rng, canonical, variants):
    return rng.choice(variants[canonical])


def _payment_lines():
    rng = random.Random(707)
    orders = _order_facts()
    picked = sorted(rng.sample(range(len(orders)), 400))
    methods = {"card": ["card", "Card", "CARD", " card ", "card "], "paypal": ["paypal", "PayPal", "PAYPAL", " paypal"],
               "apple_pay": ["apple_pay", "Apple Pay", "apple-pay", "APPLE PAY"],
               "bank_transfer": ["bank_transfer", "Bank Transfer", "bank transfer"]}
    statuses = {"SUCCESS": ["SUCCESS", "success", "Success"], "FAILED": ["FAILED", "failed"],
                "REFUNDED": ["REFUNDED", "refunded"]}
    base = []
    targets = [orders[i] for i in picked] + [(f"O99{k:04d}", orders[k * 100][1], 49.99, 1) for k in range(1, 4)]
    for n, (oid, ts, total, _q) in enumerate(targets, 1):
        paid = _dt.datetime.fromtimestamp(ts, _dt.timezone.utc).replace(tzinfo=None) + _dt.timedelta(seconds=rng.randint(60, 3600))
        base.append({"payment_id": f"PAY{n:05d}", "order_id": oid,
                     "method_c": rng.choices(list(methods), weights=[55, 25, 15, 5])[0],
                     "amount_v": round(float(total), 2),
                     "status_c": rng.choices(list(statuses), weights=[85, 10, 5])[0],
                     "paid": paid})
    for r in rng.sample(base, 2):                      # 2 negative amounts (refunds booked in the wrong feed)
        r["amount_v"] = -abs(r["amount_v"]) if r["amount_v"] else -15.0

    def render(r, status_c=None, updated=None):
        amount = f"{r['amount_v']:.2f}"
        x = rng.random()
        amount = f"  {amount} " if x < 0.10 else (f"USD {amount}" if x < 0.15 else ("N/A" if x < 0.17 else amount))
        y = rng.random()
        paid_at = r["paid"].strftime(_ISO) if y < 0.85 else (r["paid"].strftime(_EU) if y < 0.98 else None)
        z = rng.random()
        currency = "USD" if z < 0.85 else ("usd" if z < 0.95 else None)
        method = None if rng.random() < 0.02 else _variant(rng, r["method_c"], methods)
        status_c = status_c or r["status_c"]
        return {"payment_id": r["payment_id"], "order_id": r["order_id"], "method": method, "amount": amount,
                "currency": currency, "paid_at": paid_at, "status": _variant(rng, status_c, statuses),
                "updated_at": (updated or r["paid"] + _dt.timedelta(hours=1)).strftime(_ISO)}

    lines = [render(r) for r in base]
    successes = [r for r in base if r["status_c"] == "SUCCESS"]
    for r in rng.sample(successes, 15):                # re-sent later with a new status
        lines.append(render(r, status_c="REFUNDED", updated=r["paid"] + _dt.timedelta(days=3)))
    for r in rng.sample(base, 3):                      # rows that lost their key
        bad = render(r)
        bad["payment_id"] = None
        lines.append(bad)
    lines += [dict(l) for l in rng.sample(lines[:len(base)], 10)]   # exact duplicates
    rng.shuffle(lines)
    return lines


def _shipment_lines():
    rng = random.Random(717)
    orders = [o for o in _order_facts() if o[3] > 0]
    picked = sorted(rng.sample(range(len(orders)), 300))
    carriers = {"DHL": ["DHL", "dhl", " Dhl "], "ARAMEX": ["Aramex", "ARAMEX", "aramex "], "FEDEX": ["FedEx", "FEDEX", "fedex"]}
    base = []
    for n, i in enumerate(picked, 1):
        oid, ts, _t, _q = orders[i]
        shipped = _dt.datetime.fromtimestamp(ts, _dt.timezone.utc).replace(tzinfo=None) + _dt.timedelta(hours=rng.randint(6, 48), minutes=rng.randint(0, 59))
        base.append({"shipment_id": f"SHP{n:05d}", "order_id": oid,
                     "carrier_c": rng.choices(list(carriers), weights=[45, 35, 20])[0],
                     "shipped": shipped,
                     "delivered": shipped + _dt.timedelta(hours=rng.randint(12, 120), minutes=rng.randint(0, 59)),
                     "cost_v": round(rng.uniform(5, 30), 2), "cost_bad": False})
    idx = list(range(300))
    rng.shuffle(idx)
    in_transit, backwards, bad_cost, updated = idx[:20], idx[20:25], idx[25:29], idx[29:41]
    for k in in_transit:
        base[k]["delivered"] = None
    for k in backwards:                                # delivered BEFORE shipped -> quarantine
        base[k]["delivered"] = base[k]["shipped"] - _dt.timedelta(hours=rng.randint(2, 30))
    for k in bad_cost:
        base[k]["cost_bad"] = True

    def render(r, delivered="use", updated_at=None):
        d = r["delivered"] if delivered == "use" else delivered
        cost = f"{r['cost_v']:.2f}"
        x = rng.random()
        cost = "N/A" if r["cost_bad"] else (f" {cost}" if x < 0.10 else (f"USD {cost}" if x < 0.17 else cost))
        shipped = r["shipped"].strftime(_ISO) if rng.random() < 0.8 else r["shipped"].strftime(_EU)
        return {"shipment_id": r["shipment_id"], "order_id": r["order_id"],
                "carrier": _variant(rng, r["carrier_c"], carriers), "shipped_at": shipped,
                "delivered_at": d.strftime(_ISO) if d else None, "cost": cost,
                "updated_at": (updated_at or (d or r["shipped"]) + _dt.timedelta(hours=1)).strftime(_ISO)}

    lines = []
    for k, r in enumerate(base):
        if k in updated:                               # first an "in transit" record, later the delivered one
            lines.append(render(r, delivered=None, updated_at=r["shipped"] + _dt.timedelta(hours=1)))
        lines.append(render(r))
    lines += [dict(l) for l in rng.sample(lines, 6)]   # exact duplicates
    rng.shuffle(lines)
    return lines


def _customer_update_batches():
    rng = random.Random(727)
    raw = sorted(spark.read.json(f"{dataset_path}/customers-json").select("customer_id", "email", "profile").collect(),
                 key=lambda r: r["customer_id"])
    cust = {}
    for r in raw:
        p = json.loads(r["profile"])
        cust[r["customer_id"]] = {"customer_id": r["customer_id"], "first_name": p["first_name"],
                                  "last_name": p["last_name"], "email": r["email"],
                                  "city": p["address"]["city"], "country": p["address"]["country"]}
    ids = sorted(cust)
    null_email = [c for c in ids if cust[c]["email"] is None]
    others = [c for c in ids if cust[c]["email"] is not None]
    rng.shuffle(others)
    set_a, set_b, set_c, set_d = others[:20], [null_email[0]] + others[20:24], others[24:36], others[36:38]
    countries = {"Saudi Arabia": ["Riyadh", "Jeddah", "Dammam"], "United Arab Emirates": ["Dubai", "Abu Dhabi"],
                 "Egypt": ["Cairo", "Alexandria"], "United States": ["New York", "Austin", "Seattle"],
                 "United Kingdom": ["London", "Manchester"], "France": ["Paris", "Lyon"], "Germany": ["Berlin", "Munich"],
                 "India": ["Bengaluru", "Mumbai"], "Japan": ["Tokyo", "Osaka"], "Brazil": ["Sao Paulo", "Rio de Janeiro"]}

    def move(c):
        new_country = rng.choice([k for k in countries if k != c["country"]])
        return dict(c, country=new_country, city=rng.choice(countries[new_country]))

    b1, current = [], {k: dict(v) for k, v in cust.items()}
    for cid in set_a:
        current[cid] = move(current[cid])
        b1.append(dict(current[cid]))
    for n, cid in enumerate(set_b, 1):
        c = current[cid]
        current[cid] = dict(c, email=f"{c['first_name']}.{c['last_name']}.new{n}@shopwave.org".lower())
        b1.append(dict(current[cid]))
    b2 = []
    for cid in set_a[:8] + set_c:
        current[cid] = move(current[cid])
        b2.append(dict(current[cid]))
    b2 += [dict(current[cid]) for cid in set_d]        # no change at all
    for n in range(1, 6):
        first, last = rng.choice(["Rana", "Yara", "Karim", "Nadia", "Tom"]), rng.choice(["Aziz", "Stone", "Hale"])
        country = rng.choice(list(countries))
        b2.append({"customer_id": f"C{300 + n:04d}", "first_name": first, "last_name": last,
                   "email": f"{first}.{last}{300 + n}@gmail.com".lower(), "country": country,
                   "city": rng.choice(countries[country])})
    for r in b1:
        r["updated_at"] = "2026-04-01 00:00:00"
    for r in b2:
        r["updated_at"] = "2026-06-01 00:00:00"
    return b1, b2


def _prepare_lab07_sources():
    created = []
    pay_hdr = ["payment_id", "order_id", "method", "amount", "currency", "paid_at", "status", "updated_at"]
    if not path_exists(f"{dataset_path}/payments-messy"):
        lines = _payment_lines()
        half = len(lines) // 2
        _write_text(f"{dataset_path}/payments-messy/payments_export_1.csv", _csv_text(pay_hdr, lines[:half]))
        _write_text(f"{dataset_path}/payments-messy/payments_export_2.csv", _csv_text(pay_hdr, lines[half:]))
        created.append("payments-messy")
    if not path_exists(f"{dataset_path}/shipments-messy"):
        shp_hdr = ["shipment_id", "order_id", "carrier", "shipped_at", "delivered_at", "cost", "updated_at"]
        _write_text(f"{dataset_path}/shipments-messy/shipments_export.csv", _csv_text(shp_hdr, _shipment_lines()))
        created.append("shipments-messy")
    if not path_exists(f"{dataset_path}/customer-updates"):
        hdr = ["customer_id", "first_name", "last_name", "email", "city", "country", "updated_at"]
        b1, b2 = _customer_update_batches()
        _write_text(f"{dataset_path}/customer-updates/updates_2026_04_01.csv", _csv_text(hdr, b1))
        _write_text(f"{dataset_path}/customer-updates/updates_2026_06_01.csv", _csv_text(hdr, b2))
        created.append("customer-updates")
    return created


_created07 = _prepare_lab07_sources()
print("✅ Section 07 sources ready" + (f" (generated: {', '.join(_created07)})" if _created07 else ""))

# COMMAND ----------

# DBTITLE 1,Bronze tables (created only if missing)
def _csv_strings(path, sep=","):
    """Read CSV with a header and NO type inference -> every column is a STRING (typical bronze)."""
    return spark.read.option("header", True).option("sep", sep).csv(path)


def build_bronze_tables(force: bool = False):
    sources = {
        "lab07_bronze_customers": lambda: spark.read.json(f"{dataset_path}/customers-json"),
        "lab07_bronze_products": lambda: _csv_strings(f"{dataset_path}/products-csv", ";"),
        "lab07_bronze_orders": lambda: spark.read.parquet(f"{dataset_path}/orders-parquet"),
        "lab07_bronze_payments": lambda: _csv_strings(f"{dataset_path}/payments-messy"),
    }
    created = []
    for name, reader in sources.items():
        if force or not spark.catalog.tableExists(name):
            (reader().withColumn("_source_file", F.col("_metadata.file_name"))
                     .withColumn("_ingested_at", F.current_timestamp())
                     .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(name))
            created.append(name)
    if force or not spark.catalog.tableExists("lab07_country_targets"):
        rng = random.Random(737)
        countries = ["Brazil", "Egypt", "France", "Germany", "India", "Japan", "Saudi Arabia", "United Arab Emirates",
                     "United Kingdom", "United States"]
        missing = {("Brazil", "2026-05"), ("Brazil", "2026-06"), ("Japan", "2026-06"), ("Egypt", "2026-06")}
        rows = [(c, f"2026-{m:02d}", float(rng.randrange(8000, 16000, 500)))
                for c in countries for m in range(1, 7) if (c, f"2026-{m:02d}") not in missing]
        (spark.createDataFrame(rows, "country STRING, month STRING, target_revenue DOUBLE")
              .write.mode("overwrite").saveAsTable("lab07_country_targets"))
        created.append("lab07_country_targets")
    return created


_created07b = build_bronze_tables()
print("✅ Section 07 bronze tables ready" + (f" (created: {', '.join(_created07b)})" if _created07b else ""))

# COMMAND ----------

# DBTITLE 1,Reference silver tables (same logic as 07-L1)
PROFILE_SCHEMA = ("first_name STRING, last_name STRING, gender STRING, "
                  "address STRUCT<street: STRING, city: STRING, country: STRING>")


def silver_customers_df():
    return (spark.table("lab07_bronze_customers")
                 .withColumn("p", F.from_json("profile", PROFILE_SCHEMA))
                 .select("customer_id", "p.first_name", "p.last_name", "p.gender",
                         F.lower(F.trim("email")).alias("email"),
                         F.split("email", "@").getItem(1).alias("email_domain"),
                         "p.address.street", "p.address.city", "p.address.country",
                         F.to_timestamp("updated").alias("updated_at")))


def silver_products_df():
    return (spark.table("lab07_bronze_products")
                 .select(F.trim("product_id").alias("product_id"), F.trim("title").alias("title"),
                         F.trim("brand").alias("brand"), F.trim("category").alias("category"),
                         F.col("price").cast("decimal(10,2)").alias("price")))


def silver_orders_df():
    return (spark.table("lab07_bronze_orders")
                 .drop("_source_file", "_ingested_at")
                 .dropDuplicates()
                 .select("order_id", "customer_id",
                         F.timestamp_seconds("order_timestamp").alias("order_ts"),
                         F.to_date(F.timestamp_seconds("order_timestamp")).alias("order_date"),
                         "quantity", F.col("total").cast("decimal(10,2)").alias("total"),
                         (F.col("quantity") == 0).alias("is_cancelled"),
                         "items"))


def silver_order_items_df():
    return (silver_orders_df()
            .select("order_id", F.posexplode("items").alias("pos", "item"))
            .select("order_id", (F.col("pos") + 1).alias("line_no"), "item.product_id", "item.quantity",
                    F.col("item.subtotal").cast("decimal(10,2)").alias("subtotal")))


def clean_payments_df(bronze):
    """Standardise the messy payment export (types, casing, formats) - no deduplication."""
    method = F.regexp_replace(F.lower(F.trim("method")), "[ -]+", "_")
    amount = F.expr("try_cast(regexp_replace(trim(amount), '[^0-9.-]', '') AS DECIMAL(10,2))")
    paid_at = F.coalesce(F.expr("try_to_timestamp(paid_at, 'yyyy-MM-dd HH:mm:ss')"),
                         F.expr("try_to_timestamp(paid_at, 'dd/MM/yyyy HH:mm')"))
    return (bronze.dropna(subset=["payment_id"])
                  .select("payment_id", "order_id", method.alias("method"), amount.alias("amount"),
                          F.upper(F.trim("currency")).alias("currency"), paid_at.alias("paid_at"),
                          F.upper(F.trim("status")).alias("status"),
                          F.to_timestamp("updated_at").alias("updated_at"))
                  .fillna({"currency": "USD"}))


def latest_per_key(df, key, order_col):
    w = Window.partitionBy(key).orderBy(F.col(order_col).desc())
    return df.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn")


def silver_payments_df():
    return latest_per_key(clean_payments_df(spark.table("lab07_bronze_payments")), "payment_id", "updated_at")


def build_silver_tables(force: bool = False):
    """Create the lab07_silver_* tables with the reference logic if they don't exist (or force=True)."""
    builders = {"lab07_silver_customers": silver_customers_df, "lab07_silver_products": silver_products_df,
                "lab07_silver_orders": lambda: silver_orders_df().drop("items"),
                "lab07_silver_order_items": silver_order_items_df, "lab07_silver_payments": silver_payments_df}
    created = []
    for name, fn in builders.items():
        if force or not spark.catalog.tableExists(name):
            fn().write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(name)
            created.append(name)
    if created:
        print("🥈 built reference silver tables:", ", ".join(created))
    return created

# COMMAND ----------

# DBTITLE 1,Helpers
def run_sql(stmt: str):
    """Print a SQL statement, run it and return the DataFrame."""
    stmt = textwrap.dedent(stmt).strip()
    print("▶ " + stmt.replace("\n", "\n  "))
    return spark.sql(stmt)


def null_counts(df):
    """One row: number of NULLs per column."""
    return df.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in df.columns])


def plan_text(df) -> str:
    """Physical plan as text via SQL EXPLAIN (works on serverless / Spark Connect)."""
    df.createOrReplaceTempView("_lab07_plan_v")
    return spark.sql("EXPLAIN SELECT * FROM _lab07_plan_v").first()[0]


def join_strategy(plan: str) -> str:
    if "BroadcastHashJoin" in plan or "BroadcastNestedLoopJoin" in plan:
        return "broadcast"
    if "SortMergeJoin" in plan or "ShuffledHashJoin" in plan:
        return "shuffle"
    return "none"


def time_it(label: str, fn):
    """Run fn() and print how long it took (wall-clock seconds). Returns (result, seconds)."""
    start = time.perf_counter()
    result = fn()
    seconds = round(time.perf_counter() - start, 2)
    print(f"⏱️ {label}: {seconds} s")
    return result, seconds


def reset_lab07(keep_bronze: bool = True):
    """Drop the lab07 tables, views and functions (bronze + targets are kept unless keep_bronze=False)."""
    keep = ("lab07_bronze_", "lab07_country_targets") if keep_bronze else ()
    for r in spark.sql("SHOW TABLES LIKE 'lab07*'").collect():
        name = r["tableName"]
        if r["isTemporary"] or name.startswith(keep):
            continue
        for kind in ("MATERIALIZED VIEW", "VIEW", "TABLE"):
            try:
                spark.sql(f"DROP {kind} IF EXISTS {name}")
                print("dropped", name)
                break
            except Exception:
                continue
        else:
            print("⚠️ could not drop", name)
    spark.sql("DROP FUNCTION IF EXISTS lab07_clean_method")


print("🔧 Helpers ready: run_sql(), null_counts(), plan_text(), join_strategy(), time_it(), build_silver_tables(), "
      "reset_lab07()")
