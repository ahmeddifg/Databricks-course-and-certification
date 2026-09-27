# Databricks notebook source
# MAGIC %md
# MAGIC # 🔧 _05_prepare — Section 05 source files & ingestion helpers
# MAGIC Included by every Section 05 lab with `%run ./_05_prepare` (after `%run ../../Includes/_setup`).
# MAGIC
# MAGIC **Creates (only if missing) these source folders** in the raw volume `…/shopwave/raw/`:
# MAGIC
# MAGIC | Folder | Content | Used for |
# MAGIC |---|---|---|
# MAGIC | `returns-staging/` | 5 CSV batches × 40 returns (`,` delimited, header). Batches 4–5 add a column **`refund_method`**. Also a `manifest.txt` | `read_files`, `pathGlobFilter`, `COPY INTO` + schema evolution |
# MAGIC | `returns-landing/` | where `land_returns()` "delivers" the batches one by one | incremental loading with `COPY INTO` |
# MAGIC | `returns-messy/` | 1 CSV, 12 rows, **3 rows with bad values** (`n/a`, `12,50`, `31/07/2026`) | schema hints & rescued data |
# MAGIC | `reviews-json/` | 2 JSON-lines files, 60 nested product reviews (objects, arrays, optional fields) | semi-structured data, `VARIANT` |
# MAGIC | `reviews-multiline/` | 1 pretty-printed JSON **array** with 5 reviews | `multiLine` option |
# MAGIC | `app-logs/` | 2 text log files × 50 lines | `text` format |
# MAGIC | `product-images/` | 6 small PNG images | `binaryFile` format (unstructured data) |
# MAGIC | `supplier-staging/` / `supplier-landing/` | 3 JSON batches × 12 supplier prices; batch 3 adds **`discount`** | 05-L3 challenge |
# MAGIC
# MAGIC **Helpers:** `run_sql(stmt)`, `land_returns(n=1, all=False)`, `reset_returns_landing()`, `land_supplier_batch(n=1)`,
# MAGIC `reset_supplier_landing()`, `copy_into_metrics(df)`, `reset_lab05()`.

# COMMAND ----------

# DBTITLE 1,Generate Section 05 source files (idempotent)
import json
import random
import struct
import zlib
import textwrap
import datetime as _dt
from pyspark.sql import functions as F

_REASONS = ["damaged", "wrong size", "not as described", "changed mind", "late delivery", "defective"]
_METHODS = ["card", "wallet", "store_credit"]


def _png_bytes(rgb, size=16):
    """A valid, tiny, solid-colour PNG (no external libraries)."""
    def chunk(tag, data):
        return struct.pack(">I", len(data)) + tag + data + struct.pack(">I", zlib.crc32(tag + data) & 0xFFFFFFFF)
    raw = b"".join(b"\x00" + bytes(rgb) * size for _ in range(size))
    return (b"\x89PNG\r\n\x1a\n" + chunk(b"IHDR", struct.pack(">IIBBBBB", size, size, 8, 2, 0, 0, 0))
            + chunk(b"IDAT", zlib.compress(raw)) + chunk(b"IEND", b""))


def _returns_batches():
    rng = random.Random(505)
    batches, rid = [], 1
    for b in range(1, 6):
        header = ["return_id", "order_id", "product_id", "return_date", "reason", "refund_amount"]
        if b >= 4:
            header.append("refund_method")                      # schema change from batch 4 on
        lines = [",".join(header)]
        for _ in range(40):
            row = [f"R{rid:05d}", f"O{rng.randint(1, 2000):06d}", f"P{rng.randint(1, 36):03d}",
                   f"2026-07-{b:02d}", rng.choice(_REASONS), f"{rng.uniform(5, 400):.2f}"]
            if b >= 4:
                row.append(rng.choice(_METHODS))
            lines.append(",".join(row))
            rid += 1
        batches.append((f"returns_2026_07_{b:02d}.csv", "\n".join(lines) + "\n"))
    return batches


def _messy_returns():
    rng = random.Random(506)
    lines = ["return_id,order_id,product_id,return_date,reason,refund_amount"]
    bad = {3: ("refund_amount", "n/a"), 7: ("refund_amount", '"12,50"'), 10: ("return_date", "31/07/2026")}
    for i in range(1, 13):
        date, amount = "2026-07-31", f"{rng.uniform(5, 200):.2f}"
        if i in bad:
            col, val = bad[i]
            date, amount = (val, amount) if col == "return_date" else (date, val)
        lines.append(f"M{i:04d},O{rng.randint(1, 2000):06d},P{rng.randint(1, 36):03d},{date},{rng.choice(_REASONS)},{amount}")
    return "\n".join(lines) + "\n"


def _reviews(rng, start, n):
    titles = ["Great value", "Not bad", "Terrible", "Love it", "Works as expected", "Would buy again"]
    tags_pool = ["gift", "fast-shipping", "quality", "price", "packaging", "size", "battery"]
    out = []
    for i in range(start, start + n):
        rec = {"review_id": f"RV{i:04d}", "product_id": f"P{rng.randint(1, 36):03d}",
               "customer_id": f"C{rng.randint(1, 300):04d}", "rating": rng.randint(1, 5),
               "review": {"title": rng.choice(titles), "text": f"Review text number {i}", "language": rng.choice(["en", "ar", "fr"])},
               "tags": rng.sample(tags_pool, rng.randint(0, 3)),
               "submitted_at": f"2026-07-{rng.randint(1, 28):02d}T{rng.randint(0, 23):02d}:{rng.randint(0, 59):02d}:00Z",
               "meta": {"device": rng.choice(["ios", "android", "web"]), "app_version": rng.choice(["5.1.0", "5.2.1", "6.0.0"])}}
        if i % 3 == 0:
            rec["photos"] = [{"url": f"https://img.shopwave.example/{i}/{k}.jpg", "width": 1024, "height": 768}
                             for k in range(1, rng.randint(1, 3) + 1)]
        if i % 10 == 0:
            rec["verified_purchase"] = True
        out.append(rec)
    return out


def _supplier_batches():
    rng = random.Random(507)
    batches = []
    for b in range(1, 4):
        rows = []
        for k in range(12):
            rec = {"supplier_id": f"S{(k % 3) + 1:02d}", "product_id": f"P{(b - 1) * 12 + k + 1:03d}",
                   "price": round(rng.uniform(5, 450), 2), "currency": "USD",
                   "specs": {"weight_kg": round(rng.uniform(0.1, 12), 2), "color": rng.choice(["black", "white", "red", "blue"]),
                             "dims_cm": {"w": rng.randint(5, 80), "h": rng.randint(5, 80)}},
                   "updated_at": f"2026-08-{b:02d}T08:00:00Z"}
            if b == 3:
                rec["discount"] = rng.choice([0.0, 0.05, 0.1, 0.15])     # new field in batch 3
            rows.append(rec)
        batches.append((f"supplier_prices_{b:02d}.json", "\n".join(json.dumps(r) for r in rows) + "\n"))
    return batches


def _write_binary(path, data: bytes):
    dbutils.fs.mkdirs(path.rsplit("/", 1)[0])
    with open(path, "wb") as fh:          # volumes are real paths: plain Python can write files
        fh.write(data)


def _prepare_lab05_sources():
    created = []
    t = f"{dataset_path}/returns-staging"
    if not path_exists(t):
        for name, text in _returns_batches():
            _write_text(f"{t}/{name}", text)
        _write_text(f"{t}/manifest.txt", "ShopWave returns export - 5 daily CSV files\nowner: returns-team\n")
        created.append("returns-staging")
    t = f"{dataset_path}/returns-messy"
    if not path_exists(t):
        _write_text(f"{t}/returns_bad.csv", _messy_returns())
        created.append("returns-messy")
    t = f"{dataset_path}/reviews-json"
    if not path_exists(t):
        rng = random.Random(508)
        for part in range(2):
            _write_text(f"{t}/reviews_{part + 1:03d}.json", _jsonl(_reviews(rng, part * 30 + 1, 30)))
        created.append("reviews-json")
    t = f"{dataset_path}/reviews-multiline"
    if not path_exists(t):
        _write_text(f"{t}/reviews_pretty.json", json.dumps(_reviews(random.Random(509), 901, 5), indent=2) + "\n")
        created.append("reviews-multiline")
    t = f"{dataset_path}/app-logs"
    if not path_exists(t):
        rng = random.Random(510)
        for day in (1, 2):
            lines = []
            for k in range(50):
                level = rng.choices(["INFO", "WARN", "ERROR"], weights=[80, 15, 5])[0]
                event = rng.choice(["checkout", "login", "search", "add_to_cart"])
                lines.append(f"2026-07-{day:02d}T{9 + k // 10:02d}:{(k * 7) % 60:02d}:00Z {level} {event} "
                             f"order=O{rng.randint(1, 2000):06d} ms={rng.randint(20, 2500)}")
            _write_text(f"{t}/app_2026-07-{day:02d}.log", "\n".join(lines) + "\n")
        created.append("app-logs")
    t = f"{dataset_path}/product-images"
    if not path_exists(t):
        colors = [(220, 20, 60), (30, 144, 255), (46, 139, 87), (255, 165, 0), (128, 0, 128), (70, 70, 70)]
        for i, rgb in enumerate(colors, 1):
            _write_binary(f"{t}/P{i:03d}.png", _png_bytes(rgb))
        created.append("product-images")
    t = f"{dataset_path}/supplier-staging"
    if not path_exists(t):
        for name, text in _supplier_batches():
            _write_text(f"{t}/{name}", text)
        created.append("supplier-staging")
    return created


_created05 = _prepare_lab05_sources()
print("✅ Section 05 sources ready" + (f" (generated: {', '.join(_created05)})" if _created05 else ""))

# COMMAND ----------

# DBTITLE 1,Helpers
def run_sql(stmt: str):
    """Print a SQL statement (with the real paths/names), run it and return the DataFrame."""
    stmt = textwrap.dedent(stmt).strip()
    print("▶ " + stmt.replace("\n", "\n  "))
    return spark.sql(stmt)


def _land(staging, landing, n, all, pattern):
    staged = sorted(f.name for f in dbutils.fs.ls(staging) if f.name.endswith(pattern))
    landed = {f.name for f in dbutils.fs.ls(landing)} if path_exists(landing) else set()
    pending = [f for f in staged if f not in landed]
    if not pending:
        print(f"Nothing left to land - every file is already in {landing}/")
        return 0
    for name in (pending if all else pending[:max(1, n)]):
        dbutils.fs.cp(f"{staging}/{name}", f"{landing}/{name}")
        print(f"📦 Landed {name}")
    return len(pending if all else pending[:max(1, n)])


def land_returns(n: int = 1, all: bool = False) -> int:
    """Deliver the next returns CSV batch(es) from returns-staging/ to returns-landing/."""
    return _land(f"{dataset_path}/returns-staging", f"{dataset_path}/returns-landing", n, all, ".csv")


def reset_returns_landing():
    """Empty returns-landing/ and deliver batch 01 again."""
    landing = f"{dataset_path}/returns-landing"
    if path_exists(landing):
        dbutils.fs.rm(landing, True)
    dbutils.fs.mkdirs(landing)
    land_returns(1)


def land_supplier_batch(n: int = 1, all: bool = False) -> int:
    """Deliver the next supplier JSON batch(es) to supplier-landing/ (05-L3)."""
    return _land(f"{dataset_path}/supplier-staging", f"{dataset_path}/supplier-landing", n, all, ".json")


def reset_supplier_landing():
    landing = f"{dataset_path}/supplier-landing"
    if path_exists(landing):
        dbutils.fs.rm(landing, True)
    dbutils.fs.mkdirs(landing)
    land_supplier_batch(1)


def copy_into_metrics(df):
    """COPY INTO returns one row of metrics - turn it into a dict (e.g. num_inserted_rows)."""
    row = df.first()
    return {} if row is None else row.asDict()


def reset_lab05():
    """Drop every lab05_* table and reset the landing folders."""
    for t in [r["tableName"] for r in spark.sql("SHOW TABLES LIKE 'lab05*'").collect()]:
        spark.sql(f"DROP TABLE IF EXISTS {t}")
        print("dropped", t)
    reset_returns_landing()
    reset_supplier_landing()


print("🔧 Helpers ready: run_sql(), land_returns(), reset_returns_landing(), land_supplier_batch(), "
      "reset_supplier_landing(), copy_into_metrics(), reset_lab05()")
