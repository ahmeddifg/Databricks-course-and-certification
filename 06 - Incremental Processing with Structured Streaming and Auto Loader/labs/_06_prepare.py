# Databricks notebook source
# MAGIC %md
# MAGIC # 🔧 _06_prepare — Section 06 streaming sources & helpers
# MAGIC Included by every Section 06 lab with `%run ./_06_prepare` (after `%run ../../Includes/_setup`).
# MAGIC
# MAGIC **Source folders in the raw volume** (created only if missing — plus the course's `orders-staging` → `orders-landing` files):
# MAGIC
# MAGIC | Folder | Content | Used for |
# MAGIC |---|---|---|
# MAGIC | `orders-landing/` | JSON orders, one file per day (`01.json` … `10.json`, 121 rows each, incl. 1 duplicate) delivered by `land_new_orders()` | 06-L1, 06-L4 |
# MAGIC | `events-staging/` → `events-landing/` | 4 JSON-lines batches × 50 app events. Batch 3 adds a field **`coupon`**; batch 4 sends 3 **`amount`** values as text (`"12.50 EUR"`) | 06-L2 Auto Loader schema evolution & rescued data |
# MAGIC | `clicks-staging/` → `clicks-landing/` | 4 JSON-lines batches × 40 page clicks with **event time** (batch 1: 10:00–11:57, batch 2: 12:00–13:57, …). Batch 2 re-sends the last 5 clicks of batch 1 (duplicates); batch 4 also contains 4 **late** clicks from 10:10–10:13 | 06-L3 windows, watermarks, dedup |
# MAGIC
# MAGIC **Helpers:** `land_events(n)`, `reset_events_landing()`, `land_clicks(n)`, `reset_clicks_landing()`,
# MAGIC `land_until(feed, file_name)` (re-run-safe delivery), `checkpoint(name)`,
# MAGIC `run_stream(writer, table)` (start with `availableNow`, wait, print a summary), `data_batches(query)`, `progress_dict(p)`,
# MAGIC `stop_all_streams()`, `show_checkpoint(name)`, `reset_lab06()`.
# MAGIC
# MAGIC > ⚠️ **Serverless compute** only supports the `availableNow` (and deprecated `once`) triggers. Every lab stream here
# MAGIC > processes what is available and then **stops by itself** — nothing keeps running and costing money.

# COMMAND ----------

# DBTITLE 1,Generate Section 06 source files (idempotent)
import json
import random
import datetime as _dt
from pyspark.sql import functions as F

_BASE = _dt.datetime(2026, 7, 1, 10, 0, 0)


def _iso(d):
    return d.strftime("%Y-%m-%dT%H:%M:%SZ")


def _event_batches():
    rng = random.Random(606)
    types = ["page_view", "add_to_cart", "purchase", "search"]
    batches, eid = [], 1
    for b in range(1, 5):
        rows = []
        for k in range(50):
            et = rng.choices(types, weights=[50, 20, 15, 15])[0]
            rec = {"event_id": f"E{eid:05d}", "user_id": f"C{rng.randint(1, 300):04d}", "event_type": et,
                   "page": rng.choice(["/", "/search", "/p/P001", "/p/P014", "/cart", "/checkout"]),
                   "device": rng.choice(["ios", "android", "web"]),
                   "event_time": _iso(_BASE + _dt.timedelta(days=b - 1, minutes=k * 7)),
                   "amount": round(rng.uniform(10, 300), 2) if et == "purchase" else None}
            if b >= 3:
                rec["coupon"] = rng.choice(["SUMMER10", "WELCOME", None])       # NEW field from batch 3
            if b == 4 and k in (5, 17, 33):
                rec["event_type"], rec["amount"] = "purchase", "12.50 EUR"       # wrong type -> rescued data
            rows.append(rec)
            eid += 1
        batches.append((f"events_{b:02d}.json", "\n".join(json.dumps(r) for r in rows) + "\n"))
    return batches


def _click_batches():
    rng = random.Random(607)
    pages = ["/", "/search", "/p/P001", "/p/P014", "/cart"]
    batches, cid, first_batch = [], 1, []
    for b in range(1, 5):
        rows = []
        start = _BASE + _dt.timedelta(hours=2 * (b - 1))              # batch 1: 10:00-11:57, batch 2: 12:00-13:57, ...
        for k in range(40):
            rec = {"click_id": f"K{cid:05d}", "user_id": f"C{rng.randint(1, 50):04d}", "page": rng.choice(pages),
                   "event_time": _iso(start + _dt.timedelta(minutes=3 * k))}
            rows.append(rec)
            cid += 1
        if b == 1:
            first_batch = list(rows)
        if b == 2:
            rows += [dict(r) for r in first_batch[-5:]]                # 5 exact duplicates (11:45-11:57) re-sent by the app
        if b == 4:
            for m in range(4):                                         # 4 very late clicks (10:xx on the same day)
                rows.append({"click_id": f"K9{m:04d}", "user_id": "C0001", "page": "/",
                             "event_time": _iso(_BASE + _dt.timedelta(minutes=10 + m))})
        batches.append((f"clicks_{b:02d}.json", "\n".join(json.dumps(r) for r in rows) + "\n"))
    return batches


def _prepare_lab06_sources():
    created = []
    for folder, gen in (("events-staging", _event_batches), ("clicks-staging", _click_batches)):
        t = f"{dataset_path}/{folder}"
        if not path_exists(t):
            for name, text in gen():
                _write_text(f"{t}/{name}", text)
            created.append(folder)
    return created


_created06 = _prepare_lab06_sources()
print("✅ Section 06 sources ready" + (f" (generated: {', '.join(_created06)})" if _created06 else ""))

# COMMAND ----------

# DBTITLE 1,Helpers
def _land(staging, landing, n, all):
    staged = sorted(f.name for f in dbutils.fs.ls(staging) if f.name.endswith(".json"))
    landed = {f.name for f in dbutils.fs.ls(landing)} if path_exists(landing) else set()
    pending = [f for f in staged if f not in landed]
    if not pending:
        print(f"Nothing left to land - every file is already in {landing}/")
        return 0
    todo = pending if all else pending[:max(1, n)]
    for name in todo:
        dbutils.fs.cp(f"{staging}/{name}", f"{landing}/{name}")
        print(f"📦 Landed {name}")
    return len(todo)


def _reset_landing(landing, land_fn):
    if path_exists(landing):
        dbutils.fs.rm(landing, True)
    dbutils.fs.mkdirs(landing)
    land_fn(1)


def land_events(n: int = 1, all: bool = False) -> int:
    """Deliver the next app-event batch(es) to events-landing/."""
    return _land(f"{dataset_path}/events-staging", f"{dataset_path}/events-landing", n, all)


def reset_events_landing():
    _reset_landing(f"{dataset_path}/events-landing", land_events)


def land_clicks(n: int = 1, all: bool = False) -> int:
    """Deliver the next click batch(es) to clicks-landing/."""
    return _land(f"{dataset_path}/clicks-staging", f"{dataset_path}/clicks-landing", n, all)


def reset_clicks_landing():
    _reset_landing(f"{dataset_path}/clicks-landing", land_clicks)


def land_until(feed: str, file_name: str) -> int:
    """Re-run-safe delivery: land the next files of a feed ("orders", "events" or "clicks") until `file_name` is in
    its landing folder. Running the same cell again lands nothing, so the lab's numbers stay the same."""
    land = {"orders": land_new_orders, "events": land_events, "clicks": land_clicks}[feed]
    landing = f"{dataset_path}/{feed}-landing"
    landed = 0
    while not path_exists(f"{landing}/{file_name}"):
        if not land(1):
            break
        landed += 1
    if landed == 0:
        print(f"{file_name} is already in {feed}-landing/ - nothing new to land (safe to re-run)")
    return landed


def checkpoint(name: str) -> str:
    """A dedicated checkpoint folder per stream (never share one between streams!)."""
    return f"{checkpoint_path}/lab06/{name}"


def _progress_rows(query):
    progress = query.recentProgress or []
    return len(progress), sum((p.get("numInputRows", 0) if isinstance(p, dict) else p.numInputRows) for p in progress)


def progress_dict(p) -> dict:
    """lastProgress / recentProgress entries are dicts (or StreamingQueryProgress objects on newer runtimes)."""
    if p is None:
        return {}
    try:
        return json.loads(p.json) if hasattr(p, "json") and not isinstance(p.json, dict) else dict(p)
    except Exception:
        return {k: getattr(p, k, None) for k in ("id", "runId", "batchId", "numInputRows", "timestamp")}


def data_batches(query) -> int:
    """Number of micro-batches of a finished query that actually read rows."""
    return sum(1 for p in (query.recentProgress or [])
               if ((p.get("numInputRows", 0) if isinstance(p, dict) else p.numInputRows) or 0) > 0)


def run_stream(writer, table: str = None):
    """Start a DataStreamWriter with trigger(availableNow=True) - into `table` (toTable) or its configured sink -
    wait until it has processed everything available, print a summary and return the (finished) query."""
    w = writer.trigger(availableNow=True)
    query = w.toTable(table) if table else w.start()
    query.awaitTermination()
    batches, rows = _progress_rows(query)
    print(f"✅ stream{' -> ' + table if table else ''} finished: {batches} micro-batch(es), {rows} input rows")
    return query


def stop_all_streams():
    """Stop every active stream of this session (handy after experiments)."""
    for q in spark.streams.active:
        print("stopping", q.name or q.id)
        q.stop()
    print("active streams:", len(spark.streams.active))


def show_checkpoint(name: str):
    """List what Structured Streaming keeps in a checkpoint folder."""
    root = checkpoint(name)
    for f in dbutils.fs.ls(root):
        inner = [g.name for g in dbutils.fs.ls(f.path)] if f.name.endswith("/") else []
        print(f"{f.name:<14} {', '.join(inner[:8])}{' …' if len(inner) > 8 else ''}")


def reset_lab06():
    """Drop every lab06_* table, delete the lab06 checkpoints and reset all landing folders."""
    stop_all_streams()
    for t in [r["tableName"] for r in spark.sql("SHOW TABLES LIKE 'lab06*'").collect()]:
        spark.sql(f"DROP TABLE IF EXISTS {t}")
        print("dropped", t)
    if path_exists(f"{checkpoint_path}/lab06"):
        dbutils.fs.rm(f"{checkpoint_path}/lab06", True)
        print("deleted checkpoints under", f"{checkpoint_path}/lab06")
    reset_orders_landing()
    reset_events_landing()
    reset_clicks_landing()


print("🔧 Helpers ready: land_events(), land_clicks(), land_until(), checkpoint(), run_stream(), stop_all_streams(), "
      "show_checkpoint(), reset_lab06()")
