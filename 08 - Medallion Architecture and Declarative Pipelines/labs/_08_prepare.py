# Databricks notebook source
# MAGIC %md
# MAGIC # 🔧 _08_prepare — Section 08 feeds, stream helpers & pipeline helpers
# MAGIC Included by every Section 08 lab with `%run ./_08_prepare` (after `%run ../../Includes/_setup`). Idempotent: it only
# MAGIC creates what is missing.
# MAGIC
# MAGIC **New source folders** in the raw volume (generated once, deterministic):
# MAGIC
# MAGIC | Staging folder | Content | Used in |
# MAGIC |---|---|---|
# MAGIC | `orders-staging/` (from `_setup`) | 10 JSON files × 121 orders (per file: 1 cancelled, 1 `C9999`, 1 exact duplicate) | 08-L1, 08-L2 |
# MAGIC | `customers-cdc-staging/` | 3 JSON files of **change events** for customers: `01` = 300 `INSERT`s · `02` = 30 `UPDATE`s, 3 `DELETE`s, 5 new `INSERT`s, 2 invalid (`MERGE`) · `03` = 16 `UPDATE`s + 4 **late** (out-of-order) `UPDATE`s | 08-L3 AUTO CDC |
# MAGIC | `ratings-staging/` | 3 JSON files of product ratings (60 per file + problems: invalid ratings, missing / unknown products, empty comments, 3 re-sent ratings in file 2, a **new column** `helpful_votes` in file 3) | 08-L4 challenge |
# MAGIC
# MAGIC **Landing folders** (what streams and pipelines read) live under `…/raw/lab08/` and are filled **one file at a time**:
# MAGIC
# MAGIC | Feed | Landing folder | Deliver files with |
# MAGIC |---|---|---|
# MAGIC | `hc_orders` | `lab08/hc-orders/` | `land("hc_orders")` · 08-L1 (hand-coded medallion) |
# MAGIC | `sdp_orders` | `lab08/orders/` | `land("sdp_orders")` · 08-L2 (SQL pipeline) |
# MAGIC | `cdc` | `lab08/customers-cdc/` | `land("cdc")` · 08-L3 (Python pipeline) |
# MAGIC | `ratings` | `lab08/ratings/` | `land("ratings")` · 08-L4 (challenge) |
# MAGIC
# MAGIC **Helpers:** `land(feed, n=1, all=False)`, `land_until(feed, file)` (re-run safe), `landed(feed)`, `landing(feed)`,
# MAGIC `reset_feed(feed)`, `checkpoint(name)`, `run_stream(writer, table)`, `stop_all_streams()`,
# MAGIC `pipeline_source(folder)`, `create_or_update_pipeline(key)`, `run_pipeline(key, full_refresh=False)`, `find_pipeline(key)`,
# MAGIC `pipeline_link(key)`, `show_pipeline_errors(key)`, `delete_pipeline(key)`, `expectation_metrics(table)`, `table_count(t)`,
# MAGIC `reset_lab08(confirm="YES")`.

# COMMAND ----------

# DBTITLE 1,Generate Section 08 source files (first run only)
import json
import os
import random
import time
import datetime as _dt
from pyspark.sql import functions as F

_BASE08 = _dt.datetime(2026, 7, 1, 8, 0, 0)
_COMMENTS = ["Great value", "Works as described", "Arrived late", "Would buy again", "Not worth the price",
             "Excellent quality", "Okay", "Broke after a week", "Exactly what I needed", "Too small"]


def _iso08(d):
    return d.strftime("%Y-%m-%dT%H:%M:%SZ")


def _cdc_batches(customers):
    """customers: list of dicts (customer_id, email, profile JSON string) sorted by customer_id.
    Returns [(file_name, [events])] - change events with operation + sequence_num."""
    rng = random.Random(808)
    countries = sorted(_COUNTRIES)

    def person(c):
        p = json.loads(c["profile"])
        return {"customer_id": c["customer_id"], "email": c["email"], "first_name": p["first_name"],
                "last_name": p["last_name"], "city": p["address"]["city"], "country": p["address"]["country"]}

    def event(row, op, seq):
        e = dict(row)
        e.update(operation=op, sequence_num=seq, change_ts=_iso08(_BASE08 + _dt.timedelta(minutes=seq)))
        return e

    def changed(row, seq):                       # a new version: new e-mail, sometimes a move to a new city/country
        r = dict(row)
        r["email"] = f"{r['first_name']}.{r['last_name']}.{seq}@newmail.com".lower()
        if rng.random() < 0.5:
            country = rng.choice(countries)
            r["country"], r["city"] = country, rng.choice(_COUNTRIES[country])
        return r

    rows = {c["customer_id"]: person(c) for c in customers}
    ids = sorted(rows)
    b1 = [event(rows[cid], "INSERT", n) for n, cid in enumerate(ids, 1)]            # seq 1..300

    upd2 = sorted(rng.sample(ids, 30))
    del2 = sorted(rng.sample([i for i in ids if i not in upd2], 3))
    b2, seq_b2, seq = [], {}, 1000
    for cid in upd2:
        seq += 10
        rows[cid] = changed(rows[cid], seq)
        seq_b2[cid] = seq
        b2.append(event(rows[cid], "UPDATE", seq))
    for cid in del2:
        seq += 10
        b2.append(event(rows[cid], "DELETE", seq))
    for k in range(1, 6):                                                          # 5 brand-new customers
        seq += 10
        first, last = rng.choice(_FIRST), rng.choice(_LAST)
        country = rng.choice(countries)
        new = {"customer_id": f"C{300 + k:04d}", "email": f"{first}.{last}{300 + k}@shopwave.org".lower(),
               "first_name": first, "last_name": last, "city": rng.choice(_COUNTRIES[country]), "country": country}
        rows[new["customer_id"]] = new
        b2.append(event(new, "INSERT", seq))
    for cid in rng.sample([i for i in ids if i not in upd2 and i not in del2], 2):  # invalid operation -> must be dropped
        seq += 10
        b2.append(event(changed(rows[cid], seq), "MERGE", seq))
    rng.shuffle(b2)                                                                # order inside a file doesn't matter

    late = sorted(rng.sample(upd2, 4))
    pool = sorted(i for i in ids if i not in del2 and i not in late)
    b3, seq = [], 2000
    for cid in sorted(rng.sample(pool, 16)):
        seq += 10
        rows[cid] = changed(rows[cid], seq)
        b3.append(event(rows[cid], "UPDATE", seq))
    for cid in late:                               # happened BEFORE the file-02 update of the same customer, arrives now
        s = seq_b2[cid] - 5
        b3.append(event(changed(person(next(c for c in customers if c["customer_id"] == cid)), s), "UPDATE", s))
    return [("customers_cdc_01.json", b1), ("customers_cdc_02.json", b2), ("customers_cdc_03.json", b3)]


def _rating_batches(product_ids):
    rng = random.Random(818)
    batches, rid, first = [], 1, []
    for b in range(1, 4):
        rows = []
        for k in range(60):
            r = {"rating_id": f"R{rid:05d}", "product_id": rng.choice(product_ids),
                 "customer_id": f"C{rng.randint(1, 300):04d}",
                 "rating": rng.choices([1, 2, 3, 4, 5], weights=[5, 8, 17, 35, 35])[0],
                 "comment": rng.choice(_COMMENTS),
                 "rated_at": _iso08(_BASE08 + _dt.timedelta(days=b - 1, minutes=11 * k)),
                 "verified": rng.random() < 0.8}
            if b == 3:
                r["helpful_votes"] = rng.randint(0, 25)                           # NEW column in file 3
            rows.append(r)
            rid += 1
        for r in rng.sample(rows, 3):
            r["rating"] = rng.choice([0, 6, 10])                                   # invalid ratings
        for r in rng.sample(rows, 2):
            r["product_id"] = None                                                 # missing product
        for r in rng.sample(rows, 1):
            r["product_id"] = "P999"                                               # unknown product
        for r in rng.sample(rows, 4):
            r["comment"] = ""                                                      # empty comment (warning only)
        if b == 1:
            first = [dict(r) for r in rows if 1 <= r["rating"] <= 5 and r["product_id"] not in (None, "P999")]
        if b == 2:
            rows += [dict(r) for r in rng.sample(first, 3)]                        # re-sent by the app (duplicates)
        batches.append((f"ratings_{b:02d}.json", rows))
    return batches


def _jsonl08(rows):
    return "\n".join(json.dumps(r) for r in rows) + "\n"


def _prepare_lab08_sources():
    created = []
    target = f"{dataset_path}/customers-cdc-staging"
    if not path_exists(target):
        customers = [r.asDict() for r in spark.read.json(f"{dataset_path}/customers-json")
                                              .select("customer_id", "email", "profile").orderBy("customer_id").collect()]
        for name, rows in _cdc_batches(customers):
            _write_text(f"{target}/{name}", _jsonl08(rows))
        created.append("customers-cdc-staging")
    target = f"{dataset_path}/ratings-staging"
    if not path_exists(target):
        pids = sorted(r["product_id"] for r in spark.read.option("header", True).option("sep", ";")
                                                 .csv(f"{dataset_path}/products-csv").select("product_id").collect())
        for name, rows in _rating_batches(pids):
            _write_text(f"{target}/{name}", _jsonl08(rows))
        created.append("ratings-staging")
    return created


_created08 = _prepare_lab08_sources()
print("✅ Section 08 sources ready" + (f" (generated: {', '.join(_created08)})" if _created08 else ""))

# COMMAND ----------

# DBTITLE 1,Feeds: deliver files one at a time
_FEEDS08 = {
    "hc_orders": ("orders-staging", "lab08/hc-orders"),
    "sdp_orders": ("orders-staging", "lab08/orders"),
    "cdc": ("customers-cdc-staging", "lab08/customers-cdc"),
    "ratings": ("ratings-staging", "lab08/ratings"),
}


def landing(feed: str) -> str:
    """Landing folder of a feed - this is the path streams and pipelines read."""
    return f"{dataset_path}/{_FEEDS08[feed][1]}"


def landed(feed: str) -> list:
    """File names already delivered to a feed's landing folder."""
    path = landing(feed)
    return sorted(f.name for f in dbutils.fs.ls(path) if not f.name.endswith("/")) if path_exists(path) else []


def land(feed: str, n: int = 1, all: bool = False) -> int:
    """Deliver the next file(s) of a feed from its staging folder to its landing folder."""
    staging = f"{dataset_path}/{_FEEDS08[feed][0]}"
    staged = sorted(f.name for f in dbutils.fs.ls(staging) if f.name.endswith(".json"))
    done = set(landed(feed))
    pending = [f for f in staged if f not in done]
    if not pending:
        print(f"Nothing left to land - every {feed} file is already in {_FEEDS08[feed][1]}/")
        return 0
    todo = pending if all else pending[:max(1, n)]
    for name in todo:
        dbutils.fs.cp(f"{staging}/{name}", f"{landing(feed)}/{name}")
        print(f"📦 {feed}: landed {name}")
    return len(todo)


def land_until(feed: str, file_name: str) -> int:
    """Re-run-safe delivery: land files of `feed` until `file_name` is in its landing folder."""
    delivered = 0
    while file_name not in landed(feed):
        if not land(feed, 1):
            break
        delivered += 1
    if delivered == 0:
        print(f"{file_name} is already in {_FEEDS08[feed][1]}/ - nothing new to land (safe to re-run)")
    return delivered


def reset_feed(feed: str):
    """Empty a feed's landing folder (streams/pipelines that already read it keep their data)."""
    if path_exists(landing(feed)):
        dbutils.fs.rm(landing(feed), True)
    dbutils.fs.mkdirs(landing(feed))

# COMMAND ----------

# DBTITLE 1,Streaming helpers (hand-coded medallion)
def checkpoint(name: str) -> str:
    """A dedicated checkpoint (and schema-location) folder per stream - never share one between streams."""
    return f"{checkpoint_path}/lab08/{name}"


def _rows_read(query):
    total = 0
    for p in (query.recentProgress or []):
        total += (p.get("numInputRows", 0) if isinstance(p, dict) else p.numInputRows) or 0
    return total


def run_stream(writer, table: str = None, label: str = ""):
    """Start a DataStreamWriter with trigger(availableNow=True), wait until it has processed everything
    available, print a one-line summary and return the finished query."""
    w = writer.trigger(availableNow=True)
    query = w.toTable(table) if table else w.start()
    query.awaitTermination()
    print(f"✅ {label or 'stream'}{' -> ' + table if table else ''}: {_rows_read(query)} input rows")
    return query


def stop_all_streams():
    for q in spark.streams.active:
        print("stopping", q.name or q.id)
        q.stop()


def table_count(t: str):
    """Row count of a table, or None if it doesn't exist (yet)."""
    return spark.table(t).count() if spark.catalog.tableExists(t) else None

# COMMAND ----------

# DBTITLE 1,Pipeline helpers (REST API through the Databricks SDK)
# The labs show every pipeline step in the UI. These helpers do the same through the Pipelines REST API, so you can
# (re)create and run the lab pipelines from a notebook cell - and so the checks can find "your" pipeline.
PIPELINES08 = {
    "L2": {"name": "08-L2 ShopWave orders (SQL)", "folder": "shopwave_sql"},
    "L3": {"name": "08-L3 ShopWave customers CDC (Python)", "folder": "shopwave_cdc_python"},
    "L4": {"name": "08-L4 Ratings challenge", "folder": "ratings_challenge"},
}
_ws08 = None


def _ws():
    global _ws08
    if _ws08 is None:
        from databricks.sdk import WorkspaceClient
        _ws08 = WorkspaceClient()
    return _ws08


def _api(method: str, path: str, body=None, query=None):
    return _ws().api_client.do(method, path, query=query, body=body) or {}


def _first_line(e) -> str:
    return (str(e).strip().splitlines() or [repr(e)])[0][:300]


def _notebook_path() -> str:
    try:
        return dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    except Exception:
        return dbutils.notebook.getContext().notebookPath        # newer runtimes


def _section_root():
    """Workspace path of this section's folder, derived from the notebook that is running.
    Override: set SECTION08_ROOT = '/Workspace/.../08 - Medallion Architecture and Declarative Pipelines' before %run."""
    if globals().get("SECTION08_ROOT"):
        return SECTION08_ROOT
    try:
        nb = _notebook_path()                                   # /Users/.../08 - .../labs/08-L2 - ...
    except Exception as e:
        raise RuntimeError("Could not detect this notebook's workspace path. In a cell before `%run ./_08_prepare` set "
                           "SECTION08_ROOT = '/Workspace/<path to>/08 - Medallion Architecture and Declarative Pipelines'"
                           ) from e
    root = nb.rsplit("/", 2)[0]                                 # strip "<notebook>" and "labs" (or "data")
    return root if root.startswith("/Workspace") else "/Workspace" + root


def pipeline_source(folder: str) -> str:
    """Workspace path of a pipeline source folder, e.g. pipeline_source('shopwave_sql')."""
    return f"{_section_root()}/labs/pipelines/{folder}"


def _spec(key: str, folder: str = None) -> dict:
    p = PIPELINES08[key]
    root = pipeline_source(folder or p["folder"])
    return {"name": p["name"], "catalog": catalog_name, "schema": schema_name,
            "serverless": True, "continuous": False, "development": True, "channel": "CURRENT",
            "root_path": root,
            "libraries": [{"glob": {"include": f"{root}/transformations/**"}}],
            "configuration": {"dataset_path": dataset_path}}


def find_pipeline(key: str):
    """(pipeline_id, state) of the lab pipeline created by you, or None."""
    name = PIPELINES08[key]["name"]
    me = spark.sql("SELECT current_user()").first()[0]
    res = _api("GET", "/api/2.0/pipelines", query={"filter": f"name LIKE '{name}'", "max_results": 100})
    mine = [s for s in res.get("statuses", []) if s.get("name") == name and s.get("creator_user_name") in (me, None)]
    return (mine[0]["pipeline_id"], mine[0].get("state")) if mine else None


def pipeline_link(key: str):
    found = find_pipeline(key)
    if not found:
        print(f"No pipeline named '{PIPELINES08[key]['name']}' yet.")
        return
    url = f"{_ws().config.host.rstrip('/')}/pipelines/{found[0]}"
    displayHTML(f'<a href="{url}" target="_blank">🔗 Open “{PIPELINES08[key]["name"]}” in the pipeline editor</a>')


def create_or_update_pipeline(key: str, folder: str = None) -> str:
    """Create the lab pipeline (serverless, triggered, default catalog/schema = the course schema,
    configuration dataset_path) - or point the existing one at `folder`. Returns the pipeline id."""
    spec = _spec(key, folder)
    found = find_pipeline(key)
    if found:
        _api("PUT", f"/api/2.0/pipelines/{found[0]}", body=dict(spec, id=found[0]))
        print(f"🔁 Updated pipeline '{spec['name']}' ({found[0]}) -> source {spec['root_path']}")
        return found[0]
    try:
        pid = _api("POST", "/api/2.0/pipelines", body=spec)["pipeline_id"]
    except Exception as e:                                   # older workspaces: list the files instead of a glob
        print("glob libraries not accepted, retrying with file libraries:", _first_line(e))
        files = sorted(f for f in os.listdir(f"{spec['root_path']}/transformations") if f.endswith((".sql", ".py")))
        spec["libraries"] = [{"file": {"path": f"{spec['root_path']}/transformations/{f}"}} for f in files]
        pid = _api("POST", "/api/2.0/pipelines", body=spec)["pipeline_id"]
    print(f"🆕 Created pipeline '{spec['name']}' ({pid})")
    return pid


def show_pipeline_errors(key: str, n: int = 5):
    """Print the latest ERROR events of the pipeline (the same messages as in the UI)."""
    found = find_pipeline(key)
    if not found:
        return
    events = _api("GET", f"/api/2.0/pipelines/{found[0]}/events",
                  query={"max_results": 100, "filter": "level='ERROR'"}).get("events", [])
    errors = sorted(events, key=lambda e: e.get("timestamp", ""), reverse=True)[:n]
    for e in errors:
        print("❗", e.get("timestamp", ""), "|", (e.get("message") or "")[:400])
    if not errors:
        print("no ERROR events")


def run_pipeline(key: str, full_refresh: bool = False, refresh_selection=None, full_refresh_selection=None,
                 wait: bool = True, timeout_min: int = 30) -> str:
    """Start an update (like clicking 'Run pipeline') and - by default - wait for it. Returns the final state."""
    found = find_pipeline(key)
    if not found:
        raise RuntimeError(f"Create the pipeline first (UI steps in the lab, or create_or_update_pipeline('{key}')).")
    pid = found[0]
    t0 = time.time()
    while _api("GET", f"/api/2.0/pipelines/{pid}").get("state") == "RUNNING":   # an update is still active
        if time.time() - t0 > 600:
            raise RuntimeError("The pipeline is still running another update - stop it in the UI first.")
        time.sleep(10)
    body = {"full_refresh": full_refresh, "development": True}   # like a run from the editor: no automatic retries
    if refresh_selection:
        body["refresh_selection"] = list(refresh_selection)
    if full_refresh_selection:
        body["full_refresh_selection"] = list(full_refresh_selection)
    uid = _api("POST", f"/api/2.0/pipelines/{pid}/updates", body=body)["update_id"]
    kind = "full refresh" if full_refresh else ("selective refresh" if refresh_selection or full_refresh_selection else "refresh")
    print(f"▶️ {PIPELINES08[key]['name']}: {kind} started (update {uid[:8]}…)")
    if not wait:
        return "STARTED"
    last, state = None, None
    while time.time() - t0 < timeout_min * 60:
        state = _api("GET", f"/api/2.0/pipelines/{pid}/updates/{uid}").get("update", {}).get("state")
        if state != last:
            print(f"   {time.strftime('%H:%M:%S')}  {state}")
            last = state
        if state in ("COMPLETED", "FAILED", "CANCELED"):
            break
        time.sleep(10)
    print(("✅" if state == "COMPLETED" else "❌") + f" update finished: {state}")
    if state == "FAILED":
        show_pipeline_errors(key)
    return state


def delete_pipeline(key: str):
    """Delete the lab pipeline (its streaming tables and materialized views are dropped with it)."""
    found = find_pipeline(key)
    if not found:
        return
    _api("DELETE", f"/api/2.0/pipelines/{found[0]}")
    for _ in range(18):                                     # deletion (and dropping its tables) takes a moment
        try:
            if _api("GET", f"/api/2.0/pipelines/{found[0]}").get("state") in (None, "DELETED"):
                break
        except Exception:                                   # 404 -> gone
            break
        time.sleep(5)
    print(f"🗑️ deleted pipeline '{PIPELINES08[key]['name']}'")


def expectation_metrics(table: str):
    """Passed / failed records per expectation, from the event log of the pipeline that owns `table`."""
    return spark.sql(f"""
        SELECT e.dataset, e.name AS expectation,
               SUM(e.passed_records) AS passed_records, SUM(e.failed_records) AS failed_records
        FROM (SELECT explode(from_json(details:flow_progress:data_quality:expectations,
                     'array<struct<name: string, dataset: string, passed_records: bigint, failed_records: bigint>>')) AS e
              FROM event_log(TABLE({table}))
              WHERE event_type = 'flow_progress') AS x
        GROUP BY e.dataset, e.name
        ORDER BY e.dataset, e.name""")

# COMMAND ----------

# DBTITLE 1,Reset Section 08
def reset_lab08(confirm: str = ""):
    """Delete the Section 08 pipelines and their tables, drop lab08_* tables, delete lab08 checkpoints and
    empty every lab08 landing folder. Pass confirm='YES'."""
    if confirm != "YES":
        print("Nothing done. To really reset Section 08 call: reset_lab08(confirm='YES')")
        return
    stop_all_streams()
    for key in PIPELINES08:
        try:
            delete_pipeline(key)
        except Exception as e:
            print("could not delete pipeline", key, "-", _first_line(e))
    time.sleep(5)
    for r in spark.sql("SHOW TABLES LIKE 'lab08*|sdp_*|cdc_*|ch08_*'").collect():
        if r["isTemporary"]:
            continue
        for stmt in ("DROP TABLE IF EXISTS", "DROP MATERIALIZED VIEW IF EXISTS"):
            try:
                spark.sql(f"{stmt} {r['tableName']}")
                print("dropped", r["tableName"])
                break
            except Exception:
                pass
    if path_exists(f"{checkpoint_path}/lab08"):
        dbutils.fs.rm(f"{checkpoint_path}/lab08", True)
    if path_exists(f"{dataset_path}/lab08"):
        dbutils.fs.rm(f"{dataset_path}/lab08", True)
    print("🧹 Section 08 reset - landing folders are empty")


print("🔧 Section 08 helpers ready: land(), land_until(), landed(), checkpoint(), run_stream(), "
      "create_or_update_pipeline(), run_pipeline(), expectation_metrics(), reset_lab08()")
