# Databricks notebook source
# MAGIC %md
# MAGIC # 🔧 _09_prepare — Section 09 gold tables, SQL warehouse & BI helpers
# MAGIC Included by every Section 09 lab with `%run ./_09_prepare` (after `%run ../../Includes/_setup`). Idempotent: it only
# MAGIC builds what is missing.
# MAGIC
# MAGIC **Gold tables for BI** (managed Delta tables in your course schema, built from the ShopWave base data):
# MAGIC
# MAGIC | Table | Grain | Rows (Jan–Jun 2026) | Built from |
# MAGIC |---|---|---|---|
# MAGIC | `bi_customers` | one row per customer | 300 | `customers-json` (JSON `profile` flattened) |
# MAGIC | `bi_products` | one row per product | 36 | `products-csv` |
# MAGIC | `bi_orders` | one row per order (deduplicated, cancelled orders removed, `country` = `Unknown` for unknown customers) | 1,980 | `orders-parquet` |
# MAGIC | `bi_order_items` | one row per order line (+ `category`, `brand`) | 5,004 | `bi_orders` exploded |
# MAGIC | `bi_sales_daily` | one row per day × country × category (`orders`, `units`, `revenue`) | 3,530 | `bi_order_items` |
# MAGIC
# MAGIC **New data arrives:** `load_sales_until(day)` adds the July orders (`orders-staging/01.json` = 1 July … `10.json` = 10 July,
# MAGIC 119 valid orders per day) and rebuilds the three sales tables — so you can watch dashboards, alerts and materialized views
# MAGIC react to fresh data. `reset_sales()` goes back to January–June.
# MAGIC
# MAGIC **Helpers:** `loaded_days()`, `load_sales_until(day)`, `reset_sales(days=0)`, `bi_table(name)` ·
# MAGIC `warehouse()`, `show_warehouses()`, `connection_details()`, `sql_on_warehouse(statement, params)` (Statement Execution REST
# MAGIC API) · `find_queries(prefix)`, `find_dashboard(name)`, `dashboard_summary(name)`, `dashboard_link(name)`, `find_alert(name)`,
# MAGIC `recent_queries()` · `reset_lab09(confirm="YES")`.

# COMMAND ----------

# DBTITLE 1,Gold tables for BI (built on the first run only)
import json
import time
import datetime as _dt
from collections import Counter
from pyspark.sql import functions as F

BI_TABLES = ("bi_customers", "bi_products", "bi_orders", "bi_order_items", "bi_sales_daily")
_JULY09 = [f"{d:02d}.json" for d in range(1, 11)]          # orders-staging: 01.json = 2026-07-01 ... 10.json = 2026-07-10
_PRODUCT_SCHEMA09 = "product_id STRING, title STRING, brand STRING, category STRING, price DOUBLE"


def bi_table(name: str) -> str:
    """Fully qualified name of a course table - paste it into the SQL editor / dashboards."""
    return f"{catalog_name}.{schema_name}.{name}"


def _bi_customers():
    p = "profile"
    return (spark.read.json(f"{dataset_path}/customers-json")
                 .select("customer_id",
                         F.get_json_object(p, "$.first_name").alias("first_name"),
                         F.get_json_object(p, "$.last_name").alias("last_name"),
                         "email",
                         F.get_json_object(p, "$.address.city").alias("city"),
                         F.get_json_object(p, "$.address.country").alias("country"))
                 .orderBy("customer_id"))


def _bi_products():
    return (spark.read.schema(_PRODUCT_SCHEMA09).option("header", True).option("sep", ";")
                 .csv(f"{dataset_path}/products-csv").orderBy("product_id"))


def _raw_orders09(days: int = 0):
    df = spark.read.schema(ORDER_SCHEMA).parquet(f"{dataset_path}/orders-parquet")
    if days:
        files = [f"{dataset_path}/orders-staging/{f}" for f in _JULY09[:days]]
        df = df.unionByName(spark.read.schema(ORDER_SCHEMA).json(files))
    return df


def _clean_orders09(raw):
    """Silver-style cleaning: dedup, drop cancelled orders, typed timestamp/date, country of the customer."""
    cust = spark.table("bi_customers").select("customer_id", "country")
    return (raw.dropDuplicates(["order_id"])
               .where("quantity > 0")
               .withColumn("order_ts", F.timestamp_seconds("order_timestamp"))
               .withColumn("order_date", F.to_date("order_ts"))
               .join(cust, "customer_id", "left")
               .withColumn("country", F.coalesce("country", F.lit("Unknown"))))


def _write09(df, name: str):
    """First time: create the table. Afterwards: INSERT OVERWRITE - the data is replaced but the table itself (comments,
    constraints, grants, history) is kept, like a production load."""
    if spark.catalog.tableExists(name):
        df.write.insertInto(name, overwrite=True)
    else:
        df.write.mode("overwrite").saveAsTable(name)


def _rebuild_daily09():
    _write09(spark.table("bi_order_items")
                  .groupBy("order_date", "country", "category")
                  .agg(F.countDistinct("order_id").alias("orders"),
                       F.sum("quantity").alias("units"),
                       F.round(F.sum("subtotal"), 2).alias("revenue")),
             "bi_sales_daily")


def reset_sales(days: int = 0, verbose: bool = True):
    """Rebuild bi_orders, bi_order_items and bi_sales_daily from January-June + the first `days` days of July."""
    days = max(0, min(int(days), len(_JULY09)))
    clean = _clean_orders09(_raw_orders09(days))
    _write09(clean.select("order_id", "order_ts", "order_date", "customer_id", "country",
                          F.size("items").alias("item_count"), "quantity", "total"), "bi_orders")
    prod = spark.table("bi_products").select("product_id", "category", "brand")
    _write09(spark.table("bi_orders").select("order_id", "order_date", "customer_id", "country")
                  .join(clean.select("order_id", F.explode("items").alias("i")), "order_id")
                  .select("order_id", "order_date", "customer_id", "country", F.col("i.product_id").alias("product_id"),
                          F.col("i.quantity").alias("quantity"), F.col("i.subtotal").alias("subtotal"))
                  .join(prod, "product_id", "left")
                  .select("order_id", "order_date", "customer_id", "country", "product_id", "category", "brand",
                          "quantity", "subtotal"),
             "bi_order_items")
    _rebuild_daily09()
    if verbose:
        last = spark.table("bi_orders").agg(F.max("order_date")).first()[0]
        print(f"📊 sales tables rebuilt: January-June{' + ' + str(days) + ' day(s) of July' if days else ''} "
              f"(latest order_date = {last})")


def loaded_days() -> int:
    """How many July days are in the sales tables (0 = only January-June)."""
    return spark.table("bi_orders").where("order_date >= '2026-07-01'").select("order_date").distinct().count()


def load_sales_until(day: int) -> int:
    """'New data arrives': make sure July 1 ... July <day> are loaded (re-run safe). Returns the days added."""
    have = loaded_days()
    if have >= day:
        print(f"July 1-{have} already loaded - nothing new (safe to re-run)")
        return 0
    reset_sales(day, verbose=False)
    added = f"July {day}" if day == have + 1 else f"July {have + 1}-{day}"
    print(f"📦 new data: {added} added -> latest order_date = 2026-07-{day:02d}")
    return day - have


def build_bi_tables(force: bool = False) -> bool:
    if force or not all(spark.catalog.tableExists(t) for t in BI_TABLES):
        _write09(_bi_customers(), "bi_customers")
        _write09(_bi_products(), "bi_products")
        reset_sales(0, verbose=False)
        return True
    return False


_built09 = build_bi_tables()
print(("✅ Section 09 gold tables built: " if _built09 else "✅ Section 09 gold tables ready: ")
      + ", ".join(f"{t} ({spark.table(t).count():,})" for t in BI_TABLES))

# COMMAND ----------

# DBTITLE 1,REST helpers + SQL warehouse helpers
# Everything you click in Databricks SQL is also a REST API. These helpers call it through the Databricks SDK client
# (authentication is automatic inside a notebook) - the labs use them to run SQL on a warehouse and to check your work.
_ws09 = None


def _ws():
    global _ws09
    if _ws09 is None:
        from databricks.sdk import WorkspaceClient
        _ws09 = WorkspaceClient()
    return _ws09


def _api(method: str, path: str, body=None, query=None):
    return _ws().api_client.do(method, path, query=query, body=body) or {}


def _first_line(e) -> str:
    return (str(e).strip().splitlines() or [repr(e)])[0][:300]


def _host() -> str:
    return _ws().config.host.rstrip("/")


def _paged(path: str, key: str, query=None, max_pages: int = 30) -> list:
    q, out = dict(query or {}), []
    for _ in range(max_pages):
        res = _api("GET", path, query=q)
        out += res.get(key) or []
        token = res.get("next_page_token")
        if not token:
            break
        q["page_token"] = token
    return out


def _wh_type(w: dict) -> str:
    if w.get("enable_serverless_compute"):
        return "SERVERLESS"
    return w.get("warehouse_type") or "CLASSIC"


def list_warehouses() -> list:
    return _api("GET", "/api/2.0/sql/warehouses").get("warehouses", [])


_WH09 = None


def warehouse(refresh: bool = False) -> dict:
    """The SQL warehouse the labs use. Choose one yourself with COURSE_WAREHOUSE = '<name or id>' before the %run."""
    global _WH09
    if _WH09 is not None and not refresh:
        return _WH09
    whs = list_warehouses()
    if not whs:
        raise RuntimeError("No SQL warehouse is visible to you. Create one (SQL Warehouses -> Create SQL warehouse) or ask "
                           "an admin for the CAN USE permission on one.")
    wanted = globals().get("COURSE_WAREHOUSE")
    if wanted:
        match = [w for w in whs if wanted in (w.get("id"), w.get("name"))]
        if not match:
            raise RuntimeError(f"COURSE_WAREHOUSE = {wanted!r} not found. Visible: {[w.get('name') for w in whs]}")
        _WH09 = match[0]
    else:
        def rank(w):
            return (w.get("state") != "RUNNING", _wh_type(w) != "SERVERLESS", w.get("name", ""))
        _WH09 = sorted(whs, key=rank)[0]
    return _WH09


def show_warehouses():
    """Your SQL warehouses and their most important settings (same values as the UI)."""
    rows = [(w.get("name"), w.get("id"), _wh_type(w), w.get("cluster_size"), w.get("state"),
             int(w.get("auto_stop_mins") or 0), int(w.get("min_num_clusters") or 1), int(w.get("max_num_clusters") or 1),
             bool(w.get("enable_photon", True)), w.get("creator_name")) for w in list_warehouses()]
    cols = "name STRING, id STRING, type STRING, size STRING, state STRING, auto_stop_mins INT, min_clusters INT, " \
           "max_clusters INT, photon BOOLEAN, creator STRING"
    display(spark.createDataFrame(rows, cols))


def connection_details(w: dict = None) -> dict:
    """Server hostname, port and HTTP path of the warehouse - what Power BI, Tableau, JDBC/ODBC clients need."""
    w = w or warehouse()
    odbc = w.get("odbc_params") or {}
    host = odbc.get("hostname") or _host().replace("https://", "")
    path = odbc.get("path") or f"/sql/1.0/warehouses/{w.get('id')}"
    port = odbc.get("port") or 443
    details = {
        "Server hostname": host,
        "Port": port,
        "HTTP path": path,
        "JDBC URL (token auth)": f"jdbc:databricks://{host}:{port}/default;transportMode=http;ssl=1;AuthMech=3;"
                                 f"httpPath={path};UID=token;PWD=<personal-access-token>",
    }
    for k, v in details.items():
        print(f"{k:<22}: {v}")
    return details


def warehouse_link(w: dict = None):
    w = w or warehouse()
    displayHTML(f'<a href="{_host()}/sql/warehouses/{w["id"]}" target="_blank">🔗 Open warehouse “{w["name"]}”</a>')

# COMMAND ----------

# DBTITLE 1,Statement Execution API - run SQL on the warehouse from Python
_PY2SQL = {bool: "BOOLEAN", int: "BIGINT", float: "DOUBLE", _dt.date: "DATE", _dt.datetime: "TIMESTAMP"}
_INT_TYPES = ("BYTE", "SHORT", "INT", "LONG")
_FLOAT_TYPES = ("FLOAT", "DOUBLE", "DECIMAL")


def _convert(value, type_name):
    if value is None:
        return None
    if type_name in _INT_TYPES:
        return int(value)
    if type_name in _FLOAT_TYPES:
        return float(value)
    if type_name == "BOOLEAN":
        return value in ("true", True)
    return value


def sql_on_warehouse(statement: str, params: dict = None, timeout_s: int = 600, verbose: bool = True):
    """Run ONE SQL statement on the SQL warehouse through the Statement Execution REST API
    (POST /api/2.0/sql/statements) - the same API BI tools and apps use - and return a pandas DataFrame.
    params: {"name": value} for named parameter markers (:name) in the statement."""
    import pandas as pd
    w = warehouse()
    body = {"warehouse_id": w["id"], "statement": statement, "catalog": catalog_name, "schema": schema_name,
            "wait_timeout": "30s", "on_wait_timeout": "CONTINUE", "format": "JSON_ARRAY", "disposition": "INLINE"}
    if params:
        body["parameters"] = [{"name": k, "value": None if v is None else str(v),
                               "type": _PY2SQL.get(type(v), "STRING")} for k, v in params.items()]
    t0 = time.time()
    res = _api("POST", "/api/2.0/sql/statements", body=body)
    sid = res.get("statement_id")
    while (res.get("status") or {}).get("state") in ("PENDING", "RUNNING"):
        if time.time() - t0 > timeout_s:
            _api("POST", f"/api/2.0/sql/statements/{sid}/cancel")
            raise TimeoutError(f"statement {sid} still running after {timeout_s}s - cancelled")
        time.sleep(2)
        res = _api("GET", f"/api/2.0/sql/statements/{sid}")
    status = res.get("status") or {}
    if status.get("state") != "SUCCEEDED":
        raise RuntimeError(f"statement {status.get('state')}: {(status.get('error') or {}).get('message')}")
    manifest = res.get("manifest") or {}
    columns = (manifest.get("schema") or {}).get("columns") or []
    rows = list((res.get("result") or {}).get("data_array") or [])
    for chunk in range(1, int(manifest.get("total_chunk_count") or 1)):
        rows += _api("GET", f"/api/2.0/sql/statements/{sid}/result/chunks/{chunk}").get("data_array") or []
    names = [c["name"] for c in columns]
    types = [c.get("type_name", "STRING") for c in columns]
    data = [[_convert(v, t) for v, t in zip(r, types)] for r in rows]
    if verbose:
        print(f"⚡ warehouse “{w['name']}”: {status.get('state')} in {time.time() - t0:.1f}s · {len(data)} row(s) · "
              f"statement_id {sid[:8]}…")
    return pd.DataFrame(data, columns=names)

# COMMAND ----------

# DBTITLE 1,Find your saved queries, dashboards and alerts (used by the checks)
def find_queries(prefix: str = "09-") -> list:
    """Saved SQL queries whose name starts with `prefix` (SQL editor -> Save)."""
    items = _paged("/api/2.0/sql/queries", "results", {"page_size": 100})
    return [q for q in items if (q.get("display_name") or "").startswith(prefix)
            and q.get("lifecycle_state", "ACTIVE") == "ACTIVE"]


def find_dashboard(name: str):
    """The AI/BI dashboard called `name` (newest first if there are several), or None."""
    items = _paged("/api/2.0/lakeview/dashboards", "dashboards", {"page_size": 100})
    match = [d for d in items if d.get("display_name") == name and d.get("lifecycle_state", "ACTIVE") == "ACTIVE"]
    match.sort(key=lambda d: d.get("create_time", ""), reverse=True)
    return match[0] if match else None


def dashboard_link(name: str, published: bool = False):
    d = find_dashboard(name)
    if not d:
        print(f"No dashboard called '{name}' yet.")
        return
    url = f"{_host()}/sql/dashboardsv3/{d['dashboard_id']}" + ("/published" if published else "")
    displayHTML(f'<a href="{url}" target="_blank">🔗 Open “{name}”{" (published)" if published else " (draft)"}</a>')


def dashboard_summary(name: str, verbose: bool = True):
    """What the dashboard contains: datasets, widget types, filters, published state, schedules."""
    d = find_dashboard(name)
    if not d:
        if verbose:
            print(f"❌ no dashboard called '{name}'")
        return None
    did = d["dashboard_id"]
    spec = json.loads(_api("GET", f"/api/2.0/lakeview/dashboards/{did}").get("serialized_dashboard") or "{}")
    widgets = [item["widget"] for page in spec.get("pages", []) for item in page.get("layout", []) if "widget" in item]
    types = Counter((w.get("spec") or {}).get("widgetType") or "text" for w in widgets)
    try:
        pub = _api("GET", f"/api/2.0/lakeview/dashboards/{did}/published")
    except Exception:                                       # 404 -> not published
        pub = {}
    try:
        schedules = _api("GET", f"/api/2.0/lakeview/dashboards/{did}/schedules").get("schedules", [])
    except Exception:
        schedules = []
    summary = {
        "id": did,
        "datasets": len(spec.get("datasets", [])),
        "dataset_sql": [" ".join(ds.get("queryLines") or [ds.get("asset_name") or ""]) for ds in spec.get("datasets", [])],
        "pages": len([p for p in spec.get("pages", []) if p.get("pageType") != "PAGE_TYPE_GLOBAL_FILTERS"]),
        "widget_types": dict(types),
        "filters": sum(n for t, n in types.items() if t.startswith("filter") or t == "range-slider"),
        "published": bool(pub),
        "embed_credentials": pub.get("embed_credentials"),
        "schedules": len(schedules),
    }
    if verbose:
        for k, v in summary.items():
            if k != "dataset_sql":
                print(f"{k:<18}: {v}")
    return summary


def find_alert(name: str):
    """The SQL alert called `name`, or None (new alerts API, falls back to the older one)."""
    for path, key in (("/api/2.0/alerts", "alerts"), ("/api/2.0/sql/alerts", "results")):
        try:
            items = _paged(path, key, {"page_size": 100})
        except Exception:
            continue
        match = [a for a in items if a.get("display_name") == name and a.get("lifecycle_state", "ACTIVE") == "ACTIVE"]
        if match:
            return match[0]
    return None


def _source(qs: dict) -> str:
    qs = qs or {}
    for key, label in (("dashboard_id", "dashboard"), ("alert_id", "alert"), ("job_info", "job"),
                       ("genie_space_id", "genie"), ("sql_query_id", "saved query"), ("notebook_id", "notebook")):
        if qs.get(key):
            return label
    return "editor / API"


def recent_queries(n: int = 15, contains: str = None, system_table: bool = False):
    """Your latest statements on the lab warehouse.
    Default: the Query History REST API (real time - what the Query History page shows).
    system_table=True: system.query.history (SQL-queryable, all compute types, arrives with some delay)."""
    wh = warehouse()["id"]
    if system_table:
        like = f"AND statement_text ILIKE '%{contains}%'" if contains else ""
        return spark.sql(f"""
            SELECT start_time, statement_type, execution_status AS status, total_duration_ms, from_result_cache,
                   client_application,
                   CASE WHEN query_source.dashboard_id IS NOT NULL THEN 'dashboard'
                        WHEN query_source.alert_id IS NOT NULL THEN 'alert'
                        WHEN query_source.job_info.job_id IS NOT NULL THEN 'job'
                        ELSE 'editor / API' END AS source,
                   left(statement_text, 100) AS statement
            FROM system.query.history
            WHERE compute.warehouse_id = '{wh}' AND executed_by = current_user()
              AND start_time > current_timestamp() - INTERVAL 1 DAY {like}
            ORDER BY start_time DESC LIMIT {int(n)}""")
    me = spark.sql("SELECT current_user()").first()[0]
    res = _api("GET", "/api/2.0/sql/history/queries",
               query={"filter_by.warehouse_ids": wh, "max_results": 100, "include_metrics": "true"}).get("res", [])
    rows = []
    for r in res:
        text = r.get("query_text") or ""
        if r.get("user_name") not in (me, None) or (contains and contains.lower() not in text.lower()):
            continue
        start = r.get("query_start_time_ms")
        rows.append((_dt.datetime.fromtimestamp(start / 1000, _dt.timezone.utc).replace(tzinfo=None) if start else None, r.get("statement_type"),
                     r.get("status"), r.get("duration"), bool((r.get("metrics") or {}).get("result_from_cache")),
                     r.get("client_application"), _source(r.get("query_source")), " ".join(text.split())[:100]))
    return spark.createDataFrame(rows[:n], "start_time TIMESTAMP, statement_type STRING, status STRING, "
                                           "duration_ms LONG, from_result_cache BOOLEAN, client_application STRING, "
                                           "source STRING, statement STRING")

# COMMAND ----------

# DBTITLE 1,Reset Section 09
def _trash(kind: str, prefix: str = "09-L"):
    if kind == "dashboards":
        for d in _paged("/api/2.0/lakeview/dashboards", "dashboards", {"page_size": 100}):
            if (d.get("display_name") or "").startswith(prefix):
                _api("DELETE", f"/api/2.0/lakeview/dashboards/{d['dashboard_id']}")
                print("🗑️ dashboard", d["display_name"])
    elif kind == "queries":
        for q in find_queries(prefix):
            _api("DELETE", f"/api/2.0/sql/queries/{q['id']}")
            print("🗑️ query", q["display_name"])
    elif kind == "alerts":
        for a in _paged("/api/2.0/alerts", "alerts", {"page_size": 100}):
            if (a.get("display_name") or "").startswith(prefix):
                _api("DELETE", f"/api/2.0/alerts/{a['id']}")
                print("🗑️ alert", a["display_name"])


def reset_lab09(confirm: str = ""):
    """Drop every Section 09 table/view and move your '09-L…' dashboards, queries and alerts to the trash."""
    if confirm != "YES":
        print("Nothing done. To really reset Section 09 call: reset_lab09(confirm='YES')")
        return
    for kind in ("alerts", "dashboards", "queries"):
        try:
            _trash(kind)
        except Exception as e:
            print(f"could not clean {kind}:", _first_line(e))
    try:                                                    # the 09-L3 foreign key would block dropping bi_customers
        spark.sql("ALTER TABLE bi_orders DROP CONSTRAINT IF EXISTS bi_orders_customer_fk")
    except Exception:
        pass
    for r in spark.sql("SHOW TABLES").collect():
        name = r["tableName"]
        if name.startswith(("bi_", "ch09_")) and not r["isTemporary"]:
            for stmt in ("DROP MATERIALIZED VIEW IF EXISTS", "DROP VIEW IF EXISTS", "DROP TABLE IF EXISTS"):
                try:
                    spark.sql(f"{stmt} {name}")
                    break
                except Exception:
                    pass
    print("✅ Section 09 reset - re-run %run ./_09_prepare to rebuild the gold tables")


print("🔧 Section 09 helpers loaded")
