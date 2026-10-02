# Databricks notebook source
# MAGIC %md
# MAGIC # 🧪 Lab 09-L1 · SQL Warehouses and the SQL Editor (Guided)
# MAGIC **Time:** ~50 min · **Compute:** Serverless notebook + a **SQL warehouse** · **Works on Free Edition**
# MAGIC
# MAGIC The gold layer is built — now the analysts want to use it. In this lab you meet the compute they use (**SQL warehouses**),
# MAGIC query gold tables from **Python through the REST API** (exactly how apps and BI tools talk to a warehouse), then switch to
# MAGIC the **SQL editor** to write, parameterize, visualize and save queries, and finally look behind the scenes with **query
# MAGIC history**, the **query profile** and the **result cache**.
# MAGIC
# MAGIC | Part | You will… |
# MAGIC |---|---|
# MAGIC | 1 | Meet the Section 09 **gold tables** |
# MAGIC | 2 | 🖱️ Tour your **SQL warehouse**: type, size, auto stop, scaling, permissions, connection details |
# MAGIC | 3 | Run SQL on the warehouse from Python with the **Statement Execution API** (+ named parameters) |
# MAGIC | 4 | 🖱️ **SQL editor**: run, visualize (bar chart) and **save** a query |
# MAGIC | 5 | 🖱️ A **parameterized** query (`:country`, `:start_date`, `:end_date`) + line chart |
# MAGIC | 6 | 🖱️ **Query history**, **query profile** and the **result cache** |
# MAGIC | 7 | ✅ Automatic checks |
# MAGIC
# MAGIC > 🆓 **Free Edition:** you get exactly **one** SQL warehouse (*Serverless Starter Warehouse*, size 2X-Small). That's all this
# MAGIC > section needs. It stops by itself when idle.

# COMMAND ----------

# MAGIC %run ../../Includes/_setup

# COMMAND ----------

# MAGIC %run ./_09_prepare

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1 · The gold tables you'll serve
# MAGIC `_09_prepare` built five **gold tables** from the ShopWave data (January–June 2026). They are ordinary Unity Catalog tables:
# MAGIC anything that can reach a SQL warehouse — the SQL editor, dashboards, alerts, Power BI, a Python app — can read them.

# COMMAND ----------

# DBTITLE 1,The Section 09 gold tables (fully qualified names - you'll paste them into the SQL editor)
for t in BI_TABLES:
    print(f"{bi_table(t):<45} {spark.table(t).count():>6,} rows   columns: {', '.join(spark.table(t).columns)}")
display(spark.table("bi_sales_daily").orderBy("order_date", "country", "category").limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC | Table | Grain | Typical BI question |
# MAGIC |---|---|---|
# MAGIC | `bi_orders` | one order | revenue, orders, average order value by country / day |
# MAGIC | `bi_order_items` | one order line | what sells? revenue by category, brand, product |
# MAGIC | `bi_sales_daily` | day × country × category | fast pre-aggregated trends for dashboards |
# MAGIC | `bi_customers`, `bi_products` | dimensions | names, cities, brands for slicing and labels |
# MAGIC
# MAGIC > 💡 Note the **grain**: `bi_sales_daily.orders` counts orders **per category**. An order with items from 3 categories is
# MAGIC > counted 3 times, so `SUM(orders)` over all categories (4,304) is **more** than the real number of orders (1,980). Use
# MAGIC > `bi_orders` for order counts. Mixing grains is the #1 source of wrong dashboard numbers.
# MAGIC
# MAGIC ## Part 2 · 🖱️ Tour your SQL warehouse
# MAGIC A **SQL warehouse** is compute made for SQL: Photon-powered, it serves the SQL editor, dashboards, alerts, Genie and every
# MAGIC JDBC/ODBC/REST client. First, what does the API say?

# COMMAND ----------

# DBTITLE 1,Your SQL warehouses (the one marked by warehouse() is used in this section)
show_warehouses()
w = warehouse()
print(f"\nThis section uses: “{w['name']}” ({_wh_type(w)}, {w.get('cluster_size')}, state {w.get('state')})")
warehouse_link()

# COMMAND ----------

# MAGIC %md
# MAGIC Now open it in the UI 🖱️ (click the link above, or left sidebar → **SQL Warehouses** — in some workspaces **Compute** →
# MAGIC **SQL warehouses** tab) and find each item:
# MAGIC
# MAGIC | Where | What to look at | Why it matters |
# MAGIC |---|---|---|
# MAGIC | Header / **Overview** | **Type** (Serverless / Pro / Classic), **state** (Running / Stopped / Starting) | serverless starts in seconds; pro & classic take minutes |
# MAGIC | **Edit** (or **⋮ → Edit**) | **Cluster size** (2X-Small … 4X-Large) | bigger = **faster queries** (lower latency), more DBUs per hour |
# MAGIC | | **Auto stop** (minutes idle) | serverless default 10 min — you pay nothing while it's stopped |
# MAGIC | | **Scaling: min / max clusters** | more clusters = **more concurrent queries** (roughly 1 cluster per 10 concurrent queries) |
# MAGIC | | **Advanced → Channel** (Current / Preview), **Tags** | tags flow into billing / system tables for cost tracking |
# MAGIC | **Connection details** tab | **Server hostname, Port, HTTP path**, JDBC URL | what Power BI / Tableau / JDBC / ODBC clients need (09-L3) |
# MAGIC | **Monitoring** tab | running & queued queries, cluster count over time | is it too small (queueing) or too big (idle)? |
# MAGIC | **Permissions** (button) | **Can use** · **Can monitor** · **Can manage** · **Is owner** (some workspaces also show **Can view**) | analysts need **Can use** on the warehouse **and** UC privileges on the data |
# MAGIC
# MAGIC > ⚠️ **Exam trap — size vs. scaling.** *Queries are slow even when only one user runs them* → increase the **cluster size**.
# MAGIC > *Queries are fast alone but queue at 9 a.m. when 50 analysts arrive* → raise **max clusters** (scale out).
# MAGIC
# MAGIC > 🆓 Free Edition: size and type are fixed (2X-Small serverless); you can still open **Edit** to see the settings. On a paid
# MAGIC > workspace, don't change a shared warehouse — just look.
# MAGIC
# MAGIC ## Part 3 · Run SQL on the warehouse from Python (REST client)
# MAGIC Notebooks normally run SQL on **their own** compute (serverless or a cluster). Apps, BI tools and services instead send SQL
# MAGIC to a **SQL warehouse** — over **JDBC/ODBC** or the **Statement Execution REST API** (`POST /api/2.0/sql/statements`).
# MAGIC The helper `sql_on_warehouse()` does exactly that (read it in `_09_prepare` — 40 lines: submit, poll until `SUCCEEDED`,
# MAGIC read the JSON result). The first call may take a few seconds while a stopped warehouse **auto-starts**.

# COMMAND ----------

# DBTITLE 1,Revenue by country - executed on the SQL warehouse, not on the notebook's compute
country_sql = f"""
SELECT country,
       count(*)              AS orders,
       round(sum(total), 2)  AS revenue,
       round(avg(total), 2)  AS avg_order_value
FROM {bi_table('bi_orders')}
GROUP BY country
ORDER BY revenue DESC"""
by_country = sql_on_warehouse(country_sql)
display(by_country)

# COMMAND ----------

# MAGIC %md
# MAGIC Brazil has the highest revenue, although Japan has **more** orders — Brazil's average order value is higher. The
# MAGIC `Unknown` row is the 5 orders of customer `C9999`, who isn't in the customer table (data quality you met in Section 07).
# MAGIC
# MAGIC **Named parameter markers.** Never glue user input into SQL strings (SQL injection!). Write `:name` in the statement and send
# MAGIC the values separately — the same syntax the SQL editor, dashboards and alerts use.

# COMMAND ----------

# DBTITLE 1,The same API with named parameters (:country, :start_date, :end_date)
daily_sql = f"""
SELECT order_date, count(*) AS orders, round(sum(total), 2) AS revenue
FROM {bi_table('bi_orders')}
WHERE country = :country
  AND order_date BETWEEN :start_date AND :end_date
GROUP BY order_date
ORDER BY order_date"""
params = {"country": "Saudi Arabia", "start_date": _dt.date(2026, 3, 1), "end_date": _dt.date(2026, 3, 31)}
daily_sa = sql_on_warehouse(daily_sql, params)
display(daily_sa)
print(f"{len(daily_sa)} days with orders · {daily_sa['orders'].sum()} orders · revenue {daily_sa['revenue'].sum():,.2f}")

# COMMAND ----------

# MAGIC %md
# MAGIC Expected: **18** days with orders, **29** orders, revenue **15,918.35**.
# MAGIC
# MAGIC > 🎯 Exam objective 2.5 — *“Use JDBC/ODBC or REST clients in notebooks, typically orchestrated with Lakeflow Jobs”*. You just
# MAGIC > did the REST variant. JDBC/ODBC drivers need the warehouse's **Server hostname + HTTP path** and a credential (OAuth or a
# MAGIC > personal access token) — 09-L3 prints them for you.
# MAGIC
# MAGIC ## Part 4 · 🖱️ The SQL editor: run, visualize, save
# MAGIC Run the next cell — it prints the query to paste (with **your** catalog and schema).

# COMMAND ----------

# DBTITLE 1,Query 1 to paste into the SQL editor
print(country_sql.strip())

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Left sidebar → **SQL Editor** (or **+ New → Query**). A new query tab opens.
# MAGIC 2. In the editor header, open the **compute selector** and pick your SQL warehouse (the one printed in Part 2). Optionally
# MAGIC    set the default **catalog** and **schema** next to it — then short table names would work too.
# MAGIC 3. Paste the query and click **Run** (`Ctrl`/`Cmd` + `Enter`). The result table appears below.
# MAGIC 4. Add a chart: in the results panel click **+** → **Visualization**.
# MAGIC    * **Visualization type:** `Bar`
# MAGIC    * **X column:** `country` · **Y columns:** `revenue` (aggregation *Sum* or *None* — the query already aggregated)
# MAGIC    * Click **Save**. The chart becomes a **tab** next to *Results*. Rename the tab: **Revenue by country**.
# MAGIC 5. Save the query: click **Save** (top right) → name **`09-L1 Revenue by country`** → keep the default folder (your home
# MAGIC    folder) → **Save**.
# MAGIC
# MAGIC > 💡 The editor also has: **Format query**, multiple **statements** in one tab (**Run all** shows one result per statement),
# MAGIC > **Genie Code** (✨, formerly *Databricks Assistant* — write, explain or fix SQL from a prompt), the **schema browser** on the left (drag table and column
# MAGIC > names in), query **snippets**, and **Download** (CSV/TSV/Excel) on the results.
# MAGIC
# MAGIC ## Part 5 · 🖱️ A parameterized query + line chart

# COMMAND ----------

# DBTITLE 1,Query 2 to paste into a NEW query tab
print(daily_sql.strip())

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Click **+** next to the query tabs (new query), select the same warehouse, paste the query.
# MAGIC 2. As soon as you type `:country`, `:start_date` and `:end_date`, **parameter widgets** appear under the editor.
# MAGIC 3. Click the ⚙️ next to each widget and set:
# MAGIC    * `country` → type **String**, value `Saudi Arabia`
# MAGIC    * `start_date` → type **Date**, value `2026-03-01` · `end_date` → type **Date**, value `2026-03-31`
# MAGIC 4. Click **Apply changes** (this re-runs the query with the new values). Expected: **18 rows**, the same numbers as Part 3.
# MAGIC 5. **+** → **Visualization** → type `Line`, **X column** `order_date`, **Y columns** `revenue` → **Save**.
# MAGIC 6. Try another value: `country` = `Japan` → **Apply changes**. One saved query, many answers.
# MAGIC 7. **Save** the query as **`09-L1 Daily revenue (parameters)`** — with `Saudi Arabia` and March as the saved defaults.
# MAGIC
# MAGIC > 🕰️ **Legacy syntax** you may still see in older queries and exam questions: `{{ country }}` (“mustache” parameters of the
# MAGIC > legacy editor). Today: `:country`. Both are **named parameters** — not string concatenation.
# MAGIC >
# MAGIC > 💡 A parameter can't stand for a table or column name directly: use `IDENTIFIER(:table_name)`. A *date range* parameter
# MAGIC > gives you `:range.min` and `:range.max`.
# MAGIC
# MAGIC ## Part 6 · 🖱️ Query history, query profile and the result cache
# MAGIC Every statement a warehouse runs is recorded — from the SQL editor, a dashboard, an alert, a job, a BI tool, or the REST API.
# MAGIC
# MAGIC 1. Left sidebar → **Query History**. Filter by **compute** = your warehouse and by your **user**.
# MAGIC 2. Find the `Revenue by country` statement. The list shows status, duration, rows and **source** (SQL editor, dashboard,
# MAGIC    API…). Click it → the details panel shows timings (queued / compiling / executing / fetching).
# MAGIC 3. Click **See query profile**: a tree/graph of **operators** (scan, filter, aggregate, exchange/shuffle, sort) with the
# MAGIC    time, rows and memory of each. It's how you find the slow step — Section 12 goes deeper.
# MAGIC
# MAGIC Now the **result cache**: run the same statement twice through the API.

# COMMAND ----------

# DBTITLE 1,Run the same statement twice - the second answer comes from the result cache
timings = []
for attempt in (1, 2):
    t0 = time.time()
    sql_on_warehouse(country_sql, verbose=False)
    timings.append(round(time.time() - t0, 2))
print("seconds per run:", timings)
time.sleep(5)
history = recent_queries(n=10, contains="avg_order_value")
display(history)

# COMMAND ----------

# MAGIC %md
# MAGIC Look at `from_result_cache`: the repeated run of an **identical** statement on **unchanged** tables was answered from the
# MAGIC **query result cache** — no data was read. (If the first run in this cell was already cached because of Part 3, both rows show
# MAGIC `true` — that's the point!) The result cache:
# MAGIC
# MAGIC * keeps results for **24 hours**; it is **invalidated** as soon as a table in the query changes;
# MAGIC * is not used for non-deterministic queries (e.g. `current_timestamp()`, `rand()`);
# MAGIC * is *local* (per warehouse cluster) and, on **serverless**, also *remote* (shared by all warehouses of the workspace,
# MAGIC   survives restarts);
# MAGIC * can be switched off for benchmarking: `SET use_cached_result = false`.
# MAGIC
# MAGIC > 💡 `recent_queries()` uses the **Query History REST API** (real time). For analysis across all compute, use the system
# MAGIC > table **`system.query.history`** (admins by default; it arrives with some delay):
# MAGIC > `recent_queries(system_table=True)` — try it later.
# MAGIC
# MAGIC ## Part 7 · ✅ Automatic checks

# COMMAND ----------

# DBTITLE 1,Check your work
def _query_by_name(name):
    found = [q for q in find_queries("09-L1") if q.get("display_name") == name]
    return found[0] if found else None


def _has_chart(q):
    try:
        vis = _paged(f"/api/2.0/sql/queries/{q['id']}/visualizations", "results")
        return any((v.get("type") or "").upper() != "TABLE" for v in vis)
    except Exception:
        return None


_q1 = _query_by_name("09-L1 Revenue by country")
_q2 = _query_by_name("09-L1 Daily revenue (parameters)")
_expected_country = {r["country"]: round(r["revenue"], 2) for r in spark.sql(
    "SELECT country, round(sum(total), 2) AS revenue FROM bi_orders GROUP BY country").collect()}
_expected_daily = (spark.table("bi_orders")
                        .where("country = 'Saudi Arabia' AND order_date BETWEEN '2026-03-01' AND '2026-03-31'")
                        .agg(F.countDistinct("order_date"), F.count("*")).first())
_checks = {
    "a SQL warehouse is available to you": bool(w.get("id")),
    "REST result = Spark result (revenue by country)":
        {r.country: round(r.revenue, 2) for r in by_country.itertuples()} == _expected_country,
    "parameterized REST call: 18 days / 29 orders": (len(daily_sa), int(daily_sa["orders"].sum()))
                                                   == (_expected_daily[0], _expected_daily[1]) == (18, 29),
    "query '09-L1 Revenue by country' saved": _q1 is not None,
    "query '09-L1 Daily revenue (parameters)' saved with :country, :start_date, :end_date":
        _q2 is not None and all(p in (_q2.get("query_text") or "") for p in (":country", ":start_date", ":end_date")),
}
for _q, _label in ((_q1, "bar chart on 'Revenue by country'"), (_q2, "line chart on 'Daily revenue (parameters)'")):
    if _q is not None:
        _ok = _has_chart(_q)
        if _ok is not None:
            _checks[_label] = _ok
for name, ok in _checks.items():
    print(("✅ " if ok else "❌ ") + name)
_cached = history.where("from_result_cache").count()
print(("✅ " if _cached else "ℹ️ ") + f"result cache: {_cached} cached run(s) in the history sample"
      + ("" if _cached else " (history can lag a few seconds - re-run this cell; not counted as a failure)"))
print("\n🎉 Lab complete - next: 09-L2 · Build an AI/BI dashboard")
