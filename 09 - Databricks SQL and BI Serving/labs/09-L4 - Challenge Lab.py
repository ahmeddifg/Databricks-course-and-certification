# Databricks notebook source
# MAGIC %md
# MAGIC # 🏆 Lab 09-L4 · Challenge Lab — A BI Pack for the Category Managers
# MAGIC **Time:** ~60 min · **Compute:** Serverless notebook + your **SQL warehouse** · **Story:** ShopWave's category managers want
# MAGIC their own corner of the lakehouse: a monthly gold view, a "top products" object that is fast to read, a parameterized
# MAGIC query they can reuse, a dashboard, and an alert when the smallest category falls below target.
# MAGIC
# MAGIC Replace every `None` / `# TODO`, do the 🖱️ UI tasks, then run each **✅ Check**. All numbers are computed from the data — no
# MAGIC hard-coding.
# MAGIC
# MAGIC | Task | Skill |
# MAGIC |---|---|
# MAGIC | 1 | Gold **view** with a window function (revenue share per month) |
# MAGIC | 2 | Gold **materialized view** (top 3 products per category) |
# MAGIC | 3 | A **parameterized** query on the SQL warehouse (REST API) |
# MAGIC | 4 | 🖱️ An **AI/BI dashboard** with a counter, a bar chart, a line chart and a filter — **published** |
# MAGIC | 5 | 🖱️ A **SQL alert** with a *less than* condition |
# MAGIC | 6–9 | 🧠 Databricks SQL & BI concepts |

# COMMAND ----------

# MAGIC %run ../../Includes/_setup

# COMMAND ----------

# MAGIC %run ./_09_prepare

# COMMAND ----------

# DBTITLE 1,Setup for the challenge (run me - don't edit)
reset_sales(0)                                            # January-June 2026
for _stmt in ("DROP VIEW IF EXISTS ch09_category_monthly", "DROP MATERIALIZED VIEW IF EXISTS ch09_mv_top_products"):
    try:
        spark.sql(_stmt)
    except Exception as e:
        print("note:", _first_line(e))
DASH4, ALERT4 = "09-L4 Category Performance", "09-L4 Books revenue drop"
answer_task3 = answer_task6 = answer_task7 = answer_task8 = answer_task9 = None


def check(label, condition):
    print(("✅ " if condition else "❌ ") + label)
    return bool(condition)


def _expected_monthly():
    return spark.sql("""
        SELECT month, category, orders, units, revenue,
               round(revenue / sum(revenue) OVER (PARTITION BY month), 4) AS revenue_share
        FROM (SELECT CAST(date_trunc('MONTH', order_date) AS DATE) AS month, category,
                     count(DISTINCT order_id) AS orders, sum(quantity) AS units, round(sum(subtotal), 2) AS revenue
              FROM bi_order_items GROUP BY 1, 2)""")


def _expected_top():
    return spark.sql("""
        SELECT category, product, revenue, rank FROM (
          SELECT i.category, p.title AS product, round(sum(i.subtotal), 2) AS revenue,
                 row_number() OVER (PARTITION BY i.category ORDER BY sum(i.subtotal) DESC) AS rank
          FROM bi_order_items i JOIN bi_products p USING (product_id)
          GROUP BY i.category, p.title)
        WHERE rank <= 3""")


print("tables:", ", ".join(bi_table(t) for t in ("bi_order_items", "bi_products")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 1 · Monthly category view
# MAGIC Create the **view** `ch09_category_monthly` on `bi_order_items` with one row per **month × category** and the columns:
# MAGIC
# MAGIC | Column | Definition |
# MAGIC |---|---|
# MAGIC | `month` | first day of the month, type **DATE** (hint: `CAST(date_trunc('MONTH', order_date) AS DATE)`) |
# MAGIC | `category` | |
# MAGIC | `orders` | number of **distinct** orders |
# MAGIC | `units` | sum of `quantity` |
# MAGIC | `revenue` | sum of `subtotal`, rounded to 2 decimals |
# MAGIC | `revenue_share` | `revenue` ÷ total revenue of the **same month**, rounded to 4 decimals (window function) |

# COMMAND ----------

# DBTITLE 1,Task 1
# MAGIC %sql
# MAGIC -- TODO: CREATE OR REPLACE VIEW ch09_category_monthly AS ...
# MAGIC SELECT 'replace me' AS todo

# COMMAND ----------

# DBTITLE 1,✅ Check Task 1
if spark.catalog.tableExists("ch09_category_monthly"):
    _got = spark.table("ch09_category_monthly")
    check("columns month, category, orders, units, revenue, revenue_share",
          _got.columns == ["month", "category", "orders", "units", "revenue", "revenue_share"])
    check("36 rows (6 months x 6 categories)", _got.count() == 36)
    check("values match the expected result", _got.exceptAll(_expected_monthly()).count() == 0
          and _expected_monthly().exceptAll(_got).count() == 0)
    check("shares of each month add up to 1",
          _got.groupBy("month").agg(F.sum("revenue_share").alias("s")).where("abs(s - 1) > 0.001").count() == 0)
else:
    check("view ch09_category_monthly exists", False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 2 · Top products — a materialized view
# MAGIC Category managers open the "top products" table many times a day; the data changes once a day. Create the **materialized
# MAGIC view** `ch09_mv_top_products` with the **top 3 products by revenue in each category**: columns `category`, `product`
# MAGIC (= `bi_products.title`), `revenue` (rounded to 2 decimals) and `rank` (1–3, `row_number()` by revenue descending).

# COMMAND ----------

# DBTITLE 1,Task 2
# MAGIC %sql
# MAGIC -- TODO: CREATE MATERIALIZED VIEW ch09_mv_top_products AS ...
# MAGIC SELECT 'replace me' AS todo

# COMMAND ----------

# DBTITLE 1,✅ Check Task 2
if spark.catalog.tableExists("ch09_mv_top_products"):
    _mv = spark.table("ch09_mv_top_products").select("category", "product", "revenue", "rank")
    _kind = {r["col_name"]: r["data_type"] for r in spark.sql("DESCRIBE TABLE EXTENDED ch09_mv_top_products").collect()}
    check("it is a MATERIALIZED VIEW", "MATERIALIZED" in str(_kind.get("Type", "")).upper())
    check("18 rows (3 per category)", _mv.count() == 18)
    check("same products, revenue and ranks as expected", _mv.exceptAll(_expected_top()).count() == 0)
else:
    check("materialized view ch09_mv_top_products exists", False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 3 · A parameterized query on the SQL warehouse
# MAGIC Write ONE statement that returns the revenue (`revenue`, rounded to 2 decimals) of category **`:category`** between
# MAGIC **`:start_date`** and **`:end_date`** from `bi_order_items` (use the fully qualified name: `bi_table('bi_order_items')`). Run it
# MAGIC with `sql_on_warehouse(statement, params)` for **Electronics** in **Q2 2026** (1 April – 30 June) and store the number.

# COMMAND ----------

# DBTITLE 1,Task 3
task3_sql = None        # TODO: an f-string with :category, :start_date and :end_date
task3_params = None     # TODO: {"category": ..., "start_date": _dt.date(...), "end_date": _dt.date(...)}
# TODO: run it on the warehouse and set answer_task3 = the revenue (a float)

# COMMAND ----------

# DBTITLE 1,✅ Check Task 3
_exp3 = spark.sql("""SELECT round(sum(subtotal), 2) FROM bi_order_items
                     WHERE category = 'Electronics' AND order_date BETWEEN '2026-04-01' AND '2026-06-30'""").first()[0]
check("the statement uses named parameters (no values glued into the SQL)",
      bool(task3_sql) and all(p in task3_sql for p in (":category", ":start_date", ":end_date"))
      and "Electronics" not in task3_sql)
check("Electronics revenue in Q2 2026 is correct", answer_task3 is not None and abs(float(answer_task3) - _exp3) < 0.01)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 4 · 🖱️ The category dashboard
# MAGIC Build an AI/BI dashboard named **`09-L4 Category Performance`**:
# MAGIC
# MAGIC * **Datasets (≥ 2):** the view `ch09_category_monthly` and the materialized view `ch09_mv_top_products`.
# MAGIC * **Widgets:** at least **one counter** (e.g. total revenue), **one bar chart** (revenue by category), **one line chart**
# MAGIC   (monthly revenue, colored by category) and **one table** (top products).
# MAGIC * **Filter:** a *multiple values* filter on `category` that filters **both** datasets.
# MAGIC * **Publish** it with **shared data permission** (embedded credentials).
# MAGIC
# MAGIC No step list this time — 09-L2 has them all. 💡 Which filter field do you need in each dataset for the category filter to
# MAGIC work on both?

# COMMAND ----------

# DBTITLE 1,✅ Check Task 4
_s4 = dashboard_summary(DASH4, verbose=False)
if _s4:
    _t4 = _s4["widget_types"]
    _sql4 = " ".join(_s4["dataset_sql"])
    check("≥ 2 datasets, using ch09_category_monthly and ch09_mv_top_products",
          _s4["datasets"] >= 2 and "ch09_category_monthly" in _sql4 and "ch09_mv_top_products" in _sql4)
    check("a counter, a bar chart, a line chart and a table",
          all(_t4.get(k, 0) >= 1 for k in ("counter", "bar", "line", "table")))
    check("a filter widget", _s4["filters"] >= 1)
    check("published with shared data permission", _s4["published"] and _s4["embed_credentials"] is True)
    dashboard_link(DASH4, published=True)
else:
    check(f"dashboard '{DASH4}' exists", False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 5 · 🖱️ An alert when Books falls below target
# MAGIC The target for **Books** is **10,000** revenue per month. Create the alert **`09-L4 Books revenue drop`**:
# MAGIC
# MAGIC * Query: the `revenue` of category `Books` in the **latest month** of `ch09_category_monthly` (one row, one column
# MAGIC   `revenue`).
# MAGIC * Condition: `revenue` **<** `10000`.
# MAGIC * A schedule (any), then **pause** it. Click **Run now** once.
# MAGIC
# MAGIC What state do you expect? Check the data before you click.

# COMMAND ----------

# DBTITLE 1,✅ Check Task 5
_a5 = find_alert(ALERT4)
_e5 = (_a5 or {}).get("evaluation") or {}
check(f"alert '{ALERT4}' exists", _a5 is not None)
check("condition: revenue LESS_THAN 10000",
      (_e5.get("source") or {}).get("name") == "revenue" and _e5.get("comparison_operator") == "LESS_THAN"
      and abs(float(((_e5.get("threshold") or {}).get("value") or {}).get("double_value") or 0) - 10000) < 0.001)
check("the query reads ch09_category_monthly", "ch09_category_monthly" in ((_a5 or {}).get("query_text") or ""))
check("it was run once and is TRIGGERED (June: Books below target)", _e5.get("state") == "TRIGGERED")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tasks 6–9 · 🧠 Concepts
# MAGIC Answer with `"A"`, `"B"`, `"C"` or `"D"`.
# MAGIC
# MAGIC **Task 6.** Every morning at 9:00, 40 category managers open dashboards at the same time. Each query alone takes 2 seconds,
# MAGIC but at 9:00 queries wait in a queue for a minute. The warehouse is a Small serverless warehouse with min = max = 1 cluster.
# MAGIC What fixes it most directly?
# MAGIC * A. Change the size to X-Large
# MAGIC * B. Increase the **maximum** number of clusters (scaling) so more queries run concurrently
# MAGIC * C. Increase auto stop from 10 to 60 minutes
# MAGIC * D. Switch to a Classic warehouse
# MAGIC
# MAGIC **Task 7.** The dashboard must show each regional manager **only the rows of their region**; the table has a **row filter**.
# MAGIC How must the dashboard be published?
# MAGIC * A. With individual data permission — each viewer's own Unity Catalog permissions (and row filter) apply
# MAGIC * B. With shared data permission (embedded credentials) — the publisher's rights apply
# MAGIC * C. It doesn't matter; row filters are always applied with the publisher's identity
# MAGIC * D. Row filters are not supported by dashboards
# MAGIC
# MAGIC **Task 8.** The dashboard must refresh **right after** the nightly pipeline finishes (which can take 20 to 90 minutes).
# MAGIC What's the best design?
# MAGIC * A. A dashboard schedule at 06:00
# MAGIC * B. A dashboard schedule every 15 minutes
# MAGIC * C. A Lakeflow Job: pipeline task → **dashboard task** (depends on the pipeline task)
# MAGIC * D. Ask the users to click Refresh
# MAGIC
# MAGIC **Task 9.** A Power BI report on top of `bi_sales_daily` must always show the latest data without a scheduled import, and
# MAGIC the SQL warehouse is always available during business hours. Which Power BI storage mode?
# MAGIC * A. Import
# MAGIC * B. Export to CSV every hour
# MAGIC * C. Dual for every table, scheduled hourly
# MAGIC * D. DirectQuery

# COMMAND ----------

# DBTITLE 1,Tasks 6-9
answer_task6 = None   # "A", "B", "C" or "D"
answer_task7 = None
answer_task8 = None
answer_task9 = None

# COMMAND ----------

# DBTITLE 1,✅ Check Tasks 6-9
import hashlib
_h = lambda n, v: hashlib.sha256(f"{n}:{str(v).strip().upper()}".encode()).hexdigest()[:10]
_key = {6: "65ae392122", 7: "0d3757a0a5", 8: "3733a81e96", 9: "a23997d9da"}
for _n, _a in ((6, answer_task6), (7, answer_task7), (8, answer_task8), (9, answer_task9)):
    check(f"Task {_n}", _a is not None and _h(_n, _a) == _key[_n])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🏁 Final score

# COMMAND ----------

# DBTITLE 1,Final score - re-runs every check quietly
_score = {
    "Task 1 view": spark.catalog.tableExists("ch09_category_monthly")
                   and spark.table("ch09_category_monthly").exceptAll(_expected_monthly()).count() == 0,
    "Task 2 MV": spark.catalog.tableExists("ch09_mv_top_products")
                 and spark.table("ch09_mv_top_products").select("category", "product", "revenue", "rank")
                          .exceptAll(_expected_top()).count() == 0,
    "Task 3 parameters": answer_task3 is not None and abs(float(answer_task3) - _exp3) < 0.01,
    "Task 4 dashboard": bool(_s4) and _s4["published"] and _s4["filters"] >= 1,
    "Task 5 alert": _e5.get("comparison_operator") == "LESS_THAN",
    "Tasks 6-9": all(a is not None and _h(n, a) == _key[n]
                     for n, a in ((6, answer_task6), (7, answer_task7), (8, answer_task8), (9, answer_task9))),
}
for k, v in _score.items():
    print(("✅ " if v else "❌ ") + k)
print(f"\nScore: {sum(_score.values())}/{len(_score)}" + ("  🏆 Section 09 complete!" if all(_score.values()) else ""))
