# Databricks notebook source
# MAGIC %md
# MAGIC # 🗂️ 09-D · Data used in Section 09
# MAGIC
# MAGIC Section 09 is about **serving** data, so it works on a small, clean **gold model** built by `labs/_09_prepare` from the
# MAGIC ShopWave base data (no new raw files). The model is a simple **star**: one fact at order grain, one at order-line grain,
# MAGIC two dimensions and one pre-aggregated table for dashboards.
# MAGIC
# MAGIC | Table | Grain | Rows (Jan–Jun) | Columns |
# MAGIC |---|---|---|---|
# MAGIC | `bi_customers` | customer | 300 | `customer_id`, `first_name`, `last_name`, `email`, `city`, `country` |
# MAGIC | `bi_products` | product | 36 | `product_id`, `title`, `brand`, `category`, `price` |
# MAGIC | `bi_orders` | order | 1,980 | `order_id`, `order_ts`, `order_date`, `customer_id`, `country`, `item_count`, `quantity`, `total` |
# MAGIC | `bi_order_items` | order line | 5,004 | `order_id`, `order_date`, `customer_id`, `country`, `product_id`, `category`, `brand`, `quantity`, `subtotal` |
# MAGIC | `bi_sales_daily` | day × country × category | 3,530 | `order_date`, `country`, `category`, `orders`, `units`, `revenue` |
# MAGIC
# MAGIC **How it is built** (see `_09_prepare`): `orders-parquet` (2,010 rows) → drop the 10 exact duplicates → drop the 20 cancelled
# MAGIC orders (`quantity = 0`) → `order_ts = timestamp_seconds(order_timestamp)` → left join `bi_customers` for `country` (the 5
# MAGIC orders of unknown customer `C9999` get `country = 'Unknown'`) → explode `items` for the order lines → aggregate per day,
# MAGIC country and category.
# MAGIC
# MAGIC **New data:** `load_sales_until(day)` adds 1 July … 10 July from `orders-staging/01.json … 10.json` (119 valid orders per
# MAGIC day, one of them from `C9999` each day). `reset_sales()` goes back to January–June.
# MAGIC
# MAGIC | Key figure (Jan–Jun) | Value |
# MAGIC |---|---|
# MAGIC | Revenue | 1,104,394.23 |
# MAGIC | Orders · average order value | 1,980 · 557.77 |
# MAGIC | Top country by revenue | Brazil (134,810.06 from 231 orders) — Japan has more orders (245) |
# MAGIC | Top category / product | Home & Kitchen (372,132.51) / Smart Watch (132,042.24) |
# MAGIC | Saudi Arabia, March | 18 days with orders · 29 orders · 15,918.35 |
# MAGIC | + 1 July · + 2 July | 2,099 orders / 1,165,110.84 · 2,218 orders |
# MAGIC
# MAGIC **Objects the labs create**
# MAGIC
# MAGIC | Lab | Object | Kind |
# MAGIC |---|---|---|
# MAGIC | 09-L1 | `09-L1 Revenue by country`, `09-L1 Daily revenue (parameters)` | saved queries (+ bar / line visualizations) |
# MAGIC | 09-L2 | `09-L2 ShopWave Sales` | AI/BI dashboard (3 datasets, published, schedule) |
# MAGIC | 09-L3 | `09-L3 Unknown customer orders` | SQL alert |
# MAGIC | 09-L3 | `bi_v_country_monthly` · `bi_mv_category_daily` · `bi_metrics_sales` | view · materialized view · metric view |
# MAGIC | 09-L3 | comments on `bi_orders` / `bi_customers`, PK `bi_customers_pk`, FK `bi_orders_customer_fk` | metadata |
# MAGIC | 09-L4 | `ch09_category_monthly` · `ch09_mv_top_products` · `09-L4 Category Performance` · `09-L4 Books revenue drop` | view · MV · dashboard · alert |

# COMMAND ----------

# MAGIC %run ../../Includes/_setup

# COMMAND ----------

# MAGIC %run ../labs/_09_prepare

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔍 Preview

# COMMAND ----------

# DBTITLE 1,Row counts, latest day and revenue
display(spark.sql("""
    SELECT 'bi_orders' AS table_name, count(*) AS rows, max(order_date) AS latest_day, round(sum(total), 2) AS revenue FROM bi_orders
    UNION ALL SELECT 'bi_order_items', count(*), max(order_date), round(sum(subtotal), 2) FROM bi_order_items
    UNION ALL SELECT 'bi_sales_daily', count(*), max(order_date), round(sum(revenue), 2) FROM bi_sales_daily"""))
print("July days loaded:", loaded_days())

# COMMAND ----------

# DBTITLE 1,The star schema in one query
display(spark.sql("""
    SELECT c.country, p.category, count(DISTINCT i.order_id) AS orders, round(sum(i.subtotal), 2) AS revenue
    FROM bi_order_items i
    JOIN bi_products  p USING (product_id)
    LEFT JOIN bi_customers c USING (customer_id)
    GROUP BY ALL
    ORDER BY revenue DESC
    LIMIT 10"""))

# COMMAND ----------

# DBTITLE 1,Your Section 09 objects in the workspace (SQL warehouse, queries, dashboards, alerts)
try:
    w = warehouse()
    print(f"SQL warehouse : {w['name']} ({_wh_type(w)}, {w.get('cluster_size')}, {w.get('state')})")
    print("queries       :", [q["display_name"] for q in find_queries("09-")] or "none")
    for _d in ("09-L2 ShopWave Sales", "09-L4 Category Performance"):
        print(f"dashboard     : {_d:<28} {'✅' if find_dashboard(_d) else '— not created'}")
    for _a in ("09-L3 Unknown customer orders", "09-L4 Books revenue drop"):
        _found = find_alert(_a)
        _state = ((_found.get("evaluation") or {}).get("state") or "not evaluated yet") if _found else "— not created"
        print(f"alert         : {_a:<32} {_state}")
except Exception as e:
    print("workspace APIs not reachable here:", _first_line(e))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧹 Reset Section 09
# MAGIC Uncomment to drop every `bi_*` and `ch09_*` table/view and move your `09-L…` dashboards, queries and alerts to the trash.
# MAGIC The next `%run ./_09_prepare` rebuilds the gold tables.

# COMMAND ----------

# DBTITLE 1,Reset (optional)
# reset_lab09(confirm="YES")
