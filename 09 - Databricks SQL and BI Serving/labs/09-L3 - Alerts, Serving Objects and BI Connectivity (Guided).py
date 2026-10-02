# Databricks notebook source
# MAGIC %md
# MAGIC # 🧪 Lab 09-L3 · Alerts, Serving Objects and BI Connectivity (Guided)
# MAGIC **Time:** ~60 min · **Compute:** Serverless notebook + your **SQL warehouse** · **Works on Free Edition**
# MAGIC
# MAGIC Three jobs of a data engineer who serves a BI team: **warn** people when the data says something is wrong (**alerts**),
# MAGIC choose the right **gold object** for each consumer (table, view, materialized view, metric view) and make it
# MAGIC **self-explaining** (comments, keys), and give external tools a **way in** (JDBC/ODBC, Partner Connect, Power BI) with the
# MAGIC right **permissions**.
# MAGIC
# MAGIC | Part | You will… |
# MAGIC |---|---|
# MAGIC | 1 | 🖱️ Create a **SQL alert** on a data-quality signal, test it, make it **trigger** with new data |
# MAGIC | 2 | Build **gold serving objects**: view vs **materialized view** — freshness, `REFRESH`, **schedules** |
# MAGIC | 3 | Make gold tables BI-friendly: **comments**, informational **primary / foreign keys**, a **metric view** |
# MAGIC | 4 | 🖱️ **Connect BI tools**: connection details, Partner Connect, Power BI / Tableau, a Python client |
# MAGIC | 5 | **Permissions** a BI consumer needs · who ran what: **query history** by source |
# MAGIC | 6 | ✅ Automatic checks |

# COMMAND ----------

# MAGIC %run ../../Includes/_setup

# COMMAND ----------

# MAGIC %run ./_09_prepare

# COMMAND ----------

# MAGIC %md
# MAGIC ### Start clean
# MAGIC Sales back to January–June (latest day **30 June**), and this lab's view / materialized view / metric view dropped. An
# MAGIC alert from an earlier attempt is kept — the checks use the alert named **`09-L3 Unknown customer orders`**.

# COMMAND ----------

# DBTITLE 1,Start clean (safe to re-run)
reset_sales(0)
for _stmt in ("DROP VIEW IF EXISTS bi_v_country_monthly", "DROP MATERIALIZED VIEW IF EXISTS bi_mv_category_daily",
              "DROP VIEW IF EXISTS bi_metrics_sales"):
    try:
        spark.sql(_stmt)
    except Exception as e:
        print("note:", _first_line(e))
ALERT = "09-L3 Unknown customer orders"
print("existing alert:", "yes" if find_alert(ALERT) else "none yet")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1 · 🖱️ A SQL alert
# MAGIC A **Databricks SQL alert** runs a query **on a schedule** on a SQL warehouse, evaluates a **condition** on the result and
# MAGIC **notifies** people (email, Slack, Teams, webhook… *notification destinations*) when it's met.
# MAGIC
# MAGIC **The signal:** orders from customers that don't exist in `bi_customers` (country `Unknown`) on the **latest** day. Someone
# MAGIC must fix the customer feed when that happens.

# COMMAND ----------

# DBTITLE 1,The alert query (copy it)
alert_sql = f"""SELECT count(*) AS unknown_orders
FROM {bi_table('bi_orders')}
WHERE country = 'Unknown'
  AND order_date = (SELECT max(order_date) FROM {bi_table('bi_orders')})"""
print(alert_sql)
display(sql_on_warehouse(alert_sql))

# COMMAND ----------

# MAGIC %md
# MAGIC Right now (latest day 30 June) the result is **0**. Create the alert:
# MAGIC
# MAGIC 1. Left sidebar → **Alerts** → **Create alert** (or **+ New → Alert**).
# MAGIC 2. Name it **`09-L3 Unknown customer orders`**.
# MAGIC 3. Paste the query in the **query editor**, pick your **SQL warehouse** in the compute selector, click **Run**.
# MAGIC 4. **Condition:** **Value column** `unknown_orders` · **Aggregation** *none* (or *Sum*) · **Operator** `>` ·
# MAGIC    **Threshold** `0` (a static value — it could also be another column).
# MAGIC 5. Click **Test condition** → *not triggered* (0 is not > 0).
# MAGIC 6. **Schedule:** open **Edit schedule** → every **1 hour** (any value is fine — we'll run it by hand) → **Save**.
# MAGIC 7. **Notifications:** add yourself.
# MAGIC 8. Open **Advanced settings** and look at (keep the defaults):
# MAGIC    * **Notify on OK** — also notify when the alert goes back to `OK`;
# MAGIC    * **Empty result state** — which state to use if the query returns **no rows**;
# MAGIC    * **Template** — custom subject/body with variables like `{{ALERT_STATUS}}`, `{{ALERT_CONDITION}}`, `{{ALERT_THRESHOLD}}`.
# MAGIC 9. Save (**View alert**) and click **Run now** → status **OK**.
# MAGIC
# MAGIC Now new data arrives — 1 July contains an order of the unknown customer `C9999`:

# COMMAND ----------

# DBTITLE 1,New data: 1 July
load_sales_until(1)
display(sql_on_warehouse(alert_sql))

# COMMAND ----------

# MAGIC %md
# MAGIC 10. Back on the alert page click **Run now** → status **TRIGGERED** 🔔 and you receive a notification.
# MAGIC 11. Finally **pause** the schedule (**Edit schedule → Pause**, or the pause toggle) so it doesn't run every hour.
# MAGIC
# MAGIC | Alert state | Meaning |
# MAGIC |---|---|
# MAGIC | `OK` | evaluated, condition **not** met |
# MAGIC | `TRIGGERED` | evaluated, condition met → notification |
# MAGIC | `ERROR` | the query failed (missing table, no permission, warehouse problem) |
# MAGIC
# MAGIC > 🕰️ **Legacy alerts** (still in older exam questions) were built **on a saved query** and had a fourth state `UNKNOWN`
# MAGIC > (never evaluated) plus a *Refresh* schedule on the query. In the new alert editor each alert **owns its query** and its
# MAGIC > schedule. Alerts can also run as an **alert task in a Lakeflow Job** — evaluated right after the pipeline that loads the
# MAGIC > data, instead of on their own clock (Section 10).
# MAGIC
# MAGIC ## Part 2 · Gold serving objects: view vs materialized view
# MAGIC Same question — *revenue per category per day* — served three ways:
# MAGIC
# MAGIC | Object | Stores data? | Fresh? | Cost at read time |
# MAGIC |---|---|---|---|
# MAGIC | **table** `bi_sales_daily` (rebuilt by the pipeline) | ✅ | as fresh as the last pipeline run | cheap |
# MAGIC | **view** | ❌ | always (query runs on every read) | the full query, every time |
# MAGIC | **materialized view** | ✅ (precomputed) | as fresh as the last **refresh** | cheap |

# COMMAND ----------

# DBTITLE 1,A view and a materialized view for BI
# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW bi_v_country_monthly
# MAGIC COMMENT 'Monthly orders and revenue per country - always current (computed at query time)'
# MAGIC AS SELECT date_trunc('MONTH', order_date) AS month, country,
# MAGIC           count(*) AS orders, round(sum(total), 2) AS revenue, round(avg(total), 2) AS avg_order_value
# MAGIC    FROM bi_orders
# MAGIC    GROUP BY ALL;
# MAGIC
# MAGIC CREATE MATERIALIZED VIEW bi_mv_category_daily
# MAGIC COMMENT 'Daily revenue per category - precomputed, refreshed on demand or on a schedule'
# MAGIC AS SELECT order_date, category, count(DISTINCT order_id) AS orders, sum(quantity) AS units,
# MAGIC           round(sum(subtotal), 2) AS revenue
# MAGIC    FROM bi_order_items
# MAGIC    GROUP BY order_date, category;

# COMMAND ----------

# MAGIC %md
# MAGIC ⏳ Creating a materialized view starts a small **serverless pipeline** behind the scenes (that's what computes and later
# MAGIC refreshes it) — it can take a minute or two. Now another day arrives:

# COMMAND ----------

# DBTITLE 1,New data: 2 July - who sees it?
load_sales_until(2)
fresh = {
    "view bi_v_country_monthly": spark.sql("SELECT sum(orders) FROM bi_v_country_monthly WHERE month = '2026-07-01'").first()[0],
    "materialized view bi_mv_category_daily": spark.sql(
        "SELECT max(order_date) FROM bi_mv_category_daily").first()[0],
}
for k, v in fresh.items():
    print(f"{k:<40} -> {v}")

# COMMAND ----------

# MAGIC %md
# MAGIC Expected: the **view** already counts **238** July orders (1 and 2 July). The **materialized view** still ends on
# MAGIC **2026-07-01** — it shows the data of its last refresh. Refresh it:

# COMMAND ----------

# DBTITLE 1,Refresh the materialized view (on demand)
# MAGIC %sql
# MAGIC REFRESH MATERIALIZED VIEW bi_mv_category_daily;
# MAGIC SELECT max(order_date) AS latest_day, count(*) AS rows FROM bi_mv_category_daily;

# COMMAND ----------

# MAGIC %md
# MAGIC In production you don't refresh by hand. Options:
# MAGIC
# MAGIC | How | Syntax / place | Use when |
# MAGIC |---|---|---|
# MAGIC | **Schedule** | `CREATE MATERIALIZED VIEW … SCHEDULE EVERY 1 DAY` or `… SCHEDULE CRON '0 0 6 * * ?' AT TIME ZONE 'Asia/Riyadh'` · `ALTER MATERIALIZED VIEW mv ADD / ALTER / DROP SCHEDULE` | fixed freshness (every hour / day) |
# MAGIC | **Trigger on update** | `… TRIGGER ON UPDATE [AT MOST EVERY INTERVAL 1 HOUR]` | refresh when the source tables change |
# MAGIC | **Job task** | a SQL task `REFRESH MATERIALIZED VIEW …` after the load task (Section 10) | refresh exactly after the pipeline |
# MAGIC | **In a pipeline** | `CREATE OR REFRESH MATERIALIZED VIEW` in Lakeflow SDP (Section 08) | the MV belongs to the pipeline |
# MAGIC
# MAGIC Add a daily schedule, look at it, and remove it again (no daily runs while you're learning):

# COMMAND ----------

# DBTITLE 1,ADD SCHEDULE -> DESCRIBE -> DROP SCHEDULE
spark.sql("ALTER MATERIALIZED VIEW bi_mv_category_daily ADD SCHEDULE EVERY 1 DAY")
_desc = spark.sql("DESCRIBE TABLE EXTENDED bi_mv_category_daily")
display(_desc.where("lower(col_name) LIKE '%refresh%' OR lower(col_name) LIKE '%schedule%' OR col_name = 'Type'"))
spark.sql("ALTER MATERIALIZED VIEW bi_mv_category_daily DROP SCHEDULE")
print("schedule added, inspected and dropped ✅")

# COMMAND ----------

# MAGIC %md
# MAGIC > 🎯 Exam objective 3.6 — *“Build Gold layer objects including materialized views, views, streaming tables, and tables for BI
# MAGIC > and analytics teams.”* Rule of thumb: **view** = always fresh, cheap to store, pays per read (fine for small data / rarely
# MAGIC > read). **Materialized view** = precomputed, fast & cheap reads, refreshed (incrementally when possible) — the default for
# MAGIC > dashboard aggregates. **Table** = you own the write logic (MERGE, SCD2…). **Streaming table** = append-only incremental
# MAGIC > ingestion (bronze/silver), rarely what a dashboard reads directly.
# MAGIC
# MAGIC ## Part 3 · Make gold tables BI-friendly
# MAGIC BI users — and **Genie** — rely on metadata: what is this table, what does this column mean, how do tables join?

# COMMAND ----------

# DBTITLE 1,Comments for humans, Genie and BI tools
# MAGIC %sql
# MAGIC COMMENT ON TABLE bi_orders IS 'One row per valid ShopWave order (deduplicated, cancelled orders removed). Grain: order.';
# MAGIC ALTER TABLE bi_orders ALTER COLUMN total COMMENT 'Order value in USD (sum of the line subtotals)';
# MAGIC ALTER TABLE bi_orders ALTER COLUMN country COMMENT 'Customer country; Unknown = customer not found in bi_customers';
# MAGIC COMMENT ON TABLE bi_customers IS 'ShopWave customers (dimension). One row per customer.';

# COMMAND ----------

# MAGIC %md
# MAGIC **Informational primary and foreign keys** — Unity Catalog stores them, Catalog Explorer shows them in the **entity
# MAGIC relationship diagram**, BI tools such as **Power BI** use them to create **relationships** automatically, and Genie uses them
# MAGIC to join correctly. They are **not enforced** (with `RELY` the optimizer may use them).

# COMMAND ----------

# DBTITLE 1,Primary key on bi_customers, foreign key on bi_orders (re-run safe)
for _stmt in ["ALTER TABLE bi_orders DROP CONSTRAINT IF EXISTS bi_orders_customer_fk",
              "ALTER TABLE bi_customers DROP CONSTRAINT IF EXISTS bi_customers_pk",
              "ALTER TABLE bi_customers ALTER COLUMN customer_id SET NOT NULL",
              "ALTER TABLE bi_customers ADD CONSTRAINT bi_customers_pk PRIMARY KEY (customer_id)",
              "ALTER TABLE bi_orders ADD CONSTRAINT bi_orders_customer_fk FOREIGN KEY (customer_id) REFERENCES bi_customers"]:
    spark.sql(_stmt)
_orphans = spark.sql("SELECT count(*) FROM bi_orders o LEFT ANTI JOIN bi_customers c USING (customer_id)").first()[0]
print(f"constraints added ✅ - yet {_orphans} orders point to a customer that doesn't exist: the FK is NOT enforced")
display(spark.sql("DESCRIBE TABLE EXTENDED bi_orders").where("col_name LIKE '%customer_fk%' OR col_name = 'Comment'"))

# COMMAND ----------

# MAGIC %md
# MAGIC 🖱️ Open **Catalog** → your catalog → `shopwave` → `bi_orders`: the **Overview** shows the comments, and a **View
# MAGIC relationships** / **ER diagram** button shows `bi_orders → bi_customers`.
# MAGIC
# MAGIC **Metric view (bonus).** Remember the grain problem from 09-L1 (summing `orders` per category double-counts)? A **Unity
# MAGIC Catalog metric view** defines **dimensions** and **measures** once, in YAML; every consumer (SQL, dashboards, Genie, alerts)
# MAGIC asks for `MEASURE(orders)` and gets the right answer **at any grouping**.

# COMMAND ----------

# DBTITLE 1,Bonus: a metric view (needs a recent runtime - skipped gracefully otherwise)
metric_view_sql = f"""
CREATE OR REPLACE VIEW bi_metrics_sales
WITH METRICS
LANGUAGE YAML
AS $$
version: 1.1
comment: "ShopWave sales KPIs"
source: {bi_table('bi_order_items')}
dimensions:
  - name: order_month
    expr: DATE_TRUNC('MONTH', order_date)
  - name: country
    expr: country
  - name: category
    expr: category
measures:
  - name: revenue
    expr: ROUND(SUM(subtotal), 2)
  - name: units
    expr: SUM(quantity)
  - name: orders
    expr: COUNT(DISTINCT order_id)
$$"""
try:
    spark.sql(metric_view_sql)
    display(spark.sql("""SELECT category, MEASURE(orders) AS orders, MEASURE(revenue) AS revenue
                         FROM bi_metrics_sales GROUP BY category ORDER BY revenue DESC"""))
    print("all categories together:",
          spark.sql("SELECT MEASURE(orders) AS orders FROM bi_metrics_sales").first()[0], "orders (not the sum of the rows above)")
except Exception as e:
    print("metric views not available on this compute - read the code, the idea is what matters:", _first_line(e))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 4 · 🖱️ Connect BI tools
# MAGIC Every external tool connects to a **SQL warehouse** (recommended for BI) with three values plus a credential:

# COMMAND ----------

# DBTITLE 1,Connection details of your SQL warehouse
details = connection_details()

# COMMAND ----------

# MAGIC %md
# MAGIC 1. 🖱️ **SQL Warehouses** → your warehouse → **Connection details** tab: the same values, plus ready-made JDBC URLs and links
# MAGIC    to the **JDBC / ODBC drivers** and the **Python / Go / Node.js** connectors. (For a *cluster* the values are under
# MAGIC    **Advanced options → JDBC/ODBC**.)
# MAGIC 2. **Credentials:** **OAuth** (user sign-in, recommended for desktop tools) · **personal access token** · a **service
# MAGIC    principal** (OAuth machine-to-machine) for scheduled refreshes and production — never a person's token.
# MAGIC 3. 🖱️ **Partner Connect** (sidebar → **Marketplace** → **Partner Connect**, or **Partner Connect** directly): tiles for
# MAGIC    Power BI, Tableau, Fivetran, dbt… For **cloud** partners it creates the **service principal, token and SQL warehouse**
# MAGIC    connection for you (workspace admin needed). For **desktop** tools (Power BI Desktop, Tableau Desktop) it downloads a
# MAGIC    **connection file** (`.pbids` / `.tds`) pre-filled with hostname and HTTP path.
# MAGIC 4. 🖱️ **Power BI from Catalog Explorer:** **Catalog** → pick a SQL warehouse → open the `shopwave` **schema** → **Use with BI
# MAGIC    tools** → **Publish to Power BI workspace** (needs Power BI Premium + Entra ID sign-in — just look at the dialog). It
# MAGIC    creates a **semantic model** in the Power BI service; your **foreign keys** become Power BI **relationships** and your
# MAGIC    **column comments** become field **descriptions** — Part 3 pays off. Storage modes: **Import** (data copied into Power BI,
# MAGIC    refreshed on a schedule) vs **DirectQuery** (every visual queries the SQL warehouse → always current, the warehouse must be
# MAGIC    available). A **Power BI task** in Lakeflow Jobs can publish/refresh the semantic model right after the pipeline.
# MAGIC 5. 🆓 Free Edition: Partner Connect and outbound connections are limited — looking at the pages is enough.
# MAGIC
# MAGIC A **Python app** outside Databricks would use the **Databricks SQL Connector** (`pip install databricks-sql-connector`) —
# MAGIC the same three values plus a token:
# MAGIC
# MAGIC ```python
# MAGIC from databricks import sql
# MAGIC with sql.connect(server_hostname="<Server hostname>", http_path="<HTTP path>",
# MAGIC                  access_token="<token>") as conn, conn.cursor() as cur:
# MAGIC     cur.execute("SELECT country, sum(total) FROM workspace.shopwave.bi_orders GROUP BY country")
# MAGIC     print(cur.fetchall())
# MAGIC ```
# MAGIC …or the **Statement Execution REST API**, which you used through `sql_on_warehouse()` in 09-L1.
# MAGIC
# MAGIC ## Part 5 · Permissions for BI consumers · who ran what
# MAGIC A BI analyst group needs **two** kinds of permission:
# MAGIC
# MAGIC | Layer | Grant | Where |
# MAGIC |---|---|---|
# MAGIC | **Compute** | **Can use** on the SQL warehouse | warehouse → **Permissions** |
# MAGIC | **Data** (Unity Catalog) | `USE CATALOG` + `USE SCHEMA` + `SELECT` on the gold tables / views | SQL or Catalog Explorer → **Permissions** |
# MAGIC | **Dashboard** | **Can View / Can Run** on the dashboard (shared data permission → no data grants needed for viewing) | dashboard → **Share** |
# MAGIC
# MAGIC ```sql
# MAGIC -- example for a group called bi_analysts (create groups in the account console - Section 13)
# MAGIC GRANT USE CATALOG ON CATALOG workspace                TO `bi_analysts`;
# MAGIC GRANT USE SCHEMA  ON SCHEMA  workspace.shopwave       TO `bi_analysts`;
# MAGIC GRANT SELECT      ON TABLE   workspace.shopwave.bi_sales_daily TO `bi_analysts`;
# MAGIC GRANT SELECT      ON VIEW    workspace.shopwave.bi_v_country_monthly TO `bi_analysts`;
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Current grants on a gold table
# MAGIC %sql
# MAGIC SHOW GRANTS ON TABLE bi_sales_daily;

# COMMAND ----------

# MAGIC %md
# MAGIC **Who ran what?** Query history records the **source** of every statement — editor, dashboard, alert, job, API:

# COMMAND ----------

# DBTITLE 1,Your statements on the warehouse in the last minutes, with their source
display(recent_queries(n=25))

# COMMAND ----------

# MAGIC %md
# MAGIC You should see `editor / API` rows (your notebook calls) and an **alert** row from *Run now*; dashboard refreshes from 09-L2
# MAGIC appear as **dashboard**. For history across days and all compute types, query the system table (it arrives with some delay):
# MAGIC
# MAGIC ```sql
# MAGIC SELECT query_source.dashboard_id IS NOT NULL AS from_dashboard, count(*) AS statements,
# MAGIC        round(avg(total_duration_ms) / 1000, 1) AS avg_seconds, sum(CASE WHEN from_result_cache THEN 1 ELSE 0 END) AS cached
# MAGIC FROM system.query.history
# MAGIC WHERE start_time > current_date() - INTERVAL 7 DAYS
# MAGIC GROUP BY ALL
# MAGIC ```
# MAGIC
# MAGIC ## Part 6 · ✅ Automatic checks

# COMMAND ----------

# DBTITLE 1,Check your work
alert = find_alert(ALERT)
_ev = (alert or {}).get("evaluation") or {}
_src = (_ev.get("source") or {}).get("name")
_types = {r["col_name"]: r["data_type"] for r in spark.sql("DESCRIBE TABLE EXTENDED bi_mv_category_daily").collect()}
_orders_desc = spark.sql("DESCRIBE TABLE EXTENDED bi_orders")
_expected_mv = spark.sql("SELECT count(*) FROM (SELECT DISTINCT order_date, category FROM bi_order_items)").first()[0]
_checks = {
    "alert '09-L3 Unknown customer orders' exists": alert is not None,
    "alert condition: unknown_orders > 0": _src == "unknown_orders" and _ev.get("comparison_operator") == "GREATER_THAN",
    "alert was evaluated after new data -> TRIGGERED (click Run now)": _ev.get("state") == "TRIGGERED",
    "alert has a schedule": bool((alert or {}).get("schedule")),
    "view shows July immediately (238 orders)": fresh["view bi_v_country_monthly"] == 238,
    "materialized view was stale before REFRESH": str(fresh["materialized view bi_mv_category_daily"]) == "2026-07-01",
    "materialized view is current after REFRESH": spark.table("bi_mv_category_daily").count() == _expected_mv
                                                 and str(spark.sql("SELECT max(order_date) FROM bi_mv_category_daily").first()[0]) == "2026-07-02",
    "bi_mv_category_daily is a MATERIALIZED_VIEW": "MATERIALIZED" in str(_types.get("Type", "")).upper(),
    "table comment on bi_orders": _orders_desc.where("col_name = 'Comment' AND data_type LIKE '%Grain: order%'").count() == 1,
    "foreign key bi_orders -> bi_customers": _orders_desc.where("upper(data_type) LIKE '%FOREIGN KEY%'").count() >= 1,
}
for name, ok in _checks.items():
    print(("✅ " if ok else "❌ ") + name)
if alert and (alert.get("schedule") or {}).get("pause_status") != "PAUSED":
    print("ℹ️ remember to PAUSE the alert schedule")
print("\n🎉 Lab complete - next: 09-L4 · Challenge lab")
