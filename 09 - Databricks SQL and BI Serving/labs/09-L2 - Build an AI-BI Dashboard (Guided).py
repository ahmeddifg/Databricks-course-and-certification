# Databricks notebook source
# MAGIC %md
# MAGIC # 🧪 Lab 09-L2 · Build an AI/BI Dashboard (Guided)
# MAGIC **Time:** ~60 min · **Compute:** Serverless notebook + your **SQL warehouse** · **Works on Free Edition**
# MAGIC
# MAGIC ShopWave's sales team wants **one page** with the numbers they look at every morning. You'll build it as an **AI/BI
# MAGIC dashboard** (the dashboards of Databricks SQL, formerly *Lakeview*): datasets → widgets → filters → parameters → **publish**
# MAGIC → **share** → **schedule**, and then watch it pick up **new data**.
# MAGIC
# MAGIC | Part | You will… |
# MAGIC |---|---|
# MAGIC | 1 | Prepare: reset the sales tables to January–June, print the dataset SQL |
# MAGIC | 2 | 🖱️ Create the dashboard and its **datasets** (a table + SQL queries, one with a **parameter**) |
# MAGIC | 3 | 🖱️ Build the **canvas**: text, 3 counters, line chart, bar chart, table |
# MAGIC | 4 | 🖱️ Add **filters** (date range, country) and a **parameter** widget |
# MAGIC | 5 | 🖱️ **Publish** (shared vs individual data permissions) and **share** |
# MAGIC | 6 | New data arrives → **refresh** |
# MAGIC | 7 | 🖱️ **Schedule** + subscription, then Genie and export (optional) |
# MAGIC | 8 | ✅ Automatic checks |

# COMMAND ----------

# MAGIC %run ../../Includes/_setup

# COMMAND ----------

# MAGIC %run ./_09_prepare

# COMMAND ----------

# MAGIC %md
# MAGIC ### Start clean
# MAGIC Puts the sales tables back to **January–June 2026** (other labs add July days). Your dashboard — if you built one in an
# MAGIC earlier attempt — is kept; the checks always look at the newest dashboard called **`09-L2 ShopWave Sales`**.

# COMMAND ----------

# DBTITLE 1,Start clean (safe to re-run)
reset_sales(0)
DASH = "09-L2 ShopWave Sales"
_existing = find_dashboard(DASH)
print("existing dashboard:", "yes - you can continue with it" if _existing else "none yet")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1 · The three datasets
# MAGIC A dashboard is built on **datasets** (Data tab). Each dataset is a table/view **or** a SQL query; widgets then aggregate the
# MAGIC dataset's columns. Fewer, wider datasets are better: widgets that share a dataset **cross-filter** each other.
# MAGIC
# MAGIC | Dataset | Source | Grain | Feeds |
# MAGIC |---|---|---|---|
# MAGIC | **Sales daily** | table `bi_sales_daily` | day × country × category | line chart, bar chart |
# MAGIC | **Orders** | SQL on `bi_orders` | one order | counters (revenue, orders, average order value) |
# MAGIC | **Top products** | SQL with a parameter `:top_n` | one product | table |

# COMMAND ----------

# DBTITLE 1,SQL for the datasets (copy from here)
orders_ds = f"""SELECT order_id, order_date, country, item_count, quantity, total
FROM {bi_table('bi_orders')}"""
top_products_ds = f"""SELECT product, category, units, revenue
FROM (
  SELECT p.title AS product, p.category,
         sum(i.quantity)           AS units,
         round(sum(i.subtotal), 2) AS revenue,
         row_number() OVER (ORDER BY sum(i.subtotal) DESC) AS rnk
  FROM {bi_table('bi_order_items')} i
  JOIN {bi_table('bi_products')} p USING (product_id)
  GROUP BY p.title, p.category
)
WHERE rnk <= :top_n
ORDER BY revenue DESC"""
print("Sales daily  ->  table", bi_table("bi_sales_daily"))
print("\nOrders  ->\n" + orders_ds)
print("\nTop products  ->\n" + top_products_ds)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2 · 🖱️ Create the dashboard and its datasets
# MAGIC 1. Left sidebar → **Dashboards** → **Create dashboard** (or **+ New → Dashboard**). An empty **draft** opens.
# MAGIC 2. Click the title *New Dashboard …* and rename it to **`09-L2 ShopWave Sales`**.
# MAGIC 3. Open the **Data** tab (top left).
# MAGIC    * **Sales daily:** click **Add data source** (or **+ Create from SQL → Select a table**) → browse to your catalog →
# MAGIC      `shopwave` → **`bi_sales_daily`** → **Confirm**. Rename the dataset (click its name) to **Sales daily**.
# MAGIC    * **Orders:** **+ Create from SQL** → paste the *Orders* SQL → **Run** → rename the dataset to **Orders**.
# MAGIC    * **Top products:** **+ Create from SQL** → paste the *Top products* SQL. Because it contains `:top_n`, a **parameter**
# MAGIC      appears under the editor: set its type to **Numeric** (integer) and its default value to **5** → **Run** →
# MAGIC      rename the dataset to **Top products**. Expected: 5 rows, *Smart Watch* first (132,042.24).
# MAGIC 4. Check the warehouse: the dashboard runs its datasets on the SQL warehouse shown in the header / **⋮ → Settings** —
# MAGIC    pick yours if it isn't selected.
# MAGIC
# MAGIC > 💡 Each dataset shows its **schema** and a **results preview**. A dataset can also be built from a **metric view** (09-P3)
# MAGIC > — then measures are defined once in Unity Catalog instead of in every dashboard.
# MAGIC
# MAGIC ## Part 3 · 🖱️ Build the canvas
# MAGIC Switch to the **Canvas** tab (the page — rename it *Overview*). The toolbar at the bottom has **Add a visualization**,
# MAGIC **Add a text box**, **Add a filter**. Every widget is configured in the panel on the right: **Dataset**, **Visualization**
# MAGIC type, fields (**X axis / Y axis / Color / Value**), and **Widget title**.
# MAGIC
# MAGIC | # | Add | Dataset | Configuration | Title |
# MAGIC |---|---|---|---|---|
# MAGIC | 1 | **Text box** | — | `# ShopWave sales` + one line of description | — |
# MAGIC | 2 | Visualization → **Counter** | Orders | **Value:** `total` → **Sum** | Revenue |
# MAGIC | 3 | Visualization → **Counter** | Orders | **Value:** `order_id` → **Count** | Orders |
# MAGIC | 4 | Visualization → **Counter** | Orders | **Value:** `total` → **Average** | Avg order value |
# MAGIC | 5 | Visualization → **Line** | Sales daily | **X:** `order_date` (transform **Weekly**) · **Y:** `revenue` → **Sum** | Weekly revenue |
# MAGIC | 6 | Visualization → **Bar** | Sales daily | **X:** `country` · **Y:** `revenue` → **Sum** · **Color:** `category` (optional) | Revenue by country |
# MAGIC | 7 | Visualization → **Table** | Top products | columns `product`, `category`, `units`, `revenue` | Top products |
# MAGIC
# MAGIC Expected counter values (no filter): **Revenue 1,104,394.23 · Orders 1,980 · Avg order value 557.77**.
# MAGIC
# MAGIC > 💡 **AI-assisted authoring:** instead of configuring a widget by hand you can describe it in the widget's prompt box
# MAGIC > (✨, *“bar chart of revenue by country”*) — **Genie Code** builds it and you review the configuration. Use it, but always
# MAGIC > check the field and aggregation it picked.
# MAGIC >
# MAGIC > ⚠️ Why counters on **Orders** and not on **Sales daily**? Sales daily counts an order once **per category**: `Sum of
# MAGIC > orders` there gives 4,304, not 1,980. Choose the dataset whose **grain** matches the measure.
# MAGIC
# MAGIC ## Part 4 · 🖱️ Filters and a parameter
# MAGIC 1. **Add a filter** → **Filter type: Date range picker** → **Fields:** click **+** and add `order_date` from **Sales daily**
# MAGIC    **and** `order_date` from **Orders** (one filter can drive several datasets). Title: *Order date*.
# MAGIC 2. **Add a filter** → **Multiple values** → **Fields:** `country` from **Sales daily** and `country` from **Orders**.
# MAGIC    Title: *Country*.
# MAGIC 3. **Add a filter** → **Single value** → instead of a field choose **Parameters** → `top_n` from **Top products**.
# MAGIC    Title: *Top N products*. (A parameter widget sets the dataset's `:top_n` before the query runs.)
# MAGIC 4. Test it: choose **Saudi Arabia** in *Country* → the counters show **110,438.74 / 197 / 560.60**. Set *Top N* to 10 → the
# MAGIC    table shows 10 rows. Clear the filters again.
# MAGIC
# MAGIC > 🧠 **Filter vs parameter.** A **filter** narrows the rows of every dataset that has the filtered field (the dashboard adds
# MAGIC > the `WHERE` for you). A **parameter** is a value **inside the dataset SQL** (`:top_n`) — use it when the value changes the
# MAGIC > query itself (a limit, a threshold, a calculation). The *Top products* dataset has no `order_date` and no `country`
# MAGIC > column, so the date and country filters **don't** affect it — check that in the dashboard. A filter can only filter what
# MAGIC > a dataset returns.
# MAGIC >
# MAGIC > 💡 Filters on a **Global filters** page (the funnel icon / *Add global filters*) apply to **all pages**; filters placed on a
# MAGIC > page apply to that page only.
# MAGIC
# MAGIC ## Part 5 · 🖱️ Publish and share
# MAGIC Everything you've done so far is a **draft** — only editors see it. Viewers see the **published** version.
# MAGIC
# MAGIC 1. Click **Publish**. In the dialog you choose whose **data permissions** viewers use:
# MAGIC    * **Shared data permission** (*Embed credentials* — the default): viewers run the queries with **your** (the
# MAGIC      publisher's) data permissions. They see the data even without `SELECT` on the tables. Results can be cached and shared
# MAGIC      between viewers → fast.
# MAGIC    * **Individual data permission** (*Don't embed credentials*): every viewer's **own** Unity Catalog permissions apply —
# MAGIC      row filters and column masks are applied **per viewer**; viewers without access see errors.
# MAGIC
# MAGIC    In both cases the **compute** (SQL warehouse) is used with the publisher's permission. Keep the default → **Publish**.
# MAGIC 2. Open the published version (**View published** / switch *Draft ↔ Published* in the header).
# MAGIC 3. Click **Share**: add a user or group with a permission level — **Can View** · **Can Run** · **Can Edit** · **Can Manage**.
# MAGIC    Account users who aren't in this workspace can get view-only access to the published dashboard.
# MAGIC 4. Make one change in the draft (e.g. move a widget) — the published version does **not** change until you publish again.

# COMMAND ----------

# DBTITLE 1,Links to your dashboard
dashboard_link(DASH)
dashboard_link(DASH, published=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 6 · New data arrives → refresh
# MAGIC The pipeline (Section 08) just delivered **1 July**. Run the cell, then go back to the **published** dashboard.

# COMMAND ----------

# DBTITLE 1,Load the orders of 1 July 2026
load_sales_until(1)
display(spark.sql("SELECT count(*) AS orders, round(sum(total), 2) AS revenue FROM bi_orders"))

# COMMAND ----------

# MAGIC %md
# MAGIC 1. If the published dashboard was already open, it still shows the old numbers. Click **Refresh** (↻, top right): the
# MAGIC    datasets run again on the warehouse → **Revenue 1,165,110.84 · Orders 2,099**, and the line chart gets a new point.
# MAGIC 2. Why could it show old numbers? Dashboards use the **query result cache**; it's invalidated when the tables change, but an
# MAGIC    already-rendered page only re-runs its queries when you **refresh** (or reload) it.
# MAGIC
# MAGIC ## Part 7 · 🖱️ Schedule, subscribe, Genie, export
# MAGIC **Schedule** — refresh the dashboard automatically (and pre-warm the cache so the first viewer in the morning doesn't wait):
# MAGIC
# MAGIC 1. In the dashboard header click **Schedule** → **Add schedule**.
# MAGIC 2. Every **day** at **07:00**, your time zone. Under **Subscribers** add yourself (email). Subscribers get a **PDF** snapshot
# MAGIC    by email (Slack / Microsoft Teams destinations get a PNG). → **Create**.
# MAGIC 3. **⋮** next to the schedule → **Run now** (optional: you should receive the email).
# MAGIC 4. To avoid running it every day for nothing: **⋮ → Pause** (or delete it) **after** the automatic checks below.
# MAGIC
# MAGIC > 💡 A schedule refreshes **one** dashboard on its own clock. If the refresh must happen **after** the data is loaded, use a
# MAGIC > **Lakeflow Job** instead: *pipeline task → dashboard task* (Section 10) — the dashboard task refreshes the published
# MAGIC > dashboard and notifies its subscribers.
# MAGIC
# MAGIC **Optional extras**
# MAGIC * **Ask Genie:** on the published dashboard, open **Ask Genie** and ask *“Which category had the highest revenue in May?”*
# MAGIC   Genie (the dashboard's companion **Genie Agent** — formerly *Genie space*) answers from the dashboard's datasets in natural
# MAGIC   language and shows the SQL it wrote.
# MAGIC * **Export / Git:** **⋮ → Export** downloads the dashboard as a **`.lvdash.json`** file (datasets + layout as JSON). The same
# MAGIC   file lives in your workspace folder: put it in a **Git folder** or deploy it with **Declarative Automation Bundles**
# MAGIC   (Section 11) to move it from dev to prod.
# MAGIC
# MAGIC ## Part 8 · ✅ Automatic checks

# COMMAND ----------

# DBTITLE 1,Check your work
summary = dashboard_summary(DASH)
_t = (summary or {}).get("widget_types", {})
_checks = {
    "dashboard '09-L2 ShopWave Sales' exists": summary is not None,
    "at least 3 datasets": (summary or {}).get("datasets", 0) >= 3,
    "a dataset uses the :top_n parameter": any(":top_n" in q for q in (summary or {}).get("dataset_sql", [])),
    "3 counters": _t.get("counter", 0) >= 3,
    "a line chart and a bar chart": _t.get("line", 0) >= 1 and _t.get("bar", 0) >= 1,
    "a table widget": _t.get("table", 0) >= 1,
    "filter widgets on the canvas (date range, country, top N)": (summary or {}).get("filters", 0) >= 2,
    "dashboard is published": bool((summary or {}).get("published")),
    "published with shared data permission (embedded credentials)": (summary or {}).get("embed_credentials") is True,
    "July 1 loaded -> 2,099 orders": spark.table("bi_orders").count() == 2099,
}
for name, ok in _checks.items():
    print(("✅ " if ok else "❌ ") + name)
print(("✅ " if (summary or {}).get("schedules") else "ℹ️ ") + f"schedules: {(summary or {}).get('schedules', 0)} "
      "(optional - pause or delete it when you're done)")
print("\n🎉 Lab complete - next: 09-L3 · Alerts, serving objects and BI connectivity")
