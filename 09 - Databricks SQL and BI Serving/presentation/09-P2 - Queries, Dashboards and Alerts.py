# Databricks notebook source
# MAGIC %md
# MAGIC # 📊 09-P2 · Queries, Dashboards and Alerts
# MAGIC **Section 09** · exam objectives **3.6** (gold objects for BI & analytics teams), **4.2** (SQL query, **dashboard** and
# MAGIC alert tasks in jobs), **4.3–4.4** (time-based vs data-driven refresh)
# MAGIC
# MAGIC | After this presentation you can… |
# MAGIC |---|
# MAGIC | Use the **SQL editor**: saved queries, **named parameters** (`:name`), visualizations, permissions |
# MAGIC | Explain the anatomy of an **AI/BI dashboard**: datasets, widgets, filters vs parameters, pages |
# MAGIC | Choose between **shared** and **individual data permission** when publishing, and the dashboard permission levels |
# MAGIC | Keep a dashboard fresh: **refresh**, **schedules & subscriptions**, the **dashboard task** in a job |
# MAGIC | Build a **SQL alert** (query → condition → schedule → notification) and know its **states** |
# MAGIC | Place **Genie** (Genie Agents, Genie One) in the picture |
# MAGIC
# MAGIC > ▶️ **Run all** to render the diagrams.

# COMMAND ----------

# MAGIC %run ../../Includes/_style

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · The SQL editor and saved queries

# COMMAND ----------

# DBTITLE 1,Slide · Query objects
show("""
<div class="kicker">Slide 1 · A saved query is a workspace object</div>
<h2>Write → run on a warehouse → visualize → save → share / schedule / reuse</h2>
<div class="grid">
 <div class="card"><h3>✍️ Editor</h3><ul><li>compute selector (SQL warehouse) + default catalog/schema</li>
 <li>several statements per tab (<b>Run all</b>), format, snippets</li><li>✨ <b>Genie Code</b> (formerly Databricks Assistant)</li></ul></div>
 <div class="card green"><h3>📈 Visualizations</h3>bar, line, area, pie, scatter, counter, table, map, pivot… saved <b>with the query</b>
 (also available on notebook results)</div>
 <div class="card orange"><h3>💾 Saved query</h3>stored in a workspace folder (Home by default) · run by <b>job SQL tasks</b> ·
 created also via REST API, JDBC/ODBC tools, Terraform</div>
</div>
<table class="tbl">
<tr><th>Query permission</th><th>Adds the ability to…</th></tr>
<tr><td><b>CAN VIEW</b></td><td>see the query in the list and read its text</td></tr>
<tr><td><b>CAN RUN</b></td><td>refresh the result / choose different parameter values</td></tr>
<tr><td><b>CAN EDIT</b></td><td>edit the query text</td></tr>
<tr><td><b>CAN MANAGE</b></td><td>change permissions, delete</td></tr>
</table>
""" + callout("legacy", "Saved queries have a <b>Run as viewer / Run as owner</b> credential setting (owner = whoever runs it uses "
              "the owner's data access — today mostly relevant for legacy alerts and jobs). Dashboards express the same idea as "
              "<b>shared vs individual data permission</b> (§4)."))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · Named parameters
# MAGIC
# MAGIC ```sql
# MAGIC SELECT order_date, count(*) AS orders, round(sum(total), 2) AS revenue
# MAGIC FROM workspace.shopwave.bi_orders
# MAGIC WHERE country = :country                                   -- String widget
# MAGIC   AND order_date BETWEEN :period.min AND :period.max       -- Date range widget: .min / .max
# MAGIC GROUP BY order_date
# MAGIC ORDER BY order_date
# MAGIC LIMIT 100
# MAGIC ```
# MAGIC
# MAGIC | Need | Syntax |
# MAGIC |---|---|
# MAGIC | a value (string, integer, decimal, date, timestamp) | `:name` — a widget appears; set its type with ⚙️, then **Apply changes** |
# MAGIC | a table or column name | `IDENTIFIER(:table_name)` |
# MAGIC | a multi-select list | `array_contains(:countries, country)` (array / comma-split pattern) |
# MAGIC | a date range | `:period.min`, `:period.max` |
# MAGIC | 🕰️ legacy editor | `{{ country }}` (“mustache”) → migrate to `:country` |
# MAGIC
# MAGIC The **same** `:name` markers work in dashboards datasets, alerts, the Statement Execution API and `spark.sql(…, args=…)` —
# MAGIC values travel **separately** from the SQL text (no SQL injection).
# MAGIC
# MAGIC ## 3 · AI/BI dashboards — anatomy

# COMMAND ----------

# DBTITLE 1,Slide · Datasets, widgets, filters
show("""
<div class="kicker">Slide 3 · Formerly “Lakeview” dashboards — the only dashboards in Databricks SQL today</div>
<h2>Datasets → widgets on pages → filters &amp; parameters</h2>
<div class="flow">
 <div class="step"><b>🗃️ Data tab: datasets</b>a table / view / <strong>metric view</strong> or a SQL query (may contain <code>:params</code>)<br>custom calculations / measures</div><div class="arrow">➜</div>
 <div class="step"><b>🧱 Canvas: widgets</b>counter · bar · line · area · pie · table · pivot · map · text…<br>each widget aggregates ONE dataset</div><div class="arrow">➜</div>
 <div class="step"><b>🎚️ Filters</b>date range · single / multiple values · range slider<br>page-level or <strong>global</strong> (all pages)</div><div class="arrow">➜</div>
 <div class="step"><b>📤 Publish</b>a snapshot viewers see; the draft stays private</div>
</div>
<div class="grid two">
 <div class="card"><h3>🎚️ Filter</h3>narrows the rows of <b>every dataset that has the field</b> (one filter can bind several datasets);
 clicking a bar <b>cross-filters</b> widgets of the same dataset. A dataset without the field is <b>not</b> filtered.</div>
 <div class="card green"><h3>🧮 Parameter</h3>a value <b>inside the dataset SQL</b> (<code>:top_n</code>, <code>:threshold</code>) — changes the query itself;
 shown as a filter-style widget bound to the parameter.</div>
</div>
""" + callout("tip", "Design for the <b>grain</b>: a counter of orders must sit on an order-grain dataset. Fewer, wider datasets = more "
              "cross-filtering. Put business logic (CASE, COALESCE, ratios) into the dataset SQL or a metric view.") +
              callout("info", "<b>AI-assisted authoring:</b> describe a widget in plain language and <b>Genie Code</b> configures it; "
              "Power BI / Tableau reports can even be imported (2026). Always review the fields and aggregations it chose."))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · Draft vs published, data permissions and sharing

# COMMAND ----------

# DBTITLE 1,Slide · Publishing
show("""
<div class="kicker">Slide 4 · Whose permissions run the queries?</div>
<h2>Publish with <u>shared</u> or <u>individual</u> data permission</h2>
<div class="grid two">
 <div class="card orange"><h3>🤝 Shared data permission (default, “embed credentials”)</h3><ul>
 <li>viewers run queries with the <b>publisher's</b> data permissions (or a service principal's)</li>
 <li>viewers need <b>no</b> <code>SELECT</code> on the tables</li>
 <li>shared result cache → fast for everybody</li>
 <li>✅ company-wide KPI dashboards</li></ul></div>
 <div class="card"><h3>👤 Individual data permission (“don't embed”)</h3><ul>
 <li>viewers run queries with <b>their own</b> Unity Catalog permissions</li>
 <li><b>row filters / column masks</b> apply per viewer</li>
 <li>viewers without access see errors</li>
 <li>✅ “each manager sees only their region”</li></ul></div>
</div>
<p>In both modes the <b>compute</b> (SQL warehouse) is used with the <b>publisher's</b> permission — viewers don't need <i>Can use</i>.
The <b>draft</b> always runs with the editor's own permissions and is invisible to viewers until the next <b>Publish</b>.</p>
<table class="tbl">
<tr><th>Dashboard permission</th><th>Adds the ability to…</th></tr>
<tr><td><b>CAN VIEW / CAN RUN</b></td><td>view the published dashboard, its results and datasets; interact with filters &amp; widgets; subscribe</td></tr>
<tr><td><b>CAN EDIT</b></td><td>edit the draft, <b>publish</b> a new snapshot, manage schedules &amp; subscribers</td></tr>
<tr><td><b>CAN MANAGE</b></td><td>change permissions, delete</td></tr>
</table>
""" + callout("exam", "Published dashboards can be shared with <b>account users who aren't in the workspace</b> (view-only, publisher's "
              "credentials for compute) — and embedded in other web apps."))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 · Keeping dashboards fresh
# MAGIC
# MAGIC | Mechanism | What it does | When |
# MAGIC |---|---|---|
# MAGIC | **Refresh** (↻) | re-runs all datasets now | a viewer wants the latest numbers |
# MAGIC | **Schedule** (dashboard header → *Schedule*) | runs all datasets on a cron-like schedule (a chosen warehouse) → fills the **result cache**; sends **subscriptions** | daily/hourly reporting, warm cache before 8 a.m. |
# MAGIC | **Subscriptions** | email = **PDF** snapshot (also external addresses) · Slack / Microsoft Teams = **PNG**; up to **100** subscribers per schedule; CAN VIEW users subscribe themselves | push the numbers to people |
# MAGIC | **Dashboard task** in a Lakeflow Job | refreshes a **published** dashboard (serverless/pro warehouse), can notify subscribers, can pass job parameters to filters | refresh **right after** the pipeline that loads the data |
# MAGIC
# MAGIC > 🎯 Objectives 4.3/4.4 — **time-based** (schedule) vs **data-driven** (run when the data is ready): a dashboard schedule at
# MAGIC > 06:00 is time-based; *pipeline task → dashboard task* in one job (or a job with a **table update trigger**) is data-driven.
# MAGIC
# MAGIC ## 6 · Alerts

# COMMAND ----------

# DBTITLE 1,Slide · Anatomy of an alert
show("""
<div class="kicker">Slide 6 · A query that watches your data</div>
<h2>Query → condition → schedule → notification</h2>
<div class="flow">
 <div class="step"><b>📝 Query</b>owned by the alert (new editor)<br>runs on a SQL warehouse</div><div class="arrow">➜</div>
 <div class="step"><b>⚖️ Condition</b>value column (+ aggregation: sum, avg, count, min, max…)<br>operator (&gt; &ge; &lt; &le; = ≠, is null)<br>threshold: a value or another column</div><div class="arrow">➜</div>
 <div class="step"><b>⏰ Schedule</b>every N minutes/hours/days or Quartz cron · pause · <strong>Run now</strong></div><div class="arrow">➜</div>
 <div class="step"><b>📣 Notify</b>users · <strong>notification destinations</strong> (email, Slack, Teams, PagerDuty, webhook)</div>
</div>
<table class="tbl">
<tr><th>State</th><th>Meaning</th></tr>
<tr><td><code>OK</code></td><td>evaluated — condition not met</td></tr>
<tr><td><code>TRIGGERED</code></td><td>evaluated — condition met → notification</td></tr>
<tr><td><code>ERROR</code></td><td>the query failed</td></tr>
</table>
<div class="grid">
 <div class="card"><h3>⚙️ Advanced</h3><b>Notify on OK</b> (back to normal) · <b>empty result state</b> (what “no rows” means) · custom <b>template</b>
 (<code>{{ALERT_STATUS}}</code>, <code>{{ALERT_CONDITION}}</code>, <code>{{ALERT_THRESHOLD}}</code>…)</div>
 <div class="card green"><h3>🧩 In a job</h3>an <b>alert task</b> (SQL task subtype) evaluates it right after the load; downstream tasks can branch on the result</div>
 <div class="card orange"><h3>🔐 Alert permissions</h3><b>CAN RUN</b>: run manually, subscribe · <b>CAN MANAGE</b>: edit, permissions, delete</div>
</div>
""" + callout("legacy", "<b>Legacy alerts</b> were attached to a <b>saved query</b> (refresh schedule on the query), had a 4th state "
              "<code>UNKNOWN</code> (never evaluated) and the options “notify just once / each time evaluated / at most every …”. New "
              "alerts own their query and schedule; an existing saved query can't be reused as is."))

# COMMAND ----------

# MAGIC %md
# MAGIC **Typical data-engineering alerts:** rows in a quarantine table > 0 · unknown customers in today's orders > 0 · hours since
# MAGIC the last load > 2 (freshness) · today's revenue < 50 % of the 7-day average (anomaly) · failed expectations in the pipeline
# MAGIC event log > 0.
# MAGIC
# MAGIC ## 7 · Genie — questions in plain language
# MAGIC
# MAGIC | Name (2026) | Formerly | What it is | Who |
# MAGIC |---|---|---|---|
# MAGIC | **Genie Agents** | *Genie spaces* / AI/BI Genie | a curated space over a few tables / metric views + **instructions**, example SQL, trusted assets → answers questions with SQL it shows | domain experts curate, business users ask |
# MAGIC | **Genie One** | *Databricks One* | the business-user entry point: discover dashboards, apps and Genie Agents, ask questions | business users |
# MAGIC | **Genie Code** | *Databricks Assistant* | AI pair programmer in the editors (SQL, Python, dashboards, pipelines) | engineers, analysts |
# MAGIC
# MAGIC Quality of answers depends on **your gold layer**: clear names, **comments**, **PK/FK**, metric views (09-P3).
# MAGIC
# MAGIC ## 8 · Dashboards as code
# MAGIC
# MAGIC * A dashboard is a workspace file **`<name>.lvdash.json`** (datasets + pages + widgets as JSON): **Export**/**Import**, keep it
# MAGIC   in a **Git folder**, deploy it with **Declarative Automation Bundles** (Section 11), automate with the REST API
# MAGIC   (`/api/2.0/lakeview/dashboards`, publish, schedules).
# MAGIC * Legacy dashboards can be **cloned/migrated** to AI/BI dashboards (*Clone to AI/BI dashboard*).
# MAGIC
# MAGIC ## 9 · 🕰️ Legacy dashboards (recognise them)
# MAGIC The sample-course lab you may have seen builds a *legacy* DBSQL dashboard: save a query → add a **visualization** to the
# MAGIC query → **Add to dashboard** → refresh the dashboard. Legacy dashboards are **retired**; AI/BI dashboards replaced them
# MAGIC (datasets instead of saved queries, widgets configured on the canvas, draft/publish, data-permission modes). The concepts the
# MAGIC exam still asks — *visualizations come from query results, dashboards are refreshed manually or on a schedule, viewers'
# MAGIC credentials vs owner's credentials* — map 1:1 to the table in §4–§5.
# MAGIC
# MAGIC ## ✅ Key takeaways
# MAGIC 1. Saved queries live in workspace folders; permissions **CAN VIEW < CAN RUN < CAN EDIT < CAN MANAGE**; parameters `:name`
# MAGIC    (legacy `{{ }}`), `IDENTIFIER(:t)` for object names.
# MAGIC 2. AI/BI dashboard = **datasets** (Data tab) + **widgets** (canvas) + **filters** (rows of datasets that have the field) +
# MAGIC    **parameters** (values inside dataset SQL); draft vs **published** snapshot.
# MAGIC 3. **Shared data permission** = publisher's data rights (viewers need no SELECT) · **individual** = each viewer's UC rights
# MAGIC    (row filters/masks per viewer). Compute always via the publisher.
# MAGIC 4. Fresh dashboards: **Refresh**, **schedules + subscriptions** (PDF email, PNG Slack/Teams, ≤ 100 subscribers), **dashboard
# MAGIC    task** after the pipeline (data-driven).
# MAGIC 5. Alerts: query + condition + schedule + destinations; states **OK / TRIGGERED / ERROR**; alert task in jobs.
# MAGIC 6. **Genie Agents** (formerly Genie spaces) answer natural-language questions over curated tables.
# MAGIC
# MAGIC ➡️ Next: **09-P3 · Serving Gold Data to BI Tools**
