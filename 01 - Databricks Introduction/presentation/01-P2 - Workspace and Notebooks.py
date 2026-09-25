# Databricks notebook source
# MAGIC %md
# MAGIC # 📓 01-P2 · Workspace & Notebooks
# MAGIC **Section 01 — Databricks Introduction & Platform** · supports every exam domain
# MAGIC
# MAGIC | After this presentation you can… |
# MAGIC |---|
# MAGIC | Find your way around the workspace UI and its object types |
# MAGIC | Use magic commands (`%sql`, `%md`, `%run`, `%fs`, `%pip` …) correctly |
# MAGIC | Use `dbutils` for files, widgets, notebooks and secrets |
# MAGIC | Explain `%run` vs `dbutils.notebook.run()` — a favourite exam question |
# MAGIC | Pass values between Python and SQL |
# MAGIC
# MAGIC > ▶️ **Run all** to render the diagrams. Hands-on practice is in **labs/01-L1**.

# COMMAND ----------

# MAGIC %run ../../Includes/_style

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · The workspace at a glance
# MAGIC The left sidebar groups everything by persona. *(Labels shift slightly between releases — look for the same ideas.)*

# COMMAND ----------

# DBTITLE 1,Slide · Sidebar map
show("""
<div class="kicker">Slide 1 · Workspace UI</div>
<h2>What lives where in the sidebar</h2>
<div class="grid">
  <div class="card"><h3>🧭 General</h3><ul><li><b>+ New</b> — notebook, query, job, pipeline…</li><li><b>Workspace</b> — folders, notebooks, files, Git folders</li>
      <li><b>Recents</b></li><li><b>Catalog</b> — Catalog Explorer (Unity Catalog)</li><li><b>Jobs &amp; Pipelines</b> — Lakeflow</li>
      <li><b>Compute</b></li><li><b>Marketplace</b></li></ul></div>
  <div class="card green"><h3>📊 SQL</h3><ul><li>SQL Editor</li><li>Queries</li><li>Dashboards</li><li>Genie</li><li>Alerts</li>
      <li>Query History</li><li>SQL Warehouses</li></ul></div>
  <div class="card orange"><h3>⚙️ Data Engineering</h3><ul><li>Job Runs</li><li>Data Ingestion (Lakeflow Connect, file upload)</li></ul></div>
  <div class="card purple"><h3>🤖 AI / ML</h3><ul><li>Playground</li><li>Experiments (MLflow)</li><li>Features</li><li>Models</li><li>Serving</li></ul></div>
</div>
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · Workspace objects & folders
# MAGIC
# MAGIC | Object | Notes |
# MAGIC |---|---|
# MAGIC | **Folders** | `/Workspace/Users/<you>` (your **Home**), `/Workspace/Shared` (everyone), and others you create |
# MAGIC | **Notebooks** | Cells of code + markdown. Stored as **source** (`.py`, `.sql`, `.scala`, `.r`) or **`.ipynb`** |
# MAGIC | **Files** (workspace files) | Any file next to your notebooks: `.py` modules, `.yml`, small `.csv` — importable with `import` |
# MAGIC | **Git folders** | A folder synced with a Git repository (🕰️ formerly *Repos*) → Section 11 |
# MAGIC | **Queries, dashboards, alerts, Genie spaces** | Databricks SQL objects → Section 09 |
# MAGIC
# MAGIC **Import / export formats:** Source file · IPython notebook (`.ipynb`) · HTML (read-only) · DBC archive (🕰️ legacy, many notebooks) · ZIP of source files.
# MAGIC
# MAGIC > ✅ **Best practice:** keep production code in **Git folders**, not in personal Home folders.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · Anatomy of a notebook
# MAGIC
# MAGIC * Every notebook has a **default language** (shown next to the title). Each cell runs in it **unless** the cell starts with a **magic command**.
# MAGIC * Cells share **one execution context**: a variable defined in one Python cell is visible in the next.
# MAGIC * A notebook is attached to **compute** (serverless, a cluster, or a SQL warehouse for SQL-only notebooks).

# COMMAND ----------

# DBTITLE 1,Slide · Magic commands
show("""
<div class="kicker">Slide 3 · Magic commands</div>
<h2>Switch language or behaviour for one cell</h2>
<table class="tbl">
<tr><th>Magic</th><th>What it does</th><th>Example</th><th>Serverless?</th></tr>
<tr><td><code>%python</code> <code>%sql</code></td><td>Run the cell in another language</td><td><code>%sql SELECT 1</code></td><td>✅</td></tr>
<tr><td><code>%scala</code> <code>%r</code></td><td>Scala / R cell</td><td><code>%scala val x = 1</code></td><td>❌ classic compute only</td></tr>
<tr><td><code>%md</code></td><td>Markdown documentation cell</td><td><code>%md # Title</code></td><td>✅</td></tr>
<tr><td><code>%run</code></td><td>Include another notebook <b>in the current context</b></td><td><code>%run ./helpers</code></td><td>✅</td></tr>
<tr><td><code>%fs</code></td><td>Shortcut for <code>dbutils.fs</code></td><td><code>%fs ls /Volumes/…</code></td><td>✅ (volumes)</td></tr>
<tr><td><code>%sh</code></td><td>Shell command on the <b>driver</b> node only</td><td><code>%sh ls /tmp</code></td><td>⚠️ limited</td></tr>
<tr><td><code>%pip</code></td><td>Install a <b>notebook-scoped</b> Python library (restarts Python)</td><td><code>%pip install faker</code></td><td>✅</td></tr>
</table>
""" + callout("trap", "<code>%sh</code> runs <b>only on the driver</b>, not on the workers. To read files distributed across the cluster use Spark, not shell commands.")
    + callout("exam", "<code>%pip install</code> at the top of a notebook = library available <b>only for this notebook's session</b> (notebook-scoped)."))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · Seeing results: `display()` vs `.show()`
# MAGIC
# MAGIC | | `display(df)` | `df.show()` |
# MAGIC |---|---|---|
# MAGIC | Output | Interactive table (sort, filter, search, download) | Plain text |
# MAGIC | Visualizations & data profile | ✅ built-in (bar, line, map …) | ❌ |
# MAGIC | Rows returned | Up to 10,000 rows / 2 MB (preview) | 20 by default |
# MAGIC | Works on | Databricks notebooks | Anywhere Spark runs |
# MAGIC
# MAGIC Also: `displayHTML("<b>html</b>")` renders HTML (used by these slides!).

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 · `dbutils` — Databricks Utilities

# COMMAND ----------

# DBTITLE 1,Slide · dbutils modules
show("""
<div class="kicker">Slide 5 · dbutils</div>
<h2>The five utilities you must know</h2>
<div class="grid">
  <div class="card"><h3>📂 dbutils.fs</h3><code>ls</code> · <code>head</code> · <code>mkdirs</code> · <code>put</code> · <code>cp</code> · <code>mv</code> · <code>rm</code>
     <p class="muted">Works with <code>/Volumes/…</code> (and legacy <code>dbfs:/</code>) paths. <code>%fs</code> is its shortcut.</p></div>
  <div class="card green"><h3>🎛️ dbutils.widgets</h3><code>text</code> · <code>dropdown</code> · <code>combobox</code> · <code>multiselect</code> · <code>get</code> · <code>remove</code> · <code>removeAll</code>
     <p class="muted">Notebook parameters — also how jobs pass parameters to notebook tasks.</p></div>
  <div class="card orange"><h3>📓 dbutils.notebook</h3><code>run(path, timeout, args)</code> · <code>exit(value)</code>
     <p class="muted">Run another notebook as a separate run and get a return value.</p></div>
  <div class="card purple"><h3>🔐 dbutils.secrets</h3><code>get(scope, key)</code> · <code>list</code> · <code>listScopes</code>
     <p class="muted">Read credentials without hard-coding them. Printed values show as <code>[REDACTED]</code>.</p></div>
  <div class="card teal"><h3>🔗 dbutils.jobs.taskValues</h3><code>set(key, value)</code> · <code>get(taskKey, key)</code>
     <p class="muted">Pass small values between tasks of a job → Section 10.</p></div>
</div>
<p>Help is built in: <code>dbutils.help()</code>, <code>dbutils.fs.help()</code>, <code>dbutils.fs.help("cp")</code>.</p>
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6 · 🎯 `%run` vs `dbutils.notebook.run()` — exam favourite

# COMMAND ----------

# DBTITLE 1,Slide · %run vs notebook.run
show("""
<div class="kicker">Slide 6 · Modularising notebooks</div>
<h2>Two ways to call another notebook</h2>
<table class="tbl">
<tr><th></th><th><code>%run ./other_notebook</code></th><th><code>dbutils.notebook.run("./other_notebook", 600, {"p": "1"})</code></th></tr>
<tr><td>Execution context</td><td><b>Same</b> context — like copy-pasting the code in</td><td><b>New, separate</b> run (ephemeral job)</td></tr>
<tr><td>Variables &amp; functions</td><td>✅ Become available in the caller</td><td>❌ Not shared</td></tr>
<tr><td>Parameters</td><td>Only via widgets / variables set before</td><td>✅ <code>arguments</code> dict → read with <code>dbutils.widgets.get()</code></td></tr>
<tr><td>Return value</td><td>—</td><td>✅ String passed to <code>dbutils.notebook.exit(value)</code></td></tr>
<tr><td>Dynamic path / loops / if</td><td>❌ Path must be a literal; must be alone in its cell</td><td>✅ Normal Python: loops, conditions, parallel threads</td></tr>
<tr><td>Typical use</td><td>Shared setup, config, helper functions</td><td>Simple notebook workflows with branching</td></tr>
</table>
""" + callout("exam", "\"Make the functions defined in <i>helpers</i> available in this notebook\" → <b>%run</b>. "
              "\"Run a notebook with parameters and use its return value\" → <b>dbutils.notebook.run()</b>.")
    + callout("tip", "For real orchestration (retries, schedules, dependencies) use <b>Lakeflow Jobs</b> tasks instead of chaining notebooks in code."))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7 · Widgets — parameterising notebooks
# MAGIC
# MAGIC ```python
# MAGIC dbutils.widgets.text("start_date", "2026-01-01", "Start date")          # free text
# MAGIC dbutils.widgets.dropdown("country", "France", ["France", "Japan", "Egypt"])  # fixed choices
# MAGIC country = dbutils.widgets.get("country")                               # always returns a STRING
# MAGIC ```
# MAGIC ```sql
# MAGIC -- In SQL cells, reference a widget with a named parameter marker
# MAGIC SELECT * FROM customers WHERE profile:address:country = :country
# MAGIC ```
# MAGIC * Widgets appear at the top of the notebook. When the notebook runs as a **job task**, job/task **parameters override** the widget defaults.
# MAGIC * `dbutils.widgets.get()` always returns a **string** → cast it yourself (`int(...)`).
# MAGIC * 🕰️ Old syntax `${country}` / `$country` in SQL is **deprecated** — use `:country`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8 · Python ⇄ SQL in the same notebook
# MAGIC
# MAGIC | Direction | How |
# MAGIC |---|---|
# MAGIC | Python → SQL | `df.createOrReplaceTempView("v")` then `%sql SELECT * FROM v` |
# MAGIC | SQL → Python | `spark.sql("SELECT ...")` returns a DataFrame; the last `%sql` cell's result is also available as `_sqldf` |
# MAGIC | Values into SQL safely | `spark.sql("SELECT * FROM t WHERE c = :c", args={"c": value})` — **parameterised**, no string concatenation |
# MAGIC | Tables / views | Both languages see the same tables and temp views — they share one Spark session |
# MAGIC
# MAGIC > ⚠️ **Exam trap:** a **temporary view** exists only in the current **Spark session** (this notebook). Another notebook can't see it.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9 · Productivity & collaboration features
# MAGIC
# MAGIC | Feature | Why it matters |
# MAGIC |---|---|
# MAGIC | **Version history** | Every save is kept → restore an older version (File ▸ Version history) |
# MAGIC | **Comments** | Highlight code and discuss with teammates |
# MAGIC | **Real-time co-editing** | Several people in the same notebook |
# MAGIC | **Databricks Assistant** | AI help: write, explain and fix code (`Ctrl/Cmd + I`) |
# MAGIC | **Variable explorer & debugger** | Inspect Python variables, set breakpoints |
# MAGIC | **Schedule** button | Turns the notebook into a job task → Section 10 |
# MAGIC | **Git folders** | Branches, commits, pull requests → Section 11 |

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Key takeaways
# MAGIC 1. Magic commands change **one cell**: `%sql`, `%python`, `%md`, `%run`, `%fs`, `%sh` (driver only), `%pip` (notebook-scoped).
# MAGIC 2. `dbutils` = **fs**, **widgets**, **notebook**, **secrets**, **jobs.taskValues**.
# MAGIC 3. `%run` → same context (share variables); `dbutils.notebook.run()` → separate run with parameters and a return value.
# MAGIC 4. Widgets are notebook parameters; in SQL use `:name`; values are strings.
# MAGIC 5. Temp views are **session-scoped**; `spark.sql()` and `_sqldf` bridge SQL and Python.
# MAGIC
# MAGIC ➡️ Next: **01-P3 · Compute & Cost**
