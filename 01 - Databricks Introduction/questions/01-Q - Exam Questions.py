# Databricks notebook source
# MAGIC %md
# MAGIC # 📝 01-Q · Exam Questions — Databricks Introduction & Platform
# MAGIC **28 exam-style questions** covering 01-P1 (lakehouse & architecture), 01-P2 (workspace & notebooks) and 01-P3 (compute & cost).
# MAGIC
# MAGIC * Click **Check answer** for instant feedback and an explanation of *why the other options are wrong*.
# MAGIC * **📊 Show my result** gives your score per topic. Target: **≥ 80 %** before moving to Section 02.
# MAGIC * Questions marked **Choose 2** need exactly two answers.
# MAGIC
# MAGIC > ▶️ **Run all** — renders in a few seconds on any compute.

# COMMAND ----------

# MAGIC %run ../../Includes/_quiz

# COMMAND ----------

# DBTITLE 1,Question bank (collapse this cell - it contains the answers)
questions = [
    # ---------------- Lakehouse & architecture ----------------
    {"topic": "Lakehouse & architecture",
     "q": "Which statement best describes the **lakehouse** architecture?",
     "options": ["A data warehouse that copies data from the lake into a proprietary format for BI",
                 "A single platform that stores data in open formats on low-cost object storage and adds warehouse capabilities such as ACID transactions and governance",
                 "A data lake without schemas used only for machine learning",
                 "A set of separate systems for BI, data science and streaming connected by ETL jobs"],
     "answer": 1,
     "explanation": "The lakehouse keeps ONE copy of data in open formats (Delta Lake / Iceberg) on cloud object storage and adds reliability (ACID), performance and governance (Unity Catalog), serving BI, streaming and ML alike."},
    {"topic": "Lakehouse & architecture",
     "q": "In a Databricks deployment using **classic compute**, where are the **table data files** stored?",
     "options": ["In the Databricks control plane", "On the driver node's local disk",
                 "In cloud object storage (e.g. S3, ADLS, GCS)", "Inside the Unity Catalog metastore database"],
     "answer": 2,
     "explanation": "Data lives in cloud object storage. The control plane holds the web app, notebooks, job definitions and metadata — not your table files. Local disks are ephemeral."},
    {"topic": "Lakehouse & architecture",
     "q": "Where does **serverless** compute run?",
     "options": ["In the control plane, alongside the web application", "In the customer's own cloud account (VPC/VNet)",
                 "In the serverless compute plane, inside the Databricks cloud account, isolated per workspace", "On the user's laptop"],
     "answer": 2,
     "explanation": "Serverless compute runs in a separate serverless compute plane managed by Databricks (same region). Classic compute runs in your cloud account."},
    {"topic": "Lakehouse & architecture",
     "q": "Which **two** components belong to the Databricks **control plane**?",
     "options": ["The web application (workspace UI)", "Spark executors of a classic all-purpose cluster",
                 "The job scheduler", "Parquet files of a managed Delta table"],
     "answer": [0, 2],
     "explanation": "The control plane hosts the web app, notebooks, job scheduler and compute management. Executors of classic compute run in the classic compute plane (your account); data files sit in object storage."},
    {"topic": "Lakehouse & architecture",
     "q": "What is **Delta Lake**?",
     "options": ["A managed relational database service run by Databricks",
                 "An open-source storage layer that adds ACID transactions, schema enforcement and time travel on top of Parquet files using a transaction log",
                 "A proprietary file format that can only be read by Photon",
                 "The Databricks streaming engine"],
     "answer": 1,
     "explanation": "Delta Lake = Parquet data files + a `_delta_log` transaction log. It's open source and is the default table format on Databricks."},
    {"topic": "Lakehouse & architecture",
     "q": "Which is the correct Unity Catalog hierarchy, from top to bottom?",
     "options": ["Catalog → Metastore → Schema → Table", "Metastore → Catalog → Schema → Table",
                 "Workspace → Schema → Catalog → Table", "Metastore → Schema → Catalog → Table"],
     "answer": 1,
     "explanation": "Metastore (one per region) → catalog → schema (database) → tables, views, volumes, functions, models. Objects are addressed as `catalog.schema.object`."},
    {"topic": "Lakehouse & architecture",
     "q": "A team must store **PDF contracts and JSON files** in a governed way, with Unity Catalog permissions and lineage. What should they use?",
     "options": ["A DBFS mount point under `/mnt/contracts`", "The DBFS `/FileStore` folder",
                 "A Unity Catalog volume, e.g. `/Volumes/legal/contracts/raw/`", "A global temporary view"],
     "answer": 2,
     "explanation": "Volumes govern non-tabular files with UC privileges (READ VOLUME / WRITE VOLUME), audit and lineage. Mounts and /FileStore are legacy DBFS patterns with coarse access control."},
    {"topic": "Lakehouse & architecture",
     "q": "Which product was formerly known as **Delta Live Tables (DLT)**?",
     "options": ["Lakeflow Jobs", "Lakeflow Connect", "Lakeflow Spark Declarative Pipelines", "Databricks SQL"],
     "answer": 2,
     "explanation": "DLT → Lakeflow (Spark) Declarative Pipelines. Workflows → Lakeflow Jobs. Lakeflow Connect provides ingestion connectors."},
    {"topic": "Lakehouse & architecture",
     "q": "A requirement says: *\"ingest Salesforce data with no code\"*. Which Lakeflow component fits best?",
     "options": ["Lakeflow Connect managed connectors", "Lakeflow Jobs", "Auto Loader in a notebook", "Git folders"],
     "answer": 0,
     "explanation": "Lakeflow Connect **managed** connectors ingest from SaaS apps (Salesforce, Workday, …) and databases with a UI/API and no custom code. Auto Loader is for files in cloud storage."},

    # ---------------- Workspace & notebooks ----------------
    {"topic": "Notebooks",
     "q": "Notebook `etl` must use functions defined in notebook `helpers` (same folder). The functions must be available **as if defined in `etl`**. What should you use?",
     "options": ["`dbutils.notebook.run(\"./helpers\", 60)`", "`%run ./helpers`", "`import helpers`", "`%sh python helpers.py`"],
     "answer": 1,
     "explanation": "`%run` executes the other notebook in the SAME execution context, so its variables and functions become available. `dbutils.notebook.run` starts a separate run and shares nothing."},
    {"topic": "Notebooks",
     "q": "You need to run notebook `child` with the parameter `date=2026-07-01` and use a **value it returns**. Which combination is correct?",
     "options": ["`%run ./child $date=2026-07-01` and read `_sqldf`",
                 "`dbutils.notebook.run(\"./child\", 600, {\"date\": \"2026-07-01\"})` in the caller and `dbutils.notebook.exit(value)` in the child",
                 "`dbutils.widgets.run(\"./child\")` and `return value`",
                 "`spark.sql(\"RUN NOTEBOOK child\")`"],
     "answer": 1,
     "explanation": "dbutils.notebook.run passes arguments (read in the child with dbutils.widgets.get) and returns the string given to dbutils.notebook.exit()."},
    {"topic": "Notebooks",
     "q": "Where does a `%sh` command run on a multi-node cluster?",
     "options": ["On every worker node", "On the driver node only", "In the control plane", "On a SQL warehouse"],
     "answer": 1,
     "explanation": "%sh runs a shell on the driver only. Use Spark (or dbutils.fs) for distributed file processing."},
    {"topic": "Notebooks",
     "q": "A notebook starts with `%pip install great-expectations`. What is the scope of this library?",
     "options": ["All notebooks on the cluster, permanently", "All notebooks in the workspace",
                 "Only the current notebook's Python environment (notebook-scoped)", "Only the next cell"],
     "answer": 2,
     "explanation": "%pip installs notebook-scoped libraries: other notebooks attached to the same compute are not affected. The Python process restarts, so put %pip at the top."},
    {"topic": "Notebooks",
     "q": "A text widget `batch_size` contains `500`. What does `dbutils.widgets.get(\"batch_size\")` return?",
     "options": ["The integer 500", "The string \"500\"", "A Column object", "None until the notebook runs as a job"],
     "answer": 1,
     "explanation": "Widget values are always strings — cast them yourself, e.g. `int(dbutils.widgets.get(\"batch_size\"))`."},
    {"topic": "Notebooks",
     "q": "Which is the **current** way to reference a widget named `country` inside a `%sql` cell?",
     "options": ["`WHERE country = ${country}`", "`WHERE country = :country`", "`WHERE country = $country`", "`WHERE country = widget('country')`"],
     "answer": 1,
     "explanation": "Named parameter markers (`:country`) are the current syntax. `${country}` / `$country` are deprecated legacy substitutions."},
    {"topic": "Notebooks",
     "q": "In notebook A you run `df.createOrReplaceTempView(\"sales_v\")`. Who can query `sales_v`?",
     "options": ["Every user in the workspace", "Any notebook attached to the same compute",
                 "Only the Spark session that created it (notebook A)", "Anyone with SELECT on the schema"],
     "answer": 2,
     "explanation": "Temporary views are session-scoped. Another notebook has its own Spark session and can't see it. Use a real VIEW or table to share."},
    {"topic": "Notebooks",
     "q": "Which `dbutils.fs` command shows the **first bytes of a file** without reading it into a DataFrame?",
     "options": ["`dbutils.fs.ls`", "`dbutils.fs.head`", "`dbutils.fs.cat`", "`dbutils.fs.peek`"],
     "answer": 1,
     "explanation": "`dbutils.fs.head(path, maxBytes)` returns the beginning of a file as a string. `ls` lists a directory. `cat`/`peek` don't exist."},
    {"topic": "Notebooks",
     "q": "Which **two** statements about `display(df)` in a Databricks notebook are true?",
     "options": ["It renders an interactive table that can be turned into a visualization",
                 "It always writes the DataFrame to a Delta table",
                 "It can show a data profile (summary statistics) of the result",
                 "It is identical to `df.show()` and prints plain text"],
     "answer": [0, 2],
     "explanation": "display() gives an interactive, sortable table with built-in visualizations and data profiles. show() prints plain text; neither writes data."},

    # ---------------- Compute & cost ----------------
    {"topic": "Compute & cost",
     "q": "Which classic compute type is **created by the job scheduler**, **terminates when the run finishes**, and has a **lower DBU rate**?",
     "options": ["All-purpose compute", "Job compute", "Instance pool", "Pro SQL warehouse"],
     "answer": 1,
     "explanation": "Job compute is ephemeral per job run and cheaper per DBU. All-purpose compute is for interactive work and costs more."},
    {"topic": "Compute & cost",
     "q": "A team's production pipeline is written in **Scala** and uses a custom **JAR** library. Which compute should run it?",
     "options": ["Serverless compute for jobs", "Classic job compute", "Serverless SQL warehouse", "Serverless compute for notebooks"],
     "answer": 1,
     "explanation": "Serverless notebooks/jobs support Python and SQL, and don't support JAR libraries in notebooks. Classic job compute supports Scala and JARs."},
    {"topic": "Compute & cost",
     "q": "Which streaming trigger is supported on **serverless** compute?",
     "options": ["`trigger(processingTime=\"1 minute\")`", "`trigger(continuous=\"1 second\")`",
                 "`trigger(availableNow=True)`", "No streaming is supported on serverless"],
     "answer": 2,
     "explanation": "Serverless supports availableNow (and the deprecated once). Fixed-interval processingTime and continuous triggers are not supported."},
    {"topic": "Compute & cost",
     "q": "A notebook on **serverless** compute calls `df.createOrReplaceGlobalTempView(\"g\")`. What happens?",
     "options": ["It works and the view is visible to all notebooks on serverless", "It fails — global temporary views are not supported on serverless",
                 "It silently creates a permanent view", "It creates a table in the `global_temp` catalog"],
     "answer": 1,
     "explanation": "Global temp views aren't supported on serverless. Use a temporary view (session) or a real Unity Catalog view."},
    {"topic": "Compute & cost",
     "q": "Analysts need a SQL warehouse that can reach a **database inside the company's private network**. Serverless networking options are not approved. Which warehouse type should you choose?",
     "options": ["Serverless", "Pro", "Classic job compute", "All-purpose compute with Photon"],
     "answer": 1,
     "explanation": "Pro SQL warehouses run in your cloud account, so they can use custom networking (VPC peering, private endpoints to on-prem/in-VPC sources)."},
    {"topic": "Compute & cost",
     "q": "Many short scheduled jobs on **classic** compute spend most of their time waiting for VMs to start. What reduces start-up time?",
     "options": ["Enabling Photon", "Using an instance pool of idle, ready-to-use instances",
                 "Increasing auto-termination to 120 minutes", "Switching to the ML runtime"],
     "answer": 1,
     "explanation": "Pools keep pre-warmed VMs so compute starts and scales faster. Note: idle pool VMs incur cloud-provider costs (but no DBUs)."},
    {"topic": "Compute & cost",
     "q": "How should **spot instances** be used on classic compute to save cost without risking the job?",
     "options": ["Spot for the driver, on-demand for workers", "Spot for the workers of fault-tolerant jobs, on-demand for the driver",
                 "Spot for all nodes of interactive clusters", "Spot instances cannot be used on Databricks"],
     "answer": 1,
     "explanation": "Spot VMs are discounted but can be reclaimed. Losing a worker is tolerated by Spark; losing the driver kills the job — keep the driver on-demand."},
    {"topic": "Compute & cost",
     "q": "Which system table contains **DBU consumption records** you can use to analyse spend per product or per compute tag?",
     "options": ["`system.billing.usage`", "`system.access.audit`", "`information_schema.tables`", "`system.compute.clusters`"],
     "answer": 0,
     "explanation": "`system.billing.usage` holds usage (DBUs) with sku, product and custom_tags; `system.billing.list_prices` has list prices."},
    {"topic": "Compute & cost",
     "q": "A data scientist must run **R** notebooks on classic compute with Unity Catalog. Which access mode is required?",
     "options": ["Standard", "Dedicated (single user or group)", "No isolation shared", "Serverless"],
     "answer": 1,
     "explanation": "Dedicated access mode supports Python, SQL, Scala and R (plus ML runtimes). Standard supports Python, SQL and Scala; serverless supports Python and SQL."},
    {"topic": "Compute & cost",
     "q": "Developers often forget to stop their all-purpose clusters, which keep costing money overnight. Which setting fixes this?",
     "options": ["Autoscaling", "Auto-termination after N minutes of inactivity", "Photon acceleration", "Choosing an LTS runtime"],
     "answer": 1,
     "explanation": "Auto-termination shuts down all-purpose compute after an inactivity period. Autoscaling changes the number of workers but doesn't stop the cluster."},
]

render_quiz(questions, title="Section 01 · Databricks Introduction & Platform",
            meta="28 questions · D1 Databricks Intelligence Platform + notebook essentials · target ≥ 80 %")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 📌 If you scored below 80 %
# MAGIC | Weak topic | Review |
# MAGIC |---|---|
# MAGIC | Lakehouse & architecture | `presentation/01-P1 - Databricks and the Lakehouse` |
# MAGIC | Notebooks | `presentation/01-P2 - Workspace and Notebooks` + `labs/01-L1` |
# MAGIC | Compute & cost | `presentation/01-P3 - Compute and Cost` + `labs/01-L2` |

# COMMAND ----------

# DBTITLE 1,Optional - print the answer key
# print_answer_key(questions)
