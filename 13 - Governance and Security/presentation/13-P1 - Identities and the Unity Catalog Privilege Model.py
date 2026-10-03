# Databricks notebook source
# MAGIC %md
# MAGIC # 🔐 13-P1 · Identities and the Unity Catalog Privilege Model
# MAGIC **Section 13** · exam objective **7.2** — *configure access controls using the UI and SQL by applying GRANT, REVOKE, and
# MAGIC DENY privileges to principals*
# MAGIC
# MAGIC | After this presentation you can… |
# MAGIC |---|
# MAGIC | Name the **principals** (users, service principals, groups) and the **admin roles** (account, metastore, workspace) |
# MAGIC | Explain **ownership**, the **securable hierarchy** and **privilege inheritance** |
# MAGIC | Pick the **minimum privileges** for a task (read a table, create a table, read a volume, run a function) |
# MAGIC | Write and read **GRANT / REVOKE / SHOW GRANTS** — and do the same in Catalog Explorer |
# MAGIC | Answer the **DENY** question correctly: hive_metastore table ACLs vs Unity Catalog |
# MAGIC
# MAGIC > ▶️ **Run all** to render the diagrams. Section 13 = Domain 7 of the exam (**15 %**).

# COMMAND ----------

# MAGIC %run ../../Includes/_style

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · Unity Catalog in one picture

# COMMAND ----------

# DBTITLE 1,Slide · One governance layer
show("""
<div class="kicker">Slide 1 · Define access once, enforce it in every workspace, on every compute</div>
<div class="grid two">
 <div class="card gray"><h3>🕰️ Before Unity Catalog</h3><ul><li>one <b>Hive metastore per workspace</b></li><li>users, groups and table ACLs <b>per workspace</b></li>
 <li>access to files (mounts, DBFS) bypassed table ACLs</li><li>no lineage, no central audit</li></ul></div>
 <div class="card green"><h3>✅ With Unity Catalog</h3><ul><li>one <b>metastore per region</b>, attached to many workspaces</li><li><b>account-level</b> identities</li>
 <li>one privilege model for tables, views, volumes, functions, models</li><li>built-in <b>lineage</b>, <b>audit logs</b>, <b>tags</b>, search, row/column security</li></ul></div>
</div>
<div class="flow">
 <div class="step"><b>🏛️ Metastore</b>top container (per region)</div><div class="arrow">➜</div>
 <div class="step"><b>📚 Catalog</b>e.g. <code>prod</code>, <code>dev</code>, <code>workspace</code></div><div class="arrow">➜</div>
 <div class="step"><b>🗂️ Schema</b>e.g. <code>shopwave</code></div><div class="arrow">➜</div>
 <div class="step"><b>📄 Objects</b>tables · views · materialized views · streaming tables · volumes · functions · models</div>
</div>
<p class="muted">Also securable at metastore level: storage credentials, external locations, connections, service credentials, shares, recipients.</p>
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · Principals and admin roles

# COMMAND ----------

# DBTITLE 1,Slide · Who
show("""
<div class="kicker">Slide 2 · A principal is anything you can GRANT to</div>
<table class="tbl">
<tr><th>Principal</th><th>Identified by</th><th>Use it for</th></tr>
<tr><td>👤 <b>User</b></td><td>e-mail / user name</td><td>people</td></tr>
<tr><td>🤖 <b>Service principal</b></td><td>application ID</td><td>automation: jobs, CI/CD, apps — production jobs <b>run as</b> a service principal</td></tr>
<tr><td>👥 <b>Group</b></td><td>name</td><td>collections of users and service principals; groups can be <b>nested</b>. Built-in: <code>account users</code> (everyone)</td></tr>
</table>
<div class="grid">
 <div class="card"><h3>🌐 Account level + identity federation</h3>identities live in the <b>account</b> (often synced from Entra ID / Okta via <b>SCIM</b>) and are
 <b>assigned</b> to workspaces — one user, same identity everywhere. Old <i>workspace-local groups</i> can't be used in Unity Catalog grants.</div>
 <div class="card orange"><h3>🛡️ Admin roles</h3><ul><li><b>Account admin</b> — account console: metastores, workspaces, identities</li>
 <li><b>Metastore admin</b> (optional) — can manage all objects &amp; grants of a metastore</li>
 <li><b>Workspace admin</b> — workspace settings, users in the workspace, compute, workspace object ACLs</li></ul></div>
 <div class="card green"><h3>✅ Best practice</h3>grant to <b>groups</b>, not to individual users; make a <b>group</b> the owner of production objects;
 run jobs as <b>service principals</b>.</div>
</div>
""" + callout("info", "Free Edition: one user, no account console, no SSO/SCIM. You can still create groups in the workspace settings and grant to "
              "the built-in group <code>account users</code> — enough to practise every statement in this section."))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · Ownership
# MAGIC
# MAGIC * Every securable has **exactly one owner** — a user, a service principal or (best) a **group**. The **creator** is the
# MAGIC   initial owner.
# MAGIC * The owner has **all privileges** on the object and can **grant** them to others.
# MAGIC * Owners of the **parent** catalog/schema can also manage the objects inside (grant, drop, transfer ownership), as can
# MAGIC   **metastore admins** and principals with the **`MANAGE`** privilege.
# MAGIC * Transfer: ``ALTER TABLE sec_orders OWNER TO `shopwave_admins` `` (same for `CATALOG`, `SCHEMA`, `VOLUME`, `VIEW`,
# MAGIC   `FUNCTION`…). See it: `DESCRIBE TABLE EXTENDED …` → *Owner*, or Catalog Explorer → *Details*.
# MAGIC
# MAGIC ## 4 · Privileges and inheritance

# COMMAND ----------

# DBTITLE 1,Slide · Privileges
show("""
<div class="kicker">Slide 4 · What a principal may DO on a securable</div>
<table class="tbl">
<tr><th>Privilege</th><th>Granted on</th><th>Allows</th></tr>
<tr><td><b>USE CATALOG</b> · <b>USE SCHEMA</b></td><td>catalog · schema (or catalog)</td><td>“enter” the container — required to do <i>anything</i> inside, but gives no data access by itself</td></tr>
<tr><td><b>BROWSE</b></td><td>catalog</td><td>see objects and metadata in Catalog Explorer/search without data access (discovery, request access)</td></tr>
<tr><td><b>SELECT</b></td><td>table, view, MV (or schema/catalog)</td><td>read data</td></tr>
<tr><td><b>MODIFY</b></td><td>table (or schema/catalog)</td><td>INSERT, UPDATE, DELETE, MERGE</td></tr>
<tr><td><b>CREATE TABLE</b> · <b>CREATE VOLUME</b> · <b>CREATE FUNCTION</b> · <b>CREATE MATERIALIZED VIEW</b></td><td>schema (or catalog)</td><td>create objects in the schema (CREATE TABLE also covers views)</td></tr>
<tr><td><b>CREATE SCHEMA</b></td><td>catalog</td><td>create schemas</td></tr>
<tr><td><b>EXECUTE</b></td><td>function (or schema/catalog)</td><td>call a UDF / load a model</td></tr>
<tr><td><b>READ VOLUME</b> · <b>WRITE VOLUME</b></td><td>volume (or schema/catalog)</td><td>read / write files in a volume</td></tr>
<tr><td><b>READ FILES</b> · <b>WRITE FILES</b> · <b>CREATE EXTERNAL TABLE</b></td><td>external location</td><td>path-based access to cloud storage; create external tables there</td></tr>
<tr><td><b>APPLY TAG</b> · <b>REFRESH</b></td><td>object (or parent)</td><td>add tags · refresh a materialized view</td></tr>
<tr><td><b>MANAGE</b></td><td>object</td><td>manage grants, transfer ownership, drop — <b>without</b> data access</td></tr>
<tr><td><b>ALL PRIVILEGES</b></td><td>object</td><td>every applicable privilege (incl. future ones) — but <b>not</b> MANAGE or EXTERNAL USE SCHEMA</td></tr>
</table>
""" + callout("exam", "Minimum to <b>read</b> <code>prod.sales.orders</code>: <b>USE CATALOG</b> on <code>prod</code> + <b>USE SCHEMA</b> on <code>prod.sales</code> + "
              "<b>SELECT</b> on the table. Missing USE CATALOG/USE SCHEMA is the #1 reason for “table not found / permission denied”."))

# COMMAND ----------

# DBTITLE 1,Slide · Inheritance
show("""
<div class="kicker">Slide 5 · Grants flow DOWN the hierarchy — to current AND future objects</div>
<div class="flow">
 <div class="step"><b>GRANT SELECT ON CATALOG prod TO analysts</b>every table/view in every schema of <code>prod</code></div><div class="arrow">➜</div>
 <div class="step"><b>GRANT SELECT ON SCHEMA prod.sales TO analysts</b>every table/view of <code>prod.sales</code>, incl. tables created tomorrow</div><div class="arrow">➜</div>
 <div class="step"><b>GRANT SELECT ON TABLE prod.sales.orders TO analysts</b>only this table</div>
</div>
<div class="grid">
 <div class="card"><h3>🧮 Effective privileges</h3>= direct grants + grants inherited from the schema and catalog + grants to <b>groups</b> you belong to (nested too) + ownership</div>
 <div class="card red"><h3>⚠️ Inheritance trap</h3><code>REVOKE SELECT ON TABLE orders FROM analysts</code> does <b>nothing</b> if the SELECT comes from the <b>schema</b>.
 Revoke where it was granted — <code>SHOW GRANTS</code> shows the <code>objectType</code> it is inherited from.</div>
 <div class="card green"><h3>✅ Design</h3>grant broad, stable privileges high (USE, SELECT on a schema of curated gold tables); keep sensitive tables in their own schema/catalog.</div>
</div>
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6 · SQL: GRANT, REVOKE, SHOW GRANTS
# MAGIC
# MAGIC ```sql
# MAGIC -- give a group read access to one gold table
# MAGIC GRANT USE CATALOG ON CATALOG workspace TO `shopwave_analysts`;
# MAGIC GRANT USE SCHEMA  ON SCHEMA  workspace.shopwave TO `shopwave_analysts`;
# MAGIC GRANT SELECT      ON TABLE   workspace.shopwave.sec_orders TO `shopwave_analysts`;
# MAGIC
# MAGIC -- several privileges at once, on the schema (inherited by all its tables)
# MAGIC GRANT SELECT, MODIFY, CREATE TABLE ON SCHEMA workspace.shopwave TO `shopwave_engineers`;
# MAGIC
# MAGIC -- take it back
# MAGIC REVOKE MODIFY ON SCHEMA workspace.shopwave FROM `shopwave_engineers`;
# MAGIC
# MAGIC -- what affects this table? (direct AND inherited grants: principal, actionType, objectType, objectKey)
# MAGIC SHOW GRANTS ON TABLE workspace.shopwave.sec_orders;
# MAGIC SHOW GRANTS `shopwave_analysts` ON SCHEMA workspace.shopwave;      -- one principal
# MAGIC
# MAGIC -- the same as data
# MAGIC SELECT grantee, table_name, privilege_type, inherited_from
# MAGIC FROM workspace.information_schema.table_privileges WHERE table_schema = 'shopwave';
# MAGIC ```
# MAGIC
# MAGIC * Principal names with special characters (e-mails, spaces) go in **backticks**: `` `ahmed@example.com` ``,
# MAGIC   `` `account users` ``.
# MAGIC * Privilege names with spaces are written with spaces: `USE SCHEMA`, `CREATE TABLE` (older docs: `USAGE`, `CREATE_TABLE`).
# MAGIC * **UI:** Catalog Explorer → object → **Permissions** tab → **Grant** (pick principals + privileges; templates such as
# MAGIC   *Data Reader / Data Editor*) · select a row → **Revoke**. Same result, same audit log.
# MAGIC
# MAGIC ## 7 · What about DENY?

# COMMAND ----------

# DBTITLE 1,Slide · DENY
show("""
<div class="kicker">Slide 7 · The exam guide says GRANT, REVOKE <u>and DENY</u> — know exactly where DENY exists</div>
<div class="grid">
 <div class="card gray"><h3>🕰️ hive_metastore table ACLs</h3><code>DENY SELECT ON TABLE hive_metastore.hr.salaries TO `interns`</code><br>
 DENY <b>wins</b> over any grant; a deny on a schema denies all its tables. Undo it with <b>REVOKE</b> of the same privilege.
 Legacy privileges: SELECT, MODIFY, CREATE, <b>READ_METADATA</b>, <b>USAGE</b>, CREATE_NAMED_FUNCTION, ALL PRIVILEGES, on CATALOG / SCHEMA / TABLE / VIEW / FUNCTION / <b>ANY FILE</b>.</div>
 <div class="card green"><h3>✅ Unity Catalog</h3><b>allow-only</b> model: there is <b>no DENY statement</b>. Nobody has access until it is granted
 (directly, inherited, via a group, or by ownership); you remove access with <b>REVOKE</b> (where it was granted) or by removing the user from the group.</div>
 <div class="card orange"><h3>🆕 ABAC DENY policies (Beta)</h3><code>CREATE POLICY … DENY MANAGE ACCESS CONTROL FOR TABLES WHEN has_tag('sensitive')</code><br>
 deny-by-policy for <b>one</b> privilege only — <code>MANAGE ACCESS CONTROL</code> (who may change grants) — on tagged objects. Not SELECT/MODIFY.</div>
</div>
""" + callout("trap", "A question that needs “everyone in finance except interns” in Unity Catalog → grant to a group that excludes the interns (or use a "
              "row filter / ABAC policy with <code>EXCEPT</code>), not DENY. If the question is about <b>hive_metastore</b> / table ACLs, DENY is valid."))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8 · Compute, workspaces and identity
# MAGIC
# MAGIC | Setting | Why it matters for security |
# MAGIC |---|---|
# MAGIC | **Access mode** — *Standard* (formerly shared, multi-user) / *Dedicated* (formerly single user) / serverless | all enforce UC; standard isolates users from each other; fine-grained controls (row filters/masks) on dedicated clusters are executed through serverless filtering |
# MAGIC | **Workspace–catalog binding** | a catalog can be restricted to specific workspaces (e.g. `prod` only from the prod workspace) |
# MAGIC | **Job run as** (Section 10) | a job reads/writes with the run-as identity's grants — use service principals |
# MAGIC | **Dashboards** (Section 09) | shared vs individual data permission: publisher's grants vs each viewer's grants |
# MAGIC
# MAGIC ## ✅ Key takeaways
# MAGIC 1. Principals: **users**, **service principals** (automation), **groups** (nested; `account users` = everyone). Identities
# MAGIC    are **account-level** (identity federation). Roles: account admin, metastore admin, workspace admin.
# MAGIC 2. Every object has one **owner** (prefer a group); owners, parent owners, metastore admins and `MANAGE` holders manage grants.
# MAGIC 3. Hierarchy metastore → catalog → schema → object; grants on a container are **inherited** by current **and future** children.
# MAGIC 4. Reading a table needs **USE CATALOG + USE SCHEMA + SELECT**; writing needs **MODIFY**; creating needs **CREATE TABLE**
# MAGIC    on the schema; volumes **READ/WRITE VOLUME**; functions **EXECUTE**.
# MAGIC 5. `GRANT … ON … TO`, `REVOKE … ON … FROM`, `SHOW GRANTS [principal] ON …` (shows inherited grants) — or the
# MAGIC    **Permissions** tab in Catalog Explorer.
# MAGIC 6. **DENY** = legacy hive_metastore table ACLs only. Unity Catalog is allow-only (REVOKE); ABAC DENY policies (Beta) can only
# MAGIC    deny `MANAGE ACCESS CONTROL`.
# MAGIC
# MAGIC ➡️ Next: **13-P2 · Row-Level Security, Column Masks and ABAC**
