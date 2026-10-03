# Databricks notebook source
# MAGIC %md
# MAGIC # 🎭 13-P2 · Row-Level Security, Column Masks and ABAC
# MAGIC **Section 13** · exam objectives **7.3** — *understand column-level masking and row-level security to restrict data
# MAGIC visibility based on user groups* and **7.4** — *understand Unity Catalog ABAC policies to centrally control row-level
# MAGIC filtering and column masking for sensitive data*
# MAGIC
# MAGIC | After this presentation you can… |
# MAGIC |---|
# MAGIC | Restrict **rows** and **columns** per user/group with **dynamic views** (`current_user()`, `is_account_group_member()`) |
# MAGIC | Attach **row filters** and **column masks** (SQL UDFs) directly to tables — and know their limits |
# MAGIC | Use a **mapping table** to drive row-level security from data |
# MAGIC | Explain **ABAC**: **governed tags** + **policies** that filter/mask every matching table centrally |
# MAGIC | Choose between dynamic views, row filters/column masks and ABAC for a scenario |
# MAGIC
# MAGIC > ▶️ **Run all** to render the diagrams.

# COMMAND ----------

# MAGIC %run ../../Includes/_style

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · Same table, different eyes

# COMMAND ----------

# DBTITLE 1,Slide · The requirement
show("""
<div class="kicker">Slide 1 · GRANT decides WHETHER you can query a table — fine-grained controls decide WHAT you see</div>
<table class="tbl">
<tr><th>customer_id</th><th>email</th><th>card_number</th><th>country</th><th>Regional manager (Saudi Arabia) sees…</th><th>Support agent sees…</th></tr>
<tr><td>C0001</td><td>lina.haddad1@gmail.com</td><td>4000 0012 3456 7890</td><td>Saudi Arabia</td><td>✅ row · e-mail · <code>**** 7890</code></td><td>✅ row · <code>l***@gmail.com</code> · <code>**** 7890</code></td></tr>
<tr><td>C0002</td><td>omar.chen2@outlook.com</td><td>4000 0098 7654 3210</td><td>Egypt</td><td>❌ row hidden</td><td>✅ row · <code>o***@outlook.com</code> · <code>**** 3210</code></td></tr>
</table>
<div class="grid">
 <div class="card"><h3>🧱 Row-level security</h3>hide <b>rows</b> (region, department, tenant)</div>
 <div class="card orange"><h3>🎭 Column masking</h3>replace <b>values</b> (redact, partially mask, hash, NULL) — the column still exists</div>
 <div class="card green"><h3>🧠 Decided at query time</h3>by <b>who</b> runs the query: <code>current_user()</code>, <code>is_account_group_member('grp')</code> — or a <b>mapping table</b></div>
</div>
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · Identity functions
# MAGIC
# MAGIC | Function | Returns | Note |
# MAGIC |---|---|---|
# MAGIC | `current_user()` | e-mail / name of the user (or service principal) running the query | for per-user rules and mapping tables |
# MAGIC | `is_account_group_member('grp')` | `true` if the current user is in the **account** group (directly or nested) | ✅ use this with Unity Catalog |
# MAGIC | `is_member('grp')` | `true` for a **workspace-local** group | 🕰️ legacy (hive_metastore era) |
# MAGIC | `current_catalog()`, `current_schema()` | context | rarely used for security |
# MAGIC
# MAGIC > Group checks are evaluated for the **querying** principal: with a dashboard using *shared data permission* (Section 09) or
# MAGIC > a job (Section 10) that is the **publisher** / **run-as** identity, not the person looking at the result.
# MAGIC
# MAGIC ## 3 · Option A — dynamic views
# MAGIC
# MAGIC ```sql
# MAGIC CREATE OR REPLACE VIEW sec_v_customers AS
# MAGIC SELECT customer_id, first_name, country,
# MAGIC        CASE WHEN is_account_group_member('shopwave_pii_readers') THEN email
# MAGIC             ELSE regexp_replace(email, '^(.)[^@]*', '$1***') END            AS email,      -- column masking
# MAGIC        concat('**** ', right(card_number, 4))                               AS card_last4
# MAGIC FROM sec_customers
# MAGIC WHERE is_account_group_member('shopwave_admins')                                            -- row filtering
# MAGIC    OR country IN (SELECT country FROM sec_region_access WHERE user_email = current_user());
# MAGIC
# MAGIC GRANT SELECT ON VIEW sec_v_customers TO `shopwave_analysts`;      -- analysts get the VIEW, not the base table
# MAGIC ```
# MAGIC
# MAGIC * ✅ works everywhere (it's just a view), can also **reshape** data (joins, aggregates); ideal for sharing curated data.
# MAGIC * ⚠️ protects only people who use the **view** — anyone with `SELECT` on the base table bypasses it. You maintain one
# MAGIC   view per table and per rule.
# MAGIC
# MAGIC ## 4 · Option B — row filters and column masks on the table

# COMMAND ----------

# DBTITLE 1,Slide · Row filters and masks
show("""
<div class="kicker">Slide 4 · A SQL UDF attached to the TABLE — every query, every user, every tool</div>
<div class="grid two">
 <div class="card"><h3>🧱 Row filter</h3>
 <code>CREATE FUNCTION sec_country_filter(p_country STRING)<br>RETURN is_account_group_member('shopwave_admins')<br>&nbsp;&nbsp;OR EXISTS (SELECT 1 FROM sec_region_access a<br>&nbsp;&nbsp;&nbsp;&nbsp;WHERE a.user_email = current_user() AND a.country = p_country);</code><br><br>
 <code>ALTER TABLE sec_orders SET ROW FILTER sec_country_filter ON (country);</code>
 <p class="muted">returns BOOLEAN; rows where it is FALSE (or NULL) are hidden. Remove: <code>ALTER TABLE sec_orders DROP ROW FILTER</code></p></div>
 <div class="card orange"><h3>🎭 Column mask</h3>
 <code>CREATE FUNCTION sec_mask_email(email STRING)<br>RETURN CASE WHEN is_account_group_member('shopwave_pii_readers')<br>&nbsp;&nbsp;THEN email ELSE regexp_replace(email, '^(.)[^@]*', '$1***') END;</code><br><br>
 <code>ALTER TABLE sec_customers ALTER COLUMN email SET MASK sec_mask_email;</code>
 <p class="muted">first parameter = the column value; the result must have (or be castable to) the column type. Extra columns:
 <code>SET MASK sec_mask_phone USING COLUMNS (country)</code>. Remove: <code>ALTER COLUMN email DROP MASK</code></p></div>
</div>
<table class="tbl">
<tr><th>Fact</th><th>Detail</th></tr>
<tr><td>Who sets them</td><td>the table owner (or a principal with MANAGE); also at creation: <code>CREATE TABLE … WITH ROW FILTER f ON (col)</code>, <code>col STRING MASK f</code></td></tr>
<tr><td>Who is affected</td><td><b>everyone</b>, including the owner and admins — exemptions are written <b>inside</b> the function (e.g. an admin group)</td></tr>
<tr><td>Limits</td><td>one row filter per table, one mask per column · the function can't read another filtered/masked table · <b>no time travel</b>, <b>no deep/shallow clone</b> of the table · not on views (use a dynamic view)</td></tr>
<tr><td>Compute</td><td>serverless, SQL warehouses, standard access mode; dedicated clusters use serverless filtering (DBR 15.4+)</td></tr>
<tr><td>Inspect</td><td><code>DESCRIBE TABLE EXTENDED</code>, Catalog Explorer, <code>information_schema.row_filters</code> / <code>column_masks</code></td></tr>
</table>
""" + callout("tip", "A <b>mapping table</b> (<code>user_email → country</code>, <code>group → department</code>) turns access rules into <b>data</b>: giving a manager "
              "a new region is an INSERT, not a code change. Keep the mapping table itself readable only by admins."))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 · Option C — ABAC: attribute-based access control

# COMMAND ----------

# DBTITLE 1,Slide · ABAC
show("""
<div class="kicker">Slide 5 · Tag the data once — one policy protects every matching table, today and tomorrow</div>
<div class="flow">
 <div class="step"><b>🏷️ Governed tag</b>account-level key with <b>allowed values</b><br><code>pii</code> = email · phone · card_number<br>(who may assign it is controlled)</div><div class="arrow">➜</div>
 <div class="step"><b>📌 Tag columns / tables</b><code>ALTER TABLE t ALTER COLUMN email<br>SET TAGS ('pii' = 'email')</code></div><div class="arrow">➜</div>
 <div class="step" style="border:2px solid #7048e8"><b>📜 Policy on a catalog / schema</b>“mask every column tagged <code>pii</code> for everyone except <code>pii_readers</code>”</div><div class="arrow">➜</div>
 <div class="step"><b>🔁 Applied automatically</b>to all current <b>and future</b> tables in scope with matching tags</div>
</div>
<div class="grid two">
 <div class="card orange"><h3>Column mask policy</h3>
 <code>CREATE POLICY mask_pii<br>ON SCHEMA workspace.shopwave<br>COLUMN MASK sec_redact<br>TO `account users` EXCEPT `shopwave_pii_readers`<br>FOR TABLES<br>MATCH COLUMNS has_tag('pii') AS c<br>ON COLUMN c;</code></div>
 <div class="card"><h3>Row filter policy</h3>
 <code>CREATE POLICY region_rows<br>ON SCHEMA workspace.shopwave<br>ROW FILTER sec_country_filter<br>TO `account users` EXCEPT `shopwave_admins`<br>FOR TABLES<br>MATCH COLUMNS has_tag_value('geo', 'country') AS g<br>USING COLUMNS (g);</code></div>
</div>
<table class="tbl">
<tr><th>Clause</th><th>Meaning</th></tr>
<tr><td><code>ON CATALOG | SCHEMA | TABLE</code></td><td>scope — the policy covers every table below it (metastore level: Beta)</td></tr>
<tr><td><code>TO … EXCEPT …</code></td><td>principals the policy applies to / exempted principals (exempt = see raw data)</td></tr>
<tr><td><code>WHEN has_tag('x')</code></td><td>optional table condition (table tags)</td></tr>
<tr><td><code>MATCH COLUMNS has_tag_value('k','v') AS a</code></td><td>select columns by their tags (up to 3 expressions); <code>ON COLUMN a</code> = masked column; <code>USING COLUMNS (a)</code> = UDF arguments</td></tr>
<tr><td>Manage</td><td><code>SHOW POLICIES ON SCHEMA …</code> · <code>DESCRIBE POLICY name ON SCHEMA …</code> · <code>DROP POLICY name ON SCHEMA …</code> · <code>information_schema.abac_policy_definitions</code></td></tr>
<tr><td>Requirements</td><td>serverless or DBR 16.4+ · <b>MANAGE</b> on the securable to create · the UDF in Unity Catalog · <b>governed</b> tags (not free-text tags)</td></tr>
</table>
""" + callout("exam", "ABAC row filter and column mask policies are <b>GA</b>; Databricks recommends ABAC when the same rule must apply to <b>many tables</b>. "
              "A new table with a tagged <code>email</code> column is masked the moment it is created — no ALTER TABLE needed."))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6 · Which one when?
# MAGIC
# MAGIC | Need | Best fit | Why |
# MAGIC |---|---|---|
# MAGIC | Share a curated, reshaped subset (joins, aggregates) with another team | **Dynamic view** | the view *is* the product; base table stays private |
# MAGIC | One sensitive table, rule specific to it (e.g. HR salaries) | **Row filter / column mask** | enforced on the table for every reader and tool |
# MAGIC | The same PII rule across hundreds of tables, new tables protected automatically | **ABAC policy + governed tags** | central, tag-driven, scales with the catalog |
# MAGIC | “Managers see only their region”, regions change often | **Row filter (or policy) + mapping table** | access changes = data changes |
# MAGIC | Prevent non-admins from changing grants on tagged sensitive tables | **ABAC DENY policy** (Beta, `MANAGE ACCESS CONTROL`) | central guard-rail on grant management |
# MAGIC
# MAGIC ## 7 · Discovering and classifying sensitive data
# MAGIC * **Tags** on catalogs, schemas, tables, columns, volumes: `ALTER TABLE … SET TAGS ('k' = 'v')`, `ALTER TABLE … ALTER
# MAGIC   COLUMN c SET TAGS (…)`, `UNSET TAGS`; query them in `information_schema.table_tags` / `column_tags`. Assigning a tag
# MAGIC   needs **APPLY TAG** on the object (+ ASSIGN on a governed tag).
# MAGIC * **Governed tags** (account level) restrict keys/values and who can assign them; **system tags** such as
# MAGIC   `system.certification_status` and the `class.*` tags of automatic **data classification** are predefined.
# MAGIC * Tags feed ABAC, search, discovery and cost reporting.
# MAGIC
# MAGIC ## ✅ Key takeaways
# MAGIC 1. **Dynamic views**: `CASE WHEN is_account_group_member(…)` masks columns, a `WHERE` with `current_user()` / groups /
# MAGIC    mapping tables filters rows; grant the **view**, not the base table.
# MAGIC 2. **Row filter** = boolean SQL UDF attached with `ALTER TABLE … SET ROW FILTER f ON (col)`; **column mask** = UDF attached
# MAGIC    with `ALTER COLUMN c SET MASK f [USING COLUMNS (…)]`. They apply to **everyone** (owner included) on every query.
# MAGIC 3. Limits: one filter per table / one mask per column, no time travel or clones on protected tables, filters can't read
# MAGIC    other protected tables.
# MAGIC 4. **ABAC**: governed tags + `CREATE POLICY … ROW FILTER | COLUMN MASK … TO … EXCEPT … FOR TABLES [WHEN …] MATCH COLUMNS …`
# MAGIC    on a catalog/schema → automatic protection of all current and future matching tables.
# MAGIC 5. Use `is_account_group_member()` with Unity Catalog (`is_member()` is for legacy workspace-local groups).
# MAGIC
# MAGIC ➡️ Next: **13-P3 · Managed and External Tables, Lineage and Audit**
