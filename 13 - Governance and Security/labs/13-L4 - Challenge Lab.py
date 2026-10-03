# Databricks notebook source
# MAGIC %md
# MAGIC # 🏆 Lab 13-L4 · Challenge Lab — Securing the HR Directory
# MAGIC **Time:** ~45 min · **Compute:** Serverless notebook · **Story:** HR publishes `ch13_employees` (40 employees: name,
# MAGIC e-mail, department, country, **salary**). Department heads may only see **their department**; only the HR admins may see
# MAGIC salaries and e-mail addresses; the rest of the company gets a **directory view** — and nothing else.
# MAGIC
# MAGIC Replace every `# TODO` / `None`, then run each **✅ Check**. The checks change the mapping table themselves to test your
# MAGIC rules — no hard-coding.
# MAGIC
# MAGIC | Task | Skill |
# MAGIC |---|---|
# MAGIC | 1 | **Row filter** driven by a mapping table + an admin group |
# MAGIC | 2 | **Column mask** that returns `NULL` |
# MAGIC | 3 | **Dynamic view** for the company directory |
# MAGIC | 4 | **Least-privilege grants** for the group |
# MAGIC | 5–8 | 🧠 Governance concepts |

# COMMAND ----------

# MAGIC %run ../../Includes/_setup

# COMMAND ----------

# MAGIC %run ./_13_prepare

# COMMAND ----------

# DBTITLE 1,Setup for the challenge (run me - don't edit)
for _stmt in ("ALTER TABLE ch13_employees DROP ROW FILTER",):
    try:
        spark.sql(_stmt)
    except Exception:
        pass
for _c in ("salary", "email"):
    try:
        spark.sql(f"ALTER TABLE ch13_employees ALTER COLUMN {_c} DROP MASK")
    except Exception:
        pass
for _stmt in ("DROP VIEW IF EXISTS ch13_v_directory", "DROP FUNCTION IF EXISTS ch13_dept_filter",
              "DROP FUNCTION IF EXISTS ch13_mask_salary"):
    spark.sql(_stmt)
for _priv, _obj in (("SELECT", "TABLE ch13_employees"), ("ALL PRIVILEGES", "TABLE ch13_employees")):
    try:
        spark.sql(f"REVOKE {_priv} ON {_obj} FROM `{G13}`")
    except Exception:
        pass
answer_task5 = answer_task6 = answer_task7 = answer_task8 = None
ADMINS = "ch13_hr_admins"          # the HR admin group (you are NOT a member)


def check(label, condition):
    print(("✅ " if condition else "❌ ") + label)
    return bool(condition)


def _set_my_departments(depts):
    """Used by the checks: replace YOUR rows of ch13_dept_access."""
    keep = [(r["user_email"], r["department"]) for r in spark.table("ch13_dept_access").collect() if r["user_email"] != me()]
    spark.createDataFrame(keep + [(me(), d) for d in depts], "user_email STRING, department STRING") \
         .write.mode("overwrite").saveAsTable("ch13_dept_access")


def _grants(kind, name):
    rows = [{k.lower(): v for k, v in r.asDict().items()} for r in show_grants(kind, name).collect()]
    return {r["actiontype"].replace("_", " ").upper() for r in rows if r["principal"] == G13}


_set_my_departments([])
display(spark.table("ch13_dept_access"))
print("group for Task 4: G13 =", G13)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 1 · Department heads see only their department
# MAGIC * Create the function **`ch13_dept_filter(p_department STRING)`** returning `TRUE` when the current user is a member of the
# MAGIC   account group **`ch13_hr_admins`**, **or** when `ch13_dept_access` has a row for the current user and that department.
# MAGIC * Attach it as the **row filter** of `ch13_employees` on the column `department`.

# COMMAND ----------

# DBTITLE 1,Task 1
# MAGIC %sql
# MAGIC -- TODO: CREATE OR REPLACE FUNCTION ch13_dept_filter(...) ...
# MAGIC -- TODO: ALTER TABLE ch13_employees SET ROW FILTER ...
# MAGIC SELECT 'replace me' AS todo

# COMMAND ----------

# DBTITLE 1,✅ Check Task 1
_counts = []
for _depts in ([], ["Engineering"], ["Engineering", "Sales"]):
    _set_my_departments(_depts)
    _counts.append(spark.table("ch13_employees").count())
_set_my_departments([])
check("no department → 0 rows", _counts[0] == 0)
check("Engineering → 10 rows", _counts[1] == 10)
check("Engineering + Sales → 20 rows", _counts[2] == 20)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 2 · Salaries for HR admins only
# MAGIC Create **`ch13_mask_salary(salary DOUBLE)`** returning the salary for members of **`ch13_hr_admins`** and **`NULL`** for
# MAGIC everybody else, and attach it as the **mask** of `ch13_employees.salary`.

# COMMAND ----------

# DBTITLE 1,Task 2
# MAGIC %sql
# MAGIC -- TODO: CREATE OR REPLACE FUNCTION ch13_mask_salary(...) ...
# MAGIC -- TODO: ALTER TABLE ch13_employees ALTER COLUMN salary SET MASK ...
# MAGIC SELECT 'replace me' AS todo

# COMMAND ----------

# DBTITLE 1,✅ Check Task 2
_set_my_departments(["Engineering", "Finance", "HR", "Sales"])
_e = spark.table("ch13_employees")
check("all 40 rows visible with every department mapped", _e.count() == 40)
check("salary is NULL for you (not an HR admin)", _e.where("salary IS NOT NULL").count() == 0)
check("the salary column type is unchanged (DOUBLE)", dict(_e.dtypes).get("salary") == "double")
_set_my_departments([])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 3 · The company directory
# MAGIC Create the view **`ch13_v_directory`** on `ch13_employees` with exactly these columns:
# MAGIC
# MAGIC | Column | Value |
# MAGIC |---|---|
# MAGIC | `employee_id`, `full_name`, `department`, `country` | as in the table |
# MAGIC | `email` | the real e-mail for members of `ch13_hr_admins`, otherwise the text **`hidden`** |
# MAGIC
# MAGIC (Rows: whatever the table's row filter lets the reader see — the view doesn't need its own `WHERE`.)

# COMMAND ----------

# DBTITLE 1,Task 3
# MAGIC %sql
# MAGIC -- TODO: CREATE OR REPLACE VIEW ch13_v_directory AS ...
# MAGIC SELECT 'replace me' AS todo

# COMMAND ----------

# DBTITLE 1,✅ Check Task 3
_set_my_departments(["HR"])
if spark.catalog.tableExists("ch13_v_directory"):
    _v = spark.table("ch13_v_directory")
    check("columns employee_id, full_name, department, country, email",
          _v.columns == ["employee_id", "full_name", "department", "country", "email"])
    check("the view respects the row filter (HR only → 10 rows)", _v.count() == 10
          and [r[0] for r in _v.select("department").distinct().collect()] == ["HR"])
    check("every e-mail shows 'hidden'", _v.where("email <> 'hidden' OR email IS NULL").count() == 0)
else:
    check("view ch13_v_directory exists", False)
_set_my_departments([])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 4 · Least privilege for the group
# MAGIC The group **`G13`** (printed in the setup) must be able to query the **view** `ch13_v_directory` — and must **not** be able
# MAGIC to read the table `ch13_employees` directly. Grant exactly what is needed (check the schema-level privilege too).

# COMMAND ----------

# DBTITLE 1,Task 4
# TODO: try_sql(f"GRANT ... TO `{G13}`")   (one or more statements)
pass

# COMMAND ----------

# DBTITLE 1,✅ Check Task 4
_sch = _grants("SCHEMA", f"{catalog_name}.{schema_name}")
check("USE SCHEMA on the schema", "USE SCHEMA" in _sch)
check("SELECT on the view ch13_v_directory", spark.catalog.tableExists("ch13_v_directory")
      and "SELECT" in _grants("VIEW", "ch13_v_directory"))
check("NO SELECT on the table ch13_employees (neither direct nor via the schema)",
      not ({"SELECT", "ALL PRIVILEGES"} & _grants("TABLE", "ch13_employees")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tasks 5–8 · 🧠 Concepts
# MAGIC Answer with `"A"`, `"B"`, `"C"` or `"D"`.
# MAGIC
# MAGIC **Task 5.** What is the minimum an analyst needs to run `SELECT * FROM prod.hr.employees` in Unity Catalog?
# MAGIC * A. `SELECT` on the table
# MAGIC * B. `USE CATALOG` on `prod`, `USE SCHEMA` on `prod.hr`, `SELECT` on the table
# MAGIC * C. `ALL PRIVILEGES` on the catalog `prod`
# MAGIC * D. `READ FILES` on the external location of the table
# MAGIC
# MAGIC **Task 6.** The group `staff` has `SELECT` on the schema `prod.hr`; interns are members of `staff`. Interns must no longer
# MAGIC read `prod.hr.salaries`. In Unity Catalog you…
# MAGIC * A. run `DENY SELECT ON TABLE prod.hr.salaries TO interns`
# MAGIC * B. run `REVOKE SELECT ON TABLE prod.hr.salaries FROM interns`
# MAGIC * C. stop granting `SELECT` on the whole schema to `staff`: grant the needed tables (or a schema without salaries) to a
# MAGIC   group without interns — or protect the rows/columns with a filter, mask or policy that excludes them
# MAGIC * D. make the interns the owners of the table
# MAGIC
# MAGIC **Task 7.** `DROP TABLE sales_ext` on an **external** table…
# MAGIC * A. removes the table from Unity Catalog; the data files stay in the external location
# MAGIC * B. deletes the data files immediately
# MAGIC * C. fails — external tables must be converted with `SET MANAGED` first
# MAGIC * D. converts the table to a managed table
# MAGIC
# MAGIC **Task 8.** E-mail and phone columns appear in 300 tables, and new tables are created every week. Every such column must
# MAGIC be masked for everyone except the PII team. The most maintainable solution is…
# MAGIC * A. a dynamic view per table
# MAGIC * B. `ALTER TABLE … ALTER COLUMN … SET MASK` on every table, in a weekly job
# MAGIC * C. revoke `SELECT` on all 300 tables
# MAGIC * D. tag the columns with a governed tag and create one ABAC column-mask policy on the catalog, `EXCEPT` the PII team

# COMMAND ----------

# DBTITLE 1,Tasks 5-8
answer_task5 = None   # "A", "B", "C" or "D"
answer_task6 = None
answer_task7 = None
answer_task8 = None

# COMMAND ----------

# DBTITLE 1,✅ Check Tasks 5-8
import hashlib
_h = lambda n, v: hashlib.sha256(f"{n}:{str(v).strip().upper()}".encode()).hexdigest()[:10]
_key = {5: "13decc6d1a", 6: "42591f949b", 7: "0d3757a0a5", 8: "305d7ba11c"}
for _n, _a in ((5, answer_task5), (6, answer_task6), (7, answer_task7), (8, answer_task8)):
    check(f"Task {_n}", _a is not None and _h(_n, _a) == _key[_n])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🏁 Final score

# COMMAND ----------

# DBTITLE 1,Final score - re-runs every check quietly
def _quiet_score():
    res = {}
    try:
        cnt = []
        for depts in ([], ["Engineering"]):
            _set_my_departments(depts)
            cnt.append(spark.table("ch13_employees").count())
        res["Task 1 row filter"] = cnt == [0, 10]
        _set_my_departments(["Finance"])
        res["Task 2 salary mask"] = spark.table("ch13_employees").where("salary IS NOT NULL").count() == 0 \
            and spark.table("ch13_employees").count() == 10
        res["Task 3 directory view"] = spark.catalog.tableExists("ch13_v_directory") \
            and spark.table("ch13_v_directory").where("email <> 'hidden'").count() == 0 \
            and spark.table("ch13_v_directory").count() == 10
    finally:
        _set_my_departments([])
    res["Task 4 grants"] = "SELECT" in (_grants("VIEW", "ch13_v_directory") if spark.catalog.tableExists("ch13_v_directory") else set()) \
        and not ({"SELECT", "ALL PRIVILEGES"} & _grants("TABLE", "ch13_employees"))
    res["Tasks 5-8"] = all(a is not None and _h(n, a) == _key[n]
                           for n, a in ((5, answer_task5), (6, answer_task6), (7, answer_task7), (8, answer_task8)))
    return res


_score = _quiet_score()
for k, v in _score.items():
    print(("✅ " if v else "❌ ") + k)
print(f"\nScore: {sum(_score.values())}/{len(_score)}" + ("  🏆 Section 13 complete!" if all(_score.values()) else ""))
