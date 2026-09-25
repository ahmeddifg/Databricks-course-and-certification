# Databricks notebook source
# MAGIC %md
# MAGIC # 🎨 Presentation style helpers
# MAGIC Included by presentation notebooks with `%run ../../Includes/_style`.
# MAGIC Defines `show(html)` which renders HTML diagrams/cards with the course theme via `displayHTML`.
# MAGIC No Spark job is started — it runs instantly on any compute.

# COMMAND ----------

# DBTITLE 1,Theme + show()
COURSE_CSS = """
<style>
.sw{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,Helvetica,Arial,sans-serif;
    background:#ffffff;color:#1d2433;padding:18px 20px;border-radius:14px;line-height:1.45;
    border:1px solid #e3e8ef;max-width:1100px}
.sw h1{font-size:26px;margin:0 0 6px 0;color:#0b2a4a}
.sw h2{font-size:20px;margin:0 0 10px 0;color:#0b2a4a}
.sw h3{font-size:16px;margin:0 0 6px 0;color:#0b2a4a}
.sw p{margin:4px 0 8px 0}
.sw .sub{color:#5b6576;font-size:14px}
.sw .kicker{font-size:12px;letter-spacing:.08em;text-transform:uppercase;color:#e8590c;font-weight:700}
.sw .grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(210px,1fr));gap:12px;margin:10px 0}
.sw .grid.two{grid-template-columns:repeat(auto-fit,minmax(320px,1fr))}
.sw .card{background:#f7f9fc;border:1px solid #e3e8ef;border-top:5px solid #1c7ed6;border-radius:10px;
    padding:12px 14px;font-size:14px}
.sw .card h3{font-size:15px}
.sw .card.green{border-top-color:#2f9e44}.sw .card.orange{border-top-color:#e8590c}
.sw .card.purple{border-top-color:#7048e8}.sw .card.red{border-top-color:#e03131}
.sw .card.teal{border-top-color:#0c8599}.sw .card.gray{border-top-color:#868e96}
.sw .card ul{margin:4px 0 0 18px;padding:0}.sw .card li{margin:2px 0}
.sw .pill{display:inline-block;padding:2px 9px;border-radius:999px;background:#e7f1ff;color:#1c5fb8;
    font-size:12px;font-weight:600;margin:2px 4px 2px 0}
.sw .pill.green{background:#e6f7ea;color:#237a36}.sw .pill.orange{background:#fff0e6;color:#b84a09}
.sw .pill.red{background:#ffe8e8;color:#b22020}.sw .pill.purple{background:#efeaff;color:#5433c7}
.sw .pill.gray{background:#eef0f3;color:#495057}
.sw .flow{display:flex;flex-wrap:wrap;align-items:stretch;gap:6px;margin:10px 0}
.sw .flow .step{flex:1 1 120px;background:#f7f9fc;border:1px solid #d6dde8;border-radius:10px;padding:10px;
    text-align:center;font-size:13px}
.sw .flow .step b{display:block;font-size:14px;margin-bottom:3px}
.sw .flow .arrow{align-self:center;font-size:22px;color:#8a94a6}
.sw .layer{border-radius:10px;padding:9px 12px;margin:6px 0;font-size:14px;color:#fff}
.sw .layer small{opacity:.9}
.sw .callout{border-left:5px solid #1c7ed6;background:#f1f7ff;border-radius:8px;padding:10px 12px;margin:10px 0;font-size:14px}
.sw .callout.tip{border-color:#2f9e44;background:#effaf1}
.sw .callout.trap{border-color:#e03131;background:#fff3f3}
.sw .callout.legacy{border-color:#868e96;background:#f4f5f7}
.sw .callout.exam{border-color:#e8590c;background:#fff6ee}
.sw table.tbl{border-collapse:collapse;width:100%;font-size:13.5px;margin:8px 0}
.sw table.tbl th{background:#0b2a4a;color:#fff;text-align:left;padding:7px 9px}
.sw table.tbl td{border-bottom:1px solid #e3e8ef;padding:6px 9px;vertical-align:top}
.sw table.tbl tr:nth-child(even) td{background:#f9fbfd}
.sw code{background:#eef2f7;border-radius:4px;padding:1px 5px;font-size:12.5px;color:#a61e4d}
.sw .muted{color:#6b7385;font-size:12.5px}
.sw .bar{height:14px;border-radius:7px;background:#1c7ed6;display:inline-block;vertical-align:middle}
</style>
"""


def show(html: str):
    """Render an HTML fragment with the course theme."""
    displayHTML(COURSE_CSS + f'<div class="sw">{html}</div>')


def callout(kind: str, text: str) -> str:
    """kind: tip | trap | legacy | exam | info  -> returns HTML string."""
    icons = {"tip": "✅ Tip", "trap": "⚠️ Exam trap", "legacy": "🕰️ Legacy", "exam": "🎯 Exam focus", "info": "ℹ️ Note"}
    return f'<div class="callout {kind}"><b>{icons.get(kind, "")}</b> — {text}</div>'


print("🎨 Presentation theme loaded - use show(html)")
