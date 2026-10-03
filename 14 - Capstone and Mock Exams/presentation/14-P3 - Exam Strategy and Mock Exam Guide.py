# Databricks notebook source
# MAGIC %md
# MAGIC # 🎯 14-P3 · Exam Strategy and Mock Exam Guide
# MAGIC **Section 14** · how to turn what you know into points on exam day
# MAGIC
# MAGIC | After this presentation you can… |
# MAGIC |---|
# MAGIC | Describe the exam format and plan your **90 minutes** |
# MAGIC | Decode question wording (*"most cost-effective"*, *"least operational overhead"*, *"incrementally"*…) |
# MAGIC | Eliminate wrong options systematically |
# MAGIC | Use the **mock exam** (14-Q) and the weak-area map in your last week |
# MAGIC
# MAGIC > ▶️ **Run all** to render the slides.

# COMMAND ----------

# MAGIC %run ../../Includes/_style

# COMMAND ----------

# DBTITLE 1,Slide 1 · The exam at a glance
show("""
<div class="kicker">Slide 1 · Databricks Certified Data Engineer Associate (exam guide May 2026)</div>
<div class="grid">
 <div class="card"><h3>📝 Format</h3><b>45 scored</b> multiple-choice questions (single answer or “choose N”); the exam may also contain a few unscored items</div>
 <div class="card green"><h3>⏱️ Time</h3><b>90 minutes</b> → 2 minutes per question</div>
 <div class="card orange"><h3>🚫 Aids</h3>none — no docs, no notebooks, no notes</div>
 <div class="card purple"><h3>🖥️ Delivery</h3>online proctored or test centre · registration fee USD 200 · valid 2 years</div>
</div>
<table class="tbl">
<tr><th>Domain</th><th>~ weight</th><th>≈ questions</th></tr>
<tr><td>D1 Databricks Intelligence Platform</td><td>6 %</td><td>3</td></tr>
<tr><td>D2 Data Ingestion and Loading</td><td>21 %</td><td>9</td></tr>
<tr><td>D3 Data Transformation and Modeling</td><td>22 %</td><td>10</td></tr>
<tr><td>D4 Working with Lakeflow Jobs</td><td>16 %</td><td>7</td></tr>
<tr><td>D5 Implementing CI/CD</td><td>10 %</td><td>5</td></tr>
<tr><td>D6 Troubleshooting, Monitoring and Optimization</td><td>10 %</td><td>4–5</td></tr>
<tr><td>D7 Governance and Security</td><td>15 %</td><td>7</td></tr>
</table>
""" + callout("info", "Weights are the course's estimate from the official outline — always check the current exam guide on the Databricks certification page before you book."))

# COMMAND ----------

# DBTITLE 1,Slide 2 · Three passes in 90 minutes
show("""
<div class="kicker">Slide 2 · Time plan — three passes</div>
<div class="flow">
 <div class="step"><b>Pass 1 · 0–50 min</b>answer everything you know in &lt; 1 min; <strong>flag</strong> the rest, pick a provisional answer anyway</div><div class="arrow">→</div>
 <div class="step"><b>Pass 2 · 50–80 min</b>flagged questions: eliminate, re-read the scenario, decide</div><div class="arrow">→</div>
 <div class="step"><b>Pass 3 · 80–90 min</b>check “choose TWO” counts, no blanks, don't change answers without a reason</div>
</div>
""" + callout("tip", "There is no penalty for a wrong answer — <b>never leave a question blank</b>.") + callout("trap", "Changing a first answer is right only when you found a concrete reason (a keyword you missed). Gut-feeling switches lose points on average."))

# COMMAND ----------

# DBTITLE 1,Slide 3 · Decode the question
show("""
<div class="kicker">Slide 3 · Keywords that point to the answer</div>
<table class="tbl">
<tr><th>The question says…</th><th>It's usually pointing to…</th></tr>
<tr><td>“incrementally”, “only new files”, “without reprocessing”</td><td>Auto Loader / COPY INTO / streaming tables / insert-only MERGE</td></tr>
<tr><td>“minimal code”, “managed”, “SaaS source”, “CDC from a database”</td><td>Lakeflow Connect managed connector</td></tr>
<tr><td>“least operational overhead”, “no infrastructure to manage”, “start quickly”</td><td>serverless (compute, SQL warehouse, pipelines)</td></tr>
<tr><td>“most cost-effective for a scheduled production job”</td><td>jobs compute or serverless jobs — not all-purpose</td></tr>
<tr><td>“declaratively”, “data-quality expectations”, “automatic dependency management”</td><td>Lakeflow Spark Declarative Pipelines</td></tr>
<tr><td>“run only when new data arrives”</td><td>file arrival / table update trigger (data-driven)</td></tr>
<tr><td>“different settings per environment”, “promote dev → prod”</td><td>bundle targets + variables, CLI deploy -t</td></tr>
<tr><td>“all current and future tables”, “centrally”, “by tag”</td><td>schema/catalog-level grants · ABAC policies</td></tr>
<tr><td>“one task takes much longer than others”</td><td>data skew</td></tr>
<tr><td>“data written to disk during the shuffle”</td><td>spill (memory)</td></tr>
<tr><td>“keys change over time”, “high cardinality”, “avoid small files”</td><td>liquid clustering</td></tr>
<tr><td>“undo”, “accidentally deleted”</td><td>time travel + RESTORE (or UNDROP for a dropped managed table)</td></tr>
</table>
""")

# COMMAND ----------

# DBTITLE 1,Slide 4 · Eliminate
show("""
<div class="kicker">Slide 4 · Elimination checklist</div>
<div class="grid">
 <div class="card red"><h3>1 · Wrong product era</h3>legacy names in a modern scenario (<code>dbfs:/mnt</code>, <code>LIVE.</code>, <code>input_file_name</code>, DENY in UC) — unless the question is about legacy</div>
 <div class="card orange"><h3>2 · Does it even run?</h3>syntax that doesn't exist (<code>COPY INTO … MERGE</code>, <code>VACUUM … RESTORE</code>), wrong clause order, Python <code>and</code> instead of <code>&amp;</code></div>
 <div class="card purple"><h3>3 · Does it meet ALL requirements?</h3>“incremental <i>and</i> schema evolution”, “SQL <i>and</i> re-runnable” — options that satisfy only one half go</div>
 <div class="card teal"><h3>4 · Over-engineering</h3>the associate exam prefers the <b>simplest built-in</b> feature over custom code (a Python UDF vs a built-in function, polling vs a trigger)</div>
 <div class="card green"><h3>5 · Least privilege</h3>for access questions the minimal set that works beats <code>ALL PRIVILEGES</code> or ownership</div>
 <div class="card gray"><h3>6 · Absolute words</h3>“always”, “never”, “only” are often wrong — check the exception you know</div>
</div>
""")

# COMMAND ----------

# DBTITLE 1,Slide 5 · Your last 7 days
show("""
<div class="kicker">Slide 5 · Final week plan with this course</div>
<table class="tbl">
<tr><th>Day</th><th>Do</th></tr>
<tr><td>1</td><td>Mock exam <b>simulation #1</b> (14-Q, exam mode, 45 Q, 90 min) — no notes. Write down the weak domains.</td></tr>
<tr><td>2</td><td>Weak domain #1: its section's presentation + quiz; re-do the related capstone part</td></tr>
<tr><td>3</td><td>Weak domain #2 the same way · read <b>14-P2</b> traps</td></tr>
<tr><td>4</td><td>Mock exam <b>simulation #2</b> (different seed) · full 120-question practice on the weakest topics</td></tr>
<tr><td>5</td><td>Cheat sheets <b>14-CS1</b> (SQL) and <b>14-CS2</b> (Python): run, read every WHAT/EXAM line</td></tr>
<tr><td>6</td><td>Simulation #3 · target ≥ 85 % · review only wrong answers' explanations</td></tr>
<tr><td>7</td><td>Light: 14-P2 trap list, exam-day checklist, sleep 😴</td></tr>
</table>
""" + callout("tip", "Ready signal: <b>≥ 80 % in three different simulations</b> and no domain below 70 %."))

# COMMAND ----------

# DBTITLE 1,Slide 6 · How to use 14-Q
show("""
<div class="kicker">Slide 6 · The mock exam notebook (questions/14-Q)</div>
<div class="grid two">
 <div class="card"><h3>⏱️ Exam simulation</h3>45 questions drawn from the 120 with the official domain mix, <b>timed 90 min</b>, answers hidden until you click <b>Submit exam</b>. Change the widget <code>exam_seed</code> for a new exam.</div>
 <div class="card green"><h3>📚 Practice mode</h3>all 120 questions with instant feedback and explanations, grouped by domain — for learning, not for measuring.</div>
</div>
<p>After submitting you get a score per domain → the <b>weak-area map</b> at the end of the notebook tells you which presentation, lab and quiz to revisit.</p>
""")

# COMMAND ----------

# DBTITLE 1,Slide 7 · Exam-day checklist
show("""
<div class="kicker">Slide 7 · Exam day</div>
<div class="grid">
 <div class="card"><h3>🪪 Before</h3>valid photo ID · test the proctoring software/webcam the day before · quiet room, clear desk, no second screen</div>
 <div class="card green"><h3>🧠 During</h3>read the <b>last sentence</b> first (what's asked) · underline requirement words · flag &amp; move on · watch “choose TWO”</div>
 <div class="card orange"><h3>✅ After</h3>result is shown at the end; the badge/certificate arrives by e-mail — add it to LinkedIn 🎉</div>
</div>
""")
