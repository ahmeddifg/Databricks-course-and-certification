# Databricks notebook source
# MAGIC %md
# MAGIC # 🚀 Databricks Data Engineer Associate — Course Home
# MAGIC
# MAGIC Welcome! This course prepares you for the **Databricks Certified Data Engineer Associate** exam
# MAGIC (**May 2026 exam guide**) with slide-style notebooks, hands-on labs built on one realistic dataset, and exam-style questions.
# MAGIC
# MAGIC ### ✅ Do this first (5 minutes)
# MAGIC 1. Open **`00 - Course Guide and Setup` → `labs` → `00-L1 - Environment Setup`** and follow it.
# MAGIC 2. Attach **Serverless** compute (top-right compute selector) → **Run all**.
# MAGIC 3. Then work through the sections **in order**.
# MAGIC
# MAGIC ### 📁 How every section is organised
# MAGIC | Folder | What's inside | How to use it |
# MAGIC |---|---|---|
# MAGIC | `presentation` | Slide-style notebooks — concepts, diagrams, exam traps | Read top-to-bottom. **Run all** once to render the diagrams |
# MAGIC | `labs` | Guided labs, a challenge lab, and its solution | Type/run every cell. Try the challenge before opening the solution |
# MAGIC | `data` | Explains (and if needed creates) the data used by the section | Run it if a lab says data is missing |
# MAGIC | `questions` | Interactive exam-style quiz with explanations | Aim for **≥ 80 %** before moving on |
# MAGIC
# MAGIC ### 🧭 Icons used everywhere
# MAGIC | Icon | Meaning |
# MAGIC |---|---|
# MAGIC | 🎯 | **Exam focus** — this is tested |
# MAGIC | ⚠️ | **Exam trap** — a common wrong answer |
# MAGIC | 🕰️ | **Legacy** — old name/feature you must still *recognise* |
# MAGIC | ✅ | Best practice / tip |
# MAGIC | 🧪 | Hands-on step |
# MAGIC | 🖱️ | Do this in the UI (not code) |
# MAGIC
# MAGIC ### 🗺️ Sections
# MAGIC | # | Section | Exam domain (weight) |
# MAGIC |---|---|---|
# MAGIC | 00 | Course Guide & Setup | — |
# MAGIC | 01 | Databricks Introduction & Platform | D1 Databricks Intelligence Platform (6 %) |
# MAGIC | 02 | Spark Foundations | supports D3 & D6 |
# MAGIC | 03 | Delta Lake | D1 + used everywhere |
# MAGIC | 04 | Unity Catalog & Data Objects | D1 / D7 |
# MAGIC | 05 | Data Ingestion & Loading | D2 (21 %) |
# MAGIC | 06 | Structured Streaming & Auto Loader | D2 |
# MAGIC | 07 | Data Transformation & Modeling | D3 (22 %) |
# MAGIC | 08 | Medallion: hand-coded → Declarative Pipelines | D2 / D3 |
# MAGIC | 09 | Databricks SQL & BI Serving | D3 (gold layer) |
# MAGIC | 10 | Lakeflow Jobs | D4 (16 %) |
# MAGIC | 11 | CI/CD: Git folders & Declarative Automation Bundles | D5 (10 %) |
# MAGIC | 12 | Troubleshooting, Monitoring & Optimization | D6 (10 %) |
# MAGIC | 13 | Governance & Security | D7 (15 %) |
# MAGIC | 14 | Capstone & Mock Exams | all |
# MAGIC
# MAGIC ### ⚙️ Technical notes
# MAGIC * Built for **Unity Catalog + serverless compute** → runs on **Databricks Free Edition** and on paid workspaces.
# MAGIC * Every lab starts with `%run ../../Includes/_setup`, which creates schema **`shopwave`** in your default catalog and
# MAGIC   generates the data. **Keep the folder structure exactly as imported** — the `%run` paths depend on it.
# MAGIC * Nothing in this course needs internet access from your notebooks or any cloud credentials.
