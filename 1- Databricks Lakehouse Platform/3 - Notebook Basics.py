# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Databricks Notebooks – Interview Notes
# MAGIC
# MAGIC ### 1. What is a Databricks Notebook?
# MAGIC - An interactive, web‑based document where you can write and run code.
# MAGIC - Supports multiple languages: **Python, SQL, Scala, R**.
# MAGIC - Code is executed **cell‑by‑cell**.
# MAGIC
# MAGIC ### 2. Creating & Setting Up a Notebook
# MAGIC - **Create**: Workspace → Create → Notebook → give it a name (e.g., “Notebook Basics”).
# MAGIC - **Default language**: Python (can be changed anytime).
# MAGIC - **Attach to a cluster**: You must connect the notebook to a running cluster (green circle = ready).  
# MAGIC   *No cluster = no execution.*
# MAGIC
# MAGIC ### 3. Executing Cells
# MAGIC - **Run a single cell**: Click the play button or press `Shift + Enter`.
# MAGIC - **Run all above/below**: Use the drop‑down next to the play button.
# MAGIC - **New cell**: Hover below a cell and click the `+` icon.
# MAGIC
# MAGIC ### 4. Language Magic Commands
# MAGIC A magic command starts with `%` and lets you change the language **for that cell only**.
# MAGIC - `%python` – Python code  
# MAGIC - `%sql` – SQL queries  
# MAGIC - `%scala` – Scala code  
# MAGIC - `%r` – R code  
# MAGIC - `%md` – Markdown (formatted text)
# MAGIC
# MAGIC **Example**: In a Python notebook, write `%sql` at the top of a cell to run a SQL `SELECT` statement.
# MAGIC
# MAGIC ### 5. Markdown Cells (`%md`)
# MAGIC - Add titles, bold/italic text, lists, tables, links, images, even HTML.
# MAGIC - **Titles** automatically create a **Table of Contents** (left panel) for easy navigation.

# COMMAND ----------

print("Hello World!")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT "Hello world from SQL!"

# COMMAND ----------

# MAGIC %md
# MAGIC # Title 1
# MAGIC ## Title 2
# MAGIC ### Title 3
# MAGIC
# MAGIC text with a **bold** and *italicized* in it.
# MAGIC
# MAGIC Ordered list
# MAGIC 1. first
# MAGIC 1. second
# MAGIC 1. third
# MAGIC
# MAGIC Unordered list
# MAGIC * coffee
# MAGIC * tea
# MAGIC * milk
# MAGIC
# MAGIC
# MAGIC Images:
# MAGIC ![Associate-badge](https://www.databricks.com/wp-content/uploads/2022/04/associate-badge-eng.svg)
# MAGIC
# MAGIC And of course, tables:
# MAGIC
# MAGIC | user_id | user_name |
# MAGIC |---------|-----------|
# MAGIC |    1    |    Adam   |
# MAGIC |    2    |    Sarah  |
# MAGIC |    3    |    John   |
# MAGIC
# MAGIC Links (or Embedded HTML): <a href="https://docs.databricks.com/notebooks/notebooks-manage.html" target="_blank"> Managing Notebooks documentation</a>

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 6. Run Magic Command (`%run`)
# MAGIC - Execute another notebook **as if its code were written here**.
# MAGIC - Variables, functions, and imports from the called notebook become available.
# MAGIC - **Syntax**: `%run ./Includes/Setup`  
# MAGIC   (relative path to the notebook)
# MAGIC - *Benefit*: Reuse code, build modular notebooks.

# COMMAND ----------

# MAGIC %run ../Includes/Setup

# COMMAND ----------

print(full_name)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 7. Working with Files – Two Ways
# MAGIC
# MAGIC | Method | Usage | Best for |
# MAGIC |--------|-------|----------|
# MAGIC | **`%fs` magic** | `%fs ls /databricks-datasets` | Quick one‑off file listing |
# MAGIC | **`dbutils.fs`** | `dbutils.fs.ls("…")` | **Python code** – you can capture the result in a variable and process it |
# MAGIC
# MAGIC - `dbutils` (Databricks Utilities) offers many helpers: `dbutils.fs`, `dbutils.secrets`, `dbutils.widgets`.  
# MAGIC - Use `dbutils.fs.help()` to see available commands (cp, mv, rm, ls, etc.).

# COMMAND ----------

# MAGIC %fs ls '/databricks-datasets'

# COMMAND ----------

dbutils.help()

# COMMAND ----------

dbutils.fs.help()

# COMMAND ----------

files = dbutils.fs.ls('/databricks-datasets')
print(files)

# COMMAND ----------

display(files)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ### 📌 Interview Quick Recap
# MAGIC - Notebooks = interactive coding documents; attach a cluster to run.
# MAGIC - `%sql`, `%python`, `%md`, etc. are **per‑cell** language overrides.
# MAGIC - `%run` enables code reuse across notebooks.
# MAGIC - `dbutils.fs` is more powerful than `%fs` because it can be used programmatically (capture in variables).
# MAGIC - `display()` gives nice tables and charts.
# MAGIC - Export as `.ipynb` or `.dbc` to share/move between workspaces.
# MAGIC - Revision history lets you roll back to any auto‑saved version.