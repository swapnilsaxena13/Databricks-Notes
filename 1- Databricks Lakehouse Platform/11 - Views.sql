-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Preparing Sample Data

-- COMMAND ----------

--USE CATALOG hive_metastore;

CREATE TABLE IF NOT EXISTS smartphones
(id INT, name STRING, brand STRING, year INT);

INSERT INTO smartphones
VALUES (1, 'iPhone 14', 'Apple', 2022),
      (2, 'iPhone 13', 'Apple', 2021),
      (3, 'iPhone 6', 'Apple', 2014),
      (4, 'iPad Air', 'Apple', 2013),
      (5, 'Galaxy S22', 'Samsung', 2022),
      (6, 'Galaxy Z Fold', 'Samsung', 2022),
      (7, 'Galaxy S9', 'Samsung', 2016),
      (8, '12 Pro', 'Xiaomi', 2022),
      (9, 'Redmi 11T Pro', 'Xiaomi', 2022),
      (10, 'Redmi Note 11', 'Xiaomi', 2021)

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC # Views in Databricks – Interview Notes
-- MAGIC
-- MAGIC ### 1. What is a View?
-- MAGIC - A **view** is a **virtual table** – it does **not store any physical data**.
-- MAGIC - It is just a **saved SQL query** that runs every time you use the view.
-- MAGIC - You can query a view exactly like a regular table (`SELECT * FROM view_name`).
-- MAGIC
-- MAGIC ### 2. Types of Views in Databricks
-- MAGIC There are three types:
-- MAGIC
-- MAGIC | View Type | Lifetime | Scope | Created with |
-- MAGIC |-----------|----------|-------|--------------|
-- MAGIC | **Stored View** (Classical) | Permanent (until dropped) | Across any session (persisted in database) | `CREATE VIEW` |
-- MAGIC | **Temporary View** | Until the current Spark session ends | Only within the session that created it | `CREATE TEMP VIEW` (or `CREATE TEMPORARY VIEW`) |
-- MAGIC | **Global Temporary View** | Until the cluster is restarted | Any notebook on the **same cluster** | `CREATE GLOBAL TEMP VIEW` |
-- MAGIC
-- MAGIC ---
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Creating Stored  Views
-- MAGIC
-- MAGIC ### 3. Stored Views (Classic Views)
-- MAGIC - Persisted in a database (like a table, but only the query definition is saved).
-- MAGIC - **Creation**:
-- MAGIC   ```sql
-- MAGIC   CREATE VIEW my_view AS
-- MAGIC   SELECT col1, col2 FROM some_table WHERE condition;
-- MAGIC   ```
-- MAGIC - **Query**: `SELECT * FROM my_view;`
-- MAGIC - **Drop**: `DROP VIEW my_view;` (must be done manually).
-- MAGIC
-- MAGIC 💡 *Remember*: Data is **not** stored. Each time you query the view, the underlying SQL runs again.
-- MAGIC

-- COMMAND ----------

CREATE VIEW view_apple_phones
AS  SELECT * 
    FROM smartphones 
    WHERE brand = 'Apple';

-- COMMAND ----------

SELECT * FROM view_apple_phones;

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Creating Temporary Views
-- MAGIC
-- MAGIC ### 4. Temporary Views (`TEMP` / `TEMPORARY`)
-- MAGIC - **Lifetime** = the current **Spark session**.  
-- MAGIC - Once the session ends, the view is automatically dropped.
-- MAGIC - **Spark sessions are created when**:
-- MAGIC   - You open a new notebook.
-- MAGIC   - You detach and reattach a notebook to a cluster.
-- MAGIC   - The Python interpreter restarts (e.g., after installing a package).
-- MAGIC   - The cluster is restarted.
-- MAGIC - **Command**: `CREATE TEMP VIEW temp_view AS SELECT ...;`  
-- MAGIC   (or `CREATE TEMPORARY VIEW`)
-- MAGIC - Only the notebook (session) that created it can see it.
-- MAGIC

-- COMMAND ----------

CREATE TEMP VIEW temp_view_phones_brands
AS  SELECT DISTINCT brand
    FROM smartphones;

SELECT * FROM temp_view_phones_brands;

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Creating Global Temporary Views
-- MAGIC ---
-- MAGIC ### 5. Global Temporary Views (`GLOBAL TEMP`)
-- MAGIC - **Lifetime** = tied to the **cluster**, not the session.  
-- MAGIC - Stays available as long as the cluster is running, even if notebooks are detached/reattached.
-- MAGIC - Stored in a special database called **`global_temp`**.
-- MAGIC - **To query**: you must use the `global_temp` qualifier:
-- MAGIC   ```sql
-- MAGIC   SELECT * FROM global_temp.my_global_view;
-- MAGIC   ```
-- MAGIC - **Creation**: `CREATE GLOBAL TEMP VIEW my_global_view AS SELECT ...;`
-- MAGIC - Dropped automatically when the cluster is restarted.
-- MAGIC

-- COMMAND ----------

-- Note: GLOBAL TEMPORARY VIEW is not supported on serverless compute.

--CREATE GLOBAL TEMP VIEW global_temp_view_latest_phones
--AS SELECT * FROM smartphones
--    WHERE year > 2020
--    ORDER BY year DESC;

-- COMMAND ----------

--SELECT * FROM global_temp.global_temp_view_latest_phones;

-- COMMAND ----------

--SHOW TABLES;

-- COMMAND ----------

--SHOW TABLES IN global_temp;

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ### 6. Comparison at a Glance
-- MAGIC
-- MAGIC | Feature | Stored View | Temporary View | Global Temporary View |
-- MAGIC |--------|------------|----------------|------------------------|
-- MAGIC | **Persisted?** | Yes (in a database) | No (session‑scoped) | No (cluster‑scoped) |
-- MAGIC | **Visible across sessions?** | Yes | No | Yes (same cluster) |
-- MAGIC | **Visible across clusters?** | Yes | No | No |
-- MAGIC | **Drop behaviour** | Manual (`DROP VIEW`) | Auto‑dropped at session end | Auto‑dropped on cluster restart |
-- MAGIC | **Keyword** | `CREATE VIEW` | `CREATE TEMP VIEW` | `CREATE GLOBAL TEMP VIEW` |
-- MAGIC | **Qualified name** | `db.view_name` | `view_name` | `global_temp.view_name` |
-- MAGIC
-- MAGIC ---
-- MAGIC
-- MAGIC ### 7. Practical Use Cases
-- MAGIC - **Stored views**: reusable, shared logic across teams; can be used in BI tools, dashboards.
-- MAGIC - **Temporary views**: quick intermediate results within a single notebook session.
-- MAGIC - **Global temporary views**: share common data across multiple notebooks running on the same cluster without re‑creating them.
-- MAGIC
-- MAGIC ---
-- MAGIC
-- MAGIC ### 📌 Interview Quick Recap
-- MAGIC - A view is a saved query, not data storage.
-- MAGIC - **Stored views** persist in the database and require explicit `DROP`.
-- MAGIC - **Temp views** are session‑scoped and vanish when the notebook detaches or the cluster restarts.
-- MAGIC - **Global temp views** are cluster‑scoped, accessible by any notebook on that cluster via `global_temp`.
-- MAGIC - Remember the scope: session (temp) vs. cluster (global temp) vs. permanent (stored).
-- MAGIC