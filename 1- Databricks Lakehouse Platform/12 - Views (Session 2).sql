-- Databricks notebook source
--USE CATALOG hive_metastore;

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

--SHOW TABLES IN global_temp;

-- COMMAND ----------

--SELECT * FROM global_temp.global_temp_view_latest_phones;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Dropping Views

-- COMMAND ----------

DROP TABLE smartphones;

DROP VIEW view_apple_phones;
--DROP VIEW global_temp.global_temp_view_latest_phones;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Views Hands‑On Demo – Key Observations
-- MAGIC
-- MAGIC ### 🧪 What We Did
-- MAGIC 1. Created a base table `smartphones` (id, name, brand, release_year) with 10 rows.
-- MAGIC 2. Created one of each view type – all from the same notebook.
-- MAGIC 3. Opened a **new notebook** (a new Spark session) to test visibility across sessions.
-- MAGIC
-- MAGIC ---
-- MAGIC
-- MAGIC ### 📝 Views Created
-- MAGIC
-- MAGIC #### 1. Stored View (`view_apple_phones`)
-- MAGIC - Command: `CREATE VIEW view_apple_phones AS SELECT * FROM smartphones WHERE brand = 'Apple';`
-- MAGIC - **Persisted** in the `default` database (visible in `SHOW TABLES` and Data Explorer).
-- MAGIC - **Visible** in the new notebook (new session).
-- MAGIC - **Drop**: manually with `DROP VIEW`.
-- MAGIC
-- MAGIC #### 2. Temporary View (`temporary_view_phones_brands`)
-- MAGIC - Command: `CREATE TEMP VIEW temporary_view_phones_brands AS SELECT DISTINCT brand FROM smartphones;`
-- MAGIC - `SHOW TABLES` shows `isTemporary = true`.
-- MAGIC - **Not persisted** to any database.
-- MAGIC - **NOT visible** in the new notebook (new session).
-- MAGIC
-- MAGIC #### 3. Global Temporary View (`global_temp_view_latest_phones`)
-- MAGIC - Command: `CREATE GLOBAL TEMP VIEW global_temp_view_latest_phones AS SELECT * FROM smartphones WHERE release_year > 2020 ORDER BY release_year DESC;`
-- MAGIC - Stored in the special `global_temp` database (a temporary database tied to the cluster).
-- MAGIC - To query: `SELECT * FROM global_temp.global_temp_view_latest_phones;`
-- MAGIC - **Visible** in the new notebook (new session) **as long as the cluster is still running**.
-- MAGIC
-- MAGIC ---
-- MAGIC
-- MAGIC ### 🔍 Visibility Tests Across Sessions
-- MAGIC
-- MAGIC | Object | Exists in New Session? | Why |
-- MAGIC |--------|------------------------|-----|
-- MAGIC | Base table `smartphones` | ✅ Yes | Tables are persistent across sessions. |
-- MAGIC | Stored view `view_apple_phones` | ✅ Yes | Stored views are saved in the database, just like tables. |
-- MAGIC | Temporary view `temporary_view_phones_brands` | ❌ No | Temp views exist only within the session that created them. |
-- MAGIC | Global temp view `global_temp_view_latest_phones` | ✅ Yes | Global temp views live on the cluster (`global_temp`), accessible to any notebook on that cluster. |
-- MAGIC
-- MAGIC ---
-- MAGIC
-- MAGIC ### 💡 Key Takeaways for Interview
-- MAGIC - **Stored views** = permanent, cross‑session, cross‑cluster, must be dropped explicitly.
-- MAGIC - **Temporary views** = strictly single‑session; gone when you detach/reattach, restart Python, or open another notebook.
-- MAGIC - **Global temporary views** = cluster‑scoped; survive session changes but vanish on cluster restart.
-- MAGIC - `SHOW TABLES` lists stored and temp views (temp view has `isTemporary = true`).  
-- MAGIC   `SHOW TABLES IN global_temp` shows global temp views.
-- MAGIC - Always qualify global temp views with `global_temp.<view_name>`.
-- MAGIC
-- MAGIC ---
-- MAGIC
-- MAGIC ### 🧹 Cleanup
-- MAGIC - Drop base table: `DROP TABLE smartphones;`
-- MAGIC - Drop stored view: `DROP VIEW view_apple_phones;` (temp views disappear automatically).

-- COMMAND ----------

