-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Delta Lake Advanced Features – Interview Notes
-- MAGIC
-- MAGIC ### 1. Time Travel (Data Versioning)
-- MAGIC - Every write/update/delete on a Delta table is automatically **versioned**.
-- MAGIC - The **transaction log** holds the complete history, so you can query past snapshots.
-- MAGIC - Two ways to query an older version:
-- MAGIC
-- MAGIC | Method | Syntax example |
-- MAGIC |--------|----------------|
-- MAGIC | **Timestamp** | `SELECT * FROM table TIMESTAMP AS OF '2023-01-01 10:00:00'` |
-- MAGIC | **Version number** | `SELECT * FROM table VERSION AS OF 5` or simply `SELECT * FROM table@v5` |
-- MAGIC
-- MAGIC - **Rollback** a table to a previous version using `RESTORE TABLE`:
-- MAGIC   ```sql
-- MAGIC   RESTORE TABLE table_name TO TIMESTAMP AS OF '...';
-- MAGIC   RESTORE TABLE table_name TO VERSION AS OF 5;
-- MAGIC   ```
-- MAGIC   (Handy when a bad pipeline job corrupts data.)
-- MAGIC
-- MAGIC - Use `DESCRIBE HISTORY table_name` to see all available versions.
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Delta Time Travel

-- COMMAND ----------

--USE CATALOG hive_metastore

-- COMMAND ----------

DESCRIBE HISTORY employees

-- COMMAND ----------

SELECT * 
FROM employees VERSION AS OF 4

-- COMMAND ----------

SELECT * FROM employees@v4

-- COMMAND ----------

DELETE FROM employees

-- COMMAND ----------

SELECT * FROM employees

-- COMMAND ----------

DESCRIBE HISTORY employees

-- COMMAND ----------

RESTORE TABLE employees TO VERSION AS OF 6

-- COMMAND ----------

SELECT * FROM employees

-- COMMAND ----------

DESCRIBE HISTORY employees

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## OPTIMIZE Command

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ### 2. Performance Optimization: Compaction (`OPTIMIZE`)
-- MAGIC - Small files hurt read performance (too many file listing/index operations).
-- MAGIC - **`OPTIMIZE`** command compacts many small Parquet files into fewer, larger files.
-- MAGIC   ```sql
-- MAGIC   OPTIMIZE table_name;
-- MAGIC   ```
-- MAGIC - It does **not** change the data logically, only physically reorganises it.
-- MAGIC
-- MAGIC ### 3. Z‑Order Indexing (Data Skipping)
-- MAGIC - Z‑Ordering **co‑locates** related data inside the same files.
-- MAGIC - Syntax: `OPTIMIZE table_name ZORDER BY (column1, column2);`
-- MAGIC - *Example*: If you Z‑Order on `id`, a file might contain IDs 1‑50, another 51‑100.
-- MAGIC - When you query `WHERE id = 30`, Delta **skips** the second file entirely → much faster reads.
-- MAGIC - Great for columns frequently used in filters (high cardinality columns give best results).
-- MAGIC

-- COMMAND ----------

DESCRIBE DETAIL employees

-- COMMAND ----------

-- Note: The following command has no effect in this case, as an optimization operation was automatically executed on the table in version 6.
OPTIMIZE employees
ZORDER BY id

-- COMMAND ----------

DESCRIBE DETAIL employees

-- COMMAND ----------

DESCRIBE HISTORY employees

-- COMMAND ----------

--%fs ls '/path/to/employees'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## VACUUM Command

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ### 4. Garbage Collection (`VACUUM`)
-- MAGIC - Over time, the table directory accumulates unused files (uncommitted files, files no longer referenced in the latest log).
-- MAGIC - **`VACUUM`** removes these unused data files.
-- MAGIC - **Retention period** (default **7 days**) prevents accidental deletion of files that might still be needed by running operations or for time travel.
-- MAGIC   ```sql
-- MAGIC   VACUUM table_name RETAIN 168 HOURS;  -- custom retention (e.g., 7 days in hours)
-- MAGIC   ```
-- MAGIC - **Important**: After vacuuming, you **cannot time travel** to versions older than the retention period, because the physical files are gone.

-- COMMAND ----------

VACUUM employees

-- COMMAND ----------

--%fs ls '/path/to/employees'

-- COMMAND ----------

VACUUM employees RETAIN 0 HOURS

-- COMMAND ----------

-- Note: The retentionDurationCheck configuration is not available on Serverless compute
-- SET spark.databricks.delta.retentionDurationCheck.enabled = false;

-- Instead, use table properties
ALTER TABLE employees SET TBLPROPERTIES ('delta.deletedFileRetentionDuration'='interval 0 hours')

-- COMMAND ----------

VACUUM employees RETAIN 0 HOURS

-- COMMAND ----------

--%fs ls '/path/to/employees'

-- COMMAND ----------

-- Note: You may still see results due to a cached version of the table in the serverless compute environment
SELECT * FROM employees@v1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Dropping Tables

-- COMMAND ----------

DROP TABLE employees

-- COMMAND ----------

SELECT * FROM employees

-- COMMAND ----------

--%fs ls '/path/to/employees'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC ### 5. Commands Summary
-- MAGIC | Feature | Command | Purpose |
-- MAGIC |---------|---------|---------|
-- MAGIC | View history | `DESCRIBE HISTORY table;` | List all table versions |
-- MAGIC | Query old snapshot | `SELECT … TIMESTAMP AS OF …` or `VERSION AS OF …` | Time travel read |
-- MAGIC | Rollback | `RESTORE TABLE … TO VERSION AS OF …` | Undo a bad write |
-- MAGIC | Compact files | `OPTIMIZE table;` | Merge small files, improve read speed |
-- MAGIC | Indexing | `OPTIMIZE table ZORDER BY (col);` | Co‑locate data, enable data skipping |
-- MAGIC | Cleanup | `VACUUM table RETAIN 168 HOURS;` | Remove old unused files |
-- MAGIC
-- MAGIC ---
-- MAGIC
-- MAGIC ### 📌 Interview Quick Recap
-- MAGIC - **Time travel** works because the transaction log keeps every version; you can query by timestamp or version number, and even restore a previous state.
-- MAGIC - **`OPTIMIZE`** merges small files; **Z‑Ordering** places related data together so Spark can skip files during filtered queries → huge performance gain.
-- MAGIC - **`VACUUM`** deletes physical files older than a retention threshold (default 7 days). Use carefully: it breaks time travel beyond that retention.
-- MAGIC - These features together give Delta its reliability, performance, and governance on cheap object storage.
-- MAGIC