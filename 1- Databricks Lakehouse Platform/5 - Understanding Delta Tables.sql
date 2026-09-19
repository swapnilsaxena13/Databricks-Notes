-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC # Working with Delta Lake Tables
-- MAGIC
-- MAGIC ### 1. Data Catalog – Where Tables Live
-- MAGIC - Tables are organized in a **data catalog** (metadata store).
-- MAGIC - Two main catalogs in Databricks:
-- MAGIC   - **hive_metastore** – legacy catalog, simple, no extra setup.
-- MAGIC   - **Unity Catalog** – newer, full governance (fine‑grained permissions, lineage).
-- MAGIC - For learning, we often use `hive_metastore`; for production, Unity Catalog.
-- MAGIC - Set the catalog in a notebook with: `USE CATALOG hive_metastore;`
-- MAGIC
-- MAGIC ### 2. Creating a Delta Lake Table
-- MAGIC - Use standard SQL `CREATE TABLE`:
-- MAGIC   ```sql
-- MAGIC   CREATE TABLE employees (
-- MAGIC     id INT,
-- MAGIC     name STRING,
-- MAGIC     salary DOUBLE
-- MAGIC   );
-- MAGIC   ```
-- MAGIC - **Delta Lake is the default table format** on Databricks. No need to add `USING DELTA` – it’s automatically a Delta table.
-- MAGIC - After creation, the table appears in the catalog (e.g., `default.employees`).
-- MAGIC
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Creating Delta Lake Tables

-- COMMAND ----------

CREATE TABLE employees
  (id INT, name STRING, salary DOUBLE);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Catalog Explorer
-- MAGIC
-- MAGIC Check the created **employees** table in the **Catalog** explorer.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Inserting Data

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 3. Inserting Data & Understanding Files
-- MAGIC - Insert with `INSERT INTO` – each statement is a **separate transaction**.
-- MAGIC   ```sql
-- MAGIC   INSERT INTO employees VALUES (1, 'Alice', 5000);
-- MAGIC   INSERT INTO employees VALUES (2, 'Bob', 6000);
-- MAGIC   ...
-- MAGIC   ```
-- MAGIC - Each transaction writes a **small Parquet file** into the table’s storage directory.
-- MAGIC - **Only the result of the last statement** is displayed when multiple SQL statements run in one cell.
-- MAGIC

-- COMMAND ----------

INSERT INTO employees
VALUES 
  (1, "Adam", 3500.0),
  (2, "Sarah", 4020.5);

INSERT INTO employees
VALUES
  (3, "John", 2999.3),
  (4, "Thomas", 4000.3);

INSERT INTO employees
VALUES
  (5, "Anna", 2500.0);

INSERT INTO employees
VALUES
  (6, "Kim", 6200.3)

-- COMMAND ----------

SELECT * FROM employees

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 4. Examining Table Metadata – `DESCRIBE DETAIL`
-- MAGIC - Command: `DESCRIBE DETAIL employees;`
-- MAGIC - Shows critical information:
-- MAGIC   - **Location** – the cloud storage path where files live.
-- MAGIC   - **Number of files** – how many data files belong to the current table version.
-- MAGIC - Use this to verify the physical layout.
-- MAGIC

-- COMMAND ----------

DESCRIBE DETAIL employees

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Exploring Table Directory

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 8. Exploring Files with `%fs`
-- MAGIC - List files in the table directory: `%fs ls <location>`
-- MAGIC - You’ll see Parquet data files and the `_delta_log` directory.
-- MAGIC - This confirms how Delta organizes data on cloud storage.
-- MAGIC

-- COMMAND ----------

--%fs ls '/path/to/employees'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Updating Table

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 7. Updates – Copy‑on‑Write Behaviour
-- MAGIC - When you run an `UPDATE`, Delta Lake **never modifies existing Parquet files**. (Parquet files are immutable.)
-- MAGIC - It creates **new Parquet files** containing the updated records, and records the old files as `remove` in the transaction log.
-- MAGIC - Example: Update salary for employees starting with ‘A’ → two new files added, two old files marked removed.
-- MAGIC - After the update, `DESCRIBE DETAIL` shows the new **number of files = valid files only** (e.g., 4 instead of 6).
-- MAGIC - The query engine uses the transaction log to know which files are part of the current version → always reads the **latest consistent snapshot**.

-- COMMAND ----------

UPDATE employees 
SET salary = salary + 100
WHERE name LIKE "A%"

-- COMMAND ----------

SELECT * FROM employees

-- COMMAND ----------

--%fs ls '/path/to/employees'

-- COMMAND ----------

DESCRIBE DETAIL employees

-- COMMAND ----------

SELECT * FROM employees

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Exploring Table History

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 5. Viewing Table History – `DESCRIBE HISTORY`
-- MAGIC - `DESCRIBE HISTORY employees;`
-- MAGIC - Returns a list of all operations (versions) on the table, starting from version 0 (table creation).
-- MAGIC - Each row shows version, timestamp, operation (CREATE, WRITE, UPDATE, etc.).
-- MAGIC - This is the full **audit trail** thanks to the transaction log.
-- MAGIC

-- COMMAND ----------

DESCRIBE HISTORY employees

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ### 6. The Transaction Log in Practice
-- MAGIC - Located in the `_delta_log/` subdirectory under the table location.
-- MAGIC - Each committed transaction creates a new JSON file (e.g., `000000.json`, `000001.json`).
-- MAGIC - Inside a JSON log file:
-- MAGIC   - **`add`** – lists new Parquet files added in this transaction.
-- MAGIC   - **`remove`** – lists files that are no longer part of the table (soft‑deleted).
-- MAGIC - Checksum files (`*.crc`) help verify log integrity.
-- MAGIC

-- COMMAND ----------

--%fs ls '/path/to/employees/_delta_log'

-- COMMAND ----------

--%fs head '/path/to/employees/_delta_log/00000000000000000005.json'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ### 9. Practical Commands Cheat Sheet
-- MAGIC | Task | Command |
-- MAGIC |------|---------|
-- MAGIC | Set catalog | `USE CATALOG hive_metastore;` |
-- MAGIC | Create Delta table | `CREATE TABLE table_name (col1 type, col2 type);` |
-- MAGIC | Insert rows | `INSERT INTO table_name VALUES (…);` |
-- MAGIC | Query | `SELECT * FROM table_name;` |
-- MAGIC | View metadata | `DESCRIBE DETAIL table_name;` |
-- MAGIC | View history | `DESCRIBE HISTORY table_name;` |
-- MAGIC | List files | `%fs ls <path>` or `dbutils.fs.ls("<path>")` |
-- MAGIC
-- MAGIC ---
-- MAGIC
-- MAGIC ### 📌 Interview Quick Recap
-- MAGIC - Delta tables are the default on Databricks; you get ACID and versioning automatically.
-- MAGIC - Use `DESCRIBE DETAIL` for physical file info, `DESCRIBE HISTORY` for the audit trail.
-- MAGIC - Updates create new files and remove old ones via the transaction log – no in‑place edits.
-- MAGIC - The transaction log (`_delta_log`) is the single source of truth; it guarantees readers see a consistent view even during writes.
-- MAGIC - All data is stored as Parquet; the log is JSON.
-- MAGIC - Multi‑statement inserts produce multiple small files; be aware of this for performance tuning (compaction/optimize).