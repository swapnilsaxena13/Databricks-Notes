-- Databricks notebook source
-- MAGIC %md
-- MAGIC To create external tables in Databricks Express or Free Edition, you first need to set up a connection to an Amazon S3 bucket to store the table data.
-- MAGIC
-- MAGIC - Step 1: Create an S3 bucket in your AWS account
-- MAGIC - Step 2: Configure [External Location](https://docs.databricks.com/aws/en/connect/unity-catalog/cloud-storage/external-locations#-option-1-create-an-external-location-for-an-s3-bucket-using-an-aws-cloudformation-template) object in this workspace to connect your S3 bucket to Databricks
-- MAGIC - Step 3: In the cells below, replace _&lt;BUCKET&gt;_ with the name of your S3 bucket, and then proceed to run them.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ---
-- MAGIC
-- MAGIC # Databases & Tables – Hands‑On Demo Notes
-- MAGIC
-- MAGIC ### 🧪 What we did in the demo
-- MAGIC We used the Databricks notebook to create and drop different types of tables, and saw the difference between **managed** and **external** tables.
-- MAGIC
-- MAGIC ---
-- MAGIC
-- MAGIC ### 1. Starting Point – The Hive Metastore
-- MAGIC - The Data Explorer shows the **hive_metastore**, which contains a default database named `default`.
-- MAGIC - We can see all databases and tables here, even without writing code.
-- MAGIC
-- MAGIC ---

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Managed Tables

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ---
-- MAGIC ### 2. Creating a Managed Table (in `default`)
-- MAGIC ```sql
-- MAGIC CREATE TABLE managed_default (id INT, name STRING);
-- MAGIC INSERT INTO managed_default VALUES (1, 'Alice');
-- MAGIC ```
-- MAGIC - Because we didn’t use the `LOCATION` keyword, this is a **managed table**.
-- MAGIC - **Check where it lives**:
-- MAGIC   ```sql
-- MAGIC   DESCRIBE EXTENDED managed_default;
-- MAGIC   ```
-- MAGIC   - `Location` → `dbfs:/user/hive/warehouse/managed_default/`
-- MAGIC   - `Type` → `MANAGED`
-- MAGIC
-- MAGIC 💡 *Key fact*: Managed tables go inside the Hive warehouse folder automatically.

-- COMMAND ----------

CREATE CATALOG IF NOT EXISTS demo_cat
MANAGED LOCATION 's3://<BUCKET>';

USE CATALOG demo_cat;

-- COMMAND ----------

CREATE TABLE managed_default
  (width INT, length INT, height INT);

INSERT INTO managed_default
VALUES (3 INT, 2 INT, 1 INT)

-- COMMAND ----------

DESCRIBE EXTENDED managed_default

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## External Tables

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ---
-- MAGIC
-- MAGIC ### 3. Creating an External Table (in `default`)
-- MAGIC ```sql
-- MAGIC CREATE TABLE external_default (id INT, name STRING)
-- MAGIC LOCATION '/mnt/demo/external_default';
-- MAGIC INSERT INTO external_default VALUES (1, 'Bob');
-- MAGIC ```
-- MAGIC - Because we gave a `LOCATION` path, this is an **external table**.
-- MAGIC - **Check metadata**:
-- MAGIC   ```sql
-- MAGIC   DESCRIBE EXTENDED external_default;
-- MAGIC   ```
-- MAGIC   - `Location` → `dbfs:/mnt/demo/external_default/` (not in warehouse)
-- MAGIC   - `Type` → `EXTERNAL`
-- MAGIC
-- MAGIC ---

-- COMMAND ----------

CREATE TABLE external_default
  (width INT, length INT, height INT)
LOCATION 's3://<BUCKET>/external_storage/external_default';
  
INSERT INTO external_default
VALUES (3 INT, 2 INT, 1 INT)

-- COMMAND ----------

DESCRIBE EXTENDED external_default

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Dropping Tables

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ---
-- MAGIC ### 4. The Big Drop Test – See the Difference
-- MAGIC | Action | Managed Table (`managed_default`) | External Table (`external_default`) |
-- MAGIC |--------|------------------------------------|--------------------------------------|
-- MAGIC | `DROP TABLE` | Table disappears from metastore. **All data files are deleted.** | Table disappears from metastore. **Data files remain** in the given location. |
-- MAGIC | **Verify** | Folder `/user/hive/warehouse/managed_default/` is gone. | Folder `/mnt/demo/external_default/` is still there. |
-- MAGIC
-- MAGIC ✅ *Takeaway*:
-- MAGIC - Drop a managed table → data deleted.
-- MAGIC - Drop an external table → only the “pointer” (metadata) is removed; the actual files stay safe.
-- MAGIC
-- MAGIC

-- COMMAND ----------

DROP TABLE managed_default

-- COMMAND ----------

SELECT * FROM managed_default

-- COMMAND ----------

-- Note: It is not permitted to list the files of managed tables. You may examine the table files directly in your S3 bucket.
--%fs ls '/path/to/managed_default'

-- COMMAND ----------

DROP TABLE external_default

-- COMMAND ----------

-- MAGIC %fs ls 's3://<BUCKET>/external_storage/external_default'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Creating Schemas

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ---
-- MAGIC
-- MAGIC ### 5. Creating a New Database (Managed Location)
-- MAGIC ```sql
-- MAGIC CREATE SCHEMA my_db;  -- same as CREATE DATABASE my_db
-- MAGIC ```
-- MAGIC - The database folder appears as `my_db.db` inside `/user/hive/warehouse/`.  
-- MAGIC - Check its location:
-- MAGIC   ```sql
-- MAGIC   DESCRIBE DATABASE EXTENDED my_db;
-- MAGIC   ```
-- MAGIC
-- MAGIC Now switch to it:
-- MAGIC ```sql
-- MAGIC USE my_db;
-- MAGIC ```
-- MAGIC - All later `CREATE TABLE` statements will belong to `my_db` unless you specify another database.
-- MAGIC
-- MAGIC #### a. Managed table inside `my_db`
-- MAGIC ```sql
-- MAGIC CREATE TABLE managed_in_db (id INT);
-- MAGIC ```
-- MAGIC - Data is stored under `/user/hive/warehouse/my_db.db/managed_in_db/`.
-- MAGIC - `DROP TABLE managed_in_db` → data deleted.
-- MAGIC
-- MAGIC #### b. External table inside `my_db`
-- MAGIC ```sql
-- MAGIC CREATE TABLE external_in_db (id INT)
-- MAGIC LOCATION '/mnt/demo/external_in_db';
-- MAGIC ```
-- MAGIC - Data stored at `/mnt/demo/external_in_db/`.
-- MAGIC - `DROP TABLE external_in_db` → data **not** deleted (still in that external location).
-- MAGIC

-- COMMAND ----------

CREATE SCHEMA new_default

-- COMMAND ----------

DESCRIBE DATABASE EXTENDED new_default

-- COMMAND ----------

USE SCHEMA new_default;

CREATE TABLE managed_new_default
  (width INT, length INT, height INT);
  
INSERT INTO managed_new_default
VALUES (3 INT, 2 INT, 1 INT);

-----------------------------------

CREATE TABLE external_new_default
  (width INT, length INT, height INT)
LOCATION 's3://<BUCKET>/external_storage/external_new_default';
  
INSERT INTO external_new_default
VALUES (3 INT, 2 INT, 1 INT);

-- COMMAND ----------

DESCRIBE EXTENDED managed_new_default

-- COMMAND ----------

DESCRIBE EXTENDED external_new_default

-- COMMAND ----------

DROP TABLE managed_new_default;
DROP TABLE external_new_default;

-- COMMAND ----------

-- Note: It is not permitted to list the files of managed tables. You may examine the table files directly in your S3 bucket.
--%fs ls '/path/to/managed_new_default'

-- COMMAND ----------

-- MAGIC %fs ls 's3://<BUCKET>/external_storage/external_new_default'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Creating Schemas in Custom Location

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ---
-- MAGIC
-- MAGIC ### 6. Database in a Custom Location (External Database)
-- MAGIC You can even place an entire database in a custom path.
-- MAGIC ```sql
-- MAGIC CREATE SCHEMA my_custom_db
-- MAGIC LOCATION '/mnt/custom_db_folder';
-- MAGIC ```
-- MAGIC - `DESCRIBE DATABASE EXTENDED my_custom_db` shows the location is `/mnt/custom_db_folder`, not the default warehouse.
-- MAGIC
-- MAGIC Now create tables inside it:
-- MAGIC ```sql
-- MAGIC USE my_custom_db;
-- MAGIC ```
-- MAGIC
-- MAGIC - **Managed table** (no `LOCATION`):  
-- MAGIC   The data goes **inside** the database’s custom folder.  
-- MAGIC   Example: `/mnt/custom_db_folder/managed_custom/`.  
-- MAGIC   Dropping it **deletes** the data.
-- MAGIC - **External table** (with its own `LOCATION`):  
-- MAGIC   Data is **outside** the database folder, at the location you specified.  
-- MAGIC   Dropping it **keeps** the data.
-- MAGIC
-- MAGIC ---
-- MAGIC
-- MAGIC ### 7. Simple Analogy to Remember
-- MAGIC - **Managed table** = Databricks “owns” everything. If you delete the table, it’s like throwing away a book and the only copy.
-- MAGIC - **External table** = Databricks just keeps a “note” where the book is. Deleting the note doesn’t touch the book.
-- MAGIC
-- MAGIC ---

-- COMMAND ----------

CREATE SCHEMA custom
MANAGED LOCATION 's3://<BUCKET>/custom_schemas'

-- COMMAND ----------

DESCRIBE DATABASE EXTENDED custom

-- COMMAND ----------

USE SCHEMA custom;

CREATE TABLE managed_custom
  (width INT, length INT, height INT);
  
INSERT INTO managed_custom
VALUES (3 INT, 2 INT, 1 INT);

-----------------------------------

CREATE TABLE external_custom
  (width INT, length INT, height INT)
LOCATION 's3://<BUCKET>/external_storage/external_custom';
  
INSERT INTO external_custom
VALUES (3 INT, 2 INT, 1 INT);

-- COMMAND ----------

DESCRIBE EXTENDED managed_custom

-- COMMAND ----------

DESCRIBE EXTENDED external_custom

-- COMMAND ----------

DROP TABLE managed_custom;
DROP TABLE external_custom;

-- COMMAND ----------

-- Note: It is not permitted to list the files of managed tables. You may examine the table files directly in your S3 bucket.
--%fs ls '/path/to/managed_custom'

-- COMMAND ----------

-- MAGIC %fs ls 's3://<BUCKET>/external_storage/external_custom'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC ### 8. Useful Commands Cheat Sheet
-- MAGIC | Task | Command |
-- MAGIC |------|---------|
-- MAGIC | Create managed table | `CREATE TABLE t (col INT);` |
-- MAGIC | Create external table | `CREATE TABLE t (col INT) LOCATION '/mnt/path';` |
-- MAGIC | See table location & type | `DESCRIBE EXTENDED t;` |
-- MAGIC | See database location | `DESCRIBE DATABASE EXTENDED db;` |
-- MAGIC | Switch database | `USE db;` |
-- MAGIC | Drop table (managed) | `DROP TABLE t;` (deletes data) |
-- MAGIC | Drop table (external) | `DROP TABLE t;` (keeps data) |
-- MAGIC
-- MAGIC ---
-- MAGIC
-- MAGIC ### 📌 Interview Quick Recap
-- MAGIC - **Managed vs External** is all about who controls the data.
-- MAGIC - The `LOCATION` keyword makes a table external.
-- MAGIC - Dropping a managed table also deletes its files; dropping an external table leaves files untouched.
-- MAGIC - You can see the location and type of any table with `DESCRIBE EXTENDED`.
-- MAGIC - A database can also be stored in a custom location using `LOCATION` when you create it.