# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Delta Lake Table Operations – CTAS, Constraints & Cloning
# MAGIC
# MAGIC ### 1. CTAS – Create Table As Select
# MAGIC - **What is CTAS?**  
# MAGIC   A way to create a new Delta table and fill it with data in one step using a `SELECT` query.
# MAGIC - **Syntax pattern**:
# MAGIC   ```sql
# MAGIC   CREATE TABLE target_table
# MAGIC   [COMMENT '...']
# MAGIC   [PARTITIONED BY (col)]
# MAGIC   [LOCATION 'path']
# MAGIC   AS SELECT col1, col2, ... FROM source_table;
# MAGIC   ```
# MAGIC - **Key points**:
# MAGIC   - **No manual schema**: The new table’s columns and data types are automatically inferred from the `SELECT` result.
# MAGIC   - **Data inserted immediately**: You don't need a separate `INSERT INTO`.
# MAGIC   - **You can transform while creating**: rename columns, pick a subset of columns, or filter rows.
# MAGIC - **Options you can add**:
# MAGIC   - `COMMENT` – a description of the table (helps with discovery).
# MAGIC   - `PARTITIONED BY` – split data into sub‑folders by column values (e.g., by city, birthdate).  
# MAGIC     ⚠️ Best for large tables only; avoid on small/medium tables (causes many small files).
# MAGIC   - `LOCATION` – makes the table **external**, storing data outside the default warehouse.
# MAGIC - **CTAS vs regular CREATE TABLE**:
# MAGIC
# MAGIC | Regular `CREATE TABLE` | CTAS (`CREATE TABLE ... AS SELECT`) |
# MAGIC |------------------------|-------------------------------------|
# MAGIC | You must write the schema manually | Schema inferred automatically |
# MAGIC | Creates an empty table; need separate `INSERT` | Creates and fills the table in one step |
# MAGIC | Useful for defining precise schema upfront | Quick way to copy/transform data |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 2. Table Constraints (Data Quality Rules)
# MAGIC - Databricks supports two types of constraints on Delta tables:
# MAGIC   - **`NOT NULL`** – the column cannot contain `NULL` values.
# MAGIC   - **`CHECK`** – a boolean expression that every row must satisfy (like a `WHERE` clause filter).
# MAGIC - **Important rules**:
# MAGIC   - Before adding a constraint, **make sure existing data does not violate it**; otherwise the `ALTER TABLE` will fail.
# MAGIC   - After the constraint is added, any new write (INSERT, UPDATE) that breaks the constraint will **fail**.
# MAGIC - **Example**:
# MAGIC   ```sql
# MAGIC   ALTER TABLE my_table ADD CONSTRAINT valid_date CHECK (date > '2020-01-01');
# MAGIC   ```
# MAGIC   (This will reject any row with `date` older than 2020-01-01.)
# MAGIC
# MAGIC 💡 **Interview tip**: Constraints enforce data quality at the table level. They are not enforced on existing data until you validate it first.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 3. Cloning a Delta Table (Making a Copy)
# MAGIC - Used to create a copy of a table for testing, backup, or development.
# MAGIC - Two types:
# MAGIC
# MAGIC | Clone Type | What it copies | Speed | Data movement? | Use case |
# MAGIC |------------|----------------|-------|----------------|----------|
# MAGIC | **Deep Clone** | **Data + metadata** (full copy) | Slow (for large tables) | Yes, all data files copied | Complete independent backup; can incrementally sync later. |
# MAGIC | **Shallow Clone** | **Only the transaction log** (metadata) | Very fast | No data movement | Quick test environment; points to source data files until changes are made. |
# MAGIC
# MAGIC - **Commands**:
# MAGIC   ```sql
# MAGIC   -- Deep clone
# MAGIC   CREATE TABLE target DEEP CLONE source;
# MAGIC
# MAGIC   -- Shallow clone
# MAGIC   CREATE TABLE target SHALLOW CLONE source;
# MAGIC   ```
# MAGIC - **Important behaviours**:
# MAGIC   - After cloning, changes to the clone **do not affect the source** table (and vice‑versa).
# MAGIC   - Both deep and shallow clones track their own changes separately.
# MAGIC   - **Deep clone incremental sync**: Run the same `CREATE TABLE ... DEEP CLONE` again (with `OR REPLACE` or a new table name) to catch up new data from the source.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📌 Quick Recap for Interviews
# MAGIC - **CTAS** = create + insert in one step; schema is automatic; can transform on the fly.
# MAGIC - Use `COMMENT` to describe tables; `PARTITIONED BY` only for huge tables.
# MAGIC - **Constraints** (`NOT NULL`, `CHECK`) enforce data quality – validate existing data first.
# MAGIC - **Deep Clone** = full copy (data + log), good for backups; **Shallow Clone** = instant copy (only log), great for testing.
# MAGIC - Clones are independent after creation – your experiments never risk the source table.
# MAGIC