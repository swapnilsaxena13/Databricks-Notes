# Databricks notebook source
# MAGIC %md
# MAGIC # Delta Lake – Interview Notes
# MAGIC
# MAGIC ### 1. What is Delta Lake?
# MAGIC - **Open‑source storage framework** (not proprietary) that brings reliability to data lakes.
# MAGIC - It is a **storage layer**, not a storage format or medium – it sits on top of your existing data lake.
# MAGIC - Enables the **Lakehouse architecture** – unifies data warehouse reliability with data lake flexibility.
# MAGIC - **Not** a data warehouse, and **not** a database service.
# MAGIC
# MAGIC ### 2. Why Delta Lake? (Problems it solves)
# MAGIC - Traditional data lakes suffer from:
# MAGIC   - Data inconsistency
# MAGIC   - No ACID transactions
# MAGIC   - Poor performance on updates/deletes
# MAGIC   - Hard to maintain data quality
# MAGIC - Delta Lake provides:
# MAGIC   - **ACID transactions** on data lakes
# MAGIC   - Scalable metadata handling
# MAGIC   - Full **audit trail** of changes
# MAGIC   - Reliable, consistent reads even during concurrent writes
# MAGIC
# MAGIC ### 3. How Delta Lake Works – Key Components
# MAGIC - Data files are stored in **Parquet** format.
# MAGIC - Alongside the data, Delta maintains a **transaction log** in a hidden `_delta_log` directory.
# MAGIC   - The log consists of **JSON files** (e.g., `000000.json`, `000001.json`).
# MAGIC   - Each JSON file records a committed transaction (insert/update/delete), predicates used, and the list of affected files.
# MAGIC - When you query a Delta table, Spark **reads the transaction log first** to know which Parquet files(stores data column by column; highly optimized for analytics) belong to the current table version.
# MAGIC
# MAGIC ### 4. Transaction Log in Action (Scenarios)
# MAGIC
# MAGIC **Scenario 1 – Initial write**  
# MAGIC - Writer creates Parquet files (file1, file2).  
# MAGIC - After commit, a `000.json` log is written.  
# MAGIC - Reader reads the log → sees file1 & file2 → reads them.
# MAGIC ![image_1784352648723.png](./image_1784352648723.png "image_1784352648723.png")
# MAGIC
# MAGIC **Scenario 2 – Update (copy‑on‑write)**  
# MAGIC - Writer wants to update a record in file1.  
# MAGIC - Instead of modifying file1, it creates a **new file (file3)** with the update.  
# MAGIC - A new JSON log is written that says file1 is no longer part of the table.  
# MAGIC - Reader reads the latest log → sees files 2 & 3 → reads them.  
# MAGIC - *Key point*: Underlying Parquet files are immutable; changes create new files.
# MAGIC ![image_1784352693258.png](./image_1784352693258.png "image_1784352693258.png")
# MAGIC
# MAGIC **Scenario 3 – Concurrent read/write**  
# MAGIC - Writer is writing file4 but hasn’t committed yet.  
# MAGIC - Reader reads the current log (only files 2 & 3) → reads them.  
# MAGIC - Reader never sees incomplete data or deadlocks.  
# MAGIC - *Result*: Always reads the **last committed, consistent view**.
# MAGIC ![image_1784352824688.png](./image_1784352824688.png "image_1784352824688.png")
# MAGIC
# MAGIC **Scenario 4 – Failed write**  
# MAGIC - Writer fails mid‑way, leaving an incomplete file (file5) but **no new log entry**.  
# MAGIC - Reader uses the unchanged log → only sees files 2, 3, 4 → ignores the corrupt file.  
# MAGIC - *Result*: **No dirty reads**.
# MAGIC ![image_1784352807106.png](./image_1784352807106.png "image_1784352807106.png")
# MAGIC
# MAGIC ### 5. Key Benefits Recap
# MAGIC - **ACID transactions** – Atomicity, Consistency, Isolation, Durability on a data lake.
# MAGIC - **Time travel / Audit trail** – every change is recorded in the log, so you can query previous versions.
# MAGIC - **Schema enforcement & evolution** – ensures data quality.
# MAGIC - **Performance** – with file compaction and indexing (not fully covered here, but often asked).
# MAGIC - **100% open source**, works with Spark, can be used outside Databricks.
# MAGIC
# MAGIC ### 6. Important Distinctions for Interviews
# MAGIC - Delta Lake is **not** a database or a data warehouse.
# MAGIC - It is a **table format / storage layer** that uses Parquet for data and JSON for the transaction log.
# MAGIC - The log is the single source of truth – all readers consult it to know the current table state.
# MAGIC - Updates are implemented as **copy‑on‑write** (new Parquet files), never modifying existing files in place.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📌 Interview Quick Recap
# MAGIC - Delta Lake = open‑source storage layer that adds ACID to data lakes.
# MAGIC - Transaction log (`_delta_log`) is the brain; it’s a set of JSON files recording every committed operation.
# MAGIC - Readers **always** get the latest committed, consistent snapshot.
# MAGIC - Failed writes don’t corrupt existing data because they never touch the log.
# MAGIC - Data is stored as Parquet(stores data column by column; highly optimized for analytics); updates generate new files.
# MAGIC - Enables reliable lakehouse architecture on cheap object storage.