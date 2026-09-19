# Databricks notebook source
# MAGIC %md
# MAGIC # Data File Layout Optimization
# MAGIC
# MAGIC ### 1. What is Data File Layout?
# MAGIC - Refers to how the underlying **Parquet data files** of a Delta table are organized on storage.
# MAGIC - A well‑organized layout enables **data skipping** – Spark reads only the files that contain relevant data, ignoring the rest.
# MAGIC - Goal: reduce I/O, speed up queries, and lower compute cost.
# MAGIC
# MAGIC ### 2. Optimization Techniques Overview
# MAGIC Three main strategies:
# MAGIC 1. **Partitioning** (Hive‑style)
# MAGIC 2. **Z‑Order Indexing**
# MAGIC 3. **Liquid Clustering** (and its automatic variant)
# MAGIC
# MAGIC ### 3. Partitioning (`PARTITION BY`)
# MAGIC - Divides data into **subdirectories** based on a chosen column (e.g., `year`).
# MAGIC - Created with `CREATE TABLE ... PARTITION BY (year)`.
# MAGIC - When you query with a filter on the partition column, **entire partitions (folders) are skipped**.
# MAGIC - **Best for**: large tables, low‑cardinality columns (e.g., date, region, category).
# MAGIC - **Downsides**:
# MAGIC   - Not beneficial for small/medium tables – can create many small files.
# MAGIC   - `OPTIMIZE` runs per partition, so small files may persist across partitions.
# MAGIC   - High‑cardinality columns (user ID, transaction ID) lead to **too many partitions** – inefficient.
# MAGIC   - Changing partition columns requires a **full table rewrite** (expensive).
# MAGIC
# MAGIC ### 4. Z‑Order Indexing (`ZORDER BY`)
# MAGIC - Groups related data **inside** files without creating subfolders.
# MAGIC - Syntax: `OPTIMIZE table ZORDER BY (column)`.
# MAGIC - Data skipping can then skip entire files that don’t contain the needed values.
# MAGIC - **Best for**: high‑cardinality columns (e.g., user IDs, timestamps).
# MAGIC - **Downside**: **not incremental**. Every time new data arrives, you must run `OPTIMIZE` again, which may rewrite many (or all) files – can be heavy.
# MAGIC
# MAGIC ### 5. Liquid Clustering (`CLUSTER BY`)
# MAGIC - An improved, more flexible version of Z‑ordering.
# MAGIC - Defined **at table creation** (or later) with `CLUSTER BY (col1, col2)`.
# MAGIC - When you run `OPTIMIZE`, it works **incrementally**:
# MAGIC   - Already clustered files are left untouched.
# MAGIC   - Only new, unoptimised files are reorganized.
# MAGIC - **Key advantages**:
# MAGIC   - No need to specify clustering keys in every `OPTIMIZE` command.
# MAGIC   - Redefining clustering keys **does not rewrite existing data** – easily adapt to changing business needs.
# MAGIC   - Efficient, low‑overhead maintenance.
# MAGIC - **Incompatibility**: cannot be used together with partitioning or manual Z‑ordering. To migrate, create a new table and project old partition/Z‑order columns as clustering keys.
# MAGIC
# MAGIC ### 6. Choosing Clustering Keys
# MAGIC - Ideal keys are columns frequently used in query **filters** (WHERE clauses).
# MAGIC - If you don’t know query patterns yet, use **Automatic Liquid Clustering**.
# MAGIC
# MAGIC ### 7. Automatic Liquid Clustering (`CLUSTER BY AUTO`)
# MAGIC - Databricks **automatically picks clustering keys** by analysing historical query workloads.
# MAGIC - Requires **Predictive Optimization** enabled (available on Unity Catalog managed tables).
# MAGIC - Syntax: `CREATE TABLE ... CLUSTER BY AUTO;` (or `ALTER TABLE ... CLUSTER BY AUTO;`)
# MAGIC - No manual tuning needed – the platform learns and adapts over time.
# MAGIC
# MAGIC ### 8. Comparison Summary
# MAGIC
# MAGIC | Technique | Structure | Cardinality | Incremental? | Maintenance |
# MAGIC |-----------|-----------|-------------|--------------|-------------|
# MAGIC | **Partitioning** | Subdirectories | Low only | Yes (new data goes to partition) | `OPTIMIZE` per partition; full rewrite to change partition columns |
# MAGIC | **Z‑Order** | File grouping | High | No (full table optimize may be needed) | Manual `OPTIMIZE` runs |
# MAGIC | **Liquid Clustering** | File grouping (incremental) | High | Yes | `OPTIMIZE` is incremental and lightweight |
# MAGIC | **Automatic Clustering** | Same as liquid but auto‑chosen keys | Any | Yes | Fully managed |
# MAGIC
# MAGIC ### 9. Practical Commands
# MAGIC - **Partitioning**: `CREATE TABLE t (...) PARTITION BY (year);`
# MAGIC - **Z‑Order**: `OPTIMIZE t ZORDER BY (user_id);`
# MAGIC - **Liquid Clustering (manual)**: `CREATE TABLE t (...) CLUSTER BY (user_id, date);` → `OPTIMIZE t;`
# MAGIC - **Automatic clustering**: `CREATE TABLE t (...) CLUSTER BY AUTO;`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📌 Interview Quick Recap
# MAGIC - **Data file layout** is about organising Parquet files so queries can skip unnecessary data.
# MAGIC - **Partitioning** works like folders – best for low‑cardinality columns; don’t use on high‑cardinality or small tables.
# MAGIC - **Z‑Order** is good for high‑cardinality filters but needs manual re‑optimisation after every data load.
# MAGIC - **Liquid Clustering** is the modern, incremental approach – define once, optimise frequently without heavy rewrites.
# MAGIC - **Automatic Liquid Clustering** lets Databricks choose keys based on workload history – ideal for hands‑off tuning.
# MAGIC - Always choose the technique that matches your data size, query patterns, and maintenance tolerance.