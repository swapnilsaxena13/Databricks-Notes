# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks Interview Notes – Core Concepts
# MAGIC
# MAGIC ### 1. What is Databricks?
# MAGIC - **Databricks** is a **multi-cloud Lakehouse platform** built on **Apache Spark**.
# MAGIC - It combines the best of **data lakes** and **data warehouses** into one unified platform.
# MAGIC - *Simple analogy*: A lakehouse = a data lake (stores any raw data) + a data warehouse (structured, high‑performance queries) with added governance and ML capabilities.
# MAGIC
# MAGIC ### 2. What is a Data Lakehouse?
# MAGIC - **Unified analytics platform** that provides:
# MAGIC   - **Openness & flexibility** of a data lake (supports all data types, cheap storage).
# MAGIC   - **Reliability, strong governance & performance** of a data warehouse (ACID transactions, schema enforcement, high‑speed queries).
# MAGIC - You can do **data engineering, analytics, and AI** all in one place.
# MAGIC
# MAGIC ### 3. Databricks Lakehouse Architecture (3 Layers)
# MAGIC Databricks is logically divided into three layers:
# MAGIC
# MAGIC 1. **Cloud Service**  
# MAGIC    - Available on **Azure, AWS, and GCP** (multi‑cloud).
# MAGIC 2. **Databricks Runtime**  
# MAGIC    - Core engine: **Apache Spark**, **Delta Lake**, and other system libraries.
# MAGIC    - Pre‑installed on every cluster you create.
# MAGIC 3. **Workspace**  
# MAGIC    - The interactive UI where you write notebooks, manage clusters, jobs, etc.  
# MAGIC    - Allows you to develop and run data + ML workloads.
# MAGIC
# MAGIC ![image_1784346261376.png](./image_1784346261376.png "image_1784346261376.png")
# MAGIC
# MAGIC ### 4. Control Plane vs Data Plane
# MAGIC This is a critical architectural distinction (very common interview question).
# MAGIC
# MAGIC | Component       | Where it resides               | What it contains                                                                 |
# MAGIC |-----------------|--------------------------------|----------------------------------------------------------------------------------|
# MAGIC | **Control Plane** | Databricks account (managed by Databricks) | Workspace, UI, Cluster Manager, Workflows, Notebooks, job scheduling, etc.       |
# MAGIC | **Data Plane**    | **Your own cloud subscription** (AWS/Azure/GCP) | **Storage** (DBFS buckets) and **Compute** (cluster VMs). All your data lives here. |
# MAGIC
# MAGIC - **Key takeaway**: Your **data never leaves your cloud account**. Databricks provides the control tools, but the actual storage and compute are in your environment. This ensures data security and compliance.
# MAGIC
# MAGIC ![image_1784346227219.png](./image_1784346227219.png "image_1784346227219.png")
# MAGIC
# MAGIC ### 5. Databricks File System (DBFS)
# MAGIC - DBFS is a **distributed file system** pre‑installed on every Databricks cluster.
# MAGIC - It is **not a physical storage**—it’s an **abstraction layer** over your cloud object storage.
# MAGIC   - *Example*: On Azure, it maps to ADLS Gen2 / Blob Storage; on AWS, to S3 buckets.
# MAGIC - **How it works**:  
# MAGIC   - You write a file to `/dbfs/...` in your notebook.  
# MAGIC   - The file is actually stored in the underlying cloud storage.  
# MAGIC - **Why it matters**: Even after you terminate the cluster, **all data remains safe** in your cloud storage.
# MAGIC
# MAGIC ### 6. Apache Spark on Databricks
# MAGIC - Databricks was founded by the **original creators of Apache Spark**.
# MAGIC - Spark processes data **in‑memory** across a cluster of nodes → very fast.
# MAGIC - **Languages supported**: Scala, Python, SQL, R, Java.
# MAGIC - **Processing types**:
# MAGIC   - **Batch processing** (static data)
# MAGIC   - **Stream processing** (real‑time data)
# MAGIC - **Data types supported**:
# MAGIC   - Structured (tables)
# MAGIC   - Semi‑structured (JSON, XML)
# MAGIC   - Unstructured (images, videos, text)
# MAGIC - Data is **distributed** across nodes and processed in parallel.
# MAGIC
# MAGIC ### 7. Cluster & Compute
# MAGIC - You provision virtual machines (nodes) from your cloud provider → forms a **cluster**.
# MAGIC - Each cluster comes with **pre‑installed Databricks Runtime** (Spark, Delta Lake, etc.).
# MAGIC - All compute runs in **your own data plane** (your cloud account).
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📌 Interview Tips – How to Present These Concepts
# MAGIC - When asked “What is Databricks?”, start with: *“It’s a multi‑cloud Lakehouse platform built on Apache Spark, unifying data lake and warehouse capabilities.”*
# MAGIC - Always mention the **control plane / data plane separation** — it’s a strong differentiator for security and data residency.
# MAGIC - Explain DBFS as an **abstraction** that ensures durability and simplifies file operations.
# MAGIC - Emphasise that Spark in Databricks is **optimised** (Databricks Runtime) and supports **multiple languages and both batch/stream processing**.
# MAGIC
# MAGIC ---