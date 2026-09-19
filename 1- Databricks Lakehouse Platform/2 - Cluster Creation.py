# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks Cluster Creation
# MAGIC
# MAGIC ### 1. What is a Cluster?
# MAGIC - A **cluster** is a group of virtual machines (nodes) working together as a single unit.
# MAGIC - Two types of nodes:
# MAGIC   - **Driver node** – coordinates the workers and runs the Spark context. In single-node mode, it also executes the tasks.
# MAGIC   - **Worker nodes** – execute the tasks assigned by the driver in parallel.
# MAGIC
# MAGIC ### 2. Cluster Creation (Basic Steps)
# MAGIC - Go to **Compute** in the left sidebar → Click **Create Compute**.
# MAGIC - Choose a **name** (e.g., "Demo Cluster").
# MAGIC - **Policy**: leave as “Unrestricted” for full configuration.
# MAGIC - **Multi-node** vs **Single-node**:
# MAGIC   - **Multi-node**: at least one worker; typical for distributed processing.
# MAGIC   - **Single-node**: no workers; driver runs the Spark jobs. Good for learning/testing and uses **fewer DBUs**.
# MAGIC
# MAGIC ### 3. Access Mode
# MAGIC - **Shared** – multiple users can use the cluster, but only **SQL and Python** workloads are supported.
# MAGIC - **Single user** – dedicated to one user; all languages (Scala, R, etc.) are supported.
# MAGIC - *Choose based on collaboration needs and language requirements.*
# MAGIC
# MAGIC ### 4. Databricks Runtime
# MAGIC - A **VM image** with pre-installed libraries (Spark, Scala, Java, Python, etc.).
# MAGIC - Always pick the **LTS (Long Term Support)** version for stability (e.g., 13.3 LTS).
# MAGIC - *Exam tip*: Use the version aligned with your course or certification.
# MAGIC
# MAGIC ### 5. Performance Options – Photon
# MAGIC - **Photon** is a vectorized query engine written in **C++** that accelerates Spark SQL queries.
# MAGIC - Enable it for better performance, especially on SQL and DataFrame operations (requires extra cost).
# MAGIC
# MAGIC ### 6. Worker & Driver Configuration
# MAGIC - **Worker type** – choose VM size based on memory, cores, and storage (defined by cloud provider, e.g., Azure VMs).
# MAGIC - **Driver type** – can be the **same as worker** or different (for large driver‑side operations).
# MAGIC - **Number of workers**:
# MAGIC   - Fixed: specify a constant number.
# MAGIC   - **Autoscaling**: set a min‑max range; Databricks resizes the cluster automatically based on workload.
# MAGIC
# MAGIC ### 7. Auto Termination
# MAGIC - Set an idle time (e.g., **30 minutes**).  
# MAGIC - If no commands are executed during that period, the cluster **terminates automatically** to save cost.
# MAGIC - **Always enable** it unless you have a continuous job.
# MAGIC
# MAGIC ### 8. Understanding DBUs (Cost)
# MAGIC - **DBU = Databricks Unit** – unit of processing capability per hour.
# MAGIC - Each VM size consumes a specific number of DBUs per hour.
# MAGIC - *Example*: Fewer/smaller workers → fewer DBUs → lower cost.
# MAGIC - DBU consumption is shown in the **configuration summary** on the right.
# MAGIC
# MAGIC ### 9. After Cluster is Running
# MAGIC - **Cluster list** (under Compute) shows status: running / terminated.
# MAGIC - Actions: Start, Terminate, Delete, Edit permissions.
# MAGIC - **Edit configuration**: Some changes may require a **restart**.
# MAGIC - Two useful logs:
# MAGIC   - **Event log**: lifecycle events (created, terminated, edited, etc.).
# MAGIC   - **Driver log**: output from notebooks and libraries running on the driver.
# MAGIC
# MAGIC ### 10. Interview Quick Recap
# MAGIC - A cluster = driver + workers. Driver orchestrates; workers execute.
# MAGIC - Use **single-node** for development/testing → lower cost.
# MAGIC - Enable **autoscaling** to handle varying loads efficiently.
# MAGIC - Always set **auto-termination** to avoid idle costs.
# MAGIC - Runtime = Spark + libraries; choose LTS.
# MAGIC - Photon is a C++ engine that speeds up queries (optional).
# MAGIC - DBU is the cost metric; you pay per DBU‑hour.
# MAGIC - Check **Event log** for troubleshooting cluster lifecycle issues.