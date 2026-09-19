# Databricks notebook source
# MAGIC %md
# MAGIC # Relational Entities on Databricks
# MAGIC
# MAGIC ### 1. What is a Database (Schema)?
# MAGIC - Think of a **database** as a **folder on your computer** that holds related tables.
# MAGIC - In Databricks, a “database” is the same as a “schema.”  
# MAGIC   - You can create it with `CREATE DATABASE my_db;` or `CREATE SCHEMA my_db;` – they do exactly the same thing.
# MAGIC
# MAGIC ### 2. Where Does Databricks Keep Track of All This?
# MAGIC - Databricks has a central “phone book” for data called the **Hive metastore**.
# MAGIC - This metastore stores:
# MAGIC   - The list of all databases.
# MAGIC   - The structure of every table (column names, data types).
# MAGIC   - **Where the actual data files are stored** (the path in cloud storage).
# MAGIC - Every time you use a table, Databricks looks up its location in this metastore first, then reads the files.
# MAGIC
# MAGIC ### 3. The Default Folder for Data
# MAGIC - If you create a table without telling Databricks exactly where to put the data, it automatically stores the files in a default location:  
# MAGIC   `/user/hive/warehouse/`
# MAGIC - Inside that folder:
# MAGIC   - A database called `my_db` gets a subfolder named `my_db.db`.
# MAGIC   - Each managed table inside `my_db` gets its own subfolder under that.
# MAGIC
# MAGIC **Example:**  
# MAGIC `/user/hive/warehouse/my_db.db/employees/` → contains the data files for the `employees` table.
# MAGIC
# MAGIC ### 4. Two Types of Tables – Managed vs External
# MAGIC This is the most important concept to understand.
# MAGIC
# MAGIC #### Managed Table (Default)
# MAGIC - You create it with just `CREATE TABLE table_name (...)` **without** specifying a custom location.
# MAGIC - Databricks (Hive) **owns both the table definition and the data files**.
# MAGIC - Everything is stored inside the database folder under `/user/hive/warehouse/`.
# MAGIC - **If you drop (delete) a managed table, the data files are also deleted automatically.**  
# MAGIC   (Just like deleting a folder on your computer – everything inside is gone.)
# MAGIC
# MAGIC #### External Table
# MAGIC - You create it by adding the `LOCATION` keyword:  
# MAGIC   `CREATE TABLE table_name (...) LOCATION '/some/custom/path';`
# MAGIC - Databricks (Hive) **only owns the table definition**, not the data files.
# MAGIC - The data files stay exactly at the custom path you gave, **outside** the default warehouse folder.
# MAGIC - **If you drop an external table, the data files are NOT deleted.**  
# MAGIC   Only the “pointer” (metadata) in the metastore is removed. The data remains untouched.
# MAGIC - Use this when:
# MAGIC   - You already have data in a specific cloud folder and want to query it without moving it.
# MAGIC   - You want to share data with other systems or keep it safe even if the table definition is deleted.
# MAGIC
# MAGIC ### 5. Using the `LOCATION` Keyword
# MAGIC - `LOCATION` simply tells Databricks **where to store (or find) the data files**.
# MAGIC - You can use it on:
# MAGIC   - A database: `CREATE DATABASE my_db LOCATION '/mnt/custom_db';`  
# MAGIC     Now all tables inside `my_db` (if you don’t specify their own location) will go under that custom folder.
# MAGIC   - A table: `CREATE TABLE my_table LOCATION '/mnt/my_special_data';`  
# MAGIC     This makes it an external table, storing data exactly there.
# MAGIC - Without `LOCATION`, Databricks uses the default managed location.
# MAGIC
# MAGIC ### 6. Simple Analogy
# MAGIC - The **Hive metastore** is like a **library catalog** that tells you where every book (table) is on the shelves.
# MAGIC - A **managed table** is like a book that the library bought and keeps on its own shelves. If the library removes the entry from the catalog, it also throws away the book.
# MAGIC - An **external table** is like a book you own but have lent to the library – the catalog just has a note saying “the book is at this external shelf.” If the library removes the note, your book stays safe at your shelf.
# MAGIC
# MAGIC ### 7. Common Commands (for reference)
# MAGIC | Action | Command |
# MAGIC |--------|---------|
# MAGIC | Create a database | `CREATE DATABASE my_db;` |
# MAGIC | Switch to that database | `USE my_db;` |
# MAGIC | Create a managed table | `CREATE TABLE students (id INT, name STRING);` |
# MAGIC | Create an external table | `CREATE TABLE students_ext (id INT, name STRING) LOCATION '/mnt/external_data';` |
# MAGIC | Show current database | `SELECT current_database();` |
# MAGIC | Drop a managed table (deletes data) | `DROP TABLE students;` |
# MAGIC | Drop an external table (keeps data) | `DROP TABLE students_ext;` |
# MAGIC
# MAGIC ### 8. Visual Summary
# MAGIC ```
# MAGIC Default warehouse folder: /user/hive/warehouse/
# MAGIC │
# MAGIC ├── default/               ← default database (no .db)
# MAGIC │   └── table1/            ← managed table data
# MAGIC │
# MAGIC ├── my_db.db/              ← my_db database folder
# MAGIC │   └── table2/            ← managed table data under my_db
# MAGIC │
# MAGIC External location: /mnt/external_data/
# MAGIC     └── (data files for external table, not inside warehouse)
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📌 Final Recap for a Beginner
# MAGIC - A **database** is just a container (folder) for tables.
# MAGIC - All definitions are kept in the Hive metastore.
# MAGIC - If you don’t say where to put data, it goes into the **managed warehouse folder** (`/user/hive/warehouse`). Deleting the table deletes the data.
# MAGIC - If you use `LOCATION`, you create an **external table**. Deleting the table **does not** delete the data – the files stay where they are.
# MAGIC - Think “managed = Databricks controls everything”, “external = you control the data location and lifetime”.