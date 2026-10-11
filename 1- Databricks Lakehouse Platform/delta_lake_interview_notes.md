# Delta Lake & Databricks Tables — Interview Notes

**Based on the supplied course transcript.** Organized for beginner-to-interview revision. Examples illustrate the transcript's concepts; exact support can depend on Databricks runtime, catalog, and configuration.

## 1. What is Delta Lake?

Delta Lake is an **open-source storage framework/layer** that adds reliability to data lakes and helps enable lakehouse architectures. It is **not** itself a database service, data warehouse, storage medium, or separate file format. Delta tables store data in **Parquet** files and maintain a **transaction log** in `_delta_log`.

**Interview answer:** Delta Lake adds ACID transactions, versioning, reliable concurrent reads/writes, metadata tracking, time travel, and table optimization to data stored in a data lake.

## 2. Delta transaction log: the core mechanism

```text
employees/ 
  part-000...parquet
  part-001...parquet
  _delta_log/
    00000000000000000000.json
    00000000000000000001.json
    ...
```

The ordered log records committed table changes. Readers use the log to identify files belonging to a consistent table snapshot. Transaction actions include **add** (new active file) and **remove** (file no longer active). A file removed from the current snapshot may remain physically stored until cleanup.

**Illustration:**

1. Initial commit references Parquet files 1 and 2.
2. An update rewrites affected data into file 3 and records removal of file 1. Current snapshot references files 2 and 3.
3. While a writer prepares file 4, a reader sees the previous committed snapshot, not a partially written file.
4. If a write fails before commit, its incomplete/uncommitted files are not included in the table snapshot.

This supports **atomicity, consistency, isolation, and durability (ACID)**. The transcript explains updates using file rewrites; that is the mechanism demonstrated in the lesson.

**Interview question:** Why don't readers see dirty data? **Answer:** Readers follow committed transaction-log snapshots; uncommitted files are not part of the table's active file set.

## 3. Creating and inspecting Delta tables

The demo uses the legacy `hive_metastore` for simplicity, while acknowledging Unity Catalog as the newer default governance solution.

```sql
USE CATALOG hive_metastore;

CREATE TABLE employees (
  id INT,
  name STRING,
  salary DOUBLE
) USING DELTA;

INSERT INTO employees VALUES (1, 'Adam', 5000.0);
SELECT * FROM employees;

DESCRIBE DETAIL employees;
DESCRIBE HISTORY employees;
```

`USING DELTA` can be omitted where Delta is the configured default. `DESCRIBE DETAIL` shows metadata such as table location and number of active data files. `DESCRIBE HISTORY` shows committed operations and versions.

```sql
UPDATE employees
SET salary = salary + 100
WHERE name LIKE 'A%';
```

The demo inserts records in four transactions, creating four small files; an update replaces affected active files. **Files physically present** can outnumber **files in the current snapshot**.

## 4. Time travel and restore

Every committed operation produces a table version. Delta Lake can read historical snapshots while required historical files are retained.

```sql
SELECT * FROM employees VERSION AS OF 4;
SELECT * FROM employees TIMESTAMP AS OF '2026-01-01T00:00:00';

RESTORE TABLE employees TO VERSION AS OF 4;
-- Or restore to a timestamp:
RESTORE TABLE employees TO TIMESTAMP AS OF '2026-01-01T00:00:00';
```

The transcript also mentions a version shorthand such as `table_name@v4`. **Time travel reads an old snapshot; RESTORE changes the current table state and itself records a new transaction.** Historical reads require historical files to remain available.

## 5. OPTIMIZE, ZORDER, and VACUUM

### OPTIMIZE: small-file compaction

```sql
OPTIMIZE employees;
```

Many small files cause overhead. `OPTIMIZE` compacts files into a more efficient layout. Replaced files may remain physically present for time travel until cleanup.

### Z-order indexing: improve data skipping

```sql
OPTIMIZE employees ZORDER BY (id);
```

Z-ordering co-locates similar values to make file skipping more effective for queries filtering on selected columns. It does not create Hive-style partition folders. Benefit depends on data size and query patterns; the transcript notes little value on its tiny demo dataset.

### VACUUM: remove unneeded old files

```sql
VACUUM employees;
```

`VACUUM` physically removes eligible files that are no longer needed by the current table snapshot, subject to retention safeguards. The transcript gives **7 days** as the default retention threshold. Removing historical files can break time travel to older versions.

**Important:** The course disables a retention safety check for a zero-hour demonstration and explicitly warns **not to do this in production**.

| Command | Purpose | Key risk / note |
|---|---|---|
| `DESCRIBE HISTORY` | Inspect versions and operations | Does not itself restore data |
| `VERSION AS OF` | Read an older snapshot | Requires historical files |
| `RESTORE TABLE` | Make an old snapshot current | Creates a new transaction |
| `OPTIMIZE` | Compact/reorganize active data | Rewrites files |
| `ZORDER BY` | Improve data skipping | Requires layout optimization |
| `VACUUM` | Delete eligible unreferenced files | Can remove time-travel capability |

## 6. Data layout optimization: three approaches

| Feature | Hive-style partitioning | Z-order | Liquid clustering |
|---|---|---|---|
| Organization | Partition directories by column values | Co-locate related values in files | Cluster by defined keys |
| Define | `PARTITIONED BY` | `OPTIMIZE ... ZORDER BY` | `CLUSTER BY` |
| Best fit in lesson | Very large tables with suitable low-cardinality filters | High-cardinality filter columns | Flexible, incremental optimization |
| Caution | Too many partitions/small files; changing keys can require rewrite | Re-run optimization as data arrives | Not combined with partitioning/Z-order in the transcript |

**Partitioning example:**

```sql
CREATE TABLE sales (
  sale_id BIGINT,
  sale_year INT,
  amount DOUBLE
) USING DELTA
PARTITIONED BY (sale_year);
```

Filters on `sale_year` can skip unrelated directories. The lesson cautions against high-cardinality columns such as unique IDs and against partitioning small/medium tables.

**Liquid clustering example:**

```sql
CREATE TABLE events (
  event_id BIGINT,
  customer_id BIGINT,
  event_date DATE
) USING DELTA
CLUSTER BY (customer_id);

OPTIMIZE events;
```

The lesson describes liquid clustering as incremental: already well-clustered files can be left alone while newly ingested data is optimized. Clustering keys can be changed without immediately rewriting all existing data.

**Automatic liquid clustering:** `CLUSTER BY AUTO` lets Databricks select keys from observed query workloads. The lesson associates it with **Predictive Optimization on Unity Catalog managed tables**.

**Interview answer:** Prefer an appropriate file layout based on filter patterns, table size, cardinality, and maintenance cost; the course emphasizes liquid clustering as a flexible alternative to static partitioning and repeated Z-ordering.

## 7. Hive Metastore, schemas, and storage locations

The **Hive Metastore** stores metadata: schema/database definitions, tables, partitions, data format, and storage paths. In the course's legacy setup, `CREATE DATABASE` and `CREATE SCHEMA` are interchangeable.

```sql
CREATE DATABASE hr;
-- Equivalent in the lesson:
CREATE SCHEMA hr;

USE hr;
CREATE TABLE staff (id INT, name STRING) USING DELTA;
```

The legacy default warehouse directory shown is `/user/hive/warehouse`; a database directory may have a `.db` suffix. A database can also specify a custom storage `LOCATION` in the demonstrated Hive Metastore setup.

## 8. Managed vs external tables

| Managed table | External table |
|---|---|
| Default table-storage lifecycle is managed by the metastore/platform | Storage path explicitly specified via `LOCATION` |
| Dropping the table removes its managed data in the legacy demo | Dropping the table removes metadata but leaves data files in the legacy demo |
| Metadata and underlying data lifecycle managed together | Table metadata managed separately from underlying data |

```sql
CREATE TABLE managed_staff (id INT) USING DELTA;

CREATE TABLE external_staff (id INT)
USING DELTA
LOCATION '/mnt/demo/external_staff';

DESCRIBE EXTENDED external_staff;
DROP TABLE external_staff;
```

**Scope caution:** These storage-path and drop examples are from the transcript's **legacy Hive Metastore** walkthrough. Do not automatically assume identical storage paths or behavior for every Unity Catalog setup.

## 9. CTAS: Create Table As Select

CTAS creates and populates a table from query output; the schema is inferred from the SELECT result rather than manually declared in the CTAS statement.

```sql
CREATE TABLE employee_summary AS
SELECT id, name AS employee_name
FROM employees;
```

| Regular `CREATE TABLE` | CTAS |
|---|---|
| Explicit column schema in lesson | Schema inferred from query |
| Starts empty | Populated from SELECT |
| Insert later | Creation and insertion together |

The transcript also discusses comments, partitioning, and external `LOCATION` options when creating tables. It advises avoiding unnecessary partitioning for smaller tables.

## 10. Table constraints

The lesson covers **NOT NULL** and **CHECK** constraints. Existing rows must satisfy the rule before it is added; subsequent invalid writes fail.

```sql
CREATE TABLE orders (
  order_id INT NOT NULL,
  amount DOUBLE
) USING DELTA;

ALTER TABLE orders
ADD CONSTRAINT positive_amount CHECK (amount >= 0);
```

**Interview answer:** Constraints protect data quality by rejecting writes that violate declared rules.

## 11. Deep clone vs shallow clone

```sql
CREATE TABLE employees_backup DEEP CLONE employees;
CREATE TABLE employees_test SHALLOW CLONE employees;
```

| Deep clone | Shallow clone |
|---|---|
| Copies data and metadata | Copies metadata/log information and references existing data files |
| More data movement, potentially slower | Fast, with little/no initial data copying |
| Useful for independent backup/copy | Useful for quick development/testing copies |
| Can be rerun incrementally according to lesson | Depends on continued availability of referenced data files |

Changes made to either clone are tracked separately and do not directly modify the source table. **Key distinction:** a shallow clone is not an independent physical backup of the source data.

## 12. Views: stored, temporary, global temporary

A regular view is a **saved SQL query**, not a physical copy of the underlying table's data. Its query runs when the view is queried.

```sql
CREATE VIEW apple_phones AS
SELECT * FROM smartphones WHERE brand = 'Apple';

CREATE TEMP VIEW phone_brands AS
SELECT DISTINCT brand FROM smartphones;

CREATE GLOBAL TEMP VIEW recent_phones AS
SELECT * FROM smartphones WHERE release_year > 2020;

SELECT * FROM global_temp.recent_phones;
```

| Type | Lifetime | Visibility | Stored in |
|---|---|---|---|
| Stored/persistent view | Until explicitly dropped | Across sessions with access | Database/catalog metadata |
| Temporary view | Spark session | Current session | Session only |
| Global temporary view | Shared Spark application's lifetime (cluster context in lesson) | Multiple sessions on same cluster in lesson | `global_temp` namespace |

A temporary view is not available from a different Spark session. The lesson shows a global temporary view accessible from another notebook attached to the same running cluster; it disappears after the cluster/application restarts. `SHOW TABLES` and `SHOW TABLES IN global_temp` help inspect these objects.

## 13. Common interview scenarios

**Q1. A batch write crashes halfway. Will a reader see partial data?**  
No. The transcript's Delta-log model only exposes committed files in the active snapshot.

**Q2. Why are there six Parquet files on storage but `DESCRIBE DETAIL` reports four?**  
Some files were removed from the current snapshot by updates but still physically exist for historical access or cleanup.

**Q3. A developer accidentally deletes all table rows. What can you do?**  
Check `DESCRIBE HISTORY`, inspect a suitable historical version, and use `RESTORE TABLE ... TO VERSION AS OF ...` while its files remain available.

**Q4. A table has thousands of tiny files. What should you do?**  
Run `OPTIMIZE` to compact them; evaluate the query pattern before adding Z-order or clustering.

**Q5. Why shouldn't you immediately `VACUUM ... RETAIN 0 HOURS`?**  
Old files may still be required by readers or time travel; the course warns against bypassing the retention safety check in production.

**Q6. How do managed and external tables differ?**  
In the legacy Hive Metastore example, dropping managed tables deletes managed data, whereas dropping external tables leaves underlying files.

**Q7. How do CTAS and standard CREATE TABLE differ?**  
CTAS infers schema and loads data from a SELECT in one operation; regular CREATE TABLE in the lesson defines an empty schema first.

**Q8. How do deep and shallow clones differ?**  
Deep clone copies data; shallow clone initially reuses existing data files through references, making it faster but not an independent backup.

**Q9. Which view should I use to share a definition across sessions?**  
A stored view for persistent cross-session access; a global temporary view for sharing only within the relevant running Spark application/cluster context.

**Q10. Partitioning, Z-order, or liquid clustering?**  
Consider table size and filter patterns. The course favors liquid clustering for flexible incremental optimization and cautions that high-cardinality partitioning causes excessive partitions.

## 14. 90-second interview explanation

> Delta Lake is an open-source storage layer built on Parquet files plus a transaction log. The `_delta_log` tracks committed changes and active files, which supports ACID transactions and consistent reads even when writes fail or happen concurrently. Each committed change creates a version, so we can inspect history, query old versions using time travel, or restore a previous state while the required files remain. For performance, `OPTIMIZE` compacts small files, Z-order improves data skipping, and liquid clustering organizes data incrementally based on clustering keys. `VACUUM` removes eligible old files, but this can limit historical reads. Databricks tables can be managed or external; in the legacy Hive Metastore example, dropping a managed table removes its data, while dropping an external table preserves its files. CTAS creates and populates tables from SELECT queries, constraints enforce quality rules, and deep versus shallow clones trade physical independence for speed. Views can be persistent, session-scoped, or global temporary depending on their required lifetime.

## 15. Rapid revision checklist

- [ ] Delta Lake = Parquet data + `_delta_log`
- [ ] ACID transactions and consistent snapshots
- [ ] `add` and `remove` file actions
- [ ] `DESCRIBE DETAIL` vs `DESCRIBE HISTORY`
- [ ] Time travel by version or timestamp
- [ ] `RESTORE TABLE` creates a new transaction
- [ ] `OPTIMIZE` = file compaction
- [ ] ZORDER = co-location for skipping
- [ ] `VACUUM` = old-file cleanup; default 7-day safety threshold in lesson
- [ ] Partitioning drawbacks for small/high-cardinality tables
- [ ] Liquid clustering and `CLUSTER BY AUTO`
- [ ] Hive Metastore and custom `LOCATION`
- [ ] Managed vs external table drop behavior in legacy demo
- [ ] CTAS vs regular CREATE TABLE
- [ ] NOT NULL and CHECK constraints
- [ ] Deep vs shallow clone
- [ ] Stored vs temporary vs global temporary views

---

*Source boundary: This document follows the supplied transcript, including its legacy Hive Metastore demonstrations. SQL snippets are educational examples, not commands executed against a workspace.*
