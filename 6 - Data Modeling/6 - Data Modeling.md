# Databricks Interview Notes: Bronze Ingestion, Streaming Deduplication & SCD Type 2

**Level:** Beginner to interview-ready  
**Based on:** Your latest lecture transcript, covering Singleplex/Multiplex Bronze ingestion, Auto Loader options, Bronze-to-Silver parsing, Delta CHECK constraints, streaming deduplication, and Slowly Changing Dimensions.  
**Approach:** Learn the *why*, then the *how*, then practise saying an interview answer.

> **About the examples:** The bookstore dataset and concepts come from your transcript. SQL/PySpark snippets below are **simplified illustrative examples created for learning**; table names, schemas, storage paths and exact source-event formats may need changes before they run in your workspace. Where helpful, notes flag implementation details that the narration doesn't specify.

---

## Your learning map

```text
                       SOURCES
          Kafka topics / JSON files in cloud storage
                           |
                     Auto Loader
                           |
                 BRONZE: raw_events
            topic | key | value | timestamp
                           |
          +----------------+----------------+
          |                |                |
       orders            books          customers
       topic             topic            topic
          |                |                |
    Parse JSON        Track changes     Parse/clean
    Validate          using SCD2        as needed
    Deduplicate            |                |
          |                |                |
     SILVER:          SILVER:           SILVER:
     orders_silver    books_silver      customers_silver
                           |
                   current_books view/table
                     (latest versions)
```

**Big idea:** Bronze tries to retain what arrived. Silver turns it into usable, trustworthy, business-specific data.

### What you should know by the end

- Explain why there may be **one Bronze table per source** or **one shared Bronze table for many topics**.
- Explain how Auto Loader incrementally ingests files, and why checkpointing matters.
- Extract one topic from a multiplex table and parse its JSON payload into Silver columns.
- Explain the difference between **rejecting a bad write with CHECK** and **filtering/quarantining a bad row**.
- Describe duplicates **within a streaming window** versus duplicates **already in the target table**.
- Explain what `withWatermark`, `dropDuplicates`, `foreachBatch` and `MERGE` each contribute.
- Clearly compare **SCD 0, SCD 1 and SCD 2**, including why business history is not the same as Delta Time Travel.

---

# PART 1 — Bronze Layer: Where ingestion begins

## 1. What is a Bronze table?

In the **Medallion Architecture**, the Bronze layer is the first landing area for source data.

Imagine a bookstore application sends messages when:

- a customer registers;
- a book's price changes;
- an order is placed.

Bronze stores those incoming events so the pipeline can later clean and process each kind of data separately.

A common Bronze principle is **preserve source details and history as faithfully as practical**. If you remove records too early, later troubleshooting or replay becomes difficult.

**Simple interview answer:**

> The Bronze layer stores ingested source records, usually with minimal transformations. It is useful for traceability, replay and building downstream Silver tables.

## 2. Singleplex vs Multiplex ingestion

The first decision is: **How many Bronze tables should we create for different sources?**

### 2.1 Singleplex = one source to one Bronze table

```text
Customers source  ---> bronze_customers
Books source      ---> bronze_books
Orders source     ---> bronze_orders
```

This is the familiar approach: each dataset gets a different raw table.

**Why it can be useful:** Different schemas and processing needs are easier to keep separate; your transcript says this generally works well in batch-processing scenarios.

**Possible challenge:** When there are very many independent streaming topics, running a separate ingestion job for every topic may increase operational overhead and run into workspace concurrency limits, as discussed in the lecture.

### 2.2 Multiplex = many sources to one Bronze table

```text
Customers topic ---+
Books topic --------+--> bronze_events
Orders topic -------+
```

Instead of creating separate Bronze ingestion streams for every topic, one Bronze table stores messages from different topics. A column such as `topic` identifies what type of event each record represents.

Illustrative Bronze rows:

| topic | key | value (JSON payload) |
|---|---|---|
| `customers` | `C101` | `{"customer_id":"C101","name":"Mia"}` |
| `orders` | `O700` | `{"order_id":"O700","quantity":2}` |
| `books` | `B13` | `{"book_id":"B13","price":42}` |

The common table stores **an event envelope** (topic, key, value, timestamp). The `value` field may contain different JSON schemas depending on the topic.

**Why multiplex can help:** It consolidates ingestion from many related streams into a common raw landing flow. The lecture discusses Kafka/pub-sub data and a demonstration where **JSON files in cloud storage simulate Kafka messages**.

### 2.3 How does Multiplex become Silver?

Each Silver pipeline filters the shared Bronze table by topic:

```sql
-- Illustration: select only raw order events
SELECT *
FROM bronze_events
WHERE topic = 'orders';
```

Then it parses the payload into columns such as `order_id`, `customer_id` and `quantity`.

```text
bronze_events
     |
     +--> WHERE topic='orders'    --> orders_silver
     +--> WHERE topic='books'     --> books_silver
     +--> WHERE topic='customers' --> customers_silver
```

### Singleplex vs Multiplex: fast comparison

| Question | Singleplex | Multiplex |
|---|---|---|
| Mapping | 1 source → 1 Bronze table | Many topics → 1 Bronze table |
| Raw tables | Separate per source | Common event table |
| Schema handling | Naturally separated | Topic-specific parsing downstream |
| Operational consideration | Many separate pipelines as topics grow | Consolidated ingestion |
| Example | `bronze_orders` | `bronze_events` with topic = `orders` |
| Main trade-off | More pipelines/tables | More downstream routing and schema handling |

**Interview question: Which would you choose?**

> I would choose Singleplex when sources are few and have clearly separate ingestion requirements. I would consider Multiplex when many topics share an event-ingestion pattern and consolidating ingestion helps with scale and operational complexity. Then I would split them by topic into separate Silver tables.

**Common mistake:** Multiplex **does not mean** order, book and customer JSON magically have the same schema. The shared raw table can keep the payload unparsed, and each Silver transformation supplies the correct schema.

---

# PART 2 — Ingesting multiplex events with Auto Loader

## 3. Why Auto Loader?

Suppose a cloud folder gains a new file every hour.

```text
Hour 1: file_001.json
Hour 2: file_002.json
Hour 3: file_003.json
```

You usually do **not** want to reread and reinsert all historical files every hour. **Auto Loader** supports incremental discovery and ingestion of newly arrived files.

The course uses a `cloudFiles` streaming source and writes into a multiplex Bronze Delta table.

### Core PySpark pattern

```python
from pyspark.sql import functions as F

input_path = "/Volumes/my_catalog/my_schema/raw_volume/kafka_json/"
schema_path = "/Volumes/my_catalog/my_schema/state_volume/bronze_schema/"
checkpoint_path = "/Volumes/my_catalog/my_schema/state_volume/bronze_checkpoint/"

raw_stream = (
    spark.readStream
         .format("cloudFiles")
         .option("cloudFiles.format", "json")
         .option("cloudFiles.schemaLocation", schema_path)
         .load(input_path)
)

query = (
    raw_stream.writeStream
              .format("delta")
              .option("checkpointLocation", checkpoint_path)
              .trigger(availableNow=True)
              .toTable("bronze_events")
)
```

**Read it in simple words:**

1. `readStream` creates a streaming source.
2. `format("cloudFiles")` tells Databricks to use Auto Loader.
3. `cloudFiles.format = json` tells it the input files contain JSON.
4. `schemaLocation` is where inferred/evolving source schema information is stored.
5. `load(input_path)` tells it which folder to watch.
6. `writeStream` persists data to a table.
7. `checkpointLocation` records streaming progress for recovery.
8. `availableNow=True` processes the currently available backlog and stops.

> **Important:** This code is a learning template. In practice, you may explicitly provide a schema, choose different schema-evolution settings and use separate locations for checkpoints and schema tracking.

## 4. What is `availableNow`?

The transcript calls the ingestion function to process an **incremental batch**.

```text
New files available
       ↓
Auto Loader processes them
       ↓
Data written to Bronze
       ↓
Query stops
```

With `availableNow`, a streaming query processes the data currently available **in one or more micro-batches** and stops when caught up. It is useful for a scheduled job that should not remain active forever.

**Interview answer:**

> `availableNow` is a Structured Streaming trigger. It processes all currently available incremental data and stops automatically. It gives incremental tracking with batch-like execution.

## 5. Why preserve Kafka fields such as topic, key and timestamp?

The transcript describes Kafka-style records containing `topic`, `key`, `value` (binary-encoded) and `timestamp`.

| Field | Meaning | Why useful? |
|---|---|---|
| `topic` | Event category/source topic | Route to the correct Silver pipeline |
| `key` | Message key | Help identify or group source events |
| `value` | Actual event payload | Parse into business columns |
| `timestamp` | Kafka event-record timestamp | Trace when the record entered the topic |
| `partition`, `offset` (when available) | Kafka source position | Trace or uniquely identify message positions |

The lecture converts the timestamp to a readable timestamp and extracts `year_month` for the demonstration's Bronze partitioning.

> **Design note:** The course partitions the demo Bronze table by `topic` and `year_month`. Treat that as a specific design choice, **not** a universal rule: partitioning needs to suit volume, cardinality, file sizes and query patterns.

## 6. Schema evolution: What if source columns change?

Imagine JSON events currently have:

```json
{"order_id": "O1", "quantity": 2}
```

Tomorrow some messages also contain `discount`:

```json
{"order_id": "O2", "quantity": 3, "discount": 10}
```

This is a **schema change**.

The transcript discusses schema evolution and mentions both Auto Loader evolution behavior and a merge-schema option during writing.

Keep the roles separate:

- **Auto Loader schema evolution:** How the **input stream** detects and handles new source columns; configured with `cloudFiles.schemaEvolutionMode` and schema tracking.
- **Delta write schema evolution:** Whether the **target Delta table** is allowed to add compatible incoming columns; a common write option is `.option("mergeSchema", "true")`.

These are related, but **they are not the same setting**.

The supplied transcript explains that for `addNewColumns` behavior, a newly encountered field can update stored schema metadata and cause a stream stop; a following run can use the updated schema. It also notes that defaults depend on whether a schema was explicitly provided.

**Interview answer:**

> Schema evolution means adapting when the shape of incoming data changes, for example when a new column arrives. I need to consider both the Auto Loader reader's schema behavior and whether the destination Delta table can accept the changed schema.

---

# PART 3 — Auto Loader reliability options

## 7. `cloudFiles.maxBytesPerTrigger`

**Problem:** One micro-batch may be too large and cause long processing time or memory pressure.

**Option introduced in the transcript:**

```python
.option("cloudFiles.maxBytesPerTrigger", "1g")
```

The lecture's intent is to control approximately how much input the streaming engine processes per micro-batch.

**Important distinction:** This is about **batch sizing**, not the total maximum amount of data that a pipeline can ever load.

**Interview question:** Why would you reduce bytes per trigger?

> To keep individual micro-batches manageable, improve stability and make execution time more predictable when ingesting large inputs.

## 8. Handling malformed/bad records

**Problem:** Incoming JSON/CSV may contain malformed content or values that do not fit the expected schema.

The transcript introduces a bad-records path:

```python
(
    spark.readStream
         .format("cloudFiles")
         .option("cloudFiles.format", "json")
         .option("badRecordsPath", "/path/to/quarantine")
         .schema("id INT, amount DOUBLE")
         .load("/path/to/input")
)
```

### What is a quarantine area?

A separate place to keep records that need investigation, rather than immediately mixing them into trusted Silver outputs.

```text
Incoming events
      ↓
Parse/validate
   ↙       ↘
Valid       Invalid
  ↓           ↓
Silver     Quarantine
```

**Be precise:** Not every missing field, schema mismatch or malformed value is necessarily handled in exactly the same way. Actual behavior depends on the reader, parser mode, schema and rescue-column settings. The transcript introduces `badRecordsPath` as one technique to isolate bad input.

## 9. Filter files using `pathGlobFilter`

Suppose you only want PNG files from a folder:

```python
(
    spark.readStream
         .format("cloudFiles")
         .option("cloudFiles.format", "binaryFile")
         .option("pathGlobFilter", "*.png")
         .load("/path/to/files")
)
```

**Meaning:** Select files matching the filename pattern. The lecture's example is `*.png`.

## 10. Important Auto Loader options: one table to remember

| Option | Purpose | Memory trick |
|---|---|---|
| `cloudFiles.format` | Source file type | *What am I reading?* |
| `cloudFiles.schemaLocation` | Persist schema information | *What columns exist?* |
| `checkpointLocation` | Track stream progress | *What did I already process?* |
| `cloudFiles.maxBytesPerTrigger` | Control batch input size | *How much per batch?* |
| `badRecordsPath` | Store certain malformed-record details | *Where do bad parses go?* |
| `pathGlobFilter` | Filter by file pattern | *Which filenames count?* |
| `cloudFiles.schemaEvolutionMode` | Control changing source schema | *What if a column appears?* |
| `mergeSchema` (Delta writer) | Allow certain target schema changes | *Can target columns grow?* |

**Common interview trap:** **Checkpoint location** and **schema location** serve different purposes. Progress tracking is not schema tracking.

---

# PART 4 — Moving one topic from Bronze to Silver

## 11. Why do we split the topics in Silver?

Bronze stores a common event envelope. Business users, however, want understandable tables:

```text
orders_silver
order_id | customer_id | quantity | order_timestamp
```

So for orders:

1. Read Bronze as a stream.
2. Keep `topic = 'orders'`.
3. Convert Kafka-style binary `value` to a string.
4. Parse the JSON using an expected schema.
5. Validate/clean the result.
6. Write to the Orders Silver Delta table.

## 12. Parsing JSON with `from_json()`

Suppose the raw payload is a string:

```json
{"order_id":"O100","customer_id":"C1","quantity":2}
```

Spark initially sees **one text value**, not automatically three business columns.

`from_json()` converts JSON text into a **struct** whose fields can be accessed as columns.

### PySpark example

```python
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

order_schema = StructType([
    StructField("order_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("quantity", IntegerType()),
])

bronze_stream = spark.readStream.table("bronze_events")

orders_parsed = (
    bronze_stream
    .filter(F.col("topic") == "orders")
    .withColumn("payload_text", F.col("value").cast("string"))
    .withColumn("data", F.from_json(F.col("payload_text"), order_schema))
    .select(
        F.col("data.order_id").alias("order_id"),
        F.col("data.customer_id").alias("customer_id"),
        F.col("data.quantity").alias("quantity"),
    )
)
```

**Explain each important line:**

- `.filter(topic == "orders")`: ignore book/customer events.
- `.cast("string")`: turn the payload into text for JSON parsing.
- `F.from_json(..., order_schema)`: interpret that text according to the expected structure.
- `data.order_id`: take a field from the parsed struct.
- `.select(...)`: keep meaningful Silver columns.

### Equivalent SQL concept

```sql
SELECT
  parsed.order_id,
  parsed.customer_id,
  parsed.quantity
FROM (
  SELECT from_json(
           CAST(value AS STRING),
           'order_id STRING, customer_id STRING, quantity INT'
         ) AS parsed
  FROM bronze_events
  WHERE topic = 'orders'
);
```

**Important:** The course also demonstrates using a streaming temporary view to switch between SQL and PySpark. A normal batch SQL query over a table and a streaming DataFrame are **not automatically the same thing**.

## 13. Why use a temporary view in a mixed SQL/Python pipeline?

The lecture follows this workflow:

```text
PySpark readStream
      ↓
Streaming temporary view
      ↓
SQL transformations
      ↓
Temporary view holding transformed query
      ↓
PySpark DataFrame
      ↓
writeStream
```

A temporary view is a convenient **bridge** between SQL logic and PySpark streaming code.

**Interview answer:**

> If my team prefers SQL transformations but I need PySpark to manage Structured Streaming, I can expose a streaming DataFrame as a temporary view, express transformations in SQL and bring the result back to a DataFrame for `writeStream`.

## 14. Why is `.writeStream` required here?

Reading/parsing a stream only defines the transformation. To **persist** its results, the transcript uses streaming writes.

```python
(
    orders_parsed.writeStream
                 .format("delta")
                 .option("checkpointLocation", "/path/to/orders_silver_checkpoint")
                 .trigger(availableNow=True)
                 .toTable("orders_silver")
)
```

This is a **basic append-style write example**; the next sections improve the design for invalid rows and duplicates.

---

# PART 5 — Delta CHECK constraints vs cleaning data

## 15. What is a CHECK constraint?

A Delta **CHECK constraint** defines a rule that values must satisfy when rows are written.

Example business rule:

> An order quantity must be greater than zero.

```sql
ALTER TABLE orders_silver
ADD CONSTRAINT positive_quantity
CHECK (quantity > 0);
```

Another course example validates timestamps against an allowed range.

## 16. What happens when invalid data is inserted?

Suppose a write contains:

| order_id | quantity |
|---|---:|
| O1 | 3 |
| O2 | 0 |

If `CHECK (quantity > 0)` is enforced, the write containing this invalid row **fails**.

Because Delta transactions are atomic, the transcript demonstrates that **the entire write transaction fails**, not just the bad row.

```text
Batch: [valid row, invalid row]
             ↓
        CHECK violation
             ↓
       Entire write fails
```

**Remember: ACID Atomicity = all or nothing for the transaction.**

## 17. Can I add a CHECK constraint to dirty existing data?

The course tries adding a positive-quantity constraint after bad rows already exist. It fails because those existing rows violate the condition.

Therefore, before adding the constraint:

- inspect existing data;
- correct/delete/quarantine violations;
- then add the constraint.

### View/drop constraints

```sql
-- Inspect metadata/properties
DESCRIBE EXTENDED orders_silver;

-- Remove a previously added constraint
ALTER TABLE orders_silver DROP CONSTRAINT positive_quantity;
```

## 18. CHECK vs filter vs quarantine

This is a **very important practical distinction**.

| Technique | Behavior on bad rows | Best when |
|---|---|---|
| CHECK constraint | Rejects invalid write transaction | Data integrity must never be violated |
| Filter before writing | Drops unwanted rows from the output | Simple known cleaning rule; pipeline should continue |
| Quarantine table | Stores invalid rows separately | You need investigation/reprocessing/auditability |

For example:

```python
valid_orders = orders_parsed.filter(F.col("quantity") > 0)
```

But if you only filter and discard, you may lose the record. For important source issues, a separate quarantine stream/table often makes diagnosis easier.

**Interview answer:**

> CHECK constraints enforce table integrity. They are strict: if a batch contains violating data, the Delta write can fail atomically. If business requirements say the pipeline should keep running, I would validate earlier, send invalid rows to quarantine or filter them out, and write only valid records to Silver.

**Common mistake:** A CHECK constraint **does not automatically move bad records into quarantine**.

---

# PART 6 — Deduplication in Structured Streaming

## 19. Why can events be duplicated?

The transcript says source systems such as Kafka can introduce duplicate records.

Example:

```text
Event 1: order O10, quantity 2
Event 2: order O11, quantity 1
Event 3: order O10, quantity 2  <-- duplicate
```

If both copies reach the Silver order table, totals and counts can be wrong.

## 20. Why deduplicate in Silver rather than Bronze?

The lecture deliberately preserves duplicates in Bronze and cleans them in Silver.

**Why?**

- Bronze keeps the source history closer to its original state.
- You may later need to identify and investigate upstream duplicates.
- Aggressive transformations during initial ingestion can increase latency or accidentally remove data.

```text
Bronze: original received events, duplicates included
          ↓
Silver: deduplicated business rows
```

**Interview answer:**

> I normally keep the raw source events in Bronze and implement business deduplication in Silver. That preserves traceability and allows the Silver logic to define correctly what counts as a duplicate.

## 21. Batch `dropDuplicates()`

For static data, the course demonstrates:

```python
batch_df = spark.table("bronze_events")
unique_df = batch_df.dropDuplicates()
```

That removes **identical rows**.

If business uniqueness is based on an `order_id`, you can specify the key:

```python
unique_orders = orders_df.dropDuplicates(["order_id"])
```

**Very important nuance:** Exact-row equality and business-key uniqueness are different. If two events have the same order ID but different payloads, the correct rule depends on the business meaning (duplicate versus genuine update).

## 22. Streaming `dropDuplicates()` and state

In a stream, duplicates can arrive in **different micro-batches**.

```text
Micro-batch 1 → O100
Micro-batch 2 → O101
Micro-batch 3 → O100 again
```

Spark must remember previously encountered keys. That memory is known as **streaming state**.

```python
stream_without_duplicates = stream_df.dropDuplicates(["order_id"])
```

**Challenge:** If Spark remembers all keys forever, the state can keep growing as the stream grows.

## 23. What is watermarking?

A **watermark** is a streaming mechanism used to limit how much event-time-related state must be retained, based on expected lateness.

For example, the transcript demonstrates a 30-second watermark.

```python
with_watermark = (
    stream_df
    .withWatermark("event_time", "30 seconds")
    .dropDuplicates(["order_id", "event_time"])
)
```

**Read this simply:**

- `event_time` comes from the event itself.
- `30 seconds` represents an allowed lateness rule in the example.
- Spark can eventually remove old state based on watermark progress, instead of retaining it indefinitely.

### Mental model

```text
On-time event            → process
Reasonably delayed event → may still be processed
Too-late event           → may be dropped / no longer deduplicated as before
```

> **Important nuance:** A watermark is **not a guarantee that no duplicate can ever appear forever**. Once old state is removed, late arrivals and target-level duplicates require careful design. The transcript therefore adds another protection at the destination table using `MERGE`.

**Interview answer:**

> Streaming deduplication needs state because duplicates can arrive in later batches. A watermark limits state retention based on event time and expected lateness, which controls memory usage. For target-level guarantees I may combine it with an insert-only MERGE.

## 24. What is insert-only `MERGE`?

Suppose Silver already contains `O100`.

Next batch has:

```text
O100
O101
```

Desired result:

```text
O100 → skip, already exists
O101 → insert, new order
```

This is an **insert-only MERGE**:

```sql
MERGE INTO orders_silver AS target
USING orders_microbatch AS source
ON target.order_id = source.order_id
WHEN NOT MATCHED THEN
  INSERT (order_id, customer_id, quantity)
  VALUES (source.order_id, source.customer_id, source.quantity);
```

Because there is **no `WHEN MATCHED` update**, existing keys remain unchanged.

**What does idempotent mean here?**

If you rerun the same batch, the target should not gain duplicate orders for keys that already exist (assuming keys are unique as intended and the batch/source is prepared correctly).

## 25. Why `foreachBatch`?

`foreachBatch` lets you run a custom function for every streaming micro-batch.

The transcript uses it because the destination write is not a simple append; it needs **SQL MERGE logic**.

```text
Streaming DataFrame
       ↓
foreachBatch
       ↓
Micro-batch DataFrame
       ↓
Create a temporary view
       ↓
MERGE INTO orders_silver
```

### Illustrative PySpark pattern

```python
def merge_orders(microbatch_df, batch_id):
    # Assumes target table exists and source contains columns below.
    clean = microbatch_df.dropDuplicates(["order_id"])
    clean.createOrReplaceTempView("orders_microbatch")

    microbatch_df.sparkSession.sql("""
        MERGE INTO orders_silver AS t
        USING orders_microbatch AS s
        ON t.order_id = s.order_id
        WHEN NOT MATCHED THEN INSERT
          (order_id, customer_id, quantity)
        VALUES
          (s.order_id, s.customer_id, s.quantity)
    """)

query = (
    valid_orders.writeStream
                .foreachBatch(merge_orders)
                .option("checkpointLocation", "/path/to/orders_merge_checkpoint")
                .trigger(availableNow=True)
                .start()
)

query.awaitTermination()
```

**Understand it line by line:**

1. Spark hands one micro-batch to `merge_orders`.
2. The function performs key-based deduplication **inside this batch**.
3. It registers the batch as a temporary view.
4. SQL `MERGE` compares incoming keys against the **existing Silver table**.
5. Only previously missing keys are inserted.
6. Checkpointing tracks progress across runs.

> **Assumptions:** `orders_silver` must exist; `order_id` must be the correct business key; streaming input needs proper event-time handling if you rely on watermarks; and the target should not already violate the intended uniqueness rule. This is an instructional pattern, not a universal production template.

### Do you need both `dropDuplicates` and MERGE?

They address different scopes:

| Mechanism | Scope |
|---|---|
| Batch `dropDuplicates()` | Duplicates in one batch |
| Stateful streaming dedup + watermark | Duplicates arriving across micro-batches within retained state |
| Insert-only `MERGE` | Prevent inserting keys already in target Delta table |

A complete design chooses the right combination based on business key, event timing and how the source behaves.

### Frequent interview trap

`foreachBatch` is **custom batch logic**, not an automatic deduplication feature. The **logic inside** it (e.g. `MERGE`) performs the idempotent write.

---

# PART 7 — Slowly Changing Dimensions (SCD)

## 26. What is a Slowly Changing Dimension?

**SCD = Slowly Changing Dimension.**

The lecture uses this term for a design approach that determines how a table responds when an attribute changes over time.

Example: Book `B13` costs ₹35 today but ₹42 later.

How should a table store that change?

- Keep original value unchanged?
- Overwrite it with ₹42?
- Keep **both** versions with dates?

These are different SCD patterns.

## 27. SCD Type 0 — Do not change existing information

**Rule:** Historical attribute values are not updated.

Example: a static reference/lookup attribute that must not be changed after creation.

```text
book_id | original_category
B13     | Fiction
```

A later change is not applied to that field under Type 0 rules.

**Remember:** **0 = no updates to tracked attributes.**

## 28. SCD Type 1 — Overwrite the previous value

**Rule:** Keep only the current business value, overwrite the old one.

Before:

| book_id | price |
|---|---:|
| B13 | 35 |

After:

| book_id | price |
|---|---:|
| B13 | 42 |

The business table no longer represents the previous ₹35 value as a separate historical record.

**Use when:** Reporting only needs the latest value, or a correction should replace an incorrect earlier value.

**Remember:** **1 = one current version.**

## 29. SCD Type 2 — Keep a history of changes

**Rule:** When a tracked attribute changes, mark the old record inactive and **insert a new record** for the new value.

The transcript adds three fields:

- `current` (or `is_current`) — is this the active version?
- `effective_date` / `start_date` — when this version starts.
- `end_date` — when this version stops being valid.

Example:

| book_id | price | is_current | start_date | end_date |
|---|---:|---|---|---|
| B13 | 35 | false | 2026-01-01 | 2026-02-10 |
| B13 | 39 | false | 2026-02-10 | 2026-03-01 |
| B13 | 42 | true | 2026-03-01 | NULL |

This is a **conceptual half-open date-range model**: a version is valid from its start date **up to but not including** its end date.

Now you can answer:

> What price was B13 selling for on February 15?

**Answer:** ₹39.

### Query the current version

```sql
SELECT book_id, price
FROM books_silver
WHERE is_current = true;
```

### Query the value that was valid on a date

```sql
SELECT book_id, price
FROM books_silver
WHERE book_id = 'B13'
  AND start_date <= DATE '2026-02-15'
  AND (
       end_date > DATE '2026-02-15'
       OR end_date IS NULL
  );
```

**Why this matters:** Business users may need historical price, address or department values to report **what was true at that time**, not just what is true now.

## 30. SCD 0 vs 1 vs 2 — must-know comparison

| SCD Type | Change handling | Retains previous business values? | Typical example |
|---|---|---|---|
| Type 0 | Do not change tracked value | Original remains | Static lookup |
| Type 1 | Overwrite old value | No separate business history | Latest profile correction |
| Type 2 | Close old record + insert new record | Yes | Price/address history |

**Memory trick:**

```text
0 = No change
1 = Replace the old
2 = Keep both versions (historical + current)
```

---

# PART 8 — Building SCD Type 2 with MERGE

## 31. Why is SCD Type 2 harder than a normal upsert?

A normal upsert might do:

```text
Match existing key → UPDATE it
No matching key   → INSERT it
```

But SCD Type 2 needs **two actions for a changed key**:

1. **Expire the previous active record.**
2. **Insert the new active record.**

Example:

```text
Incoming: B13 price changed to 42

Old version: B13, 39, current=true
      ↓
Expire old:  B13, 39, current=false, end_date=t
Insert new:  B13, 42, current=true,  start_date=t
```

## 32. The transcript's staged-MERGE trick

In the lecture, a MERGE source is prepared using a **custom merge key**.

For an updated existing book, the same change is staged in two ways:

- One source row with `merge_key = book_id` → matches and **expires the old active version**.
- One source row with `merge_key = NULL` → cannot match by equality and therefore **inserts the new version**.

### Mini example

Current target:

| book_id | price | is_current |
|---|---:|---|
| B13 | 39 | true |

Staged source for the change:

| merge_key | book_id | new_price |
|---|---|---:|
| B13 | B13 | 42 |
| NULL | B13 | 42 |

Conceptual merge:

```sql
MERGE INTO books_silver AS target
USING staged_changes AS source
ON target.book_id = source.merge_key
   AND target.is_current = true
WHEN MATCHED THEN
  UPDATE SET
    target.is_current = false,
    target.end_date = source.effective_date
WHEN NOT MATCHED THEN
  INSERT (book_id, price, is_current, start_date, end_date)
  VALUES (source.book_id, source.new_price,
          true, source.effective_date, NULL);
```

**What happens?**

```text
B13 matched row  → expires the old version
NULL keyed row   → inserts a new current version
```

> **Implementation caveat:** That is a **simplified conceptual** MERGE based on the transcript. Real SCD2 processing must distinguish **brand-new keys** from **changed existing keys**, detect changes versus unchanged events, avoid multiple conflicting changes for the same key in a batch, handle event ordering and make reprocessing safe. The source transcript doesn't provide all those details, so the SQL above is for understanding the central trick, not a drop-in production job.

## 33. Why `foreachBatch` appears again

The course applies SCD2 merge logic separately for each new streaming micro-batch:

```text
Multiplex Bronze
       ↓ filter topic='books'
Parse book payload
       ↓
foreachBatch
       ↓
Prepare staged updates
       ↓
MERGE into books_silver
       ↓
Old row closed + new row inserted
```

As with order deduplication, the **target Delta table must exist** before SQL MERGE attempts to write into it.

## 34. The `current_books` table

The transcript also creates a simpler dataset for consumers who **only want the latest book information**.

```sql
CREATE OR REPLACE TABLE current_books AS
SELECT book_id, title, price
FROM books_silver
WHERE is_current = true;
```

**Meaning:**

- `books_silver` preserves SCD Type 2 history.
- `current_books` shows only the latest version of every book.
- `CREATE OR REPLACE TABLE` rebuilds/replaces the contents when run.

Conceptually:

```text
books_silver (history)
  B13 ₹35 old
  B13 ₹39 old
  B13 ₹42 current
         ↓
current_books (latest only)
  B13 ₹42
```

**Interview answer:**

> I can maintain a historical SCD2 Silver table and create a separate current-state table or view filtered to `is_current = true`. This allows history-based auditing without making everyday users query historical versions unnecessarily.

---

# PART 9 — SCD Type 2 vs Delta Time Travel

## 35. Why not just use Delta Time Travel?

This is one of the most valuable questions in the transcript.

**Delta Time Travel** queries an earlier **physical version of a Delta table**.

**SCD Type 2** stores **business-valid versions** of individual records inside the table.

These are different use cases.

### Example

A book price changes:

```text
2026-01-01 → ₹35
2026-02-10 → ₹39
2026-03-01 → ₹42
```

With SCD2, all three business versions can coexist in the current table, each with effective dates.

Delta Time Travel instead asks for the state of the entire Delta table at an earlier transaction version or timestamp.

### Comparison

| Question | SCD Type 2 | Delta Time Travel |
|---|---|---|
| Purpose | Business attribute history | Historical Delta table snapshot |
| Storage approach | Multiple effective-dated rows | Historical Delta files/log versions |
| Typical query | Price valid when order was placed | Table as it existed at version 15 |
| Retention | Managed by business table design | Constrained by Delta file/log retention |
| Effect of VACUUM | Business rows are still part of current SCD2 table | Older snapshots may become unreadable after needed old files are removed |
| Best for | Long-term historical business reporting | Debugging, recovery and snapshot inspection |

**Interview answer:**

> Delta Time Travel gives access to older snapshots of a table, but those snapshots depend on retention of historical files. SCD Type 2 represents business changes as rows with effective dates, making it more appropriate when reports need long-term history of what attributes were valid at a particular time.

**Common mistake:** Do not say that Time Travel and SCD2 are interchangeable simply because both involve history.

---

# PART 10 — Bring the complete project together

## 36. How this bookstore pipeline works, step by step

**Step 1 — Sources:** Bookstore emits customer, book and order events. The course simulates Kafka-formatted messages using JSON files in cloud storage.

**Step 2 — Auto Loader:** It discovers new files incrementally, processes them with `availableNow`, and writes them to a shared multiplex Bronze table.

**Step 3 — Bronze:** Each record keeps a topic/key/value/timestamp-style envelope. Multiple topics coexist in one raw table.

**Step 4 — Orders Silver:** Filter `topic = 'orders'`, cast and parse the JSON, reject/filter/quarantine invalid rows as the design requires, and deduplicate events.

**Step 5 — Reliable order writes:** Apply insert-only `MERGE` per micro-batch using `foreachBatch` to avoid inserting order IDs already in the destination.

**Step 6 — Books Silver:** Filter `topic = 'books'`, parse price updates, and implement SCD Type 2 so previous prices remain accessible with validity dates.

**Step 7 — Current Books:** Build a current-only representation for users who only need the latest price.

### Simplified diagram

```text
Kafka-style JSON files in cloud storage
                |
            Auto Loader
                |
         bronze_events
                |
       +--------+---------+
       |                  |
    orders topic       books topic
       |                  |
   from_json           from_json
       |                  |
    Filter bad          SCD2 change
    quantities         detection
       |                  |
    Deduplicate       foreachBatch
       |                  |
    foreachBatch      SCD2 MERGE
    insert MERGE           |
       |               books_silver
   orders_silver           |
                       current_books
```

## 37. What did the course actually demonstrate?

- **Multiplex Bronze:** Auto Loader ingests different event topics into one common table, using `availableNow` to process newly arrived JSON files.
- **Orders Silver:** The order topic is filtered and the JSON payload parsed; the lecture demonstrates using SQL temporary views and a PySpark equivalent.
- **CHECK constraints:** An invalid timestamp causes an atomic write failure; attempting to add a quantity constraint to already-dirty data also fails.
- **Deduplication:** Batch and streaming deduplication are explored; watermarking controls streaming state, and insert-only MERGE with `foreachBatch` protects the Silver target from existing keys.
- **SCD2 Books:** A staged MERGE expires a book's previous active price and inserts its new price as a separate active row. A current-only table is then built.

---

# PART 11 — Interview questions and answers

## Core conceptual questions

### Q1. What is Singleplex ingestion?

**Answer:** It is a one-to-one ingestion pattern where each source/topic gets its own Bronze table.

### Q2. What is Multiplex ingestion?

**Answer:** It is a many-to-one ingestion pattern where data from many topics is stored in one common Bronze table. Downstream processing uses the topic field to create separate Silver datasets.

### Q3. Why use Multiplex?

**Answer:** It can reduce the number of independent ingestion flows for many topics. This is useful when large numbers of individual streaming jobs create operational or concurrency pressure.

### Q4. Why keep a `topic` column?

**Answer:** It tells the Silver processing logic which event type each record belongs to, so orders, books and customers can be parsed separately.

### Q5. What does Auto Loader do?

**Answer:** It incrementally discovers and ingests newly arriving cloud files into a streaming pipeline.

### Q6. What is `availableNow`?

**Answer:** A streaming trigger that processes all currently available data and stops when caught up.

### Q7. What is the difference between `schemaLocation` and `checkpointLocation`?

**Answer:** Schema location tracks the inferred/evolved structure of the input. Checkpoint location tracks streaming processing progress and recovery state.

### Q8. What is schema evolution?

**Answer:** Handling a change in incoming schema, such as a new field, with configured reader and target-table behavior.

### Q9. Why cast a Kafka `value` column as STRING?

**Answer:** Kafka payload fields can be binary-encoded. Casting them to strings makes a JSON payload available to functions such as `from_json()` when the bytes contain appropriately encoded JSON text.

### Q10. Why use `from_json()`?

**Answer:** It parses JSON text into a typed Spark struct so the business fields can be accessed separately.

### Q11. Why create a streaming temporary view?

**Answer:** To express Structured Streaming transformations in Spark SQL and then use the resulting view in PySpark for writing.

### Q12. What is a Delta CHECK constraint?

**Answer:** A rule on a Delta table that prevents write transactions from introducing records that violate its condition.

### Q13. What if one row violates a CHECK constraint?

**Answer:** The affected Delta write transaction fails atomically, so good rows in that same transaction are not partially committed.

### Q14. Why not always use CHECK for bad streaming records?

**Answer:** Strict constraints can fail the whole micro-batch. If the business wants the flow to continue, filter or quarantine invalid records before writing.

### Q15. Why deduplicate in Silver rather than Bronze?

**Answer:** Bronze preserves what the source sent. Silver enforces business uniqueness without destroying the raw history used for audits and reprocessing.

### Q16. Why does streaming deduplication need state?

**Answer:** A duplicate could arrive in a later micro-batch, so Spark needs to remember previously encountered keys.

### Q17. What is watermarking?

**Answer:** A way to control event-time state retention using an expected lateness boundary. It helps prevent streaming state from growing without limit.

### Q18. Does watermarking guarantee permanent uniqueness?

**Answer:** Not by itself. Very late events and state cleanup can affect how duplicates are handled. Target-level logic such as MERGE may also be needed.

### Q19. What is insert-only MERGE?

**Answer:** A MERGE that compares incoming records with target keys and only inserts records that have no match.

### Q20. What does `foreachBatch` do?

**Answer:** It runs custom logic on each Structured Streaming micro-batch, such as a Delta MERGE.

## SCD questions

### Q21. What is SCD Type 0?

**Answer:** Tracked values are not updated; the original value remains.

### Q22. What is SCD Type 1?

**Answer:** New values overwrite old values, so the business table keeps only the latest version.

### Q23. What is SCD Type 2?

**Answer:** New values create new rows, while old rows are marked inactive and given end dates. It preserves business history.

### Q24. Which fields help implement SCD2?

**Answer:** A current flag, a start/effective date and an end date. The implementation may also use additional surrogate keys or event sequencing.

### Q25. How is SCD Type 2 implemented with MERGE in this course?

**Answer:** The changed record is staged to expire the currently active version and also insert a separate new version, using a matched business key and a NULL merge key for the insert path.

### Q26. Why create `current_books`?

**Answer:** It gives downstream users a simple latest-state dataset while the underlying SCD2 table preserves full business history.

### Q27. How is SCD2 different from Time Travel?

**Answer:** SCD2 records business-valid history in rows, while Time Travel accesses previous snapshots of a Delta table subject to retention of historic data files.

## Scenario questions

### Q28. You receive 100 source topics. What Bronze approach might help?

**Answer:** I would evaluate Multiplex ingestion if many topics share the same transport/envelope and separate jobs become costly or hit concurrency limits. Silver pipelines can then split by topic.

### Q29. Every few minutes, new files arrive. Should you reread the entire directory?

**Answer:** Usually no. I would use incremental discovery/ingestion such as Auto Loader, together with checkpointing.

### Q30. A file contains 99 valid orders and 1 invalid quantity. What happens with CHECK?

**Answer:** If all 100 rows are in one write transaction and one violates the constraint, that transaction fails. If availability matters, I would separate valid and invalid records before writing.

### Q31. The same order ID arrives in two batches. How do you avoid two Silver orders?

**Answer:** I would choose an appropriate business key, deduplicate incoming records and use an insert-only MERGE for each micro-batch so keys already present in Silver are skipped.

### Q32. A book's price changes three times. How do you report the price at the date of an order?

**Answer:** Use SCD Type 2, storing each price's effective and end dates, then join the order date to the version valid at that time.

---

# PART 12 — Interview-ready explanations you can practise speaking

## 30 seconds: Singleplex vs Multiplex

> Singleplex maps each source dataset to its own Bronze table. Multiplex combines data from multiple topics into one Bronze table, usually with topic and payload fields. In Multiplex, the Silver pipelines filter by topic and parse each event format separately. I would consider Multiplex when dealing with many related streaming topics and want to consolidate ingestion.

## 45 seconds: Reliable Bronze-to-Silver processing

> I would use Auto Loader to incrementally ingest new files into a raw Bronze Delta table with a checkpoint. If Bronze is multiplexed, I would filter each topic to create separate Silver transformations. I would parse the JSON payload using an explicit schema, validate important fields and quarantine or filter bad records when the pipeline must continue. For the final Silver write I would use the appropriate logic, such as MERGE when the business key must be unique.

## 45 seconds: Deduplication

> In streaming data, duplicates can arrive in the same micro-batch or across different batches. I usually preserve the original source records in Bronze and deduplicate in Silver. Spark's dropDuplicates can remove duplicate events, while watermarking helps limit state based on expected lateness. To prevent rows that already exist in the Silver Delta table from being inserted again, I can use an insert-only MERGE through foreachBatch.

## 60 seconds: SCD Type 2

> SCD Type 2 is used when we need the full history of changes to attributes such as product price. Instead of overwriting the old record, we mark it inactive by setting its end date and then insert a new active record with the updated value and start date. In the transcript's Databricks example, this is implemented with MERGE inside foreachBatch. We can then create a separate current-only table for normal reporting, but the historical SCD2 table lets us answer questions such as which book price was valid when a past order occurred.

---

# PART 13 — Common mistakes to avoid

1. **Assuming a multiplex table has one business schema.** It often stores a common envelope and different topic-specific payloads.
2. **Throwing away duplicates in Bronze without considering replay/audit requirements.** This can hide source issues.
3. **Confusing `availableNow` with a forever-running stream.** It stops after processing currently available data.
4. **Using the same checkpoint for unrelated streaming writes.** Each streaming query needs appropriate independent progress state.
5. **Treating `mergeSchema` as an exact synonym for Auto Loader schema evolution.** Reader and writer behaviors differ.
6. **Assuming `from_json` guesses every business type correctly.** A valid schema is important.
7. **Expecting CHECK to ignore only the bad row.** A violating Delta transaction fails as a whole.
8. **Using `dropDuplicates()` with the wrong keys.** Entire-row equality and business uniqueness are not equivalent.
9. **Assuming a watermark guarantees no duplicate forever.** State and late events require careful design.
10. **Assuming `foreachBatch` performs deduplication automatically.** The MERGE logic inside it performs that work.
11. **Using a naive SCD2 MERGE on multiple unordered changes to the same key.** You must define event ordering, change detection and replay behavior.
12. **Calling Time Travel a substitute for long-term SCD2 business history.** They answer different questions.
13. **Treating the lecture's partition choice as a universal best practice.** Optimize partitioning for actual workloads.

---

# PART 14 — Ten-minute quick revision

## Five diagrams to memorize

### 1. Bronze patterns

```text
Singleplex: A→Bronze_A, B→Bronze_B, C→Bronze_C
Multiplex:  A,B,C → Bronze_Events → separate Silver tables
```

### 2. Auto Loader

```text
New files → cloudFiles → streaming DataFrame → checkpointed write → Bronze
```

### 3. Validation

```text
CHECK      → invalid write fails
Filter     → bad row excluded
Quarantine → bad row saved separately
```

### 4. Deduplication

```text
Incoming duplicate → dropDuplicates / watermark → foreachBatch MERGE → unique Silver keys
```

### 5. SCD2

```text
Old: current=true → close it / set end_date
New: insert current=true / start_date
```

## Command/function cheat sheet

| Function / command | One-line purpose |
|---|---|
| `spark.readStream` | Read streaming data |
| `.format("cloudFiles")` | Use Auto Loader |
| `.option("cloudFiles.format", "json")` | Read JSON files |
| `.option("cloudFiles.schemaLocation", ...)` | Store source schema tracking data |
| `.option("checkpointLocation", ...)` | Store stream progress |
| `.trigger(availableNow=True)` | Process available backlog, then stop |
| `.filter(F.col("topic") == "orders")` | Route one multiplex topic |
| `cast("string")` | Turn binary-encoded JSON payload into text |
| `from_json()` | Parse JSON into typed fields |
| `ALTER TABLE ... ADD CONSTRAINT ... CHECK` | Enforce data rule |
| `dropDuplicates()` | Remove duplicates based on chosen columns |
| `withWatermark()` | Set event-time lateness/state rule |
| `foreachBatch()` | Run custom logic for each streaming micro-batch |
| `MERGE INTO` | Match source and target to insert/update as defined |
| `WHEN NOT MATCHED THEN INSERT` | Insert only missing keys |
| `is_current`, `start_date`, `end_date` | Represent SCD2 versions |
| `CREATE OR REPLACE TABLE` | Replace a table definition/data from a query |

## Self-test: five questions before an interview

1. If I have 50 Kafka topics, what would Multiplex change about my Bronze design?
2. Why do I need a checkpoint even if I use `availableNow`?
3. Why might a CHECK constraint be unsuitable if the job should keep processing valid rows?
4. How do I protect against a duplicate order that arrives **tomorrow**, after today's micro-batch has completed?
5. Why can I query a book's price last month in SCD Type 2 even when I cannot Time Travel to an old Delta snapshot?

If you can answer these five aloud, you have understood the main ideas from this transcript.

---

## One-page final memory card

**Bronze = source history. Silver = cleaned/validated/business-specific data.**  
**Singleplex = one-to-one; Multiplex = many-to-one.**  
**Auto Loader = new files incrementally.**  
**`schemaLocation` = column structure; checkpoint = stream progress.**  
**`availableNow` = process currently available data and stop.**  
**`from_json` = raw JSON text to structured fields.**  
**CHECK = strict table rule; filter/quarantine = handling bad rows without rejecting the whole output.**  
**Streaming dedup = choose key + retain relevant state; watermark limits state.**  
**`foreachBatch` = run your custom MERGE for every micro-batch.**  
**Insert-only MERGE = do not insert target keys twice.**  
**SCD0 = no tracked change; SCD1 = overwrite; SCD2 = preserve versions with dates.**  
**SCD2 = business history; Time Travel = historical Delta snapshots.**

**Study suggestion:** Read Parts 1–8 carefully once, practise the answers in Parts 11–12 aloud, then review Parts 13–14 before interviews.
