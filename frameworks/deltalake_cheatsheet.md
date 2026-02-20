# Delta Lake Problem Types Cheat Sheet

> Comprehensive guide to Delta Lake patterns for data engineering interviews.
> Covers 12 problem types with trigger words, templates, examples, and pitfalls.

---

## TABLE OF CONTENTS

01. Table Creation & Schema Management
02. MERGE (Upsert) Operations
03. Time Travel & Versioning
04. Incremental Data Processing
05. Compaction & Optimization (OPTIMIZE / Z-ORDER)
06. Partitioning Strategies
07. Change Data Capture (CDC)
08. Multi-Hop Architecture (Bronze / Silver / Gold)
09. Schema Enforcement & Evolution
10. ACID Transactions & Concurrency
11. Data Quality & Constraints
12. Performance Tuning & Maintenance

---

## 1. Table Creation & Schema Management

### Trigger Words
- "create table", "define schema", "managed table", "external table"
- "overwrite", "save as delta", "convert to delta", "location"

### Key Patterns

```python
# Pattern 1: Create managed Delta table (PySpark)
df.write.format("delta").saveAsTable("catalog.schema.table_name")

# Pattern 2: Create external Delta table at specific path
df.write.format("delta").mode("overwrite").save("/mnt/delta/my_table")

# Pattern 3: SQL - CREATE TABLE
spark.sql("""
    CREATE TABLE IF NOT EXISTS catalog.schema.my_table (
        id          BIGINT,
        name        STRING,
        amount      DECIMAL(18,2),
        event_date  DATE
    )
    USING DELTA
    PARTITIONED BY (event_date)
    LOCATION '/mnt/delta/my_table'
""")

# Pattern 4: Convert Parquet to Delta
from delta.tables import DeltaTable

DeltaTable.convertToDelta(spark, "parquet.`/mnt/data/parquet_table`")
```

### Examples

```python
# Example 1: Create table with properties
spark.sql("""
    CREATE TABLE IF NOT EXISTS sales_fact (
        sale_id       BIGINT       GENERATED ALWAYS AS IDENTITY,
        product_id    INT          NOT NULL,
        customer_id   INT,
        sale_amount   DECIMAL(18,2),
        sale_date     DATE,
        region        STRING
    )
    USING DELTA
    PARTITIONED BY (sale_date)
    TBLPROPERTIES (
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact'   = 'true',
        'delta.logRetentionDuration'       = 'interval 30 days',
        'delta.deletedFileRetentionDuration' = 'interval 7 days'
    )
""")

# Example 2: Create from DataFrame with partition
(df
    .write
    .format("delta")
    .mode("overwrite")
    .partitionBy("year", "month")
    .option("overwriteSchema", "true")
    .saveAsTable("events")
)

# Example 3: CTAS (Create Table As Select)
spark.sql("""
    CREATE OR REPLACE TABLE silver.customers
    USING DELTA
    AS
    SELECT DISTINCT
        customer_id,
        TRIM(name)          AS name,
        LOWER(email)        AS email,
        current_timestamp() AS loaded_at
    FROM bronze.raw_customers
    WHERE customer_id IS NOT NULL
""")

# Example 4: Clone a table (shallow vs deep)
# Shallow clone: metadata only, references source files
spark.sql("CREATE TABLE dev.sales SHALLOW CLONE prod.sales")

# Deep clone: full independent copy
spark.sql("CREATE TABLE backup.sales DEEP CLONE prod.sales")
```

### Common Pitfalls

```python
# WRONG: Saving without specifying format (defaults to parquet in some configs)
df.write.save("/mnt/data/table")

# CORRECT: Always specify delta format explicitly
df.write.format("delta").save("/mnt/data/table")

# WRONG: Overwriting a partitioned table without dynamic mode
df.write.format("delta").mode("overwrite").save("/mnt/delta/events")
# This replaces ALL partitions!

# CORRECT: Use replaceWhere for selective partition overwrite
(df
    .write
    .format("delta")
    .mode("overwrite")
    .option("replaceWhere", "event_date = '2024-01-15'")
    .save("/mnt/delta/events")
)
```

---

## 2. MERGE (Upsert) Operations

### Trigger Words
- "upsert", "merge", "update or insert", "SCD", "slowly changing dimension"
- "insert if not exists", "update matching", "sync tables"

### Key Pattern

```python
from delta.tables import DeltaTable

# Template: Basic upsert
delta_table = DeltaTable.forPath(spark, "/mnt/delta/target")

(delta_table
    .alias("target")
    .merge(
        source_df.alias("source"),
        "target.id = source.id"
    )
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)
```

### Merge Strategy Matrix

| Need | MERGE Clause | Description |
|------|-------------|-------------|
| Update all matched columns | `whenMatchedUpdateAll()` | Updates every column |
| Update specific columns | `whenMatchedUpdate(set={...})` | Updates listed columns only |
| Delete matched rows | `whenMatchedDelete()` | Removes matched rows |
| Insert all new rows | `whenNotMatchedInsertAll()` | Inserts with all columns |
| Insert specific columns | `whenNotMatchedInsert(values={...})` | Inserts listed columns only |
| Conditional update | `whenMatchedUpdate(condition=..., set={...})` | Updates only when condition met |
| Update target when source deleted | `whenNotMatchedBySourceDelete()` | Deletes rows not in source |

### Examples

```python
from delta.tables import DeltaTable
from pyspark.sql import functions as F

# Example 1: Basic upsert (update existing, insert new)
target = DeltaTable.forName(spark, "catalog.schema.customers")

(target
    .alias("t")
    .merge(
        new_data.alias("s"),
        "t.customer_id = s.customer_id"
    )
    .whenMatchedUpdate(set={
        "name":       "s.name",
        "email":      "s.email",
        "updated_at": "current_timestamp()"
    })
    .whenNotMatchedInsert(values={
        "customer_id": "s.customer_id",
        "name":        "s.name",
        "email":       "s.email",
        "created_at":  "current_timestamp()",
        "updated_at":  "current_timestamp()"
    })
    .execute()
)

# Example 2: SCD Type 1 (overwrite with latest)
target = DeltaTable.forPath(spark, "/mnt/delta/dim_product")

(target
    .alias("t")
    .merge(
        source_df.alias("s"),
        "t.product_id = s.product_id"
    )
    .whenMatchedUpdateAll()   # Overwrite all columns
    .whenNotMatchedInsertAll()
    .execute()
)

# Example 3: SCD Type 2 (maintain history)
target = DeltaTable.forName(spark, "dim_customer")

# Step 1: Identify changed records
staged_updates = (
    source_df.alias("s")
    .join(
        target.toDF().alias("t"),
        "customer_id"
    )
    .where("""
        s.name   != t.name   OR
        s.email  != t.email  OR
        s.address != t.address
    """)
    .select("s.*")
)

# Step 2: Union new inserts with changes
new_inserts = (
    source_df.alias("s")
    .join(target.toDF().alias("t"), "customer_id", "left_anti")
    .select("s.*")
)

# Step 3: Merge — expire old, insert new version
(target
    .alias("t")
    .merge(
        staged_updates.union(new_inserts)
            .withColumn("effective_date", F.current_date())
            .withColumn("end_date", F.lit(None).cast("date"))
            .withColumn("is_current", F.lit(True))
            .alias("s"),
        "t.customer_id = s.customer_id AND t.is_current = true"
    )
    .whenMatchedUpdate(set={
        "end_date":   "current_date()",
        "is_current": "false"
    })
    .whenNotMatchedInsertAll()
    .execute()
)

# Example 4: Conditional merge (only update if newer)
(target
    .alias("t")
    .merge(
        source_df.alias("s"),
        "t.order_id = s.order_id"
    )
    .whenMatchedUpdate(
        condition="s.updated_at > t.updated_at",
        set={
            "status":     "s.status",
            "amount":     "s.amount",
            "updated_at": "s.updated_at"
        }
    )
    .whenNotMatchedInsertAll()
    .execute()
)

# Example 5: Delete + Upsert (full sync)
(target
    .alias("t")
    .merge(
        source_df.alias("s"),
        "t.id = s.id"
    )
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .whenNotMatchedBySourceDelete()   # Remove rows not in source
    .execute()
)

# Example 6: Deduplication via MERGE
(target
    .alias("t")
    .merge(
        deduped_source.alias("s"),
        "t.event_id = s.event_id AND t.event_date = s.event_date"
    )
    .whenNotMatchedInsertAll()   # Only insert if not already present
    .execute()
)
```

### Common Pitfalls

```python
# WRONG: Merge condition produces duplicates (many-to-many)
# If source has duplicate keys, merge can insert duplicates or fail
(target.alias("t")
    .merge(source_with_dups.alias("s"), "t.id = s.id")
    .whenNotMatchedInsertAll()
    .execute()
)
# ERROR: "MERGE operation found multiple source rows matching target row"

# CORRECT: Deduplicate source before merging
from pyspark.sql.window import Window

window_spec = Window.partitionBy("id").orderBy(F.col("updated_at").desc())
deduped = (source_with_dups
    .withColumn("rn", F.row_number().over(window_spec))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

(target.alias("t")
    .merge(deduped.alias("s"), "t.id = s.id")
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)

# WRONG: Not including partition column in merge condition on partitioned tables
# This forces a full table scan

# CORRECT: Include partition column for predicate pushdown
(target.alias("t")
    .merge(
        source_df.alias("s"),
        "t.event_date = s.event_date AND t.event_id = s.event_id"
    )
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)
```

---

## 3. Time Travel & Versioning

### Trigger Words
- "previous version", "rollback", "restore", "audit"
- "point in time", "version history", "undo", "as of"

### Key Pattern

```python
# Template: Read a previous version
# By version number
df_old = spark.read.format("delta").option("versionAsOf", 5).load("/mnt/delta/table")

# By timestamp
df_old = spark.read.format("delta").option("timestampAsOf", "2024-01-15").load("/mnt/delta/table")

# SQL syntax
spark.sql("SELECT * FROM my_table VERSION AS OF 5")
spark.sql("SELECT * FROM my_table TIMESTAMP AS OF '2024-01-15T00:00:00'")
```

### Examples

```python
from delta.tables import DeltaTable

# Example 1: View table history
delta_table = DeltaTable.forPath(spark, "/mnt/delta/sales")
history = delta_table.history()
history.select("version", "timestamp", "operation", "operationParameters").show(truncate=False)

# Example 2: Compare two versions (audit changes)
df_v5 = spark.read.format("delta").option("versionAsOf", 5).load("/mnt/delta/sales")
df_v6 = spark.read.format("delta").option("versionAsOf", 6).load("/mnt/delta/sales")

# Find new rows
new_rows = df_v6.exceptAll(df_v5)

# Find deleted rows
deleted_rows = df_v5.exceptAll(df_v6)

# Find changed rows
changed = (df_v6.alias("new")
    .join(df_v5.alias("old"), "id", "inner")
    .where("new.updated_at != old.updated_at")
)

# Example 3: Restore to a previous version
delta_table = DeltaTable.forName(spark, "my_table")

# Restore by version
delta_table.restoreToVersion(5)

# Restore by timestamp
delta_table.restoreToTimestamp("2024-01-15")

# SQL
spark.sql("RESTORE TABLE my_table TO VERSION AS OF 5")

# Example 4: Create a snapshot view for reporting
spark.sql("""
    CREATE OR REPLACE TEMPORARY VIEW sales_month_end AS
    SELECT * FROM sales TIMESTAMP AS OF '2024-01-31T23:59:59'
""")

# Example 5: Read change data feed (CDF) between versions
cdf = (spark.read
    .format("delta")
    .option("readChangeFeed", "true")
    .option("startingVersion", 5)
    .option("endingVersion", 10)
    .table("my_table")
)

# _change_type column: insert, update_preimage, update_postimage, delete
cdf.filter(F.col("_change_type") == "update_postimage").show()
```

### Common Pitfalls

```python
# WRONG: Relying on time travel beyond retention period
# Default retention is 30 days for log, 7 days for deleted files

# CORRECT: Increase retention for compliance needs
spark.sql("""
    ALTER TABLE my_table SET TBLPROPERTIES (
        'delta.logRetentionDuration'          = 'interval 90 days',
        'delta.deletedFileRetentionDuration'  = 'interval 30 days'
    )
""")

# WRONG: Running VACUUM with very short retention
delta_table.vacuum(0)   # Deletes ALL unreferenced files!

# CORRECT: Use safe retention period (default 168 hours = 7 days)
delta_table.vacuum(168)
```

---

## 4. Incremental Data Processing

### Trigger Words
- "incremental", "new data only", "append", "streaming"
- "Auto Loader", "readStream", "checkpoint", "trigger once"

### Key Patterns

```python
# Pattern 1: Auto Loader (cloud files) - recommended for incremental file ingestion
df_stream = (spark
    .readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", "/mnt/checkpoint/schema")
    .option("header", "true")
    .load("/mnt/landing/raw_data/")
)

# Pattern 2: Delta table as streaming source
df_stream = (spark
    .readStream
    .format("delta")
    .load("/mnt/delta/bronze_events")
)

# Pattern 3: Trigger once (batch-style incremental)
(df_stream
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/mnt/checkpoint/my_pipeline")
    .trigger(availableNow=True)     # Process all available, then stop
    .toTable("silver.events")
)
```

### Examples

```python
# Example 1: Auto Loader with schema inference + evolution
bronze_stream = (spark
    .readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.schemaLocation", "/mnt/checkpoint/bronze_schema")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .load("/mnt/landing/events/")
)

(bronze_stream
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source_file", F.input_file_name())
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/mnt/checkpoint/bronze_events")
    .option("mergeSchema", "true")
    .trigger(availableNow=True)
    .toTable("bronze.events")
)

# Example 2: Streaming MERGE (foreachBatch)
def upsert_to_silver(batch_df, batch_id):
    """Upsert each micro-batch into the silver table."""
    target = DeltaTable.forName(spark, "silver.customers")

    (target
        .alias("t")
        .merge(
            batch_df.alias("s"),
            "t.customer_id = s.customer_id"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

(spark
    .readStream
    .format("delta")
    .table("bronze.customers")
    .writeStream
    .foreachBatch(upsert_to_silver)
    .option("checkpointLocation", "/mnt/checkpoint/silver_customers")
    .trigger(availableNow=True)
    .start()
)

# Example 3: Change Data Feed for downstream consumption
# Enable CDF on source table first:
# ALTER TABLE my_table SET TBLPROPERTIES (delta.enableChangeDataFeed = true)

changes_stream = (spark
    .readStream
    .format("delta")
    .option("readChangeFeed", "true")
    .table("silver.orders")
)

# Process only inserts and updates (ignore deletes)
filtered = changes_stream.filter(
    F.col("_change_type").isin(["insert", "update_postimage"])
)

(filtered
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/mnt/checkpoint/gold_orders")
    .trigger(availableNow=True)
    .toTable("gold.orders_summary")
)

# Example 4: Auto Loader for CSV with known schema
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType

schema = StructType([
    StructField("transaction_id", StringType(), False),
    StructField("amount", DoubleType(), True),
    StructField("transaction_date", DateType(), True),
    StructField("customer_id", StringType(), False)
])

(spark
    .readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("header", "true")
    .schema(schema)
    .load("/mnt/landing/transactions/")
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/mnt/checkpoint/bronze_txn")
    .trigger(availableNow=True)
    .toTable("bronze.transactions")
)
```

### Trigger Mode Comparison

| Trigger | Behavior | Use Case |
|---------|----------|----------|
| `trigger(availableNow=True)` | Process all available, stop | Scheduled batch jobs |
| `trigger(processingTime="10 seconds")` | Micro-batch every 10s | Near-real-time |
| `trigger(once=True)` | Process one micro-batch, stop | Legacy (use availableNow) |
| No trigger (continuous) | Always running | Real-time dashboards |

### Common Pitfalls

```python
# WRONG: No checkpoint location (loses track of processed files)
df_stream.writeStream.format("delta").start()

# CORRECT: Always specify checkpoint
df_stream.writeStream.format("delta") \
    .option("checkpointLocation", "/mnt/checkpoint/my_stream") \
    .start()

# WRONG: Using overwrite mode with streaming
df_stream.writeStream.format("delta").outputMode("complete").start()
# "complete" mode rewrites the entire table each micro-batch

# CORRECT: Use append mode for incremental
df_stream.writeStream.format("delta").outputMode("append").start()
```

---

## 5. Compaction & Optimization (OPTIMIZE / Z-ORDER)

### Trigger Words
- "small files", "compact", "optimize", "z-order"
- "slow reads", "file size", "bin packing", "coalesce"

### Key Patterns

```python
# Pattern 1: OPTIMIZE (bin-packing / compaction)
spark.sql("OPTIMIZE my_table")

# Pattern 2: OPTIMIZE with Z-ORDER (co-locate data for faster queries)
spark.sql("OPTIMIZE my_table ZORDER BY (customer_id, order_date)")

# Pattern 3: OPTIMIZE a specific partition
spark.sql("OPTIMIZE my_table WHERE event_date = '2024-01-15'")

# Pattern 4: VACUUM (remove old/unreferenced files)
spark.sql("VACUUM my_table RETAIN 168 HOURS")
```

### Examples

```python
from delta.tables import DeltaTable

# Example 1: Compact small files in a table
spark.sql("OPTIMIZE catalog.schema.sales")

# Example 2: Z-ORDER by common filter columns
# Ideal when queries frequently filter on these columns
spark.sql("""
    OPTIMIZE catalog.schema.events
    ZORDER BY (user_id, event_type)
""")

# Example 3: Partition-level optimize (for large tables)
spark.sql("""
    OPTIMIZE catalog.schema.events
    WHERE event_date >= '2024-01-01' AND event_date < '2024-02-01'
    ZORDER BY (user_id)
""")

# Example 4: VACUUM to reclaim storage
delta_table = DeltaTable.forName(spark, "my_table")

# View files that would be deleted (dry run)
delta_table.vacuum(168)  # 168 hours = 7 days retention

# Example 5: Auto-optimize settings (applies on write)
spark.sql("""
    ALTER TABLE my_table SET TBLPROPERTIES (
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact'   = 'true'
    )
""")

# Example 6: Target file size
spark.sql("""
    ALTER TABLE my_table SET TBLPROPERTIES (
        'delta.targetFileSize' = '128mb'
    )
""")
```

### Z-ORDER vs Partitioning Decision

| Criteria | Partition By | Z-ORDER By |
|----------|-------------|------------|
| Cardinality | Low (< 1000 values) | High (millions of values) |
| Query patterns | Always filter on this column | Frequently filter on this column |
| Example columns | date, region, country | customer_id, user_id, product_id |
| File organization | Separate directories | Co-located within files |
| Write overhead | Low | Medium (compaction needed) |

### Common Pitfalls

```python
# WRONG: Z-ORDER on too many columns (diminishing returns after 3-4)
spark.sql("OPTIMIZE t ZORDER BY (a, b, c, d, e, f)")

# CORRECT: Z-ORDER on 1-3 most-queried columns
spark.sql("OPTIMIZE t ZORDER BY (customer_id, order_date)")

# WRONG: VACUUM with 0 retention (breaks time travel)
spark.sql("VACUUM my_table RETAIN 0 HOURS")

# CORRECT: Keep at least 7 days (168 hours)
spark.sql("VACUUM my_table RETAIN 168 HOURS")

# WRONG: Running OPTIMIZE on every write
# This creates overhead — small files are fine temporarily

# CORRECT: Schedule OPTIMIZE periodically (daily or weekly)
```

---

## 6. Partitioning Strategies

### Trigger Words
- "partition", "date partition", "cardinality", "data skew"
- "partition pruning", "predicate pushdown", "query performance"

### Key Pattern

```python
# Template: Partition by low-cardinality column
(df
    .write
    .format("delta")
    .partitionBy("event_date")
    .mode("overwrite")
    .saveAsTable("events")
)
```

### Partitioning Decision Guide

| Factor | Good Partition Column | Bad Partition Column |
|--------|----------------------|---------------------|
| Cardinality | < 1,000 distinct values | > 10,000 distinct values |
| Query pattern | Always in WHERE clause | Rarely filtered on |
| Data distribution | Relatively even | Heavily skewed |
| Example | date, region, status | user_id, transaction_id |

### Examples

```python
# Example 1: Date partitioned table
(df
    .withColumn("event_date", F.to_date("event_timestamp"))
    .write
    .format("delta")
    .partitionBy("event_date")
    .mode("overwrite")
    .saveAsTable("events")
)

# Example 2: Multi-level partitioning
(df
    .write
    .format("delta")
    .partitionBy("year", "month")
    .mode("overwrite")
    .saveAsTable("sales")
)

# Example 3: Liquid Clustering (Delta Lake 3.0+ / Databricks)
# Modern replacement for partitioning + Z-ORDER
spark.sql("""
    CREATE TABLE events (
        event_id    BIGINT,
        user_id     BIGINT,
        event_type  STRING,
        event_date  DATE
    )
    USING DELTA
    CLUSTER BY (event_date, user_id)
""")

# Optimize triggers clustering
spark.sql("OPTIMIZE events")

# Example 4: Selective partition overwrite
(df
    .write
    .format("delta")
    .mode("overwrite")
    .option("replaceWhere", "event_date = '2024-01-15'")
    .saveAsTable("events")
)

# Example 5: Check partition sizes (detect skew)
partition_stats = (spark
    .read
    .format("delta")
    .load("/mnt/delta/events")
    .groupBy("event_date")
    .agg(F.count("*").alias("row_count"))
    .orderBy(F.col("row_count").desc())
)
partition_stats.show(20)
```

### Common Pitfalls

```python
# WRONG: Partitioning by high-cardinality column
df.write.format("delta").partitionBy("user_id").saveAsTable("events")
# Creates millions of tiny directories with 1 file each!

# CORRECT: Partition by date, Z-ORDER by user_id
df.write.format("delta").partitionBy("event_date").saveAsTable("events")
spark.sql("OPTIMIZE events ZORDER BY (user_id)")

# WRONG: Too many partition levels
df.write.format("delta").partitionBy("year", "month", "day", "hour", "region")
# Combinatorial explosion of directories

# CORRECT: 1-2 partition levels max
df.write.format("delta").partitionBy("event_date")
```

---

## 7. Change Data Capture (CDC)

### Trigger Words
- "CDC", "change data capture", "track changes", "audit trail"
- "inserts and updates", "capture deletes", "change feed"

### Key Pattern

```python
# Enable Change Data Feed on table
spark.sql("""
    ALTER TABLE my_table SET TBLPROPERTIES (
        delta.enableChangeDataFeed = true
    )
""")

# Read changes between versions
changes = (spark.read
    .format("delta")
    .option("readChangeFeed", "true")
    .option("startingVersion", 10)
    .option("endingVersion", 15)
    .table("my_table")
)
```

### Change Types

| `_change_type` | Meaning |
|----------------|---------|
| `insert` | New row added |
| `update_preimage` | Row state BEFORE update |
| `update_postimage` | Row state AFTER update |
| `delete` | Row was deleted |

### Examples

```python
# Example 1: Read all changes since version 5
changes = (spark.read
    .format("delta")
    .option("readChangeFeed", "true")
    .option("startingVersion", 5)
    .table("silver.customers")
)

# Separate by change type
inserts = changes.filter(F.col("_change_type") == "insert")
updates_new = changes.filter(F.col("_change_type") == "update_postimage")
deletes = changes.filter(F.col("_change_type") == "delete")

print(f"Inserts: {inserts.count()}, Updates: {updates_new.count()}, Deletes: {deletes.count()}")

# Example 2: Streaming CDC to downstream table
(spark
    .readStream
    .format("delta")
    .option("readChangeFeed", "true")
    .table("silver.orders")
    .filter(F.col("_change_type").isin(["insert", "update_postimage"]))
    .writeStream
    .foreachBatch(lambda df, id: upsert_gold(df, id))
    .option("checkpointLocation", "/mnt/checkpoint/gold_orders")
    .trigger(availableNow=True)
    .start()
)

# Example 3: Audit log from CDC
audit_log = (spark.read
    .format("delta")
    .option("readChangeFeed", "true")
    .option("startingTimestamp", "2024-01-01")
    .table("silver.customers")
    .select(
        "_change_type",
        "_commit_version",
        "_commit_timestamp",
        "customer_id",
        "name",
        "email"
    )
    .orderBy("_commit_timestamp")
)

# Example 4: Compute net changes (latest state per key)
from pyspark.sql.window import Window

window_spec = Window.partitionBy("customer_id").orderBy(F.col("_commit_version").desc())

net_changes = (changes
    .withColumn("rn", F.row_number().over(window_spec))
    .filter(F.col("rn") == 1)
    .drop("rn")
    .filter(F.col("_change_type") != "update_preimage")
)
```

### Common Pitfalls

```python
# WRONG: Reading CDF without enabling it first
spark.read.format("delta").option("readChangeFeed", "true").table("my_table")
# ERROR: Change data feed is not enabled for table

# CORRECT: Enable first, then read
spark.sql("ALTER TABLE my_table SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")
# CDF only captures changes AFTER enabling

# WRONG: Processing both preimage and postimage (double counting)
changes.filter(F.col("_change_type").isin(["update_preimage", "update_postimage"]))

# CORRECT: Use postimage for current state
changes.filter(F.col("_change_type").isin(["insert", "update_postimage"]))
```

---

## 8. Multi-Hop Architecture (Bronze / Silver / Gold)

### Trigger Words
- "medallion", "bronze silver gold", "layered", "data lakehouse"
- "raw to curated", "staging to serving", "ETL pipeline layers"

### Architecture Overview

```
Landing Zone  -->  Bronze (Raw)  -->  Silver (Cleaned)  -->  Gold (Aggregated)
  (files)          (1:1 copy)        (dedupe, validate)     (business metrics)
```

| Layer | Purpose | Data Quality | Schema |
|-------|---------|-------------|--------|
| **Bronze** | Raw ingestion, append-only | As-is from source | Source schema + metadata |
| **Silver** | Cleaned, deduplicated, validated | Standardized, nulls handled | Conformed schema |
| **Gold** | Business-level aggregations | High quality, ready for BI | Star schema / flat |

### Examples

```python
# ─── BRONZE LAYER ──────────────────────────────────────────────────────
# Ingest raw files with metadata columns

bronze_stream = (spark
    .readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", "/mnt/checkpoint/bronze_schema")
    .load("/mnt/landing/orders/")
)

(bronze_stream
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source_file", F.input_file_name())
    .withColumn("_raw_data", F.to_json(F.struct("*")))
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/mnt/checkpoint/bronze_orders")
    .trigger(availableNow=True)
    .toTable("bronze.orders")
)

# ─── SILVER LAYER ──────────────────────────────────────────────────────
# Clean, validate, deduplicate

def bronze_to_silver(batch_df, batch_id):
    """Clean and upsert bronze data into silver."""
    from pyspark.sql.window import Window

    # 1. Parse and clean
    cleaned = (batch_df
        .select(
            F.col("order_id").cast("long"),
            F.trim(F.col("customer_name")).alias("customer_name"),
            F.lower(F.col("email")).alias("email"),
            F.col("amount").cast("decimal(18,2)"),
            F.to_date("order_date", "yyyy-MM-dd").alias("order_date"),
            F.col("_ingested_at")
        )
        .filter(F.col("order_id").isNotNull())  # Drop nulls
        .filter(F.col("amount") > 0)             # Business rule
    )

    # 2. Deduplicate within batch
    window_spec = Window.partitionBy("order_id").orderBy(F.col("_ingested_at").desc())
    deduped = (cleaned
        .withColumn("rn", F.row_number().over(window_spec))
        .filter(F.col("rn") == 1)
        .drop("rn")
    )

    # 3. Upsert into silver
    target = DeltaTable.forName(spark, "silver.orders")
    (target.alias("t")
        .merge(deduped.alias("s"), "t.order_id = s.order_id")
        .whenMatchedUpdate(
            condition="s._ingested_at > t._ingested_at",
            set={"customer_name": "s.customer_name", "email": "s.email",
                 "amount": "s.amount", "order_date": "s.order_date",
                 "_ingested_at": "s._ingested_at"}
        )
        .whenNotMatchedInsertAll()
        .execute()
    )

(spark
    .readStream
    .format("delta")
    .table("bronze.orders")
    .writeStream
    .foreachBatch(bronze_to_silver)
    .option("checkpointLocation", "/mnt/checkpoint/silver_orders")
    .trigger(availableNow=True)
    .start()
)

# ─── GOLD LAYER ──────────────────────────────────────────────────────
# Business-level aggregations

spark.sql("""
    CREATE OR REPLACE TABLE gold.daily_sales_summary AS
    SELECT
        order_date,
        COUNT(DISTINCT order_id)      AS total_orders,
        COUNT(DISTINCT email)         AS unique_customers,
        SUM(amount)                   AS total_revenue,
        AVG(amount)                   AS avg_order_value,
        MAX(amount)                   AS max_order_value
    FROM silver.orders
    GROUP BY order_date
    ORDER BY order_date DESC
""")
```

---

## 9. Schema Enforcement & Evolution

### Trigger Words
- "schema mismatch", "new columns", "column type change"
- "mergeSchema", "overwriteSchema", "schema evolution"

### Key Patterns

```python
# Pattern 1: Schema enforcement (default - rejects mismatched writes)
df.write.format("delta").mode("append").saveAsTable("my_table")
# Fails if df schema doesn't match table schema

# Pattern 2: Schema evolution - add new columns automatically
df.write.format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .saveAsTable("my_table")

# Pattern 3: Schema overwrite - replace entire schema
df.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("my_table")
```

### Schema Evolution Rules

| Operation | `mergeSchema` | `overwriteSchema` | Behavior |
|-----------|:---:|:---:|----------|
| Add new columns | Yes | Yes | New columns added to table |
| Remove columns | No | Yes | Old columns remain (mergeSchema) |
| Change column type (widen) | Yes | Yes | INT -> BIGINT allowed |
| Change column type (narrow) | No | Yes | BIGINT -> INT only with overwrite |
| Rename columns | No | Yes | Treated as drop + add |

### Examples

```python
# Example 1: Append with new columns (schema evolution)
# Table has: id, name, email
# New data has: id, name, email, phone  <-- phone is new

(df_with_phone
    .write
    .format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .saveAsTable("customers")
)
# Now table has: id, name, email, phone (phone is NULL for old rows)

# Example 2: Merge with schema evolution
(target.alias("t")
    .merge(source_with_new_cols.alias("s"), "t.id = s.id")
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)
# Use spark config for merge schema evolution:
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

# Example 3: Add column via ALTER TABLE
spark.sql("ALTER TABLE my_table ADD COLUMNS (phone STRING AFTER email)")

# Example 4: Change column type
spark.sql("ALTER TABLE my_table ALTER COLUMN amount TYPE DECIMAL(18,4)")

# Example 5: Rename column
spark.sql("ALTER TABLE my_table RENAME COLUMN old_name TO new_name")

# Example 6: Add column comment
spark.sql("ALTER TABLE my_table ALTER COLUMN email COMMENT 'Primary contact email'")

# Example 7: Set NOT NULL constraint
spark.sql("ALTER TABLE my_table ALTER COLUMN customer_id SET NOT NULL")
```

### Common Pitfalls

```python
# WRONG: Writing data with different schema without mergeSchema
# This will FAIL with AnalysisException
df_new_schema.write.format("delta").mode("append").saveAsTable("table")

# CORRECT: Enable schema evolution
df_new_schema.write.format("delta").mode("append") \
    .option("mergeSchema", "true").saveAsTable("table")

# WRONG: Using overwriteSchema when you just want to add columns
# This DROPS all existing data columns that aren't in the new DataFrame!
df.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true").saveAsTable("table")

# CORRECT: Use mergeSchema for additive changes
df.write.format("delta").mode("append") \
    .option("mergeSchema", "true").saveAsTable("table")
```

---

## 10. ACID Transactions & Concurrency

### Trigger Words
- "concurrent writes", "transaction", "isolation", "conflict"
- "optimistic concurrency", "write conflict", "atomic"

### Key Concepts

| Property | Delta Lake Guarantee |
|----------|---------------------|
| **Atomicity** | Writes are all-or-nothing (committed via `_delta_log`) |
| **Consistency** | Schema enforcement prevents corrupt writes |
| **Isolation** | Serializable isolation via optimistic concurrency control |
| **Durability** | Written to durable cloud storage (S3, ADLS, GCS) |

### Examples

```python
# Example 1: Concurrent writes - Delta handles automatically
# Writer A: MERGE into customers WHERE region = 'East'
# Writer B: MERGE into customers WHERE region = 'West'
# Both succeed because they touch different partitions (no conflict)

# Example 2: Handling write conflicts with retry
from delta.exceptions import ConcurrentAppendException
import time

def safe_merge(source_df, target_path, max_retries=3):
    """Merge with retry on conflict."""
    for attempt in range(max_retries):
        try:
            target = DeltaTable.forPath(spark, target_path)
            (target.alias("t")
                .merge(source_df.alias("s"), "t.id = s.id")
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
            )
            return  # Success
        except ConcurrentAppendException:
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)  # Exponential backoff
            else:
                raise

# Example 3: Isolation levels
# Writes: Serializable (conflicts detected and retried)
# Reads: Snapshot isolation (consistent point-in-time view)

# Reader always sees a consistent snapshot even during writes
df = spark.read.format("delta").load("/mnt/delta/table")
# This is safe even while another process is writing

# Example 4: Enable write conflict detection tuning
spark.sql("""
    ALTER TABLE my_table SET TBLPROPERTIES (
        'delta.isolationLevel' = 'WriteSerializable'
    )
""")
# WriteSerializable: Allows some concurrent appends (less strict, more throughput)
# Serializable (default): Strictest isolation
```

---

## 11. Data Quality & Constraints

### Trigger Words
- "constraints", "check", "not null", "expectations"
- "data quality", "validation rules", "enforce rules"

### Key Patterns

```python
# Pattern 1: NOT NULL constraint
spark.sql("ALTER TABLE my_table ALTER COLUMN id SET NOT NULL")

# Pattern 2: CHECK constraint
spark.sql("ALTER TABLE my_table ADD CONSTRAINT positive_amount CHECK (amount > 0)")

# Pattern 3: Delta Expectations (Databricks DLT)
# @dlt.expect("valid_amount", "amount > 0")
# @dlt.expect_or_drop("valid_date", "order_date IS NOT NULL")
# @dlt.expect_or_fail("valid_id", "id IS NOT NULL")
```

### Constraint Types

| Constraint | Syntax | Behavior |
|-----------|--------|----------|
| NOT NULL | `ALTER COLUMN col SET NOT NULL` | Rejects NULL inserts |
| CHECK | `ADD CONSTRAINT name CHECK (expr)` | Rejects rows failing condition |
| Informational (DLT) | `@dlt.expect(...)` | Logs violations, doesn't block |
| Drop invalid (DLT) | `@dlt.expect_or_drop(...)` | Silently drops bad rows |
| Fail on invalid (DLT) | `@dlt.expect_or_fail(...)` | Fails pipeline on violation |

### Examples

```python
# Example 1: Add constraints
spark.sql("ALTER TABLE orders ALTER COLUMN order_id SET NOT NULL")
spark.sql("ALTER TABLE orders ADD CONSTRAINT valid_amount CHECK (amount >= 0)")
spark.sql("ALTER TABLE orders ADD CONSTRAINT valid_date CHECK (order_date <= current_date())")

# Example 2: View constraints
spark.sql("DESCRIBE DETAIL orders").show()
spark.sql("SHOW TBLPROPERTIES orders").show()

# Example 3: Drop a constraint
spark.sql("ALTER TABLE orders DROP CONSTRAINT valid_amount")

# Example 4: Application-level quality checks before write
def validate_and_write(df, table_name):
    """Validate data quality before writing."""

    total = df.count()

    # Check for nulls in required columns
    null_check = df.filter(
        F.col("order_id").isNull() | F.col("amount").isNull()
    ).count()

    if null_check > 0:
        raise ValueError(f"Found {null_check}/{total} rows with NULL required fields")

    # Check for invalid values
    invalid = df.filter(F.col("amount") < 0).count()
    if invalid > 0:
        raise ValueError(f"Found {invalid} rows with negative amount")

    # Check for duplicates
    dup_count = df.groupBy("order_id").count().filter(F.col("count") > 1).count()
    if dup_count > 0:
        raise ValueError(f"Found {dup_count} duplicate order_ids")

    # All checks passed — write
    df.write.format("delta").mode("append").saveAsTable(table_name)
    print(f"Successfully wrote {total} rows to {table_name}")

# Example 5: Quality metrics table
quality_metrics = (df
    .agg(
        F.count("*").alias("total_rows"),
        F.sum(F.when(F.col("order_id").isNull(), 1).otherwise(0)).alias("null_order_ids"),
        F.sum(F.when(F.col("amount") < 0, 1).otherwise(0)).alias("negative_amounts"),
        F.sum(F.when(F.col("email").rlike(r'^[\w.]+@[\w.]+\.\w+$'), 0)
              .otherwise(1)).alias("invalid_emails"),
        F.countDistinct("order_id").alias("distinct_orders")
    )
    .withColumn("duplicate_orders",
                F.col("total_rows") - F.col("distinct_orders"))
    .withColumn("quality_score",
                (1 - (F.col("null_order_ids") + F.col("negative_amounts")
                      + F.col("invalid_emails")) / F.col("total_rows")) * 100)
    .withColumn("check_timestamp", F.current_timestamp())
)

quality_metrics.write.format("delta").mode("append").saveAsTable("audit.data_quality_log")
```

---

## 12. Performance Tuning & Maintenance

### Trigger Words
- "slow query", "performance", "vacuum", "analyze"
- "file count", "table maintenance", "statistics", "cache"

### Key Maintenance Commands

```sql
-- Compact small files
OPTIMIZE my_table;

-- Compact + co-locate data by columns
OPTIMIZE my_table ZORDER BY (customer_id);

-- Remove old unreferenced files
VACUUM my_table RETAIN 168 HOURS;

-- Compute column statistics for query optimizer
ANALYZE TABLE my_table COMPUTE STATISTICS FOR ALL COLUMNS;

-- View table details
DESCRIBE DETAIL my_table;
DESCRIBE HISTORY my_table;
```

### Performance Checklist

| Do | Don't |
|----|-------|
| Partition by low-cardinality date columns | Partition by high-cardinality IDs |
| Z-ORDER by frequently-queried columns | Z-ORDER by more than 3-4 columns |
| Enable auto-optimize for streaming tables | Run OPTIMIZE on every write |
| VACUUM periodically (weekly) | VACUUM with 0 hour retention |
| Use ANALYZE TABLE for stats | Ignore stale statistics |
| Use predicate pushdown (filter on partition cols) | Read entire table and filter in Spark |
| Use liquid clustering (Delta 3.0+) | Over-partition small tables |
| Cache hot tables in memory | Cache everything |

### Examples

```python
# Example 1: Full maintenance routine
def maintain_table(table_name, zorder_cols=None, vacuum_hours=168):
    """Run full maintenance on a Delta table."""

    # 1. Optimize (compact small files)
    if zorder_cols:
        cols = ", ".join(zorder_cols)
        spark.sql(f"OPTIMIZE {table_name} ZORDER BY ({cols})")
    else:
        spark.sql(f"OPTIMIZE {table_name}")

    # 2. Vacuum old files
    spark.sql(f"VACUUM {table_name} RETAIN {vacuum_hours} HOURS")

    # 3. Compute statistics
    spark.sql(f"ANALYZE TABLE {table_name} COMPUTE STATISTICS FOR ALL COLUMNS")

    print(f"Maintenance complete for {table_name}")

maintain_table("silver.orders", zorder_cols=["customer_id", "order_date"])

# Example 2: Diagnose small file problem
detail = spark.sql("DESCRIBE DETAIL silver.orders").first()
print(f"Number of files: {detail['numFiles']}")
print(f"Size in bytes:   {detail['sizeInBytes']}")
print(f"Avg file size:   {detail['sizeInBytes'] / max(detail['numFiles'], 1) / 1024 / 1024:.1f} MB")

# Target: 128 MB - 1 GB per file
# If avg file size < 32 MB, run OPTIMIZE

# Example 3: Monitor table growth
history = spark.sql("DESCRIBE HISTORY silver.orders LIMIT 20")
history.select(
    "version", "timestamp", "operation",
    "operationMetrics.numOutputRows",
    "operationMetrics.numOutputBytes"
).show(truncate=False)

# Example 4: Table properties for performance
spark.sql("""
    ALTER TABLE silver.orders SET TBLPROPERTIES (
        'delta.autoOptimize.optimizeWrite'       = 'true',
        'delta.autoOptimize.autoCompact'         = 'true',
        'delta.targetFileSize'                   = '134217728',
        'delta.tuneFileSizesForRewrites'         = 'true',
        'delta.logRetentionDuration'             = 'interval 30 days',
        'delta.deletedFileRetentionDuration'     = 'interval 7 days'
    )
""")

# Example 5: Check for data skew
partition_sizes = (spark
    .read.format("delta").load("/mnt/delta/events")
    .groupBy("event_date")
    .agg(F.count("*").alias("rows"))
    .orderBy(F.col("rows").desc())
)
partition_sizes.show(20)
# If max/min ratio > 10x, consider re-partitioning strategy
```

---

## Quick Reference Card

### DELTA LAKE OPERATIONS AT A GLANCE

| Task | Command / Pattern |
|------|-------------------|
| **Create table** | `df.write.format("delta").saveAsTable(name)` |
| **Upsert (MERGE)** | `delta_table.alias("t").merge(src.alias("s"), ...).whenMatched...execute()` |
| **Read version N** | `.option("versionAsOf", N).load(path)` |
| **Restore version** | `delta_table.restoreToVersion(N)` |
| **Compact files** | `OPTIMIZE table_name` |
| **Co-locate data** | `OPTIMIZE table ZORDER BY (col1, col2)` |
| **Remove old files** | `VACUUM table RETAIN 168 HOURS` |
| **Add column** | `ALTER TABLE t ADD COLUMNS (col TYPE)` |
| **Schema evolution** | `.option("mergeSchema", "true")` |
| **Change Data Feed** | `.option("readChangeFeed", "true")` |
| **Streaming ingest** | `readStream.format("cloudFiles")...writeStream.format("delta")` |
| **Streaming merge** | `.writeStream.foreachBatch(upsert_fn)` |
| **NOT NULL** | `ALTER TABLE t ALTER COLUMN col SET NOT NULL` |
| **CHECK constraint** | `ALTER TABLE t ADD CONSTRAINT name CHECK (expr)` |
| **Table stats** | `ANALYZE TABLE t COMPUTE STATISTICS FOR ALL COLUMNS` |
| **View history** | `DESCRIBE HISTORY table_name` |
| **Table details** | `DESCRIBE DETAIL table_name` |
| **Deep clone** | `CREATE TABLE copy DEEP CLONE source` |
| **Shallow clone** | `CREATE TABLE copy SHALLOW CLONE source` |

---

## Interview Pattern Recognition

| Interview Phrase | Delta Lake Pattern |
|------------------|--------------------|
| "update or insert" | `MERGE ... whenMatchedUpdate ... whenNotMatchedInsert` |
| "track all changes" | Change Data Feed (`readChangeFeed`) |
| "go back to yesterday" | Time Travel (`versionAsOf` / `timestampAsOf`) |
| "too many small files" | `OPTIMIZE` (bin-packing / compaction) |
| "speed up queries" | Z-ORDER, partitioning, ANALYZE TABLE |
| "schema changed" | Schema evolution (`mergeSchema = true`) |
| "SCD Type 2" | MERGE with expire-old + insert-new pattern |
| "real-time ingestion" | Auto Loader + Structured Streaming |
| "raw to curated" | Medallion architecture (Bronze/Silver/Gold) |
| "data quality" | Constraints, expectations, quality metrics table |
| "concurrent writes" | ACID transactions, optimistic concurrency |
| "undo a bad write" | `RESTORE TABLE ... TO VERSION AS OF N` |
| "audit who changed what" | `DESCRIBE HISTORY`, Change Data Feed |
| "partition strategy" | Low-cardinality partition + Z-ORDER high-cardinality |

---

## Decision Tree: Choosing the Right Pattern

```
Need to write data?
├── First time (new table) → CREATE TABLE / df.write.saveAsTable()
├── Append only (logs, events) → df.write.mode("append")
├── Replace partition → .option("replaceWhere", "date = ...")
├── Update existing rows → MERGE (upsert)
│   ├── Simple overwrite → whenMatchedUpdateAll + whenNotMatchedInsertAll
│   ├── Keep history (SCD2) → Expire old row + insert new version
│   └── Full sync → Add whenNotMatchedBySourceDelete
└── Delete rows → MERGE with whenMatchedDelete or DELETE FROM

Need to read data?
├── Current state → spark.read.format("delta").load(path)
├── Previous version → .option("versionAsOf", N)
├── Changes between versions → .option("readChangeFeed", "true")
└── Streaming → spark.readStream.format("delta")

Need to optimize?
├── Small files → OPTIMIZE
├── Slow filtered queries → ZORDER BY (filter columns)
├── Old files wasting storage → VACUUM
├── Bad statistics → ANALYZE TABLE
└── Over-partitioned → Re-create with fewer partitions / liquid clustering
```

---

## Common Pitfalls & Solutions

| Pitfall | Solution |
|---------|----------|
| MERGE fails: duplicate source keys | Deduplicate source BEFORE merging |
| Overwrite deletes all partitions | Use `replaceWhere` for selective overwrite |
| Time travel fails (version not found) | Increase `logRetentionDuration` |
| VACUUM breaks time travel | Never VACUUM with < 7 day retention |
| Schema mismatch on append | Use `mergeSchema = true` or fix upstream |
| Too many small files | Enable auto-optimize or schedule OPTIMIZE |
| High-cardinality partitioning | Use Z-ORDER or liquid clustering instead |
| CDF not capturing changes | Enable CDF first; captures changes only after enabling |
| Slow MERGE on large tables | Include partition column in merge condition |
| Concurrent write conflicts | Implement retry with exponential backoff |

---

Happy Lakehousing!
