# Scenario-Based Q&A
## MAANG/FAANG Level | AWS Glue / ETL Orchestrator Context

---

## Table of Contents

1. [One Task Running Much Longer Than Others](#scenario-1-one-task-running-much-longer-than-others)
2. [Handling Late Arriving Data in Streaming Pipelines](#scenario-2-handling-late-arriving-data-in-streaming-pipelines)
3. [Skewed Join Problem](#scenario-3-skewed-join-problem)
4. [Pipeline Needs Incremental Processing](#scenario-4-pipeline-needs-incremental-processing)
5. [Handling Schema Evolution in Data Lake](#scenario-5-handling-schema-evolution-in-data-lake)
6. [Broadcast Join Fails in Production](#scenario-6-broadcast-join-fails-in-production)
7. [Spark Streaming Job Restart Causes Data Loss](#scenario-7-spark-streaming-job-restart-causes-data-loss)
8. [Join Between Tables With Different Partitioning](#scenario-8-join-between-tables-with-different-partitioning)

---

## Scenario 1: One Task Running Much Longer Than Others

**Scenario:** In the Spark UI you observe: 199 tasks completed quickly, 1 task running for 20 minutes.

**Interview Questions:**
- What is the likely cause of this issue?
- How would you fix it?

### Root Cause

This is a classic **data skew** problem. One partition contains disproportionately more data than the others — typically caused by a highly frequent key value (e.g., `NULL`, `unknown`, or a single dominant user_id). All 199 tasks process their balanced partitions quickly while the one oversized partition monopolises a single executor for 20+ minutes.

**How to confirm in Spark UI:** Open the stage detail → sort tasks by "Duration" → the outlier task will show a dramatically higher "Input Size / Records" than peers.

---

### Solution 1 — Salting the Skewed Key (Most Common Fix)

Add a random "salt" suffix to the skewed key to artificially distribute it across multiple partitions, then strip the salt after the operation.

```python
from pyspark.sql import functions as F

# Identify skewed key distribution
df.groupBy("user_id").count().orderBy(F.desc("count")).show(10)

# Step 1: Salt the skewed DataFrame (spread one key across N buckets)
SALT_BUCKETS = 50
df_skewed = df.withColumn(
    "salted_key",
    F.concat(F.col("user_id"), F.lit("_"), (F.rand() * SALT_BUCKETS).cast("int"))
)

# Step 2: Explode the lookup/smaller DataFrame to match all salt values
df_lookup = df_lookup.withColumn("salt", F.array([F.lit(i) for i in range(SALT_BUCKETS)]))
df_lookup_exploded = df_lookup.withColumn("salt_val", F.explode("salt")).drop("salt")
df_lookup_exploded = df_lookup_exploded.withColumn(
    "salted_key",
    F.concat(F.col("user_id"), F.lit("_"), F.col("salt_val"))
)

# Step 3: Join on salted key
df_result = df_skewed.join(df_lookup_exploded, on="salted_key", how="left")

# Step 4: Drop the salt columns
df_result = df_result.drop("salted_key", "salt_val")
```

**Tradeoff:** Exploding the lookup table increases its size by N times. Use only when lookup table is manageable (< a few GB).

---

### Solution 2 — Adaptive Query Execution (AQE) — Spark 3.x

Enable Spark's built-in runtime skew detection and partition splitting. Zero code changes required.

```python
spark = SparkSession.builder \
    .appName("SkewFix") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.skewJoin.enabled", "true") \
    .config("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5") \
    .config("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256MB") \
    .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128MB") \
    .getOrCreate()

# AQE detects at runtime that a partition is > 5x the median size
# and automatically splits it — no manual salting needed
df_result = df_large.join(df_other, on="user_id", how="inner")
df_result.write.parquet("s3://bucket/output/")
```

**In AWS Glue:** Set `--conf spark.sql.adaptive.enabled=true` in the Glue job parameters.

---

### Solution 3 — Repartition Before the Shuffle-Heavy Operation

Force a balanced repartition using a high-cardinality secondary key alongside the skewed key.

```python
# Repartition by a composite key to break the skew
# user_id alone is skewed; adding event_date distributes load more evenly
df_balanced = df.repartition(200, "user_id", "event_date")

# Now the groupBy/join operates on balanced partitions
df_result = df_balanced \
    .groupBy("user_id", "event_date") \
    .agg(
        F.sum("revenue").alias("total_revenue"),
        F.count("*").alias("event_count")
    )
```

**When to use:** When you cannot use AQE (Spark 2.x / older Glue) and the skew is on a key that has a natural secondary dimension to split on.

---

## Scenario 2: Handling Late Arriving Data in Streaming Pipelines

**Scenario:** You are building a real-time event processing pipeline using Structured Streaming. Some events arrive 10 minutes late.

**Interview Questions:**
- How would you design the pipeline to handle late-arriving data?
- What Spark features would you use?

### Root Cause

In event-time based aggregations, if you close a window the moment wall-clock time passes the window boundary, late events are silently dropped. For financial or audit-critical data (as in this ETL orchestrator), that is data loss.

---

### Solution 1 — Watermarking with Event-Time Windows (Primary Approach)

Use `withWatermark` to tell Spark how late data can arrive. Spark holds state for the window until `event_time + watermark_delay` passes.

```python
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType

# Read from Kafka (as used in streaming ETL)
df_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "broker:9092") \
    .option("subscribe", "etl_events") \
    .load()

df_parsed = df_stream.selectExpr("CAST(value AS STRING) as json") \
    .select(F.from_json("json", schema).alias("data")) \
    .select("data.*")

# Define watermark: accept events up to 10 minutes late
df_watermarked = df_parsed.withWatermark("event_time", "10 minutes")

# Tumbling window aggregation — window stays open 10 min past its end
df_aggregated = df_watermarked \
    .groupBy(
        F.window("event_time", "5 minutes"),   # 5-minute tumbling window
        "user_id"
    ) \
    .agg(
        F.sum("amount").alias("total_amount"),
        F.count("*").alias("event_count")
    )

# Write with append mode (only emit results after watermark passes)
query = df_aggregated.writeStream \
    .outputMode("append") \
    .format("delta") \
    .option("checkpointLocation", "s3://bucket/checkpoints/stream_agg/") \
    .start("s3://bucket/output/aggregated/")
```

**Key rule:** `append` mode only emits a window result after `window_end + watermark_delay` passes. `update` mode emits partial results immediately but can produce duplicates downstream.

---

### Solution 2 — Sliding Windows for Overlapping Late Event Coverage

When late events must be captured across multiple overlapping aggregation windows.

```python
# Sliding window: 10-minute window, sliding every 2 minutes
# Each event falls into multiple windows — better late event coverage
df_sliding = df_watermarked \
    .groupBy(
        F.window("event_time", "10 minutes", "2 minutes"),  # (window, slide)
        "product_id"
    ) \
    .agg(F.sum("revenue").alias("windowed_revenue"))

query = df_sliding.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("checkpointLocation", "s3://bucket/checkpoints/sliding/") \
    .partitionBy("product_id") \
    .start("s3://bucket/output/sliding_agg/")
```

**Tradeoff:** Sliding windows multiply state size. A 10-min window sliding every 2 minutes = 5x the state of a tumbling window. Size checkpoints accordingly.

---

### Solution 3 — Lambda Architecture: Stream + Batch Reconciliation

For SLA-critical pipelines where late data must be reconciled exactly (e.g., financial reporting), combine a fast streaming layer with a nightly batch correction pass.

```python
# --- STREAMING LAYER (speed layer): near-real-time, may miss late events ---
df_stream_result = df_watermarked \
    .groupBy(F.window("event_time", "1 hour"), "account_id") \
    .agg(F.sum("amount").alias("streaming_total")) \
    .writeStream \
    .outputMode("append") \
    .format("delta") \
    .option("checkpointLocation", "s3://bucket/checkpoints/speed/") \
    .start("s3://bucket/speed_layer/")

# --- BATCH LAYER (correctness layer): nightly, reads full day including late arrivals ---
# Run as a separate Glue batch job
df_full_day = spark.read.parquet("s3://bucket/raw/events/date=2024-01-15/")

df_batch_result = df_full_day \
    .groupBy(F.window("event_time", "1 hour"), "account_id") \
    .agg(F.sum("amount").alias("batch_total"))

# MERGE batch results into serving layer (overwrites streaming totals)
# Uses Delta Lake MERGE for upsert — same pattern as the ETL orchestrator
from delta.tables import DeltaTable

serving_table = DeltaTable.forPath(spark, "s3://bucket/serving_layer/")
serving_table.alias("target").merge(
    df_batch_result.alias("source"),
    "target.account_id = source.account_id AND target.window = source.window"
) \
.whenMatchedUpdateAll() \
.whenNotMatchedInsertAll() \
.execute()
```

---

## Scenario 3: Skewed Join Problem

**Scenario:** You are joining two large datasets on `user_id`. During execution, one executor takes significantly longer than others.

**Interview Questions:**
- How would you detect data skew in Spark?
- What techniques would you use to fix skewed joins?

### Root Cause

A small number of `user_id` values (e.g., guest users, bots, internal service accounts) have millions of records. When Spark shuffles by `user_id`, all records for that key go to one executor partition — causing the "straggler task" symptom.

---

### Solution 1 — Detect Skew Then Apply Broadcast for Hot Keys

Separate the skewed "hot keys" from normal keys, handle them differently, then union results.

```python
from pyspark.sql import functions as F

# Step 1: Detect hot keys (keys with count > threshold)
SKEW_THRESHOLD = 500_000
key_counts = df_large.groupBy("user_id").count()
hot_keys = key_counts.filter(F.col("count") > SKEW_THRESHOLD).select("user_id")

# Step 2: Split the dataset
df_hot = df_large.join(F.broadcast(hot_keys), on="user_id", how="inner")
df_normal = df_large.join(F.broadcast(hot_keys), on="user_id", how="left_anti")

# Step 3: Handle hot keys with broadcast join (no shuffle)
df_lookup_broadcast = F.broadcast(df_lookup)
df_hot_joined = df_hot.join(df_lookup_broadcast, on="user_id", how="left")

# Step 4: Handle normal keys with standard shuffle join
df_normal_joined = df_normal.join(df_lookup, on="user_id", how="left")

# Step 5: Union results
df_final = df_hot_joined.union(df_normal_joined)
```

---

### Solution 2 — Salted Join (Generic Skew Fix)

```python
import math

SALT_FACTOR = 50  # Number of salt buckets (tune based on skew severity)

# Salt the large (skewed) DataFrame
df_large_salted = df_large.withColumn(
    "salt", (F.rand() * SALT_FACTOR).cast("int")
).withColumn(
    "join_key", F.concat_ws("_", F.col("user_id"), F.col("salt"))
)

# Replicate the small DataFrame across all salt values
df_small_replicated = df_lookup.withColumn(
    "salt_array", F.array([F.lit(i) for i in range(SALT_FACTOR)])
).withColumn(
    "salt", F.explode("salt_array")
).withColumn(
    "join_key", F.concat_ws("_", F.col("user_id"), F.col("salt"))
).drop("salt_array", "salt")

# Join on salted key — skew is now spread across SALT_FACTOR partitions
df_result = df_large_salted.join(df_small_replicated, on="join_key", how="left") \
    .drop("salt", "join_key")
```

---

### Solution 3 — AQE Skew Join + Coalesce Hint

Configure AQE to handle skew dynamically at runtime, plus add a coalesce hint to prevent over-partitioning post-join.

```python
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
# A partition is skewed if it's > 5x the median AND > 256MB
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256MB")
# AQE will coalesce small post-shuffle partitions automatically
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.minPartitionSize", "64MB")

df_result = df_large.join(df_lookup, on="user_id", how="left")

# Validate skew is resolved — check task duration distribution in Spark UI
# Or programmatically:
df_result.explain(mode="formatted")  # Look for "SkewJoin" in the plan
```

---

## Scenario 4: Pipeline Needs Incremental Processing

**Scenario:** A dataset in a data lake grows by 500 GB daily. Reprocessing the entire dataset every day is too expensive.

**Interview Questions:**
- How would you design an incremental processing pipeline in Spark?
- What techniques would you use for efficient updates?

### Root Cause

Full reprocessing at 500 GB/day means scanning terabytes of unchanged historical data on every run. At AWS Glue pricing (~$0.44/DPU-hour), a 2-hour full scan on 10 workers costs ~$8.80/run × 365 = ~$3,200/year just for compute — before S3 data transfer costs.

---

### Solution 1 — Partition-Based Incremental Load (Simplest, Most Common)

Partition data by date at write time. Each day's job reads only the new partition.

```python
from datetime import date, timedelta

# Write with date partitioning (done once at ingestion)
df_raw.write \
    .mode("append") \
    .partitionBy("year", "month", "day") \
    .parquet("s3://bucket/data/events/")

# Incremental read: only yesterday's partition
yesterday = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")
year, month, day = yesterday.split("-")

df_incremental = spark.read.parquet(
    f"s3://bucket/data/events/year={year}/month={month}/day={day}/"
)

# Process only new data
df_processed = df_incremental \
    .filter(F.col("status") == "ACTIVE") \
    .withColumn("processed_at", F.current_timestamp())

# Append results — never touch historical partitions
df_processed.write \
    .mode("append") \
    .partitionBy("year", "month", "day") \
    .parquet("s3://bucket/data/processed/")
```

**In the ETL Orchestrator context:** This maps to the S3 file lifecycle — new CSVs land in `data/in/` daily. The Glue job reads only the new file (not the full history) and merges into Redshift.

---

### Solution 2 — Delta Lake MERGE for Upsert-Based Incremental Loads

When records can be updated (not just appended), use Delta Lake's MERGE to apply only changes.

```python
from delta.tables import DeltaTable
from pyspark.sql import functions as F

# Read only today's changed/new records from S3
df_new_records = spark.read.parquet("s3://bucket/data/in/Products_2024-01-15.parquet")

# The target is the full Delta table (large, but MERGE only touches affected files)
target_table = DeltaTable.forPath(spark, "s3://bucket/delta/products/")

# MERGE: upsert new/changed records, leave unchanged records untouched
target_table.alias("target").merge(
    df_new_records.alias("source"),
    "target.product_id = source.product_id"
) \
.whenMatchedUpdate(
    condition="target.updated_at < source.updated_at",  # Only update if source is newer
    set={
        "product_name": "source.product_name",
        "price": "source.price",
        "updated_at": "source.updated_at"
    }
) \
.whenNotMatchedInsertAll() \
.execute()

# Delta's file pruning means only affected Parquet files are rewritten
# Not the entire 500GB dataset
print(target_table.history(1).select("operationMetrics").first())
```

---

### Solution 3 — Glue Job Bookmarks for Automatic Incremental Tracking

AWS Glue's built-in mechanism that tracks which S3 files have already been processed.

```python
# Enable in Glue job parameters: --job-bookmark-option = job-bookmark-enable
# Glue stores bookmark state in its own metadata store — no manual tracking needed

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
import sys

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
glueContext = GlueContext(spark.sparkContext)
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Glue automatically reads ONLY files not processed in previous runs
# Files already seen by the bookmark are skipped entirely
datasource = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={
        "paths": ["s3://bucket/data/in/"],
        "recurse": True
    },
    format="csv",
    format_options={"withHeader": True},
    transformation_ctx="datasource"   # transformation_ctx is required for bookmarks
)

df_new = datasource.toDF()

# Process and write
df_transformed = df_new.withColumn("load_date", F.current_date())
df_transformed.write.mode("append").parquet("s3://bucket/data/processed/")

# Commit the bookmark — marks these files as processed
job.commit()
```

**Key rule:** `transformation_ctx` must be unique per data source and consistent across runs — it's the key Glue uses to look up the bookmark state.

---

## Scenario 5: Handling Schema Evolution in Data Lake

**Scenario:** A source system adds new columns to a dataset frequently.

**Interview Questions:**
- How would you design a Spark pipeline that supports schema evolution without breaking downstream jobs?

### Root Cause

By default, Spark enforces a fixed schema at read time. A new column added by the source causes `AnalysisException` on the reader. Downstream jobs relying on positional column access (not named access) silently read wrong data.

---

### Solution 1 — Delta Lake Schema Evolution with `mergeSchema`

Delta Lake tracks schema history and can automatically evolve it when new columns arrive.

```python
# Writer side: enable schema merging on write
df_new_with_extra_column.write \
    .format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \  # New columns are added to the Delta schema
    .save("s3://bucket/delta/products/")

# Reader side: always reads current schema, new columns are NULL for old records
df = spark.read.format("delta").load("s3://bucket/delta/products/")
df.printSchema()  # Shows the evolved schema including new columns

# For MERGE operations, schema evolution must be enabled at session level
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

target_table.alias("t").merge(
    df_new.alias("s"), "t.product_id = s.product_id"
) \
.whenMatchedUpdateAll() \
.whenNotMatchedInsertAll() \
.execute()
```

---

### Solution 2 — Explicit Schema Reconciliation (Used in This ETL Orchestrator)

Compare incoming schema against the target schema and apply `ALTER TABLE` for new columns — the same pattern implemented in `glue_job.py`.

```python
from pyspark.sql.types import StringType, LongType, DoubleType

def reconcile_schema(df_source, target_table_name, redshift_conn):
    """
    Detect schema drift between source DataFrame and Redshift target.
    Add missing columns to Redshift. Fill missing source columns with defaults.
    """
    source_cols = set(df_source.columns)

    # Fetch current Redshift schema
    existing_cols_df = spark.read \
        .format("jdbc") \
        .option("url", redshift_conn) \
        .option("query", f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name='{target_table_name}'") \
        .load()
    existing_cols = {row.column_name: row.data_type for row in existing_cols_df.collect()}

    df_reconciled = df_source

    # New columns in source → ALTER TABLE in Redshift
    for col_name in source_cols - set(existing_cols.keys()):
        spark_dtype = df_source.schema[col_name].dataType
        redshift_type = "VARCHAR(65535)" if isinstance(spark_dtype, StringType) \
                   else "BIGINT" if isinstance(spark_dtype, LongType) \
                   else "DOUBLE PRECISION"
        print(f"New column detected: {col_name} ({redshift_type}) — running ALTER TABLE")
        # Execute ALTER TABLE via Redshift Data API (as done in utility procedures)
        execute_redshift_sql(f"ALTER TABLE {target_table_name} ADD COLUMN {col_name} {redshift_type}")

    # Columns in Redshift but missing from source → fill with NULL
    for col_name in set(existing_cols.keys()) - source_cols:
        print(f"Missing column in source: {col_name} — filling with NULL")
        df_reconciled = df_reconciled.withColumn(col_name, F.lit(None))

    return df_reconciled

df_ready = reconcile_schema(df_source, "products", redshift_jdbc_url)
```

---

### Solution 3 — Schema Registry with Spark + Avro

For streaming pipelines, use a Schema Registry to version and enforce schema contracts.

```python
# Read from Kafka with Confluent Schema Registry (Avro encoded)
df_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "broker:9092") \
    .option("subscribe", "products_topic") \
    .load()

# Deserialize using Schema Registry — automatically handles schema versions
from pyspark.sql.avro.functions import from_avro

schema_registry_conf = {"url": "https://schema-registry:8081"}

df_deserialized = df_stream.select(
    from_avro(
        F.col("value"),
        "products_value",       # Subject name in registry
        schema_registry_conf
    ).alias("data")
).select("data.*")

# Schema Registry enforces compatibility rules:
# BACKWARD: new schema can read old data (safe to add optional fields)
# FORWARD: old schema can read new data
# FULL: both directions (strictest)

# If source violates compatibility, deserialization throws before bad data enters pipeline
df_deserialized.writeStream \
    .format("delta") \
    .option("checkpointLocation", "s3://bucket/checkpoints/products/") \
    .option("mergeSchema", "true") \
    .start("s3://bucket/delta/products/")
```

---

## Scenario 6: Broadcast Join Fails in Production

**Scenario:** A small lookup table (20 MB) is supposed to be broadcast during a join with a 3 TB dataset. However, Spark is performing a shuffle join instead.

**Interview Questions:**
- Why might Spark ignore the broadcast strategy?
- How would you force or debug broadcast joins?

### Root Cause

Spark's optimizer uses table statistics to auto-decide join strategy. If statistics are stale, missing, or the table size estimate exceeds `spark.sql.autoBroadcastJoinThreshold` (default 10 MB), Spark falls back to sort-merge join — even if the actual table is small enough to broadcast.

**Common causes in order of likelihood:**
1. Table statistics not computed → Spark estimates size as very large
2. `autoBroadcastJoinThreshold` default is 10 MB but table is 20 MB
3. Table read through a subquery/view — size not propagated to optimizer
4. `spark.sql.adaptive.enabled` is overriding the hint

---

### Solution 1 — Explicit Broadcast Hint (Override the Optimizer)

```python
from pyspark.sql.functions import broadcast

# Force broadcast regardless of size estimate
df_result = df_large.join(
    broadcast(df_lookup),   # Explicit hint — optimizer cannot ignore this
    on="product_id",
    how="left"
)

# Verify broadcast is used in the plan — look for "BroadcastHashJoin"
df_result.explain(mode="formatted")
# Expected output contains:
# == Physical Plan ==
# *(2) BroadcastHashJoin [product_id#1], [product_id#2], ...
#   +- BroadcastExchange HashedRelationBroadcastMode(...)
```

---

### Solution 2 — Raise the Broadcast Threshold + Update Statistics

```python
# Raise the threshold to cover 20 MB table (set to 30 MB for headroom)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", str(30 * 1024 * 1024))  # 30 MB

# If reading from a catalog table, refresh statistics so optimizer knows the true size
spark.sql("ANALYZE TABLE lookup_products COMPUTE STATISTICS")
spark.sql("ANALYZE TABLE lookup_products COMPUTE STATISTICS FOR ALL COLUMNS")

# Now the optimizer has accurate size info and will auto-broadcast
df_result = df_large.join(df_lookup, on="product_id", how="left")
df_result.explain()  # Should now show BroadcastHashJoin
```

**In AWS Glue / Redshift context:** If the lookup is read from Redshift, Glue may not know the row count. Materialise the lookup to a temp Parquet file first, then broadcast.

```python
# Materialise small table to avoid "unknown size" from JDBC source
df_lookup.write.mode("overwrite").parquet("s3://bucket/tmp/lookup_cache/")
df_lookup_cached = spark.read.parquet("s3://bucket/tmp/lookup_cache/")

df_result = df_large.join(broadcast(df_lookup_cached), on="product_id", how="left")
```

---

### Solution 3 — Debug with AQE + Fallback Diagnostics

```python
# Enable AQE with broadcast coercion — AQE can promote to broadcast at runtime
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.localShuffleReader.enabled", "true")

# If AQE still chooses sort-merge, check the runtime plan
df_result = df_large.join(df_lookup, on="product_id", how="left")

# Print AQE-adjusted plan (runtime, not compile-time)
df_result.explain(mode="cost")

# Check estimated sizes programmatically
from pyspark.sql.functions import broadcast
import json

# Inspect the optimizer stats
stats = spark.sessionState.executePlan(
    df_lookup._jdf.queryExecution().analyzed()
).optimizedPlan().stats()
print(f"Optimizer estimated size: {stats.sizeInBytes()} bytes")
# If this shows Long.MaxValue, statistics are missing → use Solution 1 or 2
```

---

## Scenario 7: Spark Streaming Job Restart Causes Data Loss

**Scenario:** After restarting a streaming job, some events from Kafka are missing.

**Interview Questions:**
- What could cause data loss in a Spark streaming pipeline?
- How would you design fault-tolerant streaming pipelines?

### Root Cause

Three most common causes of data loss on restart:

1. **No checkpoint configured** — Spark has no record of the last committed Kafka offset. On restart it defaults to `latest`, skipping events published during the downtime.
2. **Checkpoint deleted or corrupted** — S3 checkpoint path was overwritten or cleaned up.
3. **Kafka topic retention expired** — Events were published but Kafka deleted them (default 7-day retention) before the restarted job could read them.

---

### Solution 1 — Checkpoint + Explicit Kafka Offset Management (Correct Foundation)

```python
query = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "broker:9092") \
    .option("subscribe", "etl_events") \
    .option("startingOffsets", "earliest")  \  # On FIRST run only; checkpoint takes over after
    .option("failOnDataLoss", "true") \         # Fail rather than silently skip missing offsets
    .load() \
    .select(F.from_json(F.col("value").cast("string"), schema).alias("data")) \
    .select("data.*") \
    .writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "s3://bucket/checkpoints/etl_stream/") \  # NEVER delete this
    .trigger(processingTime="30 seconds") \
    .start("s3://bucket/delta/events/")
```

**Critical rule:** The checkpoint path stores the exact Kafka partition offsets. On restart, Spark reads from exactly where it left off. If you delete the checkpoint, you lose offset history.

---

### Solution 2 — Idempotent Writes with Exactly-Once Semantics

Even with a checkpoint, a failure between "data written" and "offset committed" can cause reprocessing. Make writes idempotent so reprocessing a batch produces the same result.

```python
# Use Delta Lake's idempotent write with a unique batch ID
def write_batch_idempotent(batch_df, batch_id):
    """
    foreachBatch handler — exactly-once via Delta MERGE on event_id.
    Reprocessing the same batch_id is safe: MERGE deduplicates on event_id.
    """
    if batch_df.isEmpty():
        return

    target = DeltaTable.forPath(spark, "s3://bucket/delta/events/")

    target.alias("t").merge(
        batch_df.alias("s"),
        "t.event_id = s.event_id"   # Natural dedup key
    ) \
    .whenNotMatchedInsertAll() \     # Only insert if not already present
    .execute()

    print(f"Batch {batch_id} written idempotently. Rows: {batch_df.count()}")

query = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "broker:9092") \
    .option("subscribe", "etl_events") \
    .option("failOnDataLoss", "true") \
    .load() \
    .select(F.from_json(F.col("value").cast("string"), schema).alias("data")) \
    .select("data.*") \
    .writeStream \
    .foreachBatch(write_batch_idempotent) \
    .option("checkpointLocation", "s3://bucket/checkpoints/etl_stream_v2/") \
    .trigger(processingTime="60 seconds") \
    .start()
```

---

### Solution 3 — Versioned Checkpoint Strategy for Safe Job Updates

When you need to change the streaming logic (schema change, new aggregation), the old checkpoint becomes incompatible. Design a versioned checkpoint rotation strategy.

```python
import os
from datetime import datetime

def get_checkpoint_path(job_version: str) -> str:
    """Version checkpoints so schema changes don't corrupt state."""
    return f"s3://bucket/checkpoints/etl_stream/v{job_version}/"

JOB_VERSION = "3"  # Bump when streaming logic changes incompatibly
CHECKPOINT_PATH = get_checkpoint_path(JOB_VERSION)

# On a new version, the checkpoint doesn't exist yet → job starts from "earliest"
# Previous version's checkpoint is preserved for audit/rollback

query = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "broker:9092")
    .option("subscribe", "etl_events")
    .option("startingOffsets", "earliest")   # Catches up from beginning on new version
    .option("failOnDataLoss", "true")
    .load()
    .writeStream
    .foreachBatch(write_batch_idempotent)    # Idempotent = safe to replay from earliest
    .option("checkpointLocation", CHECKPOINT_PATH)
    .trigger(processingTime="60 seconds")
    .start()
)

print(f"Streaming query started with checkpoint: {CHECKPOINT_PATH}")
query.awaitTermination()
```

---

## Scenario 8: Join Between Tables With Different Partitioning

**Scenario:** Two datasets have different partition strategies, and joining them causes heavy shuffle.

**Interview Questions:**
- How can you optimize joins between differently partitioned datasets?

### Root Cause

When `df_orders` is partitioned by `order_date` and `df_customers` is partitioned by `region`, Spark cannot co-locate matching records on the same executor. Every join requires a full shuffle of both datasets across the network — the most expensive operation in distributed processing.

---

### Solution 1 — Bucket Both Tables on the Join Key (Best for Repeated Joins)

Pre-bucket data at write time on the join key. Spark detects matching buckets and skips the shuffle entirely.

```python
# Write both tables bucketed on the same join key with the same number of buckets
# Do this ONCE at ingestion time — pays off on every subsequent join

df_orders.write \
    .bucketBy(200, "customer_id") \   # 200 buckets, joined on customer_id
    .sortBy("customer_id") \
    .mode("overwrite") \
    .saveAsTable("orders_bucketed")   # Must be a Hive-managed or catalog table

df_customers.write \
    .bucketBy(200, "customer_id") \   # SAME number of buckets, SAME key
    .sortBy("customer_id") \
    .mode("overwrite") \
    .saveAsTable("customers_bucketed")

# Join: Spark skips the shuffle because matching keys are already co-located
df_orders_b = spark.table("orders_bucketed")
df_customers_b = spark.table("customers_bucketed")
df_result = df_orders_b.join(df_customers_b, on="customer_id", how="left")

df_result.explain()
# Expected: No Exchange/Shuffle operator in plan
# == Physical Plan ==
# *(3) SortMergeJoin [customer_id#1], [customer_id#2], LeftOuter
#   *(1) Sort [customer_id#1 ASC] ... (no Exchange above this)
```

---

### Solution 2 — Repartition Both DataFrames Before Join

When buckets are not available, repartition both datasets to the same partitioning scheme before the join — consolidating two shuffles into one.

```python
NUM_PARTITIONS = 400  # Rule of thumb: partition size ~ 128-256 MB each

# Repartition both DataFrames on the join key
# This causes ONE shuffle per DataFrame, but the join itself requires no additional shuffle
df_orders_repartitioned = df_orders.repartition(NUM_PARTITIONS, "customer_id")
df_customers_repartitioned = df_customers.repartition(NUM_PARTITIONS, "customer_id")

# Cache the repartitioned DataFrames if used multiple times in the same job
df_orders_repartitioned.cache()
df_customers_repartitioned.cache()

# Trigger materialization
df_orders_repartitioned.count()
df_customers_repartitioned.count()

df_result = df_orders_repartitioned.join(
    df_customers_repartitioned,
    on="customer_id",
    how="left"
)

# Optional: persist result for downstream reuse
df_result.write \
    .partitionBy("order_date") \
    .mode("overwrite") \
    .parquet("s3://bucket/data/orders_enriched/")
```

---

### Solution 3 — Partition Pruning + Selective Repartition (For Date-Scoped Joins)

When joins are always scoped to a time window, push the date filter before the repartition to minimise shuffled data volume.

```python
from datetime import date, timedelta

# Process only last 7 days (incremental pattern from Scenario 4)
start_date = date.today() - timedelta(days=7)

# Push filters BEFORE repartition — prune data early
df_orders_filtered = spark.read.parquet("s3://bucket/data/orders/") \
    .filter(F.col("order_date") >= F.lit(str(start_date)))  # Partition pruning

df_customers_filtered = spark.read.parquet("s3://bucket/data/customers/") \
    .filter(F.col("is_active") == True)  # Reduces customer dataset before shuffle

# Only repartition the pruned, smaller DataFrames
df_orders_r = df_orders_filtered.repartition(200, "customer_id")
df_customers_r = df_customers_filtered.repartition(200, "customer_id")

df_result = df_orders_r.join(df_customers_r, on="customer_id", how="left")

# Validate partition efficiency
print(f"Orders partitions: {df_orders_r.rdd.getNumPartitions()}")
print(f"Customers partitions: {df_customers_r.rdd.getNumPartitions()}")

# Check for residual shuffle in plan
df_result.explain(mode="formatted")
```

---

## Quick Reference: Scenario → Recommended Solution

| Scenario | Primary Fix | Spark Feature | Fallback |
|---|---|---|---|
| One straggler task | AQE skew join | `spark.sql.adaptive.skewJoin.enabled` | Salting |
| Late streaming data | Watermark + append mode | `withWatermark()` | Lambda architecture |
| Skewed join | Hot-key broadcast split | `broadcast()` hint | Salting + AQE |
| Incremental processing | Delta MERGE / Glue bookmarks | `DeltaTable.merge()` | Partition-based reads |
| Schema evolution | Delta `mergeSchema` | `.option("mergeSchema","true")` | Explicit ALTER TABLE |
| Broadcast join ignored | Explicit `broadcast()` hint | `broadcast(df)` | Raise threshold + ANALYZE |
| Streaming data loss | Checkpoint + `failOnDataLoss` | `checkpointLocation` | Idempotent foreachBatch |
| Partition mismatch join | Bucket on join key | `bucketBy()` | Repartition before join |
