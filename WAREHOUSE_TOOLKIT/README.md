# Incremental Data Loading Patterns with PySpark

A comprehensive implementation of four common incremental data loading patterns used in data warehousing.

## Overview

This module provides production-ready implementations for:

1. **Timestamp-based Loading** - Track changes using modified/created timestamps
2. **Change Data Capture (CDC)** - Process INSERT/UPDATE/DELETE operations from change streams
3. **Delta/Diff Detection** - Compare snapshots using hash-based change detection
4. **Sequence Number Loading** - Track progress using auto-incrementing IDs

## Installation

```bash
# Requires PySpark
pip install pyspark
```

## Quick Start

```python
from pyspark.sql import SparkSession
from incremental_loading import TimestampBasedLoader

# Initialize Spark
spark = SparkSession.builder \
    .appName("Incremental Loading") \
    .getOrCreate()

# Create loader
loader = TimestampBasedLoader(spark)

# Load incremental data
result = loader.load_incremental(
    source_path="s3://bucket/source_data",
    target_path="s3://bucket/target_data",
    watermark_path="s3://bucket/watermarks",
    timestamp_column="modified_date"
)
```

## Pattern Details

### 1. Timestamp-Based Loading

**Best for:** Systems with reliable timestamp columns (modified_date, created_date)

**How it works:**
- Maintains a watermark (last processed timestamp)
- Loads only records with timestamps > watermark
- Updates watermark after successful load

**Usage:**
```python
from incremental_loading import TimestampBasedLoader

loader = TimestampBasedLoader(spark)
result = loader.load_incremental(
    source_path="/path/to/source",
    target_path="/path/to/target",
    watermark_path="/path/to/watermarks",
    timestamp_column="modified_date",
    table_name="my_table"
)
```

**Pros:**
- Simple and efficient
- Low overhead
- Works with most source systems

**Cons:**
- Requires reliable timestamp columns
- Doesn't detect deletes
- Clock skew can cause issues

---

### 2. Change Data Capture (CDC)

**Best for:** Systems with CDC capabilities (database logs, triggers, CDC tools)

**How it works:**
- Processes change events (INSERT, UPDATE, DELETE)
- Applies changes in sequence order
- Handles deduplication for same keys

**Expected CDC data format:**
```python
# Columns: id, name, age, operation, sequence_num
# operation: INSERT, UPDATE, or DELETE
# sequence_num: Monotonically increasing
```

**Usage:**
```python
from incremental_loading import CDCLoader

loader = CDCLoader(spark)
result = loader.process_cdc(
    cdc_source_path="/path/to/cdc_stream",
    target_path="/path/to/target",
    watermark_path="/path/to/watermarks",
    primary_key="id",
    table_name="my_table"
)
```

**Pros:**
- Captures all changes (including deletes)
- Very accurate
- Near real-time capability

**Cons:**
- Requires CDC infrastructure
- More complex setup
- Higher resource usage

---

### 3. Delta/Diff Detection

**Best for:** Systems without timestamps or CDC, batch processing

**How it works:**
- Creates hash for each row
- Compares current source with last snapshot
- Identifies inserts, updates, and deletes based on hash differences

**Usage:**
```python
from incremental_loading import DeltaDiffLoader

loader = DeltaDiffLoader(spark)
changes = loader.detect_changes(
    source_path="/path/to/current_source",
    target_path="/path/to/target",
    snapshot_path="/path/to/snapshots",
    primary_key="id",
    table_name="my_table"
)

# Access different change types
inserts = changes["inserts"]
updates = changes["updates"]
deletes = changes["deletes"]
```

**Pros:**
- No dependency on timestamps
- Detects all change types
- Works with any source

**Cons:**
- Resource intensive (full comparison)
- Requires storing snapshots
- Slower for large datasets

---

### 4. Sequence Number Based Loading

**Best for:** Event streams, log data, systems with monotonic IDs

**How it works:**
- Tracks highest processed sequence number
- Loads records with sequence > watermark
- Guarantees ordering

**Usage:**
```python
from incremental_loading import SequenceBasedLoader

loader = SequenceBasedLoader(spark)
result = loader.load_incremental(
    source_path="/path/to/source",
    target_path="/path/to/target",
    watermark_path="/path/to/watermarks",
    sequence_column="sequence_id",
    table_name="my_table"
)
```

**Pros:**
- Guaranteed ordering
- No clock skew issues
- Efficient for event streams

**Cons:**
- Requires sequence column
- Doesn't detect updates (only appends)
- Gap handling needed

## Watermark Management

All loaders use a watermark system to track progress:

```python
# Watermark stored as parquet with structure:
# table_name | watermark_value | last_updated
# my_table   | 2024-01-15      | 2024-01-15 10:30:00
```

Watermarks are automatically:
- Created on first run
- Updated after successful loads
- Persisted for recovery

## Error Handling and Best Practices

### 1. Idempotency
All loaders are designed to be idempotent - running the same load multiple times produces the same result.

### 2. Transactional Safety
```python
# Always wrap in try-except for production
try:
    result = loader.load_incremental(...)
    # Only update watermark if successful
except Exception as e:
    print(f"Load failed: {e}")
    # Watermark not updated, can retry
```

### 3. Data Quality Checks
```python
# Add validation before loading
if incremental_df.count() > expected_max:
    raise ValueError("Unexpected data volume")

# Check for nulls in critical columns
if incremental_df.filter(F.col("id").isNull()).count() > 0:
    raise ValueError("Null IDs detected")
```

### 4. Monitoring
```python
# Log metrics for monitoring
print(f"Records loaded: {incremental_df.count()}")
print(f"Load duration: {end_time - start_time}")
print(f"New watermark: {new_watermark}")
```

## Production Deployment

### Using with Databricks
```python
# Mount S3 bucket
dbutils.fs.mount(
    source="s3a://bucket",
    mount_point="/mnt/data"
)

# Run loader
loader = TimestampBasedLoader(spark)
loader.load_incremental(
    source_path="/mnt/data/source",
    target_path="/mnt/data/target",
    watermark_path="/mnt/data/watermarks"
)
```

### Using with AWS EMR
```python
spark = SparkSession.builder \
    .appName("Incremental Load") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()
```

### Using with Airflow
```python
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

load_task = SparkSubmitOperator(
    task_id='incremental_load',
    application='/path/to/incremental_loading.py',
    conf={
        'spark.executor.memory': '4g',
        'spark.driver.memory': '2g'
    }
)
```

## Performance Optimization

### 1. Partitioning
```python
# Write with partitioning for better query performance
incremental_df.write \
    .partitionBy("year", "month") \
    .mode("append") \
    .parquet(target_path)
```

### 2. Caching
```python
# Cache large datasets used multiple times
source_df.cache()
changes = detect_changes(...)
source_df.unpersist()
```

### 3. Broadcast Joins
```python
# For small lookup tables
broadcast(watermark_df).join(source_df, ...)
```

## Testing

Run the included test suite:
```bash
python incremental_loading.py
```

This will:
1. Create sample data
2. Test all four patterns
3. Display results
4. Clean up test data

## Common Issues and Solutions

### Issue: Duplicate records
**Solution:** Ensure primary key uniqueness, use window functions to deduplicate

### Issue: Missing data
**Solution:** Check watermark values, validate timestamp columns are timezone-aware

### Issue: Performance degradation
**Solution:** Add partitioning, increase Spark resources, consider bucketing

### Issue: Late-arriving data
**Solution:** Implement lookback window in timestamp queries:
```python
# Load with 1-hour lookback
incremental_df = source_df.filter(
    F.col(timestamp_column) > (last_watermark - timedelta(hours=1))
)
```

## Advanced Usage

### Combining Multiple Patterns
```python
# Use CDC for transactional data, timestamp for logs
cdc_loader = CDCLoader(spark)
timestamp_loader = TimestampBasedLoader(spark)

# Load transactions via CDC
cdc_loader.process_cdc(...)

# Load logs via timestamp
timestamp_loader.load_incremental(...)
```

### Custom Watermark Logic
```python
class CustomLoader(IncrementalLoader):
    def custom_watermark_logic(self):
        # Implement your own watermark strategy
        pass
```

## License

MIT License - feel free to use in your projects!

## Contributing

Contributions welcome! Please submit issues and pull requests.

## Support

For questions or issues, please open a GitHub issue.
