# Historical Data Backfilling with PySpark

Comprehensive guide and implementation for efficiently loading historical data into data warehouses.

## Overview

Backfilling is the process of loading historical data into a data warehouse. This module provides production-ready patterns for:

1. **Sequential Time-based Backfill** - Process historical data in time chunks
2. **Parallel Backfill** - Leverage distributed processing for faster loads
3. **Sequence-based Backfill** - Use sequence numbers for ordered data
4. **Full Historical Load** - One-time bulk load with validation
5. **Backfill Validation** - Ensure data completeness and accuracy

## Why Backfilling Matters

Common scenarios requiring backfilling:
- **Data warehouse migration** - Moving from legacy systems
- **New analytics platform** - Initializing historical data
- **Data recovery** - Restoring lost or corrupted data
- **System consolidation** - Merging multiple data sources
- **Regulatory compliance** - Loading historical records for audits

## Quick Start

```python
from pyspark.sql import SparkSession
from backfill_historical import TimestampBackfill

spark = SparkSession.builder.appName("Backfill").getOrCreate()

# Create backfill manager
backfill = TimestampBackfill(spark)

# Backfill 1 year of data in weekly chunks
stats = backfill.backfill_by_time_range(
    source_path="s3://bucket/source",
    target_path="s3://bucket/warehouse",
    state_path="s3://bucket/state",
    start_date="2023-01-01",
    end_date="2024-01-01",
    timestamp_column="created_date",
    interval_days=7
)
```

## Detailed Pattern Guide

### 1. Sequential Time-based Backfill

**Best for:** 
- Large historical periods (years of data)
- When you need checkpoint/resume capability
- Limited memory/compute resources

**How it works:**
- Divides time period into manageable chunks
- Processes each chunk sequentially
- Saves checkpoint after each chunk
- Can resume if interrupted

**Usage:**
```python
from backfill_historical import TimestampBackfill

backfill = TimestampBackfill(spark)

stats = backfill.backfill_by_time_range(
    source_path="/path/to/source",
    target_path="/path/to/target",
    state_path="/path/to/checkpoints",
    start_date="2019-01-01",
    end_date="2024-01-01",
    timestamp_column="transaction_date",
    interval_days=7,           # Process 1 week at a time
    backfill_id="my_backfill", # Unique identifier
    resume=True                # Resume from checkpoint
)
```

**Configuration Guidelines:**

| Data Volume | Recommended Interval | Expected Duration |
|-------------|---------------------|-------------------|
| < 1 GB/day | 30 days | Minutes to hours |
| 1-10 GB/day | 7 days | Hours |
| 10-100 GB/day | 1 day | Hours to days |
| > 100 GB/day | 12 hours or partition | Days |

**Pros:**
- Checkpoint/resume capability
- Predictable resource usage
- Easy to monitor progress
- Can handle very large datasets

**Cons:**
- Slower than parallel approaches
- Sequential processing only

---

### 2. Parallel Backfill

**Best for:**
- Time-sensitive migrations
- Abundant compute resources
- Data that can be partitioned

**How it works:**
- Leverages Spark's distributed processing
- Processes multiple partitions simultaneously
- No checkpointing (all-or-nothing)

**Usage:**
```python
from backfill_historical import ParallelBackfill

backfill = ParallelBackfill(spark)

stats = backfill.backfill_parallel(
    source_path="/path/to/source",
    target_path="/path/to/target",
    start_date="2023-01-01",
    end_date="2024-01-01",
    timestamp_column="event_date",
    num_partitions=20,              # Number of parallel tasks
    partition_column="customer_id"  # Optional: partition by column
)
```

**Performance Tuning:**

```python
# Increase parallelism for large datasets
spark.conf.set("spark.sql.shuffle.partitions", "200")
spark.conf.set("spark.default.parallelism", "100")

# Tune executor resources
spark.conf.set("spark.executor.memory", "8g")
spark.conf.set("spark.executor.cores", "4")
```

**Pros:**
- Much faster than sequential
- Scales with cluster size
- Efficient resource utilization

**Cons:**
- No checkpointing
- Requires more resources
- All-or-nothing approach

---

### 3. Sequence-based Backfill

**Best for:**
- Event logs with sequence numbers
- Append-only data
- Message queues or streaming data

**How it works:**
- Tracks highest processed sequence number
- Processes in batches
- Maintains ordering

**Usage:**
```python
from backfill_historical import SequenceBackfill

backfill = SequenceBackfill(spark)

stats = backfill.backfill_by_sequence_range(
    source_path="/path/to/source",
    target_path="/path/to/target",
    state_path="/path/to/checkpoints",
    start_sequence=1,
    end_sequence=10000000,
    sequence_column="event_id",
    batch_size=100000,
    backfill_id="event_backfill"
)
```

**Pros:**
- Guaranteed ordering
- No timestamp dependencies
- Efficient for event data

**Cons:**
- Requires sequence column
- Sequential processing
- Gap handling needed

---

### 4. Full Historical Load with Validation

**Best for:**
- One-time initial loads
- Data quality concerns
- Need for deduplication

**How it works:**
- Loads entire dataset at once
- Applies validation rules
- Deduplicates data
- Partitions output

**Usage:**
```python
from backfill_historical import FullHistoricalLoad

# Define validation rules
validation_rules = {
    'remove_nulls': lambda df: df.filter(F.col("id").isNotNull()),
    'valid_dates': lambda df: df.filter(F.col("date") >= F.lit("2020-01-01")),
    'positive_amounts': lambda df: df.filter(F.col("amount") > 0)
}

backfill = FullHistoricalLoad(spark)

stats = backfill.full_load_with_validation(
    source_path="/path/to/source",
    target_path="/path/to/target",
    validation_rules=validation_rules,
    dedup_column="transaction_id",
    partition_by=["year", "month"]
)
```

**Common Validation Rules:**

```python
validation_rules = {
    # Remove null primary keys
    'no_null_ids': lambda df: df.filter(F.col("id").isNotNull()),
    
    # Date range validation
    'valid_date_range': lambda df: df.filter(
        (F.col("date") >= F.lit("2020-01-01")) & 
        (F.col("date") <= F.current_date())
    ),
    
    # Numeric range validation
    'positive_amounts': lambda df: df.filter(F.col("amount") > 0),
    
    # String validation
    'valid_emails': lambda df: df.filter(F.col("email").contains("@")),
    
    # Referential integrity
    'valid_status': lambda df: df.filter(
        F.col("status").isin(['active', 'inactive', 'pending'])
    )
}
```

**Pros:**
- Data quality assurance
- Automatic deduplication
- Optimized partitioning

**Cons:**
- No checkpointing
- Memory intensive
- Slower validation

---

### 5. Backfill Validation

**Best for:**
- Verifying backfill completeness
- Reconciliation with source
- Audit requirements

**How it works:**
- Compares source vs target counts
- Validates by groups/partitions
- Identifies mismatches

**Usage:**
```python
from backfill_historical import IncrementalBackfillValidator

validator = IncrementalBackfillValidator(spark)

results = validator.validate_backfill(
    source_path="/path/to/source",
    target_path="/path/to/target",
    date_column="created_date",
    start_date="2023-01-01",
    end_date="2024-01-01",
    group_by_columns=["region", "product_category"]
)

if results['is_valid']:
    print("Backfill validation PASSED ✓")
else:
    print(f"Validation FAILED - Missing {results['difference']} records")
```

**Validation Levels:**

```python
# Basic count validation
validator.validate_backfill(
    source_path="...",
    target_path="...",
    date_column="date",
    start_date="2023-01-01",
    end_date="2024-01-01"
)

# Detailed validation by dimensions
validator.validate_backfill(
    source_path="...",
    target_path="...",
    date_column="date",
    start_date="2023-01-01",
    end_date="2024-01-01",
    group_by_columns=["country", "product_line", "customer_segment"]
)
```

---

## Real-World Scenarios

### Scenario 1: Data Warehouse Migration

```python
# Migrate 5 years of customer data
backfill = TimestampBackfill(spark)

for year in range(2019, 2024):
    stats = backfill.backfill_by_time_range(
        source_path=f"s3://old-warehouse/customers",
        target_path=f"s3://new-warehouse/customers",
        state_path=f"s3://checkpoints/migration",
        start_date=f"{year}-01-01",
        end_date=f"{year+1}-01-01",
        timestamp_column="created_date",
        interval_days=30,
        backfill_id=f"customer_migration_{year}"
    )
```

### Scenario 2: High-Volume IoT Data

```python
# Backfill 1 billion sensor readings
backfill = ParallelBackfill(spark)

stats = backfill.backfill_parallel(
    source_path="s3://iot-data/sensors",
    target_path="s3://warehouse/sensors",
    start_date="2023-01-01",
    end_date="2024-01-01",
    timestamp_column="reading_time",
    num_partitions=100,
    partition_column="sensor_id"
)
```

### Scenario 3: Financial Transactions with Validation

```python
# Backfill financial data with strict validation
validation_rules = {
    'no_nulls': lambda df: df.filter(
        F.col("transaction_id").isNotNull() &
        F.col("amount").isNotNull()
    ),
    'positive_amounts': lambda df: df.filter(F.col("amount") > 0),
    'valid_status': lambda df: df.filter(
        F.col("status").isin(['completed', 'pending', 'failed'])
    )
}

backfill = FullHistoricalLoad(spark)

stats = backfill.full_load_with_validation(
    source_path="s3://transactions/raw",
    target_path="s3://warehouse/transactions",
    validation_rules=validation_rules,
    dedup_column="transaction_id",
    partition_by=["year", "month", "day"]
)
```

## Best Practices

### 1. Resource Planning

**Memory Sizing:**
```python
# Calculate required memory
records_per_day = 1_000_000
avg_record_size_kb = 2
days_per_chunk = 7

memory_needed_gb = (records_per_day * days_per_chunk * avg_record_size_kb) / 1_000_000
executor_memory = memory_needed_gb * 1.5  # Add 50% buffer

spark.conf.set("spark.executor.memory", f"{int(executor_memory)}g")
```

**Partition Sizing:**
```python
# Optimal partition size: 100-200 MB
total_data_size_gb = 1000
target_partition_size_mb = 128

num_partitions = (total_data_size_gb * 1024) / target_partition_size_mb
spark.conf.set("spark.sql.shuffle.partitions", str(int(num_partitions)))
```

### 2. Monitoring Progress

```python
# Add custom monitoring
class MonitoredBackfill(TimestampBackfill):
    def log_progress(self, message, metrics=None):
        super().log_progress(message, metrics)
        
        # Send to monitoring system
        send_to_datadog(message, metrics)
        send_to_cloudwatch(message, metrics)
```

### 3. Error Handling

```python
# Implement retry logic
max_retries = 3
for attempt in range(max_retries):
    try:
        stats = backfill.backfill_by_time_range(...)
        break
    except Exception as e:
        if attempt == max_retries - 1:
            raise
        print(f"Attempt {attempt + 1} failed, retrying...")
        time.sleep(60 * (attempt + 1))  # Exponential backoff
```

### 4. Data Quality

```python
# Add quality checks after backfill
def quality_checks(df: DataFrame):
    # Check for nulls
    null_count = df.filter(F.col("id").isNull()).count()
    assert null_count == 0, f"Found {null_count} null IDs"
    
    # Check date range
    min_date = df.agg(F.min("date")).first()[0]
    max_date = df.agg(F.max("date")).first()[0]
    assert min_date >= expected_start_date
    assert max_date <= expected_end_date
    
    # Check row count
    actual_count = df.count()
    assert abs(actual_count - expected_count) < 1000
```

## Performance Optimization

### 1. Increase Parallelism
```python
spark.conf.set("spark.sql.shuffle.partitions", "200")
spark.conf.set("spark.default.parallelism", "100")
```

### 2. Use Columnar Formats
```python
# Parquet is much faster than CSV/JSON
df.write.parquet("path")  # Preferred
# vs
df.write.csv("path")  # Avoid for large data
```

### 3. Optimize File Sizes
```python
# Coalesce to optimal file sizes (128MB-1GB)
df.repartition(num_files).write.parquet("path")
```

### 4. Broadcast Small Lookups
```python
# Broadcast dimension tables
from pyspark.sql.functions import broadcast
fact_df.join(broadcast(dim_df), "key")
```

### 5. Cache Intermediate Results
```python
# Cache data used multiple times
source_df = spark.read.parquet("source")
source_df.cache()

# Use cached data
result1 = source_df.filter(...)
result2 = source_df.groupBy(...)

source_df.unpersist()  # Clean up
```

## Troubleshooting

### Issue: Out of Memory Errors
**Solution:** Reduce chunk size or increase executor memory
```python
# Reduce interval_days
interval_days=1  # Instead of 7

# Or increase memory
spark.conf.set("spark.executor.memory", "16g")
```

### Issue: Slow Performance
**Solution:** Increase partitions or use parallel backfill
```python
# Increase partitions
num_partitions=50  # Instead of 10

# Or switch to parallel backfill
backfill = ParallelBackfill(spark)
```

### Issue: Checkpoint Corruption
**Solution:** Use different backfill_id and restart
```python
backfill_id="my_backfill_v2",  # New ID
resume=False  # Start fresh
```

### Issue: Data Skew
**Solution:** Repartition by high-cardinality column
```python
df.repartition(100, "user_id").write.parquet("path")
```

## Production Deployment

### AWS EMR
```python
spark = SparkSession.builder \
    .appName("Production Backfill") \
    .config("spark.executor.instances", "20") \
    .config("spark.executor.cores", "4") \
    .config("spark.executor.memory", "16g") \
    .config("spark.driver.memory", "8g") \
    .getOrCreate()
```

### Databricks
```python
# Use cluster configuration
# Cluster: 10 workers, i3.2xlarge
# Enable auto-scaling for cost optimization

spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
```

### Airflow DAG
```python
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

backfill_task = SparkSubmitOperator(
    task_id='backfill_historical_data',
    application='/path/to/backfill_historical.py',
    conf={
        'spark.executor.memory': '16g',
        'spark.executor.cores': '4',
        'spark.executor.instances': '20'
    },
    application_args=[
        '--start-date', '2023-01-01',
        '--end-date', '2024-01-01',
        '--interval-days', '7'
    ]
)
```

## Testing

Run included tests:
```bash
# Run main backfill tests
python backfill_historical.py

# Run practical scenarios
python backfill_examples.py
```

## License

MIT License

## Support

For issues or questions, please open a GitHub issue.
