# Schema Evolution & Data Quality Framework

Comprehensive PySpark modules for managing schema changes and ensuring data quality at scale.

## Overview

This toolkit provides two critical capabilities for data warehouses:

1. **Schema Evolution Management** - Handle schema changes between source systems and Redshift
2. **Data Quality Framework** - Comprehensive quality checks optimized for huge data volumes

## Table of Contents

- [Schema Evolution](#schema-evolution)
  - [Features](#schema-evolution-features)
  - [Quick Start](#schema-evolution-quick-start)
  - [Evolution Strategies](#evolution-strategies)
- [Data Quality](#data-quality)
  - [Quality Dimensions](#quality-dimensions)
  - [Quick Start](#data-quality-quick-start)
  - [Optimization for Large Data](#optimization-for-large-data)
- [Best Practices](#best-practices)
- [Examples](#examples)

---

## Schema Evolution

### Schema Evolution Features

✅ **Compare schemas** between source DataFrames and Redshift tables  
✅ **Detect changes**: added/dropped columns, type changes, nullable modifications  
✅ **Generate DDL** for schema migrations  
✅ **Multiple strategies**: STRICT, ADD_ONLY, FLEXIBLE, PERMISSIVE  
✅ **DataFrame alignment** to match target schemas  
✅ **JDBC and Data API** support for Redshift  

### Schema Evolution Quick Start

```python
from pyspark.sql import SparkSession
from schema_evolution import SchemaEvolutionManager, EvolutionStrategy

spark = SparkSession.builder.appName("SchemaEvolution").getOrCreate()

# Initialize manager
manager = SchemaEvolutionManager(spark, redshift_config={
    'host': 'cluster.region.redshift.amazonaws.com',
    'database': 'mydb',
    'user': 'username',
    'password': 'password',
    'port': 5439
})

# Compare schemas
schema_diff = manager.compare_schemas(
    source_df=my_dataframe,
    target_table='customers',
    use_simulated=False  # Use real Redshift connection
)

# Apply evolution with FLEXIBLE strategy
result = manager.apply_schema_evolution(
    source_df=my_dataframe,
    target_table='customers',
    strategy=EvolutionStrategy.FLEXIBLE,
    dry_run=False  # Set True to only generate DDL
)

if result['success']:
    print("Schema evolution succeeded!")
    for ddl in result['ddl_statements']:
        print(ddl)
```

### Evolution Strategies

#### 1. STRICT
**Use when:** Production tables where any schema change requires review

```python
strategy=EvolutionStrategy.STRICT
```

**Behavior:**
- ❌ Fails on ANY schema change
- ✅ Only succeeds if schemas match exactly
- 🎯 Best for: Critical production tables, regulatory compliance

---

#### 2. ADD_ONLY
**Use when:** You can add columns but never remove or modify them

```python
strategy=EvolutionStrategy.ADD_ONLY
```

**Behavior:**
- ✅ Allows adding new columns
- ❌ Fails on dropped columns
- ❌ Fails on type changes
- 🎯 Best for: Append-only analytics, backward compatibility

---

#### 3. FLEXIBLE (Recommended)
**Use when:** Safe evolution is acceptable

```python
strategy=EvolutionStrategy.FLEXIBLE
```

**Behavior:**
- ✅ Allows adding new columns
- ✅ Allows compatible type changes (int → bigint, float → double)
- ❌ Fails on dropped columns
- ❌ Fails on incompatible type changes
- 🎯 Best for: Most production scenarios

**Compatible Type Changes:**
```
IntegerType  → BIGINT, DECIMAL
LongType     → DECIMAL
FloatType    → DOUBLE PRECISION, DECIMAL
StringType   → VARCHAR, TEXT
```

---

#### 4. PERMISSIVE
**Use when:** Development/testing, full flexibility needed

```python
strategy=EvolutionStrategy.PERMISSIVE
```

**Behavior:**
- ✅ Allows ALL changes
- ✅ Adds columns
- ✅ Drops columns
- ✅ Changes types
- 🎯 Best for: Development, testing, data recovery

---

### Schema Evolution Examples

#### Example 1: New Columns Added

```python
# Original schema: id, name, email
# New schema: id, name, email, phone, address

manager.apply_schema_evolution(
    source_df=new_customer_df,
    target_table='customers',
    strategy=EvolutionStrategy.FLEXIBLE
)

# Generated DDL:
# ALTER TABLE customers ADD COLUMN phone VARCHAR(65535) NULL;
# ALTER TABLE customers ADD COLUMN address VARCHAR(65535) NULL;
```

#### Example 2: Align DataFrame to Existing Schema

```python
# Source has: id, name, email
# Target needs: id, name, email, created_date, updated_date

aligned_df = manager.align_dataframe_to_target(
    source_df=source_df,
    target_table='customers'
)

# Result: DataFrame with NULL values for created_date, updated_date
```

#### Example 3: Validate Before Loading

```python
is_valid, message = manager.validate_evolution(
    source_df=my_df,
    target_table='orders',
    strategy=EvolutionStrategy.FLEXIBLE
)

if is_valid:
    # Proceed with load
    load_data(my_df)
else:
    print(f"Schema evolution not allowed: {message}")
    # Handle the error
```

---

## Data Quality

### Quality Dimensions

The framework covers all major data quality dimensions:

| Dimension | Checks | Purpose |
|-----------|--------|---------|
| **Completeness** | Null counts, required fields | Ensure no missing data |
| **Uniqueness** | Duplicates, primary keys | Prevent duplicate records |
| **Validity** | Value ranges, enums, regex | Ensure correct formats/values |
| **Consistency** | Referential integrity, cross-field rules | Maintain relationships |
| **Timeliness** | Freshness, date ranges | Ensure data is current |
| **Accuracy** | Statistical profiling | Understand data distributions |

### Data Quality Quick Start

```python
from pyspark.sql import SparkSession
from data_quality import DataQualityFramework

spark = SparkSession.builder.appName("DataQuality").getOrCreate()
quality = DataQualityFramework(spark)

# Define quality checks
quality_config = {
    'null_checks': {
        'columns': ['customer_id', 'email', 'order_date'],
        'threshold_pct': 5.0  # Fail if > 5% nulls
    },
    'required_fields': ['customer_id', 'order_id'],
    'duplicates': {
        'key_columns': ['order_id'],
        'sample_size': 10
    },
    'primary_key': 'order_id',
    'value_ranges': {
        'amount': (0, 1000000),
        'quantity': (1, 100)
    },
    'valid_values': {
        'status': ['pending', 'completed', 'shipped', 'cancelled']
    },
    'regex_patterns': {
        'email': r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    }
}

# Run comprehensive checks
results = quality.run_comprehensive_checks(df, quality_config)

# Check results
if results['summary']['failed'] == 0:
    print("✓ All quality checks passed!")
else:
    print(f"✗ {results['summary']['failed']} checks failed")
    
# Get detailed report
report = quality.get_quality_report()
report.show()
```

### Optimization for Large Data

#### 1. Sampling for Huge Datasets

```python
# Check null counts on 10% sample (10B records → 1B sample)
quality.check_null_counts(
    df,
    columns=['customer_id', 'email'],
    threshold_pct=5.0,
    sample_fraction=0.1  # 10% sample
)
```

**Performance Impact:**
- Full scan: 10 minutes on 10B records
- 10% sample: 1 minute (10x faster)
- Accuracy: ±1% with 10% sample

#### 2. Partitioned Processing

```python
# Repartition by high-cardinality column
df = df.repartition(200, 'user_id')

# Run quality checks (automatically parallelized)
quality.check_duplicates(df, key_columns=['transaction_id'])
```

**Benefits:**
- Even data distribution across workers
- Parallel processing of quality checks
- Reduced shuffle operations

#### 3. Caching for Multiple Checks

```python
# Cache when running multiple checks
df.cache()

quality.check_null_counts(df)
quality.check_duplicates(df, ['order_id'])
quality.check_value_ranges(df, {'amount': (0, 10000)})
quality.profile_numeric_columns(df)

df.unpersist()  # Clean up
```

#### 4. Incremental Aggregations

```python
# Single-pass aggregation (efficient for huge data)
stats = df.agg(
    F.count('*').alias('total'),
    F.countDistinct('user_id').alias('unique_users'),
    F.sum(F.when(F.col('amount').isNull(), 1).otherwise(0)).alias('null_amounts'),
    F.min('amount').alias('min_amount'),
    F.max('amount').alias('max_amount'),
    F.avg('amount').alias('avg_amount')
).first()
```

---

## Quality Check Details

### Completeness Checks

#### Check Null Counts

```python
result = quality.check_null_counts(
    df,
    columns=['customer_id', 'email', 'phone'],
    threshold_pct=5.0,
    sample_fraction=1.0  # Use 0.1 for 10% sample on huge data
)

# Returns:
{
    'status': 'PASSED' or 'FAILED',
    'results': [
        {
            'column': 'customer_id',
            'null_count': 150,
            'null_percentage': 1.5,
            'threshold': 5.0,
            'status': 'PASSED'
        },
        ...
    ],
    'failed_columns': ['phone']  # If any failed
}
```

#### Check Required Fields

```python
result = quality.check_required_fields(
    df,
    required_columns=['customer_id', 'order_id', 'amount']
)

# Fails if any required field has NULL values
```

### Uniqueness Checks

#### Check Duplicates

```python
result = quality.check_duplicates(
    df,
    key_columns=['order_id'],
    sample_size=10  # Number of duplicate examples to return
)

# Returns duplicate groups and total duplicate count
```

#### Check Primary Key

```python
result = quality.check_primary_key_uniqueness(
    df,
    primary_key='customer_id'
)

# Ensures column has unique values
```

### Validity Checks

#### Value Ranges

```python
result = quality.check_value_ranges(
    df,
    range_rules={
        'age': (0, 120),
        'price': (0, 999999),
        'quantity': (1, 1000)
    }
)

# Validates numeric values are within bounds
```

#### Valid Values (Enums)

```python
result = quality.check_valid_values(
    df,
    valid_values={
        'status': ['pending', 'active', 'completed', 'cancelled'],
        'country': ['US', 'CA', 'UK', 'DE', 'FR'],
        'priority': ['low', 'medium', 'high', 'urgent']
    }
)

# Ensures categorical columns only contain allowed values
```

#### Regex Patterns

```python
result = quality.check_regex_patterns(
    df,
    pattern_rules={
        'email': r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$',
        'phone': r'^\+?1?\d{10}$',
        'zip_code': r'^\d{5}(-\d{4})?$',
        'product_id': r'^PROD-\d{6}$'
    }
)

# Validates string patterns
```

### Consistency Checks

#### Referential Integrity

```python
result = quality.check_referential_integrity(
    fact_df=orders,
    dimension_df=customers,
    fact_key='customer_id',
    dimension_key='customer_id'
)

# Finds orphaned records (foreign keys without matching primary keys)
```

#### Cross-Field Business Rules

```python
business_rules = [
    # Rule 1: Ship date after order date
    (
        "ship_after_order",
        lambda df: df.filter(F.col('ship_date') < F.col('order_date'))
    ),
    
    # Rule 2: Discount cannot exceed price
    (
        "valid_discount",
        lambda df: df.filter(F.col('discount') > F.col('price'))
    ),
    
    # Rule 3: Total = subtotal + tax
    (
        "total_calculation",
        lambda df: df.filter(
            F.abs(F.col('total') - (F.col('subtotal') + F.col('tax'))) > 0.01
        )
    )
]

result = quality.check_cross_field_rules(df, business_rules)
```

### Timeliness Checks

#### Data Freshness

```python
result = quality.check_data_freshness(
    df,
    timestamp_column='created_at',
    max_age_hours=24  # Fail if data is > 24 hours old
)

# Ensures data is current
```

#### Date Ranges

```python
result = quality.check_date_ranges(
    df,
    date_column='transaction_date',
    expected_start='2024-01-01',
    expected_end='2024-12-31'
)

# Validates dates are within expected range
```

### Statistical Profiling

#### Numeric Profiling

```python
profile = quality.profile_numeric_columns(
    df,
    columns=['amount', 'quantity', 'discount'],
    percentiles=[0.25, 0.5, 0.75, 0.95, 0.99]
)

# Returns:
{
    'amount': {
        'count': 1000000,
        'distinct': 95432,
        'nulls': 150,
        'min': 0.01,
        'max': 9999.99,
        'mean': 125.45,
        'stddev': 78.32,
        'percentiles': {
            '25%': 50.00,
            '50%': 100.00,
            '75%': 175.00,
            '95%': 350.00,
            '99%': 500.00
        }
    }
}
```

#### Categorical Profiling

```python
profile = quality.profile_categorical_columns(
    df,
    columns=['category', 'payment_method', 'country'],
    top_n=10
)

# Returns top values and cardinality metrics
```

---

## Best Practices

### Schema Evolution Best Practices

1. **Always use dry_run first**
```python
# Test schema changes before applying
result = manager.apply_schema_evolution(
    source_df=df,
    target_table='customers',
    strategy=EvolutionStrategy.FLEXIBLE,
    dry_run=True  # Generate DDL without executing
)
```

2. **Choose appropriate strategy**
```python
# Production: FLEXIBLE (safe changes only)
# Development: PERMISSIVE (all changes allowed)
# Critical tables: STRICT (no changes)
```

3. **Validate before load**
```python
is_valid, message = manager.validate_evolution(df, 'table_name', strategy)
if not is_valid:
    raise ValueError(f"Schema evolution failed: {message}")
```

4. **Align DataFrames when needed**
```python
# When loading to existing table with extra columns
aligned_df = manager.align_dataframe_to_target(source_df, 'table_name')
```

### Data Quality Best Practices

1. **Use sampling for huge datasets**
```python
# 10B+ records: use 10% sample for null checks
quality.check_null_counts(df, sample_fraction=0.1)
```

2. **Cache for multiple checks**
```python
df.cache()
# Run multiple quality checks
df.unpersist()
```

3. **Set appropriate thresholds**
```python
quality_config = {
    'null_checks': {
        'threshold_pct': 5.0  # Adjust based on data
    },
    'value_ranges': {
        'amount': (0, 1000000)  # Set realistic bounds
    }
}
```

4. **Monitor quality trends**
```python
# Save quality reports for trending
report = quality.get_quality_report()
report.write.mode("append").partitionBy("date").parquet("s3://quality-reports/")
```

5. **Optimize for data volume**

| Data Size | Optimization |
|-----------|--------------|
| < 1M rows | Full checks, no sampling |
| 1M - 100M rows | Full checks, partition by key columns |
| 100M - 1B rows | 10% sampling for null checks, cache DataFrame |
| > 1B rows | 1-10% sampling, increase partitions to 200+ |

---

## Production Deployment

### Airflow Integration

```python
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

quality_check_task = SparkSubmitOperator(
    task_id='run_quality_checks',
    application='/path/to/quality_examples.py',
    conf={
        'spark.executor.memory': '16g',
        'spark.sql.shuffle.partitions': '200'
    },
    application_args=[
        '--table', 'customers',
        '--config', 'quality_config.json'
    ]
)
```

### Databricks Integration

```python
# In Databricks notebook
%run /path/to/schema_evolution
%run /path/to/data_quality

# Run quality checks
quality = DataQualityFramework(spark)
results = quality.run_comprehensive_checks(df, config)

# Log to Delta Lake
quality.get_quality_report() \
    .write.format("delta") \
    .mode("append") \
    .saveAsTable("quality_metrics")
```

---

## Troubleshooting

### Common Issues

**Issue: Schema comparison fails with JDBC**
```python
# Solution: Ensure Redshift JDBC driver is available
spark.conf.set("spark.jars", "/path/to/redshift-jdbc.jar")
```

**Issue: Quality checks timeout on huge data**
```python
# Solution: Use sampling
quality.check_null_counts(df, sample_fraction=0.1)

# Or increase resources
spark.conf.set("spark.executor.memory", "32g")
spark.conf.set("spark.sql.shuffle.partitions", "500")
```

**Issue: Out of memory during profiling**
```python
# Solution: Profile in batches
for partition in df.select('partition_key').distinct().collect():
    partition_df = df.filter(F.col('partition_key') == partition.partition_key)
    quality.profile_numeric_columns(partition_df)
```

---

## License

MIT License

## Support

For questions or issues, please open a GitHub issue.
