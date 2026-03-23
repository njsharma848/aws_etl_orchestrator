# Banking scenario based questions - guide

---

## Section 1: Data Pipeline Design & ETL/ELT

---

### 1.1 Real-Time Fraud Detection Pipeline (Kafka → Spark Streaming → Data Lake)

#### Overview

A real-time fraud detection pipeline needs three core layers:
- **Ingestion** — Kafka receives raw transaction events
- **Processing** — Spark Streaming applies fraud rules/ML models in near real-time
- **Storage** — Processed data lands in a Data Lake (e.g., AWS S3, Azure ADLS, HDFS) in Parquet/Delta format

#### Architecture Flow

```
[Core Banking / POS Systems]
        ↓ (produce events)
    [Apache Kafka]
     Topic: transactions
        ↓
  [Spark Structured Streaming]
   - Parse & validate schema
   - Apply fraud rules (velocity checks, geo-anomaly, ML model)
   - Enrich with customer profile (broadcast join)
        ↓                          ↓
  [Fraud Alert Topic]       [Data Lake (S3/ADLS)]
  (real-time alerts)         Parquet / Delta Lake
                              Partitioned by date/hour
        ↓
  [Alerting System / Case Mgmt]
```

#### Simple Code Example — Spark Structured Streaming

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp
from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType

spark = SparkSession.builder.appName("FraudDetection").getOrCreate()

# Define the schema of incoming Kafka messages
schema = StructType() \
    .add("transaction_id", StringType()) \
    .add("account_id", StringType()) \
    .add("amount", DoubleType()) \
    .add("merchant", StringType()) \
    .add("timestamp", TimestampType())

# Read from Kafka
raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka-broker:9092") \
    .option("subscribe", "transactions") \
    .option("startingOffsets", "latest") \
    .load()

# Parse JSON payload
transactions = raw_stream \
    .select(from_json(col("value").cast("string"), schema).alias("data")) \
    .select("data.*")

# Simple fraud rule: flag transactions > ₹1,00,000
flagged = transactions.filter(col("amount") > 100000) \
    .withColumn("flagged_at", current_timestamp())

# Write alerts to another Kafka topic
flagged.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka-broker:9092") \
    .option("topic", "fraud-alerts") \
    .option("checkpointLocation", "/checkpoints/fraud-alerts") \
    .start()

# Write all transactions to Data Lake (Delta/Parquet)
transactions.writeStream \
    .format("delta") \
    .option("checkpointLocation", "/checkpoints/transactions") \
    .partitionBy("date") \
    .start("/data-lake/transactions/")
```

#### Key Design Decisions

| Decision | Recommendation | Reason |
|---|---|---|
| Kafka Offset Management | Commit offsets only after successful write | Prevents data loss on failure |
| Checkpointing | Always enable in Spark Streaming | Enables recovery from failures |
| Partitioning in Data Lake | By `date` and `hour` | Efficient for downstream queries |
| Output Mode | `append` for raw, `update` for aggregations | Avoids full rewrite |
| State Store | RocksDB state store for windowed aggregations | Handles large stateful operations |

---

### 1.2 Idempotent Pipelines — Avoiding Duplicate Records

#### The Problem

If an ETL job crashes halfway and re-runs, it may insert the same records again — causing double-counting in balance calculations, duplicate fraud alerts, etc.

#### Solution Strategies

**Strategy 1: Natural Deduplication Key + MERGE (UPSERT)**

Instead of blindly inserting, use a `MERGE` statement. If the record already exists (based on a unique business key like `transaction_id`), update it; otherwise insert.

```sql
-- SQL: Idempotent UPSERT using MERGE
MERGE INTO transactions_fact AS target
USING staging_transactions AS source
ON target.transaction_id = source.transaction_id
WHEN MATCHED THEN
    UPDATE SET
        target.amount = source.amount,
        target.status = source.status,
        target.updated_at = source.updated_at
WHEN NOT MATCHED THEN
    INSERT (transaction_id, account_id, amount, status, created_at)
    VALUES (source.transaction_id, source.account_id, source.amount, source.status, source.created_at);
```

**Strategy 2: Watermark-Based Incremental Loads**

Track the last successfully processed timestamp and only load new records each run.

```python
# Python ETL: Idempotent incremental load
import psycopg2

def get_last_processed_timestamp(conn):
    cursor = conn.cursor()
    cursor.execute("SELECT MAX(processed_at) FROM etl_checkpoint WHERE job_name = 'txn_load'")
    return cursor.fetchone()[0]

def run_idempotent_etl():
    conn = psycopg2.connect("your_db_connection_string")
    last_ts = get_last_processed_timestamp(conn)

    # Load only NEW records since last run
    query = f"""
        SELECT * FROM source_transactions
        WHERE created_at > '{last_ts}'
    """
    new_records = fetch_from_source(query)
    
    # UPSERT into target
    upsert_to_target(new_records)
    
    # Update checkpoint ONLY after successful load
    update_checkpoint(conn, job_name='txn_load')
```

**Strategy 3: Delta Lake / Iceberg ACID Transactions**

Use Delta Lake or Apache Iceberg which support ACID guarantees, so re-runs within a transaction are safe — either the whole batch commits or it rolls back.

```python
# Delta Lake merge — idempotent by design
from delta.tables import DeltaTable

delta_table = DeltaTable.forPath(spark, "/data-lake/transactions/")

delta_table.alias("target").merge(
    source=new_batch_df.alias("source"),
    condition="target.transaction_id = source.transaction_id"
).whenMatchedUpdateAll() \
 .whenNotMatchedInsertAll() \
 .execute()
```

---

### 1.3 Schema Evolution — Handling Upstream Schema Changes

#### The Problem

An upstream banking application adds a new column (`risk_score`) to the transactions table. Pipelines that expect a fixed schema will break.

#### Solution Strategies

**Strategy 1: Schema-on-Read with Parquet/Delta**

Do not enforce schema at write time. Use `mergeSchema` option in Spark to automatically handle new columns.

```python
# Spark: Allow schema evolution when writing to Delta
df.write \
    .format("delta") \
    .option("mergeSchema", "true") \  # New columns are added automatically
    .mode("append") \
    .save("/data-lake/transactions/")
```

**Strategy 2: Schema Registry with Avro (Kafka)**

Use Confluent Schema Registry to version schemas. Producers register a new schema version; consumers can decode older and newer versions.

```
Schema v1: { transaction_id, account_id, amount }
Schema v2: { transaction_id, account_id, amount, risk_score }  ← new field added
```

Consumers using v1 will simply ignore `risk_score` (backward compatibility). New consumers can use v2.

**Strategy 3: Defensive Coding in ETL**

```python
# Handle missing columns gracefully
from pyspark.sql.functions import lit

expected_columns = ["transaction_id", "account_id", "amount", "risk_score"]

for col_name in expected_columns:
    if col_name not in df.columns:
        df = df.withColumn(col_name, lit(None))  # Add missing column as NULL

df = df.select(expected_columns)  # Enforce consistent column order
```

**Schema Evolution Compatibility Rules:**

| Type | Example | Safe? |
|---|---|---|
| Add optional field | Add `risk_score` with default null | ✅ Yes |
| Remove field | Remove `branch_code` | ⚠️ Needs coordination |
| Rename field | `amt` → `amount` | ❌ Breaking change |
| Change data type | `amount` INT → BIGINT | ⚠️ Usually safe with widening |

---

### 1.4 Batch vs. Stream Processing in Banking

#### When to Use Each

| Criteria | Batch Processing | Stream Processing |
|---|---|---|
| Latency Requirement | Hours / Daily (OK with delay) | Milliseconds to seconds |
| Data Volume | Very large, historical | Continuous, real-time |
| Use Case Examples | Daily reconciliation, month-end reports, GL entries | Fraud detection, transaction monitoring, AML alerts |
| Cost | Lower compute cost | Higher infrastructure cost |
| Complexity | Simpler | More complex (state, watermarks, ordering) |

#### Banking Examples

**Batch (Daily Reconciliation):**
- Run at midnight; compare total debits/credits in Core Banking System vs. Data Warehouse
- Acceptable to run for 2–3 hours as long as numbers match before business opens

```python
# Batch job: Daily reconciliation
def daily_reconciliation():
    # Load yesterday's data
    cbs_total = get_cbs_totals(date='yesterday')
    dw_total = get_dw_totals(date='yesterday')
    
    if abs(cbs_total - dw_total) > 0:
        send_alert(f"Reconciliation mismatch: CBS={cbs_total}, DW={dw_total}")
    else:
        log("Reconciliation PASSED")
```

**Stream (Fraud Alert):**
- A customer's card is used in Mumbai and Singapore within 5 minutes → alert immediately
- Cannot wait for a daily batch — the fraud window closes in seconds

**The Rule of Thumb:**
> If the business impact of a 1-hour delay is unacceptable → **Stream**. If overnight is fine → **Batch**.

---

## Section 2: SQL & Analytical Data Modeling

---

### 2.1 Slow Query Optimization — Large Fact + Dimension Join

#### Common Problem

```sql
-- Slow Query: Full table scan + unoptimized join
SELECT c.customer_name, SUM(t.amount) AS total_spent
FROM transactions t
JOIN customer c ON t.customer_id = c.customer_id
WHERE t.transaction_date BETWEEN '2024-01-01' AND '2024-12-31'
GROUP BY c.customer_name;
```

#### Optimization Steps

**Step 1: Add Indexes**
```sql
-- Index on join key and filter column
CREATE INDEX idx_txn_customer_id ON transactions(customer_id);
CREATE INDEX idx_txn_date ON transactions(transaction_date);
CREATE INDEX idx_cust_id ON customer(customer_id);
```

**Step 2: Partition the Fact Table**
```sql
-- Partition transactions by year/month
CREATE TABLE transactions (
    transaction_id BIGINT,
    customer_id INT,
    amount DECIMAL(15,2),
    transaction_date DATE
) PARTITION BY RANGE (transaction_date);

CREATE TABLE transactions_2024 PARTITION OF transactions
    FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');
```

**Step 3: Use Columnar Storage + Materialized Views for Aggregations**
```sql
-- Pre-aggregate frequently queried data
CREATE MATERIALIZED VIEW monthly_customer_spend AS
SELECT
    customer_id,
    DATE_TRUNC('month', transaction_date) AS month,
    SUM(amount) AS total_amount,
    COUNT(*) AS txn_count
FROM transactions
GROUP BY customer_id, DATE_TRUNC('month', transaction_date);

-- Refresh periodically
REFRESH MATERIALIZED VIEW monthly_customer_spend;
```

**Step 4: Avoid SELECT \***
```sql
-- Good: Select only needed columns
SELECT c.customer_name, m.total_amount
FROM monthly_customer_spend m
JOIN customer c ON m.customer_id = c.customer_id
WHERE m.month = '2024-01-01';
```

**Step 5: Use EXPLAIN / EXPLAIN ANALYZE to identify bottlenecks**
```sql
EXPLAIN ANALYZE
SELECT c.customer_name, SUM(t.amount)
FROM transactions t
JOIN customer c ON t.customer_id = c.customer_id
WHERE t.transaction_date = '2024-01-01'
GROUP BY c.customer_name;
-- Look for: Seq Scan → should be Index Scan, Hash Join → Nested Loop for small sets
```

---

### 2.2 Window Functions — Detecting Consecutive High-Value Transactions

#### Problem: Detect accounts with 3+ consecutive transactions above ₹50,000 within a 1-hour window

```sql
WITH ranked_txns AS (
    SELECT
        transaction_id,
        account_id,
        amount,
        transaction_time,
        -- Assign row number per account ordered by time
        ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY transaction_time) AS rn,
        -- Rank by amount within each account
        RANK() OVER (PARTITION BY account_id ORDER BY amount DESC) AS amount_rank,
        -- Get the next transaction's time for time-window check
        LEAD(transaction_time, 1) OVER (PARTITION BY account_id ORDER BY transaction_time) AS next_txn_time,
        LEAD(amount, 1) OVER (PARTITION BY account_id ORDER BY transaction_time) AS next_amount
    FROM transactions
    WHERE transaction_date = CURRENT_DATE
),
consecutive AS (
    SELECT
        account_id,
        transaction_id,
        amount,
        transaction_time,
        next_txn_time,
        -- Check if next transaction is within 1 hour AND also above threshold
        CASE
            WHEN amount > 50000
                AND next_amount > 50000
                AND next_txn_time <= transaction_time + INTERVAL '1 hour'
            THEN 1 ELSE 0
        END AS is_consecutive_high
    FROM ranked_txns
)
-- Flag accounts with 2+ consecutive high-value transactions
SELECT account_id, COUNT(*) AS consecutive_high_txns
FROM consecutive
WHERE is_consecutive_high = 1
GROUP BY account_id
HAVING COUNT(*) >= 2
ORDER BY consecutive_high_txns DESC;
```

**What Each Window Function Does:**

| Function | Use in This Query |
|---|---|
| `ROW_NUMBER()` | Sequential numbering to track order of transactions |
| `RANK()` | Ranks by amount — useful to find top transactions per account |
| `LEAD()` | Looks at the NEXT row's value — used to compare consecutive transactions |
| `LAG()` | Opposite of LEAD — looks at previous row (useful for time-since-last-txn) |

---

### 2.3 Handling Data Skewness in Spark

#### The Problem

A Spark job processing transactions by `branch_id` stalls because one branch (e.g., Head Office Mumbai) has 80% of all transactions. All data is shuffled to one executor → OOM or timeout.

#### Solutions

**Solution 1: Salting (Add Random Prefix to Skewed Key)**

```python
from pyspark.sql.functions import col, concat, lit, floor, rand

# Add a salt (0 to 9) to the skewed key to spread data
salted_transactions = transactions \
    .withColumn("salt", (rand() * 10).cast("int")) \
    .withColumn("salted_branch", concat(col("branch_id"), lit("_"), col("salt")))

# Similarly salt the lookup table by expanding it 10x
from pyspark.sql.functions import explode, array

lookup_expanded = lookup_table \
    .withColumn("salt", explode(array([lit(i) for i in range(10)]))) \
    .withColumn("salted_branch", concat(col("branch_id"), lit("_"), col("salt")))

# Now join on salted key — data is evenly spread
result = salted_transactions.join(lookup_expanded, on="salted_branch")
```

**Solution 2: Broadcast Join (for Small Lookup Tables)**

```python
from pyspark.sql.functions import broadcast

# If 'branch' table is small (<200MB), broadcast it to all executors
result = transactions.join(
    broadcast(branch_lookup),
    on="branch_id"
)
# No shuffle needed → no skew possible
```

**Solution 3: Increase Partitions + Skew Hint (Spark 3.x)**

```python
# Spark 3.0+ AQE (Adaptive Query Execution) handles skew automatically
spark = SparkSession.builder \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.skewJoin.enabled", "true") \
    .getOrCreate()
```

**Solution 4: Repartition Before Join**

```python
# Force even distribution before a heavy aggregation
transactions.repartition(200, col("branch_id")) \
    .groupBy("branch_id") \
    .agg({"amount": "sum"})
```

---

### 2.4 Star vs. Snowflake Schema for Loan BI

#### Star Schema

All dimension data is **denormalized** — one flat table per dimension.

```
         [dim_customer]
               |
[dim_date] — [fact_loan] — [dim_product]
               |
         [dim_branch]
```

```sql
-- Star Schema example
CREATE TABLE fact_loan (
    loan_id BIGINT,
    customer_id INT,       -- FK to dim_customer (denormalized)
    product_id INT,        -- FK to dim_product
    branch_id INT,
    date_id INT,
    loan_amount DECIMAL(15,2),
    outstanding_balance DECIMAL(15,2),
    emi_amount DECIMAL(12,2)
);

CREATE TABLE dim_customer (
    customer_id INT PRIMARY KEY,
    customer_name VARCHAR(100),
    city VARCHAR(50),
    state VARCHAR(50),       -- Denormalized: city+state together
    segment VARCHAR(20),
    credit_score INT
);
```

#### Snowflake Schema

Dimensions are **normalized** — split into sub-tables.

```
[dim_state] → [dim_city] → [dim_customer]
                                  |
              [dim_date] — [fact_loan] — [dim_product_type] → [dim_product]
                                  |
                            [dim_branch]
```

#### Comparison

| Feature | Star Schema | Snowflake Schema |
|---|---|---|
| Query Performance | ✅ Faster (fewer joins) | ⚠️ Slower (more joins) |
| Storage | ❌ More (redundant data) | ✅ Less (normalized) |
| Maintenance | Easier | More complex |
| BI Tool Friendly | ✅ Yes | Moderate |
| Data Integrity | Lower | Higher |

**Recommendation for Banking BI (Loans):**
> Use **Star Schema** for BI dashboards and reporting. Business users and BI tools (Power BI, Tableau) generate faster, simpler queries. Reserve Snowflake schema for OLTP or when storage is a critical constraint.

---

## Section 3: Big Data Technologies (Spark, Kafka, Hadoop)

---

### 3.1 Spark Partitioning (`partitionBy`) vs. Bucketing (`bucketBy`)

#### `partitionBy` — File-System Level Partitioning

Creates physical folder hierarchies on disk. Data is split into directories.

```python
# Writes data into folders: /data-lake/transactions/year=2024/month=01/
df.write \
    .format("parquet") \
    .partitionBy("year", "month") \
    .save("/data-lake/transactions/")
```

**Best for:** Date/region-based filtering in ad-hoc queries. Example: "Show me all transactions from January 2024."

#### `bucketBy` — Hash-Based Bucketing

Data is hashed into a fixed number of buckets (files) per partition. Useful for **joins and aggregations** on the same key.

```python
# Buckets data by account_id into 64 buckets — sorted within each bucket
df.write \
    .format("parquet") \
    .bucketBy(64, "account_id") \
    .sortBy("account_id") \
    .saveAsTable("transactions_bucketed")  # Must save as table, not path
```

**Best for:** Repeated joins on `account_id` or `customer_id`. Spark can skip the shuffle step entirely (sort-merge join without shuffle).

#### When to Use Bucketing in Banking

```
Use Case: Daily risk report joins transactions with customer_profile on customer_id
→ Both tables bucketed by customer_id into same number of buckets
→ Spark performs shuffle-free join = 5–10x faster for large datasets
```

| Feature | `partitionBy` | `bucketBy` |
|---|---|---|
| Storage Level | File system (folders) | Within files (hash) |
| Best For | Date/region filters | Joins, group-bys |
| Works With | All formats | Hive/Delta tables only |
| Scan Pruning | Yes (partition pruning) | Yes (bucket pruning) |

---

### 3.2 Broadcast Joins in Spark

#### What Is It?

In a regular join, Spark **shuffles** both datasets across the network so matching keys end up on the same executor. With a broadcast join, Spark **copies the entire small table to every executor** — eliminating the shuffle.

#### Simple Analogy

> Regular join: Everyone meets at a central hall (expensive travel).
> Broadcast join: Make photocopies of the small table and distribute to everyone's desk.

```python
from pyspark.sql.functions import broadcast

# transactions = 500 GB  |  branch_lookup = 2 MB
result = transactions.join(
    broadcast(branch_lookup),   # Sends branch_lookup to all executors
    on="branch_id",
    how="left"
)
```

#### When to Use

| Condition | Use Broadcast Join? |
|---|---|
| Small table < 200 MB (default threshold) | ✅ Yes |
| Both tables are large (GB+) | ❌ No — use sort-merge join |
| Joining transaction with country codes / currency lookup | ✅ Yes |
| Joining two fact tables | ❌ No |

#### Auto-Broadcast Threshold (Spark Config)

```python
# Spark auto-broadcasts tables smaller than this threshold
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "209715200")  # 200 MB
# Set to -1 to disable auto-broadcast
```

---

### 3.3 Kafka Partitioning — Guaranteeing Order Per Account

#### The Problem

Kafka distributes messages across multiple partitions for parallelism. By default, messages are round-robin assigned — so transaction 1 and transaction 2 for the same account may go to different partitions, and consumers see them out of order.

#### Solution: Use `account_id` as the Message Key

Kafka guarantees **ordering within a partition**. If all messages for the same `account_id` go to the same partition, order is preserved.

```python
from kafka import KafkaProducer
import json

producer = KafkaProducer(
    bootstrap_servers='kafka-broker:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: k.encode('utf-8')
)

# Use account_id as KEY → all messages for same account go to same partition
transaction = {
    "transaction_id": "TXN001",
    "account_id": "ACC12345",
    "amount": 5000.0
}

producer.send(
    topic='transactions',
    key=transaction['account_id'],   # ← This is the key
    value=transaction
)
```

**How it works:**
```
Kafka uses: partition = hash(key) % num_partitions
→ Same account_id → same hash → same partition → ordered delivery
```

**Important Caveat:** Order is guaranteed per partition, not globally. If you need global ordering, use a single partition (but that kills parallelism — acceptable only for very low-volume topics).

---

### 3.4 Storage Formats — Why Parquet/ORC Over CSV

#### CSV Problems in Banking Data

- No schema enforcement — a pipeline bug can write `"abc"` into an `amount` column
- Row-based: To find total amounts, you must read ALL columns (inefficient)
- No compression (or inefficient one) — a 100 GB transactions file stays ~100 GB
- No support for nested data (arrays, structs)

#### Parquet / ORC Advantages

**1. Columnar Storage — Read Only What You Need**

```
CSV Row Read:    [txn_id, account_id, amount, merchant, location, timestamp, ...]
                 ← reads all columns even if you need only 'amount'

Parquet Column Read: [amount, amount, amount, ...]
                     ← reads ONLY the 'amount' column → 10x less I/O
```

**2. Efficient Compression**

```
Transactions table (1 billion rows)
CSV:     ~450 GB
Parquet: ~45 GB  (10x smaller — columnar data compresses better)
```

**3. Schema Enforcement**

```python
# Parquet stores schema inside the file
df.write.format("parquet").save("/data-lake/transactions/")

# When you read it back, schema is automatically applied
df_read = spark.read.format("parquet").load("/data-lake/transactions/")
df_read.printSchema()
# root
#  |-- transaction_id: string
#  |-- amount: decimal(15,2)    ← types preserved!
#  |-- transaction_date: date
```

**4. Predicate Pushdown**

```python
# Spark + Parquet: Only reads files/row-groups where date = '2024-01-01'
# Does NOT scan the entire dataset
df = spark.read.parquet("/data-lake/transactions/") \
    .filter("transaction_date = '2024-01-01'")
```

| Feature | CSV | Parquet | ORC |
|---|---|---|---|
| Storage Layout | Row | Column | Column |
| Compression | Poor | Excellent | Excellent |
| Schema | None | Embedded | Embedded |
| Predicate Pushdown | No | Yes | Yes |
| Nested Data | No | Yes | Yes |
| Best Used With | Export/Import | Spark, Hive | Hive, Presto |

---

## Section 4: Data Quality, Security, and Compliance

---

### 4.1 Handling PII Data in a Data Lake

#### Categories of PII in Banking

| Data | Example | Sensitivity |
|---|---|---|
| Customer Name | Rajesh Kumar | Medium |
| PAN Number | ABCDE1234F | High |
| Aadhaar Number | 1234 5678 9012 | Very High |
| Account Number | 000123456789 | High |
| Mobile Number | 9876543210 | High |

#### Strategy 1: Tokenization

Replace PII with a reversible token. The mapping is stored in a secure vault.

```python
# Pseudocode: Tokenization
def tokenize(value, field_name):
    token = vault.store(value, field_name)   # Returns a token like "TKN_PAN_001"
    return token

df = df.withColumn("pan_number", tokenize_udf(col("pan_number"), lit("pan")))
# Original: ABCDE1234F  →  Stored as: TKN_PAN_001
# Only authorized systems can reverse-lookup
```

#### Strategy 2: Masking

One-way transformation — cannot be reversed. Used for analytics where original value isn't needed.

```python
from pyspark.sql.functions import col, regexp_replace, sha2, concat, lit

# Mask: Show only last 4 digits of account number
df = df.withColumn(
    "account_number_masked",
    concat(lit("XXXX-XXXX-"), col("account_number").substr(-4, 4))
)
# 000123456789 → XXXX-XXXX-6789

# Hash PAN for joins/matching without exposing original
df = df.withColumn("pan_hash", sha2(col("pan_number"), 256))
```

#### Strategy 3: Column-Level Encryption

```python
from cryptography.fernet import Fernet

key = Fernet.generate_key()
cipher = Fernet(key)

def encrypt_column(value):
    return cipher.encrypt(value.encode()).decode()

encrypt_udf = udf(encrypt_column, StringType())
df = df.withColumn("pan_encrypted", encrypt_udf(col("pan_number")))
```

#### Strategy 4: Data Lake Zone Architecture

```
Raw Zone (Encrypted, Restricted Access)
    ↓ PII Masking/Tokenization
Curated Zone (No raw PII, analysts can access)
    ↓ Further aggregation
Reporting Zone (No PII at all)
```

**Access Control:**
- Use Apache Ranger or AWS Lake Formation for column-level access control
- Role-based access: Analysts see masked data; Compliance team sees tokenized data; Vault team can decrypt

---

### 4.2 Automated Data Quality Checks

#### Framework: Great Expectations or Custom Checks

```python
# Custom Data Quality Check Framework

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, when, isnull, lit
from datetime import datetime

def run_dq_checks(df: DataFrame, table_name: str) -> dict:
    results = {}
    total_rows = df.count()
    
    # Check 1: No null account_ids
    null_account_ids = df.filter(isnull(col("account_id"))).count()
    results["no_null_account_ids"] = {
        "passed": null_account_ids == 0,
        "failed_count": null_account_ids,
        "message": f"{null_account_ids} rows have null account_id"
    }
    
    # Check 2: Transaction amounts must be positive
    negative_amounts = df.filter(col("amount") <= 0).count()
    results["positive_amounts"] = {
        "passed": negative_amounts == 0,
        "failed_count": negative_amounts,
        "message": f"{negative_amounts} rows have non-positive amount"
    }
    
    # Check 3: Completeness — less than 1% nulls in critical columns
    null_timestamps = df.filter(isnull(col("transaction_time"))).count()
    null_pct = (null_timestamps / total_rows) * 100
    results["timestamp_completeness"] = {
        "passed": null_pct < 1.0,
        "null_percentage": round(null_pct, 2),
        "message": f"{null_pct:.2f}% rows have null timestamp"
    }
    
    # Check 4: No duplicate transaction IDs
    distinct_txn_ids = df.select("transaction_id").distinct().count()
    results["no_duplicates"] = {
        "passed": distinct_txn_ids == total_rows,
        "duplicate_count": total_rows - distinct_txn_ids
    }
    
    # Log results and raise alert if any check fails
    failed_checks = [k for k, v in results.items() if not v["passed"]]
    if failed_checks:
        send_alert(f"DQ FAILED for {table_name}: {failed_checks}")
        raise Exception(f"Data quality checks failed: {failed_checks}")
    
    return results

# Run in pipeline
dq_results = run_dq_checks(transactions_df, "transactions_fact")
```

**Using Great Expectations (Production-Grade):**

```python
import great_expectations as ge

# Wrap DataFrame
ge_df = ge.from_pandas(transactions_df.toPandas())

# Define expectations
ge_df.expect_column_values_to_not_be_null("account_id")
ge_df.expect_column_values_to_be_between("amount", min_value=0.01, max_value=10000000)
ge_df.expect_column_values_to_be_unique("transaction_id")
ge_df.expect_column_values_to_match_regex("pan_number", r"^[A-Z]{5}[0-9]{4}[A-Z]$")

# Validate
results = ge_df.validate()
if not results["success"]:
    raise Exception("Data quality validation failed")
```

---

### 4.3 Reconciliation — Core Banking System vs. Data Warehouse

#### The Goal

Ensure zero data loss between the source (CBS) and destination (DW).

#### Reconciliation Framework

```python
# Step 1: Compute control totals from Source (CBS)
cbs_summary = spark.read.jdbc(url=CBS_JDBC_URL, table="transactions") \
    .filter("transaction_date = '2024-01-15'") \
    .agg(
        count("*").alias("row_count"),
        sum("amount").alias("total_amount"),
        countDistinct("account_id").alias("unique_accounts")
    ).collect()[0]

# Step 2: Compute same totals from Destination (DW)
dw_summary = spark.read \
    .parquet("/data-lake/transactions/date=2024-01-15/") \
    .agg(
        count("*").alias("row_count"),
        sum("amount").alias("total_amount"),
        countDistinct("account_id").alias("unique_accounts")
    ).collect()[0]

# Step 3: Compare
tolerance = 0.01  # Allow 1 paise tolerance for floating point

checks = {
    "row_count_match":    cbs_summary["row_count"] == dw_summary["row_count"],
    "amount_match":       abs(cbs_summary["total_amount"] - dw_summary["total_amount"]) < tolerance,
    "account_match":      cbs_summary["unique_accounts"] == dw_summary["unique_accounts"]
}

failed = [k for k, v in checks.items() if not v]
if failed:
    # Find the specific mismatched records
    cbs_df = spark.read.jdbc(...).filter("transaction_date = '2024-01-15'")
    dw_df = spark.read.parquet(...)
    
    missing_in_dw = cbs_df.join(dw_df, on="transaction_id", how="left_anti")
    missing_in_dw.write.mode("overwrite").save("/recon/missing_records/2024-01-15/")
    
    alert_team(f"Recon FAILED: {failed}. Missing records saved for investigation.")
```

**Three-Tier Reconciliation:**
1. **Count Check** — Total rows match
2. **Sum Check** — Total amounts match (within tolerance)
3. **Hash Check** — MD5/SHA hash of sorted records matches (for complete byte-level accuracy)

---

### 4.4 Audit Trails, Data Lineage, and Regulatory Compliance

#### Data Lineage — Tracking Data from Origin to Report

Use **Apache Atlas** (open-source) or **AWS Glue Data Catalog** / **Azure Purview** for automated lineage tracking.

```
CBS Transaction Table
    ↓ (Kafka ingestion)
Raw Layer: /data-lake/raw/transactions/
    ↓ (Spark ETL - masked PAN, validated amounts)
Curated Layer: /data-lake/curated/transactions/
    ↓ (Aggregation job)
DW: fact_transaction_daily
    ↓ (BI Query)
Dashboard: Fraud Summary Report
```

Every step is logged with: who ran it, when, what transformation was applied, which records were affected.

#### Audit Trail Implementation

```python
# Add audit columns to every dataset
from pyspark.sql.functions import current_timestamp, lit

def add_audit_columns(df, job_name, source_system):
    return df \
        .withColumn("_ingested_at", current_timestamp()) \
        .withColumn("_job_name", lit(job_name)) \
        .withColumn("_source_system", lit(source_system)) \
        .withColumn("_pipeline_run_id", lit(get_run_id()))

transactions_with_audit = add_audit_columns(
    transactions_df,
    job_name="txn_daily_load",
    source_system="CBS_FINACLE"
)
```

#### Compliance — GDPR & Basel III

| Regulation | Requirement | Implementation |
|---|---|---|
| GDPR | Right to erasure (delete customer data on request) | Soft-delete flag + tokenization; delete token mapping in vault |
| GDPR | Data minimization | Only collect and store necessary fields |
| Basel III | Accurate risk exposure data with full audit trail | Immutable audit logs; version-controlled datasets (Delta Lake time travel) |
| Basel III | Data aggregation accuracy | Automated reconciliation checks, control totals |

**Delta Lake Time Travel for Immutable Audit:**

```python
# Delta Lake keeps history of all changes
df_jan_15 = spark.read \
    .format("delta") \
    .option("timestampAsOf", "2024-01-15") \  # Read data as it was on Jan 15
    .load("/data-lake/transactions/")

# Or by version number
df_v5 = spark.read \
    .format("delta") \
    .option("versionAsOf", 5) \
    .load("/data-lake/transactions/")
```

This allows regulators to audit exactly what data looked like at any point in time — fulfilling Basel III and GDPR requirements.

---

## Section 5: Scenario-Based & Behavioral Questions

---

### 5.1 Handling Massive Unexpected Surge in Transaction Volume

**Situation:**
During a festive season (Diwali / IPO day), transaction volume jumped from 500K to 5 million transactions/hour — 10x the normal load. Spark jobs started failing with OOM errors, Kafka consumer lag spiked to 50 million messages.

**What I Did:**

1. **Immediate Triage:** Identified that Spark streaming micro-batches were too large because the trigger interval was 30 seconds. Reduced trigger to 5 seconds so each batch was smaller.

```python
# Changed from:
.trigger(processingTime="30 seconds")
# To:
.trigger(processingTime="5 seconds")
```

2. **Scale Out Spark:** Added 20 more executor nodes via YARN/Kubernetes auto-scaling. Increased `spark.executor.memory` from 8GB to 16GB to handle larger shuffle operations.

3. **Kafka Consumer Group Scaling:** Increased the number of Spark partitions to match Kafka topic partitions — allowing more parallel consumers.

```python
# Increase parallelism to match Kafka partitions
spark.conf.set("spark.sql.shuffle.partitions", "400")  # Was 200
```

4. **Back-Pressure:** Enabled Kafka back-pressure so Spark doesn't pull more data than it can process.

```python
.option("maxOffsetsPerTrigger", 500000)  # Cap at 500K records per trigger
```

5. **Post-Incident:** Set up auto-scaling alerts based on Kafka consumer lag metrics (via Prometheus/Grafana). If lag > 10 million, auto-provision additional Spark executors.

**Lesson Learned:** Always capacity-plan for 5x peak load. Test your auto-scaling runbooks before the peak event.

---

### 5.2 Third-Party API Rate Limiting (Exchange Rates)

**Situation:**
A critical pipeline fetches live exchange rates from an external provider to convert multi-currency transactions to INR. The provider started returning HTTP 429 (Too Many Requests), causing thousands of records to be processed without conversion.

**Solution:**

1. **Caching:** Cache exchange rates at the beginning of each pipeline run. Rates don't change every second — a 15-minute-old rate is acceptable for most use cases.

```python
import redis
import requests
from datetime import timedelta

def get_exchange_rate(from_currency, to_currency="INR"):
    cache_key = f"fx_rate:{from_currency}:{to_currency}"
    redis_client = redis.Redis(host='cache-server')
    
    # Check cache first
    cached = redis_client.get(cache_key)
    if cached:
        return float(cached)
    
    # Fetch from API (only if not in cache)
    response = requests.get(f"https://api.exchangerates.io/latest?base={from_currency}")
    rate = response.json()["rates"][to_currency]
    
    # Cache for 15 minutes
    redis_client.setex(cache_key, timedelta(minutes=15), str(rate))
    return rate
```

2. **Exponential Backoff + Retry:**

```python
import time

def fetch_with_retry(url, max_retries=5):
    for attempt in range(max_retries):
        response = requests.get(url)
        if response.status_code == 200:
            return response.json()
        elif response.status_code == 429:
            wait_time = 2 ** attempt  # 1, 2, 4, 8, 16 seconds
            print(f"Rate limited. Waiting {wait_time}s...")
            time.sleep(wait_time)
    raise Exception("Max retries exceeded")
```

3. **Fallback to Yesterday's Rates:** If live API is unavailable, use the previous day's stored rates from the data lake.

```python
def get_exchange_rate_with_fallback(currency):
    try:
        return get_exchange_rate(currency)   # Try live API
    except Exception:
        # Fall back to last known good rate from data lake
        return get_historical_rate(currency, date=yesterday())
```

4. **Long-Term:** Negotiate a higher rate limit tier with the provider. Alternatively, switch to a provider like RBI's FBIL rates (official, reliable, no rate limiting for banking use cases).

---

### 5.3 On-Premises Hadoop to Cloud Migration (Zero Downtime)

**Situation:**
Migrate a 500 TB on-premises Hadoop data warehouse to AWS (S3 + EMR + Redshift) with minimal disruption to daily banking operations.

**Migration Strategy: Parallel Run (Blue-Green Migration)**

#### Phase 1: Assess & Prepare (Weeks 1–4)
- Inventory all HDFS data, Hive tables, Spark jobs, Oozie/Airflow workflows
- Map on-premises components to cloud equivalents:

| On-Prem | AWS Equivalent |
|---|---|
| HDFS | S3 |
| Hive Metastore | AWS Glue Data Catalog |
| Spark on YARN | EMR (Spark) |
| Oozie | AWS MWAA (Managed Airflow) |
| HBase | DynamoDB or RDS |

#### Phase 2: Data Migration (Weeks 5–8)
- Use **AWS DataSync** or **DistCp** to copy historical data from HDFS to S3

```bash
# DistCp: Copy HDFS data to S3 (runs as a MapReduce job)
hadoop distcp \
    hdfs://namenode:8020/data/transactions/ \
    s3a://banking-datalake/transactions/

# For large datasets, use --bandwidth flag to not overwhelm the network
hadoop distcp -bandwidth 100 \  # 100 MB/s per map task
    hdfs://namenode:8020/data/ \
    s3a://banking-datalake/
```

#### Phase 3: Parallel Run (Weeks 9–12)
- Run **both** on-prem and cloud pipelines simultaneously
- Compare outputs daily (automated reconciliation)
- Cloud is "shadow" — results are validated but not served to business users yet

```
[Kafka] → [On-Prem Spark (Active)] → [On-Prem HDFS] → [Reports]
    ↓
[Cloud Spark (Shadow)] → [S3 + Redshift] → [Reconciliation Check]
```

#### Phase 4: Cutover (Week 13)
- On a low-traffic day (Sunday midnight), switch traffic:
  - Change Kafka consumer group to point to cloud Spark
  - Update BI tool connection strings to Redshift
  - Keep on-prem in read-only mode for 2 weeks (rollback option)

#### Phase 5: Decommission (Weeks 14–16)
- After 2 weeks of stable cloud operation, decommission on-prem cluster
- Archive cold data (>3 years) to S3 Glacier

**Key Risk Mitigations:**

| Risk | Mitigation |
|---|---|
| Data loss during copy | Checksum validation after DistCp; run reconciliation |
| Pipeline failures on cloud | Keep on-prem active until cloud is proven stable |
| Cost overrun | Use Spot instances for EMR batch jobs; Reserved instances for critical always-on |
| Latency increase to S3 | Use S3 Express One Zone for latency-sensitive workloads |
| Compliance/data residency | Ensure S3 bucket region is within India (ap-south-1 Mumbai) |

---

*End of Guide*

---
> **Tip for Interviews:** When answering scenario-based questions, always use the **STAR format** — Situation, Task, Action, Result. Quantify impact wherever possible (e.g., "reduced query time from 4 hours to 20 minutes", "processed 5M transactions per hour with zero data loss").
