# Delta Lake Problem-Solving Framework

> A systematic approach to solving Delta Lake interview questions.
> Adapted from the UDACT framework for data lakehouse scenarios.

---

## Why a Framework?

Delta Lake interview questions test more than syntax — they test whether you can **design data pipelines** that are reliable, performant, and maintainable. Having a systematic thinking framework is MORE important than memorizing Delta Lake APIs.

**Interview evaluation breakdown (approximate):**

| Aspect | Weight | What They're Looking For |
|--------|--------|-------------------------|
| Structured thinking | 40% | Systematic approach, considering trade-offs |
| Communication | 30% | Clear reasoning, explaining why not just what |
| Correctness | 20% | Working solution, handles edge cases |
| Optimization | 10% | Performance awareness, best practices |

---

## The UDACT Framework (Adapted for Delta Lake)

```
U → Understand the Problem
D → Design the Data Flow
A → Approach Selection
C → Code Incrementally
T → Test, Optimize & Communicate
```

---

## Phase 1: UNDERSTAND the Problem

### Goal: Clarify requirements before writing ANY code

### Questions to Ask

**Data Source Questions:**
- "What format is the source data?" (CSV, JSON, Parquet, API, streaming)
- "How often does new data arrive?" (real-time, hourly, daily, ad-hoc)
- "What's the expected data volume?" (MBs, GBs, TBs per load)
- "Can source data contain duplicates?"
- "Can the source schema change over time?"

**Processing Requirements:**
- "Is this a full load or incremental?"
- "How should we handle late-arriving data?"
- "What's the freshness requirement?" (real-time, near-real-time, daily)
- "Do we need to track historical changes (SCD Type 2)?"

**Output Requirements:**
- "Who consumes this data?" (BI dashboards, ML pipelines, other teams)
- "What query patterns are most common?" (filter by date, user_id, etc.)
- "Are there SLA requirements for data availability?"
- "Do we need to support time-travel queries?"

**Data Quality:**
- "What are the business rules for valid data?"
- "How should we handle NULL values?"
- "What are the primary/unique keys?"
- "What happens when a quality check fails — drop, quarantine, or fail the pipeline?"

### Example Clarification

> **Interviewer:** "Design a pipeline to ingest customer data from S3 into a Delta Lake table."
>
> **You:** "Before I start, let me clarify a few things:
> 1. Is the data arriving as files in S3? What format — CSV, JSON, Parquet?
> 2. Is this a one-time load or will files arrive continuously?
> 3. Can customer records be updated — meaning do we need upsert logic?
> 4. What columns uniquely identify a customer? Is it customer_id?
> 5. Should we maintain history of changes, or just keep the latest state?
> 6. What's the approximate data volume — thousands or millions of customers?"

---

## Phase 2: DESIGN the Data Flow

### Goal: Visualize the pipeline architecture before coding

### Step 1: Identify the Layers

For most Delta Lake problems, think in terms of the **medallion architecture**:

```
Source → Bronze (Raw) → Silver (Cleaned) → Gold (Aggregated)
```

Ask yourself:
- **Bronze:** What does the raw ingestion look like? Append-only? Schema?
- **Silver:** What cleaning, deduplication, and validation is needed?
- **Gold:** What aggregations or business views are needed downstream?

### Step 2: Draw the Data Flow

```
Example: Customer data pipeline

  S3 (CSV files)
       │
       ▼
  Auto Loader (readStream + cloudFiles)
       │
       ▼
  Bronze: raw_customers (append-only, + metadata columns)
       │
       ▼
  Structured Streaming (foreachBatch)
       │  ┌─ Parse & type-cast
       │  ├─ Validate (not null, valid email)
       │  ├─ Deduplicate (by customer_id, keep latest)
       │  └─ MERGE into silver
       ▼
  Silver: customers (cleaned, deduplicated, SCD Type 1)
       │
       ▼
  Scheduled job / streaming
       │  ┌─ Aggregate by region, segment
       │  └─ Compute KPIs
       ▼
  Gold: customer_summary (business metrics)
```

### Step 3: Identify Key Decisions

| Decision Point | Options | Trade-offs |
|----------------|---------|------------|
| Ingestion method | Auto Loader vs COPY INTO vs batch read | Auto Loader = best for streaming, COPY INTO = simpler for batch |
| Processing mode | Streaming vs batch | Streaming = lower latency, batch = simpler debugging |
| Merge strategy | SCD1 (overwrite) vs SCD2 (history) | SCD1 = simpler, SCD2 = full audit trail |
| Partitioning | Date vs region vs none | Depends on query patterns and data volume |
| File compaction | Auto-optimize vs scheduled OPTIMIZE | Auto = easier, scheduled = more control |

---

## Phase 3: APPROACH Selection

### Goal: Choose the right Delta Lake patterns for each layer

### Decision Matrix: Ingestion Pattern

```
How does data arrive?
├── Files landing in cloud storage
│   ├── Continuous/frequent → Auto Loader (cloudFiles)
│   └── Scheduled/batch → COPY INTO or spark.read
├── Change data from source DB
│   └── CDC stream → readStream with foreachBatch MERGE
├── API/push
│   └── Write to landing zone first, then ingest
└── One-time migration
    └── Batch read + df.write.format("delta")
```

### Decision Matrix: Write Pattern

```
How should data be written to target?
├── Append-only (logs, events, bronze) → .mode("append")
├── Overwrite all → .mode("overwrite")
├── Overwrite partition → .option("replaceWhere", ...)
├── Upsert (insert or update) → MERGE
│   ├── Simple upsert → whenMatchedUpdateAll + whenNotMatchedInsertAll
│   ├── SCD Type 2 → Expire old + insert new version
│   └── Full sync → Add whenNotMatchedBySourceDelete
└── Deduplicate on insert → MERGE with whenNotMatchedInsertAll only
```

### Decision Matrix: Optimization

```
What's the performance bottleneck?
├── Too many small files → OPTIMIZE (compaction)
├── Slow filtered queries → Z-ORDER by filter columns
├── High-cardinality partition → Switch to Z-ORDER or liquid clustering
├── Slow MERGE → Include partition column in merge condition
├── Large shuffle → Repartition before write
└── Repeated scans → Cache intermediate results
```

### Communicate Your Choice

> "I'll use Auto Loader for ingestion because files arrive continuously and it handles exactly-once semantics via checkpointing. For the silver layer, I'll use a streaming foreachBatch MERGE because we need upsert logic — customers can be updated. I'll partition the silver table by registration_date since most queries filter by date range, and Z-ORDER by customer_id since that's a frequent lookup key."

---

## Phase 4: CODE Incrementally

### Goal: Build the solution step by step, verifying each layer

### Coding Strategy

**Do NOT** write the entire pipeline at once. Build layer by layer:

1. **Start with the schema** — define expected types
2. **Build Bronze** — get raw data ingested correctly
3. **Build Silver** — add cleaning and merge logic
4. **Build Gold** — add aggregations
5. **Add error handling** — quality checks, logging, alerts
6. **Add maintenance** — OPTIMIZE, VACUUM, monitoring

### Step-by-Step Example

```python
# ─── STEP 1: Define schemas ─────────────────────────────────────────
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType

customer_schema = StructType([
    StructField("customer_id",  StringType(),  False),
    StructField("name",         StringType(),  True),
    StructField("email",        StringType(),  True),
    StructField("region",       StringType(),  True),
    StructField("signup_date",  DateType(),    True),
])

# ─── STEP 2: Bronze - Raw ingestion ─────────────────────────────────
from pyspark.sql import functions as F

bronze_stream = (spark
    .readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("header", "true")
    .schema(customer_schema)
    .load("/mnt/landing/customers/")
)

bronze_with_meta = (bronze_stream
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source_file", F.input_file_name())
)

# Verify: "At this point, bronze just appends raw data with metadata."

(bronze_with_meta
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/mnt/checkpoint/bronze_customers")
    .trigger(availableNow=True)
    .toTable("bronze.customers")
)

# ─── STEP 3: Silver - Clean + upsert ────────────────────────────────
from delta.tables import DeltaTable
from pyspark.sql.window import Window

def bronze_to_silver(batch_df, batch_id):
    # Clean
    cleaned = (batch_df
        .filter(F.col("customer_id").isNotNull())
        .withColumn("name",  F.trim(F.col("name")))
        .withColumn("email", F.lower(F.trim(F.col("email"))))
    )

    # Deduplicate within batch
    w = Window.partitionBy("customer_id").orderBy(F.col("_ingested_at").desc())
    deduped = cleaned.withColumn("rn", F.row_number().over(w)).filter("rn = 1").drop("rn")

    # Upsert
    target = DeltaTable.forName(spark, "silver.customers")
    (target.alias("t")
        .merge(deduped.alias("s"), "t.customer_id = s.customer_id")
        .whenMatchedUpdate(
            condition="s._ingested_at > t._ingested_at",
            set={"name": "s.name", "email": "s.email", "region": "s.region",
                 "signup_date": "s.signup_date", "_ingested_at": "s._ingested_at"}
        )
        .whenNotMatchedInsertAll()
        .execute()
    )

# Verify: "Now silver has clean, deduplicated customer records."

(spark
    .readStream.format("delta").table("bronze.customers")
    .writeStream
    .foreachBatch(bronze_to_silver)
    .option("checkpointLocation", "/mnt/checkpoint/silver_customers")
    .trigger(availableNow=True)
    .start()
)

# ─── STEP 4: Gold - Business aggregations ────────────────────────────
spark.sql("""
    CREATE OR REPLACE TABLE gold.customer_summary AS
    SELECT
        region,
        COUNT(*)                         AS total_customers,
        COUNT(DISTINCT email)            AS unique_emails,
        MIN(signup_date)                 AS earliest_signup,
        MAX(signup_date)                 AS latest_signup,
        current_timestamp()              AS refreshed_at
    FROM silver.customers
    GROUP BY region
""")

# Verify: "Gold layer gives us per-region customer metrics."

# ─── STEP 5: Maintenance ────────────────────────────────────────────
spark.sql("OPTIMIZE silver.customers ZORDER BY (customer_id)")
spark.sql("VACUUM silver.customers RETAIN 168 HOURS")
spark.sql("ANALYZE TABLE silver.customers COMPUTE STATISTICS FOR ALL COLUMNS")
```

### Communication During Coding

Narrate as you write:

- "First, I'll set up the bronze layer with Auto Loader to handle incremental file ingestion..."
- "For the merge condition, I'm including the partition column to enable predicate pushdown..."
- "I'm deduplicating within the batch before merging, because Delta MERGE requires unique source keys..."
- "The conditional update `s._ingested_at > t._ingested_at` ensures we don't overwrite with stale data..."

---

## Phase 5: TEST, Optimize & Communicate

### Goal: Verify correctness, handle edge cases, discuss improvements

### Testing Checklist

| Check | How to Verify |
|-------|---------------|
| **Correctness** | Query silver table — are records accurate? |
| **Deduplication** | `GROUP BY key HAVING COUNT(*) > 1` — should return 0 |
| **NULL handling** | Filter for NULLs in required columns |
| **Idempotency** | Run pipeline twice — results should be the same |
| **Schema evolution** | Add a column to source — does pipeline handle it? |
| **Late data** | Insert old-dated record — is it processed correctly? |
| **Merge conflict** | Simulate concurrent writes — does it succeed? |

### Edge Cases to Mention

```python
# Edge case 1: Duplicate source keys in single batch
# "I deduplicate the source before MERGE to avoid the 'multiple source rows' error."

# Edge case 2: Schema change in source files
# "Auto Loader with schemaEvolutionMode = addNewColumns handles new columns
#  automatically. For type changes, I'd add alerting and review."

# Edge case 3: Very large MERGE (billions of rows)
# "I'd include the partition column in the merge condition for predicate pushdown,
#  and consider processing in date-range batches."

# Edge case 4: Pipeline failure mid-write
# "Delta's ACID transactions mean a failed write is automatically rolled back.
#  The checkpoint tracks progress, so on retry we pick up where we left off."

# Edge case 5: Data arriving out of order
# "The conditional update (s.updated_at > t.updated_at) ensures we only apply
#  the latest version, even if data arrives out of order."
```

### Optimization Discussion

After your solution works, proactively discuss performance:

> "For production, I'd recommend:
> 1. **Partitioning** silver table by `signup_date` since most queries filter by date
> 2. **Z-ORDER** by `customer_id` for fast point lookups
> 3. **Auto-optimize** enabled for the bronze table since it receives many small appends
> 4. **Scheduled OPTIMIZE + VACUUM** as a nightly maintenance job
> 5. **Change Data Feed** enabled on silver for downstream consumers to read only changes
> 6. **Monitoring** via quality metrics table and alerts on constraint violations"

---

## Framework Quick Reference Card

```
┌─────────────────────────────────────────────────────────────────┐
│                     UDACT FRAMEWORK                              │
├──────────┬──────────────────────────────────────────────────────┤
│ UNDERSTAND│ Ask about: source format, volume, frequency,         │
│           │ keys, SCD type, consumers, quality rules             │
├──────────┼──────────────────────────────────────────────────────┤
│ DESIGN    │ Draw: Source → Bronze → Silver → Gold                │
│           │ Identify: ingestion, write, optimization patterns    │
├──────────┼──────────────────────────────────────────────────────┤
│ APPROACH  │ Choose: Auto Loader vs batch, append vs merge,       │
│           │ SCD1 vs SCD2, partition vs Z-ORDER                   │
│           │ Communicate WHY you chose each                       │
├──────────┼──────────────────────────────────────────────────────┤
│ CODE      │ Build incrementally: schema → bronze → silver → gold │
│           │ Narrate each step as you code                        │
│           │ Verify each layer before moving on                   │
├──────────┼──────────────────────────────────────────────────────┤
│ TEST &    │ Check: correctness, duplicates, nulls, idempotency   │
│ OPTIMIZE  │ Discuss: edge cases, partition strategy, maintenance  │
│           │ Mention: monitoring, alerting, SLA considerations    │
└──────────┴──────────────────────────────────────────────────────┘
```

---

## Common Interview Scenarios

### Scenario 1: "Design an incremental data pipeline"
- **U:** Clarify source, frequency, volume, idempotency needs
- **D:** Auto Loader → Bronze → foreachBatch MERGE → Silver → Gold
- **A:** Use `trigger(availableNow=True)` for scheduled incremental
- **C:** Build each layer, deduplicate before MERGE
- **T:** Test idempotency, discuss checkpoint recovery

### Scenario 2: "Implement SCD Type 2"
- **U:** Clarify which columns trigger a new version, effective dating
- **D:** Source → detect changes vs current → expire old + insert new
- **A:** MERGE with `is_current` flag and `effective_date`/`end_date`
- **C:** Join source with current records, identify changes, merge
- **T:** Verify history is preserved, test same-day corrections

### Scenario 3: "Optimize a slow-running query"
- **U:** What's the query? What columns are filtered? Table size?
- **D:** Check file count, partition strategy, statistics
- **A:** OPTIMIZE + Z-ORDER on filter columns, fix partitioning
- **C:** Run OPTIMIZE, ANALYZE TABLE, compare query plans
- **T:** Measure before/after, discuss ongoing maintenance

### Scenario 4: "Handle schema changes in source"
- **U:** What kind of changes? New columns? Type changes? Drops?
- **D:** Auto Loader with schema evolution → Bronze → validation → Silver
- **A:** `mergeSchema` for additive, alerting for breaking changes
- **C:** Configure Auto Loader's `schemaEvolutionMode`, add validation
- **T:** Test with new column, changed type, dropped column

### Scenario 5: "Build a real-time CDC pipeline"
- **U:** Source DB? Change format? Latency requirements?
- **D:** CDC source → Bronze (raw events) → Silver (materialized view)
- **A:** Streaming with foreachBatch MERGE, enable CDF for downstream
- **C:** Handle insert/update/delete operations in merge logic
- **T:** Test all operation types, verify ordering, discuss exactly-once

---

## Key Phrases to Use in Interviews

| Situation | What to Say |
|-----------|-------------|
| Starting | "Let me make sure I understand the requirements..." |
| Designing | "I'd use a medallion architecture here because..." |
| Choosing MERGE | "MERGE gives us exactly-once upsert semantics..." |
| Deduplicating | "I'll deduplicate before merging to avoid the multi-source-row error..." |
| Partitioning | "I'll partition by date for pruning and Z-ORDER by the lookup key..." |
| Schema changes | "Auto Loader with schema evolution handles new columns gracefully..." |
| Optimization | "To improve read performance, I'd OPTIMIZE with Z-ORDER on the most-queried columns..." |
| Edge cases | "One thing to watch for is late-arriving data — my conditional update handles this..." |
| Monitoring | "In production, I'd add data quality checks and a metrics table for observability..." |

---

Remember: Interviewers care MORE about your thinking process than the final code. Communicate at every step, explain trade-offs, and show that you understand not just the syntax but the WHY behind each decision.
