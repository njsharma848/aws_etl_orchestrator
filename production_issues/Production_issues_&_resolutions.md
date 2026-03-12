
# AWS ETL Orchestrator: Critical Production Issues and Solutions

Below are the most important production issues expected in this ETL orchestrator along with real scenarios, root causes, and solutions. The explanations are written from a production-grade platform engineering perspective similar to what a MAANG-level engineer would discuss during architecture reviews.

---

## 1. Redshift Merge Conflicts and Table-Level Contention

### Issue
Even with SQS FIFO and Lambda concurrency = 1, conflicts can occur when:
- Multiple files target the same table in close succession
- A Glue job is still running while another begins
- Long-running transactions hold locks
- SCD loads and fact loads overlap

### Scenario
Two files arrive quickly:
- Products_10_00.csv
- Products_10_03.csv

The first Glue job starts a transaction.  
The second job tries to delete/insert into the same table causing lock waits or deadlocks.

### Solution
Implement table-level orchestration locks:
- Maintain a lock record per table using DynamoDB or an audit table
- Prevent parallel execution on the same table

Enhancement:
- Step Functions states: AcquireLock → RunGlue → ReleaseLock
- Add idempotency keys using file name + etag.

---

## 2. Duplicate Event Delivery

### Issue
AWS systems provide **at-least-once delivery**, not exactly-once.

Duplicates may occur due to:
- EventBridge duplicate delivery
- SQS retries
- Lambda timeout retries
- Step Function invocation retries

### Scenario
S3 uploads a file once but two EventBridge events are produced.
Two Step Function executions start and process the same file.

### Solution
Implement file-level idempotency.

Create audit table:

etl_file_audit
- file_id
- bucket
- object_key
- etag
- job_id
- status
- execution_id
- received_ts
- completed_ts

Only allow a file to run if its status is not SUCCESS.

Enable S3 versioning and process using object version IDs.

---

## 3. Schema Drift Breaking Glue or Redshift

### Issue
Upstream systems change schema unexpectedly.

Examples:
- Integer column becomes string
- New column added
- Column renamed
- Date format changed

### Scenario
customer_id previously numeric suddenly becomes 'C12345'.  
Glue infers string while Redshift expects BIGINT causing load failure.

### Solution
Introduce schema contracts.

Validation categories:
Safe:
- nullable column add
- varchar expansion

Breaking:
- key column change
- datatype change
- column removal

Add a schema validation step before loading data.

---

## 4. Incorrect SCD Type 2 History

### Issue
SCD Type 2 logic can create incorrect history if data normalization is not handled.

### Scenario
Two records appear different due to trailing spaces or case differences.  
Pipeline interprets this as a change and creates a new dimension version.

### Solution
Normalize values before comparison:
- trim strings
- normalize case
- convert null/blank consistently

Use a **hash of tracked attributes** to detect real changes.

Also validate:
- only one current record per natural key
- no overlapping date ranges.

---

## 5. Incorrect Surrogate Key Resolution in Facts

### Issue
Fact loads depend on correct dimension lookup.

### Scenario
Fact arrives before dimension update or dimension history is incorrect.  
Fact resolves wrong surrogate key.

### Solution
Perform dimension lookup using:

natural_key AND fact_date BETWEEN effective_date AND end_date

Use fallback **unknown dimension member** for unresolved lookups.

---

## 6. Lambda Timeout and Message Reprocessing

### Issue
Lambda times out before message acknowledgement.

SQS makes message visible again causing duplicate orchestration.

### Scenario
Lambda starts Step Function but times out before updating audit table.

### Solution
Lambda should only:
- validate event
- check idempotency
- start Step Function
- return quickly

Use deterministic Step Function execution names to prevent duplicates.

---

## 7. Poison Files Blocking Queue

### Issue
FIFO queues with concurrency 1 can block when a single bad file repeatedly fails.

### Scenario
Malformed CSV causes Glue failure three times before moving to DLQ, delaying subsequent files.

### Solution
Classify failures:

Permanent:
- malformed CSV
- schema violation

Transient:
- Redshift lock
- AWS throttling

Route permanent failures immediately to quarantine location.

---

## 8. Redshift Serverless Performance Issues

### Issue
Delete + insert merges are expensive on large tables.

### Scenario
Large dataset triggers full table scans leading to long runtimes and increased compute cost.

### Solution
Optimize:
- distribution key
- sort key
- partition-based replacement strategies

Monitor:
- query runtime
- queue wait time
- workgroup utilization.

---

## 9. Stored Procedure Partial Failures

### Issue
Stored procedures may partially complete leaving inconsistent state.

### Scenario
Merge succeeds but audit logging fails resulting in duplicate processing during retries.

### Solution
Define clear pipeline phases:

1. job_start
2. staging_load
3. merge_target
4. dq_checks
5. view_refresh
6. job_complete

Each phase should write audit status.

---

## 10. View Recreation Causing BI Failures

### Issue
Dropping and recreating views introduces downtime.

### Scenario
ETL drops a view while BI dashboards query it.

### Solution
Use:

CREATE OR REPLACE VIEW

Avoid drop statements where possible.

---

## 11. Configuration Drift

### Issue
Config-driven pipelines can break due to incorrect configuration changes.

### Scenario
Upsert keys are accidentally changed causing incorrect deduplication.

### Solution
Treat configuration as code:
- JSON schema validation
- CI validation checks
- peer review before deployment.

---

## 12. File Routing Errors

### Issue
File naming mismatches prevent pipeline routing.

### Scenario
Pipeline expects Products.csv but upstream sends Products_20260311.csv.

### Solution
Use regex or prefix-based routing instead of strict filename matching.

---

## 13. Data Quality Checks Too Late

### Issue
Data quality validation runs only after loading.

### Scenario
Invalid records enter warehouse before validation fails.

### Solution
Introduce:

Pre-load checks
- schema validation
- record count check
- header/trailer validation

Post-load checks
- referential integrity
- anomaly detection.

---

## 14. Weak Observability

### Issue
Alerts lack enough context to debug failures quickly.

### Solution
Log correlation identifiers:
- S3 object key
- Lambda request ID
- Step Function execution ID
- Glue job run ID
- Redshift query ID

Monitor metrics:
- queue backlog
- job latency
- failure rates.

---

## 15. Environment Drift

### Issue
Dev environment differs from production causing runtime surprises.

### Solution
Maintain environment parity:
- same IAM structure
- similar dataset size testing
- automated validation tests across environments.

---

## Top 5 Critical Risks

1. Duplicate processing due to at-least-once delivery
2. Redshift table contention
3. Schema drift
4. Incorrect SCD Type 2 logic
5. Poison files blocking the pipeline

---

## Closing Perspective

A production-grade data platform must prioritize:

- Idempotent processing
- Strong schema governance
- Deterministic dimension modeling
- Robust observability
- Config validation

These controls transform an ETL pipeline into a reliable enterprise data platform.
