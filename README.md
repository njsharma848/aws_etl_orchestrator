# AWS ETL Orchestrator

A production-ready, config-driven ETL pipeline that ingests CSV files from S3, transforms them with PySpark, and loads them into Amazon Redshift Serverless. Supports both flat table ingestion and star schema dimensional modeling with SCD Type 1 and Type 2 tracking.

---

## Architecture

```
                                 AWS ETL Orchestrator
  +---------------------------------------------------------------------------+
  |                                                                           |
  |   +-------+     +-----------+     +-----------+     +--------+            |
  |   |  S3   |---->|EventBridge|---->| SQS FIFO  |---->| Lambda |            |
  |   | (CSV) |     |  (Rule)   |     | (Buffer)  |     | (Orch) |            |
  |   +-------+     +-----------+     +-----------+     +---+----+            |
  |       |                                |                |                 |
  |       |                           +----+----+      +----+------+          |
  |       |                           |   DLQ   |      |   Step    |          |
  |       |                           | (FIFO)  |      | Functions |          |
  |       |                           +---------+      +----+------+          |
  |       |                                                 |                 |
  |       |    +--------------------------------------------+                 |
  |       |    |                                                              |
  |       |    v                                                              |
  |   +---+--------+     +----------+     +-------+     +-------+            |
  |   | Glue (ETL) |---->| Redshift |     |  SNS  |     |Secrets|            |
  |   | PySpark    |     |Serverless|     |(Alert)|     |Manager|            |
  |   +------------+     +----------+     +-------+     +-------+            |
  |                                                                           |
  +---------------------------------------------------------------------------+
```

### Data Flow

1. **Ingest** -- A CSV file lands in `s3://{bucket}/data/in/`
2. **Detect** -- EventBridge captures the `Object Created` event
3. **Buffer** -- SQS FIFO queue guarantees ordered, exactly-once delivery
4. **Route** -- Lambda matches the file against `config.json` job definitions
5. **Orchestrate** -- Step Functions launches the Glue ETL job and waits for completion
6. **Transform** -- Glue reads the CSV, reconciles schema, and merges into Redshift
7. **Model** -- (Optional) Populates star schema dimensions and facts from `dimensional_mappings.json`
8. **Notify** -- SNS sends success/failure alerts

### Key Design Decisions

| Decision | Rationale |
|----------|-----------|
| **SQS FIFO** between EventBridge and Lambda | Guarantees ordered processing, prevents concurrent Lambda invocations, DLQ for failed messages |
| **Reserved concurrency = 1** on Lambda | Ensures serial file processing to avoid Redshift merge conflicts |
| **Step Functions sync integration** | `glue:startJobRun.sync` waits for job completion before proceeding |
| **Config-driven pipeline** | Add new ETL jobs by editing JSON configs -- no code changes required |
| **Exponential backoff retries** | Resilient Redshift Data API calls with `base_delay * 2^(attempt-1)` capping at 120s |

---

## Project Structure

```
aws_etl_orchestrator/
|
|-- .github/workflows/              # CI/CD pipelines
|   |-- ci.yml                      #   PR validation: lint + terraform plan
|   |-- deploy.yml                  #   Deploy: auto dev, manual qa/staging/prod
|   +-- destroy.yml                 #   Teardown: manual with confirmation
|
|-- configs/                         # Pipeline configuration (synced to S3)
|   |-- config.json                 #   Job definitions (source -> target mapping)
|   |-- config_view.json            #   Redshift view definitions
|   +-- dimensional_mappings.json   #   Star schema: dimensions + facts config
|
|-- glue_jobs/
|   |-- without_data_model/
|   |   +-- glue_job.py             #   Core ETL: CSV -> Redshift (flat tables)
|   +-- with_data_model/
|       +-- glue_job_with_data_model.py  # ETL + dimensional modeling (SCD)
|
|-- lambda_functions/
|   |-- lambda_function.py          #   Orchestrator: SQS -> config match -> Step Functions
|   +-- logs_to_smb.py              #   Log transfer: S3 -> SFTP
|
|-- state_machines/
|   +-- state_machine.json          #   Step Functions workflow definition
|
|-- terraform/
|   |-- main.tf                     #   Root module wiring all components
|   |-- variables.tf                #   Configurable parameters
|   |-- outputs.tf                  #   Stack outputs
|   |-- versions.tf                 #   Provider constraints
|   |-- environments/               #   Per-environment configuration
|   |   |-- dev/
|   |   |-- qa/
|   |   |-- staging/
|   |   +-- prod/
|   +-- modules/                    #   Reusable Terraform modules
|       |-- s3/                     #     Data lake bucket
|       |-- eventbridge/            #     S3 event rules
|       |-- sqs/                    #     FIFO queue + DLQ
|       |-- lambda/                 #     Orchestrator + log transfer functions
|       |-- iam/                    #     Roles and policies
|       |-- glue/                   #     ETL job definition
|       |-- step_functions/         #     State machine
|       +-- sns/                    #     Notification topics
|
|-- event_bridge_rules/
|   +-- event_bridge_rule.json      #   EventBridge pattern reference
|
|-- data_source/                     #   Sample CSV source files
|-- data_modeling/                   #   Star schema documentation
|   +-- terminologies/              #     Data modeling concepts (SCD, grain, etc.)
|
|-- warehouse_toolkits/              #   Utility libraries
|   |-- data_quality_schema/        #     Data quality checks + schema evolution
|   |-- historical_data/            #     Backfill utilities
|   |-- incremental_loading/        #     CDC and incremental patterns
|   +-- python_etl_essentials/      #     Python/PySpark reference material
|
|-- stored_procedures/                #   Redshift stored procedures
|   |-- 00_setup.sql                 #     Audit tables setup
|   |-- 01_utility_procedures.sql    #     Helper procedures
|   |-- 02_merge_upsert.sql         #     Core merge/upsert
|   |-- 03_scd_type2.sql            #     SCD Type 2 dimension processing
|   |-- 04_scd_type1.sql            #     SCD Type 1 dimension processing
|   |-- 05_fact_loader.sql          #     Fact table loader
|   |-- 06_audit_logging.sql        #     Job status logging + views
|   |-- 07_data_quality.sql         #     Data quality checks
|   +-- 08_table_maintenance.sql    #     VACUUM, ANALYZE, maintenance
|
|-- design_flow/                     #   Architecture workflow docs
|-- enhancements/                    #   Future enhancement notes
|-- investment_mgmts/                #   Domain documentation
+-- s3_directory_structure/          #   S3 bucket layout reference
```

---

## Configuration

### Job Definitions (`configs/config.json`)

Define ETL jobs that map source CSV files to Redshift tables:

```json
[
    {
        "job_id": "job_01",
        "job_name": "Job one",
        "source_file_name": "s3://ingestion/data/in/Products.csv",
        "target_table": "products",
        "upsert_keys": ["product_id", "time"],
        "is_active": true
    }
]
```

| Field | Description |
|-------|-------------|
| `job_id` | Unique identifier for the job |
| `job_name` | Human-readable name |
| `source_file_name` | S3 path to the expected source CSV |
| `target_table` | Target Redshift table name |
| `upsert_keys` | Columns used for MERGE (delete + insert) logic |
| `is_active` | Set `false` to disable without removing |

When a file arrives in S3 that doesn't match any config entry, Lambda moves it to `data/new_files/` and sends an SNS alert.

### Dimensional Mappings (`configs/dimensional_mappings.json`)

Configure star schema processing for source tables:

```json
[
    {
        "source_table": "aum_revenue",
        "dimensional_model": {
            "dimensions": [
                {
                    "dimension_table": "dim_fund",
                    "scd_type": 2,
                    "natural_keys": ["fund_code"],
                    "scd_attributes": ["fund_name", "fund_category"],
                    "column_mappings": {
                        "fund_code": "fund_code",
                        "fund_name": "fund_name",
                        "fund_category": "fund_category"
                    }
                }
            ],
            "facts": [
                {
                    "fact_table": "fact_aum_revenue",
                    "dimension_lookups": [
                        {
                            "dimension": "dim_fund",
                            "natural_keys": ["fund_code"],
                            "surrogate_key": "fund_key"
                        }
                    ],
                    "measures": ["aum_amount", "revenue_amount"],
                    "degenerate_dimensions": ["report_date"]
                }
            ]
        }
    }
]
```

**Supported SCD Types:**
- **Type 1** -- Overwrite on change (no history)
- **Type 2** -- Track history with `effective_date`, `end_date`, `is_current`, `version`

### View Definitions (`configs/config_view.json`)

Auto-create or refresh Redshift views after data loads:

```json
[
    {
        "view_name": "products_v",
        "source_table": "products",
        "schema_name": "operations",
        "definition": "CREATE OR REPLACE VIEW {schema_name}.{view_name} AS SELECT * FROM {schema_name}.{source_table}"
    }
]
```

---

## Environments

Four isolated environments with independent infrastructure:

| Environment | Glue Workers | Auto-Deploy | Protection |
|-------------|-------------|-------------|------------|
| **dev** | 2 | On push to `main` | None |
| **qa** | 2 | Manual dispatch | Optional |
| **staging** | 3 | Manual dispatch | Required |
| **prod** | 5 | Manual dispatch | Required |

Each environment has:
- Separate S3 bucket (`etl-orchestrator-{env}-data`)
- Separate Redshift workgroup and database
- Separate Terraform state backend (S3 + DynamoDB locking)
- Independent IAM roles scoped to that environment

---

## Deployment

### Prerequisites

- AWS account with appropriate permissions
- Terraform >= 1.7.0
- AWS CLI configured
- GitHub repository with OIDC configured for AWS

### Manual Deployment

```bash
# Initialize (first time only)
cd terraform/environments/dev
terraform init

# Plan
terraform plan -var-file=terraform.tfvars -out=tfplan

# Apply
terraform apply tfplan

# Upload application code to S3
BUCKET="etl-orchestrator-dev-data"
aws s3 cp glue_jobs/without_data_model/glue_job.py "s3://${BUCKET}/scripts/glue_job.py"
aws s3 cp glue_jobs/with_data_model/glue_job_with_data_model.py "s3://${BUCKET}/scripts/glue_job_with_data_model.py"
aws s3 sync configs/ "s3://${BUCKET}/config/"
```

### CI/CD Pipelines

**On Pull Request** (`ci.yml`):
- Lints Python code with flake8
- Validates Terraform configuration
- Runs `terraform plan` for dev environment

**On Push to main** (`deploy.yml`):
- Automatically deploys to `dev`
- Manual workflow dispatch for `qa`, `staging`, `prod`

**Teardown** (`destroy.yml`):
- Manual trigger with `yes` confirmation required
- Runs `terraform destroy` for the selected environment

### Required GitHub Secrets

| Secret | Description |
|--------|-------------|
| `AWS_ROLE_ARN` | IAM role ARN for OIDC authentication (deploy/destroy) |
| `AWS_ROLE_ARN_DEV` | IAM role ARN for CI plan operations |

---

## ETL Pipeline Details

### Schema Reconciliation

The Glue job automatically handles schema drift:

- **New columns in source** -- Added to Redshift via `ALTER TABLE ADD COLUMN`
- **Missing columns in source** -- Filled with type-appropriate defaults
- **VARCHAR too short** -- Column length expanded to fit new data
- **Integer overflow** -- Automatically promoted (`INTEGER` -> `BIGINT`)
- **Views rebuilt** -- Dependent views dropped and recreated after schema changes

### Merge Strategy

Uses a delete-then-insert pattern within a transaction:

```sql
BEGIN TRANSACTION;
    DELETE FROM target WHERE upsert_keys IN (SELECT upsert_keys FROM staging);
    INSERT INTO target SELECT * FROM staging;
    DROP TABLE staging;
COMMIT;
```

### S3 File Lifecycle

```
data/in/file.csv          -- Source file lands here (triggers pipeline)
data/staging/{table}/      -- Temporary staging data (auto-cleaned)
data/archive/YYYY/MM/      -- Successfully processed files
data/unprocessed/YYYY/MM/  -- Files that failed processing
data/new_files/            -- Files with no matching config
logs/YYYY/MM/DD/           -- Structured JSON log files
```

---

## SQS Concurrency Control

```
                   +-------------------+
EventBridge -----> | SQS FIFO Queue    |-----> Lambda (concurrency=1)
                   | (ordered, dedup)  |
                   +--------+----------+
                            |
                            | maxReceiveCount=3
                            v
                   +-------------------+
                   | Dead Letter Queue |
                   | (14-day retain)   |
                   +-------------------+
```

| Parameter | Value | Rationale |
|-----------|-------|-----------|
| Queue type | FIFO | Order preservation for dependent files |
| Deduplication | Content-based | Prevents duplicate processing |
| Visibility timeout | 360s | 6x Lambda timeout (60s) for safety margin |
| Message retention | 4 days | Buffer for extended outages |
| DLQ max receives | 3 | Three attempts before DLQ |
| DLQ retention | 14 days | Debugging window for failed messages |

---

## Terraform Modules

| Module | Resources | Description |
|--------|-----------|-------------|
| `s3` | Bucket, versioning, lifecycle, event notification | Data lake with EventBridge integration |
| `eventbridge` | Rule, SQS target | S3 `Object Created` events in `data/in/` |
| `sqs` | FIFO queue, DLQ, queue policy | Buffered event delivery with failure handling |
| `lambda` | 2 functions, event source mapping | Orchestrator + SFTP log transfer |
| `iam` | 4 roles, policies | Least-privilege roles for each service |
| `glue` | Job definition | PySpark ETL with configurable workers |
| `step_functions` | State machine | Serial job orchestration |
| `sns` | Topic, subscription | Email alerts for pipeline events |

---

## Adding a New ETL Job

1. **Add job definition** to `configs/config.json`:
   ```json
   {
       "job_id": "job_04",
       "job_name": "New Data Feed",
       "source_file_name": "s3://ingestion/data/in/NewFeed.csv",
       "target_table": "new_feed",
       "upsert_keys": ["id", "timestamp"],
       "is_active": true
   }
   ```

2. **(Optional) Add dimensional model** to `configs/dimensional_mappings.json` if star schema processing is needed.

3. **(Optional) Add view** to `configs/config_view.json` for downstream consumers.

4. **Deploy** -- Push to `main` (auto-deploys to dev) or trigger manual deploy.

5. **Upload source file** to `s3://{bucket}/data/in/NewFeed.csv` to trigger the pipeline.

No code changes required -- the pipeline is entirely config-driven.

---

## Stored Procedures

Redshift stored procedures are provided in `stored_procedures/` and should be run in order:

| File | Procedures | Description |
|------|-----------|-------------|
| `00_setup.sql` | Tables: `job_sts`, `data_quality_results` | Creates audit and DQ tables (run first) |
| `01_utility_procedures.sql` | `sp_check_table_exists`, `sp_get_row_count`, `sp_create_staging_table`, `sp_copy_from_s3`, `sp_drop_view_if_exists`, `sp_alter_table_add_column`, `sp_alter_varchar_length`, `sp_widen_integer_column` | Lightweight helpers used across the pipeline |
| `02_merge_upsert.sql` | `sp_merge_from_staging` | Core delete-then-insert merge with dynamic key parsing |
| `03_scd_type2.sql` | `sp_process_scd_type2` | SCD Type 2: expire changed records, insert new versions, insert new members |
| `04_scd_type1.sql` | `sp_process_scd_type1` | SCD Type 1: overwrite attributes, insert new members |
| `05_fact_loader.sql` | `sp_load_fact_table` | Fact loading with dimension surrogate key lookups |
| `06_audit_logging.sql` | `sp_log_job_status`, `sp_log_job_start`, views: `v_job_history`, `v_job_summary` | ETL audit trail and monitoring dashboards |
| `07_data_quality.sql` | `sp_dq_check_nulls`, `sp_dq_check_duplicates`, `sp_dq_check_row_count`, `sp_dq_check_freshness`, `sp_dq_check_referential_integrity`, view: `v_data_quality_dashboard` | Automated data quality checks with result logging |
| `08_table_maintenance.sql` | `sp_vacuum_table`, `sp_analyze_table`, `sp_maintenance_all_etl_tables`, `sp_purge_old_audit_records`, view: `v_table_health` | VACUUM, ANALYZE, scheduled maintenance, audit cleanup |

### Installation

```bash
# Connect to Redshift and run in order:
psql -h <workgroup-endpoint> -d <database> -U <user> \
  -f stored_procedures/00_setup.sql \
  -f stored_procedures/01_utility_procedures.sql \
  -f stored_procedures/02_merge_upsert.sql \
  -f stored_procedures/03_scd_type2.sql \
  -f stored_procedures/04_scd_type1.sql \
  -f stored_procedures/05_fact_loader.sql \
  -f stored_procedures/06_audit_logging.sql \
  -f stored_procedures/07_data_quality.sql \
  -f stored_procedures/08_table_maintenance.sql
```

---

## License

This project is proprietary. All rights reserved.
