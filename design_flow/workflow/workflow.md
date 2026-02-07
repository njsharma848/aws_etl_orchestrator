# Workflow

- **File delivery to S3 (in folder)**
- **EventBridge trigger execution**
- **Lambda validation using config.json**
- **SNS alert for new file detection**
- **Step Function workflow initiation and parameter passing**
- **Glue job execution with retry logic**
- **Schema inference for source files**
- **Schema validation against Redshift target**
- **Redshift table creation for new files**
- **Table alteration for schema mismatch**
- **Addition of audit columns in processed files**
- **COPY command execution to Redshift staging**
- **Merge/UPSERT logic from staging to target table**
- **Data validation (record counts, null checks, duplicate checks)**
- **JOB_STS audit table logging**
- **CloudWatch log generation**
- **S3 logs generation**
- **Shared drive log replication**
- **File archival to S3 archive folder with timestamp**
- **Cleanup of staging files upon successful load**
- **Reconciliation validation**

---
