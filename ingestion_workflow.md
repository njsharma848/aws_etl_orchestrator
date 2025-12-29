# AWS Data Ingestion Workflow - Complete Documentation

## Table of Contents
1. [Overview](#overview)
2. [Testing Strategy](#testing-strategy)
3. [Architecture Components](#architecture-components)
4. [Workflow Diagram](#workflow-diagram)
5. [Configuration File Structure](#configuration-file-structure)
6. [Detailed Process Flow](#detailed-process-flow)
7. [AWS Services Used](#aws-services-used)
8. [Validation Points](#validation-points)

---

## Overview

This document describes a complete data ingestion workflow across AWS components for processing files from S3 through various AWS services to Redshift Data Warehouse. The architecture implements an event-driven, serverless pipeline with automatic schema detection, validation, transformation, and loading capabilities.

---

## Testing Strategy

The testing strategy focuses on validating the complete ingestion workflow across AWS components. The following validations will be carried out:

- File delivery to S3 (in folder)
- EventBridge trigger execution
- Lambda validation using config.json
- SNS alert for new file detection
- Step Function workflow initiation and parameter passing
- Glue job execution with retry logic
- Schema inference for source files
- Schema validation against Redshift target
- Redshift table creation for new files
- Table alteration for schema mismatch
- Addition of audit columns in processed files
- COPY command execution to Redshift staging
- Merge/UPSERT logic from staging to target table
- Data validation (record counts, null checks, duplicate checks)
- JOB_STS audit table logging
- CloudWatch log generation
- S3 logs generation
- Shared drive log replication
- File archival to S3 archive folder with timestamp
- Cleanup of staging files upon successful load
- Reconciliation validation

---

## Architecture Components

### Primary AWS Services
- **Amazon S3** - Input storage, staging area, archive, and logs
- **Amazon EventBridge** - Event-driven trigger mechanism
- **AWS Lambda** - Orchestration and validation logic
- **AWS Step Functions** - Workflow management and parameter passing
- **AWS Glue** - ETL job execution with retry logic
- **Amazon SNS** - Alert notifications
- **Amazon Redshift** - Target data warehouse
- **Amazon CloudWatch** - Centralized logging and monitoring

---

## Workflow Diagram

### High-Level Architecture Flow
```
┌──────────┐     ┌──────────────┐     ┌────────────┐     ┌──────────────┐
│  Start   │────▶│  S3: Input   │────▶│ EventBridge│────▶│   Lambda     │
└──────────┘     └──────────────┘     └────────────┘     └──────────────┘
                                                                   │
                                                                   ▼
                                                          ┌──────────────┐
                                                          │ Config File  │
                                                          └──────────────┘
                                                                   │
                    ┌──────────────────────────────────────────────┴─────────────────┐
                    │                                                                  │
                    ▼                                                                  ▼
        ┌───────────────────────┐                                        ┌──────────────────────┐
        │  New File & Table     │                                        │  Old File with       │
        │  Doesn't Exist        │                                        │  Matching Schema     │
        └───────────────────────┘                                        └──────────────────────┘
                    │                                                                  │
                    ▼                                                                  ▼
        ┌───────────────────────┐                                        ┌──────────────────────┐
        │  Raise SNS Alert      │                                        │  Step Function       │
        │  for Configuration    │                                        └──────────────────────┘
        └───────────────────────┘                                                    │
                                                                                     ▼
                                                                         ┌──────────────────────┐
                                                                         │     Glue Job         │
                                                                         └──────────────────────┘
                                                                                     │
                    ┌────────────────────────────────────────────────────────────────┘
                    │
                    ▼
        ┌───────────────────────┐
        │  Schema Matching?     │
        └───────────────────────┘
                    │
        ┌───────────┴───────────┐
        │                       │
       YES                     NO
        │                       │
        ▼                       ▼
┌──────────────┐    ┌──────────────────────────┐
│ S3 Staging   │    │ Alter Redshift Table     │
│ (with audit  │    │ as per inferred schema   │
│  columns)    │    └──────────────────────────┘
└──────────────┘                │
        │                       │
        │                       ▼
        │              ┌──────────────┐
        │              │ S3 Staging   │
        │              │ (add audit)  │
        │              └──────────────┘
        │                       │
        └───────────┬───────────┘
                    │
                    ▼
        ┌───────────────────────┐
        │   Redshift COPY       │
        └───────────────────────┘
                    │
                    ▼
        ┌───────────────────────┐
        │   Redshift DW         │
        └───────────────────────┘
                    │
                    ▼
        ┌───────────────────────┐
        │   COPY Failed?        │
        └───────────────────────┘
                    │
        ┌───────────┴───────────┐
        │                       │
       YES                     NO
        │                       │
        ▼                       ▼
┌──────────────┐    ┌──────────────────────────┐
│ Insert log   │    │ Load to staging tables   │
│ into JOB_STS │    │ Merge with Target table  │
│ Table        │    │ (upsert logic)           │
└──────────────┘    └──────────────────────────┘
        │                       │
        │                       ▼
        │              ┌──────────────┐
        │              │ Insert log   │
        │              │ into JOB_STS │
        │              │ Table        │
        │              └──────────────┘
        │                       │
        └───────────┬───────────┘
                    │
                    ▼
        ┌───────────────────────┐
        │   Send Logs to:       │
        │   - CloudWatch        │
        │   - S3 log bucket     │
        │   - Shared Drive Path │
        └───────────────────────┘
                    │
                    ▼
        ┌───────────────────────┐
        │   Job End             │
        └───────────────────────┘
```

---

## Configuration File Structure

The Lambda function uses a configuration file (config.json) to manage job parameters and processing logic.

### Config File Parameters:

**job_id** – Unique identifier for the ETL or data processing job

**job_name** – Descriptive name of the job for readability and logging

**source_file_path** – Location or pattern of the source data file in S3 or filesystem

**target_table** – Destination table where transformed data will be loaded

**update_keys** – Keys for upsert logic (used only in insert/update strategies)

**is_active** – Boolean flag indicating whether the job is enabled for execution

### Example Configuration:
```json
{
  "job_id": "job_001",
  "job_name": "Customer_Data_Ingestion",
  "source_file_path": "s3://input-bucket/customer_data/*.csv",
  "target_table": "customers",
  "update_keys": ["customer_id"],
  "is_active": true
}
```

---

## Detailed Process Flow

### Step 1: File Arrival and Initial Processing

**S3 Input Configuration:**
- For CSV files: header=true, inferSchema=true
- Normalize column names (lowercase, underscores)

**Flow:**
```
File arrives in S3 → EventBridge detects event → Triggers Lambda function
```

### Step 2: Lambda Validation and Decision Logic

The Lambda function performs initial validation and determines the processing path based on three scenarios:

#### Scenario A: New File and Table Doesn't Exist

**Action:** Raise SNS alert for the team to configure the config file

**Process:**
- Lambda detects new file type with no existing configuration
- SNS notification sent to support team
- Once config file is updated manually by support team, the process validates the schema and creates tables if required

#### Scenario B: File Exists with Matching Schema (Old file with matching schema)

**Process:**
- Lambda validates file against existing configuration
- Triggers Step Function by passing job parameters
- Step Function triggers Glue Job instance
- Glue Job processes the particular source file
- Data loaded to S3 staging

#### Scenario C: File Exists with Non-Matching Schema (Old file with not matching schema and constraints)

**Process:**
- Lambda detects schema mismatch
- Schema validation and target table schema changes performed
- Process handled in Glue job
- Logs sent for monitoring

**Important Notes:**
- System infers schema from source file for creation of target table if table info is not in config file or table not created by support team for new file
- System casts data types to Redshift datatypes or maps datatypes to corresponding Redshift datatypes to avoid datatype discrepancy (especially in case of bool, date data type)

### Step 3: Step Function Workflow

The Step Function orchestrates the workflow and manages the following:

**Responsibilities:**
- Workflow initiation and parameter passing
- Triggering Glue Job with appropriate parameters
- Managing decision logic for schema matching
- Error handling and retry logic

### Step 4: Glue Job Execution

**Glue Job performs the following operations:**

1. **Schema Inference** - Automatically detect schema from source files
2. **Schema Validation** - Validate against Redshift target schema
3. **Table Operations:**
   - Create new tables for new files
   - Alter existing tables for schema mismatches
4. **Data Processing:**
   - Add audit columns to processed files
   - Apply data transformations
5. **Data Validation:**
   - Record counts
   - Null checks
   - Duplicate checks
6. **Retry Logic** - Automatic retry on transient failures

**Output:** Processed data with audit fields loaded to S3 staging area

### Step 5: Schema Matching Decision

**If Schema Matches (YES):**
```
Glue Job → S3 staging (with audit columns) → Redshift COPY
```

**If Schema Doesn't Match (NO):**
```
Glue Job → Alter Redshift table → Update schema → S3 staging (add audit columns) → Redshift COPY
```

### Step 6: Redshift COPY Operation

**Process:**
```
S3 staging → Redshift COPY command → Redshift DW
```

**COPY Command Features:**
- Bulk loading from S3 to Redshift
- Atomic operation (all or nothing)
- Automatic compression
- Parallel processing

### Step 7: COPY Status Check and Error Handling

#### If COPY Failed (YES):

**Process:**
- Redshift manages rollbacks on failure (Atomic in nature)
- Insert failure log into JOB_STS Table
- Send logs to monitoring systems

**Logging Destinations:**
- AWS CloudWatch
- AWS S3 logs bucket
- Shared Drive Path

#### If COPY Succeeded (NO):

**Process:**
1. Load data to staging tables
2. Merge with Target table using upsert logic
3. Insert success log into JOB_STS Table
4. Update JOB_STS Table with processing metrics

### Step 8: Merge/UPSERT Logic

**Process:**
```
Staging table → Apply merge/upsert logic → Target table
```

**UPSERT Logic:**
- Uses update_keys from configuration
- Updates existing records
- Inserts new records
- Maintains data integrity

### Step 9: Audit and Logging

**JOB_STS Table Fields:**
- **Job_ID** - Unique job identifier
- **Start_time** - Job start timestamp
- **End_time** - Job completion timestamp
- **Job_status** - Success/Failure status
- **Error_Message** - Error details if failed
- **Records_processed** - Total records processed
- **Records_inserted** - Total records inserted
- **Reconciliation_sts** - Reconciliation status

**Logging to Multiple Destinations:**
1. **CloudWatch Logs** - Real-time monitoring and alerting
2. **AWS S3 logs** - Long-term storage and analysis
3. **Shared Drive Path** - Cross-system log replication

### Step 10: Cleanup and Archival

**Final Actions:**
1. **File Archival** - Move processed files to S3 archive folder with timestamp
2. **Staging Cleanup** - Remove staging files upon successful load
3. **Reconciliation Validation** - Verify data integrity
4. **Job Completion** - Mark job as complete in JOB_STS table

---

## AWS Services Used

### 1. Amazon S3 (Simple Storage Service)
**Purpose:** Storage layer for input files, staging area, archives, and logs

**Buckets:**
- Input bucket - Raw file landing zone
- Staging bucket - Intermediate processed data
- Archive bucket - Historical file storage with timestamps
- Logs bucket - Application and process logs

### 2. Amazon EventBridge
**Purpose:** Event-driven trigger mechanism

**Features:**
- Detects new file arrivals in S3
- Triggers Lambda functions automatically
- Supports event filtering and routing

### 3. AWS Lambda
**Purpose:** Serverless orchestration and validation logic

**Responsibilities:**
- File validation against configuration
- Schema validation
- Decision logic for processing paths
- Trigger Step Functions
- SNS notifications

### 4. Amazon SNS (Simple Notification Service)
**Purpose:** Alert notifications

**Use Cases:**
- New file type detection
- Configuration required alerts
- Failure notifications
- Team notifications

### 5. AWS Step Functions
**Purpose:** Workflow orchestration and state management

**Features:**
- Manages complex workflows
- Parameter passing between services
- Error handling and retry logic
- Visual workflow monitoring

### 6. AWS Glue
**Purpose:** ETL (Extract, Transform, Load) processing

**Capabilities:**
- Schema inference and cataloging
- Data transformation
- Schema validation
- Retry logic for transient failures
- Audit column addition

### 7. Amazon Redshift
**Purpose:** Cloud data warehouse for analytical queries

**Features:**
- Columnar storage
- Massively parallel processing
- COPY command for bulk loading
- Atomic transactions with rollback
- Schema evolution support

### 8. Amazon CloudWatch
**Purpose:** Monitoring and observability

**Features:**
- Centralized logging
- Real-time monitoring
- Alerting and notifications
- Performance metrics
- Log aggregation

---

## Validation Points

### 1. Data Ingestion Validation
- File delivery to S3 (in folder)
- File format and structure validation
- File size and integrity checks

### 2. Trigger and Orchestration Validation
- EventBridge trigger execution
- Lambda function invocation
- Step Function workflow initiation and parameter passing

### 3. Configuration and Schema Validation
- Lambda validation using config.json
- Schema inference for source files
- Schema validation against Redshift target
- Table creation for new files
- Table alteration for schema mismatch

### 4. Notification Validation
- SNS alert for new file detection
- Error notifications
- Status updates

### 5. ETL Processing Validation
- Glue job execution with retry logic
- Addition of audit columns in processed files
- Data transformation accuracy

### 6. Data Loading Validation
- COPY command execution to Redshift staging
- Staging table population
- Merge/UPSERT logic from staging to target table

### 7. Data Quality Validation
- Record counts validation
- Null checks
- Duplicate checks
- Data type validation
- Reconciliation validation

### 8. Audit and Logging Validation
- JOB_STS audit table logging
- CloudWatch log generation
- S3 logs generation
- Shared drive log replication

### 9. Cleanup and Archival Validation
- File archival to S3 archive folder with timestamp
- Cleanup of staging files upon successful load
- Retention policy compliance

---

## Error Handling and Retry Mechanisms

### Lambda Error Handling
- Validation errors trigger SNS alerts
- Configuration errors logged to CloudWatch
- Automatic retry for transient failures

### Glue Job Retry Logic
- Configurable retry attempts
- Exponential backoff strategy
- Error logging for debugging

### Redshift Error Handling
- Automatic rollback on COPY failure
- Transaction management
- Detailed error logging in JOB_STS table

### Step Function Error Handling
- State-based error handling
- Catch and retry patterns
- Fallback mechanisms

---

## Performance Considerations

### Optimization Strategies

1. **Parallel Processing**
   - Glue jobs support parallel processing
   - Redshift COPY command uses parallel loading
   - Step Functions can execute parallel branches

2. **Data Partitioning**
   - S3 data organized by date/time partitions
   - Efficient data pruning in queries

3. **Compression**
   - Redshift automatic compression
   - S3 object compression for storage efficiency

4. **Resource Allocation**
   - Glue DPU (Data Processing Units) configuration
   - Redshift cluster sizing
   - Lambda memory allocation

---

## Security Considerations

### IAM Roles and Permissions
- Least privilege access for all services
- Service-specific IAM roles
- Cross-service access policies

### Data Encryption
- S3 server-side encryption
- Redshift encryption at rest
- SSL/TLS for data in transit

### Network Security
- VPC configuration for Redshift
- Private subnets for sensitive resources
- Security groups and NACLs

### Audit and Compliance
- CloudTrail logging for API calls
- S3 access logging
- Redshift audit logging

---

## Monitoring and Alerting

### CloudWatch Dashboards
- Real-time workflow monitoring
- Performance metrics visualization
- Error rate tracking

### Alerts Configuration
- Failed job notifications
- Performance threshold alerts
- Data quality issue alerts
- Schema mismatch notifications

### Log Analysis
- Centralized log aggregation
- Log retention policies
- Search and analytics capabilities

---

## Summary

This AWS data ingestion workflow provides a robust, scalable, and automated solution for processing files from S3 to Redshift. Key features include:

✓ **Event-Driven Architecture** - Automatic processing on file arrival
✓ **Schema Evolution** - Dynamic schema detection and adaptation
✓ **Data Quality** - Comprehensive validation checks
✓ **Error Handling** - Retry logic and rollback mechanisms
✓ **Audit Trail** - Complete logging in JOB_STS table
✓ **Monitoring** - Multi-layer logging to CloudWatch, S3, and shared drives
✓ **Scalability** - Serverless components scale automatically
✓ **Cost Optimization** - Pay-per-use model with AWS services

The architecture ensures data integrity, reliability, and observability throughout the entire ingestion pipeline.

---

## Appendix

### File Naming Conventions
- Input files: `{source_system}_{entity}_{timestamp}.{extension}`
- Archive files: `{original_name}_{archive_timestamp}.{extension}`
- Log files: `{job_id}_{execution_timestamp}.log`

### Supported File Formats
- CSV (with header)
- JSON
- Parquet
- Avro

### Data Type Mappings
Special attention to data type discrepancies:
- Boolean types mapped correctly
- Date formats standardized
- Numeric precision maintained
- String encoding handled

---

**Document Version:** 1.0  
**Last Updated:** December 2025  
**Page:** 1

---
