# Handling Late-Arriving Data in Data Warehousing

Late-arriving data is a common challenge in data warehousing. Here are the primary strategies for handling it:

## Late-Arriving Dimensions

When dimension records arrive after their associated facts:

### 1. Default/Dummy Records
- Create placeholder dimension records (e.g., "Unknown Customer")
- Facts initially reference these dummy records
- Update foreign keys once actual dimension data arrives

### 2. Inferred Members
- Create skeletal dimension records with minimal information
- Populate additional attributes when complete data arrives
- Mark records with flags like `IsInferred = TRUE`

## Late-Arriving Facts

When fact records arrive after their normal load window:

### 1. Separate Late-Arriving Fact Tables
- Maintain a dedicated table for late arrivals
- Union views combine on-time and late-arriving facts
- Periodic consolidation into main fact table

### 2. Reprocessing Windows
- Keep load windows open for specific periods (e.g., 7-30 days)
- Allow facts to be inserted during this grace period
- Rerun aggregations and reports as needed

### 3. Version/Audit Columns
- Track `LoadDate`, `BusinessDate`, and `LastModifiedDate`
- Helps identify and handle late arrivals
- Enables temporal queries

## Best Practices

- **Monitoring**: Alert on late-arriving data volumes and patterns
- **SLAs**: Define acceptable latency windows with business stakeholders
- **Idempotent Loads**: Design processes to handle re-runs safely
- **Slowly Changing Dimensions (SCD)**: Type 2 SCD naturally accommodates late updates
- **Event Time vs. Processing Time**: Distinguish when events occurred vs. when they were processed

## Implementation Considerations

### For Your AWS Glue Pipeline

Given your current infrastructure, here's how you might implement late-arrival handling:

1. **Watermark-based detection**: Use your existing DynamoDB watermark tracking to identify late-arriving records by comparing `business_date` vs `load_date`

2. **Separate processing path**: Create a dedicated Step Functions workflow for late-arrival processing that:
   - Detects records with `business_date` older than the watermark
   - Routes them to late-arrival staging tables
   - Triggers reconciliation jobs

3. **Incremental loading strategy**: Your timestamp-based and CDC strategies can naturally handle late arrivals if configured with appropriate lookback windows

4. **Data quality checks**: Extend your quality framework to flag and report late arrivals for monitoring

### Example Configuration

```json
{
  "table_name": "fact_revenue",
  "incremental_strategy": "timestamp_based",
  "timestamp_column": "business_date",
  "lookback_window_days": 7,
  "late_arrival_handling": {
    "enabled": true,
    "detection_method": "business_vs_load_date",
    "staging_table": "fact_revenue_late_arrivals",
    "reconciliation_schedule": "daily"
  }
}
```

This approach maintains your configuration-driven design while adding late-arrival capabilities.