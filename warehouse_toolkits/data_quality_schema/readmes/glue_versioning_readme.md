# Complete Schema Evolution Suite

Comprehensive PySpark framework for managing schema evolution with Redshift, AWS Glue Catalog, and schema versioning.

## Overview

This suite provides three integrated modules for complete schema management:

1. **Schema Evolution** - Compare and migrate schemas between DataFrames and Redshift
2. **Glue Catalog Integration** - Sync schemas with AWS Glue Catalog metadata
3. **Schema Versioning** - Track and manage schema changes over time

## Table of Contents

- [Quick Start](#quick-start)
- [Module 1: Schema Evolution](#module-1-schema-evolution)
- [Module 2: Glue Catalog Integration](#module-2-glue-catalog-integration)
- [Module 3: Schema Versioning](#module-3-schema-versioning)
- [Integrated Workflows](#integrated-workflows)
- [Best Practices](#best-practices)
- [Real-World Scenarios](#real-world-scenarios)

---

## Quick Start

### Installation

```bash
pip install pyspark boto3
```

### Basic Usage

```python
from pyspark.sql import SparkSession
from glue_schema_versioning import IntegratedSchemaEvolution

spark = SparkSession.builder.appName("SchemaEvolution").getOrCreate()

# Initialize integrated manager
manager = IntegratedSchemaEvolution(
    spark,
    version_storage_path='s3://bucket/schema-versions/',
    aws_region='us-east-1',
    glue_database='my_database'
)

# Complete schema evolution workflow
result = manager.evolve_schema_with_versioning(
    source_df=my_dataframe,
    table_name='customers',
    glue_table_name='customers',
    auto_update_glue=True,
    metadata={'author': 'data_team', 'sprint': 'Q1-2024'}
)
```

---

## Module 1: Schema Evolution

Compare schemas between DataFrames and Redshift, generate migration DDL.

### Features

✅ Compare DataFrame schemas with Redshift tables  
✅ Detect added/removed columns, type changes, nullable modifications  
✅ Generate DDL for Redshift migrations  
✅ Four evolution strategies (STRICT, ADD_ONLY, FLEXIBLE, PERMISSIVE)  
✅ Align DataFrames to target schemas  

### Basic Example

```python
from schema_evolution import SchemaEvolutionManager, EvolutionStrategy

manager = SchemaEvolutionManager(spark, redshift_config={
    'host': 'cluster.region.redshift.amazonaws.com',
    'database': 'mydb',
    'user': 'username',
    'password': 'password'
})

# Compare schemas
diff = manager.compare_schemas(source_df, 'customers')

# Generate migration DDL
result = manager.apply_schema_evolution(
    source_df,
    'customers',
    strategy=EvolutionStrategy.FLEXIBLE,
    dry_run=True
)
```

### Evolution Strategies

| Strategy | Add | Drop | Type Change | Use Case |
|----------|-----|------|-------------|----------|
| STRICT | ❌ | ❌ | ❌ | Production critical tables |
| ADD_ONLY | ✅ | ❌ | ❌ | Append-only, backward compatible |
| FLEXIBLE | ✅ | ❌ | ✅ (safe) | Most production scenarios |
| PERMISSIVE | ✅ | ✅ | ✅ (all) | Development/testing |

---

## Module 2: Glue Catalog Integration

Sync schemas with AWS Glue Catalog, the central metadata repository for data lakes.

### Features

✅ Retrieve schema metadata from Glue Catalog  
✅ Compare DataFrame schemas with Glue tables  
✅ Update Glue Catalog with new schemas  
✅ Support for partitioned tables  
✅ Glue Catalog version history  
✅ Type mapping between Spark and Glue  

### Why Use Glue Catalog?

- **Central metadata store** for S3 data lakes
- **Integration** with Athena, EMR, Glue ETL, Redshift Spectrum
- **Automatic schema discovery** via Glue Crawlers
- **Version tracking** built-in to Glue
- **Governance** and data cataloging

### Basic Example

```python
from glue_schema_versioning import GlueCatalogSchemaManager

glue_manager = GlueCatalogSchemaManager(
    spark,
    aws_region='us-east-1',
    glue_database='analytics'
)

# Compare with Glue Catalog
diff = glue_manager.compare_with_glue_catalog(
    source_df=my_dataframe,
    glue_table_name='customer_events'
)

# Update Glue Catalog
success = glue_manager.update_glue_catalog(
    table_name='customer_events',
    schema=my_dataframe.schema,
    location='s3://bucket/data/customer_events/',
    partition_keys=['year', 'month', 'day']
)
```

### Glue Catalog Operations

#### Get Schema from Glue

```python
# Retrieve table schema
glue_schema = glue_manager.get_glue_table_schema('orders')

# Returns:
{
    'table_name': 'orders',
    'database': 'analytics',
    'columns': {
        'order_id': {'type': 'bigint', 'comment': ''},
        'customer_id': {'type': 'int', 'comment': ''},
        'amount': {'type': 'double', 'comment': ''}
    },
    'location': 's3://bucket/orders/',
    'input_format': '...',
    'output_format': '...'
}
```

#### Update Glue Catalog

```python
# Create or update table
glue_manager.update_glue_catalog(
    table_name='transactions',
    schema=df.schema,
    location='s3://data-lake/transactions/',
    partition_keys=['year', 'month'],
    table_type='EXTERNAL_TABLE',
    input_format='org.apache.hadoop.mapred.TextInputFormat',
    output_format='org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
    serde_info={
        'SerializationLibrary': 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe',
        'Parameters': {'serialization.format': '1'}
    }
)
```

#### Compare with Glue Catalog

```python
diff = glue_manager.compare_with_glue_catalog(
    source_df=new_data,
    glue_table_name='orders'
)

# Returns:
{
    'added_columns': ['shipping_address', 'estimated_delivery'],
    'removed_columns': [],
    'type_changes': [
        {
            'column': 'customer_id',
            'source_type': 'LongType',
            'glue_type': 'int',
            'compatible': True
        }
    ],
    'glue_location': 's3://bucket/orders/',
    'glue_parameters': {...}
}
```

### Type Mapping

#### Spark to Glue

| Spark Type | Glue Type |
|------------|-----------|
| IntegerType | int |
| LongType | bigint |
| ShortType | smallint |
| FloatType | float |
| DoubleType | double |
| DecimalType | decimal |
| StringType | string |
| BooleanType | boolean |
| DateType | date |
| TimestampType | timestamp |
| BinaryType | binary |

#### Glue to Spark

Compatible type mappings for reading from Glue:

```python
glue_to_spark_types = {
    'int': IntegerType(),
    'bigint': LongType(),
    'string': StringType(),
    'double': DoubleType(),
    'timestamp': TimestampType(),
    # ... etc
}
```

---

## Module 3: Schema Versioning

Track schema changes over time with full version history.

### Features

✅ Register schema versions with metadata  
✅ Retrieve specific schema versions  
✅ Compare versions to see evolution  
✅ List version history  
✅ Support for rollbacks  
✅ Multi-environment version management  

### Basic Example

```python
from glue_schema_versioning import SchemaVersionManager

version_manager = SchemaVersionManager(
    spark,
    version_storage_path='s3://bucket/schema-versions/'
)

# Register a version
version_manager.register_schema_version(
    table_name='orders',
    schema=df.schema,
    version='v1.5.0',
    metadata={
        'author': 'data_team',
        'sprint': 'Q1-2024',
        'description': 'Added payment tracking'
    }
)

# List all versions
versions = version_manager.list_schema_versions('orders')

# Compare versions
diff = version_manager.compare_versions(
    'orders',
    from_version='v1.0.0',
    to_version='v1.5.0'
)
```

### Version Operations

#### Register Schema Version

```python
version_record = version_manager.register_schema_version(
    table_name='customers',
    schema=customer_schema,
    version='v2.1.0',  # Auto-generated if None
    metadata={
        'author': 'analytics_team',
        'release': 'sprint_45',
        'description': 'Added loyalty program fields',
        'jira_ticket': 'DATA-1234'
    }
)

# Returns:
{
    'table_name': 'customers',
    'version': 'v2.1.0',
    'schema': '<schema_json>',
    'timestamp': '2024-01-15 10:30:00',
    'metadata': {...}
}
```

#### List Version History

```python
versions = version_manager.list_schema_versions('customers')

# Returns list of versions (newest first):
[
    {
        'version': 'v2.1.0',
        'timestamp': '2024-01-15 10:30:00',
        'metadata': {'author': 'analytics_team', ...}
    },
    {
        'version': 'v2.0.0',
        'timestamp': '2024-01-01 08:00:00',
        'metadata': {'author': 'data_team', ...}
    },
    ...
]
```

#### Compare Versions

```python
diff = version_manager.compare_versions(
    table_name='orders',
    from_version='v1.0.0',
    to_version='v2.0.0'
)

# Returns:
{
    'added_fields': ['payment_method', 'shipping_address'],
    'removed_fields': [],
    'type_changes': [
        {
            'field': 'customer_id',
            'from_type': 'IntegerType()',
            'to_type': 'LongType()'
        }
    ],
    'nullable_changes': [
        {
            'field': 'email',
            'from_nullable': True,
            'to_nullable': False
        }
    ]
}
```

#### Schema Rollback

```python
# Retrieve previous version
previous_schema = version_manager.get_schema_version(
    table_name='orders',
    version='v1.5.0'
)

# Register rollback
version_manager.register_schema_version(
    table_name='orders',
    schema=previous_schema,
    version='v1.5.1',
    metadata={
        'rollback_from': 'v2.0.0',
        'rollback_to': 'v1.5.0',
        'reason': 'Performance regression in v2.0.0'
    }
)
```

---

## Integrated Workflows

The `IntegratedSchemaEvolution` class combines all three modules for complete schema management.

### Complete Evolution Workflow

```python
from glue_schema_versioning import IntegratedSchemaEvolution

manager = IntegratedSchemaEvolution(
    spark,
    version_storage_path='s3://bucket/schema-versions/',
    aws_region='us-east-1',
    glue_database='analytics'
)

result = manager.evolve_schema_with_versioning(
    source_df=new_customer_data,
    table_name='customers',
    glue_table_name='customers',
    auto_update_glue=True,
    metadata={
        'author': 'data_team',
        'sprint': 'Q1-2024',
        'description': 'Added customer segmentation'
    }
)

# Result contains:
{
    'glue_comparison': {...},      # Diff with Glue Catalog
    'new_version': {...},          # New version if schema changed
    'glue_updated': True/False,    # Whether Glue was updated
    'version_history': [...]       # All versions for this table
}
```

### What This Does

1. **Compare with Glue Catalog** - Detect schema differences
2. **Check Version History** - See if schema changed from last version
3. **Register New Version** - Save new schema version if changed
4. **Update Glue Catalog** - Optionally update Glue metadata
5. **Return Results** - Complete summary of changes

---

## Best Practices

### 1. Version Naming Convention

```python
# Semantic versioning
'v1.0.0'  # Major.Minor.Patch

# Date-based versioning
'v2024.01.15'  # Year.Month.Day

# Environment + version
'prod-v1.5.0'
'staging-v2.0.0-beta'

# Auto-generated timestamps
'v20240115_103045'  # Default if no version provided
```

### 2. Metadata Best Practices

Always include:
```python
metadata = {
    'author': 'team_name',           # Who made the change
    'description': 'What changed',   # Brief description
    'jira_ticket': 'DATA-1234',     # Tracking ticket
    'release': 'Q1-2024',           # Release identifier
    'breaking_change': False         # Is it backward compatible?
}
```

### 3. Multi-Environment Strategy

```python
# Separate version stores per environment
dev_manager = SchemaVersionManager(spark, 's3://bucket/schemas/dev/')
staging_manager = SchemaVersionManager(spark, 's3://bucket/schemas/staging/')
prod_manager = SchemaVersionManager(spark, 's3://bucket/schemas/prod/')

# Version naming by environment
dev: 'dev-2024.01.20'
staging: 'staging-2024.01.18'
production: 'v1.5.0'
```

### 4. Backward Compatibility Checks

```python
def is_backward_compatible(from_schema, to_schema):
    """Check if schema change is backward compatible"""
    
    from_fields = {f.name: f for f in from_schema.fields}
    to_fields = {f.name: f for f in to_schema.fields}
    
    # Check 1: No removed columns
    removed = set(from_fields.keys()) - set(to_fields.keys())
    if removed:
        return False, f"Removed columns: {removed}"
    
    # Check 2: New columns must be nullable
    added = set(to_fields.keys()) - set(from_fields.keys())
    for field_name in added:
        if not to_fields[field_name].nullable:
            return False, f"New column {field_name} is not nullable"
    
    # Check 3: No type narrowing
    for field_name in set(from_fields.keys()) & set(to_fields.keys()):
        from_type = str(from_fields[field_name].dataType)
        to_type = str(to_fields[field_name].dataType)
        
        if from_type != to_type:
            # Check if widening (safe) or narrowing (unsafe)
            if not is_type_widening(from_type, to_type):
                return False, f"Type narrowing in {field_name}: {from_type} → {to_type}"
    
    return True, "Schema is backward compatible"
```

### 5. CI/CD Integration

```python
# In your CI/CD pipeline
def schema_validation_tests(new_schema, table_name):
    """Automated schema tests for CI/CD"""
    
    # Get production schema
    prod_schema = version_manager.get_schema_version(table_name)
    
    # Test 1: Backward compatibility
    is_compatible, msg = is_backward_compatible(prod_schema, new_schema)
    assert is_compatible, f"Breaking change detected: {msg}"
    
    # Test 2: No duplicate column names
    col_names = [f.name for f in new_schema.fields]
    assert len(col_names) == len(set(col_names)), "Duplicate columns"
    
    # Test 3: Required columns present
    required = {'id', 'created_at', 'updated_at'}
    actual = set(col_names)
    assert required.issubset(actual), f"Missing required columns: {required - actual}"
    
    print("✓ All schema validation tests passed")
```

---

## Real-World Scenarios

### Scenario 1: Data Lake to Data Warehouse Migration

```python
# 1. Read from S3 data lake with Glue Catalog
glue_manager = GlueCatalogSchemaManager(spark, glue_database='datalake')
lake_schema = glue_manager.get_glue_table_schema('raw_events')
lake_df = spark.read.parquet('s3://lake/raw_events/')

# 2. Compare with Redshift warehouse
redshift_manager = SchemaEvolutionManager(spark, redshift_config)
diff = redshift_manager.compare_schemas(lake_df, 'warehouse.events')

# 3. Generate migration DDL
migration = redshift_manager.generate_migration_ddl(
    'warehouse.events',
    diff,
    lake_df,
    strategy=EvolutionStrategy.FLEXIBLE
)

# 4. Register version
version_manager.register_schema_version(
    'events',
    lake_df.schema,
    metadata={'source': 'datalake', 'target': 'warehouse'}
)
```

### Scenario 2: Schema Registry for Microservices

```python
# Each microservice registers its event schemas
services = ['orders', 'payments', 'shipping', 'notifications']

for service in services:
    event_schema = get_service_schema(service)
    
    version_manager.register_schema_version(
        table_name=f'{service}_events',
        schema=event_schema,
        version=f'{service}-v1.0.0',
        metadata={
            'service': service,
            'team': f'{service}_team',
            'api_version': '1.0'
        }
    )

# Consumers can retrieve specific versions
orders_schema_v1 = version_manager.get_schema_version('orders_events', 'orders-v1.0.0')
```

### Scenario 3: Blue-Green Schema Deployment

```python
# Blue (current production)
blue_schema = version_manager.get_schema_version('users', 'v2.0.0')

# Green (new version)
green_schema = StructType([...])  # New schema with changes

# Validate compatibility
is_compatible, msg = is_backward_compatible(blue_schema, green_schema)

if is_compatible:
    # Deploy green
    version_manager.register_schema_version('users', green_schema, 'v2.1.0')
    glue_manager.update_glue_catalog('users', green_schema, 's3://...')
    
    # Update routing to green
    update_routing('green')
else:
    print(f"Deployment blocked: {msg}")
    # Keep blue active
```

### Scenario 4: Schema Governance and Auditing

```python
# Audit all schema changes in last 30 days
all_tables = ['customers', 'orders', 'products', 'transactions']

audit_report = []
for table in all_tables:
    versions = version_manager.list_schema_versions(table)
    
    recent_versions = [
        v for v in versions 
        if datetime.strptime(v['timestamp'], '%Y-%m-%d %H:%M:%S') > 
           datetime.now() - timedelta(days=30)
    ]
    
    audit_report.append({
        'table': table,
        'changes_last_30_days': len(recent_versions),
        'latest_version': versions[0] if versions else None,
        'authors': list(set(v['metadata'].get('author') for v in recent_versions))
    })

# Generate compliance report
generate_compliance_report(audit_report)
```

---

## Troubleshooting

### Common Issues

**Issue: Glue Catalog connection fails**
```python
# Solution: Verify AWS credentials and region
import boto3

# Test connection
glue = boto3.client('glue', region_name='us-east-1')
response = glue.get_databases()
print("Glue connection successful:", response)
```

**Issue: Version storage path not accessible**
```python
# Solution: Verify S3 permissions
# Ensure EMR/Spark has read/write access to version storage path
spark.conf.set("spark.hadoop.fs.s3a.access.key", "YOUR_KEY")
spark.conf.set("spark.hadoop.fs.s3a.secret.key", "YOUR_SECRET")
```

**Issue: Schema comparison shows false positives**
```python
# Solution: Type mapping differences
# Some types may map differently between systems
# Use strict comparison only for critical fields

# Example: Decimal precision differences
# Glue: decimal(10,2)
# Spark: DecimalType(38,18)
# These may be functionally equivalent
```

---

## API Reference

### SchemaVersionManager

```python
register_schema_version(table_name, schema, version=None, metadata=None)
get_schema_version(table_name, version=None)
list_schema_versions(table_name)
compare_versions(table_name, from_version, to_version)
```

### GlueCatalogSchemaManager

```python
get_glue_table_schema(table_name)
compare_with_glue_catalog(source_df, glue_table_name)
update_glue_catalog(table_name, schema, location, ...)
get_table_versions(table_name)
glue_schema_to_spark(glue_schema)
```

### IntegratedSchemaEvolution

```python
evolve_schema_with_versioning(source_df, table_name, glue_table_name=None, 
                              auto_update_glue=False, metadata=None)
```

---

## License

MIT License

## Support

For issues or questions, please open a GitHub issue.
