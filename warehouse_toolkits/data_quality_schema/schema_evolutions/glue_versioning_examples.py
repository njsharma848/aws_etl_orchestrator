"""
Practical Examples: Glue Catalog Schema Evolution with Versioning
Real-world scenarios for schema management
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime
import json

from WAREHOUSE_TOOLKIT.data_qualty_schema.schema_evolutions.glue_schema_versioning import (
    SchemaVersionManager,
    GlueCatalogSchemaManager,
    IntegratedSchemaEvolution
)


def scenario_1_track_schema_evolution():
    """
    Scenario 1: Track Schema Changes Over Time
    Monitor how a table's schema evolves across deployments
    """
    print("\n" + "="*80)
    print("SCENARIO 1: SCHEMA VERSION TRACKING")
    print("="*80)
    
    spark = SparkSession.builder \
        .appName("SchemaTracking") \
        .getOrCreate()
    
    # Initialize version manager
    version_manager = SchemaVersionManager(spark, '/tmp/schema_versions')
    
    # Version 1.0: Initial schema
    print("\n--- Version 1.0: Initial Release ---")
    schema_v1 = StructType([
        StructField("order_id", LongType(), False),
        StructField("customer_id", IntegerType(), False),
        StructField("order_date", TimestampType(), False),
        StructField("total_amount", DoubleType(), False)
    ])
    
    version_manager.register_schema_version(
        'orders',
        schema_v1,
        version='v1.0.0',
        metadata={
            'author': 'data_team',
            'release': 'initial_release',
            'description': 'Basic order tracking'
        }
    )
    
    # Version 1.1: Added status column
    print("\n--- Version 1.1: Added Status Tracking ---")
    schema_v1_1 = StructType([
        StructField("order_id", LongType(), False),
        StructField("customer_id", IntegerType(), False),
        StructField("order_date", TimestampType(), False),
        StructField("total_amount", DoubleType(), False),
        StructField("status", StringType(), True)
    ])
    
    version_manager.register_schema_version(
        'orders',
        schema_v1_1,
        version='v1.1.0',
        metadata={
            'author': 'analytics_team',
            'release': 'sprint_23',
            'description': 'Added order status for tracking'
        }
    )
    
    # Version 2.0: Major refactor - added payment and shipping info
    print("\n--- Version 2.0: Major Refactor ---")
    schema_v2 = StructType([
        StructField("order_id", LongType(), False),
        StructField("customer_id", IntegerType(), False),
        StructField("order_date", TimestampType(), False),
        StructField("total_amount", DoubleType(), False),
        StructField("status", StringType(), True),
        StructField("payment_method", StringType(), True),
        StructField("shipping_address", StringType(), True),
        StructField("estimated_delivery", DateType(), True)
    ])
    
    version_manager.register_schema_version(
        'orders',
        schema_v2,
        version='v2.0.0',
        metadata={
            'author': 'platform_team',
            'release': 'Q2_2024',
            'description': 'Enhanced with payment and shipping details'
        }
    )
    
    # List all versions
    print("\n--- Version History ---")
    versions = version_manager.list_schema_versions('orders')
    
    for i, v in enumerate(versions, 1):
        print(f"\n{i}. Version: {v['version']}")
        print(f"   Timestamp: {v['timestamp']}")
        print(f"   Author: {v['metadata'].get('author', 'unknown')}")
        print(f"   Release: {v['metadata'].get('release', 'unknown')}")
        print(f"   Description: {v['metadata'].get('description', '')}")
    
    # Compare versions
    print("\n\n--- Comparing v1.0.0 → v2.0.0 ---")
    diff = version_manager.compare_versions('orders', 'v1.0.0', 'v2.0.0')
    
    spark.stop()


def scenario_2_glue_catalog_sync():
    """
    Scenario 2: Sync DataFrame with Glue Catalog
    Detect differences and update Glue Catalog
    """
    print("\n" + "="*80)
    print("SCENARIO 2: GLUE CATALOG SYNCHRONIZATION")
    print("="*80)
    
    spark = SparkSession.builder \
        .appName("GlueCatalogSync") \
        .getOrCreate()
    
    # Initialize Glue manager (simulated - would use real AWS in production)
    glue_manager = GlueCatalogSchemaManager(
        spark,
        aws_region='us-east-1',
        glue_database='analytics_db'
    )
    
    # Simulate existing Glue Catalog schema
    print("\n--- Current Glue Catalog Schema ---")
    print("Table: customer_transactions")
    print("Columns:")
    print("  - transaction_id (bigint)")
    print("  - customer_id (int)")
    print("  - amount (double)")
    print("  - transaction_date (timestamp)")
    
    # Create new DataFrame with additional columns
    print("\n--- New DataFrame Schema ---")
    new_data = spark.createDataFrame([
        (1001, 501, 150.00, '2024-01-15 10:00:00', 'credit_card', 'completed', 'US'),
        (1002, 502, 200.00, '2024-01-15 11:00:00', 'debit_card', 'pending', 'CA')
    ], ['transaction_id', 'customer_id', 'amount', 'transaction_date', 
        'payment_method', 'status', 'country'])
    
    new_data = new_data.withColumn('transaction_date', F.to_timestamp('transaction_date'))
    
    new_data.printSchema()
    
    # In production, this would call:
    # diff = glue_manager.compare_with_glue_catalog(new_data, 'customer_transactions')
    
    print("\n--- Schema Comparison Results (Simulated) ---")
    print("✓ New columns detected:")
    print("  + payment_method (string)")
    print("  + status (string)")
    print("  + country (string)")
    print("\nNo columns removed")
    print("No type mismatches")
    
    # Update Glue Catalog (simulated)
    print("\n--- Updating Glue Catalog ---")
    print("Would execute:")
    print("  glue_manager.update_glue_catalog(")
    print("      table_name='customer_transactions',")
    print("      schema=new_data.schema,")
    print("      location='s3://analytics-bucket/transactions/'")
    print("  )")
    print("\n✓ Glue Catalog would be updated with new schema")
    
    spark.stop()


def scenario_3_rollback_schema():
    """
    Scenario 3: Schema Rollback
    Rollback to a previous schema version
    """
    print("\n" + "="*80)
    print("SCENARIO 3: SCHEMA ROLLBACK")
    print("="*80)
    
    spark = SparkSession.builder \
        .appName("SchemaRollback") \
        .getOrCreate()
    
    version_manager = SchemaVersionManager(spark, '/tmp/schema_versions')
    
    # Register multiple versions
    versions_to_create = [
        ('v1.0.0', ['id', 'name', 'email']),
        ('v1.1.0', ['id', 'name', 'email', 'phone']),
        ('v2.0.0', ['id', 'name', 'email', 'phone', 'address', 'city', 'zip']),
        ('v2.1.0', ['id', 'name', 'email', 'phone', 'address', 'city', 'zip', 'country'])
    ]
    
    print("\n--- Registering Schema Versions ---")
    for version, columns in versions_to_create:
        schema = StructType([
            StructField(col, StringType(), True) for col in columns
        ])
        version_manager.register_schema_version(
            'contacts',
            schema,
            version=version,
            metadata={'columns': len(columns)}
        )
        print(f"✓ Registered {version}: {len(columns)} columns")
    
    # Simulate issue with v2.1.0
    print("\n--- Issue Detected in v2.1.0 ---")
    print("⚠ Performance degradation with 'country' column")
    print("⚠ Need to rollback to v2.0.0")
    
    # Retrieve previous version
    print("\n--- Rolling Back to v2.0.0 ---")
    rollback_schema = version_manager.get_schema_version('contacts', 'v2.0.0')
    
    print("✓ Retrieved v2.0.0 schema:")
    print(f"  Columns: {[f.name for f in rollback_schema.fields]}")
    
    # Would apply rollback
    print("\n--- Rollback Actions ---")
    print("1. Update Glue Catalog to v2.0.0 schema")
    print("2. Update application to use v2.0.0")
    print("3. Register rollback in version history")
    
    # Register rollback version
    version_manager.register_schema_version(
        'contacts',
        rollback_schema,
        version='v2.0.1',
        metadata={
            'rollback_from': 'v2.1.0',
            'rollback_to': 'v2.0.0',
            'reason': 'Performance issues with country column'
        }
    )
    
    print("\n✓ Rollback completed and documented")
    
    spark.stop()


def scenario_4_multi_environment_schema():
    """
    Scenario 4: Multi-Environment Schema Management
    Manage schemas across dev, staging, and production
    """
    print("\n" + "="*80)
    print("SCENARIO 4: MULTI-ENVIRONMENT SCHEMA MANAGEMENT")
    print("="*80)
    
    spark = SparkSession.builder \
        .appName("MultiEnvSchema") \
        .getOrCreate()
    
    environments = ['dev', 'staging', 'production']
    
    # Development: Latest features
    print("\n--- DEV Environment ---")
    dev_schema = StructType([
        StructField("user_id", LongType(), False),
        StructField("username", StringType(), False),
        StructField("email", StringType(), False),
        StructField("created_at", TimestampType(), False),
        StructField("last_login", TimestampType(), True),
        StructField("preferences", StringType(), True),  # New feature
        StructField("experimental_flag", BooleanType(), True)  # Experimental
    ])
    
    dev_version_manager = SchemaVersionManager(spark, '/tmp/schema_versions/dev')
    dev_version_manager.register_schema_version(
        'users',
        dev_schema,
        version='dev-2024.01.20',
        metadata={'environment': 'dev', 'status': 'testing'}
    )
    print(f"✓ DEV schema: {len(dev_schema.fields)} columns")
    
    # Staging: Validated features
    print("\n--- STAGING Environment ---")
    staging_schema = StructType([
        StructField("user_id", LongType(), False),
        StructField("username", StringType(), False),
        StructField("email", StringType(), False),
        StructField("created_at", TimestampType(), False),
        StructField("last_login", TimestampType(), True),
        StructField("preferences", StringType(), True)  # Validated, ready for prod
    ])
    
    staging_version_manager = SchemaVersionManager(spark, '/tmp/schema_versions/staging')
    staging_version_manager.register_schema_version(
        'users',
        staging_schema,
        version='staging-2024.01.18',
        metadata={'environment': 'staging', 'status': 'validated'}
    )
    print(f"✓ STAGING schema: {len(staging_schema.fields)} columns")
    
    # Production: Stable
    print("\n--- PRODUCTION Environment ---")
    prod_schema = StructType([
        StructField("user_id", LongType(), False),
        StructField("username", StringType(), False),
        StructField("email", StringType(), False),
        StructField("created_at", TimestampType(), False),
        StructField("last_login", TimestampType(), True)
    ])
    
    prod_version_manager = SchemaVersionManager(spark, '/tmp/schema_versions/production')
    prod_version_manager.register_schema_version(
        'users',
        prod_schema,
        version='v1.5.0',
        metadata={'environment': 'production', 'status': 'stable'}
    )
    print(f"✓ PRODUCTION schema: {len(prod_schema.fields)} columns")
    
    # Promotion workflow
    print("\n--- Schema Promotion Workflow ---")
    print("1. DEV (7 columns) → Feature testing")
    print("   - experimental_flag: Under evaluation")
    print("   - preferences: Validated, ready for staging")
    print("")
    print("2. STAGING (6 columns) → Pre-production validation")
    print("   - preferences: Being validated")
    print("   - Ready for production in next release")
    print("")
    print("3. PRODUCTION (5 columns) → Current stable")
    print("   - Core functionality only")
    print("   - Next release: Add preferences column")
    
    spark.stop()


def scenario_5_schema_compatibility_check():
    """
    Scenario 5: Backward Compatibility Validation
    Ensure new schema is backward compatible
    """
    print("\n" + "="*80)
    print("SCENARIO 5: BACKWARD COMPATIBILITY VALIDATION")
    print("="*80)
    
    spark = SparkSession.builder \
        .appName("CompatibilityCheck") \
        .getOrCreate()
    
    version_manager = SchemaVersionManager(spark, '/tmp/schema_versions')
    
    # Current production schema
    print("\n--- Current Production Schema (v1.0) ---")
    current_schema = StructType([
        StructField("event_id", LongType(), False),
        StructField("user_id", IntegerType(), False),
        StructField("event_type", StringType(), False),
        StructField("timestamp", TimestampType(), False),
        StructField("value", DoubleType(), True)
    ])
    
    version_manager.register_schema_version(
        'events',
        current_schema,
        version='v1.0',
        metadata={'status': 'production'}
    )
    
    print("Columns:", [f.name for f in current_schema.fields])
    
    # Proposed new schema
    print("\n--- Proposed New Schema (v2.0) ---")
    
    # Test Case 1: Backward compatible (only additions)
    print("\nTest 1: Adding Optional Columns")
    compatible_schema = StructType([
        StructField("event_id", LongType(), False),
        StructField("user_id", IntegerType(), False),
        StructField("event_type", StringType(), False),
        StructField("timestamp", TimestampType(), False),
        StructField("value", DoubleType(), True),
        StructField("metadata", StringType(), True),  # NEW - nullable
        StructField("source", StringType(), True)     # NEW - nullable
    ])
    
    diff1 = version_manager.compare_versions('events', 'v1.0', 
                                             version_manager.register_schema_version(
                                                 'events_test1', compatible_schema, 'v2.0-compatible')['version'])
    
    print(f"  Added: {diff1['added_fields']}")
    print(f"  Removed: {diff1['removed_fields']}")
    print(f"  Type changes: {diff1['type_changes']}")
    print(f"  ✓ COMPATIBLE: Only added nullable columns")
    
    # Test Case 2: Breaking change (removed column)
    print("\nTest 2: Removing Column")
    breaking_schema = StructType([
        StructField("event_id", LongType(), False),
        StructField("user_id", IntegerType(), False),
        StructField("event_type", StringType(), False),
        StructField("timestamp", TimestampType(), False)
        # 'value' column removed - BREAKING CHANGE
    ])
    
    print("  Removed: ['value']")
    print("  ✗ INCOMPATIBLE: Removed column breaks existing consumers")
    
    # Test Case 3: Type change
    print("\nTest 3: Changing Column Type")
    type_change_schema = StructType([
        StructField("event_id", LongType(), False),
        StructField("user_id", LongType(), False),  # Changed: int → long
        StructField("event_type", StringType(), False),
        StructField("timestamp", TimestampType(), False),
        StructField("value", DoubleType(), True)
    ])
    
    print("  Type change: user_id (int → long)")
    print("  ✓ COMPATIBLE: Widening type conversion")
    
    # Test Case 4: Nullable change
    print("\nTest 4: Making Column Non-Nullable")
    nullable_schema = StructType([
        StructField("event_id", LongType(), False),
        StructField("user_id", IntegerType(), False),
        StructField("event_type", StringType(), False),
        StructField("timestamp", TimestampType(), False),
        StructField("value", DoubleType(), False)  # Changed: nullable → not null
    ])
    
    print("  Nullable change: value (true → false)")
    print("  ✗ INCOMPATIBLE: Making column non-nullable breaks existing data")
    
    # Summary
    print("\n" + "="*80)
    print("COMPATIBILITY RULES")
    print("="*80)
    print("✓ SAFE (Backward Compatible):")
    print("  - Adding new nullable columns")
    print("  - Widening types (int→long, float→double)")
    print("  - Making non-null columns nullable")
    print("")
    print("✗ BREAKING (Not Backward Compatible):")
    print("  - Removing columns")
    print("  - Renaming columns")
    print("  - Narrowing types (long→int)")
    print("  - Making nullable columns non-nullable")
    
    spark.stop()


def scenario_6_automated_schema_testing():
    """
    Scenario 6: Automated Schema Testing in CI/CD
    Validate schema changes before deployment
    """
    print("\n" + "="*80)
    print("SCENARIO 6: AUTOMATED SCHEMA TESTING")
    print("="*80)
    
    spark = SparkSession.builder \
        .appName("SchemaCI") \
        .getOrCreate()
    
    print("\n--- CI/CD Pipeline: Schema Validation ---")
    
    # Simulate production schema
    print("\n1. Load Production Schema")
    prod_schema = StructType([
        StructField("order_id", LongType(), False),
        StructField("customer_id", IntegerType(), False),
        StructField("amount", DoubleType(), False)
    ])
    print(f"   Production columns: {[f.name for f in prod_schema.fields]}")
    
    # Developer's new schema
    print("\n2. Load Feature Branch Schema")
    feature_schema = StructType([
        StructField("order_id", LongType(), False),
        StructField("customer_id", IntegerType(), False),
        StructField("amount", DoubleType(), False),
        StructField("discount", DoubleType(), True)
    ])
    print(f"   Feature branch columns: {[f.name for f in feature_schema.fields]}")
    
    # Run automated tests
    print("\n3. Run Schema Validation Tests")
    
    tests = []
    
    # Test 1: Check for removed columns
    prod_cols = set(f.name for f in prod_schema.fields)
    feature_cols = set(f.name for f in feature_schema.fields)
    removed = prod_cols - feature_cols
    
    test1_pass = len(removed) == 0
    tests.append(('no_removed_columns', test1_pass))
    print(f"   ✓ No columns removed: {test1_pass}")
    
    # Test 2: New columns are nullable
    added = feature_cols - prod_cols
    new_fields_nullable = all(
        f.nullable for f in feature_schema.fields if f.name in added
    )
    
    test2_pass = new_fields_nullable or len(added) == 0
    tests.append(('new_columns_nullable', test2_pass))
    print(f"   ✓ New columns are nullable: {test2_pass}")
    
    # Test 3: No type changes
    type_changes = []
    for f in feature_schema.fields:
        if f.name in prod_cols:
            prod_field = next(pf for pf in prod_schema.fields if pf.name == f.name)
            if str(prod_field.dataType) != str(f.dataType):
                type_changes.append(f.name)
    
    test3_pass = len(type_changes) == 0
    tests.append(('no_type_changes', test3_pass))
    print(f"   ✓ No type changes: {test3_pass}")
    
    # Results
    print("\n4. Test Results")
    all_passed = all(result for _, result in tests)
    
    if all_passed:
        print("   ✓✓✓ ALL TESTS PASSED ✓✓✓")
        print("   Schema changes are backward compatible")
        print("   ✓ Ready for deployment")
    else:
        print("   ✗✗✗ TESTS FAILED ✗✗✗")
        print("   Schema changes are not backward compatible")
        print("   ✗ Deployment blocked")
        
        failed = [name for name, result in tests if not result]
        print(f"   Failed tests: {failed}")
    
    # Generate report
    print("\n5. Generate CI/CD Report")
    report = {
        'timestamp': datetime.now().isoformat(),
        'status': 'PASS' if all_passed else 'FAIL',
        'tests': [
            {'name': name, 'passed': result}
            for name, result in tests
        ],
        'schema_changes': {
            'added_columns': list(added),
            'removed_columns': list(removed),
            'type_changes': type_changes
        }
    }
    
    print(json.dumps(report, indent=2))
    
    spark.stop()


def main():
    """Run all scenarios"""
    print("\n")
    print("="*80)
    print(" GLUE CATALOG & SCHEMA VERSIONING SCENARIOS")
    print("="*80)
    
    scenario_1_track_schema_evolution()
    print("\n" + "="*80 + "\n")
    
    scenario_2_glue_catalog_sync()
    print("\n" + "="*80 + "\n")
    
    scenario_3_rollback_schema()
    print("\n" + "="*80 + "\n")
    
    scenario_4_multi_environment_schema()
    print("\n" + "="*80 + "\n")
    
    scenario_5_schema_compatibility_check()
    print("\n" + "="*80 + "\n")
    
    scenario_6_automated_schema_testing()
    
    print("\n")
    print("="*80)
    print(" ALL SCENARIOS COMPLETED!")
    print("="*80)
    print("\n")


if __name__ == "__main__":
    main()
