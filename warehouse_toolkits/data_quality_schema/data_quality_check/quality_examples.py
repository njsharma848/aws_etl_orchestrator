"""
Practical Examples for Schema Evolution and Data Quality
Real-world scenarios with huge data volumes
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime, timedelta
import random

from WAREHOUSE_TOOLKIT.data_qualty_schema.schema_evolutions.schema_evolution import SchemaEvolutionManager, EvolutionStrategy
from data_quality import DataQualityFramework


def scenario_1_schema_evolution_migration():
    """
    Scenario 1: Data Warehouse Migration with Schema Evolution
    Source system schema changed, need to adapt target schema
    """
    print("\n" + "="*80)
    print("SCENARIO 1: SCHEMA EVOLUTION DURING MIGRATION")
    print("="*80)
    
    spark = SparkSession.builder \
        .appName("SchemaEvolution") \
        .getOrCreate()
    
    # Original source schema (Version 1)
    print("\n--- Version 1: Original Schema ---")
    orders_v1 = spark.createDataFrame([
        (1, 101, '2024-01-01 10:00:00', 150.00, 'completed'),
        (2, 102, '2024-01-02 11:00:00', 200.00, 'shipped'),
        (3, 103, '2024-01-03 12:00:00', 175.00, 'pending')
    ], ['order_id', 'customer_id', 'order_date', 'total_amount', 'status'])
    
    orders_v1 = orders_v1.withColumn('order_date', F.to_timestamp('order_date'))
    print("Original columns:", orders_v1.columns)
    
    # New source schema (Version 2) - added columns
    print("\n--- Version 2: Schema with New Columns ---")
    orders_v2 = spark.createDataFrame([
        (4, 104, '2024-01-04 13:00:00', 300.00, 'completed', 'credit_card', 'US', 'standard'),
        (5, 105, '2024-01-05 14:00:00', 250.00, 'shipped', 'paypal', 'CA', 'express')
    ], ['order_id', 'customer_id', 'order_date', 'total_amount', 'status', 
        'payment_method', 'country', 'shipping_type'])
    
    orders_v2 = orders_v2.withColumn('order_date', F.to_timestamp('order_date'))
    print("New columns:", orders_v2.columns)
    
    # Initialize schema manager
    manager = SchemaEvolutionManager(spark)
    
    # Compare schemas
    diff = manager.compare_schemas(orders_v2, 'orders', use_simulated=True)
    
    # Attempt migration with FLEXIBLE strategy
    result = manager.apply_schema_evolution(
        orders_v2,
        'orders',
        strategy=EvolutionStrategy.FLEXIBLE,
        dry_run=True
    )
    
    print(f"\n✓ Schema evolution would {'succeed' if result['success'] else 'fail'}")
    print(f"Message: {result['message']}")
    
    spark.stop()


def scenario_2_comprehensive_quality_checks():
    """
    Scenario 2: Comprehensive Data Quality Checks on Large Dataset
    E-commerce transactions with various quality issues
    """
    print("\n" + "="*80)
    print("SCENARIO 2: COMPREHENSIVE DATA QUALITY CHECKS")
    print("="*80)
    
    spark = SparkSession.builder \
        .appName("DataQuality") \
        .config("spark.sql.shuffle.partitions", "10") \
        .getOrCreate()
    
    # Generate large dataset with quality issues
    print("\nGenerating test data with quality issues...")
    
    num_records = 10000
    data = []
    
    for i in range(num_records):
        # Introduce various quality issues
        
        # 5% null customer IDs (completeness issue)
        customer_id = None if i % 20 == 0 else 1000 + (i % 500)
        
        # 3% null emails (required field violation)
        email = None if i % 33 == 0 else f"customer{i}@example.com"
        
        # 10% duplicate order IDs (uniqueness issue)
        order_id = (i // 10) if i % 10 == 0 else i
        
        # Some negative amounts (validity issue)
        amount = -abs(random.uniform(10, 1000)) if i % 100 == 0 else random.uniform(10, 1000)
        
        # Invalid status values (validity issue)
        if i % 50 == 0:
            status = 'INVALID_STATUS'
        else:
            status = random.choice(['pending', 'completed', 'shipped', 'cancelled'])
        
        # Invalid email formats (pattern issue)
        if i % 40 == 0 and email:
            email = f"invalid_email_{i}"  # No @ symbol
        
        # Date range issues
        base_date = datetime(2024, 1, 1)
        if i % 75 == 0:
            order_date = base_date - timedelta(days=365)  # Too old
        else:
            order_date = base_date + timedelta(days=random.randint(0, 90))
        
        data.append((
            order_id,
            customer_id,
            email,
            round(amount, 2),
            status,
            order_date.strftime('%Y-%m-%d %H:%M:%S'),
            f"PROD-{(i % 100) + 1:03d}",
            random.randint(1, 10)
        ))
    
    schema = StructType([
        StructField("order_id", IntegerType(), False),
        StructField("customer_id", IntegerType(), True),
        StructField("email", StringType(), True),
        StructField("amount", DoubleType(), False),
        StructField("status", StringType(), False),
        StructField("order_date", StringType(), False),
        StructField("product_id", StringType(), False),
        StructField("quantity", IntegerType(), False)
    ])
    
    df = spark.createDataFrame(data, schema)
    df = df.withColumn("order_date", F.to_timestamp("order_date"))
    
    print(f"Generated {num_records:,} records with intentional quality issues")
    
    # Initialize quality framework
    quality = DataQualityFramework(spark)
    
    # Configure comprehensive quality checks
    quality_config = {
        'null_checks': {
            'columns': ['customer_id', 'email', 'amount', 'status'],
            'threshold_pct': 5.0,
            'sample_fraction': 1.0
        },
        'required_fields': ['order_id', 'amount', 'order_date'],
        'duplicates': {
            'key_columns': ['order_id'],
            'sample_size': 10
        },
        'primary_key': 'order_id',
        'value_ranges': {
            'amount': (0, 10000),
            'quantity': (1, 100)
        },
        'valid_values': {
            'status': ['pending', 'completed', 'shipped', 'cancelled']
        },
        'regex_patterns': {
            'email': r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$',
            'product_id': r'^PROD-\d{3}$'
        },
        'freshness': {
            'timestamp_column': 'order_date',
            'max_age_hours': 2400  # 100 days
        }
    }
    
    # Run comprehensive quality checks
    results = quality.run_comprehensive_checks(df, quality_config)
    
    # Get quality report
    print("\n\n" + "="*80)
    print("DETAILED QUALITY REPORT")
    print("="*80)
    
    quality_report = quality.get_quality_report()
    quality_report.show(truncate=False)
    
    spark.stop()


def scenario_3_referential_integrity():
    """
    Scenario 3: Referential Integrity Checks Between Tables
    Fact-Dimension relationship validation
    """
    print("\n" + "="*80)
    print("SCENARIO 3: REFERENTIAL INTEGRITY CHECKS")
    print("="*80)
    
    spark = SparkSession.builder \
        .appName("ReferentialIntegrity") \
        .getOrCreate()
    
    # Create dimension table (customers)
    customers = spark.createDataFrame([
        (101, 'Alice Johnson', 'alice@email.com'),
        (102, 'Bob Smith', 'bob@email.com'),
        (103, 'Charlie Brown', 'charlie@email.com'),
        (104, 'Diana Prince', 'diana@email.com')
    ], ['customer_id', 'name', 'email'])
    
    # Create fact table (orders) with some orphaned records
    orders = spark.createDataFrame([
        (1, 101, 150.00, '2024-01-01'),
        (2, 102, 200.00, '2024-01-02'),
        (3, 105, 175.00, '2024-01-03'),  # Orphan - customer_id 105 doesn't exist
        (4, 103, 300.00, '2024-01-04'),
        (5, 106, 250.00, '2024-01-05'),  # Orphan - customer_id 106 doesn't exist
        (6, 104, 180.00, '2024-01-06'),
        (7, 107, 220.00, '2024-01-07')   # Orphan - customer_id 107 doesn't exist
    ], ['order_id', 'customer_id', 'amount', 'order_date'])
    
    print(f"\nCustomers: {customers.count()} records")
    print(f"Orders: {orders.count()} records")
    
    # Initialize quality framework
    quality = DataQualityFramework(spark)
    
    # Check referential integrity
    result = quality.check_referential_integrity(
        fact_df=orders,
        dimension_df=customers,
        fact_key='customer_id',
        dimension_key='customer_id'
    )
    
    print(f"\n{'='*60}")
    print(f"Referential Integrity: {'✓ PASSED' if result['status'] == 'PASSED' else '✗ FAILED'}")
    print(f"Orphaned records: {result['orphaned_count']}")
    print(f"{'='*60}")
    
    spark.stop()


def scenario_4_cross_field_validations():
    """
    Scenario 4: Complex Business Rule Validations
    Cross-field and business logic checks
    """
    print("\n" + "="*80)
    print("SCENARIO 4: CROSS-FIELD BUSINESS RULE VALIDATION")
    print("="*80)
    
    spark = SparkSession.builder \
        .appName("BusinessRules") \
        .getOrCreate()
    
    # Create dataset with business rule violations
    transactions = spark.createDataFrame([
        (1, '2024-01-01', '2024-01-05', 100.00, 10.00, 110.00, 'active'),
        (2, '2024-01-02', '2024-01-03', 200.00, 20.00, 220.00, 'active'),
        (3, '2024-01-03', '2024-01-02', 150.00, 15.00, 165.00, 'active'),  # ship < order
        (4, '2024-01-04', '2024-01-06', 300.00, 30.00, 320.00, 'active'),  # total != subtotal + tax
        (5, '2024-01-05', '2024-01-07', 250.00, 25.00, 275.00, 'active'),
        (6, '2024-01-06', '2024-01-08', 180.00, -5.00, 175.00, 'active'),  # negative tax
        (7, '2024-01-07', '2024-01-09', 0.00, 0.00, 0.00, 'active'),      # zero amount
    ], ['txn_id', 'order_date', 'ship_date', 'subtotal', 'tax', 'total', 'status'])
    
    transactions = transactions.withColumn('order_date', F.to_date('order_date'))
    transactions = transactions.withColumn('ship_date', F.to_date('ship_date'))
    
    # Define business rules
    business_rules = [
        # Rule 1: Ship date must be >= order date
        (
            "ship_date_after_order_date",
            lambda df: df.filter(F.col('ship_date') < F.col('order_date'))
        ),
        
        # Rule 2: Total must equal subtotal + tax
        (
            "total_equals_subtotal_plus_tax",
            lambda df: df.filter(
                F.abs(F.col('total') - (F.col('subtotal') + F.col('tax'))) > 0.01
            )
        ),
        
        # Rule 3: Tax must be positive
        (
            "positive_tax",
            lambda df: df.filter(F.col('tax') < 0)
        ),
        
        # Rule 4: Subtotal must be greater than zero for active transactions
        (
            "non_zero_active_transactions",
            lambda df: df.filter(
                (F.col('status') == 'active') & (F.col('subtotal') <= 0)
            )
        )
    ]
    
    # Initialize quality framework
    quality = DataQualityFramework(spark)
    
    # Run cross-field validations
    result = quality.check_cross_field_rules(transactions, business_rules)
    
    print("\n" + "="*60)
    if result['violations']:
        print("Business rule violations found:")
        for v in result['violations']:
            print(f"  - {v['rule']}: {v['violations']} violations")
    else:
        print("✓ All business rules passed!")
    print("="*60)
    
    spark.stop()


def scenario_5_data_profiling():
    """
    Scenario 5: Statistical Profiling of Large Dataset
    Generate comprehensive data profile
    """
    print("\n" + "="*80)
    print("SCENARIO 5: DATA PROFILING")
    print("="*80)
    
    spark = SparkSession.builder \
        .appName("DataProfiling") \
        .getOrCreate()
    
    # Generate realistic sales data
    print("\nGenerating sales data...")
    
    num_records = 50000
    data = []
    
    for i in range(num_records):
        data.append((
            i + 1,
            f"CUST-{(i % 1000) + 1:04d}",
            f"PROD-{(i % 100) + 1:03d}",
            round(random.uniform(10, 1000), 2),
            random.randint(1, 20),
            random.choice(['online', 'store', 'mobile', 'phone']),
            random.choice(['credit_card', 'debit_card', 'paypal', 'cash']),
            random.choice(['US', 'CA', 'UK', 'DE', 'FR', 'JP']),
            (datetime(2024, 1, 1) + timedelta(days=random.randint(0, 365))).strftime('%Y-%m-%d')
        ))
    
    df = spark.createDataFrame(data, [
        'transaction_id', 'customer_id', 'product_id', 'amount', 'quantity',
        'channel', 'payment_method', 'country', 'transaction_date'
    ])
    
    df = df.withColumn('transaction_date', F.to_date('transaction_date'))
    
    print(f"Generated {num_records:,} transaction records")
    
    # Initialize quality framework
    quality = DataQualityFramework(spark)
    
    # Profile numeric columns
    print("\n" + "-"*80)
    numeric_profile = quality.profile_numeric_columns(
        df,
        columns=['amount', 'quantity'],
        percentiles=[0.25, 0.5, 0.75, 0.95, 0.99]
    )
    
    # Profile categorical columns
    print("\n" + "-"*80)
    categorical_profile = quality.profile_categorical_columns(
        df,
        columns=['channel', 'payment_method', 'country'],
        top_n=10
    )
    
    # Analyze patterns
    print("\n" + "-"*80)
    print("DATA INSIGHTS")
    print("-"*80)
    
    # Top selling products
    print("\nTop 10 Products by Sales Volume:")
    df.groupBy('product_id') \
        .agg(
            F.sum('amount').alias('total_revenue'),
            F.count('*').alias('transactions')
        ) \
        .orderBy(F.col('total_revenue').desc()) \
        .show(10)
    
    # Sales by channel
    print("\nSales Distribution by Channel:")
    df.groupBy('channel') \
        .agg(
            F.sum('amount').alias('revenue'),
            F.count('*').alias('transactions'),
            F.avg('amount').alias('avg_transaction')
        ) \
        .orderBy(F.col('revenue').desc()) \
        .show()
    
    spark.stop()


def scenario_6_huge_volume_optimization():
    """
    Scenario 6: Quality Checks on Huge Data Volumes (Billions of records)
    Demonstrates optimization techniques
    """
    print("\n" + "="*80)
    print("SCENARIO 6: OPTIMIZED QUALITY CHECKS FOR HUGE VOLUMES")
    print("="*80)
    
    spark = SparkSession.builder \
        .appName("HugeVolumeQuality") \
        .config("spark.sql.shuffle.partitions", "200") \
        .config("spark.default.parallelism", "200") \
        .getOrCreate()
    
    # Simulate huge dataset
    print("\nSimulating huge dataset with partitioning...")
    
    # In reality this would be billions of records
    # For demo, we'll simulate the approach
    num_records = 100000  # Represents 1B+ in production
    
    data = []
    for i in range(num_records):
        data.append((
            i,
            f"user_{i % 10000}",
            random.uniform(1, 100),
            random.choice(['A', 'B', 'C', 'D']),
            (datetime(2024, 1, 1) + timedelta(hours=i % 8760)).strftime('%Y-%m-%d %H:%M:%S')
        ))
    
    df = spark.createDataFrame(
        data,
        ['event_id', 'user_id', 'value', 'category', 'timestamp']
    )
    df = df.withColumn('timestamp', F.to_timestamp('timestamp'))
    
    # Repartition for optimal processing
    df = df.repartition(20, 'category')
    df.cache()  # Cache for multiple operations
    
    print(f"Dataset size: {num_records:,} records (represents billions)")
    print(f"Partitions: {df.rdd.getNumPartitions()}")
    
    # Initialize quality framework
    quality = DataQualityFramework(spark)
    
    print("\n" + "-"*80)
    print("OPTIMIZATION TECHNIQUES FOR HUGE DATA:")
    print("-"*80)
    
    # Technique 1: Sample-based quality checks
    print("\n1. SAMPLING for null checks (faster on huge data)")
    start = datetime.now()
    quality.check_null_counts(df, sample_fraction=0.1)  # 10% sample
    print(f"   Time with sampling: {(datetime.now() - start).total_seconds():.2f}s")
    
    # Technique 2: Partitioned processing
    print("\n2. PARTITIONED processing for categorical analysis")
    start = datetime.now()
    df.groupBy('category').count().show()
    print(f"   Time with partitioning: {(datetime.now() - start).total_seconds():.2f}s")
    
    # Technique 3: Incremental aggregations
    print("\n3. INCREMENTAL aggregations (single pass)")
    start = datetime.now()
    stats = df.agg(
        F.count('*').alias('total'),
        F.countDistinct('user_id').alias('unique_users'),
        F.min('value').alias('min_value'),
        F.max('value').alias('max_value'),
        F.avg('value').alias('avg_value')
    ).first()
    print(f"   Statistics computed in: {(datetime.now() - start).total_seconds():.2f}s")
    print(f"   Total: {stats.total:,}, Unique users: {stats.unique_users:,}")
    
    df.unpersist()
    spark.stop()


def main():
    """Run all scenarios"""
    print("\n")
    print("="*80)
    print(" SCHEMA EVOLUTION & DATA QUALITY SCENARIOS")
    print("="*80)
    
    # Schema Evolution
    scenario_1_schema_evolution_migration()
    print("\n" + "="*80 + "\n")
    
    # Data Quality
    scenario_2_comprehensive_quality_checks()
    print("\n" + "="*80 + "\n")
    
    scenario_3_referential_integrity()
    print("\n" + "="*80 + "\n")
    
    scenario_4_cross_field_validations()
    print("\n" + "="*80 + "\n")
    
    scenario_5_data_profiling()
    print("\n" + "="*80 + "\n")
    
    scenario_6_huge_volume_optimization()
    
    print("\n")
    print("="*80)
    print(" ALL SCENARIOS COMPLETED!")
    print("="*80)
    print("\n")


if __name__ == "__main__":
    main()
