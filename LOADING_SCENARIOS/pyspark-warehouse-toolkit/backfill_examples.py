"""
Practical Backfilling Examples
Real-world scenarios for historical data loading
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime, timedelta
from backfill_historical import (
    TimestampBackfill,
    ParallelBackfill,
    SequenceBackfill,
    FullHistoricalLoad,
    IncrementalBackfillValidator
)


def scenario_1_migrate_data_warehouse():
    """
    Scenario 1: Data Warehouse Migration
    Migrate 5 years of historical data from old system to new data warehouse
    """
    print("\n" + "="*80)
    print("SCENARIO 1: DATA WAREHOUSE MIGRATION (5 YEARS)")
    print("="*80)
    
    spark = SparkSession.builder \
        .appName("WarehouseMigration") \
        .config("spark.sql.shuffle.partitions", "20") \
        .getOrCreate()
    
    # Simulate 5 years of customer data
    print("\nSimulating 5 years of customer data...")
    
    base_date = datetime(2019, 1, 1)
    customer_data = []
    customer_id = 1
    
    # Generate data for 5 years (1,825 days)
    for day in range(1825):
        current_date = base_date + timedelta(days=day)
        
        # 20-50 new customers per day
        new_customers = 20 + (day % 30)
        
        for i in range(new_customers):
            customer_data.append((
                customer_id,
                f"customer_{customer_id}@email.com",
                f"Customer {customer_id}",
                ['Bronze', 'Silver', 'Gold', 'Platinum'][i % 4],
                round(100 + (i * 25.5), 2),
                current_date.strftime('%Y-%m-%d %H:%M:%S')
            ))
            customer_id += 1
    
    schema = StructType([
        StructField("customer_id", IntegerType(), False),
        StructField("email", StringType(), False),
        StructField("name", StringType(), False),
        StructField("tier", StringType(), False),
        StructField("lifetime_value", DoubleType(), False),
        StructField("created_date", StringType(), False)
    ])
    
    customers_df = spark.createDataFrame(customer_data, schema)
    customers_df = customers_df.withColumn("created_date", F.to_timestamp("created_date"))
    customers_df.write.mode("overwrite").parquet("/tmp/scenarios/old_warehouse/customers")
    
    print(f"Created {customers_df.count():,} customer records")
    
    # Strategy: Backfill year by year with monthly chunks
    print("\nBackfilling strategy: Year by year, month by month")
    
    backfill_manager = TimestampBackfill(spark)
    
    for year in range(2019, 2024):
        print(f"\n{'='*60}")
        print(f"Processing year {year}")
        print(f"{'='*60}")
        
        stats = backfill_manager.backfill_by_time_range(
            source_path="/tmp/scenarios/old_warehouse/customers",
            target_path="/tmp/scenarios/new_warehouse/customers",
            state_path="/tmp/scenarios/state",
            start_date=f"{year}-01-01",
            end_date=f"{year+1}-01-01",
            timestamp_column="created_date",
            interval_days=30,  # Monthly chunks
            backfill_id=f"customer_migration_{year}"
        )
    
    # Validate migration
    print("\n\nValidating complete migration...")
    validator = IncrementalBackfillValidator(spark)
    validation = validator.validate_backfill(
        source_path="/tmp/scenarios/old_warehouse/customers",
        target_path="/tmp/scenarios/new_warehouse/customers",
        date_column="created_date",
        start_date="2019-01-01",
        end_date="2024-01-01",
        group_by_columns=["tier"]
    )
    
    print("\n" + "="*80)
    print(f"MIGRATION STATUS: {'SUCCESS ✓' if validation['is_valid'] else 'FAILED ✗'}")
    print("="*80)
    
    spark.stop()


def scenario_2_iot_sensor_data_backfill():
    """
    Scenario 2: IoT Sensor Data Backfill
    Load 1 year of high-volume sensor readings (millions of records)
    """
    print("\n" + "="*80)
    print("SCENARIO 2: IOT SENSOR DATA BACKFILL")
    print("="*80)
    
    spark = SparkSession.builder \
        .appName("IoTBackfill") \
        .config("spark.sql.shuffle.partitions", "50") \
        .getOrCreate()
    
    # Simulate sensor data (1 week for demo - in reality would be 1 year)
    print("\nGenerating sensor data...")
    
    base_timestamp = datetime(2023, 1, 1, 0, 0, 0)
    sensor_data = []
    sequence_id = 1
    
    # 100 sensors reporting every 5 minutes for 7 days
    num_sensors = 100
    intervals_per_day = (24 * 60) // 5  # 288 intervals per day
    days = 7
    
    for day in range(days):
        for interval in range(intervals_per_day):
            timestamp = base_timestamp + timedelta(days=day, minutes=interval*5)
            
            for sensor_id in range(1, num_sensors + 1):
                sensor_data.append((
                    sequence_id,
                    sensor_id,
                    f"DEVICE-{sensor_id:03d}",
                    round(20 + (sensor_id % 10) + (interval % 20) * 0.5, 2),  # temperature
                    round(40 + (sensor_id % 15) + (interval % 25) * 0.3, 2),  # humidity
                    timestamp.strftime('%Y-%m-%d %H:%M:%S')
                ))
                sequence_id += 1
    
    schema = StructType([
        StructField("sequence_id", IntegerType(), False),
        StructField("sensor_id", IntegerType(), False),
        StructField("device_name", StringType(), False),
        StructField("temperature", DoubleType(), False),
        StructField("humidity", DoubleType(), False),
        StructField("reading_timestamp", StringType(), False)
    ])
    
    sensors_df = spark.createDataFrame(sensor_data, schema)
    sensors_df = sensors_df.withColumn("reading_timestamp", F.to_timestamp("reading_timestamp"))
    sensors_df.write.mode("overwrite").parquet("/tmp/scenarios/iot_source")
    
    total_readings = sensors_df.count()
    print(f"Generated {total_readings:,} sensor readings")
    
    # Use parallel backfill for high volume
    print("\nUsing parallel backfill for high-volume data...")
    
    parallel_backfill = ParallelBackfill(spark)
    stats = parallel_backfill.backfill_parallel(
        source_path="/tmp/scenarios/iot_source",
        target_path="/tmp/scenarios/iot_target",
        start_date="2023-01-01",
        end_date="2023-01-08",
        timestamp_column="reading_timestamp",
        num_partitions=20,
        partition_column="sensor_id"
    )
    
    # Show sample analytics
    print("\n\nSample Analytics on Backfilled Data:")
    target_df = spark.read.parquet("/tmp/scenarios/iot_target")
    
    print("\nAverage temperature by sensor:")
    target_df.groupBy("device_name") \
        .agg(
            F.avg("temperature").alias("avg_temp"),
            F.avg("humidity").alias("avg_humidity"),
            F.count("*").alias("readings")
        ) \
        .orderBy("device_name") \
        .show(10)
    
    spark.stop()


def scenario_3_financial_transactions_backfill():
    """
    Scenario 3: Financial Transactions Backfill
    Load historical transactions with validation and deduplication
    """
    print("\n" + "="*80)
    print("SCENARIO 3: FINANCIAL TRANSACTIONS BACKFILL")
    print("="*80)
    
    spark = SparkSession.builder \
        .appName("FinancialBackfill") \
        .getOrCreate()
    
    # Simulate transaction data with some quality issues
    print("\nGenerating transaction data (with quality issues)...")
    
    base_date = datetime(2023, 1, 1)
    transactions = []
    
    for day in range(180):  # 6 months
        current_date = base_date + timedelta(days=day)
        
        for txn in range(500):  # 500 transactions per day
            txn_id = (day * 500) + txn + 1
            
            # Introduce some data quality issues
            amount = round(10 + (txn * 5.25), 2)
            
            # 2% null amounts (data quality issue)
            if txn % 50 == 0:
                amount = None
            
            # 1% negative amounts (data quality issue)
            if txn % 100 == 0:
                amount = -abs(amount) if amount else None
            
            transactions.append((
                txn_id,
                f"ACC-{(txn % 100) + 1:04d}",
                amount,
                ['DEBIT', 'CREDIT'][txn % 2],
                ['ONLINE', 'ATM', 'POS', 'TRANSFER'][txn % 4],
                current_date.strftime('%Y-%m-%d %H:%M:%S'),
                f"MERCHANT-{(txn % 50) + 1}"
            ))
    
    # Add duplicates (5%)
    duplicates = transactions[:4500]
    all_transactions = transactions + duplicates
    
    schema = StructType([
        StructField("transaction_id", IntegerType(), False),
        StructField("account_number", StringType(), False),
        StructField("amount", DoubleType(), True),
        StructField("transaction_type", StringType(), False),
        StructField("channel", StringType(), False),
        StructField("transaction_date", StringType(), False),
        StructField("merchant_id", StringType(), False)
    ])
    
    txn_df = spark.createDataFrame(all_transactions, schema)
    txn_df = txn_df.withColumn("transaction_date", F.to_timestamp("transaction_date"))
    txn_df.write.mode("overwrite").parquet("/tmp/scenarios/financial_source")
    
    print(f"Generated {len(all_transactions):,} transactions (including duplicates and bad data)")
    
    # Define strict validation rules for financial data
    validation_rules = {
        'remove_null_amounts': lambda df: df.filter(F.col("amount").isNotNull()),
        'remove_negative_amounts': lambda df: df.filter(F.col("amount") > 0),
        'remove_null_dates': lambda df: df.filter(F.col("transaction_date").isNotNull()),
        'remove_invalid_accounts': lambda df: df.filter(F.col("account_number").isNotNull()),
        'amount_range_check': lambda df: df.filter((F.col("amount") >= 0.01) & (F.col("amount") <= 1000000))
    }
    
    # Full load with validation and deduplication
    print("\nPerforming full load with validation and deduplication...")
    
    full_load = FullHistoricalLoad(spark)
    stats = full_load.full_load_with_validation(
        source_path="/tmp/scenarios/financial_source",
        target_path="/tmp/scenarios/financial_target",
        validation_rules=validation_rules,
        dedup_column="transaction_id",
        partition_by=["channel"]
    )
    
    # Show data quality report
    print("\n\nData Quality Report:")
    target_df = spark.read.parquet("/tmp/scenarios/financial_target")
    
    print("\nTransactions by channel:")
    target_df.groupBy("channel").agg(
        F.count("*").alias("count"),
        F.sum("amount").alias("total_amount"),
        F.avg("amount").alias("avg_amount")
    ).orderBy("channel").show()
    
    print("\nDaily transaction volume:")
    target_df.withColumn("date", F.to_date("transaction_date")) \
        .groupBy("date") \
        .agg(
            F.count("*").alias("transactions"),
            F.sum("amount").alias("daily_volume")
        ) \
        .orderBy("date") \
        .show(10)
    
    spark.stop()


def scenario_4_ecommerce_orders_resumable_backfill():
    """
    Scenario 4: E-commerce Orders - Resumable Backfill
    Demonstrate checkpoint/resume capability for long-running backfills
    """
    print("\n" + "="*80)
    print("SCENARIO 4: RESUMABLE BACKFILL WITH CHECKPOINTS")
    print("="*80)
    
    spark = SparkSession.builder \
        .appName("ResumableBackfill") \
        .getOrCreate()
    
    # Generate order data
    print("\nGenerating order data for 12 months...")
    
    base_date = datetime(2023, 1, 1)
    orders = []
    order_id = 1000000
    
    for day in range(365):
        current_date = base_date + timedelta(days=day)
        
        # Variable order volume (50-200 per day)
        daily_orders = 50 + (day % 150)
        
        for i in range(daily_orders):
            orders.append((
                order_id,
                f"ORD-2023-{order_id}",
                f"CUST-{(i % 5000) + 1:06d}",
                round(25 + (i * 12.75), 2),
                ['pending', 'processing', 'shipped', 'delivered'][i % 4],
                current_date.strftime('%Y-%m-%d %H:%M:%S')
            ))
            order_id += 1
    
    schema = StructType([
        StructField("order_id", IntegerType(), False),
        StructField("order_number", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("order_total", DoubleType(), False),
        StructField("status", StringType(), False),
        StructField("order_date", StringType(), False)
    ])
    
    orders_df = spark.createDataFrame(orders, schema)
    orders_df = orders_df.withColumn("order_date", F.to_timestamp("order_date"))
    orders_df.write.mode("overwrite").parquet("/tmp/scenarios/orders_source")
    
    print(f"Generated {len(orders):,} orders")
    
    # First backfill attempt - will be "interrupted"
    print("\n\nAttempt 1: Starting backfill (will process only Q1)...")
    
    backfill_manager = TimestampBackfill(spark)
    
    # Only backfill Q1
    stats1 = backfill_manager.backfill_by_time_range(
        source_path="/tmp/scenarios/orders_source",
        target_path="/tmp/scenarios/orders_target",
        state_path="/tmp/scenarios/state",
        start_date="2023-01-01",
        end_date="2023-04-01",
        timestamp_column="order_date",
        interval_days=7,
        backfill_id="orders_backfill_2023",
        resume=True
    )
    
    print("\n[SIMULATING INTERRUPTION - Process stopped after Q1]")
    
    # Resume backfill - will continue from Q2
    print("\n\nAttempt 2: Resuming backfill from checkpoint...")
    
    stats2 = backfill_manager.backfill_by_time_range(
        source_path="/tmp/scenarios/orders_source",
        target_path="/tmp/scenarios/orders_target",
        state_path="/tmp/scenarios/state",
        start_date="2023-01-01",  # Same range as before
        end_date="2024-01-01",     # But now going to full year
        timestamp_column="order_date",
        interval_days=7,
        backfill_id="orders_backfill_2023",
        resume=True  # Resume from checkpoint
    )
    
    # Validate final result
    print("\n\nValidating complete backfill...")
    
    validator = IncrementalBackfillValidator(spark)
    validation = validator.validate_backfill(
        source_path="/tmp/scenarios/orders_source",
        target_path="/tmp/scenarios/orders_target",
        date_column="order_date",
        start_date="2023-01-01",
        end_date="2024-01-01",
        group_by_columns=["status"]
    )
    
    spark.stop()


def scenario_5_multi_source_consolidation():
    """
    Scenario 5: Multi-Source Data Consolidation
    Backfill data from multiple sources into unified data warehouse
    """
    print("\n" + "="*80)
    print("SCENARIO 5: MULTI-SOURCE DATA CONSOLIDATION")
    print("="*80)
    
    spark = SparkSession.builder \
        .appName("MultiSourceBackfill") \
        .getOrCreate()
    
    # Simulate data from 3 different regional systems
    print("\nGenerating data from 3 regional sources...")
    
    regions = ["US", "EU", "APAC"]
    base_date = datetime(2023, 1, 1)
    
    for region in regions:
        sales_data = []
        
        for day in range(90):  # 3 months per region
            current_date = base_date + timedelta(days=day)
            
            # Different volume per region
            daily_sales = {
                "US": 1000,
                "EU": 750,
                "APAC": 500
            }[region]
            
            for i in range(daily_sales):
                sales_data.append((
                    f"{region}-{day}-{i}",
                    region,
                    f"PROD-{(i % 100) + 1:03d}",
                    round(50 + (i * 2.5), 2),
                    i % 10 + 1,
                    current_date.strftime('%Y-%m-%d %H:%M:%S')
                ))
        
        schema = StructType([
            StructField("sale_id", StringType(), False),
            StructField("region", StringType(), False),
            StructField("product_id", StringType(), False),
            StructField("amount", DoubleType(), False),
            StructField("quantity", IntegerType(), False),
            StructField("sale_date", StringType(), False)
        ])
        
        sales_df = spark.createDataFrame(sales_data, schema)
        sales_df = sales_df.withColumn("sale_date", F.to_timestamp("sale_date"))
        sales_df.write.mode("overwrite").parquet(f"/tmp/scenarios/source_{region}")
        
        print(f"  {region}: {len(sales_data):,} sales records")
    
    # Backfill each region in parallel
    print("\n\nBackfilling all regions into unified warehouse...")
    
    for region in regions:
        print(f"\n--- Processing {region} region ---")
        
        parallel_backfill = ParallelBackfill(spark)
        stats = parallel_backfill.backfill_parallel(
            source_path=f"/tmp/scenarios/source_{region}",
            target_path="/tmp/scenarios/unified_warehouse",
            start_date="2023-01-01",
            end_date="2023-04-01",
            timestamp_column="sale_date",
            num_partitions=5,
            partition_column="product_id"
        )
    
    # Analyze consolidated data
    print("\n\nConsolidated Data Analysis:")
    unified_df = spark.read.parquet("/tmp/scenarios/unified_warehouse")
    
    print("\nSales by region:")
    unified_df.groupBy("region").agg(
        F.count("*").alias("transactions"),
        F.sum("amount").alias("total_revenue"),
        F.avg("amount").alias("avg_transaction")
    ).orderBy("region").show()
    
    print("\nTop 10 products globally:")
    unified_df.groupBy("product_id").agg(
        F.sum("amount").alias("revenue"),
        F.sum("quantity").alias("units_sold")
    ).orderBy(F.col("revenue").desc()).show(10)
    
    spark.stop()


def main():
    """Run all practical scenarios"""
    print("\n")
    print("="*80)
    print(" PRACTICAL BACKFILLING SCENARIOS")
    print("="*80)
    
    # Run scenarios
    scenario_1_migrate_data_warehouse()
    print("\n" + "="*80 + "\n")
    
    scenario_2_iot_sensor_data_backfill()
    print("\n" + "="*80 + "\n")
    
    scenario_3_financial_transactions_backfill()
    print("\n" + "="*80 + "\n")
    
    scenario_4_ecommerce_orders_resumable_backfill()
    print("\n" + "="*80 + "\n")
    
    scenario_5_multi_source_consolidation()
    
    print("\n")
    print("="*80)
    print(" ALL SCENARIOS COMPLETED!")
    print("="*80)
    print("\n")


if __name__ == "__main__":
    main()
