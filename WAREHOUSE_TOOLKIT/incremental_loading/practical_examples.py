"""
Practical Examples for Incremental Loading Patterns
Real-world scenarios and use cases for each pattern.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
from incremental_loading import (
    TimestampBasedLoader,
    CDCLoader,
    DeltaDiffLoader,
    SequenceBasedLoader
)


def example_1_ecommerce_orders():
    """
    Example 1: E-commerce Orders - Timestamp-based Loading
    Scenario: Load new orders from an e-commerce system daily
    """
    print("\n" + "="*80)
    print("EXAMPLE 1: E-COMMERCE ORDERS (TIMESTAMP-BASED)")
    print("="*80)
    
    spark = SparkSession.builder.appName("EcommerceOrders").getOrCreate()
    
    # Simulate order data with timestamps
    orders_data = [
        (1001, 'ORD-2024-001', 250.50, 'completed', '2024-01-10 08:30:00'),
        (1002, 'ORD-2024-002', 175.00, 'completed', '2024-01-10 09:15:00'),
        (1003, 'ORD-2024-003', 420.75, 'pending', '2024-01-11 10:20:00'),
        (1004, 'ORD-2024-004', 89.99, 'completed', '2024-01-11 14:45:00'),
        (1005, 'ORD-2024-005', 310.00, 'shipped', '2024-01-12 11:30:00'),
    ]
    
    schema = StructType([
        StructField("order_id", IntegerType(), False),
        StructField("order_number", StringType(), False),
        StructField("total_amount", DoubleType(), False),
        StructField("status", StringType(), False),
        StructField("created_at", StringType(), False)
    ])
    
    orders_df = spark.createDataFrame(orders_data, schema)
    orders_df = orders_df.withColumn("created_at", F.to_timestamp("created_at"))
    
    # Save as source
    orders_df.write.mode("overwrite").parquet("/tmp/examples/orders_source")
    
    # Load incrementally
    loader = TimestampBasedLoader(spark)
    
    # First load - gets all records
    print("\nFirst load - loading all historical orders...")
    result = loader.load_incremental(
        source_path="/tmp/examples/orders_source",
        target_path="/tmp/examples/orders_target",
        watermark_path="/tmp/examples/watermarks",
        timestamp_column="created_at",
        table_name="orders"
    )
    
    print(f"\nOrders loaded:")
    result.show(truncate=False)
    
    # Simulate new orders coming in
    new_orders_data = [
        (1006, 'ORD-2024-006', 567.25, 'pending', '2024-01-13 09:00:00'),
        (1007, 'ORD-2024-007', 123.45, 'completed', '2024-01-13 10:30:00'),
    ]
    
    new_orders_df = spark.createDataFrame(new_orders_data, schema)
    new_orders_df = new_orders_df.withColumn("created_at", F.to_timestamp("created_at"))
    
    # Append to source
    all_orders = orders_df.union(new_orders_df)
    all_orders.write.mode("overwrite").parquet("/tmp/examples/orders_source")
    
    # Second load - only gets new records
    print("\nSecond load - loading only new orders...")
    result = loader.load_incremental(
        source_path="/tmp/examples/orders_source",
        target_path="/tmp/examples/orders_target",
        watermark_path="/tmp/examples/watermarks",
        timestamp_column="created_at",
        table_name="orders"
    )
    
    print(f"\nNew orders loaded:")
    result.show(truncate=False)
    
    # Show final state
    print("\nFinal orders table:")
    final_df = spark.read.parquet("/tmp/examples/orders_target")
    final_df.orderBy("order_id").show(truncate=False)
    
    spark.stop()


def example_2_customer_database_cdc():
    """
    Example 2: Customer Database - CDC Pattern
    Scenario: Sync customer changes from operational database using CDC
    """
    print("\n" + "="*80)
    print("EXAMPLE 2: CUSTOMER DATABASE SYNC (CDC)")
    print("="*80)
    
    spark = SparkSession.builder.appName("CustomerCDC").getOrCreate()
    
    # Simulate CDC events from database
    cdc_events = [
        # Initial inserts
        (101, 'john.doe@email.com', 'John Doe', 'New York', 'INSERT', 1),
        (102, 'jane.smith@email.com', 'Jane Smith', 'Los Angeles', 'INSERT', 2),
        (103, 'bob.wilson@email.com', 'Bob Wilson', 'Chicago', 'INSERT', 3),
        
        # Customer updates their email and city
        (101, 'john.newemail@email.com', 'John Doe', 'Boston', 'UPDATE', 4),
        
        # New customer
        (104, 'alice.brown@email.com', 'Alice Brown', 'Seattle', 'INSERT', 5),
        
        # Customer updates name
        (102, 'jane.smith@email.com', 'Jane Smith-Johnson', 'Los Angeles', 'UPDATE', 6),
        
        # Customer deleted (GDPR request)
        (103, None, None, None, 'DELETE', 7),
        
        # New customer
        (105, 'charlie.davis@email.com', 'Charlie Davis', 'Miami', 'INSERT', 8),
    ]
    
    schema = StructType([
        StructField("customer_id", IntegerType(), False),
        StructField("email", StringType(), True),
        StructField("name", StringType(), True),
        StructField("city", StringType(), True),
        StructField("operation", StringType(), False),
        StructField("sequence_num", IntegerType(), False)
    ])
    
    cdc_df = spark.createDataFrame(cdc_events, schema)
    cdc_df.write.mode("overwrite").parquet("/tmp/examples/customer_cdc")
    
    # Process CDC events
    loader = CDCLoader(spark)
    
    print("\nProcessing CDC events...")
    result = loader.process_cdc(
        cdc_source_path="/tmp/examples/customer_cdc",
        target_path="/tmp/examples/customers_target",
        watermark_path="/tmp/examples/watermarks",
        primary_key="customer_id",
        table_name="customers"
    )
    
    print(f"\nFinal customer state after applying all CDC events:")
    result.orderBy("customer_id").show(truncate=False)
    
    print("\nNote: Customer 103 (Bob Wilson) has been removed due to DELETE operation")
    print("Customer 101 and 102 show their updated information")
    
    spark.stop()


def example_3_product_catalog_diff():
    """
    Example 3: Product Catalog - Delta/Diff Detection
    Scenario: Sync product catalog where we don't have timestamps
    """
    print("\n" + "="*80)
    print("EXAMPLE 3: PRODUCT CATALOG SYNC (DELTA/DIFF)")
    print("="*80)
    
    spark = SparkSession.builder.appName("ProductCatalog").getOrCreate()
    
    # Initial product catalog (Day 1)
    initial_products = [
        (1, 'LAP-001', 'Gaming Laptop', 1299.99, 'Electronics', True),
        (2, 'PHN-001', 'Smartphone Pro', 899.99, 'Electronics', True),
        (3, 'HDR-001', 'Wireless Headphones', 199.99, 'Audio', True),
        (4, 'KBD-001', 'Mechanical Keyboard', 149.99, 'Accessories', True),
        (5, 'MOU-001', 'Gaming Mouse', 79.99, 'Accessories', True),
    ]
    
    schema = StructType([
        StructField("product_id", IntegerType(), False),
        StructField("sku", StringType(), False),
        StructField("product_name", StringType(), False),
        StructField("price", DoubleType(), False),
        StructField("category", StringType(), False),
        StructField("in_stock", BooleanType(), False)
    ])
    
    products_df = spark.createDataFrame(initial_products, schema)
    products_df.write.mode("overwrite").parquet("/tmp/examples/products_source")
    
    loader = DeltaDiffLoader(spark)
    
    # First sync - all products are new
    print("\nFirst sync - loading initial catalog...")
    changes = loader.detect_changes(
        source_path="/tmp/examples/products_source",
        target_path="/tmp/examples/products_target",
        snapshot_path="/tmp/examples/products_snapshot",
        primary_key="product_id",
        table_name="products"
    )
    
    # Updated catalog (Day 2)
    # - Price changes for products 1 and 2
    # - Product 3 out of stock
    # - Product 4 discontinued (deleted)
    # - New product 6 added
    updated_products = [
        (1, 'LAP-001', 'Gaming Laptop', 1199.99, 'Electronics', True),  # Price reduced
        (2, 'PHN-001', 'Smartphone Pro', 799.99, 'Electronics', True),  # Price reduced
        (3, 'HDR-001', 'Wireless Headphones', 199.99, 'Audio', False),  # Out of stock
        # Product 4 removed
        (5, 'MOU-001', 'Gaming Mouse', 79.99, 'Accessories', True),     # No change
        (6, 'MON-001', '4K Monitor', 399.99, 'Electronics', True),      # New product
    ]
    
    updated_df = spark.createDataFrame(updated_products, schema)
    updated_df.write.mode("overwrite").parquet("/tmp/examples/products_source")
    
    # Second sync - detect changes
    print("\n\nSecond sync - detecting changes...")
    changes = loader.detect_changes(
        source_path="/tmp/examples/products_source",
        target_path="/tmp/examples/products_target",
        snapshot_path="/tmp/examples/products_snapshot",
        primary_key="product_id",
        table_name="products"
    )
    
    print("\n--- INSERTS (New Products) ---")
    changes["inserts"].show(truncate=False)
    
    print("\n--- UPDATES (Modified Products) ---")
    changes["updates"].show(truncate=False)
    
    print("\n--- DELETES (Discontinued Products) ---")
    changes["deletes"].show(truncate=False)
    
    print("\n--- FINAL CATALOG STATE ---")
    final_catalog = spark.read.parquet("/tmp/examples/products_target")
    final_catalog.orderBy("product_id").show(truncate=False)
    
    spark.stop()


def example_4_event_logs_sequence():
    """
    Example 4: Application Event Logs - Sequence-based Loading
    Scenario: Process application logs with monotonically increasing IDs
    """
    print("\n" + "="*80)
    print("EXAMPLE 4: APPLICATION EVENT LOGS (SEQUENCE-BASED)")
    print("="*80)
    
    spark = SparkSession.builder.appName("EventLogs").getOrCreate()
    
    # Simulate application event logs
    event_logs = [
        (1, 'user_login', 'user_123', 'success', '2024-01-15 08:00:00'),
        (2, 'page_view', 'user_123', 'homepage', '2024-01-15 08:00:15'),
        (3, 'user_login', 'user_456', 'success', '2024-01-15 08:01:00'),
        (4, 'add_to_cart', 'user_123', 'product_789', '2024-01-15 08:02:30'),
        (5, 'checkout', 'user_123', 'order_001', '2024-01-15 08:05:00'),
        (6, 'page_view', 'user_456', 'products', '2024-01-15 08:05:30'),
        (7, 'user_logout', 'user_123', 'success', '2024-01-15 08:10:00'),
        (8, 'user_login', 'user_789', 'failed', '2024-01-15 08:11:00'),
        (9, 'user_login', 'user_789', 'success', '2024-01-15 08:11:30'),
        (10, 'page_view', 'user_789', 'homepage', '2024-01-15 08:11:45'),
    ]
    
    schema = StructType([
        StructField("sequence_id", IntegerType(), False),
        StructField("event_type", StringType(), False),
        StructField("user_id", StringType(), False),
        StructField("event_data", StringType(), False),
        StructField("timestamp", StringType(), False)
    ])
    
    events_df = spark.createDataFrame(event_logs, schema)
    events_df = events_df.withColumn("timestamp", F.to_timestamp("timestamp"))
    events_df.write.mode("overwrite").parquet("/tmp/examples/events_source")
    
    loader = SequenceBasedLoader(spark)
    
    # First load - process events 1-10
    print("\nFirst batch - processing events 1-10...")
    result = loader.load_incremental(
        source_path="/tmp/examples/events_source",
        target_path="/tmp/examples/events_target",
        watermark_path="/tmp/examples/watermarks",
        sequence_column="sequence_id",
        table_name="app_events"
    )
    
    print(f"\nEvents processed:")
    result.show(truncate=False)
    
    # Simulate new events arriving
    new_events = [
        (11, 'add_to_cart', 'user_789', 'product_456', '2024-01-15 08:12:00'),
        (12, 'search', 'user_456', 'laptop', '2024-01-15 08:13:00'),
        (13, 'page_view', 'user_456', 'product_123', '2024-01-15 08:13:30'),
        (14, 'checkout', 'user_789', 'order_002', '2024-01-15 08:14:00'),
        (15, 'user_logout', 'user_789', 'success', '2024-01-15 08:15:00'),
    ]
    
    new_events_df = spark.createDataFrame(new_events, schema)
    new_events_df = new_events_df.withColumn("timestamp", F.to_timestamp("timestamp"))
    
    all_events = events_df.union(new_events_df)
    all_events.write.mode("overwrite").parquet("/tmp/examples/events_source")
    
    # Second load - only process new events
    print("\n\nSecond batch - processing events 11-15...")
    result = loader.load_incremental(
        source_path="/tmp/examples/events_source",
        target_path="/tmp/examples/events_target",
        watermark_path="/tmp/examples/watermarks",
        sequence_column="sequence_id",
        table_name="app_events"
    )
    
    print(f"\nNew events processed:")
    result.show(truncate=False)
    
    # Show analytics on all events
    print("\n--- EVENT ANALYTICS ---")
    all_events_df = spark.read.parquet("/tmp/examples/events_target")
    
    print("\nEvent counts by type:")
    all_events_df.groupBy("event_type").count().orderBy("count", ascending=False).show()
    
    print("\nUser activity summary:")
    all_events_df.groupBy("user_id").agg(
        F.count("*").alias("total_events"),
        F.min("timestamp").alias("first_activity"),
        F.max("timestamp").alias("last_activity")
    ).show(truncate=False)
    
    spark.stop()


def example_5_scd_type2_implementation():
    """
    Example 5: Slowly Changing Dimension Type 2
    Scenario: Track customer dimension changes over time
    """
    print("\n" + "="*80)
    print("EXAMPLE 5: SLOWLY CHANGING DIMENSION TYPE 2")
    print("="*80)
    
    spark = SparkSession.builder.appName("SCD_Type2").getOrCreate()
    
    # Initial customer dimension
    initial_customers = [
        (1, 'Alice Johnson', 'alice@email.com', 'Gold', 'New York', 
         '2024-01-01', '9999-12-31', True),
        (2, 'Bob Smith', 'bob@email.com', 'Silver', 'Los Angeles', 
         '2024-01-01', '9999-12-31', True),
        (3, 'Charlie Brown', 'charlie@email.com', 'Bronze', 'Chicago', 
         '2024-01-01', '9999-12-31', True),
    ]
    
    schema = StructType([
        StructField("customer_id", IntegerType(), False),
        StructField("customer_name", StringType(), False),
        StructField("email", StringType(), False),
        StructField("tier", StringType(), False),
        StructField("city", StringType(), False),
        StructField("effective_date", StringType(), False),
        StructField("end_date", StringType(), False),
        StructField("is_current", BooleanType(), False)
    ])
    
    customers_df = spark.createDataFrame(initial_customers, schema)
    customers_df.write.mode("overwrite").parquet("/tmp/examples/dim_customer")
    
    print("\nInitial customer dimension:")
    customers_df.show(truncate=False)
    
    # Simulate changes: Alice upgraded to Platinum, Bob moved to Seattle
    print("\n\nSimulating dimension changes...")
    print("- Alice Johnson upgraded from Gold to Platinum")
    print("- Bob Smith relocated from Los Angeles to Seattle")
    
    # Read current dimension
    current_dim = spark.read.parquet("/tmp/examples/dim_customer")
    
    # Close old records
    updated_dim = current_dim.withColumn(
        "end_date",
        F.when(
            (F.col("customer_id").isin([1, 2])) & (F.col("is_current") == True),
            F.lit('2024-01-15')
        ).otherwise(F.col("end_date"))
    ).withColumn(
        "is_current",
        F.when(
            (F.col("customer_id").isin([1, 2])) & (F.col("is_current") == True),
            False
        ).otherwise(F.col("is_current"))
    )
    
    # Add new records
    new_records = [
        (1, 'Alice Johnson', 'alice@email.com', 'Platinum', 'New York', 
         '2024-01-16', '9999-12-31', True),
        (2, 'Bob Smith', 'bob@email.com', 'Silver', 'Seattle', 
         '2024-01-16', '9999-12-31', True),
    ]
    
    new_records_df = spark.createDataFrame(new_records, schema)
    
    # Combine and save
    final_dim = updated_dim.union(new_records_df)
    final_dim.write.mode("overwrite").parquet("/tmp/examples/dim_customer")
    
    print("\n\nUpdated customer dimension (with history):")
    final_df = spark.read.parquet("/tmp/examples/dim_customer")
    final_df.orderBy("customer_id", "effective_date").show(truncate=False)
    
    print("\n\nCurrent records only:")
    final_df.filter(F.col("is_current") == True).show(truncate=False)
    
    print("\n\nHistorical records for Alice (customer_id=1):")
    final_df.filter(F.col("customer_id") == 1).orderBy("effective_date").show(truncate=False)
    
    spark.stop()


def main():
    """Run all practical examples"""
    print("\n")
    print("="*80)
    print(" PRACTICAL EXAMPLES FOR INCREMENTAL LOADING PATTERNS")
    print("="*80)
    
    # Run all examples
    example_1_ecommerce_orders()
    print("\n" + "="*80 + "\n")
    
    example_2_customer_database_cdc()
    print("\n" + "="*80 + "\n")
    
    example_3_product_catalog_diff()
    print("\n" + "="*80 + "\n")
    
    example_4_event_logs_sequence()
    print("\n" + "="*80 + "\n")
    
    example_5_scd_type2_implementation()
    
    print("\n")
    print("="*80)
    print(" ALL EXAMPLES COMPLETED SUCCESSFULLY!")
    print("="*80)
    print("\n")


if __name__ == "__main__":
    main()
