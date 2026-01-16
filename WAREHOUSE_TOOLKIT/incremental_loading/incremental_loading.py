"""
Incremental Data Loading Patterns with PySpark
This module implements four common patterns for incremental data loading in data warehouses.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime
import hashlib


class IncrementalLoader:
    """Base class for incremental data loading operations"""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
    
    def save_watermark(self, table_name: str, watermark_value: str, watermark_path: str):
        """Save watermark value for tracking"""
        watermark_df = self.spark.createDataFrame(
            [(table_name, watermark_value, datetime.now().strftime('%Y-%m-%d %H:%M:%S'))],
            ["table_name", "watermark_value", "last_updated"]
        )
        watermark_df.write.mode("overwrite").parquet(f"{watermark_path}/{table_name}")
    
    def get_watermark(self, table_name: str, watermark_path: str, default_value: str = "1900-01-01"):
        """Retrieve last watermark value"""
        try:
            watermark_df = self.spark.read.parquet(f"{watermark_path}/{table_name}")
            return watermark_df.select("watermark_value").first()[0]
        except:
            return default_value


class TimestampBasedLoader(IncrementalLoader):
    """
    Pattern 1: Timestamp-based Incremental Loading
    Uses timestamp columns to identify new or changed records.
    """
    
    def load_incremental(
        self, 
        source_path: str, 
        target_path: str, 
        watermark_path: str,
        timestamp_column: str = "modified_date",
        table_name: str = "incremental_table"
    ) -> DataFrame:
        """
        Load data incrementally based on timestamp
        
        Args:
            source_path: Path to source data
            target_path: Path to target/destination
            watermark_path: Path to store watermark
            timestamp_column: Name of timestamp column
            table_name: Table identifier for watermark
            
        Returns:
            DataFrame with newly loaded records
        """
        print(f"Starting timestamp-based incremental load for {table_name}...")
        
        # Get last watermark
        last_watermark = self.get_watermark(table_name, watermark_path)
        print(f"Last watermark: {last_watermark}")
        
        # Read source data
        source_df = self.spark.read.parquet(source_path)
        
        # Filter for new/modified records
        incremental_df = source_df.filter(
            F.col(timestamp_column) > F.lit(last_watermark)
        )
        
        record_count = incremental_df.count()
        print(f"Found {record_count} new/modified records")
        
        if record_count > 0:
            # Write incremental data
            incremental_df.write.mode("append").parquet(target_path)
            
            # Update watermark with max timestamp
            new_watermark = incremental_df.agg(
                F.max(timestamp_column).alias("max_timestamp")
            ).first()["max_timestamp"]
            
            self.save_watermark(table_name, str(new_watermark), watermark_path)
            print(f"New watermark: {new_watermark}")
        
        return incremental_df


class CDCLoader(IncrementalLoader):
    """
    Pattern 2: Change Data Capture (CDC) Loading
    Processes CDC events (INSERT, UPDATE, DELETE) from change streams.
    """
    
    def process_cdc(
        self,
        cdc_source_path: str,
        target_path: str,
        watermark_path: str,
        primary_key: str = "id",
        table_name: str = "cdc_table"
    ) -> DataFrame:
        """
        Process CDC events and apply changes to target
        
        Args:
            cdc_source_path: Path to CDC change stream data
            target_path: Path to target table
            watermark_path: Path to store watermark
            primary_key: Primary key column name
            table_name: Table identifier
            
        Returns:
            DataFrame with final state after applying CDC
        """
        print(f"Starting CDC processing for {table_name}...")
        
        # Get last processed sequence
        last_sequence = self.get_watermark(table_name, watermark_path, "0")
        print(f"Last processed sequence: {last_sequence}")
        
        # Read CDC data (expected columns: operation, sequence_num, and data columns)
        cdc_df = self.spark.read.parquet(cdc_source_path)
        
        # Filter for new CDC events
        new_cdc_events = cdc_df.filter(
            F.col("sequence_num") > F.lit(int(last_sequence))
        ).orderBy("sequence_num")
        
        event_count = new_cdc_events.count()
        print(f"Found {event_count} new CDC events")
        
        if event_count == 0:
            return self.spark.read.parquet(target_path)
        
        # Read current target data
        try:
            target_df = self.spark.read.parquet(target_path)
        except:
            target_df = self.spark.createDataFrame([], new_cdc_events.schema)
        
        # Get the latest operation for each primary key using window function
        window_spec = Window.partitionBy(primary_key).orderBy(F.col("sequence_num").desc())
        
        latest_changes = new_cdc_events.withColumn(
            "row_num", F.row_number().over(window_spec)
        ).filter(F.col("row_num") == 1).drop("row_num")
        
        # Separate inserts/updates from deletes
        inserts_updates = latest_changes.filter(
            F.col("operation").isin(["INSERT", "UPDATE"])
        ).drop("operation", "sequence_num")
        
        deletes = latest_changes.filter(
            F.col("operation") == "DELETE"
        ).select(primary_key)
        
        # Apply changes
        # 1. Remove deleted records and records to be updated
        keys_to_remove = inserts_updates.select(primary_key).union(
            deletes.select(primary_key)
        )
        
        result_df = target_df.join(
            keys_to_remove,
            on=primary_key,
            how="left_anti"
        )
        
        # 2. Add inserts and updates
        result_df = result_df.union(inserts_updates)
        
        # Write result
        result_df.write.mode("overwrite").parquet(target_path)
        
        # Update watermark
        new_watermark = new_cdc_events.agg(
            F.max("sequence_num").alias("max_seq")
        ).first()["max_seq"]
        
        self.save_watermark(table_name, str(new_watermark), watermark_path)
        print(f"CDC processing complete. New sequence: {new_watermark}")
        
        return result_df


class DeltaDiffLoader(IncrementalLoader):
    """
    Pattern 3: Delta/Diff Detection
    Compares current source with last snapshot using hash/checksum to detect changes.
    """
    
    def calculate_hash(self, df: DataFrame, exclude_columns: list = None) -> DataFrame:
        """Calculate hash for each row"""
        if exclude_columns is None:
            exclude_columns = []
        
        # Get columns to hash
        hash_columns = [col for col in df.columns if col not in exclude_columns]
        
        # Concatenate columns and create hash
        return df.withColumn(
            "row_hash",
            F.sha2(F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("")) 
                                        for c in hash_columns]), 256)
        )
    
    def detect_changes(
        self,
        source_path: str,
        target_path: str,
        snapshot_path: str,
        primary_key: str = "id",
        table_name: str = "delta_table"
    ) -> dict:
        """
        Detect changes between source and last snapshot
        
        Args:
            source_path: Path to current source data
            target_path: Path to target table
            snapshot_path: Path to store snapshots
            primary_key: Primary key column name
            table_name: Table identifier
            
        Returns:
            Dictionary containing inserts, updates, and deletes DataFrames
        """
        print(f"Starting delta/diff detection for {table_name}...")
        
        # Read current source
        source_df = self.spark.read.parquet(source_path)
        source_with_hash = self.calculate_hash(source_df, exclude_columns=[])
        
        # Read last snapshot
        try:
            snapshot_df = self.spark.read.parquet(snapshot_path)
            snapshot_with_hash = self.calculate_hash(snapshot_df, exclude_columns=[])
        except:
            print("No previous snapshot found. Treating all records as inserts.")
            snapshot_with_hash = self.spark.createDataFrame([], source_with_hash.schema)
        
        # Detect inserts (records in source but not in snapshot)
        inserts = source_with_hash.join(
            snapshot_with_hash.select(primary_key),
            on=primary_key,
            how="left_anti"
        ).drop("row_hash")
        
        # Detect updates (records with same key but different hash)
        updates = source_with_hash.alias("src").join(
            snapshot_with_hash.alias("snap"),
            on=primary_key,
            how="inner"
        ).filter(
            F.col("src.row_hash") != F.col("snap.row_hash")
        ).select("src.*").drop("row_hash")
        
        # Detect deletes (records in snapshot but not in source)
        deletes = snapshot_with_hash.join(
            source_with_hash.select(primary_key),
            on=primary_key,
            how="left_anti"
        ).drop("row_hash")
        
        insert_count = inserts.count()
        update_count = updates.count()
        delete_count = deletes.count()
        
        print(f"Detected changes - Inserts: {insert_count}, Updates: {update_count}, Deletes: {delete_count}")
        
        # Apply changes to target
        if insert_count + update_count + delete_count > 0:
            try:
                target_df = self.spark.read.parquet(target_path)
            except:
                target_df = self.spark.createDataFrame([], source_df.schema)
            
            # Remove updated and deleted records
            keys_to_remove = updates.select(primary_key).union(deletes.select(primary_key))
            result_df = target_df.join(keys_to_remove, on=primary_key, how="left_anti")
            
            # Add inserts and updates
            result_df = result_df.union(inserts).union(updates)
            
            # Write result
            result_df.write.mode("overwrite").parquet(target_path)
            
            # Save current snapshot
            source_df.write.mode("overwrite").parquet(snapshot_path)
            print(f"Changes applied successfully")
        
        return {
            "inserts": inserts,
            "updates": updates,
            "deletes": deletes
        }


class SequenceBasedLoader(IncrementalLoader):
    """
    Pattern 4: Sequence Number Based Loading
    Uses auto-incrementing sequence numbers or version columns.
    """
    
    def load_incremental(
        self,
        source_path: str,
        target_path: str,
        watermark_path: str,
        sequence_column: str = "sequence_id",
        table_name: str = "sequence_table"
    ) -> DataFrame:
        """
        Load data incrementally based on sequence number
        
        Args:
            source_path: Path to source data
            target_path: Path to target/destination
            watermark_path: Path to store watermark
            sequence_column: Name of sequence column
            table_name: Table identifier
            
        Returns:
            DataFrame with newly loaded records
        """
        print(f"Starting sequence-based incremental load for {table_name}...")
        
        # Get last processed sequence
        last_sequence = self.get_watermark(table_name, watermark_path, "0")
        print(f"Last processed sequence: {last_sequence}")
        
        # Read source data
        source_df = self.spark.read.parquet(source_path)
        
        # Filter for new records
        incremental_df = source_df.filter(
            F.col(sequence_column) > F.lit(int(last_sequence))
        ).orderBy(sequence_column)
        
        record_count = incremental_df.count()
        print(f"Found {record_count} new records")
        
        if record_count > 0:
            # Write incremental data
            incremental_df.write.mode("append").parquet(target_path)
            
            # Update watermark with max sequence
            new_watermark = incremental_df.agg(
                F.max(sequence_column).alias("max_seq")
            ).first()["max_seq"]
            
            self.save_watermark(table_name, str(new_watermark), watermark_path)
            print(f"New sequence watermark: {new_watermark}")
        
        return incremental_df


# Example usage and testing
def create_sample_data(spark: SparkSession):
    """Create sample data for testing"""
    
    # Sample data for timestamp-based loading
    timestamp_data = [
        (1, "Product A", 100.0, "2024-01-01 10:00:00"),
        (2, "Product B", 150.0, "2024-01-02 11:00:00"),
        (3, "Product C", 200.0, "2024-01-03 12:00:00"),
        (4, "Product D", 250.0, "2024-01-04 13:00:00"),
    ]
    timestamp_df = spark.createDataFrame(
        timestamp_data, 
        ["id", "product_name", "price", "modified_date"]
    )
    timestamp_df = timestamp_df.withColumn(
        "modified_date", 
        F.to_timestamp("modified_date")
    )
    timestamp_df.write.mode("overwrite").parquet("/tmp/source_timestamp")
    
    # Sample CDC data
    cdc_data = [
        (1, "Alice", 25, "INSERT", 1),
        (2, "Bob", 30, "INSERT", 2),
        (1, "Alice Smith", 26, "UPDATE", 3),
        (3, "Charlie", 35, "INSERT", 4),
        (2, None, None, "DELETE", 5),
    ]
    cdc_df = spark.createDataFrame(
        cdc_data,
        ["id", "name", "age", "operation", "sequence_num"]
    )
    cdc_df.write.mode("overwrite").parquet("/tmp/source_cdc")
    
    # Sample data for delta detection
    delta_data = [
        (1, "Item A", 100),
        (2, "Item B", 200),
        (3, "Item C", 300),
    ]
    delta_df = spark.createDataFrame(delta_data, ["id", "item_name", "quantity"])
    delta_df.write.mode("overwrite").parquet("/tmp/source_delta")
    
    # Sample sequence data
    sequence_data = [
        (1, "Order 1", 500.0, 1),
        (2, "Order 2", 750.0, 2),
        (3, "Order 3", 1000.0, 3),
        (4, "Order 4", 1250.0, 4),
    ]
    sequence_df = spark.createDataFrame(
        sequence_data,
        ["id", "order_name", "amount", "sequence_id"]
    )
    sequence_df.write.mode("overwrite").parquet("/tmp/source_sequence")
    
    print("Sample data created successfully!")


def main():
    """Main function demonstrating all four patterns"""
    
    # Initialize Spark
    spark = SparkSession.builder \
        .appName("Incremental Loading Patterns") \
        .master("local[*]") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    print("=" * 80)
    print("INCREMENTAL LOADING PATTERNS WITH PYSPARK")
    print("=" * 80)
    
    # Create sample data
    create_sample_data(spark)
    
    # Pattern 1: Timestamp-based Loading
    print("\n" + "=" * 80)
    print("PATTERN 1: TIMESTAMP-BASED LOADING")
    print("=" * 80)
    timestamp_loader = TimestampBasedLoader(spark)
    result1 = timestamp_loader.load_incremental(
        source_path="/tmp/source_timestamp",
        target_path="/tmp/target_timestamp",
        watermark_path="/tmp/watermark",
        timestamp_column="modified_date",
        table_name="timestamp_table"
    )
    print(f"\nLoaded records:")
    result1.show(truncate=False)
    
    # Pattern 2: CDC Loading
    print("\n" + "=" * 80)
    print("PATTERN 2: CHANGE DATA CAPTURE (CDC) LOADING")
    print("=" * 80)
    cdc_loader = CDCLoader(spark)
    result2 = cdc_loader.process_cdc(
        cdc_source_path="/tmp/source_cdc",
        target_path="/tmp/target_cdc",
        watermark_path="/tmp/watermark",
        primary_key="id",
        table_name="cdc_table"
    )
    print(f"\nFinal state after CDC:")
    result2.show(truncate=False)
    
    # Pattern 3: Delta/Diff Detection
    print("\n" + "=" * 80)
    print("PATTERN 3: DELTA/DIFF DETECTION")
    print("=" * 80)
    delta_loader = DeltaDiffLoader(spark)
    result3 = delta_loader.detect_changes(
        source_path="/tmp/source_delta",
        target_path="/tmp/target_delta",
        snapshot_path="/tmp/snapshot_delta",
        primary_key="id",
        table_name="delta_table"
    )
    print(f"\nInserts:")
    result3["inserts"].show(truncate=False)
    
    # Pattern 4: Sequence-based Loading
    print("\n" + "=" * 80)
    print("PATTERN 4: SEQUENCE NUMBER BASED LOADING")
    print("=" * 80)
    sequence_loader = SequenceBasedLoader(spark)
    result4 = sequence_loader.load_incremental(
        source_path="/tmp/source_sequence",
        target_path="/tmp/target_sequence",
        watermark_path="/tmp/watermark",
        sequence_column="sequence_id",
        table_name="sequence_table"
    )
    print(f"\nLoaded records:")
    result4.show(truncate=False)
    
    print("\n" + "=" * 80)
    print("ALL PATTERNS COMPLETED SUCCESSFULLY!")
    print("=" * 80)
    
    spark.stop()


if __name__ == "__main__":
    main()
