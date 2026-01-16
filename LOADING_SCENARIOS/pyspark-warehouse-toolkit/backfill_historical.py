"""
Backfilling Historical Data with PySpark
This module implements various patterns for efficiently loading historical data.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime, timedelta
from typing import List, Tuple, Optional, Dict
import json


class BackfillManager:
    """Base class for backfilling operations"""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
    
    def save_backfill_state(self, backfill_id: str, state: dict, state_path: str):
        """Save backfill progress state"""
        state_data = [(
            backfill_id,
            json.dumps(state),
            datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        )]
        
        state_df = self.spark.createDataFrame(
            state_data,
            ["backfill_id", "state_json", "last_updated"]
        )
        state_df.write.mode("overwrite").parquet(f"{state_path}/{backfill_id}")
    
    def get_backfill_state(self, backfill_id: str, state_path: str) -> Optional[dict]:
        """Retrieve backfill state"""
        try:
            state_df = self.spark.read.parquet(f"{state_path}/{backfill_id}")
            state_json = state_df.select("state_json").first()[0]
            return json.loads(state_json)
        except:
            return None
    
    def log_progress(self, message: str, metrics: dict = None):
        """Log progress with optional metrics"""
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print(f"[{timestamp}] {message}")
        if metrics:
            for key, value in metrics.items():
                print(f"  - {key}: {value}")


class TimestampBackfill(BackfillManager):
    """
    Backfill historical data using timestamp-based partitioning.
    Divides the historical period into smaller chunks for efficient processing.
    """
    
    def generate_time_ranges(
        self,
        start_date: str,
        end_date: str,
        interval_days: int = 1
    ) -> List[Tuple[str, str]]:
        """
        Generate list of time ranges for backfilling
        
        Args:
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            interval_days: Days per chunk
            
        Returns:
            List of (start, end) timestamp tuples
        """
        start = datetime.strptime(start_date, '%Y-%m-%d')
        end = datetime.strptime(end_date, '%Y-%m-%d')
        
        ranges = []
        current = start
        
        while current < end:
            range_end = min(current + timedelta(days=interval_days), end)
            ranges.append((
                current.strftime('%Y-%m-%d %H:%M:%S'),
                range_end.strftime('%Y-%m-%d %H:%M:%S')
            ))
            current = range_end
        
        return ranges
    
    def backfill_by_time_range(
        self,
        source_path: str,
        target_path: str,
        state_path: str,
        start_date: str,
        end_date: str,
        timestamp_column: str = "created_date",
        interval_days: int = 1,
        backfill_id: str = "timestamp_backfill",
        resume: bool = True
    ) -> Dict[str, any]:
        """
        Backfill data by processing time ranges sequentially
        
        Args:
            source_path: Path to source data
            target_path: Path to target destination
            state_path: Path to save state for resumability
            start_date: Backfill start date (YYYY-MM-DD)
            end_date: Backfill end date (YYYY-MM-DD)
            timestamp_column: Name of timestamp column
            interval_days: Days per processing chunk
            backfill_id: Unique identifier for this backfill job
            resume: Whether to resume from last successful checkpoint
            
        Returns:
            Dictionary with backfill statistics
        """
        print("="*80)
        print(f"TIMESTAMP-BASED BACKFILL: {start_date} to {end_date}")
        print("="*80)
        
        # Generate time ranges
        time_ranges = self.generate_time_ranges(start_date, end_date, interval_days)
        total_ranges = len(time_ranges)
        
        print(f"\nBackfill plan: {total_ranges} chunks of {interval_days} day(s) each")
        
        # Check for existing state
        completed_ranges = []
        start_index = 0
        
        if resume:
            state = self.get_backfill_state(backfill_id, state_path)
            if state:
                completed_ranges = state.get('completed_ranges', [])
                start_index = len(completed_ranges)
                print(f"Resuming from checkpoint: {start_index}/{total_ranges} ranges completed")
        
        # Statistics
        stats = {
            'total_ranges': total_ranges,
            'completed_ranges': start_index,
            'total_records': 0,
            'failed_ranges': [],
            'start_time': datetime.now()
        }
        
        # Read source data once
        source_df = self.spark.read.parquet(source_path)
        
        # Process each time range
        for idx in range(start_index, total_ranges):
            range_start, range_end = time_ranges[idx]
            
            try:
                self.log_progress(
                    f"Processing range {idx+1}/{total_ranges}: {range_start} to {range_end}"
                )
                
                # Filter data for this time range
                range_df = source_df.filter(
                    (F.col(timestamp_column) >= F.lit(range_start)) &
                    (F.col(timestamp_column) < F.lit(range_end))
                )
                
                record_count = range_df.count()
                
                if record_count > 0:
                    # Write data
                    range_df.write.mode("append").parquet(target_path)
                    stats['total_records'] += record_count
                    
                    self.log_progress(
                        f"Range completed",
                        {'records_loaded': record_count, 'total_so_far': stats['total_records']}
                    )
                else:
                    self.log_progress("No records in this range")
                
                # Save checkpoint
                completed_ranges.append({
                    'range': (range_start, range_end),
                    'records': record_count,
                    'completed_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                })
                
                self.save_backfill_state(
                    backfill_id,
                    {'completed_ranges': completed_ranges},
                    state_path
                )
                
            except Exception as e:
                self.log_progress(f"ERROR in range {idx+1}: {str(e)}")
                stats['failed_ranges'].append({
                    'range': (range_start, range_end),
                    'error': str(e)
                })
        
        stats['end_time'] = datetime.now()
        stats['duration'] = str(stats['end_time'] - stats['start_time'])
        
        print("\n" + "="*80)
        print("BACKFILL SUMMARY")
        print("="*80)
        print(f"Total records loaded: {stats['total_records']:,}")
        print(f"Ranges processed: {len(completed_ranges)}/{total_ranges}")
        print(f"Failed ranges: {len(stats['failed_ranges'])}")
        print(f"Duration: {stats['duration']}")
        
        return stats


class ParallelBackfill(BackfillManager):
    """
    Backfill historical data using parallel processing.
    Processes multiple time ranges or partitions simultaneously.
    """
    
    def backfill_parallel(
        self,
        source_path: str,
        target_path: str,
        start_date: str,
        end_date: str,
        timestamp_column: str = "created_date",
        num_partitions: int = 10,
        partition_column: str = None
    ) -> Dict[str, any]:
        """
        Backfill data using parallel processing
        
        Args:
            source_path: Path to source data
            target_path: Path to target destination
            start_date: Backfill start date
            end_date: Backfill end date
            timestamp_column: Name of timestamp column
            num_partitions: Number of parallel partitions
            partition_column: Optional column to partition by (e.g., 'country', 'product_category')
            
        Returns:
            Dictionary with backfill statistics
        """
        print("="*80)
        print(f"PARALLEL BACKFILL: {start_date} to {end_date}")
        print("="*80)
        
        start_time = datetime.now()
        
        # Read source data
        source_df = self.spark.read.parquet(source_path)
        
        # Filter for backfill period
        backfill_df = source_df.filter(
            (F.col(timestamp_column) >= F.lit(start_date)) &
            (F.col(timestamp_column) < F.lit(end_date))
        )
        
        total_records = backfill_df.count()
        print(f"\nTotal records to backfill: {total_records:,}")
        
        # Repartition for parallel processing
        if partition_column:
            print(f"Partitioning by column: {partition_column}")
            backfill_df = backfill_df.repartition(num_partitions, partition_column)
        else:
            print(f"Using {num_partitions} partitions")
            backfill_df = backfill_df.repartition(num_partitions)
        
        # Write in parallel
        print("\nWriting data in parallel...")
        backfill_df.write.mode("append").parquet(target_path)
        
        end_time = datetime.now()
        duration = end_time - start_time
        
        stats = {
            'total_records': total_records,
            'num_partitions': num_partitions,
            'duration': str(duration),
            'records_per_second': total_records / duration.total_seconds() if duration.total_seconds() > 0 else 0
        }
        
        print("\n" + "="*80)
        print("PARALLEL BACKFILL SUMMARY")
        print("="*80)
        print(f"Total records: {stats['total_records']:,}")
        print(f"Partitions: {stats['num_partitions']}")
        print(f"Duration: {stats['duration']}")
        print(f"Throughput: {stats['records_per_second']:,.0f} records/second")
        
        return stats


class SequenceBackfill(BackfillManager):
    """
    Backfill historical data using sequence number ranges.
    Useful for event logs and append-only data.
    """
    
    def backfill_by_sequence_range(
        self,
        source_path: str,
        target_path: str,
        state_path: str,
        start_sequence: int,
        end_sequence: int,
        sequence_column: str = "sequence_id",
        batch_size: int = 100000,
        backfill_id: str = "sequence_backfill",
        resume: bool = True
    ) -> Dict[str, any]:
        """
        Backfill data by processing sequence number ranges
        
        Args:
            source_path: Path to source data
            target_path: Path to target destination
            state_path: Path to save state
            start_sequence: Starting sequence number
            end_sequence: Ending sequence number
            sequence_column: Name of sequence column
            batch_size: Records per batch
            backfill_id: Unique identifier
            resume: Whether to resume from checkpoint
            
        Returns:
            Dictionary with backfill statistics
        """
        print("="*80)
        print(f"SEQUENCE-BASED BACKFILL: {start_sequence} to {end_sequence}")
        print("="*80)
        
        # Calculate batches
        total_sequences = end_sequence - start_sequence + 1
        num_batches = (total_sequences + batch_size - 1) // batch_size
        
        print(f"\nBackfill plan: {num_batches} batches of {batch_size:,} records each")
        
        # Check for existing state
        current_sequence = start_sequence
        
        if resume:
            state = self.get_backfill_state(backfill_id, state_path)
            if state:
                current_sequence = state.get('last_sequence', start_sequence)
                print(f"Resuming from sequence: {current_sequence}")
        
        # Statistics
        stats = {
            'total_records': 0,
            'batches_processed': 0,
            'start_time': datetime.now()
        }
        
        # Read source data
        source_df = self.spark.read.parquet(source_path)
        
        # Process in batches
        while current_sequence <= end_sequence:
            batch_end = min(current_sequence + batch_size - 1, end_sequence)
            
            try:
                self.log_progress(
                    f"Processing sequences {current_sequence} to {batch_end}"
                )
                
                # Filter for this batch
                batch_df = source_df.filter(
                    (F.col(sequence_column) >= F.lit(current_sequence)) &
                    (F.col(sequence_column) <= F.lit(batch_end))
                )
                
                record_count = batch_df.count()
                
                if record_count > 0:
                    # Write batch
                    batch_df.write.mode("append").parquet(target_path)
                    stats['total_records'] += record_count
                    stats['batches_processed'] += 1
                    
                    self.log_progress(
                        f"Batch completed",
                        {'records': record_count, 'total': stats['total_records']}
                    )
                
                # Save checkpoint
                self.save_backfill_state(
                    backfill_id,
                    {'last_sequence': batch_end},
                    state_path
                )
                
                current_sequence = batch_end + 1
                
            except Exception as e:
                self.log_progress(f"ERROR: {str(e)}")
                break
        
        stats['end_time'] = datetime.now()
        stats['duration'] = str(stats['end_time'] - stats['start_time'])
        
        print("\n" + "="*80)
        print("BACKFILL SUMMARY")
        print("="*80)
        print(f"Total records: {stats['total_records']:,}")
        print(f"Batches processed: {stats['batches_processed']}")
        print(f"Duration: {stats['duration']}")
        
        return stats


class FullHistoricalLoad(BackfillManager):
    """
    One-time full historical load with validation and quality checks.
    """
    
    def full_load_with_validation(
        self,
        source_path: str,
        target_path: str,
        validation_rules: dict = None,
        dedup_column: str = None,
        partition_by: List[str] = None
    ) -> Dict[str, any]:
        """
        Load full historical data with validation
        
        Args:
            source_path: Path to source data
            target_path: Path to target destination
            validation_rules: Dictionary of validation rules
            dedup_column: Column to deduplicate on (keeps latest)
            partition_by: List of columns to partition output by
            
        Returns:
            Dictionary with load statistics
        """
        print("="*80)
        print("FULL HISTORICAL LOAD WITH VALIDATION")
        print("="*80)
        
        start_time = datetime.now()
        
        # Read source data
        print("\nReading source data...")
        source_df = self.spark.read.parquet(source_path)
        initial_count = source_df.count()
        
        print(f"Source records: {initial_count:,}")
        
        # Apply validation rules
        if validation_rules:
            print("\nApplying validation rules...")
            for rule_name, rule_func in validation_rules.items():
                before_count = source_df.count()
                source_df = rule_func(source_df)
                after_count = source_df.count()
                removed = before_count - after_count
                
                self.log_progress(
                    f"Validation: {rule_name}",
                    {'removed': removed, 'remaining': after_count}
                )
        
        # Deduplication
        if dedup_column:
            print(f"\nDeduplicating on column: {dedup_column}")
            before_count = source_df.count()
            
            window_spec = Window.partitionBy(dedup_column).orderBy(F.col("_input_file_name").desc())
            source_df = source_df.withColumn("row_num", F.row_number().over(window_spec))
            source_df = source_df.filter(F.col("row_num") == 1).drop("row_num")
            
            after_count = source_df.count()
            duplicates = before_count - after_count
            
            self.log_progress(
                "Deduplication complete",
                {'duplicates_removed': duplicates, 'unique_records': after_count}
            )
        
        final_count = source_df.count()
        
        # Write data
        print("\nWriting data to target...")
        writer = source_df.write.mode("overwrite")
        
        if partition_by:
            print(f"Partitioning by: {', '.join(partition_by)}")
            writer = writer.partitionBy(*partition_by)
        
        writer.parquet(target_path)
        
        end_time = datetime.now()
        duration = end_time - start_time
        
        stats = {
            'initial_records': initial_count,
            'final_records': final_count,
            'records_removed': initial_count - final_count,
            'removal_percentage': ((initial_count - final_count) / initial_count * 100) if initial_count > 0 else 0,
            'duration': str(duration),
            'partitioned_by': partition_by or 'None'
        }
        
        print("\n" + "="*80)
        print("FULL LOAD SUMMARY")
        print("="*80)
        print(f"Initial records: {stats['initial_records']:,}")
        print(f"Final records: {stats['final_records']:,}")
        print(f"Records removed: {stats['records_removed']:,} ({stats['removal_percentage']:.2f}%)")
        print(f"Duration: {stats['duration']}")
        
        return stats


class IncrementalBackfillValidator(BackfillManager):
    """
    Validates backfilled data against source to ensure completeness and accuracy.
    """
    
    def validate_backfill(
        self,
        source_path: str,
        target_path: str,
        date_column: str,
        start_date: str,
        end_date: str,
        group_by_columns: List[str] = None
    ) -> Dict[str, any]:
        """
        Validate that backfill matches source data
        
        Args:
            source_path: Path to source data
            target_path: Path to backfilled data
            date_column: Date column for filtering
            start_date: Start of backfill period
            end_date: End of backfill period
            group_by_columns: Columns to group by for detailed validation
            
        Returns:
            Dictionary with validation results
        """
        print("="*80)
        print("BACKFILL VALIDATION")
        print("="*80)
        
        # Read source and target
        source_df = self.spark.read.parquet(source_path)
        target_df = self.spark.read.parquet(target_path)
        
        # Filter for backfill period
        source_filtered = source_df.filter(
            (F.col(date_column) >= F.lit(start_date)) &
            (F.col(date_column) < F.lit(end_date))
        )
        
        target_filtered = target_df.filter(
            (F.col(date_column) >= F.lit(start_date)) &
            (F.col(date_column) < F.lit(end_date))
        )
        
        # Count validation
        source_count = source_filtered.count()
        target_count = target_filtered.count()
        
        print(f"\nRecord count validation:")
        print(f"  Source: {source_count:,}")
        print(f"  Target: {target_count:,}")
        print(f"  Difference: {abs(source_count - target_count):,}")
        
        validation_results = {
            'source_count': source_count,
            'target_count': target_count,
            'count_match': source_count == target_count,
            'difference': abs(source_count - target_count)
        }
        
        # Detailed validation by groups
        if group_by_columns:
            print(f"\nDetailed validation by {', '.join(group_by_columns)}:")
            
            source_grouped = source_filtered.groupBy(group_by_columns).count().withColumnRenamed("count", "source_count")
            target_grouped = target_filtered.groupBy(group_by_columns).count().withColumnRenamed("count", "target_count")
            
            comparison = source_grouped.join(
                target_grouped,
                on=group_by_columns,
                how="outer"
            ).fillna(0, subset=["source_count", "target_count"])
            
            comparison = comparison.withColumn(
                "difference",
                F.abs(F.col("source_count") - F.col("target_count"))
            )
            
            mismatches = comparison.filter(F.col("difference") > 0)
            mismatch_count = mismatches.count()
            
            if mismatch_count > 0:
                print(f"\n  Found {mismatch_count} mismatches:")
                mismatches.orderBy(F.col("difference").desc()).show(20, truncate=False)
                validation_results['group_mismatches'] = mismatch_count
            else:
                print("  All groups match perfectly!")
                validation_results['group_mismatches'] = 0
        
        # Overall validation status
        is_valid = validation_results['count_match']
        if group_by_columns:
            is_valid = is_valid and (validation_results['group_mismatches'] == 0)
        
        validation_results['is_valid'] = is_valid
        
        print("\n" + "="*80)
        print(f"VALIDATION RESULT: {'PASS ✓' if is_valid else 'FAIL ✗'}")
        print("="*80)
        
        return validation_results


# Example usage and testing
def create_historical_data(spark: SparkSession):
    """Create sample historical data for testing"""
    from pyspark.sql.types import *
    
    # Create 90 days of historical sales data
    print("Creating historical sample data...")
    
    base_date = datetime(2023, 10, 1)
    data = []
    
    sequence_id = 1
    for day in range(90):
        current_date = base_date + timedelta(days=day)
        
        # Generate 100-500 records per day
        records_per_day = 100 + (day * 5)
        
        for i in range(records_per_day):
            data.append((
                sequence_id,
                1000 + (day * 1000) + i,
                f"PROD-{(i % 10) + 1}",
                round(50 + (i % 100) * 3.5, 2),
                (i % 5) + 1,
                current_date.strftime('%Y-%m-%d %H:%M:%S'),
                f"STORE-{(i % 20) + 1}"
            ))
            sequence_id += 1
    
    schema = StructType([
        StructField("sequence_id", IntegerType(), False),
        StructField("transaction_id", IntegerType(), False),
        StructField("product_id", StringType(), False),
        StructField("amount", DoubleType(), False),
        StructField("quantity", IntegerType(), False),
        StructField("transaction_date", StringType(), False),
        StructField("store_id", StringType(), False)
    ])
    
    sales_df = spark.createDataFrame(data, schema)
    sales_df = sales_df.withColumn("transaction_date", F.to_timestamp("transaction_date"))
    
    # Add some duplicates for deduplication testing
    duplicates = sales_df.limit(100)
    sales_df = sales_df.union(duplicates)
    
    sales_df.write.mode("overwrite").parquet("/tmp/backfill/historical_source")
    
    total_records = sales_df.count()
    print(f"Created {total_records:,} historical records (90 days)")
    
    return sales_df


def main():
    """Main function demonstrating all backfill patterns"""
    
    spark = SparkSession.builder \
        .appName("Backfill Patterns") \
        .master("local[*]") \
        .config("spark.sql.shuffle.partitions", "10") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    print("="*80)
    print("HISTORICAL DATA BACKFILLING PATTERNS WITH PYSPARK")
    print("="*80)
    
    # Create historical sample data
    create_historical_data(spark)
    
    # Pattern 1: Sequential Time-based Backfill
    print("\n\n")
    print("#" * 80)
    print("PATTERN 1: SEQUENTIAL TIME-BASED BACKFILL")
    print("#" * 80)
    
    timestamp_backfill = TimestampBackfill(spark)
    stats1 = timestamp_backfill.backfill_by_time_range(
        source_path="/tmp/backfill/historical_source",
        target_path="/tmp/backfill/target_sequential",
        state_path="/tmp/backfill/state",
        start_date="2023-10-01",
        end_date="2023-10-31",
        timestamp_column="transaction_date",
        interval_days=7,  # Process 1 week at a time
        backfill_id="sales_backfill_sequential"
    )
    
    # Pattern 2: Parallel Backfill
    print("\n\n")
    print("#" * 80)
    print("PATTERN 2: PARALLEL BACKFILL")
    print("#" * 80)
    
    parallel_backfill = ParallelBackfill(spark)
    stats2 = parallel_backfill.backfill_parallel(
        source_path="/tmp/backfill/historical_source",
        target_path="/tmp/backfill/target_parallel",
        start_date="2023-11-01",
        end_date="2023-11-30",
        timestamp_column="transaction_date",
        num_partitions=8,
        partition_column="store_id"
    )
    
    # Pattern 3: Sequence-based Backfill
    print("\n\n")
    print("#" * 80)
    print("PATTERN 3: SEQUENCE-BASED BACKFILL")
    print("#" * 80)
    
    sequence_backfill = SequenceBackfill(spark)
    stats3 = sequence_backfill.backfill_by_sequence_range(
        source_path="/tmp/backfill/historical_source",
        target_path="/tmp/backfill/target_sequence",
        state_path="/tmp/backfill/state",
        start_sequence=1,
        end_sequence=50000,
        sequence_column="sequence_id",
        batch_size=10000,
        backfill_id="sales_backfill_sequence"
    )
    
    # Pattern 4: Full Historical Load with Validation
    print("\n\n")
    print("#" * 80)
    print("PATTERN 4: FULL HISTORICAL LOAD WITH VALIDATION")
    print("#" * 80)
    
    # Define validation rules
    validation_rules = {
        'remove_nulls': lambda df: df.filter(F.col("transaction_id").isNotNull()),
        'positive_amounts': lambda df: df.filter(F.col("amount") > 0),
        'valid_dates': lambda df: df.filter(F.col("transaction_date").isNotNull())
    }
    
    full_load = FullHistoricalLoad(spark)
    stats4 = full_load.full_load_with_validation(
        source_path="/tmp/backfill/historical_source",
        target_path="/tmp/backfill/target_full",
        validation_rules=validation_rules,
        dedup_column="transaction_id",
        partition_by=["store_id"]
    )
    
    # Pattern 5: Validate Backfill
    print("\n\n")
    print("#" * 80)
    print("PATTERN 5: BACKFILL VALIDATION")
    print("#" * 80)
    
    validator = IncrementalBackfillValidator(spark)
    validation_result = validator.validate_backfill(
        source_path="/tmp/backfill/historical_source",
        target_path="/tmp/backfill/target_sequential",
        date_column="transaction_date",
        start_date="2023-10-01",
        end_date="2023-10-31",
        group_by_columns=["store_id"]
    )
    
    print("\n\n")
    print("="*80)
    print("ALL BACKFILL PATTERNS COMPLETED!")
    print("="*80)
    
    spark.stop()


if __name__ == "__main__":
    main()
