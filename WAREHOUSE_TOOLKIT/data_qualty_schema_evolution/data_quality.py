"""
Data Quality Framework for PySpark
Comprehensive data quality checks optimized for huge data volumes
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import *
from typing import Dict, List, Tuple, Optional, Callable
from datetime import datetime, timedelta
from enum import Enum
import json


class QualityCheckStatus(Enum):
    """Status of quality check"""
    PASSED = "PASSED"
    FAILED = "FAILED"
    WARNING = "WARNING"


class DataQualityFramework:
    """
    Comprehensive data quality framework for PySpark.
    Optimized for processing huge volumes of data efficiently.
    """
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.quality_results = []
    
    def log_check(self, check_name: str, status: QualityCheckStatus, 
                  details: dict, metrics: dict = None):
        """Log quality check result"""
        result = {
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'check_name': check_name,
            'status': status.value,
            'details': details,
            'metrics': metrics or {}
        }
        self.quality_results.append(result)
    
    def get_quality_report(self) -> DataFrame:
        """Get quality check results as DataFrame"""
        if not self.quality_results:
            return self.spark.createDataFrame([], 
                StructType([
                    StructField("timestamp", StringType()),
                    StructField("check_name", StringType()),
                    StructField("status", StringType()),
                    StructField("details", StringType()),
                    StructField("metrics", StringType())
                ])
            )
        
        report_data = [
            (
                r['timestamp'],
                r['check_name'],
                r['status'],
                json.dumps(r['details']),
                json.dumps(r['metrics'])
            )
            for r in self.quality_results
        ]
        
        return self.spark.createDataFrame(
            report_data,
            ['timestamp', 'check_name', 'status', 'details', 'metrics']
        )
    
    # ========================================================================
    # COMPLETENESS CHECKS
    # ========================================================================
    
    def check_null_counts(
        self,
        df: DataFrame,
        columns: List[str] = None,
        threshold_pct: float = 5.0,
        sample_fraction: float = 1.0
    ) -> Dict:
        """
        Check for null values in columns
        
        Args:
            df: Input DataFrame
            columns: List of columns to check (None = all columns)
            threshold_pct: Threshold percentage for null values (fail if exceeded)
            sample_fraction: Fraction of data to sample (0.0-1.0) for huge datasets
            
        Returns:
            Dictionary with null count results
        """
        print(f"\n{'='*80}")
        print(f"COMPLETENESS CHECK: Null Value Analysis")
        print(f"{'='*80}")
        
        # Sample if needed for performance
        working_df = df.sample(sample_fraction) if sample_fraction < 1.0 else df
        total_rows = working_df.count()
        
        columns = columns or df.columns
        
        # Calculate null counts efficiently in single pass
        null_exprs = [
            F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(c) 
            for c in columns
        ]
        
        null_counts = working_df.select(null_exprs).first().asDict()
        
        # Calculate percentages and status
        results = []
        failed_columns = []
        
        for col, null_count in null_counts.items():
            null_pct = (null_count / total_rows * 100) if total_rows > 0 else 0
            status = QualityCheckStatus.FAILED if null_pct > threshold_pct else QualityCheckStatus.PASSED
            
            results.append({
                'column': col,
                'null_count': null_count,
                'total_rows': total_rows,
                'null_percentage': round(null_pct, 2),
                'threshold': threshold_pct,
                'status': status.value
            })
            
            if status == QualityCheckStatus.FAILED:
                failed_columns.append(col)
            
            print(f"  {col:30s}: {null_count:10,d} nulls ({null_pct:5.2f}%) - {status.value}")
        
        overall_status = QualityCheckStatus.FAILED if failed_columns else QualityCheckStatus.PASSED
        
        self.log_check(
            'null_counts',
            overall_status,
            {'failed_columns': failed_columns},
            {'total_rows': total_rows, 'columns_checked': len(columns)}
        )
        
        return {
            'status': overall_status.value,
            'results': results,
            'failed_columns': failed_columns
        }
    
    def check_required_fields(
        self,
        df: DataFrame,
        required_columns: List[str]
    ) -> Dict:
        """
        Check that required columns have no null values
        
        Args:
            df: Input DataFrame
            required_columns: List of columns that must not have nulls
            
        Returns:
            Dictionary with check results
        """
        print(f"\n{'='*80}")
        print(f"COMPLETENESS CHECK: Required Fields")
        print(f"{'='*80}")
        
        total_rows = df.count()
        violations = []
        
        for col in required_columns:
            if col not in df.columns:
                violations.append({
                    'column': col,
                    'issue': 'Column does not exist',
                    'null_count': None
                })
                continue
            
            null_count = df.filter(F.col(col).isNull()).count()
            
            if null_count > 0:
                violations.append({
                    'column': col,
                    'issue': 'Contains null values',
                    'null_count': null_count,
                    'null_percentage': round(null_count / total_rows * 100, 2)
                })
                print(f"  ✗ {col}: {null_count:,} null values ({null_count/total_rows*100:.2f}%)")
            else:
                print(f"  ✓ {col}: No null values")
        
        status = QualityCheckStatus.FAILED if violations else QualityCheckStatus.PASSED
        
        self.log_check(
            'required_fields',
            status,
            {'violations': violations}
        )
        
        return {
            'status': status.value,
            'violations': violations
        }
    
    # ========================================================================
    # UNIQUENESS CHECKS
    # ========================================================================
    
    def check_duplicates(
        self,
        df: DataFrame,
        key_columns: List[str],
        sample_size: int = None
    ) -> Dict:
        """
        Check for duplicate records based on key columns
        
        Args:
            df: Input DataFrame
            key_columns: Columns that should be unique
            sample_size: Number of duplicate examples to return
            
        Returns:
            Dictionary with duplicate analysis
        """
        print(f"\n{'='*80}")
        print(f"UNIQUENESS CHECK: Duplicate Detection")
        print(f"Key Columns: {', '.join(key_columns)}")
        print(f"{'='*80}")
        
        total_rows = df.count()
        
        # Find duplicates
        duplicate_check = df.groupBy(key_columns).count().filter(F.col("count") > 1)
        duplicate_groups = duplicate_check.count()
        total_duplicates = duplicate_check.agg(F.sum("count")).first()[0] or 0
        
        duplicate_percentage = (total_duplicates / total_rows * 100) if total_rows > 0 else 0
        
        print(f"\nTotal rows: {total_rows:,}")
        print(f"Duplicate groups: {duplicate_groups:,}")
        print(f"Total duplicate records: {total_duplicates:,} ({duplicate_percentage:.2f}%)")
        
        # Get sample duplicates
        sample_duplicates = []
        if sample_size and duplicate_groups > 0:
            sample_duplicates = duplicate_check.limit(sample_size).collect()
            
            print(f"\nSample duplicates (showing {min(sample_size, len(sample_duplicates))}):")
            for row in sample_duplicates[:10]:
                key_vals = {col: row[col] for col in key_columns}
                print(f"  {key_vals}: {row['count']} occurrences")
        
        status = QualityCheckStatus.FAILED if duplicate_groups > 0 else QualityCheckStatus.PASSED
        
        self.log_check(
            'duplicates',
            status,
            {'duplicate_groups': duplicate_groups, 'total_duplicates': total_duplicates},
            {'total_rows': total_rows, 'duplicate_percentage': round(duplicate_percentage, 2)}
        )
        
        return {
            'status': status.value,
            'total_rows': total_rows,
            'duplicate_groups': duplicate_groups,
            'total_duplicates': total_duplicates,
            'duplicate_percentage': round(duplicate_percentage, 2)
        }
    
    def check_primary_key_uniqueness(
        self,
        df: DataFrame,
        primary_key: str
    ) -> Dict:
        """
        Check that primary key column has unique values
        
        Args:
            df: Input DataFrame
            primary_key: Primary key column name
            
        Returns:
            Dictionary with uniqueness results
        """
        print(f"\n{'='*80}")
        print(f"UNIQUENESS CHECK: Primary Key '{primary_key}'")
        print(f"{'='*80}")
        
        total_rows = df.count()
        distinct_count = df.select(primary_key).distinct().count()
        
        is_unique = total_rows == distinct_count
        duplicates = total_rows - distinct_count
        
        print(f"Total rows: {total_rows:,}")
        print(f"Distinct values: {distinct_count:,}")
        print(f"Duplicates: {duplicates:,}")
        print(f"Status: {'✓ UNIQUE' if is_unique else '✗ NOT UNIQUE'}")
        
        status = QualityCheckStatus.PASSED if is_unique else QualityCheckStatus.FAILED
        
        self.log_check(
            'primary_key_uniqueness',
            status,
            {'primary_key': primary_key, 'duplicates': duplicates}
        )
        
        return {
            'status': status.value,
            'is_unique': is_unique,
            'total_rows': total_rows,
            'distinct_count': distinct_count,
            'duplicates': duplicates
        }
    
    # ========================================================================
    # VALIDITY CHECKS
    # ========================================================================
    
    def check_value_ranges(
        self,
        df: DataFrame,
        range_rules: Dict[str, Tuple[float, float]]
    ) -> Dict:
        """
        Check that numeric values are within expected ranges
        
        Args:
            df: Input DataFrame
            range_rules: Dict mapping column names to (min, max) tuples
            
        Returns:
            Dictionary with range validation results
        """
        print(f"\n{'='*80}")
        print(f"VALIDITY CHECK: Value Ranges")
        print(f"{'='*80}")
        
        total_rows = df.count()
        violations = []
        
        for col, (min_val, max_val) in range_rules.items():
            if col not in df.columns:
                continue
            
            # Count violations
            out_of_range = df.filter(
                (F.col(col) < min_val) | (F.col(col) > max_val)
            ).count()
            
            violation_pct = (out_of_range / total_rows * 100) if total_rows > 0 else 0
            
            if out_of_range > 0:
                # Get min/max values in data
                stats = df.agg(
                    F.min(col).alias('actual_min'),
                    F.max(col).alias('actual_max')
                ).first()
                
                violations.append({
                    'column': col,
                    'expected_range': f'[{min_val}, {max_val}]',
                    'actual_range': f'[{stats.actual_min}, {stats.actual_max}]',
                    'violations': out_of_range,
                    'violation_percentage': round(violation_pct, 2)
                })
                
                print(f"  ✗ {col}: {out_of_range:,} violations ({violation_pct:.2f}%)")
                print(f"    Expected: [{min_val}, {max_val}]")
                print(f"    Actual: [{stats.actual_min}, {stats.actual_max}]")
            else:
                print(f"  ✓ {col}: All values in range [{min_val}, {max_val}]")
        
        status = QualityCheckStatus.FAILED if violations else QualityCheckStatus.PASSED
        
        self.log_check(
            'value_ranges',
            status,
            {'violations': violations}
        )
        
        return {
            'status': status.value,
            'violations': violations
        }
    
    def check_valid_values(
        self,
        df: DataFrame,
        valid_values: Dict[str, List]
    ) -> Dict:
        """
        Check that categorical columns only contain valid values
        
        Args:
            df: Input DataFrame
            valid_values: Dict mapping column names to list of valid values
            
        Returns:
            Dictionary with validation results
        """
        print(f"\n{'='*80}")
        print(f"VALIDITY CHECK: Valid Values (Enums)")
        print(f"{'='*80}")
        
        total_rows = df.count()
        violations = []
        
        for col, valid_list in valid_values.items():
            if col not in df.columns:
                continue
            
            # Find invalid values
            invalid_values = df.filter(
                ~F.col(col).isin(valid_list) & F.col(col).isNotNull()
            )
            
            invalid_count = invalid_values.count()
            
            if invalid_count > 0:
                # Get sample of invalid values
                invalid_samples = invalid_values.select(col).distinct().limit(10).collect()
                invalid_samples = [row[col] for row in invalid_samples]
                
                violation_pct = (invalid_count / total_rows * 100)
                
                violations.append({
                    'column': col,
                    'valid_values': valid_list,
                    'invalid_count': invalid_count,
                    'invalid_percentage': round(violation_pct, 2),
                    'sample_invalid_values': invalid_samples
                })
                
                print(f"  ✗ {col}: {invalid_count:,} invalid values ({violation_pct:.2f}%)")
                print(f"    Valid: {valid_list}")
                print(f"    Found: {invalid_samples}")
            else:
                print(f"  ✓ {col}: All values are valid")
        
        status = QualityCheckStatus.FAILED if violations else QualityCheckStatus.PASSED
        
        self.log_check(
            'valid_values',
            status,
            {'violations': violations}
        )
        
        return {
            'status': status.value,
            'violations': violations
        }
    
    def check_regex_patterns(
        self,
        df: DataFrame,
        pattern_rules: Dict[str, str]
    ) -> Dict:
        """
        Check that string columns match expected regex patterns
        
        Args:
            df: Input DataFrame
            pattern_rules: Dict mapping column names to regex patterns
            
        Returns:
            Dictionary with pattern validation results
        """
        print(f"\n{'='*80}")
        print(f"VALIDITY CHECK: Regex Pattern Matching")
        print(f"{'='*80}")
        
        total_rows = df.count()
        violations = []
        
        for col, pattern in pattern_rules.items():
            if col not in df.columns:
                continue
            
            # Find values not matching pattern
            invalid_pattern = df.filter(
                ~F.col(col).rlike(pattern) & F.col(col).isNotNull()
            )
            
            invalid_count = invalid_pattern.count()
            
            if invalid_count > 0:
                # Get sample of invalid values
                invalid_samples = invalid_pattern.select(col).limit(5).collect()
                invalid_samples = [row[col] for row in invalid_samples]
                
                violation_pct = (invalid_count / total_rows * 100)
                
                violations.append({
                    'column': col,
                    'pattern': pattern,
                    'invalid_count': invalid_count,
                    'invalid_percentage': round(violation_pct, 2),
                    'sample_invalid_values': invalid_samples
                })
                
                print(f"  ✗ {col}: {invalid_count:,} pattern violations ({violation_pct:.2f}%)")
                print(f"    Pattern: {pattern}")
                print(f"    Samples: {invalid_samples[:3]}")
            else:
                print(f"  ✓ {col}: All values match pattern")
        
        status = QualityCheckStatus.FAILED if violations else QualityCheckStatus.PASSED
        
        self.log_check(
            'regex_patterns',
            status,
            {'violations': violations}
        )
        
        return {
            'status': status.value,
            'violations': violations
        }
    
    # ========================================================================
    # CONSISTENCY CHECKS
    # ========================================================================
    
    def check_referential_integrity(
        self,
        fact_df: DataFrame,
        dimension_df: DataFrame,
        fact_key: str,
        dimension_key: str
    ) -> Dict:
        """
        Check referential integrity between fact and dimension tables
        
        Args:
            fact_df: Fact table DataFrame
            dimension_df: Dimension table DataFrame
            fact_key: Foreign key column in fact table
            dimension_key: Primary key column in dimension table
            
        Returns:
            Dictionary with referential integrity results
        """
        print(f"\n{'='*80}")
        print(f"CONSISTENCY CHECK: Referential Integrity")
        print(f"Fact.{fact_key} -> Dimension.{dimension_key}")
        print(f"{'='*80}")
        
        fact_count = fact_df.count()
        
        # Find orphaned records (keys in fact not in dimension)
        orphaned = fact_df.join(
            dimension_df.select(dimension_key),
            fact_df[fact_key] == dimension_df[dimension_key],
            'left_anti'
        ).filter(F.col(fact_key).isNotNull())
        
        orphaned_count = orphaned.count()
        orphaned_pct = (orphaned_count / fact_count * 100) if fact_count > 0 else 0
        
        print(f"Total fact records: {fact_count:,}")
        print(f"Orphaned records: {orphaned_count:,} ({orphaned_pct:.2f}%)")
        
        # Get sample of orphaned keys
        orphaned_sample = []
        if orphaned_count > 0:
            orphaned_sample = orphaned.select(fact_key).distinct().limit(10).collect()
            orphaned_sample = [row[fact_key] for row in orphaned_sample]
            print(f"Sample orphaned keys: {orphaned_sample[:5]}")
        
        status = QualityCheckStatus.FAILED if orphaned_count > 0 else QualityCheckStatus.PASSED
        
        self.log_check(
            'referential_integrity',
            status,
            {
                'fact_key': fact_key,
                'dimension_key': dimension_key,
                'orphaned_count': orphaned_count
            },
            {'fact_count': fact_count, 'orphaned_percentage': round(orphaned_pct, 2)}
        )
        
        return {
            'status': status.value,
            'fact_count': fact_count,
            'orphaned_count': orphaned_count,
            'orphaned_percentage': round(orphaned_pct, 2),
            'orphaned_sample': orphaned_sample[:10]
        }
    
    def check_cross_field_rules(
        self,
        df: DataFrame,
        rules: List[Tuple[str, Callable[[DataFrame], DataFrame]]]
    ) -> Dict:
        """
        Check cross-field business rules
        
        Args:
            df: Input DataFrame
            rules: List of (rule_name, rule_function) tuples
                   rule_function takes DataFrame and returns filtered DataFrame with violations
            
        Returns:
            Dictionary with rule validation results
        """
        print(f"\n{'='*80}")
        print(f"CONSISTENCY CHECK: Cross-Field Business Rules")
        print(f"{'='*80}")
        
        total_rows = df.count()
        violations = []
        
        for rule_name, rule_func in rules:
            # Apply rule (should return DataFrame with violations)
            rule_violations = rule_func(df)
            violation_count = rule_violations.count()
            violation_pct = (violation_count / total_rows * 100) if total_rows > 0 else 0
            
            if violation_count > 0:
                violations.append({
                    'rule': rule_name,
                    'violations': violation_count,
                    'violation_percentage': round(violation_pct, 2)
                })
                
                print(f"  ✗ {rule_name}: {violation_count:,} violations ({violation_pct:.2f}%)")
            else:
                print(f"  ✓ {rule_name}: No violations")
        
        status = QualityCheckStatus.FAILED if violations else QualityCheckStatus.PASSED
        
        self.log_check(
            'cross_field_rules',
            status,
            {'violations': violations}
        )
        
        return {
            'status': status.value,
            'violations': violations
        }
    
    # ========================================================================
    # TIMELINESS CHECKS
    # ========================================================================
    
    def check_data_freshness(
        self,
        df: DataFrame,
        timestamp_column: str,
        max_age_hours: int = 24
    ) -> Dict:
        """
        Check that data is fresh (recent)
        
        Args:
            df: Input DataFrame
            timestamp_column: Column containing timestamps
            max_age_hours: Maximum acceptable age in hours
            
        Returns:
            Dictionary with freshness results
        """
        print(f"\n{'='*80}")
        print(f"TIMELINESS CHECK: Data Freshness")
        print(f"Maximum age: {max_age_hours} hours")
        print(f"{'='*80}")
        
        # Get latest timestamp
        latest = df.agg(F.max(timestamp_column).alias('latest')).first()['latest']
        
        if latest is None:
            return {
                'status': QualityCheckStatus.FAILED.value,
                'message': 'No timestamp data found'
            }
        
        # Calculate age
        current_time = datetime.now()
        data_age = current_time - latest
        age_hours = data_age.total_seconds() / 3600
        
        is_fresh = age_hours <= max_age_hours
        
        print(f"Latest record: {latest}")
        print(f"Current time: {current_time}")
        print(f"Data age: {age_hours:.2f} hours")
        print(f"Status: {'✓ FRESH' if is_fresh else '✗ STALE'}")
        
        status = QualityCheckStatus.PASSED if is_fresh else QualityCheckStatus.FAILED
        
        self.log_check(
            'data_freshness',
            status,
            {
                'latest_timestamp': str(latest),
                'age_hours': round(age_hours, 2),
                'threshold_hours': max_age_hours
            }
        )
        
        return {
            'status': status.value,
            'latest_timestamp': latest,
            'age_hours': round(age_hours, 2),
            'is_fresh': is_fresh
        }
    
    def check_date_ranges(
        self,
        df: DataFrame,
        date_column: str,
        expected_start: str,
        expected_end: str
    ) -> Dict:
        """
        Check that dates are within expected range
        
        Args:
            df: Input DataFrame
            date_column: Date column to check
            expected_start: Expected start date (YYYY-MM-DD)
            expected_end: Expected end date (YYYY-MM-DD)
            
        Returns:
            Dictionary with date range results
        """
        print(f"\n{'='*80}")
        print(f"TIMELINESS CHECK: Date Range Validation")
        print(f"Expected: {expected_start} to {expected_end}")
        print(f"{'='*80}")
        
        # Get actual date range
        stats = df.agg(
            F.min(date_column).alias('min_date'),
            F.max(date_column).alias('max_date')
        ).first()
        
        actual_start = stats.min_date
        actual_end = stats.max_date
        
        # Count out of range
        out_of_range = df.filter(
            (F.col(date_column) < F.lit(expected_start)) |
            (F.col(date_column) > F.lit(expected_end))
        ).count()
        
        total_rows = df.count()
        out_of_range_pct = (out_of_range / total_rows * 100) if total_rows > 0 else 0
        
        print(f"Actual range: {actual_start} to {actual_end}")
        print(f"Out of range: {out_of_range:,} ({out_of_range_pct:.2f}%)")
        
        status = QualityCheckStatus.FAILED if out_of_range > 0 else QualityCheckStatus.PASSED
        
        self.log_check(
            'date_ranges',
            status,
            {
                'expected_range': f'{expected_start} to {expected_end}',
                'actual_range': f'{actual_start} to {actual_end}',
                'out_of_range': out_of_range
            }
        )
        
        return {
            'status': status.value,
            'expected_start': expected_start,
            'expected_end': expected_end,
            'actual_start': str(actual_start),
            'actual_end': str(actual_end),
            'out_of_range': out_of_range,
            'out_of_range_percentage': round(out_of_range_pct, 2)
        }
    
    # ========================================================================
    # STATISTICAL PROFILING
    # ========================================================================
    
    def profile_numeric_columns(
        self,
        df: DataFrame,
        columns: List[str] = None,
        percentiles: List[float] = [0.25, 0.5, 0.75, 0.95, 0.99]
    ) -> Dict:
        """
        Generate statistical profile for numeric columns
        
        Args:
            df: Input DataFrame
            columns: List of numeric columns to profile
            percentiles: List of percentiles to calculate
            
        Returns:
            Dictionary with profiling results
        """
        print(f"\n{'='*80}")
        print(f"PROFILING: Numeric Column Statistics")
        print(f"{'='*80}")
        
        if columns is None:
            # Auto-detect numeric columns
            columns = [
                f.name for f in df.schema.fields 
                if isinstance(f.dataType, (IntegerType, LongType, FloatType, DoubleType, DecimalType))
            ]
        
        profiles = {}
        
        for col in columns:
            print(f"\n{col}:")
            
            # Calculate statistics
            stats = df.select(col).summary(*[str(int(p*100)) + '%' for p in percentiles])
            stats_dict = {row['summary']: float(row[col]) if row[col] else None 
                         for row in stats.collect()}
            
            # Additional stats
            detailed_stats = df.agg(
                F.count(col).alias('count'),
                F.countDistinct(col).alias('distinct'),
                F.min(col).alias('min'),
                F.max(col).alias('max'),
                F.mean(col).alias('mean'),
                F.stddev(col).alias('stddev'),
                F.sum(F.when(F.col(col).isNull(), 1).otherwise(0)).alias('nulls')
            ).first()
            
            profile = {
                'count': detailed_stats['count'],
                'distinct': detailed_stats['distinct'],
                'nulls': detailed_stats['nulls'],
                'min': detailed_stats['min'],
                'max': detailed_stats['max'],
                'mean': round(detailed_stats['mean'], 2) if detailed_stats['mean'] else None,
                'stddev': round(detailed_stats['stddev'], 2) if detailed_stats['stddev'] else None,
                'percentiles': stats_dict
            }
            
            profiles[col] = profile
            
            # Print summary
            for key, val in profile.items():
                if key != 'percentiles':
                    print(f"  {key:15s}: {val}")
        
        return profiles
    
    def profile_categorical_columns(
        self,
        df: DataFrame,
        columns: List[str],
        top_n: int = 10
    ) -> Dict:
        """
        Generate profile for categorical columns
        
        Args:
            df: Input DataFrame
            columns: List of categorical columns to profile
            top_n: Number of top values to show
            
        Returns:
            Dictionary with profiling results
        """
        print(f"\n{'='*80}")
        print(f"PROFILING: Categorical Column Analysis")
        print(f"{'='*80}")
        
        profiles = {}
        
        for col in columns:
            print(f"\n{col}:")
            
            # Get value counts
            value_counts = df.groupBy(col).count().orderBy(F.col('count').desc())
            
            total = df.count()
            distinct = value_counts.count()
            top_values = value_counts.limit(top_n).collect()
            
            profile = {
                'total': total,
                'distinct': distinct,
                'cardinality_ratio': round(distinct / total, 4) if total > 0 else 0,
                'top_values': [
                    {
                        'value': row[col],
                        'count': row['count'],
                        'percentage': round(row['count'] / total * 100, 2)
                    }
                    for row in top_values
                ]
            }
            
            profiles[col] = profile
            
            print(f"  Total: {total:,}")
            print(f"  Distinct: {distinct:,}")
            print(f"  Cardinality: {profile['cardinality_ratio']:.4f}")
            print(f"  Top {top_n} values:")
            for item in profile['top_values']:
                print(f"    {item['value']}: {item['count']:,} ({item['percentage']:.2f}%)")
        
        return profiles
    
    # ========================================================================
    # COMPREHENSIVE QUALITY SUITE
    # ========================================================================
    
    def run_comprehensive_checks(
        self,
        df: DataFrame,
        config: Dict
    ) -> Dict:
        """
        Run comprehensive suite of data quality checks
        
        Args:
            df: Input DataFrame
            config: Configuration dictionary with check parameters
            
        Returns:
            Dictionary with all check results
        """
        print(f"\n{'#'*80}")
        print(f"RUNNING COMPREHENSIVE DATA QUALITY CHECKS")
        print(f"{'#'*80}")
        
        start_time = datetime.now()
        results = {}
        
        # Completeness checks
        if 'null_checks' in config:
            results['null_checks'] = self.check_null_counts(
                df,
                config['null_checks'].get('columns'),
                config['null_checks'].get('threshold_pct', 5.0),
                config['null_checks'].get('sample_fraction', 1.0)
            )
        
        if 'required_fields' in config:
            results['required_fields'] = self.check_required_fields(
                df,
                config['required_fields']
            )
        
        # Uniqueness checks
        if 'duplicates' in config:
            results['duplicates'] = self.check_duplicates(
                df,
                config['duplicates']['key_columns'],
                config['duplicates'].get('sample_size', 10)
            )
        
        if 'primary_key' in config:
            results['primary_key'] = self.check_primary_key_uniqueness(
                df,
                config['primary_key']
            )
        
        # Validity checks
        if 'value_ranges' in config:
            results['value_ranges'] = self.check_value_ranges(
                df,
                config['value_ranges']
            )
        
        if 'valid_values' in config:
            results['valid_values'] = self.check_valid_values(
                df,
                config['valid_values']
            )
        
        if 'regex_patterns' in config:
            results['regex_patterns'] = self.check_regex_patterns(
                df,
                config['regex_patterns']
            )
        
        # Timeliness checks
        if 'freshness' in config:
            results['freshness'] = self.check_data_freshness(
                df,
                config['freshness']['timestamp_column'],
                config['freshness'].get('max_age_hours', 24)
            )
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        # Summary
        print(f"\n{'='*80}")
        print(f"QUALITY CHECK SUMMARY")
        print(f"{'='*80}")
        print(f"Duration: {duration:.2f} seconds")
        print(f"Total checks run: {len(results)}")
        
        passed = sum(1 for r in results.values() if r.get('status') == 'PASSED')
        failed = len(results) - passed
        
        print(f"Passed: {passed}")
        print(f"Failed: {failed}")
        print(f"Success rate: {passed/len(results)*100:.1f}%")
        
        return {
            'results': results,
            'summary': {
                'total_checks': len(results),
                'passed': passed,
                'failed': failed,
                'success_rate': round(passed/len(results)*100, 2),
                'duration_seconds': round(duration, 2)
            }
        }


# Usage will be in separate examples file
if __name__ == "__main__":
    print("Data Quality Framework loaded successfully")
    print("See data_quality_examples.py for usage examples")
