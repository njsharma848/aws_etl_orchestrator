"""
Schema Evolution Management with PySpark and Redshift
Handles schema changes between source DataFrames and Redshift tables
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import *
from typing import Dict, List, Tuple, Optional
from enum import Enum
import json
from datetime import datetime


class SchemaChangeType(Enum):
    """Types of schema changes"""
    ADD_COLUMN = "ADD_COLUMN"
    DROP_COLUMN = "DROP_COLUMN"
    MODIFY_TYPE = "MODIFY_TYPE"
    MODIFY_NULLABLE = "MODIFY_NULLABLE"
    NO_CHANGE = "NO_CHANGE"


class EvolutionStrategy(Enum):
    """Strategies for handling schema evolution"""
    STRICT = "STRICT"              # Fail on any schema change
    ADD_ONLY = "ADD_ONLY"          # Allow adding columns only
    FLEXIBLE = "FLEXIBLE"          # Allow add/modify (safe changes)
    PERMISSIVE = "PERMISSIVE"      # Allow all changes including drops


class SchemaEvolutionManager:
    """
    Manages schema evolution between source DataFrames and Redshift tables.
    Detects changes, validates compatibility, and applies migrations.
    """
    
    def __init__(self, spark: SparkSession, redshift_config: dict = None):
        """
        Initialize Schema Evolution Manager
        
        Args:
            spark: SparkSession instance
            redshift_config: Configuration for Redshift connection
                {
                    'host': 'cluster.region.redshift.amazonaws.com',
                    'database': 'mydb',
                    'user': 'username',
                    'password': 'password',
                    'port': 5439
                }
        """
        self.spark = spark
        self.redshift_config = redshift_config or {}
        
        # Type mapping between Spark and Redshift
        self.spark_to_redshift_types = {
            'IntegerType': 'INTEGER',
            'LongType': 'BIGINT',
            'ShortType': 'SMALLINT',
            'ByteType': 'SMALLINT',
            'FloatType': 'REAL',
            'DoubleType': 'DOUBLE PRECISION',
            'DecimalType': 'DECIMAL',
            'StringType': 'VARCHAR(65535)',
            'BooleanType': 'BOOLEAN',
            'TimestampType': 'TIMESTAMP',
            'DateType': 'DATE',
            'BinaryType': 'VARBINARY'
        }
        
        self.redshift_to_spark_types = {
            'INTEGER': IntegerType(),
            'INT': IntegerType(),
            'BIGINT': LongType(),
            'SMALLINT': ShortType(),
            'REAL': FloatType(),
            'FLOAT': FloatType(),
            'FLOAT4': FloatType(),
            'FLOAT8': DoubleType(),
            'DOUBLE PRECISION': DoubleType(),
            'DECIMAL': DecimalType(38, 18),
            'NUMERIC': DecimalType(38, 18),
            'VARCHAR': StringType(),
            'CHAR': StringType(),
            'TEXT': StringType(),
            'BOOLEAN': BooleanType(),
            'BOOL': BooleanType(),
            'TIMESTAMP': TimestampType(),
            'TIMESTAMPTZ': TimestampType(),
            'DATE': DateType()
        }
    
    def get_redshift_schema_via_jdbc(
        self,
        table_name: str,
        schema: str = 'public'
    ) -> Dict[str, dict]:
        """
        Get Redshift table schema using JDBC
        
        Args:
            table_name: Name of the Redshift table
            schema: Schema name (default: public)
            
        Returns:
            Dictionary mapping column names to their properties
        """
        try:
            # Build JDBC URL
            jdbc_url = (
                f"jdbc:redshift://{self.redshift_config['host']}:"
                f"{self.redshift_config.get('port', 5439)}/"
                f"{self.redshift_config['database']}"
            )
            
            # Query to get table schema
            query = f"""
                SELECT 
                    column_name,
                    data_type,
                    character_maximum_length,
                    numeric_precision,
                    numeric_scale,
                    is_nullable
                FROM information_schema.columns
                WHERE table_schema = '{schema}'
                AND table_name = '{table_name}'
                ORDER BY ordinal_position
            """
            
            # Read schema info
            schema_df = self.spark.read \
                .format("jdbc") \
                .option("url", jdbc_url) \
                .option("query", query) \
                .option("user", self.redshift_config['user']) \
                .option("password", self.redshift_config['password']) \
                .option("driver", "com.amazon.redshift.jdbc.Driver") \
                .load()
            
            # Convert to dictionary
            redshift_schema = {}
            for row in schema_df.collect():
                col_name = row['column_name']
                data_type = row['data_type'].upper()
                
                # Handle types with precision/scale
                if data_type in ['DECIMAL', 'NUMERIC'] and row['numeric_precision']:
                    data_type = f"{data_type}({row['numeric_precision']},{row['numeric_scale']})"
                elif data_type in ['VARCHAR', 'CHAR'] and row['character_maximum_length']:
                    data_type = f"{data_type}({row['character_maximum_length']})"
                
                redshift_schema[col_name] = {
                    'data_type': data_type,
                    'nullable': row['is_nullable'] == 'YES'
                }
            
            return redshift_schema
            
        except Exception as e:
            print(f"Error retrieving Redshift schema: {e}")
            return {}
    
    def get_redshift_schema_simulated(
        self,
        table_name: str
    ) -> Dict[str, dict]:
        """
        Simulated Redshift schema for testing without actual Redshift connection
        In production, use get_redshift_schema_via_jdbc() or Redshift Data API
        
        Args:
            table_name: Name of the table
            
        Returns:
            Dictionary mapping column names to their properties
        """
        # Simulated existing table schema
        simulated_schemas = {
            'customers': {
                'customer_id': {'data_type': 'INTEGER', 'nullable': False},
                'email': {'data_type': 'VARCHAR(255)', 'nullable': False},
                'name': {'data_type': 'VARCHAR(255)', 'nullable': True},
                'created_date': {'data_type': 'TIMESTAMP', 'nullable': True}
            },
            'orders': {
                'order_id': {'data_type': 'BIGINT', 'nullable': False},
                'customer_id': {'data_type': 'INTEGER', 'nullable': False},
                'order_date': {'data_type': 'TIMESTAMP', 'nullable': False},
                'total_amount': {'data_type': 'DECIMAL(10,2)', 'nullable': True},
                'status': {'data_type': 'VARCHAR(50)', 'nullable': True}
            }
        }
        
        return simulated_schemas.get(table_name, {})
    
    def get_dataframe_schema(self, df: DataFrame) -> Dict[str, dict]:
        """
        Extract schema from PySpark DataFrame
        
        Args:
            df: Source DataFrame
            
        Returns:
            Dictionary mapping column names to their properties
        """
        df_schema = {}
        
        for field in df.schema.fields:
            spark_type = type(field.dataType).__name__
            
            df_schema[field.name] = {
                'data_type': spark_type,
                'nullable': field.nullable,
                'spark_type': field.dataType
            }
        
        return df_schema
    
    def compare_schemas(
        self,
        source_df: DataFrame,
        target_table: str,
        use_simulated: bool = True
    ) -> Dict[str, List]:
        """
        Compare source DataFrame schema with Redshift table schema
        
        Args:
            source_df: Source DataFrame
            target_table: Redshift table name
            use_simulated: Use simulated schema (for testing)
            
        Returns:
            Dictionary containing schema differences:
            {
                'added_columns': [...],
                'dropped_columns': [...],
                'modified_columns': [...],
                'type_changes': [...],
                'nullable_changes': [...]
            }
        """
        print(f"\n{'='*80}")
        print(f"SCHEMA COMPARISON: Source DataFrame vs Redshift Table '{target_table}'")
        print(f"{'='*80}")
        
        # Get schemas
        source_schema = self.get_dataframe_schema(source_df)
        
        if use_simulated:
            target_schema = self.get_redshift_schema_simulated(target_table)
        else:
            target_schema = self.get_redshift_schema_via_jdbc(target_table)
        
        # Find differences
        source_cols = set(source_schema.keys())
        target_cols = set(target_schema.keys())
        
        added_columns = list(source_cols - target_cols)
        dropped_columns = list(target_cols - source_cols)
        common_columns = source_cols & target_cols
        
        type_changes = []
        nullable_changes = []
        
        for col in common_columns:
            source_info = source_schema[col]
            target_info = target_schema[col]
            
            # Check type compatibility
            source_type = source_info['data_type']
            target_type = target_info['data_type'].split('(')[0]  # Remove precision/scale
            
            # Map Spark type to expected Redshift type
            expected_redshift_type = self.spark_to_redshift_types.get(source_type, 'VARCHAR(65535)')
            expected_redshift_type = expected_redshift_type.split('(')[0]
            
            if expected_redshift_type != target_type:
                type_changes.append({
                    'column': col,
                    'source_type': source_type,
                    'target_type': target_type,
                    'compatible': self._is_type_compatible(source_type, target_type)
                })
            
            # Check nullable changes
            if source_info['nullable'] != target_info['nullable']:
                nullable_changes.append({
                    'column': col,
                    'source_nullable': source_info['nullable'],
                    'target_nullable': target_info['nullable']
                })
        
        results = {
            'added_columns': added_columns,
            'dropped_columns': dropped_columns,
            'type_changes': type_changes,
            'nullable_changes': nullable_changes,
            'common_columns': list(common_columns)
        }
        
        # Print summary
        self._print_schema_comparison(results)
        
        return results
    
    def _is_type_compatible(self, source_type: str, target_type: str) -> bool:
        """Check if types are compatible for safe evolution"""
        compatible_mappings = {
            'IntegerType': ['INTEGER', 'BIGINT', 'DECIMAL', 'NUMERIC'],
            'LongType': ['BIGINT', 'DECIMAL', 'NUMERIC'],
            'FloatType': ['REAL', 'FLOAT', 'DOUBLE PRECISION', 'DECIMAL', 'NUMERIC'],
            'DoubleType': ['DOUBLE PRECISION', 'DECIMAL', 'NUMERIC'],
            'StringType': ['VARCHAR', 'CHAR', 'TEXT'],
            'BooleanType': ['BOOLEAN', 'BOOL'],
            'TimestampType': ['TIMESTAMP', 'TIMESTAMPTZ'],
            'DateType': ['DATE']
        }
        
        compatible_targets = compatible_mappings.get(source_type, [])
        return target_type in compatible_targets
    
    def _print_schema_comparison(self, results: Dict):
        """Print schema comparison results"""
        print(f"\n--- SCHEMA COMPARISON RESULTS ---\n")
        
        if results['added_columns']:
            print(f"✓ Added Columns ({len(results['added_columns'])}):")
            for col in results['added_columns']:
                print(f"  + {col}")
        
        if results['dropped_columns']:
            print(f"\n⚠ Dropped Columns ({len(results['dropped_columns'])}):")
            for col in results['dropped_columns']:
                print(f"  - {col}")
        
        if results['type_changes']:
            print(f"\n⚡ Type Changes ({len(results['type_changes'])}):")
            for change in results['type_changes']:
                compat = "✓" if change['compatible'] else "✗"
                print(f"  {compat} {change['column']}: {change['source_type']} -> {change['target_type']}")
        
        if results['nullable_changes']:
            print(f"\n◆ Nullable Changes ({len(results['nullable_changes'])}):")
            for change in results['nullable_changes']:
                print(f"  • {change['column']}: nullable={change['source_nullable']} -> {change['target_nullable']}")
        
        if not any([results['added_columns'], results['dropped_columns'], 
                   results['type_changes'], results['nullable_changes']]):
            print("✓ No schema changes detected - schemas match perfectly!")
    
    def generate_migration_ddl(
        self,
        target_table: str,
        schema_diff: Dict,
        source_df: DataFrame,
        strategy: EvolutionStrategy = EvolutionStrategy.FLEXIBLE
    ) -> List[str]:
        """
        Generate DDL statements to migrate Redshift table schema
        
        Args:
            target_table: Redshift table name
            schema_diff: Schema differences from compare_schemas()
            source_df: Source DataFrame
            strategy: Evolution strategy
            
        Returns:
            List of DDL statements to execute
        """
        print(f"\n{'='*80}")
        print(f"GENERATING MIGRATION DDL (Strategy: {strategy.value})")
        print(f"{'='*80}\n")
        
        ddl_statements = []
        source_schema = self.get_dataframe_schema(source_df)
        
        # Handle added columns
        if schema_diff['added_columns']:
            if strategy in [EvolutionStrategy.ADD_ONLY, EvolutionStrategy.FLEXIBLE, 
                          EvolutionStrategy.PERMISSIVE]:
                for col in schema_diff['added_columns']:
                    col_info = source_schema[col]
                    spark_type = col_info['data_type']
                    redshift_type = self.spark_to_redshift_types.get(spark_type, 'VARCHAR(65535)')
                    nullable = 'NULL' if col_info['nullable'] else 'NOT NULL'
                    
                    ddl = f"ALTER TABLE {target_table} ADD COLUMN {col} {redshift_type} {nullable};"
                    ddl_statements.append(ddl)
            else:
                print(f"⚠ Strategy {strategy.value} does not allow adding columns")
        
        # Handle dropped columns
        if schema_diff['dropped_columns']:
            if strategy == EvolutionStrategy.PERMISSIVE:
                for col in schema_diff['dropped_columns']:
                    ddl = f"ALTER TABLE {target_table} DROP COLUMN {col};"
                    ddl_statements.append(ddl)
            else:
                print(f"⚠ Strategy {strategy.value} does not allow dropping columns")
        
        # Handle type changes
        if schema_diff['type_changes']:
            if strategy in [EvolutionStrategy.FLEXIBLE, EvolutionStrategy.PERMISSIVE]:
                for change in schema_diff['type_changes']:
                    if change['compatible']:
                        col = change['column']
                        source_type = change['source_type']
                        redshift_type = self.spark_to_redshift_types.get(source_type, 'VARCHAR(65535)')
                        
                        # Redshift doesn't support ALTER COLUMN TYPE directly
                        # Need to create new column, copy data, drop old, rename new
                        temp_col = f"{col}_new"
                        ddl_statements.extend([
                            f"ALTER TABLE {target_table} ADD COLUMN {temp_col} {redshift_type};",
                            f"UPDATE {target_table} SET {temp_col} = {col};",
                            f"ALTER TABLE {target_table} DROP COLUMN {col};",
                            f"ALTER TABLE {target_table} RENAME COLUMN {temp_col} TO {col};"
                        ])
                    else:
                        print(f"⚠ Incompatible type change for {change['column']}, skipping")
        
        # Print generated DDL
        if ddl_statements:
            print("Generated DDL Statements:")
            for i, ddl in enumerate(ddl_statements, 1):
                print(f"{i}. {ddl}")
        else:
            print("No DDL statements needed")
        
        return ddl_statements
    
    def validate_evolution(
        self,
        source_df: DataFrame,
        target_table: str,
        strategy: EvolutionStrategy = EvolutionStrategy.FLEXIBLE,
        use_simulated: bool = True
    ) -> Tuple[bool, str]:
        """
        Validate if schema evolution is allowed under given strategy
        
        Args:
            source_df: Source DataFrame
            target_table: Redshift table name
            strategy: Evolution strategy
            use_simulated: Use simulated schema
            
        Returns:
            Tuple of (is_valid, message)
        """
        schema_diff = self.compare_schemas(source_df, target_table, use_simulated)
        
        if strategy == EvolutionStrategy.STRICT:
            if any([schema_diff['added_columns'], schema_diff['dropped_columns'],
                   schema_diff['type_changes'], schema_diff['nullable_changes']]):
                return False, "STRICT mode: No schema changes allowed"
            return True, "Schema matches exactly"
        
        elif strategy == EvolutionStrategy.ADD_ONLY:
            if schema_diff['dropped_columns'] or schema_diff['type_changes']:
                return False, "ADD_ONLY mode: Cannot drop columns or change types"
            return True, "Only adding columns is allowed"
        
        elif strategy == EvolutionStrategy.FLEXIBLE:
            if schema_diff['dropped_columns']:
                return False, "FLEXIBLE mode: Cannot drop columns"
            
            # Check for incompatible type changes
            incompatible_changes = [
                c for c in schema_diff['type_changes'] 
                if not c['compatible']
            ]
            if incompatible_changes:
                return False, f"FLEXIBLE mode: Incompatible type changes found: {incompatible_changes}"
            
            return True, "Safe schema evolution allowed"
        
        else:  # PERMISSIVE
            return True, "All schema changes allowed"
    
    def apply_schema_evolution(
        self,
        source_df: DataFrame,
        target_table: str,
        strategy: EvolutionStrategy = EvolutionStrategy.FLEXIBLE,
        dry_run: bool = True,
        use_simulated: bool = True
    ) -> Dict:
        """
        Apply schema evolution to Redshift table
        
        Args:
            source_df: Source DataFrame
            target_table: Redshift table name
            strategy: Evolution strategy
            dry_run: If True, only generate DDL without executing
            use_simulated: Use simulated schema
            
        Returns:
            Dictionary with execution results
        """
        print(f"\n{'='*80}")
        print(f"APPLYING SCHEMA EVOLUTION")
        print(f"Table: {target_table}")
        print(f"Strategy: {strategy.value}")
        print(f"Dry Run: {dry_run}")
        print(f"{'='*80}")
        
        # Validate evolution
        is_valid, message = self.validate_evolution(source_df, target_table, strategy, use_simulated)
        
        if not is_valid:
            return {
                'success': False,
                'message': message,
                'ddl_statements': []
            }
        
        # Get schema differences
        schema_diff = self.compare_schemas(source_df, target_table, use_simulated)
        
        # Generate DDL
        ddl_statements = self.generate_migration_ddl(
            target_table,
            schema_diff,
            source_df,
            strategy
        )
        
        results = {
            'success': True,
            'message': message,
            'ddl_statements': ddl_statements,
            'schema_changes': schema_diff
        }
        
        if not dry_run and ddl_statements:
            # Execute DDL statements
            # In production, execute these via JDBC or Redshift Data API
            print("\n⚠ DRY RUN MODE - DDL not executed")
            print("In production, execute DDL via:")
            print("  - JDBC connection")
            print("  - Redshift Data API")
            print("  - AWS SDK boto3.client('redshift-data')")
        
        return results
    
    def align_dataframe_to_target(
        self,
        source_df: DataFrame,
        target_table: str,
        use_simulated: bool = True
    ) -> DataFrame:
        """
        Align source DataFrame columns to match target table schema
        Adds missing columns with nulls, drops extra columns, reorders
        
        Args:
            source_df: Source DataFrame
            target_table: Redshift table name
            use_simulated: Use simulated schema
            
        Returns:
            Aligned DataFrame
        """
        print(f"\nAligning DataFrame to target table: {target_table}")
        
        # Get target schema
        if use_simulated:
            target_schema = self.get_redshift_schema_simulated(target_table)
        else:
            target_schema = self.get_redshift_schema_via_jdbc(target_table)
        
        # Get source columns
        source_cols = set(source_df.columns)
        target_cols = set(target_schema.keys())
        
        aligned_df = source_df
        
        # Add missing columns with nulls
        missing_cols = target_cols - source_cols
        for col in missing_cols:
            target_type = target_schema[col]['data_type'].split('(')[0]
            spark_type = self.redshift_to_spark_types.get(target_type, StringType())
            aligned_df = aligned_df.withColumn(col, F.lit(None).cast(spark_type))
            print(f"  + Added column: {col} (NULL)")
        
        # Drop extra columns
        extra_cols = source_cols - target_cols
        for col in extra_cols:
            aligned_df = aligned_df.drop(col)
            print(f"  - Dropped column: {col}")
        
        # Reorder columns to match target
        aligned_df = aligned_df.select(*target_schema.keys())
        
        print(f"✓ DataFrame aligned: {len(target_cols)} columns")
        
        return aligned_df


# Example usage
def example_schema_evolution():
    """Demonstrate schema evolution scenarios"""
    
    spark = SparkSession.builder \
        .appName("SchemaEvolution") \
        .getOrCreate()
    
    print("\n" + "="*80)
    print("SCHEMA EVOLUTION EXAMPLES")
    print("="*80)
    
    # Initialize manager
    manager = SchemaEvolutionManager(spark)
    
    # Scenario 1: New columns added
    print("\n\n" + "#"*80)
    print("SCENARIO 1: Source has NEW columns")
    print("#"*80)
    
    customers_v2 = spark.createDataFrame([
        (1, 'john@email.com', 'John Doe', 'Gold', '2024-01-01 10:00:00', '555-1234'),
        (2, 'jane@email.com', 'Jane Smith', 'Platinum', '2024-01-02 11:00:00', '555-5678')
    ], ['customer_id', 'email', 'name', 'tier', 'created_date', 'phone'])
    
    customers_v2 = customers_v2.withColumn('created_date', F.to_timestamp('created_date'))
    
    # Compare schemas
    diff = manager.compare_schemas(customers_v2, 'customers', use_simulated=True)
    
    # Apply evolution with FLEXIBLE strategy
    result = manager.apply_schema_evolution(
        customers_v2,
        'customers',
        strategy=EvolutionStrategy.FLEXIBLE,
        dry_run=True
    )
    
    # Scenario 2: Columns dropped
    print("\n\n" + "#"*80)
    print("SCENARIO 2: Source has FEWER columns")
    print("#"*80)
    
    customers_v3 = spark.createDataFrame([
        (1, 'john@email.com', 'John Doe'),
        (2, 'jane@email.com', 'Jane Smith')
    ], ['customer_id', 'email', 'name'])
    
    diff = manager.compare_schemas(customers_v3, 'customers', use_simulated=True)
    
    # This should fail with FLEXIBLE strategy
    result = manager.apply_schema_evolution(
        customers_v3,
        'customers',
        strategy=EvolutionStrategy.FLEXIBLE,
        dry_run=True
    )
    
    # Scenario 3: Align DataFrame to existing target
    print("\n\n" + "#"*80)
    print("SCENARIO 3: Align DataFrame to target schema")
    print("#"*80)
    
    aligned_df = manager.align_dataframe_to_target(
        customers_v3,
        'customers',
        use_simulated=True
    )
    
    print("\nAligned DataFrame schema:")
    aligned_df.printSchema()
    aligned_df.show(truncate=False)
    
    spark.stop()


if __name__ == "__main__":
    example_schema_evolution()
