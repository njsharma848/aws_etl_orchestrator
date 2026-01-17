"""
AWS Glue Catalog Schema Evolution with Versioning
Handles schema evolution using Glue Catalog metadata and version tracking
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import *
from typing import Dict, List, Tuple, Optional
from datetime import datetime
from enum import Enum
import json
import boto3
from botocore.exceptions import ClientError


class SchemaVersionManager:
    """
    Manages schema versions and tracks schema evolution over time.
    Stores schema history and enables version comparison.
    """
    
    def __init__(self, spark: SparkSession, version_storage_path: str):
        """
        Initialize Schema Version Manager
        
        Args:
            spark: SparkSession instance
            version_storage_path: S3/local path to store version history
        """
        self.spark = spark
        self.version_storage_path = version_storage_path
    
    def register_schema_version(
        self,
        table_name: str,
        schema: StructType,
        version: str = None,
        metadata: Dict = None
    ) -> Dict:
        """
        Register a new schema version
        
        Args:
            table_name: Name of the table
            schema: PySpark StructType schema
            version: Version identifier (auto-generated if None)
            metadata: Additional metadata (author, description, etc.)
            
        Returns:
            Dictionary with version information
        """
        # Auto-generate version if not provided
        if version is None:
            version = datetime.now().strftime('v%Y%m%d_%H%M%S')
        
        # Convert schema to JSON
        schema_json = schema.json()
        
        # Create version record
        version_record = {
            'table_name': table_name,
            'version': version,
            'schema': schema_json,
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'metadata': metadata or {}
        }
        
        # Save to version storage
        version_path = f"{self.version_storage_path}/{table_name}/versions/{version}.json"
        
        # Convert to DataFrame and save
        version_df = self.spark.createDataFrame([
            (
                version_record['table_name'],
                version_record['version'],
                version_record['schema'],
                version_record['timestamp'],
                json.dumps(version_record['metadata'])
            )
        ], ['table_name', 'version', 'schema', 'timestamp', 'metadata'])
        
        version_df.write.mode('overwrite').json(version_path)
        
        print(f"✓ Registered schema version: {table_name} - {version}")
        
        return version_record
    
    def get_schema_version(
        self,
        table_name: str,
        version: str = None
    ) -> Optional[StructType]:
        """
        Retrieve a specific schema version
        
        Args:
            table_name: Name of the table
            version: Version identifier (latest if None)
            
        Returns:
            StructType schema or None if not found
        """
        try:
            if version:
                version_path = f"{self.version_storage_path}/{table_name}/versions/{version}.json"
                version_df = self.spark.read.json(version_path)
            else:
                # Get latest version
                versions_path = f"{self.version_storage_path}/{table_name}/versions/"
                version_df = self.spark.read.json(versions_path)
                version_df = version_df.orderBy(F.col('timestamp').desc()).limit(1)
            
            if version_df.count() == 0:
                return None
            
            schema_json = version_df.first()['schema']
            return StructType.fromJson(json.loads(schema_json))
            
        except Exception as e:
            print(f"Error retrieving schema version: {e}")
            return None
    
    def list_schema_versions(self, table_name: str) -> List[Dict]:
        """
        List all versions for a table
        
        Args:
            table_name: Name of the table
            
        Returns:
            List of version records
        """
        try:
            versions_path = f"{self.version_storage_path}/{table_name}/versions/"
            versions_df = self.spark.read.json(versions_path)
            
            versions = versions_df.orderBy(F.col('timestamp').desc()).collect()
            
            return [
                {
                    'version': row['version'],
                    'timestamp': row['timestamp'],
                    'metadata': json.loads(row['metadata']) if row['metadata'] else {}
                }
                for row in versions
            ]
        except Exception as e:
            print(f"No versions found for {table_name}: {e}")
            return []
    
    def compare_versions(
        self,
        table_name: str,
        from_version: str,
        to_version: str
    ) -> Dict:
        """
        Compare two schema versions
        
        Args:
            table_name: Name of the table
            from_version: Starting version
            to_version: Target version
            
        Returns:
            Dictionary with schema differences
        """
        print(f"\n{'='*80}")
        print(f"COMPARING SCHEMA VERSIONS: {from_version} → {to_version}")
        print(f"{'='*80}")
        
        from_schema = self.get_schema_version(table_name, from_version)
        to_schema = self.get_schema_version(table_name, to_version)
        
        if not from_schema or not to_schema:
            return {'error': 'Version not found'}
        
        # Extract field information
        from_fields = {f.name: f for f in from_schema.fields}
        to_fields = {f.name: f for f in to_schema.fields}
        
        # Find differences
        added = set(to_fields.keys()) - set(from_fields.keys())
        removed = set(from_fields.keys()) - set(to_fields.keys())
        common = set(from_fields.keys()) & set(to_fields.keys())
        
        type_changes = []
        nullable_changes = []
        
        for field_name in common:
            from_field = from_fields[field_name]
            to_field = to_fields[field_name]
            
            # Check type changes
            if str(from_field.dataType) != str(to_field.dataType):
                type_changes.append({
                    'field': field_name,
                    'from_type': str(from_field.dataType),
                    'to_type': str(to_field.dataType)
                })
            
            # Check nullable changes
            if from_field.nullable != to_field.nullable:
                nullable_changes.append({
                    'field': field_name,
                    'from_nullable': from_field.nullable,
                    'to_nullable': to_field.nullable
                })
        
        # Print summary
        print(f"\n✓ Added fields: {list(added) if added else 'None'}")
        print(f"✗ Removed fields: {list(removed) if removed else 'None'}")
        print(f"⚡ Type changes: {len(type_changes)}")
        print(f"◆ Nullable changes: {len(nullable_changes)}")
        
        return {
            'added_fields': list(added),
            'removed_fields': list(removed),
            'type_changes': type_changes,
            'nullable_changes': nullable_changes
        }


class GlueCatalogSchemaManager:
    """
    Manages schema evolution using AWS Glue Catalog.
    Integrates with Glue Catalog for schema metadata.
    """
    
    def __init__(
        self,
        spark: SparkSession,
        aws_region: str = 'us-east-1',
        glue_database: str = 'default'
    ):
        """
        Initialize Glue Catalog Schema Manager
        
        Args:
            spark: SparkSession instance
            aws_region: AWS region for Glue Catalog
            glue_database: Glue database name
        """
        self.spark = spark
        self.aws_region = aws_region
        self.glue_database = glue_database
        self.glue_client = boto3.client('glue', region_name=aws_region)
        
        # Type mapping between Glue and Spark
        self.glue_to_spark_types = {
            'string': StringType(),
            'int': IntegerType(),
            'bigint': LongType(),
            'smallint': ShortType(),
            'tinyint': ByteType(),
            'double': DoubleType(),
            'float': FloatType(),
            'decimal': DecimalType(38, 18),
            'boolean': BooleanType(),
            'date': DateType(),
            'timestamp': TimestampType(),
            'binary': BinaryType(),
            'array': ArrayType(StringType()),
            'map': MapType(StringType(), StringType()),
            'struct': StructType([])
        }
        
        self.spark_to_glue_types = {
            'StringType': 'string',
            'IntegerType': 'int',
            'LongType': 'bigint',
            'ShortType': 'smallint',
            'ByteType': 'tinyint',
            'DoubleType': 'double',
            'FloatType': 'float',
            'DecimalType': 'decimal',
            'BooleanType': 'boolean',
            'DateType': 'date',
            'TimestampType': 'timestamp',
            'BinaryType': 'binary'
        }
    
    def get_glue_table_schema(self, table_name: str) -> Dict:
        """
        Get schema from Glue Catalog
        
        Args:
            table_name: Name of the Glue table
            
        Returns:
            Dictionary with schema information
        """
        try:
            response = self.glue_client.get_table(
                DatabaseName=self.glue_database,
                Name=table_name
            )
            
            table = response['Table']
            storage_descriptor = table['StorageDescriptor']
            columns = storage_descriptor['Columns']
            
            # Add partition columns if they exist
            if 'PartitionKeys' in table:
                columns.extend(table['PartitionKeys'])
            
            schema_dict = {}
            for col in columns:
                schema_dict[col['Name']] = {
                    'type': col['Type'],
                    'comment': col.get('Comment', '')
                }
            
            return {
                'table_name': table_name,
                'database': self.glue_database,
                'columns': schema_dict,
                'location': storage_descriptor.get('Location', ''),
                'input_format': storage_descriptor.get('InputFormat', ''),
                'output_format': storage_descriptor.get('OutputFormat', ''),
                'parameters': table.get('Parameters', {})
            }
            
        except ClientError as e:
            print(f"Error retrieving Glue table schema: {e}")
            return {}
    
    def glue_schema_to_spark(self, glue_schema: Dict) -> StructType:
        """
        Convert Glue schema to Spark StructType
        
        Args:
            glue_schema: Glue schema dictionary
            
        Returns:
            Spark StructType
        """
        fields = []
        
        for col_name, col_info in glue_schema['columns'].items():
            glue_type = col_info['type'].lower()
            
            # Handle complex types with parameters
            if '(' in glue_type:
                base_type = glue_type.split('(')[0]
                spark_type = self.glue_to_spark_types.get(base_type, StringType())
            else:
                spark_type = self.glue_to_spark_types.get(glue_type, StringType())
            
            fields.append(StructField(col_name, spark_type, True))
        
        return StructType(fields)
    
    def compare_with_glue_catalog(
        self,
        source_df: DataFrame,
        glue_table_name: str
    ) -> Dict:
        """
        Compare source DataFrame schema with Glue Catalog table
        
        Args:
            source_df: Source DataFrame
            glue_table_name: Name of Glue Catalog table
            
        Returns:
            Dictionary with schema differences
        """
        print(f"\n{'='*80}")
        print(f"COMPARING WITH GLUE CATALOG: {glue_table_name}")
        print(f"{'='*80}")
        
        # Get Glue schema
        glue_schema = self.get_glue_table_schema(glue_table_name)
        
        if not glue_schema:
            return {'error': 'Glue table not found'}
        
        # Get source schema
        source_schema = source_df.schema
        source_fields = {f.name: f for f in source_schema.fields}
        glue_fields = glue_schema['columns']
        
        # Find differences
        added = set(source_fields.keys()) - set(glue_fields.keys())
        removed = set(glue_fields.keys()) - set(source_fields.keys())
        common = set(source_fields.keys()) & set(glue_fields.keys())
        
        type_changes = []
        
        for field_name in common:
            source_field = source_fields[field_name]
            glue_type = glue_fields[field_name]['type']
            
            source_type = type(source_field.dataType).__name__
            expected_glue_type = self.spark_to_glue_types.get(source_type, 'string')
            
            # Normalize types for comparison
            glue_type_normalized = glue_type.split('(')[0].lower()
            
            if expected_glue_type != glue_type_normalized:
                type_changes.append({
                    'column': field_name,
                    'source_type': source_type,
                    'glue_type': glue_type,
                    'compatible': self._is_compatible(source_type, glue_type_normalized)
                })
        
        # Print summary
        print(f"\n--- SCHEMA COMPARISON ---")
        print(f"Source DataFrame columns: {len(source_fields)}")
        print(f"Glue Catalog columns: {len(glue_fields)}")
        
        if added:
            print(f"\n✓ New columns in source ({len(added)}):")
            for col in added:
                print(f"  + {col}: {source_fields[col].dataType}")
        
        if removed:
            print(f"\n✗ Columns in Glue but not in source ({len(removed)}):")
            for col in removed:
                print(f"  - {col}: {glue_fields[col]['type']}")
        
        if type_changes:
            print(f"\n⚡ Type mismatches ({len(type_changes)}):")
            for change in type_changes:
                compat = "✓" if change['compatible'] else "✗"
                print(f"  {compat} {change['column']}: {change['source_type']} vs {change['glue_type']}")
        
        if not added and not removed and not type_changes:
            print("\n✓ Schemas match perfectly!")
        
        return {
            'added_columns': list(added),
            'removed_columns': list(removed),
            'type_changes': type_changes,
            'glue_location': glue_schema.get('location', ''),
            'glue_parameters': glue_schema.get('parameters', {})
        }
    
    def _is_compatible(self, spark_type: str, glue_type: str) -> bool:
        """Check if types are compatible"""
        compatible_mappings = {
            'IntegerType': ['int', 'bigint', 'decimal'],
            'LongType': ['bigint', 'decimal'],
            'FloatType': ['float', 'double', 'decimal'],
            'DoubleType': ['double', 'decimal'],
            'StringType': ['string'],
            'BooleanType': ['boolean'],
            'TimestampType': ['timestamp'],
            'DateType': ['date']
        }
        
        compatible = compatible_mappings.get(spark_type, [])
        return glue_type in compatible
    
    def update_glue_catalog(
        self,
        table_name: str,
        schema: StructType,
        location: str,
        partition_keys: List[str] = None,
        table_type: str = 'EXTERNAL_TABLE',
        input_format: str = 'org.apache.hadoop.mapred.TextInputFormat',
        output_format: str = 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
        serde_info: Dict = None
    ) -> bool:
        """
        Update Glue Catalog table schema
        
        Args:
            table_name: Name of the table
            schema: Spark StructType schema
            location: S3 location of data
            partition_keys: List of partition column names
            table_type: EXTERNAL_TABLE or MANAGED_TABLE
            input_format: Input format class
            output_format: Output format class
            serde_info: SerDe information
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Convert Spark schema to Glue columns
            columns = []
            partition_cols = partition_keys or []
            
            for field in schema.fields:
                if field.name not in partition_cols:
                    spark_type = type(field.dataType).__name__
                    glue_type = self.spark_to_glue_types.get(spark_type, 'string')
                    
                    columns.append({
                        'Name': field.name,
                        'Type': glue_type,
                        'Comment': ''
                    })
            
            # Build partition keys
            partition_keys_list = []
            for pk in partition_cols:
                field = next((f for f in schema.fields if f.name == pk), None)
                if field:
                    spark_type = type(field.dataType).__name__
                    glue_type = self.spark_to_glue_types.get(spark_type, 'string')
                    partition_keys_list.append({
                        'Name': pk,
                        'Type': glue_type
                    })
            
            # Default SerDe info for Parquet
            if serde_info is None:
                serde_info = {
                    'SerializationLibrary': 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe',
                    'Parameters': {
                        'serialization.format': '1'
                    }
                }
            
            # Build table input
            table_input = {
                'Name': table_name,
                'StorageDescriptor': {
                    'Columns': columns,
                    'Location': location,
                    'InputFormat': input_format,
                    'OutputFormat': output_format,
                    'SerdeInfo': serde_info,
                    'Compressed': False
                },
                'PartitionKeys': partition_keys_list,
                'TableType': table_type,
                'Parameters': {
                    'classification': 'parquet',
                    'compressionType': 'none',
                    'typeOfData': 'file'
                }
            }
            
            # Try to update existing table, create if doesn't exist
            try:
                self.glue_client.update_table(
                    DatabaseName=self.glue_database,
                    TableInput=table_input
                )
                print(f"✓ Updated Glue Catalog table: {table_name}")
            except ClientError as e:
                if e.response['Error']['Code'] == 'EntityNotFoundException':
                    self.glue_client.create_table(
                        DatabaseName=self.glue_database,
                        TableInput=table_input
                    )
                    print(f"✓ Created Glue Catalog table: {table_name}")
                else:
                    raise
            
            return True
            
        except Exception as e:
            print(f"Error updating Glue Catalog: {e}")
            return False
    
    def get_table_versions(self, table_name: str) -> List[Dict]:
        """
        Get version history from Glue Catalog
        
        Args:
            table_name: Name of the table
            
        Returns:
            List of table versions
        """
        try:
            response = self.glue_client.get_table_versions(
                DatabaseName=self.glue_database,
                TableName=table_name,
                MaxResults=100
            )
            
            versions = []
            for version in response.get('TableVersions', []):
                table = version['Table']
                versions.append({
                    'version_id': version.get('VersionId', ''),
                    'update_time': table.get('UpdateTime', ''),
                    'columns': len(table['StorageDescriptor']['Columns']),
                    'location': table['StorageDescriptor'].get('Location', '')
                })
            
            return versions
            
        except ClientError as e:
            print(f"Error retrieving table versions: {e}")
            return []


class IntegratedSchemaEvolution:
    """
    Integrated schema evolution combining Glue Catalog and version management
    """
    
    def __init__(
        self,
        spark: SparkSession,
        version_storage_path: str,
        aws_region: str = 'us-east-1',
        glue_database: str = 'default'
    ):
        """
        Initialize Integrated Schema Evolution
        
        Args:
            spark: SparkSession
            version_storage_path: Path for version storage
            aws_region: AWS region
            glue_database: Glue database name
        """
        self.version_manager = SchemaVersionManager(spark, version_storage_path)
        self.glue_manager = GlueCatalogSchemaManager(spark, aws_region, glue_database)
        self.spark = spark
    
    def evolve_schema_with_versioning(
        self,
        source_df: DataFrame,
        table_name: str,
        glue_table_name: str = None,
        auto_update_glue: bool = False,
        metadata: Dict = None
    ) -> Dict:
        """
        Complete schema evolution workflow with versioning
        
        Args:
            source_df: Source DataFrame
            table_name: Logical table name for versioning
            glue_table_name: Glue Catalog table name (None = same as table_name)
            auto_update_glue: Automatically update Glue Catalog
            metadata: Version metadata
            
        Returns:
            Dictionary with evolution results
        """
        glue_table_name = glue_table_name or table_name
        
        print(f"\n{'#'*80}")
        print(f"INTEGRATED SCHEMA EVOLUTION: {table_name}")
        print(f"{'#'*80}")
        
        results = {}
        
        # Step 1: Compare with Glue Catalog
        print(f"\n--- STEP 1: Compare with Glue Catalog ---")
        glue_diff = self.glue_manager.compare_with_glue_catalog(source_df, glue_table_name)
        results['glue_comparison'] = glue_diff
        
        # Step 2: Get latest version
        print(f"\n--- STEP 2: Check Version History ---")
        latest_version = self.version_manager.get_schema_version(table_name)
        
        if latest_version:
            print(f"Found existing schema version")
            
            # Register new version if schema changed
            if source_df.schema != latest_version:
                new_version = self.version_manager.register_schema_version(
                    table_name,
                    source_df.schema,
                    metadata=metadata
                )
                results['new_version'] = new_version
                print(f"Registered new version: {new_version['version']}")
            else:
                print("Schema unchanged - no new version needed")
                results['new_version'] = None
        else:
            print(f"No existing version - registering initial version")
            initial_version = self.version_manager.register_schema_version(
                table_name,
                source_df.schema,
                version='v1.0.0',
                metadata=metadata
            )
            results['new_version'] = initial_version
        
        # Step 3: Update Glue Catalog if requested
        if auto_update_glue and glue_diff.get('added_columns'):
            print(f"\n--- STEP 3: Update Glue Catalog ---")
            # Get location from Glue or use default
            location = glue_diff.get('glue_location', f's3://bucket/warehouse/{table_name}/')
            
            success = self.glue_manager.update_glue_catalog(
                glue_table_name,
                source_df.schema,
                location
            )
            results['glue_updated'] = success
        
        # Step 4: List all versions
        versions = self.version_manager.list_schema_versions(table_name)
        results['version_history'] = versions
        
        print(f"\n{'='*80}")
        print(f"EVOLUTION SUMMARY")
        print(f"{'='*80}")
        print(f"Glue changes detected: {bool(glue_diff.get('added_columns') or glue_diff.get('type_changes'))}")
        print(f"New version created: {bool(results.get('new_version'))}")
        print(f"Total versions: {len(versions)}")
        
        return results


# Example usage
def example_glue_versioning():
    """Demonstrate Glue Catalog integration with versioning"""
    
    spark = SparkSession.builder \
        .appName("GlueVersioning") \
        .getOrCreate()
    
    print("="*80)
    print("GLUE CATALOG SCHEMA EVOLUTION WITH VERSIONING")
    print("="*80)
    
    # Initialize integrated manager
    manager = IntegratedSchemaEvolution(
        spark,
        version_storage_path='/tmp/schema_versions',
        aws_region='us-east-1',
        glue_database='my_database'
    )
    
    # Create sample DataFrames (Version 1)
    df_v1 = spark.createDataFrame([
        (1, 'Alice', 'alice@email.com'),
        (2, 'Bob', 'bob@email.com')
    ], ['id', 'name', 'email'])
    
    print("\n--- Version 1: Initial Schema ---")
    df_v1.printSchema()
    
    # Evolve schema
    result_v1 = manager.evolve_schema_with_versioning(
        df_v1,
        table_name='customers',
        glue_table_name='customers',
        metadata={'author': 'data_team', 'description': 'Initial customer schema'}
    )
    
    # Create Version 2 with new columns
    df_v2 = spark.createDataFrame([
        (1, 'Alice', 'alice@email.com', 'Gold', '555-1234'),
        (2, 'Bob', 'bob@email.com', 'Silver', '555-5678')
    ], ['id', 'name', 'email', 'tier', 'phone'])
    
    print("\n\n--- Version 2: Evolved Schema ---")
    df_v2.printSchema()
    
    # Evolve schema again
    result_v2 = manager.evolve_schema_with_versioning(
        df_v2,
        table_name='customers',
        glue_table_name='customers',
        metadata={'author': 'data_team', 'description': 'Added tier and phone columns'}
    )
    
    spark.stop()


if __name__ == "__main__":
    example_glue_versioning()
