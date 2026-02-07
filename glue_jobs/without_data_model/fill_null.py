import sys
import json
import re
import boto3
import pandas as pd
from datetime import import datetime
import time
import traceback
import uuid
import traceback

from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql.functions import lit, col, regexp_replace, to_timestamp
from pyspark.sql.types import (
    StructType, StructField, StringType,
    IntegerType, FloatType, DoubleType, LongType, DecimalType, BooleanType, TimestampType, DateType, BinaryType
)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

source_file_path = "s3://nyl-invgai-dev-s3-anaplan-bucket/Data/Input/plan_expense.csv"
source_dataframe = spark.read.option("header", "true").option("inferSchema", "true").csv(source_file_path)

def clean(name):
    name = name.lower()
    name = re.sub(r"[^a-z0-9]", "_", name)
    name = re.sub(r"_+", "_", name)
    return name.strip("_")

# Clean column names in the dataframe
for old in source_dataframe.columns:
    new = clean(old)
    if old != new:
        source_dataframe = source_dataframe.withColumnRenamed(old, new)
time_now = datetime.now().strftime("%Y-%m-%d")
source_dataframe = source_dataframe.withColumn("run_date", lit("time_now")).withColumn("file_name", lit("source_file[name]").cast("string"))

def read_redshift_table_schema():
    client = boto3.client("redshift-data", region_name="us-east-1")
    
    sql_query = "SELECT * FROM dev.public.aws_expense_testing WHERE 1=0"
    
    response = client.execute_statement(
        WorkgroupName="nyl-invgai-dev-wg",
        Database="dev",
        Sql=sql_query,
        SecretArn="arn:aws:secretsmanager:us-east-1:231139216201:secret:redshift-admin-credentials-ZbIY8a",
    )
    
    stmt_id = response["Id"]
    
    # Poll until finished
    while True:
        status = client.describe_statement(Id=stmt_id)["Status"]
        if status in ("FINISHED", "FAILED", "ABORTED"):
            break
        time.sleep(0.5)
    
    if status != "FINISHED":
        raise Exception("Query failed")
    
    result = client.get_statement_result(Id=stmt_id)
    metadata = result["ColumnMetadata"]
    
    # Define mapping function
    def map_redshift_dtype(dtype):
        dtype = dtype.lower()
        if dtype in ("varchar", "char", "character varying"):
            return StringType()
        if dtype in ("int", "integer", "int4"):
            return IntegerType()
        if dtype in ("bigint", "int8"):
            return LongType()
        if dtype in ("smallint", "int2"):
            return IntegerType()
        if dtype in ("decimal", "numeric"):
            return DecimalType(38, 18)
        if dtype in ("float", "float8", "double precision"):
            return DoubleType()
        if dtype in ("boolean", "bool"):
            return BooleanType()
        if dtype in ("timestamp", "timestamp without time zone"):
            return TimestampType()
        if dtype in ("date",):
            return DateType()
        return StringType()  # fallback
    
    schema = StructType([
        StructField(col["name"], map_redshift_dtype(col["typeName"]), True)
        for col in metadata
    ])
    
    rdf = spark.createDataFrame([], schema)
    return rdf

redshift_dataframe = read_redshift_table_schema()

def get_default_value(dtype):
    """
    Get a default value based on the Spark data type.
    """
    if isinstance(dtype, StringType):
        return ""
    elif isinstance(dtype, (IntegerType, FloatType, DoubleType, LongType)):
        return 0
    elif isinstance(dtype, DecimalType):
        return 0.0  # or Decimal('0.0') if you want exact type
    elif isinstance(dtype, BooleanType):
        return False
    elif isinstance(dtype, (DateType, TimestampType)):
        return None  # or datetime.date.today() if you want a real date
    elif isinstance(dtype, BinaryType):
        return b""  # empty bytes
    else:
        return None

def fill_null_values(source_dataframe, redshift_dataframe):
    
    source_cols = [i.name for i in source_dataframe.schema.fields]
    
    for col in redshift_dataframe.schema.fields:
        if col.name not in source_cols:
            default_value = get_default_value(col.dataType)
            source_dataframe = source_dataframe.withColumn(col.name, lit(default_value))

s3_staging_path = f"s3://nyl-invgai-dev-s3-anaplan-bucket/Data/Input/output/aws_expense_testing_{datetime.now().strftime('%Y%m%d_%H%M%S')}/"

if len(source_dataframe.columns) < len(redshift_dataframe.columns):
    
    print("source file has less columns than target table")
    fill_null_values(source_dataframe, redshift_dataframe)
    
    redshift_dataframe = read_redshift_table_schema()
    source_df = source_dataframe.select(*[redshift_dataframe.columns])
    print(len(redshift_dataframe.columns))
    source_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(s3_staging_path)

client = boto3.client("redshift-data", region_name="us-east-1")
ddl = f"""
    COPY dev.public.aws_expense_testing
    FROM '{s3_staging_path}'
    IAM_ROLE 'arn:aws:iam::231139216201:role/application/nyl-invgai-dev-redshift_s3_role'
    FORMAT AS CSV
    DELIMITER ','
    QUOTE '"'
    IGNOREHEADER 1;
"""

response = client.execute_statement(
    WorkgroupName="nyl-invgai-dev-wg",
    Database="dev",
    Sql=ddl,
    SecretArn="arn:aws:secretsmanager:us-east-1:231139216201:secret:redshift-admin-credentials-ZbIY8a",
)
stmt_id = response["Id"]
while True:
    desc = client.describe_statement(Id=stmt_id)
    if desc["Status"] == "FINISHED":
        break
    if desc["Status"] in ("FAILED", "ABORTED"):
        raise Exception(f"COPY failed: {desc}")
    time.sleep(1)
