import sys
import json
from awsglue.transforms import *  # noqa: F401,F403
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'job_id', 'job_name', 'source_file_path', 'target_table', 'upsert_keys'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Parse upsert_keys from JSON string to list
upsert_keys = json.loads(args['upsert_keys'])

print(f"Job ID: {args['job_id']}")
print(f"Job Name: {args['job_name']}")
print(f"Source File: {args['source_file_path']}")
print(f"Target Table: {args['target_table']}")
print(f"Upsert Keys: {upsert_keys}")

job.commit()
