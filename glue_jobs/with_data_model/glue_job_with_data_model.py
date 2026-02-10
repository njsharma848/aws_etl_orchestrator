import sys
import json
import re
import time
import uuid
import traceback
import functools
from datetime import datetime, timezone
from textwrap import dedent

import boto3
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.functions import lit
from pyspark.sql.types import (
    StructType, StructField, StringType,
    IntegerType, FloatType, DoubleType, LongType, DecimalType,
    BooleanType, TimestampType, DateType, BinaryType
)

# ===============================================================
# LOGGING (structured + S3 export)
# ===============================================================


class LogBuffer:
    def __init__(self, run_id: str):
        self.lines = []
        self.run_id = run_id

    @staticmethod
    def _ts() -> str:
        return datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')

    def info(self, msg: str, **kv):
        payload = {"level": "INFO", "ts": self._ts(), "run_id": self.run_id, "msg": msg}
        if kv:
            payload.update(kv)
        self.lines.append(json.dumps(payload, ensure_ascii=False))
        print(json.dumps(payload))  # also emit to CloudWatch

    def warning(self, msg: str, **kv):
        payload = {"level": "WARNING", "ts": self._ts(), "run_id": self.run_id, "msg": msg}
        if kv:
            payload.update(kv)
        self.lines.append(json.dumps(payload, ensure_ascii=False))
        print(json.dumps(payload))

    def error(self, msg: str, **kv):
        payload = {"level": "ERROR", "ts": self._ts(), "run_id": self.run_id, "msg": msg}
        if kv:
            payload.update(kv)
        self.lines.append(json.dumps(payload, ensure_ascii=False))
        print(json.dumps(payload))

    def export_to_s3(self, bucket: str, key_prefix: str, base_filename: str) -> str:
        key = f"{key_prefix.rstrip('/')}/{base_filename}"
        s3 = boto3.client('s3')
        body = "\n".join(self.lines).encode('utf-8')
        s3.put_object(Bucket=bucket, Key=key, Body=body)
        print(f"Logs exported to s3://{bucket}/{key}")
        return f"s3://{bucket}/{key}"

# ======================================
# retry logic (exponential backoff)
# ===================================


def retry_on_exception(max_attempts=3, base_delay=5, max_delay=120, exceptions=(Exception,)):
    """Retry with exponential backoff: base_delay * 2^(attempt-1), capped at max_delay."""
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            attempt = 0

            _log = kwargs.get('log') or kwargs.get('logger')
            if _log is None:
                for obj in reversed(args):  # look through positional args
                    if hasattr(obj, 'warning') and hasattr(obj, 'error'):
                        _log = obj
                        break

            while attempt < max_attempts:
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    attempt += 1
                    if attempt >= max_attempts:
                        if _log:
                            _log.error(f"{func.__name__} failed after {attempt} attempts: {e}")
                        else:
                            print(f"{func.__name__} failed after {attempt} attempts: {e}")
                        raise
                    wait = min(base_delay * (2 ** (attempt - 1)), max_delay)
                    if _log:
                        _log.warning(
                            f"{func.__name__} failed with {type(e).__name__}: {e}. "
                            f"Retrying in {wait}s (attempt {attempt}/{max_attempts})..."
                        )
                    else:
                        print(
                            f"{func.__name__} failed with {type(e).__name__}: {e}. "
                            f"Retrying in {wait}s (attempt {attempt}/{max_attempts})..."
                        )
                    time.sleep(wait)
        return wrapper
    return decorator

# ===============================================================
# GLUE INIT
# ===============================================================


def initialize_glue(job_name: str):
    sc = SparkContext.getOrCreate()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session
    job = Job(glue_context)
    job.init(job_name, {"job_name": job_name})
    return spark, glue_context, job

# ===============================================================
# DATAFRAME HELPERS
# ===============================================================


def _clean_colname(name: str) -> str:
    name = name.lower()
    name = re.sub(r"[^a-z0-9]", "_", name)
    name = re.sub(r"_+", "_", name)
    return name.strip("_")


def read_csv_file(config: dict, spark):
    try:
        source_file = f"s3://{config['src_bucket']}/data/in/{config['source_file_name']}"

        df = (spark.read.option("header", "true").option("inferSchema", "true").csv(source_file))

        # normalize column names
        for old in df.columns:
            new = _clean_colname(old)
            if old != new:
                df = df.withColumnRenamed(old, new)

        def cast_like(df, config: dict):
            out_cols = []
            ref_fields = {f.name: f.dataType for f in df.schema.fields}
            upsert_keys = [i.lower() for i in config['upsert_keys']]
            for name, dtype in ref_fields.items():
                if isinstance(dtype, DoubleType):
                    if name in df.columns and name in upsert_keys:
                        out_cols.append(F.col(name))
                    else:
                        out_cols.append(F.col(name).cast(DecimalType(38, 18)).alias(name))
                else:
                    if name in df.columns:
                        out_cols.append(F.col(name).cast(dtype).alias(name))
                    else:
                        out_cols.append(F.lit(None).cast(dtype).alias(name))

            return df.select(*out_cols)

        df = cast_like(df, config)

        run_ts = datetime.now(timezone.utc)
        file_name = source_file.split("/")[-1]

        df = (
            df.withColumn("run_date", lit(run_ts.strftime("%Y-%m-%d %H:%M:%S")).cast(TimestampType()))
              .withColumn("file_name", lit(file_name))
        )

        return df

    except Exception as e:
        raise RuntimeError(f"Failed to read the source file: {e}")

# ===============================================================
# REDSHIFT DATA API UTILITIES
# ===============================================================


def _poll_statement(client, stmt_id: str, ctx: str, sleep_s: float = 0.5):
    while True:
        desc = client.describe_statement(Id=stmt_id)
        status = desc.get("Status")
        if status in ("FINISHED", "FAILED", "ABORTED"):
            break
        time.sleep(sleep_s)
    if status != "FINISHED":
        err = desc.get("Error")
        raise RuntimeError(f"{ctx} failed. Status={status}, Error={err}")
    return desc


def execute_sql(sql: str, redshift_conn: dict, client):
    resp = client.execute_statement(
        WorkgroupName=redshift_conn['workgroup_name'],
        Database=redshift_conn['database'],
        Sql=sql,
        SecretArn=redshift_conn['secret_arn']
    )
    return _poll_statement(client, resp["Id"], ctx="SQL")

# ===============================================================
# SCHEMA DISCOVERY & HARMONIZATION
# ===============================================================


def read_redshift_table_schema(config: dict, redshift_conn: dict, spark, client):
    schema_name = redshift_conn['schema_name']
    table_name = config['target_table']
    sql = f"SELECT * FROM {schema_name}.{table_name} WHERE 1=0;"
    resp = client.execute_statement(
        WorkgroupName=redshift_conn["workgroup_name"],
        Database=redshift_conn["database"],
        Sql=sql,
        SecretArn=redshift_conn["secret_arn"]
    )
    _poll_statement(client, resp["Id"], ctx="Read target schema")
    result = client.get_statement_result(Id=resp["Id"])  # we only need ColumnMetadata
    metadata = result["ColumnMetadata"]

    def map_dtype(dtype: str):
        d = dtype.lower()
        if d in ("varchar", "char", "character varying"):
            return StringType()
        if d in ("int", "integer", "int4"):
            return IntegerType()
        if d in ("bigint", "int8"):
            return LongType()
        if d in ("smallint", "int2"):
            return IntegerType()
        if d in ("float", "float8", "double precision"):
            return DoubleType()
        if d in ("decimal", "numeric"):
            return DecimalType(38, 18)
        if d in ("boolean", "bool"):
            return BooleanType()
        if d in ("timestamp", "timestamp without time zone"):
            return TimestampType()
        if d == "date":
            return DateType()
        return StringType()

    schema = StructType([
        StructField(col_meta["name"], map_dtype(col_meta["typeName"]), True)
        for col_meta in metadata
    ])
    return spark.createDataFrame([], schema)


def check_table_exists(redshift_conn: dict, config: dict, client) -> bool:
    sql = dedent(f"""
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = '{redshift_conn['schema_name']}'
          AND table_name   = '{config['target_table']}'
        LIMIT 1;
    """)
    resp = client.execute_statement(
        WorkgroupName=redshift_conn["workgroup_name"],
        Database=redshift_conn["database"],
        Sql=sql,
        SecretArn=redshift_conn["secret_arn"]
    )
    _poll_statement(client, resp["Id"], ctx="Check table exists")
    result = client.get_statement_result(Id=resp["Id"])  # contains Records
    return bool(result.get("Records"))


def _spark_to_redshift_type(data_type) -> str:
    # Prefer unquoted identifiers to avoid case-sensitivity pain in Redshift
    if isinstance(data_type, StringType):
        return "VARCHAR(256)"
    if isinstance(data_type, IntegerType):
        return "INTEGER"
    if isinstance(data_type, LongType):
        return "BIGINT"
    if isinstance(data_type, FloatType):
        return "REAL"
    if isinstance(data_type, DoubleType):
        return "DOUBLE PRECISION"
    if isinstance(data_type, BooleanType):
        return "BOOLEAN"
    if isinstance(data_type, DateType):
        return "DATE"
    if isinstance(data_type, TimestampType):
        return "TIMESTAMP"
    if isinstance(data_type, DecimalType):
        # standardize scale/precision to a safe default
        return "DECIMAL(38,18)"
    if isinstance(data_type, BinaryType):
        return "VARBYTE"
    return "VARCHAR(256)"


def create_new_redshift_table(config: dict, redshift_conn: dict, df, client, log):
    log.info("Target table does not exist; creating")

    upsert_keys = set(config.get('upsert_keys', []))

    cols_ddls = []
    for field in df.schema.fields:
        col_type = _spark_to_redshift_type(field.dataType)
        # If this column is in upsert_keys, enforce NOT NULL
        not_null_clause = " NOT NULL" if field.name in upsert_keys else ""
        cols_ddls.append(f"{field.name} {col_type}{not_null_clause}")

    ddl = dedent(f"""
        CREATE TABLE IF NOT EXISTS {redshift_conn['schema_name']}.{config['target_table']} (
            {', '.join(cols_ddls)}
        );
    """)

    desc = execute_sql(ddl, redshift_conn, client)
    status = desc.get("Status")
    if status == "FINISHED":
        log.info(f"'{config['target_table']}' table successfully created")
        create_views(config, redshift_conn, client, log)


@retry_on_exception(max_attempts=3, base_delay=5, max_delay=60, exceptions=(Exception,))
def alter_redshift_table(config: dict, redshift_conn: dict, df, redshift_df, client, log, spark):
    source_df = read_redshift_table_schema(config, redshift_conn, spark, client)
    if set(source_df.columns) != set(redshift_df.columns):
        # drop view
        drop_views(config, redshift_conn, client, log)

        target_cols = [c.name for c in redshift_df.schema.fields]
        missed_cols = []
        for colf in source_df.schema.fields:
            if colf.name not in target_cols:
                missed_cols.append(colf.name)
                rtype = _spark_to_redshift_type(colf.dataType)
                sql = f"ALTER TABLE {redshift_conn['schema_name']}.{config['target_table']} ADD COLUMN {colf.name} {rtype};"
                execute_sql(sql, redshift_conn, client)
        if missed_cols:
            log.info(f"columns: {missed_cols}' are added successfully")
            desc = create_views(config, redshift_conn, client, log)
            if desc and desc.get('Status') == 'FINISHED':
                log.info("view is refreshed successfully")

# VARCHAR length management


def get_metadata(config: dict, redshift_conn: dict, client) -> dict:
    sql = dedent(f"""
        SELECT column_name, data_type, character_maximum_length
        FROM SVV_COLUMNS
        WHERE table_schema = '{redshift_conn['schema_name']}'
        AND table_name = '{config['target_table']}';
    """)
    desc = execute_sql(sql, redshift_conn, client)
    rows = client.get_statement_result(Id=desc["Id"]).get("Records", [])
    meta = {}
    for r in rows:
        colname = r[0]['stringValue']
        dtype = r[1]['stringValue'].lower()
        length = r[2].get('longValue') if r[2] else None
        meta[colname] = {"dtype": dtype, "length": length}
    return meta


@retry_on_exception(max_attempts=3, base_delay=5, max_delay=120, exceptions=(Exception,))
def alter_varchar_columns(config: dict, redshift_conn: dict, df, client, log):
    metadata = get_metadata(config, redshift_conn, client)
    INT_RANGES = {
        "smallint": 32767,
        "int2": 32767,
        "integer": 2147483647,
        "int": 2147483647,
        "int4": 2147483647,
        "bigint": 9223372036854775807,
        "int8": 9223372036854775807,
    }

    string_cols = []
    int_cols = []

    # Handle string length
    for f in df.schema.fields:
        if isinstance(f.dataType, StringType):
            string_cols.append(f.name)
        elif isinstance(f.dataType, (IntegerType, LongType)):
            int_cols.append(f.name)

    if not string_cols and not int_cols:
        return

    agg_expr = []

    for c in string_cols:
        agg_expr.append(F.max(F.length(F.col(c))).alias(c))

    for c in int_cols:
        agg_expr.append(F.max(F.abs(F.col(c))).alias(c))

    row = df.agg(*agg_expr).collect()[0]

    str_altered_cols = []
    for colname in string_cols:
        src_len = int(row[colname] or 0)
        curr_len = int(metadata.get(colname, {}).get("length") or 0)
        if src_len > curr_len:
            drop_views(config, redshift_conn, client, log)
            new_len = min(src_len + 10, 65535)

            sql = f"""
                ALTER TABLE {redshift_conn['schema_name']}.{config['target_table']}
                ALTER COLUMN {colname} TYPE VARCHAR({new_len});
            """
            desc = execute_sql(sql, redshift_conn, client)
            str_altered_cols.append({"column_name": colname, "source_length": src_len, "current_length": curr_len, "new_length": new_len})

    if str_altered_cols:
        log.info(f"columns: {str_altered_cols} are altered with new length")
        desc = create_views(config, redshift_conn, client, log)
        if desc and desc.get('Status') == 'FINISHED':
            log.info("view is refreshed successfully")

    # ---- Handle INTEGER widening ----
    int_altered_cols = []
    for colname in int_cols:
        max_val = int(row[colname] or 0)
        curr_dtype = metadata.get(colname, {}).get("dtype")

        if not curr_dtype or curr_dtype not in INT_RANGES:
            continue

        curr_max = INT_RANGES[curr_dtype]

        if max_val > curr_max:
            if curr_dtype in ("smallint", "int2"):
                new_type = "INTEGER"
            elif curr_dtype in ("integer", "int", "int4"):
                new_type = "BIGINT"
            else:
                continue  # already BIGINT

            # drop view before altering the redshift table
            drop_views(config, redshift_conn, client, log)

            add_sql = f"""
                ALTER TABLE {redshift_conn['schema_name']}.{config['target_table']} ADD COLUMN sample_col {new_type};"""
            set_sql = f"""
                UPDATE {redshift_conn['schema_name']}.{config['target_table']} SET sample_col = {colname}::{new_type};"""
            drop_sql = f"""
                ALTER TABLE {redshift_conn['schema_name']}.{config['target_table']} DROP COLUMN {colname};"""
            rename_sql = f"""
                ALTER TABLE {redshift_conn['schema_name']}.{config['target_table']} RENAME COLUMN sample_col TO {colname};"""

            desc = execute_sql(add_sql, redshift_conn, client)
            if desc['Status'] == 'FINISHED':
                desc = execute_sql(set_sql, redshift_conn, client)
                if desc['Status'] == 'FINISHED':
                    desc = execute_sql(drop_sql, redshift_conn, client)
                    if desc['Status'] == 'FINISHED':
                        desc = execute_sql(rename_sql, redshift_conn, client)

            int_altered_cols.append({"column_name": colname, "current_datatype": curr_dtype, "new_datatype": new_type})

    if int_altered_cols:
        log.info(f"columns: {int_altered_cols} are altered with new datatype")
        desc = create_views(config, redshift_conn, client, log)
        if desc and desc.get('Status') == 'FINISHED':
            log.info("view is refreshed successfully")

# Fill missing columns to match target layout ----------------------------------


def get_default_value(dtype):
    if isinstance(dtype, StringType):
        return ""
    if isinstance(dtype, (IntegerType, FloatType, DoubleType, LongType)):
        return 0
    if isinstance(dtype, DecimalType):
        return 0.0
    if isinstance(dtype, BooleanType):
        return False
    if isinstance(dtype, (DateType, TimestampType)):
        return None
    if isinstance(dtype, BinaryType):
        return b""
    return None


def fill_missing_columns(df, redshift_df, log):
    src_cols = set(df.columns)
    missed_cols = []
    for colf in redshift_df.schema.fields:
        if colf.name not in src_cols:
            missed_cols.append(colf.name)
            default_value = get_default_value(colf.dataType)
            df = df.withColumn(colf.name, lit(default_value))
    if missed_cols:
        log.info(f"columns: {missed_cols} filled with null values")

    return df

# ===============================================================
# STAGING TABLE + COPY
# ===============================================================


def create_staging_table(config: dict, redshift_conn: dict, staging_table_name: str, client, log):
    staging = staging_table_name
    log.info(f"Creating staging table: {staging}")
    schema = redshift_conn['schema_name']
    sql = f"CALL public.sp_create_staging_table('{schema}', '{staging}', '{config['target_table']}')"
    execute_sql(sql, redshift_conn, client)


def _find_single_csv_in_prefix(s3_uri: str) -> str:
    """Return the single CSV object key within the given prefix (coalesce(1) write).
    Expects s3_uri as s3://bucket/prefix
    """
    s3 = boto3.client('s3')
    bucket, prefix = s3_uri.replace("s3://", "").split("/", 1)
    resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    contents = resp.get("Contents", [])
    # Prefer part file with .csv extension; Spark writes files as part-00000-...csv
    candidates = [o["Key"] for o in contents if o["Key"].endswith(".csv")]
    if not candidates:
        # Fallback: any part-* file
        candidates = [o["Key"] for o in contents if "/part-" in o["Key"]]
    if not candidates:
        raise RuntimeError(f"No CSV objects found under {s3_uri}")
    # choose the smallest alphabetical (there should be only one)
    return f"s3://{bucket}/{sorted(candidates)[0]}"


def copy_to_redshift(s3_staging_path: str, redshift_conn: dict, staging_table_name: str, client, log):
    staging = staging_table_name
    log.info(f"Data is being copied from s3 to staging table({staging})")
    schema = redshift_conn['schema_name']
    # Resolve single-file path to avoid Redshift attempting to read _SUCCESS
    s3_single_file = _find_single_csv_in_prefix(s3_staging_path)
    sql = f"CALL public.sp_copy_from_s3('{schema}', '{staging}', '{s3_single_file}', '{redshift_conn['iam_role']}')"
    execute_sql(sql, redshift_conn, client)

# ===============================================================
# MERGE (delete+insert) with transaction
# ===============================================================


@retry_on_exception(max_attempts=3, base_delay=5, max_delay=120, exceptions=(Exception,))
def run_merge(config: dict, redshift_conn: dict, staging_table_name: str, client, log):
    keys = config['upsert_keys']
    schema = redshift_conn['schema_name']
    log.info(f"Running merge between {schema}.{config['target_table']} and {staging_table_name}")
    upsert_keys_csv = ','.join(keys)
    sql = f"CALL public.sp_merge_from_staging('{schema}', '{config['target_table']}', '{staging_table_name}', '{upsert_keys_csv}')"
    execute_sql(sql, redshift_conn, client)


def get_row_count(config: dict, redshift_conn: dict, client) -> int:
    sql = f"SELECT COUNT(*) FROM {redshift_conn['schema_name']}.{config['target_table']};"
    resp = client.execute_statement(
        WorkgroupName=redshift_conn['workgroup_name'],
        Database=redshift_conn['database'],
        Sql=sql,
        SecretArn=redshift_conn['secret_arn']
    )
    _poll_statement(client, resp["Id"], ctx="Row count")
    result = client.get_statement_result(Id=resp["Id"])  # Records
    records = result.get("Records", [])
    if not records:
        return 0
    first_cell = records[0][0]
    if "longValue" in first_cell:
        return int(first_cell["longValue"])
    if "doubleValue" in first_cell:
        return int(first_cell["doubleValue"])
    if "stringValue" in first_cell:
        return int(first_cell["stringValue"])
    raise ValueError(f"Unexpected count cell format: {first_cell}")

# ====================================================
# view config helper
# ====================================================


def _load_view_config(config: dict, log):
    bucket = config['src_bucket']
    if not bucket:
        log.error("source bucket is not defined")
        raise ValueError("source bucket not found")

    key = "config/config_view.json"
    s3 = boto3.client('s3')
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        json_file = response['Body'].read().decode('utf-8')
        data = json.loads(json_file)
    except Exception as e:
        log.error(f"Failed to download or parse config file from S3: {e}")
        raise

    v_config = None
    for d in data:
        if d['source_table'] == config['target_table']:
            v_config = {
                'source_table': d['source_table'],
                'view_name': d['view_name'],
                'schema_name': d['schema_name'],
                'definition': d['definition']
            }
    return v_config

# ====================================================
# create view
# ====================================================


def create_views(config: dict, redshift_conn: dict, client, log):
    v_config = _load_view_config(config, log)
    if not v_config:
        log.info("No configuration found for the target table")
        return None

    view_name = v_config['view_name']
    schema_name = v_config['schema_name']
    source_table = v_config['source_table']
    definition = v_config['definition']

    ddl = definition.format(
        schema_name=schema_name,
        view_name=view_name,
        source_table=source_table
    )

    try:
        resp = client.execute_statement(
            WorkgroupName=redshift_conn['workgroup_name'],
            Database=redshift_conn['database'],
            Sql=ddl,
            SecretArn=redshift_conn['secret_arn']
        )
        stmt_id = resp["Id"]

        while True:
            desc = client.describe_statement(Id=stmt_id)
            if desc["Status"] == "FINISHED":
                break
            if desc["Status"] in ("ABORTED", "FAILED"):
                raise RuntimeError(desc.get("Error", "view creation failed"))
            time.sleep(1)
        return desc
    except Exception as e:
        log.error(f"Failed to create view '{view_name}' with error: {e}")
        raise


def drop_views(config: dict, redshift_conn: dict, client, log):
    v_config = _load_view_config(config, log)
    if not v_config:
        log.warning("No view configuration found for the target table — nothing to drop")
        return None

    view_name = v_config['view_name']
    schema_name = v_config['schema_name']

    ddl = f"""DROP VIEW IF EXISTS {schema_name}.{view_name}"""

    try:
        resp = client.execute_statement(
            WorkgroupName=redshift_conn['workgroup_name'],
            Database=redshift_conn['database'],
            Sql=ddl,
            SecretArn=redshift_conn['secret_arn']
        )
        stmt_id = resp["Id"]

        while True:
            desc = client.describe_statement(Id=stmt_id)
            if desc["Status"] == "FINISHED":
                break
            if desc["Status"] in ("ABORTED", "FAILED"):
                raise RuntimeError(desc.get("Error", "view drop failed"))
            time.sleep(1)
        return desc
    except Exception as e:
        log.error(f"Failed to drop view '{view_name}' with error: {e}")
        raise

# ===============================================================
# AUDIT TABLE UPDATE
# ===============================================================


def update_job_sts_table(config: dict, redshift_conn: dict,
                         run_start_ts: str, run_end_ts: str,
                         source_filename: str,
                         records_read: int,
                         records_updated: int,
                         records_inserted: int,
                         status: str, error_message: str,
                         client):
    run_id = str(int(time.time()))
    schema = redshift_conn['schema_name']
    safe_error = str(error_message).replace("'", "''")[:4096] if error_message else ''
    safe_filename = source_filename.replace("'", "''")
    sql = (
        f"CALL public.sp_log_job_status("
        f"'{schema}', '{run_id}', '{config['job_id']}', "
        f"'{run_start_ts}', '{run_end_ts}', "
        f"'{safe_filename}', '{config['target_table']}', "
        f"{records_read}, {records_updated}, {records_inserted}, "
        f"'{status}', '{safe_error}')"
    )
    execute_sql(sql, redshift_conn, client)

# ===============================================================
# S3 HELPERS (archive & cleanup)
# ===============================================================


def move_s3_file_to_archive(config: dict, target_file_path: str, log):
    s3_client = boto3.client('s3')
    source_file_path = f"s3://{config['src_bucket']}/data/in/{config['source_file_name']}"
    source_bucket, source_key = source_file_path.replace("s3://", "").split("/", 1)
    target_bucket, target_key = target_file_path.replace("s3://", "").split("/", 1)
    try:
        s3_client.copy_object(
            Bucket=target_bucket,
            CopySource={'Bucket': source_bucket, 'Key': source_key},
            Key=target_key
        )
        log.info(f"File copied from {source_file_path} to {target_file_path}")
        s3_client.delete_object(Bucket=source_bucket, Key=source_key)
        log.info(f"Original file {source_file_path} deleted.")
    except Exception as e:
        log.error("Error while moving file", error=str(e))


def delete_staging_s3_files(s3_staging_path: str, log):
    s3_client = boto3.client('s3')
    bucket_name, prefix = s3_staging_path.replace("s3://", "").split("/", 1)
    try:
        objects = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        for obj in objects.get("Contents", []):
            s3_client.delete_object(Bucket=bucket_name, Key=obj["Key"])
            log.info(f"Deleted staging file: {obj['Key']}")
    except Exception as e:
        log.error("Error deleting staging files", error=str(e))


def move_s3_file_to_unprocessed(config: dict, target_file_path: str, log):
    s3_client = boto3.client('s3')
    source_file_path = f"s3://{config['src_bucket']}/data/in/{config['source_file_name']}"
    source_bucket, source_key = source_file_path.replace("s3://", "").split("/", 1)
    target_bucket, target_key = target_file_path.replace("s3://", "").split("/", 1)
    try:
        s3_client.copy_object(
            Bucket=target_bucket,
            CopySource={'Bucket': source_bucket, 'Key': source_key},
            Key=target_key
        )
        log.info(f"File copied from {source_file_path} to {target_file_path}")
        s3_client.delete_object(Bucket=source_bucket, Key=source_key)
        log.info(f"Original file {source_file_path} deleted.")
    except Exception as e:
        log.error("Error while moving file", error=str(e))

# ===============================================================
# DIMENSIONAL MODEL CONFIG LOADER
# ===============================================================


def load_dimensional_config(bucket: str, config_key: str = "config/dimensional_mappings.json"):
    """Load dimensional model configuration from S3"""
    s3 = boto3.client('s3')
    try:
        response = s3.get_object(Bucket=bucket, Key=config_key)
        config = json.loads(response['Body'].read().decode('utf-8'))
        return {item['source_table']: item['dimensional_model'] for item in config}
    except Exception as e:
        print(f"No dimensional config found or error loading: {e}")
        return {}

# ===============================================================
# GENERIC SCD TYPE 2 PROCESSOR
# ===============================================================


def process_dimension_from_config(df, dim_config: dict, redshift_conn: dict,
                                  spark, client, log):
    """
    Process any dimension using config-driven approach (SCD Type 2)
    """

    dim_table = dim_config['dimension_table']
    natural_keys = dim_config['natural_keys']
    scd_attrs = dim_config.get('scd_attributes', [])
    col_mappings = dim_config.get('column_mappings', {})

    log.info(f"Processing SCD Type 2 dimension: {dim_table}")

    # Apply column mappings (rename source columns to dimension columns)
    dim_df = df
    for dim_col, source_col in col_mappings.items():
        if source_col in df.columns:
            dim_df = dim_df.withColumnRenamed(source_col, dim_col)

    # Select only columns needed for dimension
    dim_columns = list(col_mappings.keys())
    dim_df = dim_df.select(*[c for c in dim_columns if c in dim_df.columns]).distinct()

    # Add SCD metadata
    from pyspark.sql.functions import current_date, current_timestamp
    dim_df = (dim_df
              .withColumn("effective_date", current_date())
              .withColumn("end_date", lit(None).cast(DateType()))
              .withColumn("is_current", lit(True))
              .withColumn("version", lit(1))
              .withColumn("created_date", current_timestamp())
              .withColumn("updated_date", current_timestamp())
              )

    schema = redshift_conn['schema_name']
    run_id = uuid.uuid4().hex
    staging_dim = f"stg_{dim_table}_{run_id}"
    s3_staging_path = f"s3://{redshift_conn['src_bucket']}/data/staging/dimensions/{staging_dim}"

    # Write to S3
    dim_df.coalesce(1).write.mode("overwrite") \
        .option("header", True) \
        .option("quote", '"') \
        .csv(s3_staging_path)

    # Build explicit column list matching the CSV (no surrogate key)
    surrogate_key_col = f"{dim_table.replace('dim_', '')}_key"
    scd_meta_cols = ["effective_date", "end_date", "is_current", "version", "created_date", "updated_date"]
    staging_columns = dim_columns + scd_meta_cols

    # Create staging table with only CSV columns (excludes surrogate key)
    create_stg_sql = f"""
        DROP TABLE IF EXISTS {schema}.{staging_dim};
        CREATE TABLE {schema}.{staging_dim} AS
        SELECT {', '.join(staging_columns)}
        FROM {schema}.{dim_table} WHERE 1=0;
    """
    execute_sql(create_stg_sql, redshift_conn, client)

    # COPY to staging via stored procedure
    s3_single_file = _find_single_csv_in_prefix(s3_staging_path)
    copy_sql = f"CALL public.sp_copy_from_s3('{schema}', '{staging_dim}', '{s3_single_file}', '{redshift_conn['iam_role']}')"
    execute_sql(copy_sql, redshift_conn, client)

    # SCD Type 2 merge via stored procedure
    natural_keys_csv = ','.join(natural_keys)
    scd_attrs_csv = ','.join(scd_attrs) if scd_attrs else ''
    dim_columns_csv = ','.join(dim_columns)

    scd2_sql = (
        f"CALL public.sp_process_scd_type2("
        f"'{schema}', '{dim_table}', '{staging_dim}', "
        f"'{surrogate_key_col}', '{natural_keys_csv}', "
        f"'{scd_attrs_csv}', '{dim_columns_csv}')"
    )
    execute_sql(scd2_sql, redshift_conn, client)
    log.info(f"{dim_table} SCD Type 2 processed successfully")

# ===============================================================
# GENERIC SCD TYPE 1 PROCESSOR
# ===============================================================


def process_scd_type1_dimension(df, dim_config: dict, redshift_conn: dict,
                                spark, client, log):
    """
    Process SCD Type 1 dimension (overwrites on change, no history)
    """

    dim_table = dim_config['dimension_table']
    natural_keys = dim_config['natural_keys']
    col_mappings = dim_config.get('column_mappings', {})

    log.info(f"Processing SCD Type 1 dimension: {dim_table}")

    # Apply column mappings
    dim_df = df
    for dim_col, source_col in col_mappings.items():
        if source_col in df.columns:
            dim_df = dim_df.withColumnRenamed(source_col, dim_col)

    # Select only columns needed
    dim_columns = list(col_mappings.keys())
    dim_df = dim_df.select(*[c for c in dim_columns if c in dim_df.columns]).distinct()

    # Add audit columns (no SCD fields for Type 1)
    from pyspark.sql.functions import current_timestamp
    dim_df = dim_df.withColumn("created_date", current_timestamp())

    schema = redshift_conn['schema_name']
    run_id = uuid.uuid4().hex
    staging_dim = f"stg_{dim_table}_{run_id}"
    s3_staging_path = f"s3://{redshift_conn['src_bucket']}/data/staging/dimensions/{staging_dim}"

    # Write to S3
    dim_df.coalesce(1).write.mode("overwrite") \
        .option("header", True) \
        .option("quote", '"') \
        .csv(s3_staging_path)

    # Build explicit column list matching the CSV (no surrogate key)
    surrogate_key_col = f"{dim_table.replace('dim_', '')}_key"
    staging_columns = dim_columns + ["created_date"]

    # Create staging table with only CSV columns (excludes surrogate key)
    create_stg_sql = f"""
        DROP TABLE IF EXISTS {schema}.{staging_dim};
        CREATE TABLE {schema}.{staging_dim} AS
        SELECT {', '.join(staging_columns)}
        FROM {schema}.{dim_table} WHERE 1=0;
    """
    execute_sql(create_stg_sql, redshift_conn, client)

    # COPY to staging via stored procedure
    s3_single_file = _find_single_csv_in_prefix(s3_staging_path)
    copy_sql = f"CALL public.sp_copy_from_s3('{schema}', '{staging_dim}', '{s3_single_file}', '{redshift_conn['iam_role']}')"
    execute_sql(copy_sql, redshift_conn, client)

    # SCD Type 1 merge via stored procedure
    natural_keys_csv = ','.join(natural_keys)
    update_cols = [col for col in dim_columns if col not in natural_keys]
    update_cols_csv = ','.join(update_cols)
    dim_columns_csv = ','.join(dim_columns)

    scd1_sql = (
        f"CALL public.sp_process_scd_type1("
        f"'{schema}', '{dim_table}', '{staging_dim}', "
        f"'{surrogate_key_col}', '{natural_keys_csv}', "
        f"'{update_cols_csv}', '{dim_columns_csv}')"
    )
    execute_sql(scd1_sql, redshift_conn, client)
    log.info(f"{dim_table} (Type 1) processed successfully")

# ===============================================================
# GENERIC FACT TABLE PROCESSOR
# ===============================================================


def process_fact_from_config(df, fact_config: dict, redshift_conn: dict,
                             spark, client, log):
    """
    Process fact table using config-driven lookups
    """

    fact_table = fact_config['fact_table']
    dim_lookups = fact_config['dimension_lookups']
    measures = fact_config['measures']
    measure_mappings = fact_config.get('measure_mappings', {})
    degen_dims = fact_config.get('degenerate_dimensions', [])
    degen_mappings = fact_config.get('degenerate_mappings', {})

    log.info(f"Processing fact: {fact_table}")

    schema = redshift_conn['schema_name']
    fact_df = df

    # Apply measure column mappings (rename source columns to target columns)
    for target_col, source_col in measure_mappings.items():
        if source_col in fact_df.columns:
            fact_df = fact_df.withColumnRenamed(source_col, target_col)

    # Apply degenerate dimension mappings
    for target_col, source_col in degen_mappings.items():
        if source_col in fact_df.columns:
            fact_df = fact_df.withColumnRenamed(source_col, target_col)

    # Build select columns list
    select_cols = []

    # Add measures
    for measure in measures:
        if measure in fact_df.columns:
            select_cols.append(measure)

    # Add degenerate dimensions
    for degen in degen_dims:
        if degen in fact_df.columns:
            select_cols.append(degen)

    # Add natural keys for dimension lookups (needed for JOIN in Redshift)
    for lookup in dim_lookups:
        for nat_key in lookup['natural_keys']:
            if nat_key in fact_df.columns and nat_key not in select_cols:
                select_cols.append(nat_key)

    # Add audit columns
    fact_df = fact_df.withColumn("load_timestamp", F.current_timestamp())
    select_cols.append("load_timestamp")

    # Write fact staging to S3
    run_id = uuid.uuid4().hex
    staging_fact = f"stg_{fact_table}_{run_id}"
    s3_staging_path = f"s3://{redshift_conn['src_bucket']}/data/staging/facts/{staging_fact}"

    fact_df.select(*select_cols).coalesce(1).write.mode("overwrite") \
        .option("header", True) \
        .csv(s3_staging_path)

    # Create staging fact table with only CSV columns (excludes surrogate keys added via JOINs)
    stg_col_list = [c for c in select_cols if c != "load_timestamp"] + ["load_timestamp"]
    create_stg_sql = f"""
        DROP TABLE IF EXISTS {schema}.{staging_fact};
        CREATE TABLE {schema}.{staging_fact} (
            {', '.join(f'{c} VARCHAR(512)' for c in stg_col_list)}
        );
    """
    execute_sql(create_stg_sql, redshift_conn, client)

    # COPY to staging via stored procedure
    s3_single_file = _find_single_csv_in_prefix(s3_staging_path)
    copy_sql = f"CALL public.sp_copy_from_s3('{schema}', '{staging_fact}', '{s3_single_file}', '{redshift_conn['iam_role']}')"
    execute_sql(copy_sql, redshift_conn, client)

    # Build dimension lookup parameters for sp_load_fact_table
    join_clauses = []
    surrogate_keys = []

    for lookup in dim_lookups:
        dim_name = lookup['dimension']
        nat_keys = lookup['natural_keys']
        surrogate = lookup['surrogate_key']

        alias = dim_name.replace('dim_', 'd_')

        join_condition = " AND ".join([
            f"stg.{k} = {alias}.{k}" for k in nat_keys
        ])

        # Check if dimension has is_current column (SCD Type 2)
        if dim_name not in ['dim_date', 'dim_version', 'dim_data_source']:
            current_check = f"AND {alias}.is_current = TRUE"
        else:
            current_check = ""

        join_clauses.append(
            f"LEFT JOIN {schema}.{dim_name} {alias} ON {join_condition} {current_check}"
        )

        surrogate_keys.append(f"{alias}.{surrogate}")

    surrogate_selects = ', '.join(surrogate_keys)
    measure_cols = ', '.join(
        [f"stg.{m}" for m in measures]
        + [f"stg.{d}" for d in degen_dims]
        + ["stg.load_timestamp"]
    )
    join_clause_str = ' '.join(join_clauses)

    # Load fact table via stored procedure
    fact_sql = (
        f"CALL public.sp_load_fact_table("
        f"'{schema}', '{fact_table}', '{staging_fact}', "
        f"'{surrogate_selects}', '{measure_cols}', "
        f"'{join_clause_str}')"
    )
    execute_sql(fact_sql, redshift_conn, client)
    log.info(f"{fact_table} populated successfully")

# ===============================================================
# MAIN
# ===============================================================


def main():
    args = getResolvedOptions(
        sys.argv,
        [
            'job_id', 'job_name', 'source_file_name', 'target_table', 'upsert_keys',
            'workgroup_name', 'database', 'region', 'secret_arn', 'iam_role',
            'schema_name', 'src_bucket'
        ]
    )

    # Build configs
    config = {
        'job_id': args['job_id'],
        'job_name': args['job_name'],
        'source_file_name': args['source_file_name'],
        'target_table': args['target_table'],
        'upsert_keys': json.loads(args['upsert_keys']),
        'src_bucket': args['src_bucket']
    }
    redshift_conn = {
        'workgroup_name': args['workgroup_name'],
        'database': args['database'],
        'region': args['region'],
        'secret_arn': args['secret_arn'],
        'iam_role': args['iam_role'],
        'schema_name': args['schema_name'],
        'src_bucket': args['src_bucket']  # Added for dimensional processing
    }

    run_id = uuid.uuid4().hex
    log = LogBuffer(run_id)
    spark, glue_context, job = initialize_glue(config['job_name'])
    client = boto3.client("redshift-data", region_name=redshift_conn['region'])
    run_start_ts = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
    year = datetime.now().strftime('%Y')
    month = datetime.now().strftime('%m')
    source_filename = config['source_file_name']
    log_base_name = f"{source_filename.split('.')[0]}_log_{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}.txt"
    s3_archive_path = f"s3://{config['src_bucket']}/data/archive/{year}/{month}/{source_filename.split('.')[0]}_{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}.csv"
    s3_unprocessed_path = f"s3://{config['src_bucket']}/data/unprocessed/{year}/{month}/{source_filename}"
    staging = _clean_colname(config['source_file_name'].split('.')[0])
    s3_staging_path = f"s3://{config['src_bucket']}/data/staging/{config['target_table']}_{staging}/{run_id}"

    try:
        df = read_csv_file(config, spark)

        records_read = df.count()
        log.info("Records read", count=records_read)

        if check_table_exists(redshift_conn, config, client):
            log.info("Target table exists")

            redshift_df = read_redshift_table_schema(config, redshift_conn, spark, client)
            rows_before = get_row_count(config, redshift_conn, client)

            # Reconcile schema
            if set(df.columns) != set(redshift_df.columns):
                log.info("Reconciling new/missing columns")

                # fill null values for missing columns and align column order
                df = fill_missing_columns(df, redshift_df, log)

                # Alter redshift table(Add new columns) if new columns are added in the source file
                alter_redshift_table(config, redshift_conn, df, redshift_df, client, log, spark)

        else:
            log.info("Target table does not exist; creating")

            # Create new table in redshift database based on source DF schema
            create_new_redshift_table(config, redshift_conn, df, client, log)
            rows_before = 0

        # Alter varchar length if needed
        alter_varchar_columns(config, redshift_conn, df, client, log)
        redshift_df = read_redshift_table_schema(config, redshift_conn, spark, client)

        df = df.select(*[c.name for c in redshift_df.schema.fields])

        # Write DF to S3 (single file) then COPY
        df.coalesce(1).write.mode("overwrite").option("header", True)\
            .option("quote", '"').option("escape", '"').csv(s3_staging_path)

        staging_table_name = f"{config['target_table']}_{re.sub(r'_', '', staging)}"

        # create staging table
        create_staging_table(config, redshift_conn, staging_table_name, client, log)
        # run copy to staging table
        copy_to_redshift(s3_staging_path, redshift_conn, staging_table_name, client, log)
        # Run merge and remove staging table after merge
        run_merge(config, redshift_conn, staging_table_name, client, log)

        # ============================================================
        # DIMENSIONAL MODEL PROCESSING (CONFIG-DRIVEN)
        # Wrapped in try/except so base ETL success is preserved
        # even if star-schema population fails.
        # ============================================================
        try:
            dim_config = load_dimensional_config(config['src_bucket'])

            if config['target_table'] in dim_config:
                log.info(f"Dimensional model found for {config['target_table']}")

                model_config = dim_config[config['target_table']]

                # Process dimensions first (SCD Type 1 or Type 2)
                for dim in model_config.get('dimensions', []):
                    if dim.get('scd_type') == 1:
                        process_scd_type1_dimension(
                            df=df,
                            dim_config=dim,
                            redshift_conn=redshift_conn,
                            spark=spark,
                            client=client,
                            log=log
                        )
                    else:  # Default to Type 2
                        process_dimension_from_config(
                            df=df,
                            dim_config=dim,
                            redshift_conn=redshift_conn,
                            spark=spark,
                            client=client,
                            log=log
                        )

                # Then process facts (with surrogate key lookups)
                for fact in model_config.get('facts', []):
                    process_fact_from_config(
                        df=df,
                        fact_config=fact,
                        redshift_conn=redshift_conn,
                        spark=spark,
                        client=client,
                        log=log
                    )

                log.info(f"Dimensional model processing completed for {config['target_table']}")

        except Exception as dim_err:
            log.error("Dimensional model processing failed (base ETL succeeded)", error=str(dim_err))
            traceback.print_exc()
        finally:
            # Cleanup dimension/fact staging files from S3
            bucket = config['src_bucket']
            for prefix in ["data/staging/dimensions/", "data/staging/facts/"]:
                try:
                    delete_staging_s3_files(f"s3://{bucket}/{prefix}", log)
                except Exception:
                    pass

        job.commit()

        rows_after = get_row_count(config, redshift_conn, client)
        records_inserted = max(rows_after - rows_before, 0)
        records_updated = max(records_read - records_inserted, 0)
        run_end_ts = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')

        # Update status table with results
        update_job_sts_table(
            config, redshift_conn, run_start_ts, run_end_ts, source_filename,
            records_read, records_updated, records_inserted, "SUCCESS", "NULL", client
        )
        log.info("ETL Job Completed Successfully")

        # Cleanup
        delete_staging_s3_files(s3_staging_path, log)
        move_s3_file_to_archive(config, s3_archive_path, log)

        # Export logs
        log.export_to_s3(config['src_bucket'], f"logs/{datetime.now(timezone.utc).strftime('%Y/%m/%d')}", log_base_name)

    except Exception as e:
        log.error("ETL FAILED", error=str(e))
        traceback.print_exc()
        try:
            # best-effort: record failure status (use records_read from outer scope if available)
            run_end_ts = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
            fail_records_read = records_read if 'records_read' in dir() else 0
            update_job_sts_table(
                config, redshift_conn, run_start_ts, run_end_ts, source_filename,
                fail_records_read, 0, 0, "FAILED", str(e), client
            )
        except Exception as audit_ex:
            log.error("Failed to record job status", error=str(audit_ex))
        finally:
            try:
                move_s3_file_to_unprocessed(config, s3_unprocessed_path, log)
                log.export_to_s3(config['src_bucket'], f"logs/{datetime.now(timezone.utc).strftime('%Y/%m/%d')}", log_base_name)
            except Exception:
                pass
        raise


if __name__ == '__main__':
    main()
