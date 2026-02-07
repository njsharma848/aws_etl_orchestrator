import boto3
import time

ddl = """CREATE OR REPLACE VIEW anaplan.general_account_invoice_v AS
SELECT * FROM anaplan.general_account_invoice"""

client = boto3.client("redshift-data", region_name="us-east-1")

resp = client.execute_statement(
    WorkgroupName="nyl-invgai-wg",        # Redshift Serverless workgroup
    Database="nyl_anaplan_db",
    Sql=ddl,
    SecretArn="arn:aws:secretsmanager:us-east-1:231139216201:secret:redshift/anaplan_batch_user-nWKC1N",
)

stmt_id = resp["Id"]

# Poll until the statement finishes
while True:
    desc = client.describe_statement(Id=stmt_id)
    status = desc["Status"]
    if status == "FINISHED":
        print("view created successfully.")
        break
    if status in ("FAILED", "ABORTED"):
        print("view creation failed:", desc.get("Error"))
        raise RuntimeError("DDL failed")
    time.sleep(1)
    