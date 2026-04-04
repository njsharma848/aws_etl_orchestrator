# AWS Glue 5.0 — New Features Guide

 Serverless, scalable data integration on AWS

---

## 🧠 Mnemonic to Remember Glue 5.0 Features

### **"FASTER DL"**

| Letter | Feature |
|--------|---------|
| **F** | **F**ramework Upgrades (Spark 3.5.4, Python 3.11, Java 17) |
| **A** | **A**ccess Control — Spark-native Lake Formation FGAC |
| **S** | **S**ageMaker Integration (Unified Studio + Lakehouse) |
| **T** | **T**able Formats — Iceberg 1.7.1, Delta 3.3.0, Hudi 0.15.0 |
| **E** | **E**nhanced S3 (Access Grants + S3 Table Buckets) |
| **R** | **R**equirements.txt — Native Python dependency management |
| **D** | **D**ata Lineage in Amazon DataZone |
| **L** | **L**akehouse Views — Multi-dialect Glue Data Catalog views |

> Think: **"Glue 5.0 runs FASTER DL (Data Lake)"** — because that's exactly what it does!

---

## Performance at a Glance

| Metric | Glue 4.0 | Glue 5.0 | Improvement |
|--------|----------|----------|-------------|
| Speed vs open-source Spark | baseline | 3.9× faster | ⚡ |
| Speed vs Glue 4.0 | baseline | **32% faster** | ⚡ |
| Cost vs Glue 4.0 | baseline | **22% cheaper** | 💰 |
| TPC-DS 3TB benchmark | 1,896s | 1,197s | **58% faster** |

---

## 1. F — Framework Upgrades

### What it is
Glue 5.0 upgrades all core engine versions for better performance, security, and access to modern Python features.

| Component | Glue 4.0 | Glue 5.0 |
|-----------|----------|----------|
| Apache Spark | 3.3.0 | **3.5.4** |
| Python | 3.10 | **3.11** |
| Java | 8 | **17** |
| Scala | 2.12 | **2.12.18** |
| Arrow | 7.0.0 | **12.0.1** |

### Simple Example

```python
# Glue 5.0 job — just set GlueVersion to "5.0" in your job config
# The newer Spark and Python versions work automatically

import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Python 3.11 feature — exception groups now available
# Spark 3.5.4 — Arrow-optimized UDFs work out of the box
print(spark.version)   # 3.5.4
import sys
print(sys.version)     # 3.11.x
```

**AWS Console — enable Glue 5.0:**
```
Job details → Glue version → "Glue 5.0 (Spark 3.5, Python 3)"
```

**Why it matters:** Python 3.11 is ~60% faster than Python 3.9. Spark 3.5 brings Arrow-optimized UDFs, better streaming, and tighter pandas integration — all available for free just by switching the version.

---

## 2. A — Spark-Native Fine-Grained Access Control (Lake Formation)

### What it is
In Glue 4.0, you needed `DynamicFrame` to use Lake Formation permissions. Glue 5.0 lets you use **native Spark DataFrames and Spark SQL** with full table, column, row, and cell-level security.

### Simple Example

```python
# Glue 4.0 — forced to use DynamicFrame for Lake Formation (cumbersome)
dyf = glueContext.create_dynamic_frame.from_catalog(
    database="sales_db",
    table_name="orders"
    # No row/column filtering possible here
)

# -------------------------------------------------------

# Glue 5.0 — use plain Spark SQL, Lake Formation permissions apply automatically
df = spark.sql("SELECT order_id, amount FROM sales_db.orders")
# If the IAM role only has access to certain columns,
# Lake Formation automatically filters them — no extra code needed!
df.show()
```

```python
# Write with FGAC (new in Glue 5.0 — write permissions were NOT supported before)
spark.sql("""
    INSERT INTO sales_db.orders
    VALUES (1001, 'Alice', 500.00)
""")
# Lake Formation checks your write permissions automatically
```

**Real-world use:** A data analyst's Glue job should only see the `amount` column but not `customer_ssn`. In Glue 5.0, Lake Formation enforces that at the Spark layer — no custom filtering code required.

---

## 3. S — SageMaker Integration

### What it is
Glue 5.0 connects natively to two SageMaker services:
- **SageMaker Unified Studio** — run Glue 5.0 jobs directly from notebooks and visual ETL editor
- **SageMaker Lakehouse** — unified access to both S3 data lakes AND Redshift data warehouses in one query

### Simple Example

```python
# SageMaker Lakehouse — query S3 + Redshift in one Spark job
# No separate connectors needed

spark = SparkSession.builder \
    .appName("LakehouseQuery") \
    .config("spark.sql.defaultCatalog", "sagemaker_lakehouse") \
    .getOrCreate()

# This single query joins data from S3 data lake AND Redshift warehouse
result = spark.sql("""
    SELECT
        lake.customer_id,
        lake.purchase_date,       -- comes from S3 data lake
        wh.customer_segment       -- comes from Redshift warehouse
    FROM s3_catalog.sales.transactions AS lake
    JOIN redshift_catalog.crm.customers AS wh
      ON lake.customer_id = wh.customer_id
""")

result.show()
```

**Visual ETL — no code needed:**
```
SageMaker Unified Studio
  → Open Visual ETL Editor
  → Select Glue 5.0 as runtime
  → Drag-and-drop source → transform → target
  → Run directly from the browser
```

**Why it matters:** Before, you needed separate Glue jobs for S3 and separate Redshift connectors. Now one Lakehouse query handles both.

---

## 4. T — Table Format Upgrades (Iceberg, Delta Lake, Hudi)

### What it is
All three major open table formats are upgraded with significant new capabilities.

| Format | Glue 4.0 | Glue 5.0 | Key New Feature |
|--------|----------|----------|-----------------|
| Apache Iceberg | 1.0.0 | **1.7.1** | Branching, tagging, snapshot management |
| Delta Lake | 2.1.0 | **3.3.0** | Deletion vectors, optimised small-file writes |
| Apache Hudi | 0.12.1 | **0.15.0** | Record Level Index, faster upserts |

### Simple Example — Iceberg Branching (new in 1.7.1)

```python
# Iceberg branching — like Git branches, but for your data table!

# Create a "dev" branch to test changes safely
spark.sql("ALTER TABLE glue_catalog.db.orders CREATE BRANCH dev")

# Write experimental data to the dev branch only
spark.sql("""
    INSERT INTO glue_catalog.db.orders.branch_dev
    VALUES (9999, 'TEST_ORDER', 0.00)
""")

# Production branch is untouched — check it
spark.sql("SELECT COUNT(*) FROM glue_catalog.db.orders").show()  # No TEST row

# Once validated, merge dev into main
spark.sql("ALTER TABLE glue_catalog.db.orders FAST FORWARD BRANCH main TO BRANCH dev")
```

### Simple Example — Delta Lake Deletion Vectors

```python
# Delta Lake 3.3.0 — deletion vectors make DELETEs and UPDATEs much faster
# Instead of rewriting entire Parquet files, it just marks rows as deleted

spark.sql("""
    DELETE FROM delta.`s3://my-bucket/orders`
    WHERE order_status = 'CANCELLED'
""")
# Glue 5.0: marks rows with deletion vectors — 10× faster than rewriting files
# Glue 4.0: had to rewrite the entire Parquet file
```

---

## 5. E — Enhanced S3 (Access Grants + S3 Table Buckets)

### What it is
Two new S3 security and storage features:
- **S3 Access Grants** — grant fine-grained S3 permissions by bucket, prefix, or object
- **S3 Table Buckets** — dedicated bucket type optimised for Iceberg tables

### Simple Example — S3 Access Grants

```python
# Enable S3 Access Grants in your Glue job parameters:
# Key:   --conf
# Value: hadoop.fs.s3.s3AccessGrants.enabled=true
#        --conf spark.hadoop.fs.s3.s3AccessGrants.fallbackToIAM=false

# Once enabled, Access Grants permissions are enforced automatically
# No code changes needed — just add the config and Glue handles the rest

df = spark.read.parquet("s3://company-data/finance/2024/")
# If the running IAM role only has Access Grant for /finance/2024/
# it cannot accidentally read /finance/2023/ — even if IAM is broader
```

### Simple Example — S3 Table Buckets

```python
# S3 Table Buckets — purpose-built Iceberg storage, managed by AWS

spark = SparkSession.builder \
    .config("spark.sql.defaultCatalog", "s3tables") \
    .config("spark.sql.catalog.s3tables", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.s3tables.warehouse", "s3://my-table-bucket/warehouse/") \
    .getOrCreate()

# Create and use tables directly in S3 Table Bucket
spark.sql("""
    CREATE TABLE IF NOT EXISTS my_namespace.sales (
        id       INT,
        product  STRING,
        amount   DOUBLE
    )
""")

spark.sql("INSERT INTO my_namespace.sales VALUES (1, 'Laptop', 999.99)")
spark.sql("SELECT * FROM my_namespace.sales").show()
```

**Why it matters:** S3 Table Buckets auto-manage compaction, snapshots, and garbage collection — no cron jobs or maintenance scripts needed.

---

## 6. R — Requirements.txt Support

### What it is
Previously, installing Python libraries in Glue required messy `--additional-python-modules` parameters one by one. Glue 5.0 supports the standard `requirements.txt` file — just like any Python project.

### Simple Example

**Step 1 — Create requirements.txt and upload to S3:**
```
# requirements.txt
awswrangler==3.9.1
scikit-learn==1.5.2
SQLAlchemy==2.0.36
PyMySQL==1.1.1
PyYAML==6.0.2
```

```bash
aws s3 cp requirements.txt s3://my-glue-assets/requirements.txt
```

**Step 2 — Add two job parameters in AWS Console:**
```
Parameter 1:
  Key:   --python-modules-installer-option
  Value: -r

Parameter 2:
  Key:   --additional-python-modules
  Value: s3://my-glue-assets/requirements.txt
```

**Step 3 — Use the libraries directly in your job:**
```python
# Your Glue 5.0 job script
import awswrangler as wr
from sklearn.preprocessing import StandardScaler
import sqlalchemy

# All installed automatically from requirements.txt — no extra setup!
df = wr.s3.read_parquet("s3://my-bucket/data/")
scaler = StandardScaler()
```

**Before Glue 5.0 — cumbersome one-by-one:**
```
--additional-python-modules awswrangler==3.9.1,scikit-learn==1.5.2,SQLAlchemy==2.0.36,...
```

**Why it matters:** You can now reuse the same `requirements.txt` across local development, CI/CD, and Glue jobs — one source of truth for dependencies.

---

## 7. D — Data Lineage in Amazon DataZone

### What it is
Glue 5.0 automatically tracks **where data comes from and where it goes** during every Spark job run, and sends that lineage to Amazon DataZone for visualization — zero custom code needed.

### Simple Example

**Enable in AWS Console:**
```
Glue Job → Job Details tab
  ✅ Enable "Generate lineage events"
  Enter your DataZone Domain ID: dzd_xxxxxxxxxxxx
```

**Or via job parameter:**
```
Key:   --conf
Value: spark.extraListeners=io.openlineage.spark.agent.OpenLineageSparkListener
       --conf spark.openlineage.transport.type=amazon_datazone_api
       --conf spark.openlineage.transport.domainId=dzd_xxxxxxxxxxxx
```

**What gets tracked automatically:**
```
Your Glue Job runs this:
  spark.read.parquet("s3://raw/orders/")       ← tracked as INPUT
      .filter("amount > 100")                  ← transformation logged
      .write.parquet("s3://curated/orders/")   ← tracked as OUTPUT

DataZone shows:
  s3://raw/orders/ ──[Glue Job: CleanOrders]──► s3://curated/orders/
```

**Why it matters:** When something breaks in a downstream dashboard, lineage tells you instantly which upstream job and dataset caused it — no manual investigation.

---

## 8. L — Multi-Dialect Glue Data Catalog Views

### What it is
Create a **single view definition** in the Glue Data Catalog that can be queried by multiple engines (Spark, Athena, Redshift) without rewriting SQL for each one.

### Simple Example

```python
# Create a multi-dialect view — works across Spark, Athena, and Redshift
spark.sql("""
    CREATE PROTECTED MULTI DIALECT VIEW glue_catalog.sales_db.monthly_summary
    SECURITY DEFINER AS
    SELECT
        order_date,
        SUM(total_price) AS revenue,
        COUNT(*)         AS order_count
    FROM glue_catalog.sales_db.orders
    GROUP BY order_date
""")
```

```sql
-- Same view, queried from Athena (no SQL rewriting needed)
SELECT * FROM sales_db.monthly_summary WHERE order_date >= '2024-01-01';
```

```python
# Same view, queried from another Glue Spark job
df = spark.sql("SELECT * FROM glue_catalog.sales_db.monthly_summary")
df.show()
```

**Before Glue 5.0:** You had to maintain separate view definitions in Athena, Redshift, and Spark — three places to update every time the business logic changed.

**After Glue 5.0:** One view definition, governed centrally, works everywhere.

---

## Quick Reference Summary

| # | Feature | One-liner |
|---|---------|-----------|
| **F** | Framework Upgrades | Spark 3.5.4, Python 3.11, Java 17 — 32% faster, 22% cheaper |
| **A** | Lake Formation FGAC | Native Spark DataFrame/SQL with row, column, cell-level security |
| **S** | SageMaker Integration | Unified Studio notebooks + Lakehouse cross-S3/Redshift queries |
| **T** | Table Format Upgrades | Iceberg 1.7.1 branching, Delta 3.3.0 deletion vectors, Hudi 0.15.0 RLI |
| **E** | Enhanced S3 | S3 Access Grants (fine-grained) + S3 Table Buckets (managed Iceberg) |
| **R** | Requirements.txt | Standard Python dependency management — no more one-by-one installs |
| **D** | Data Lineage | Auto-track data flow and visualise in Amazon DataZone |
| **L** | Multi-Dialect Views | One view definition shared across Spark, Athena, and Redshift |

---

## Version Comparison Cheat Sheet

| Component | Glue 3.0 | Glue 4.0 | **Glue 5.0** |
|-----------|----------|----------|--------------|
| Spark | 3.1.1 | 3.3.0 | **3.5.4** |
| Python | 3.9 | 3.10 | **3.11** |
| Java | 8 | 8 | **17** |
| Iceberg | — | 1.0.0 | **1.7.1** |
| Delta Lake | — | 2.1.0 | **3.3.0** |
| Hudi | — | 0.12.1 | **0.15.0** |
| Lineage | ❌ | ❌ | **✅ DataZone** |
| FGAC via Spark | ❌ | ❌ | **✅ Native** |
| requirements.txt | ❌ | ❌ | **✅ Native** |
| Multi-dialect Views | ❌ | ❌ | **✅ Native** |

---

## 🧠 Mnemonic Recap

```
F — Framework Upgrades    (Spark 3.5.4, Python 3.11, Java 17)
A — Access Control        (Spark-native Lake Formation FGAC)
S — SageMaker Integration (Unified Studio + Lakehouse)
T — Table Formats         (Iceberg 1.7.1, Delta 3.3.0, Hudi 0.15.0)
E — Enhanced S3           (Access Grants + S3 Table Buckets)
R — Requirements.txt      (native Python dependency management)
D — Data Lineage          (auto-tracked, visualised in DataZone)
L — Lakehouse Views       (multi-dialect Data Catalog views)

👉 "Glue 5.0 runs FASTER DL (Data Lake)" 🚀
```

---

*AWS Glue 5.0 — Generally Available: December 2024 | Updated: March 2025*
*Available in all major AWS regions including ap-south-1 (Mumbai)*
