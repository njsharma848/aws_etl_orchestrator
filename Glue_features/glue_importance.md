# 🔗 AWS Glue: The Complete Guide

> *"The connective tissue of your data ecosystem"*

---

## 🧠 Master Mnemonic — Remember AWS Glue with **"GLUE CATS"**

```
G — Graph-based visual ETL (no-code pipeline builder)
L — Lake Formation integration (governance & security)
U — Unified catalog (Data Catalog = single metadata store)
E — Extract, Transform, Load (core ETL engine)

C — Crawlers (auto-discover schema from raw data)
A — Apache Spark under the hood (distributed compute)
T — Triggers & scheduling (orchestrate job runs)
S — Serverless (no infrastructure to manage)
```

> 💡 **GLUE CATS** — because Glue *sticks* your data together, and cats always land on their feet (serverless resilience!).

---

## 📖 What is AWS Glue?

AWS Glue is a **fully managed, serverless Extract-Transform-Load (ETL) service** provided by Amazon Web Services. It is designed to make it easy to prepare, move, and transform data for analytics, machine learning, and application development — **without provisioning or managing any infrastructure**.

Think of AWS Glue as the **universal adapter** in your data ecosystem. It can connect to dozens of data sources, understand their schemas automatically, and move/transform data to wherever it needs to go — all while you pay only for what you use.

```
┌─────────────────────────────────────────────────────────────────────┐
│                        AWS GLUE ECOSYSTEM                           │
│                                                                     │
│  ┌──────────┐    ┌──────────┐    ┌──────────┐    ┌──────────────┐  │
│  │ Data     │    │  Glue    │    │  Glue    │    │  Data        │  │
│  │ Sources  │───►│ Crawlers │───►│ ETL Jobs │───►│  Targets     │  │
│  │          │    │          │    │          │    │              │  │
│  │ S3, RDS  │    │ Schema   │    │ Spark /  │    │ S3, Redshift │  │
│  │ DynamoDB │    │ Discovery│    │ Python   │    │ RDS, Lake    │  │
│  │ JDBC, .. │    │          │    │ Ray      │    │ Formation    │  │
│  └──────────┘    └──────────┘    └──────────┘    └──────────────┘  │
│                        │                                            │
│                   ┌────▼─────┐                                      │
│                   │  Glue    │                                      │
│                   │  Data    │                                      │
│                   │ Catalog  │                                      │
│                   └──────────┘                                      │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 🏛️ Core Components of AWS Glue

### 1. 🗂️ AWS Glue Data Catalog

The **central metadata repository** for all your data assets. Think of it as the "table of contents" for your entire data lake.

- Stores table definitions, schemas, and data locations
- Acts as a **Hive-compatible metastore** (used by Athena, EMR, Redshift Spectrum)
- Automatically updated by Crawlers
- Organizes data into **Databases → Tables → Partitions**
- Supports schema versioning — track how your schema evolves over time

```
Data Catalog Structure:
├── Database: analytics_db
│   ├── Table: raw_orders          (points to s3://bucket/raw/orders/)
│   ├── Table: cleaned_customers   (points to s3://bucket/clean/customers/)
│   └── Table: product_catalog     (points to RDS MySQL)
└── Database: ml_features_db
    └── Table: user_features       (points to s3://bucket/features/)
```

---

### 2. 🕷️ AWS Glue Crawlers

**Automated schema discovery agents** that scan your data sources, infer schemas, and populate the Data Catalog — all without you writing any code.

**How a Crawler works:**

```
Step 1: You point a Crawler at a data source (S3, RDS, DynamoDB, etc.)
Step 2: Crawler samples the data and infers column names, data types, partitions
Step 3: Crawler writes the schema as a table definition into the Data Catalog
Step 4: Schedule it to re-run — it detects schema changes automatically
```

**Key Crawler behaviors:**
- Detects file formats: JSON, CSV, Parquet, ORC, Avro, XML
- Infers partition structures (e.g., `year=2024/month=01/day=15/`)
- Merges schemas when multiple files have slightly different columns
- Can crawl **multiple data stores** in a single crawler run
- Supports **incremental crawling** to only scan new/changed data

---

### 3. ⚙️ AWS Glue ETL Jobs

The **compute engine** that runs your data transformation logic. This is where the actual ETL work happens.

**Three types of Glue Jobs:**

| Job Type | Language | Best For |
|---|---|---|
| **Spark** | Python (PySpark) or Scala | Large-scale batch transformations |
| **Python Shell** | Python 3 | Lightweight scripts, small datasets |
| **Ray** | Python | Distributed ML workloads, data science |
| **Streaming** | PySpark Streaming | Real-time data processing |

---

### 4. 🔗 Glue Connections

Reusable **connection definitions** that store credentials and network config for data sources/targets (JDBC, S3, Kafka, MongoDB, etc.). Connections can be shared across multiple jobs.

---

### 5. ⏰ Glue Triggers

**Orchestration mechanisms** that control when and how Glue Jobs run:

| Trigger Type | Description |
|---|---|
| **On-Demand** | Manually start a job via console or API |
| **Scheduled** | Cron-based scheduling (e.g., every day at 2 AM) |
| **Event-Based** | Start when another job succeeds/fails |
| **EventBridge** | Trigger from any AWS event (S3 upload, SNS, etc.) |

---

### 6. 🎨 Glue Studio (Visual ETL)

A **drag-and-drop visual interface** to build ETL pipelines without writing code. You connect source → transform → target nodes on a canvas, and Glue generates the PySpark code automatically.

---

### 7. 🏗️ Glue DataBrew

A **visual data preparation tool** aimed at data analysts and non-engineers. Allows 250+ built-in data transformations (cleaning, normalization, pivoting) with zero code.

---

## 🔧 AWS Glue Job Parameters — In Deep Detail

This is the most critical section for engineers. Every Glue Job is controlled by parameters that determine its behavior, performance, and cost.

---

### 🧠 Mnemonic for Glue Job Parameters — **"SWIM WTF"**

```
S — Script location        (where your ETL code lives)
W — Worker type            (G.1X, G.2X, G.4X, G.8X, Z.2X)
I — IAM Role               (permissions to read/write data)
M — Max retries            (how many times to retry on failure)
W — Worker count           (number of parallel workers)
T — Timeout                (max job runtime before forced stop)
F — File format & output   (Parquet, JSON, CSV, ORC, Avro)
```

---

### 📋 Complete Parameter Reference

#### 1. 📄 `--script-location` / Script Path
The S3 path to your ETL script (`.py` for PySpark/Python Shell, `.scala` for Scala).

```
s3://my-glue-scripts/etl/transform_orders.py
```

This is the **entry point** of your Glue Job. All your transformation logic lives here.

---

#### 2. 👷 `--worker-type` — Worker Type

Defines the **compute capacity** of each individual worker node. This is one of the most impactful parameters for performance and cost.

| Worker Type | vCPUs | Memory | Disk | Best For |
|---|---|---|---|---|
| **G.1X** | 4 vCPUs | 16 GB | 64 GB | Standard ETL, memory-light jobs |
| **G.2X** | 8 vCPUs | 32 GB | 128 GB | Memory-intensive transforms, skewed data |
| **G.4X** | 16 vCPUs | 64 GB | 256 GB | Very large datasets, complex joins |
| **G.8X** | 32 vCPUs | 128 GB | 512 GB | Extremely large workloads |
| **Z.2X** | 8 vCPUs | 64 GB | 128 GB | ML workloads, Ray jobs |

> 💡 **Rule of thumb:** Start with `G.1X`. If you hit memory errors (OOM), upgrade to `G.2X`. Only go higher for datasets exceeding 100 GB with complex transformations.

---

#### 3. 🔢 `--number-of-workers`

The **total number of worker nodes** to allocate for the job. Combined with `worker-type`, this defines your total cluster size.

```
Total vCPUs  = number_of_workers × vCPUs_per_worker
Total Memory = number_of_workers × memory_per_worker
Glue DPUs    = number_of_workers × DPU_per_worker_type
```

**Example:**
- 10 workers × G.2X (8 vCPUs, 32 GB) = 80 vCPUs, 320 GB total memory

> ⚠️ **Minimum is 2 workers** (1 driver + 1 executor). Maximum varies by account quota.

---

#### 4. ⏱️ `--timeout`

Maximum time (in **minutes**) a job is allowed to run before AWS forcefully terminates it. Prevents runaway jobs from incurring infinite costs.

```
Default: 2880 minutes (48 hours)
Recommended: Set based on your expected runtime + 20% buffer
Example: If job typically takes 45 min → set timeout to 60 min
```

> ⚠️ When a job times out, it counts as a **FAILED** run and will trigger retries (if configured).

---

#### 5. 🔁 `--max-retries`

Number of **automatic retry attempts** if a job fails (not including the initial run).

```
Range: 0 to 10
Default: 0 (no retries)
```

**When to use retries:**
- Transient network failures (JDBC connection drops)
- Temporary S3 throttling
- Intermittent upstream data availability issues

**When NOT to use retries:**
- Script logic errors (retrying won't fix a bug)
- Insufficient memory (retrying same config will fail again)
- Idempotency concern — ensure your job handles re-runs safely

---

#### 6. 🛡️ `--role` — IAM Role

The **AWS IAM Role** that the Glue Job assumes during execution. This role must have permissions to:

```
✅ Read from source (S3 GetObject, RDS read access)
✅ Write to target (S3 PutObject, Redshift COPY)
✅ Access Glue Data Catalog (glue:GetTable, glue:UpdateTable)
✅ Write CloudWatch logs (logs:CreateLogGroup, logs:PutLogEvents)
✅ Access KMS keys (if data is encrypted)
✅ Use Glue Connections (ec2:DescribeVpcs for VPC-based sources)
```

> 🔒 **Security best practice:** Follow least-privilege. Create a dedicated role per job type rather than one super-role for all jobs.

---

#### 7. 🐍 `--glue-version`

The version of Apache Spark and Python runtime used by the job.

| Glue Version | Spark Version | Python | Key Features |
|---|---|---|---|
| **4.0** | Spark 3.3 | Python 3.10 | Latest, Ray support, best performance |
| **3.0** | Spark 3.1 | Python 3.7 | Stable, widely used |
| **2.0** | Spark 2.4 | Python 3.7 | Legacy, avoid for new jobs |

> ✅ **Always use Glue 4.0** for new jobs unless a specific library requires an older version.

---

#### 8. 📦 `--extra-py-files` / `--extra-jars`

Attach **custom Python libraries or JAR files** that your script depends on.

```
--extra-py-files     s3://my-bucket/libs/my_utils.zip,s3://my-bucket/libs/custom_lib.whl
--extra-jars         s3://my-bucket/jars/mysql-connector-java-8.0.jar
```

**Use cases:**
- Custom Python utility modules shared across jobs
- Third-party JDBC drivers not bundled with Glue
- Custom UDFs (User Defined Functions) packaged as JARs

---

#### 9. 🌐 `--connections`

Reference to a **Glue Connection** that stores JDBC/VPC/Kafka connection details.

```
--connections    my-rds-mysql-connection
```

When a connection is attached:
- Glue spins up an **Elastic Network Interface (ENI)** inside your VPC
- The job can access private RDS, Redshift, or self-hosted databases
- Credentials are fetched from the connection (stored in Glue, not hardcoded)

---

#### 10. 🔑 Custom Job Parameters (`--key value`)

You can pass **any arbitrary key-value parameter** to your Glue script, accessible via `getResolvedOptions()`:

```python
# In your Glue script:
import sys
from awsglue.utils import getResolvedOptions

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'source_bucket', 'target_path', 'run_date'])

source = args['source_bucket']     # s3://raw-data-bucket
target = args['target_path']       # s3://clean-data/orders/
run_date = args['run_date']        # 2024-03-15
```

This makes your jobs **reusable and environment-agnostic** — the same script can run for dev/staging/prod by passing different parameters.

---

#### 11. 📊 `--enable-metrics`

Enables **enhanced CloudWatch metrics** for monitoring job performance in detail.

Metrics captured:
- `glue.driver.aggregate.bytesRead` — data read volume
- `glue.ALL.s3.filesystem.write_bytes` — data written to S3
- `glue.driver.BlockManager.disk.diskSpaceUsed_MB` — disk usage
- `glue.ALL.jvm.heap.used` — JVM memory consumption
- Executor CPU usage, shuffle read/write stats

---

#### 12. 🧹 `--enable-auto-scaling`

When enabled, Glue **automatically scales workers up or down** based on the actual workload, instead of using a fixed number.

```
Without auto-scaling: Pay for 20 workers × 2 hours = 40 DPU-hours
With auto-scaling:    Glue uses 20 workers at peak, scales to 5 at end
                      Actual cost: ~22 DPU-hours (45% savings)
```

> ✅ **Highly recommended** for jobs with uneven workload distribution (most real-world ETL jobs).

---

#### 13. 📁 `--enable-continuous-cloudwatch-log`

Streams **real-time logs** to CloudWatch as the job runs, rather than waiting until completion.

```
Default:  Logs appear after job finishes
Enabled:  Logs stream live — great for debugging long-running jobs
```

---

#### 14. 🔖 `--job-bookmark`

**Job bookmarks** track which data has already been processed, enabling **incremental ETL** — only new or changed data is processed in subsequent runs.

| Bookmark Setting | Behavior |
|---|---|
| `job-bookmark-enable` | Track processed data, skip on re-run |
| `job-bookmark-disable` | Reprocess all data every run |
| `job-bookmark-pause` | Temporarily pause tracking (for re-processing) |

```
First run:  Processes 1,000,000 records → bookmark saved
Second run: Only processes 50,000 NEW records since last run
```

> 💡 **Critical for cost optimization.** Without bookmarks, every run reprocesses the entire dataset.

---

#### 15. 💾 `--TempDir`

S3 path where Glue stores **temporary intermediate files** during job execution (shuffle data, spill-to-disk, DynamicFrame conversions).

```
s3://my-glue-temp/temp-dir/
```

> ⚠️ Set a **lifecycle policy** on this S3 path to auto-delete files older than 7 days, or costs accumulate silently.

---

## ✅ Pros of AWS Glue

### 1. ☁️ Fully Serverless — Zero Infrastructure Management
There are no servers to provision, no clusters to size, no OS patches to apply. You provide the code; AWS handles everything else — cluster creation, scaling, tear-down, and maintenance. This removes entire categories of operational burden.

### 2. 💰 Pay-Per-Use Pricing Model
You are billed only for the **DPU-seconds** your job actually consumes. There is no charge when jobs aren't running. For sporadic or infrequent ETL workloads, this can be dramatically cheaper than maintaining a persistent cluster (like an always-on EMR cluster).

```
Cost = Number of DPUs × Duration (hours) × $0.44/DPU-hour
Example: 10 DPUs × 0.5 hours = $2.20 for a single job run
```

### 3. 🔍 Automatic Schema Discovery via Crawlers
Crawlers eliminate one of the most tedious parts of data engineering — manually documenting and maintaining schema definitions. Just point a Crawler at your S3 bucket or database, and Glue figures out column names, types, and partitions automatically.

### 4. 🔗 Native AWS Integration
Glue integrates deeply with the AWS ecosystem out of the box:
- **S3** — primary data lake storage
- **Redshift** — direct COPY/UNLOAD integration
- **Athena** — uses Glue Catalog as its metastore
- **Lake Formation** — fine-grained access control
- **EMR** — shared metastore
- **EventBridge** — event-driven triggering
- **CloudWatch** — logging and monitoring
- **Secrets Manager** — secure credential storage

### 5. 📈 Auto-Scaling Workers
With `--enable-auto-scaling`, Glue dynamically adjusts the number of workers based on workload. This prevents both over-provisioning (wasting money) and under-provisioning (causing slowdowns).

### 6. 🎨 Visual ETL with Glue Studio
Non-engineers and data analysts can build ETL pipelines using the drag-and-drop Glue Studio interface without writing any code. Glue auto-generates the PySpark script, which developers can then customize further.

### 7. 🔖 Job Bookmarks for Incremental Processing
Built-in checkpoint mechanism ensures only new data is processed, making pipelines efficient and cost-effective at scale without writing custom state management code.

### 8. 🔒 Security & Compliance Built-In
- Encryption at rest and in transit
- VPC support for accessing private data sources
- Lake Formation integration for column-level and row-level security
- IAM-based access control
- CloudTrail audit logging for every API call

### 9. 🛠️ Flexible Language Support
Write ETL logic in **PySpark, Scala, or Python Shell**. Data scientists can use familiar Python; data engineers can leverage the full Spark ecosystem.

### 10. 📚 Glue Data Catalog as Universal Metastore
A single catalog serves Glue, Athena, EMR, Redshift Spectrum, and Lake Formation — eliminating metadata silos and providing one consistent view of all data assets.

---

## ❌ Cons of AWS Glue

### 1. ⏳ Cold Start Latency
Every Glue Job incurs a **startup time of 1–3 minutes** before any actual processing begins. Glue must provision the Spark cluster, download dependencies, and initialize the execution environment. This makes Glue unsuitable for sub-minute latency requirements.

```
Job submitted → [2 min cold start] → Actual ETL begins → Job completes
```

For a job that processes 10 seconds of data, the 2-minute overhead is significant.

### 2. 💸 Can Be Expensive for Always-On Workloads
While pay-per-use is great for infrequent jobs, for **continuous or high-frequency pipelines**, a persistent Spark cluster (e.g., EMR, Databricks) may be far cheaper. The DPU-hour model adds up quickly for 24/7 streaming workloads.

### 3. 🐛 Debugging Difficulty
Glue abstracts away the underlying infrastructure, which makes debugging harder:
- Log latency (without continuous CloudWatch logging enabled)
- Complex Spark errors can be cryptic
- Local testing is difficult — no perfect local Glue emulator
- The `DynamicFrame` abstraction adds another layer of complexity over standard Spark DataFrames

### 4. 🔒 Vendor Lock-In
Glue uses AWS-specific APIs (`DynamicFrame`, `GlueContext`, `getResolvedOptions`), the Glue Data Catalog, and Glue-specific parameters. Migrating to another platform (Azure, GCP, on-prem) requires significant code rewriting.

### 5. 📉 Limited Control Over Spark Configuration
Unlike a self-managed Spark cluster or EMR, you cannot fine-tune deep Spark parameters (garbage collection settings, custom memory fractions, executor configurations). You work within Glue's predefined worker types.

### 6. 🕷️ Crawler Inaccuracies
Crawlers sometimes infer incorrect data types (e.g., treating a numeric column as `string` because of one malformed record). In complex schemas, crawler output needs manual correction and ongoing maintenance.

### 7. 📦 Dependency Management Complexity
Installing custom Python libraries in Glue is more complex than in a local environment. You must:
- Package libraries as `.whl` or `.zip` files
- Upload to S3
- Reference via `--extra-py-files`
- Or use Glue's limited pre-bundled library set

This adds friction compared to simply running `pip install` on a VM.

### 8. 🌐 VPC Configuration Complexity
Connecting Glue to private data sources (RDS, Redshift in a VPC) requires proper VPC configuration, security groups, NAT gateways, and Glue Connections. Misconfiguration is a common source of failures that can be hard to diagnose.

### 9. 🧩 Not Ideal for Very Small or Very Large Jobs
- **Too small:** Python Shell jobs have a 1 DPU minimum. Lambda or Step Functions may be cheaper for tiny transformations.
- **Too large:** At petabyte scale with complex ML workloads, Databricks or EMR with fine-tuned Spark configs may outperform Glue.

### 10. 🔄 Limited Real-Time Streaming Support
Glue Streaming (Spark Structured Streaming) has a minimum **100-second micro-batch interval**. For true sub-second streaming, you need Kinesis Data Analytics (Flink) or Kafka Streams.

---

## ⚖️ Pros vs Cons — Quick Reference

| Dimension | ✅ Pro | ❌ Con |
|---|---|---|
| **Infrastructure** | Fully serverless | No deep Spark tuning |
| **Cost** | Pay-per-use | Expensive for always-on |
| **Onboarding** | Visual ETL, no-code option | Cold start 1–3 min |
| **Integration** | Deep AWS native | Vendor lock-in |
| **Schema** | Auto-discovery via Crawlers | Crawler can misclassify types |
| **Incremental** | Job bookmarks built-in | Bookmark can be tricky to reset |
| **Libraries** | Flexible language support | Dependency packaging friction |
| **Streaming** | Supports micro-batch | No sub-second latency |
| **Security** | Lake Formation, VPC, KMS | VPC setup is complex |
| **Scale** | Auto-scaling workers | Not ideal at petabyte+ scale |

---

## 🏆 Importance of AWS Glue

### 1. 🏗️ Foundation of the AWS Data Lake Architecture
AWS Glue is the **official ETL backbone** of the AWS Modern Data Strategy. In the reference architecture — S3 Data Lake → Glue ETL → Redshift/Athena → QuickSight — Glue is the critical transformation layer that makes raw data usable.

### 2. 🔍 Enables the Serverless Analytics Stack
Athena cannot query data efficiently without a well-maintained Glue Data Catalog. Glue Crawlers keep the catalog current, making **serverless SQL-on-S3** possible at scale without any database administration.

### 3. 📐 Democratizes Data Engineering
With Glue Studio's visual interface and DataBrew's no-code transformations, organizations can empower **data analysts and business users** to build pipelines without writing Spark code — dramatically reducing the bottleneck on data engineering teams.

### 4. 🔒 Enables Enterprise Data Governance
When paired with **AWS Lake Formation**, Glue becomes the ETL engine within a fully governed data lake — complete with column-level security, row-level filtering, data lineage, and centralized audit logs. This is critical for GDPR, HIPAA, and SOC 2 compliance.

### 5. 🤝 Glue as the Universal Translator
Glue can read from **50+ data sources** and write to **30+ targets**. It acts as the universal translator between legacy operational databases and modern cloud analytics platforms — reducing integration effort that would otherwise require custom engineering for every source-target pair.

### 6. 💡 Cost Efficiency at Scale
For batch ETL workloads that run a few times per day, Glue's serverless model eliminates the cost of idle cluster time that plagues persistent Spark deployments. Organizations routinely report **40–70% cost reduction** over always-on EMR clusters for comparable batch workloads.

### 7. 🧠 Enabling ML Pipelines
Glue handles the **data preparation phase** of ML pipelines — cleaning, joining, feature engineering — at scale, feeding clean data into SageMaker for model training. This integration makes end-to-end ML pipelines on AWS seamless.

---

## 🗺️ AWS Glue vs. Alternatives

| Feature | AWS Glue | Databricks | Apache Airflow + EMR | AWS Lambda |
|---|---|---|---|---|
| **Serverless** | ✅ Yes | ❌ Cluster-based | ❌ Managed infra | ✅ Yes |
| **Spark Native** | ✅ Yes | ✅ Yes (optimized) | ✅ Yes | ❌ No |
| **Visual ETL** | ✅ Glue Studio | ✅ Notebooks | ❌ Limited | ❌ No |
| **Streaming** | ⚠️ Micro-batch | ✅ Full streaming | ✅ Full streaming | ⚠️ Limited |
| **ML Support** | ⚠️ Basic | ✅ MLflow native | ⚠️ Manual | ❌ No |
| **Cold Start** | ❌ 1–3 min | ⚠️ Cluster startup | ❌ Slow | ✅ Sub-second |
| **Cost Model** | DPU-hours | DBU-hours | EC2 + EMR hours | Per invocation |
| **AWS Native** | ✅ Deep | ⚠️ Partial | ⚠️ Partial | ✅ Deep |

---

## 💻 Quick Code Example — A Complete Glue ETL Job

```python
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, to_date, upper, trim

# ── 1. Initialize Glue Context ───────────────────────────────────────
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'source_database',
    'source_table',
    'target_path',
    'run_date'
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# ── 2. Read from Glue Data Catalog (Bronze Layer) ───────────────────
raw_dyf = glueContext.create_dynamic_frame.from_catalog(
    database=args['source_database'],
    table_name=args['source_table'],
    transformation_ctx="raw_dyf"           # Job bookmark key
)

# ── 3. Convert to Spark DataFrame for rich transformations ───────────
raw_df = raw_dyf.toDF()

# ── 4. Transform — Clean & Enrich (Silver Layer logic) ──────────────
cleaned_df = (
    raw_df
    .filter(col("order_id").isNotNull())            # Drop nulls
    .dropDuplicates(["order_id"])                   # Dedup
    .withColumn("customer_name", upper(trim(col("customer_name"))))
    .withColumn("order_date", to_date(col("order_ts"), "yyyy-MM-dd"))
    .withColumn("amount", col("amount").cast("double"))
    .filter(col("amount") > 0)                      # Remove invalid amounts
)

# ── 5. Write to S3 as Parquet (partitioned by date) ─────────────────
glueContext.write_dynamic_frame.from_options(
    frame=DynamicFrame.fromDF(cleaned_df, glueContext, "cleaned_df"),
    connection_type="s3",
    connection_options={
        "path": args['target_path'],
        "partitionKeys": ["order_date"]
    },
    format="glueparquet",
    format_options={"compression": "snappy"},
    transformation_ctx="write_cleaned"
)

# ── 6. Commit job bookmark — mark data as processed ─────────────────
job.commit()

print(f"✅ Job complete. Processed {cleaned_df.count()} records.")
```

---

## 🗂️ All Parameters — Master Cheat Sheet

```
┌─────────────────────────────────────────────────────────────────────┐
│                   GLUE JOB PARAMETERS CHEAT SHEET                  │
├──────────────────────────┬──────────────────────────────────────────┤
│ Parameter                │ Purpose                                  │
├──────────────────────────┼──────────────────────────────────────────┤
│ --script-location        │ S3 path to your ETL script               │
│ --worker-type            │ G.1X / G.2X / G.4X / G.8X / Z.2X        │
│ --number-of-workers      │ Total worker nodes (min: 2)              │
│ --timeout                │ Max runtime in minutes                   │
│ --max-retries            │ Auto-retry count on failure (0–10)       │
│ --role                   │ IAM role ARN for job execution           │
│ --glue-version           │ Spark/Python runtime (use 4.0)           │
│ --extra-py-files         │ Custom Python libs (.zip/.whl) on S3     │
│ --extra-jars             │ Custom JARs (JDBC drivers, UDFs)         │
│ --connections            │ Glue Connection name for JDBC/VPC        │
│ --TempDir                │ S3 path for temporary shuffle files      │
│ --enable-metrics         │ Enhanced CloudWatch metrics              │
│ --enable-auto-scaling    │ Dynamic worker scaling                   │
│ --enable-continuous-     │ Stream logs in real-time to CloudWatch   │
│   cloudwatch-log         │                                          │
│ --job-bookmark           │ enable / disable / pause                 │
│ --{custom-key}           │ Any user-defined parameter               │
└──────────────────────────┴──────────────────────────────────────────┘
```

---

## 🧠 Final Mnemonic Recap

### Remember the 3 layers of Glue with **"CDC"**
```
C — Catalog    (store metadata about your data)
D — Discovery  (Crawlers discover schema automatically)
C — Compute    (ETL Jobs do the actual transformation)
```

### Remember Glue Job Parameters with **"SWIM WTF"**
```
S — Script location
W — Worker type
I — IAM Role
M — Max retries
W — Worker count (number-of-workers)
T — Timeout
F — Format & extra files (extra-py-files, TempDir, format)
```

### Remember when to use Glue vs alternatives with **"BATCH"**
```
B — Batch workloads          → ✅ Use Glue
A — Always-on pipelines      → ❌ Use Databricks / EMR
T — Tiny scripts             → ❌ Use Lambda
C — Complex ML               → ❌ Use Databricks
H — Hybrid (batch + catalog) → ✅ Use Glue
```

---

## 📝 Summary

AWS Glue is the **cornerstone serverless ETL service** on AWS. It excels at batch data integration, automatic schema discovery, incremental processing, and serving as the metadata backbone for the entire AWS analytics stack.

Its serverless nature removes infrastructure burden, its Crawlers automate schema management, its job bookmarks enable efficient incremental loads, and its deep integration with S3, Athena, Redshift, and Lake Formation make it the natural choice for AWS-native data platforms.

However, it is not a silver bullet. For low-latency pipelines, petabyte-scale ML workloads, or complex real-time streaming, purpose-built tools like Databricks, Flink, or EMR may serve better.

**The ideal scenario for Glue:** A well-governed AWS data lake with multiple batch ETL pipelines running daily, feeding an analytics layer powered by Athena or Redshift — exactly the architecture used by thousands of enterprises on AWS today.

---

*Last updated: March 2026 | Technologies: AWS Glue 4.0, Apache Spark 3.3, Python 3.10, Delta Lake, AWS Lake Formation*
