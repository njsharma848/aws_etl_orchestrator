# AWS Architecture Interview Q&A: ETL Orchestrator
## MAANG/FAANG Level

---

## Table of Contents

1. [Why SQS FIFO over Standard SQS or EventBridge Pipes?](#1-why-sqs-fifo-over-standard-sqs-or-eventbridge-pipes-for-buffering-s3-events)
2. [Why Lambda (concurrency = 1) as the orchestrator?](#2-why-lambda-concurrency--1-as-the-orchestrator-instead-of-running-glue-directly-from-eventbridge)
3. [Why Step Functions over Lambda-native Glue polling?](#3-why-step-functions-over-lambda-native-glue-polling-for-job-orchestration)
4. [Why Redshift Serverless over provisioned Redshift or alternatives?](#4-why-redshift-serverless-over-provisioned-redshift-or-alternatives-like-athenasnoflake)
5. [Why delete-then-insert MERGE pattern?](#5-why-use-the-delete-then-insert-merge-pattern-instead-of-native-redshift-merge)
6. [Why EventBridge over S3 Event Notifications?](#6-why-eventbridge-as-the-s3-event-detector-instead-of-s3-event-notifications-directly-to-sqs)
7. [Why store Glue scripts in S3?](#7-why-store-glue-scripts-and-configs-in-s3-rather-than-packaging-them-in-the-terraform-deployment)
8. [How does the pipeline handle schema drift?](#8-how-does-the-pipeline-handle-schema-drift-and-why-is-this-operationally-important)
9. [Why Secrets Manager over IAM database authentication?](#9-why-secrets-manager-for-redshift-credentials-instead-of-iam-database-authentication)
10. [How would you scale this architecture to 10x volume?](#10-how-would-you-scale-this-architecture-to-handle-10x-the-current-file-volume)

---

## 1. Why SQS FIFO over Standard SQS or EventBridge Pipes for buffering S3 events?

**Answer:**

The core requirement is **ordered, exactly-once delivery** to a serial Lambda consumer. Each choice fails at least one of those:

| Queue Type | Ordering | Deduplication | Throughput | Cost |
|---|---|---|---|---|
| **SQS FIFO** ✅ | Strict (MessageGroupId) | Content-based, native | 3,000 msg/s (batched) | ~$0.50/M msgs |
| Standard SQS | Best-effort only | None native | Nearly unlimited | ~$0.40/M msgs |
| EventBridge Pipes | No buffering | None | Limited | Higher |
| Kinesis | Ordered per shard | None | High | Per-shard cost |

**Why FIFO wins here:** If `Orders.csv` and `OrderItems.csv` arrive simultaneously, `OrderItems` has a foreign key dependency on `Orders`. Standard SQS could process `OrderItems` first, causing a referential integrity failure in Redshift. FIFO guarantees `Orders` is processed first.

**The tradeoff you must acknowledge:** FIFO's 3,000 msg/s limit is irrelevant at CSV-file granularity (one message per file), but would be a disqualifier for event-per-row streaming pipelines.

---

## 2. Why Lambda (concurrency = 1) as the orchestrator instead of running Glue directly from EventBridge?

**Answer:**

EventBridge → Glue directly is technically possible but breaks three things:

**Problem 1 — No config matching logic.**
EventBridge rules are pattern-based, not data-aware. You cannot read `config.json` from S3, match the incoming filename, and dynamically select a Glue job without compute in the middle.

**Problem 2 — No concurrency control.**
Without `reserved_concurrency = 1` on Lambda, two S3 events arriving 50ms apart could trigger two simultaneous Glue jobs writing to the same Redshift table. Redshift's MVCC handles concurrent reads well, but concurrent `DELETE + INSERT` merges on the same target table produce deadlocks or dirty data.

**Problem 3 — No dead-letter handling.**
Lambda + SQS gives you `maxReceiveCount = 3` before DLQ. EventBridge → Glue gives you a retry but no structured failure queue.

**Why not use Step Functions as the entry point?**
Step Functions has no native SQS trigger. Lambda is the glue (no pun intended) between SQS and Step Functions.

> **FAANG follow-up you'll get:**
> *"What happens if Lambda crashes after starting Step Functions but before deleting the SQS message?"*
>
> **Answer:** SQS visibility timeout (360s) expires, message reappears. Lambda re-invokes. Step Functions `startExecution` with the same `name` parameter returns the existing execution ARN instead of starting a duplicate — idempotency is handled at the Step Functions layer.

---

## 3. Why Step Functions over Lambda-native Glue polling for job orchestration?

**Answer:**

The naive alternative is Lambda calling `glue.start_job_run()`, then polling with `time.sleep()`. This fails for a concrete reason:

**Lambda max timeout is 15 minutes. Glue jobs on large datasets run 30–90 minutes.**

Step Functions `.sync` integration solves this with an **event-driven wait** — it registers a callback with Glue and suspends the execution at zero cost until Glue sends a task token back. No polling, no timeout risk.

| Approach | Max Wait | Cost During Wait | Failure Handling |
|---|---|---|---|
| Lambda polling | 15 min hard limit | Charged per 100ms | Manual try/catch |
| **Step Functions .sync** ✅ | 1 year | $0 (no state transitions) | Built-in retry/catch |
| EventBridge scheduled check | Unlimited | Minimal | Complex state mgmt |

**Additional benefit:** Step Functions provides a **visual audit trail** in the AWS console. For a MAANG-level ops team, being able to see exactly which state a pipeline is in, replay failed executions, and query execution history via API is operationally critical — something you cannot get from Lambda logs alone.

---

## 4. Why Redshift Serverless over provisioned Redshift or alternatives like Athena/Snowflake?

**Answer:**

This is a three-way comparison that interviewers love:

### Redshift Serverless vs. Provisioned Redshift

Provisioned requires you to choose a node type (ra3.xlplus at ~$1.086/node/hr) and manage cluster sizing. For an ETL pipeline with **bursty, unpredictable load** (files arrive sporadically), you'd be paying for idle capacity 80% of the time. Serverless scales to zero between jobs and bills per RPU-hour (Redshift Processing Unit), making it cost-optimal for this pattern.

### Redshift vs. Athena

Athena is S3-native and schema-on-read — excellent for ad-hoc queries but has **no support for stored procedures, transactions, or MERGE statements**. The entire SCD Type 1/2 implementation relies on `BEGIN TRANSACTION / COMMIT` and complex stored procedures. Athena cannot run these. Redshift's PostgreSQL-compatible dialect supports all of them natively.

### Redshift vs. Snowflake

Snowflake would work architecturally, but introduces a **cross-cloud dependency** (Snowflake is not an AWS-native service). For a pipeline where every other component (Glue, Lambda, Step Functions, S3) is AWS-native, adding Snowflake means separate IAM patterns, separate credential management, separate networking, and additional cost. For an AWS-first organization, Redshift keeps everything inside one security boundary.

---

## 5. Why use the delete-then-insert MERGE pattern instead of native Redshift MERGE?

**Answer:**

Redshift added `MERGE` syntax in 2022, but the stored procedure uses an explicit `DELETE + INSERT` pattern. There are two legitimate reasons:

**Reason 1 — Multi-key upsert flexibility.**
Native `MERGE` requires a single `ON` condition. The pipeline supports composite `upsert_keys` like `["product_id", "time"]`. The stored procedure dynamically constructs the `WHERE` clause from a JSON array at runtime, which is awkward to express in static `MERGE` syntax while maintaining a config-driven, generic procedure.

**Reason 2 — Staging table control.**
The delete-then-insert pattern uses an explicit staging table that is dropped after commit. This gives you a **rollback point** — if the `INSERT` fails after the `DELETE`, the transaction rolls back and the target table is unchanged. With native `MERGE` as a single statement, you lose the intermediate inspection point that's useful for debugging data issues.

```sql
BEGIN TRANSACTION;
    DELETE FROM target WHERE upsert_keys IN (SELECT upsert_keys FROM staging);
    INSERT INTO target SELECT * FROM staging;
    DROP TABLE staging;
COMMIT;
```

**The tradeoff to acknowledge:** DELETE + INSERT on large tables causes significant dead tuple bloat in Redshift, which is why `sp_vacuum_table` exists in `08_table_maintenance.sql`. Native MERGE has lower write amplification. For tables exceeding ~100M rows, you'd want to benchmark both.

---

## 6. Why EventBridge as the S3 event detector instead of S3 Event Notifications directly to SQS?

**Answer:**

S3 can notify SQS directly — so why add EventBridge in the middle?

**Filtering capability:**
S3 native notifications support only prefix/suffix filters. EventBridge supports **content-based filtering** on any event field using patterns, including `detail.object.key` matching with wildcards. If you later need to route `data/in/finance/*.csv` to one queue and `data/in/ops/*.csv` to another, EventBridge handles this without any code change. S3 native notifications would require two separate notification configurations and two queues.

**Fan-out capability:**
EventBridge can send the same S3 event to SQS *and* CloudWatch *and* a Lambda for monitoring simultaneously. S3 native notifications support only one destination per event type.

**Audit trail:**
EventBridge logs all matched events to CloudTrail. This gives you a queryable record of every file that triggered the pipeline — operationally invaluable for debugging "why didn't this file get processed?"

**The honest tradeoff:** EventBridge adds ~1-2 seconds of latency and marginal cost ($1/million events). For near-real-time requirements, S3 → SQS direct is faster and simpler.

---

## 7. Why store Glue scripts and configs in S3 rather than packaging them in the Terraform deployment?

**Answer:**

This is an **operational velocity vs. deployment coupling** decision.

If Glue scripts were bundled into the Terraform module, every script change would require a `terraform apply` cycle — roughly 3–8 minutes including plan, approval gates, and state lock acquisition. More critically, in a production environment with required approvals on Terraform runs, a hotfix to a Glue transformation would be blocked behind the same approval process as infrastructure changes.

By storing scripts in S3 separately:

- **Glue always reads the latest script at job start time** — no Glue job update needed
- **Config changes** (`config.json` additions for new files) are a 10-second `aws s3 cp` with zero infrastructure risk
- **Rollback** is `aws s3 cp` of a previous version — S3 versioning is enabled on the bucket

**The risk this introduces:** Script and infrastructure can drift out of sync. A Glue job definition might reference a script path that no longer exists. Mitigation: the CI pipeline validates that all script paths referenced in Terraform variables exist in S3 before deploying.

---

## 8. How does the pipeline handle schema drift, and why is this operationally important?

**Answer:**

Schema drift — source files gaining, losing, or changing columns — is the **most common cause of ETL pipeline failures in production**. Most pipelines fail with a column mismatch error and require manual intervention.

This pipeline handles four drift scenarios automatically:

| Scenario | Detection | Action | Risk |
|---|---|---|---|
| New column in source | Compare source schema vs Redshift catalog | `ALTER TABLE ADD COLUMN` | Low — additive change |
| Missing column in source | Compare source schema vs Redshift catalog | Fill with type default (NULL/0) | Medium — silent data gap |
| VARCHAR too short | Compare max string length vs column length | `ALTER COLUMN TYPE VARCHAR(n)` | Low — Redshift allows online resize |
| Integer overflow | Detect values exceeding INT max (2,147,483,647) | `ALTER COLUMN TYPE BIGINT` | Low — widening cast is safe |

**Why this matters at FAANG scale:** A source team changing a column type in their upstream system at 2am should not page your on-call engineer. The pipeline self-heals and logs the schema change to the audit table, which the data team reviews in the morning.

**What it deliberately does NOT handle:** Column renames and column deletions. A rename looks like "old column disappeared + new column appeared," which would result in the old column being NULL-filled and a new column being added — preserving data but losing the semantic link. This requires human review, which is the correct behavior for a destructive schema change.

---

## 9. Why Secrets Manager for Redshift credentials instead of IAM database authentication?

**Answer:**

Redshift supports both, but they serve different access patterns:

### IAM Authentication

Works by exchanging an IAM role for a temporary database password (15-minute TTL). It is ideal for **human users and interactive query tools** (e.g., a data analyst connecting via SQL Workbench with their IAM identity). The credential is ephemeral and tied to a human identity — perfect for auditability of who ran which query.

### Secrets Manager (chosen approach)

Better for **service-to-service authentication** in this pipeline because:

1. Glue jobs need a stable, always-valid credential. IAM temp passwords expire mid-job on long-running transforms.
2. The Glue job uses the JDBC connection string format: `jdbc:redshift://host:5439/db?user=X&password=Y`. Secrets Manager provides these as a JSON secret that Glue fetches at job start.
3. Secrets Manager **auto-rotates** the password on a configurable schedule without any code change — the new secret value is fetched fresh on each job run.

**The alternative worth mentioning:** AWS Glue now supports Glue Connections with IAM-based Redshift auth. For a greenfield design today, this is worth evaluating as it eliminates secret storage entirely. The stored credential approach here reflects a common pragmatic choice given Glue JDBC driver compatibility requirements.

---

## 10. How would you scale this architecture to handle 10x the current file volume?

**Answer:**

This is a **design extension question** — the right answer identifies bottlenecks in order of likelihood:

### Bottleneck 1 — Lambda concurrency = 1 (hits first)

The serial constraint becomes a queue depth problem. At 10x volume, SQS message backlog grows faster than Lambda can drain it.

**Solution:** Introduce **file-type-based message group IDs** in SQS FIFO. Files targeting different Redshift tables have no merge conflict, so they can run in parallel. Concurrency becomes "1 per target table" rather than "1 globally." Lambda reserved concurrency increases proportionally.

### Bottleneck 2 — Glue job startup latency (~2–3 minutes per job)

At high volume, Glue's cold start dominates throughput.

**Solution:** Switch to **Glue Streaming** or **Glue job bookmarks with file batching** — accumulate N files and process them in one job run rather than one job per file.

### Bottleneck 3 — Redshift Serverless RPU limits

Serverless has a configurable max RPU cap. At 10x concurrent merges, you may hit this.

**Solution:** Increase max RPU cap (immediate) or evaluate **Redshift provisioned ra3 nodes** if the workload is consistently high (better price/performance at sustained load above ~8 hours/day).

### Bottleneck 4 — S3 request rate

S3 supports 3,500 PUT/s per prefix — almost never the bottleneck for CSV file ingestion, but worth noting for the interviewer that you've considered it.

> **The key FAANG-level signal here** is identifying bottlenecks **in order** and proposing targeted solutions rather than immediately suggesting "rewrite in Kafka."

---

## Quick Reference: Service Selection Rationale

| Decision | Service Chosen | Primary Reason | Key Alternative Rejected |

| Event buffering | SQS FIFO | Ordered, exactly-once delivery | Standard SQS (no ordering) |
| Orchestration entry | Lambda (concurrency=1) | Config matching + serial control | EventBridge → Glue direct (no logic layer) |
| Job wait | Step Functions .sync | 15-min Lambda timeout bypass | Lambda polling (hits timeout) |
| Data warehouse | Redshift Serverless | Transactions + stored procedures + AWS-native | Athena (no ACID), Snowflake (cross-cloud) |
| Merge strategy | DELETE + INSERT | Dynamic composite keys + rollback point | Native MERGE (static ON clause) |
| S3 event detection | EventBridge | Content-based filtering + fan-out | S3 native notifications (prefix/suffix only) |
| Credential management | Secrets Manager | Long-running job stability + auto-rotation | IAM DB auth (15-min TTL breaks Glue) |
