# AWS ETL Orchestrator — Project-Specific KPIs

> **Mnemonic: R.O.L.A.C.** — *"Really Outstanding Latency And Cost"*
> Five dimensions every production data platform must win: **R**eliability · **O**verhead · **L**atency · **A**uditability · **C**ost

---

## KPI Overview

| # | Letter | KPI | Before | After | Improvement |
|---|--------|-----|--------|-------|-------------|
| 1 | **R** | Pipeline SLA — % of S3 files processed into Redshift within 30 min | ~80% on-time | **99.9% SLA** | +20pp; zero silent drops |
| 2 | **O** | Analyst time spent on manual pipeline ops per week | ~14 hrs/week (2–3 hrs/day) | **<15 min/week** | ~90% reduction |
| 3 | **L** | Data freshness — time from S3 file arrival to Redshift queryability | T+24 hrs (nightly batch) | **T+30 minutes** | 96% latency reduction |
| 4 | **A** | % of data loads covered by automated DQ checks with permanent audit record | 0% (ad-hoc manual) | **100% every load** | Full regulatory audit trail |
| 5 | **C** | Infrastructure spend — serverless vs always-on fixed cluster | Fixed: RDS + EMR + EC2 24/7 | **~65% cost reduction** | Near-zero off-peak idle cost |

---

## KPI 1 — R: Pipeline Reliability

**Metric:** % of source CSV files successfully processed and landed in Redshift **within 30 minutes** of arriving in `s3://bucket/data/in/`.

**Before:** ~80% on-time delivery. Bespoke batch scripts ran on a schedule, silently failed with no retry, no alerting, and no dead-letter capture. Failures were discovered hours later through downstream data quality complaints.

**After:** 99.9% SLA. Every file is processed within the 30-minute window. Zero messages are silently dropped.

### Specific mechanisms that drove this number

- **SQS FIFO + Dead Letter Queue** — Lambda retries failed messages up to 3 times automatically. After 3 failures the message parks in the DLQ with a 14-day retention window. Nothing is ever silently lost.
- **Step Functions `glue:startJobRun.sync`** — The orchestrator *waits* for the Glue job to reach a terminal state before marking the step successful. No fire-and-forget; failure surfaces immediately in the execution graph.
- **Exponential backoff on Redshift API calls** — Transient failures (network blips, Redshift Serverless cold starts) are retried with `base_delay × 2^(attempt-1)`, capped at 120 seconds. The job heals itself without human intervention.
- **Lambda reserved concurrency = 1** — Enforces serial processing of files. Prevents two Lambda invocations from running concurrent `MERGE` statements against the same Redshift target table, which would cause lock conflicts and data corruption.

### Interview framing

> "We moved from a best-effort batch script that would silently fail — and that we'd only discover had failed when a fund manager noticed stale data — to an architecture where every failure is captured, retried automatically, and surfaced via SNS alerts. The on-time SLA improved from approximately 80% to 99.9%."

---

## KPI 2 — O: Operational Overhead

**Metric:** Engineer and analyst hours spent per week on pipeline monitoring, failure diagnosis, manual reruns, and new feed onboarding.

**Before:** 2–3 analyst hours per day (~14 hrs/week). An analyst manually checked the S3 landing folder each morning, triggered scripts by hand, and diagnosed failures by trawling raw CloudWatch logs. Adding a new data feed required 1–2 days of developer work: write a new script, test it, deploy it, and document it.

**After:** Less than 15 minutes per week of alert triage. New data feed onboarding takes 15 minutes.

### Specific mechanisms that drove this number

- **Config-driven job routing (`config.json`)** — The Glue job reads a single JSON config at runtime to determine source schema, target table, transformation rules, SCD type, and load strategy. Adding a new upstream feed = editing one JSON file. No code deployment, no infrastructure change.
- **Automated schema evolution in Glue** — At runtime the job introspects the source CSV headers against the Redshift catalog. New columns trigger `ALTER TABLE ... ADD COLUMN`. VARCHAR columns that are too narrow are widened via `ALTER TABLE ... ALTER COLUMN`. Integer columns that have promoted to float are re-typed. No manual schema migration scripts exist in this system.
- **SNS alerts on failure and unrecognised files** — The team is notified within seconds of a failure, rather than discovering it hours later through a data quality issue.
- **Structured audit trail** — The `sp_log_job_status` stored procedure writes a record for every execution. The `v_job_history` view exposes what ran, when, how long it took, how many rows were loaded, and whether it succeeded. This replaced manual log trawling completely.
- **S3 file lifecycle as implicit state machine** — Files move from `data/in/` → `data/archive/` on success, or `data/in/` → `data/unprocessed/` on failure. The S3 prefix itself tells an operator the current status of every file without opening a console.

### Interview framing

> "The previous state required an analyst to check an S3 folder every morning, manually trigger scripts, and diagnose failures from raw CloudWatch logs. We eliminated all of that. Adding a new data source went from 1–2 days of writing, testing, and deploying a new script to 15 minutes of editing a JSON config file."

---

## KPI 3 — L: Data Latency (Freshness)

**Metric:** Elapsed time between a source file landing in `s3://bucket/data/in/` and that data being queryable in Redshift Serverless by fund managers.

**Before:** T+1 day. A nightly cron job polled S3 at midnight, processed files in a batch window, and loaded results into a provisioned RDS/Redshift cluster. Morning NAV and position data was available at end-of-day at the earliest.

**After:** T+30 minutes. Data is available within 30 minutes of file arrival, at any time of day.

**Business impact:** Fund managers and risk teams can query morning NAV and position data by 9:00 AM instead of waiting for an end-of-day batch run. This directly impacts the timeliness of investment decisions and daily risk reporting.

### Specific mechanisms that drove this number

- **EventBridge S3 event trigger** — EventBridge fires an event the moment a `data/in/` prefix object is created. There is no polling interval, no cron window, no "wait until midnight" — latency between file arrival and pipeline start is under 1 second.
- **SQS FIFO as a millisecond buffer** — The queue adds ordering and deduplication guarantees with negligible latency overhead (milliseconds), not a scheduling delay.
- **Glue Serverless on-demand workers** — Workers provision immediately when the job starts. There is no persistent cluster to wait for, no warm-up period, and no resource contention with other jobs. The Glue job itself runs in 3–8 minutes depending on file size.
- **Redshift Serverless auto-scaling** — No capacity pre-provisioning needed. The warehouse scales to handle the `MERGE` workload for this file's rows immediately, without queueing behind a fixed concurrency limit.

### Interview framing

> "We went from a T+1 batch pipeline — where yesterday's data was available today — to a near-real-time event-driven architecture where data is queryable within 30 minutes of the source file arriving. That's a 96% reduction in data latency, and it changed what the business could actually do with the data."

---

## KPI 4 — A: Data Quality Auditability

**Metric:** % of data loads that have a complete, permanent, queryable audit record covering automated DQ check results, row counts, load timestamps, and point-in-time dimension history.

**Before:** 0% automated coverage. Quality was assessed through ad-hoc manual spot checks by analysts — typically triggered only when a downstream report looked wrong. No audit trail existed. Historical dimension changes (e.g. a fund reclassification) overwrote previous values with no version history.

**After:** 100% of loads produce a permanent DQ audit record. All dimension changes are versioned with full point-in-time query capability.

### Specific mechanisms that drove this number

**Seven categories of automated DQ checks** — stored procedures that run on every load:

| Stored Procedure | What it checks |
|---|---|
| `sp_dq_check_nulls` | Non-nullable columns contain no nulls |
| `sp_dq_check_duplicates` | Primary key uniqueness in the loaded batch |
| `sp_dq_check_row_count` | Loaded row count is within expected thresholds |
| `sp_dq_check_freshness` | Source file timestamp is within expected recency window |
| `sp_dq_check_referential_integrity` | Foreign keys in fact tables resolve to existing dimension members |
| `sp_dq_check_schema_drift` | Source columns match expected config schema |
| `sp_log_job_status` | Execution metadata: start time, end time, rows loaded, status, error message |

All results are written to the `data_quality_results` table and exposed via `v_data_quality_dashboard`.

- **SCD Type 2 history tracking** — Every change to a dimension attribute (e.g. fund category reclassification, client segment change) is versioned with `effective_date`, `end_date`, `is_current`, and `version` columns. A `BEGIN/COMMIT` block in the stored procedure atomically expires the current record and inserts the new version — late-arriving records are handled correctly.
- **S3 archive as file-level audit trail** — Processed files move to `data/archive/YYYY/MM/DD/` with the original filename preserved. Every file that has ever been processed is retained and traceable.

### Interview framing

> "In a regulated investment management context, 'the data is probably right' is not acceptable. We built automated quality gates that run on every single load and persist results permanently. If a regulator asks what the AUM was for Fund X on March 1st 2024, we can answer it exactly — both what the value was, and when it was loaded."

---

## KPI 5 — C: Infrastructure Cost Efficiency

**Metric:** Monthly infrastructure cost and idle cost during off-peak hours, compared to the previous fixed-capacity architecture.

**Before:** Fixed always-on costs. A provisioned Redshift or RDS cluster, a persistent EMR cluster for Spark jobs, and a dedicated EC2 instance running the scheduler all ran 24/7 regardless of actual workload. Data files arrive for approximately 8 hours per day; infrastructure ran idle for the remaining 16 hours.

**After:** ~65% infrastructure cost reduction. During the 16 off-peak hours per day when no files are arriving, compute cost is near zero.

### Specific mechanisms that drove this number

- **Redshift Serverless** — Billing is per RPU-second of actual query execution. A `MERGE` statement that runs for 45 seconds costs 45 RPU-seconds. During off-peak windows with no active queries, cost is effectively zero — no provisioned cluster running idle.
- **Glue Serverless workers** — Workers spin up for the duration of the ETL job (3–8 minutes) and terminate on completion. No persistent EMR cluster. Workers are right-sized per environment: Dev/QA use 2 DPU workers, Staging uses 3, Production uses 5.
- **Lambda + Step Functions** — Priced per invocation and per state transition. At the file volumes in this platform, these services cost fractions of a cent per day.
- **S3 lifecycle policies** — Staging data is deleted after processing. Archive data transitions to S3-IA (Infrequent Access) after 30 days and to S3 Glacier after 90 days, reducing storage cost for historical files automatically.
- **Environment tiering** — Rather than over-provisioning all environments uniformly, each environment is right-sized: 2 Glue workers for Dev, 3 for Staging, 5 for Production. This alone reduces Dev/QA Glue costs by 60% versus using Production-sized resources everywhere.

### Interview framing

> "We replaced a fixed-cost always-on architecture — a provisioned Redshift cluster, a persistent EMR cluster, and a dedicated EC2 scheduler — with a fully serverless, event-driven stack. Infrastructure cost dropped by approximately 65% because we pay only for what we actually use. The weekend and overnight idle cost is effectively zero."

---

## Quick Reference — STAR Answer Framework

Use this structure when asked *"Tell me about a project you're proud of"*:

**Situation:** An investment management firm was running fragmented, bespoke ETL scripts — no standardisation, no observability, no automated recovery, and data arriving T+1 for time-sensitive fund reporting.

**Task:** Design a production-grade, scalable, event-driven ETL platform that non-engineers could extend without writing code, and that met the reliability and audit requirements of a regulated financial environment.

**Action:** Built a layered AWS serverless architecture — EventBridge → SQS FIFO → Lambda → Step Functions → AWS Glue (PySpark) → Redshift Serverless — with a config-driven job router, automated schema evolution, SCD Type 1 and Type 2 dimensional modeling, and a seven-check automated DQ framework with permanent audit logging.

**Result (R.O.L.A.C.):**
- **R**eliability: 99.9% pipeline SLA (from ~80%)
- **O**verhead: <15 min/week manual ops (from 14 hrs/week)
- **L**atency: T+30 min data freshness (from T+1 day) — 96% reduction
- **A**uditability: 100% automated DQ coverage with full regulatory audit trail
- **C**ost: ~65% infrastructure savings with near-zero idle cost

---

## Technical Depth Signals to Drop in Interviews

These are the specific, non-obvious decisions that signal genuine ownership:

1. **On reliability:** "We set Lambda reserved concurrency to 1 specifically to prevent concurrent Redshift `MERGE` statements on the same target table — not just for ordering. Two concurrent merges on the same table would cause lock contention and corrupt the upsert logic."

2. **On schema evolution:** "Schema drift was a real operational pain point. The Glue job dynamically introspects the source CSV headers at runtime, compares them against the Redshift catalog via the Data API, and issues `ALTER TABLE` statements for any drift — new columns added, VARCHAR columns widened, integer types promoted. No human in the loop."

3. **On SCD Type 2:** "SCD Type 2 in Redshift requires careful transaction handling. We expire the current dimension record, insert the new version, and handle late-arriving records all within a single stored procedure wrapped in a `BEGIN/COMMIT` block. This guarantees the dimension table is never in a partially updated state."

4. **On cost:** "The key insight was that our files arrive in an 8-hour window each business day. With a provisioned cluster you pay for 24 hours to cover 8. With Redshift Serverless you pay for the exact RPU-seconds consumed during those 8 hours. For this workload pattern, serverless wins decisively on cost."

5. **On the DLQ:** "The DLQ isn't just a failure bucket — it's a 14-day replay window. If we discover a bug in the transformation logic, we can fix the Glue job and replay every failed message from the DLQ without re-ingesting from the source system."

---

*Document prepared for MAANG/FAANG interview use. All KPIs and mechanisms derived from the AWS ETL Orchestrator production architecture.*
