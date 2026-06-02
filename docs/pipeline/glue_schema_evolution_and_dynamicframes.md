# Glue Schema Evolution, DynamicFrames & Bookmarks

This is the reference for **how the Glue ETL jobs handle schema**, **how every module
would look if rebuilt on AWS Glue `DynamicFrame`s**, and **how Glue job bookmarks work
(and fail)** in this orchestrator.

It complements [`data_pipeline_logic.md`](./data_pipeline_logic.md) (the end-to-end
pipeline walkthrough). Where that doc explains *the flow*, this one explains *schema
handling, the DynamicFrame alternative module-by-module, and bookmarks*.

**Source files referenced throughout:**

| Job | File |
|-----|------|
| Simple (flat-table) ETL | `glue_jobs/without_data_model/glue_job.py` |
| Data-model (star schema) ETL | `glue_jobs/with_data_model/glue_job_with_data_model.py` |
| Reference / prototypes | `docs/scratch/*.py`, `glue_features/glue_features.md`, `scenario_based_questions/scenario_based_questions.md` |

---

## Table of Contents

1. [TL;DR](#1-tldr)
2. [Two paradigms: DataFrame vs DynamicFrame](#2-two-paradigms-dataframe-vs-dynamicframe)
3. [The current architecture (DataFrame flow)](#3-the-current-architecture-dataframe-flow)
4. [Schema evolution strategies (as implemented)](#4-schema-evolution-strategies-as-implemented)
5. [DynamicFrames: creation and management](#5-dynamicframes-creation-and-management)
6. [Schema evolution with DynamicFrames](#6-schema-evolution-with-dynamicframes)
7. [Complete module reference (every function, today and on DynamicFrames)](#7-complete-module-reference-every-function-today-and-on-dynamicframes)
8. [Building the entire ELT on DynamicFrames](#8-building-the-entire-elt-on-dynamicframes)
9. [Glue job bookmarks: schema, keys, and failure modes](#9-glue-job-bookmarks-schema-keys-and-failure-modes)
10. [Design considerations and trade-offs](#10-design-considerations-and-trade-offs)
11. [Quick reference](#11-quick-reference)

---

## 1. TL;DR

- **The production jobs do schema evolution with native Spark `DataFrame`s**, not Glue
  `DynamicFrame`s. The job reads the CSV with Spark, reads the **target Redshift
  table's schema** via the Redshift Data API, and **reconciles** the two — adding/
  altering Redshift columns and back-filling source columns as needed.
- The **Redshift table is the schema authority**. Evolution is **additive and
  target-anchored**: new columns are added, missing columns back-filled, `VARCHAR`/
  integer columns widened. Nothing is dropped or narrowed automatically.
- **`DynamicFrame`s** defer type resolution (the `choice` type), capture bad records,
  integrate with the Glue Catalog/bookmarks, and (via the Redshift connector) collapse
  the whole `coalesce → COPY → stage → merge` plumbing into one call. They are **not
  used here**, but §5–§8 show exactly how each module would change if they were.
- **Job bookmarks are effectively inert in the current jobs**: both read via
  `spark.read.csv`, which does **not** participate in bookmarks, so `job.commit()`
  advances no source state. Incrementality today comes from **event-driven single-file
  triggering + archiving**, not bookmarks (see §9).
- §7 is a **complete module inventory** of both jobs; §9 is a deep dive on bookmark
  schema/keys and failure modes.

---

## 2. Two paradigms: DataFrame vs DynamicFrame

AWS Glue gives you two in-memory abstractions that interoperate freely
(`dyf.toDF()` / `DynamicFrame.fromDF(df, glueContext, "name")`).

| Aspect | Spark `DataFrame` (used here) | Glue `DynamicFrame` |
|--------|-------------------------------|----------------------|
| Schema | **Fixed** — resolved at read time (`inferSchema`/explicit) | **Self-describing** — per-record; ambiguity preserved as a `choice` type |
| Type conflicts | Fail at read, or coerce | Captured as a `choice`, resolved later with `resolveChoice` |
| Best for | Tabular, predictable (CSV/Parquet, stable schema) | Messy, nested, evolving, semi-structured (JSON, logs, dirty CSV) |
| Transforms | Full Spark SQL / functions | Glue transforms (`ApplyMapping`, `ResolveChoice`, `Relationalize`, `Unbox`, `Join`…) + drop to Spark via `toDF()` |
| Catalog / bookmarks | Manual / not tracked | First-class (`from_catalog`, `enableUpdateCatalog`, bookmarks via `transformation_ctx`) |
| Error handling | Exceptions | Per-record capture (`errorsAsDynamicFrame`, `stageThreshold`) |
| Performance | Generally faster | Slight per-record overhead; avoids re-reads on drift |

> **Why this repo chose `DataFrame`s:** the sources are well-defined business CSVs
> flowing into **typed Redshift tables**. The target schema — not the file — is the
> contract, and a `DataFrame` + explicit reconciliation gives precise control over
> Redshift DDL (column order, `DECIMAL(38,18)`, `NOT NULL` keys, `VARCHAR` widths).

---

## 3. The current architecture (DataFrame flow)

Both jobs follow the same schema lifecycle inside `main()`; the Redshift target is the
source of truth and the incoming CSV is conformed to it.

```
read_csv_file()                      # Spark reads CSV, normalises columns, casts types
        │
        ▼
check_table_exists()                 # information_schema.tables lookup
        │
   ┌────┴─────────────────────────┐
   │ table missing                │ table exists
   ▼                              ▼
create_new_redshift_table()    read_redshift_table_schema()   # SELECT ... WHERE 1=0 → ColumnMetadata
   │ (DDL from Spark schema)      │
   │                             if column sets differ:
   │                                fill_missing_columns()     # back-fill target cols absent in source
   │                                alter_redshift_table()     # ADD COLUMN for source cols absent in target
   └──────────────┬───────────────┘
                  ▼
alter_varchar_columns()              # widen VARCHAR(n) / promote SMALLINT→INT→BIGINT
                  ▼
df.select(<target column order>)     # align DataFrame to the reconciled Redshift schema
                  ▼
coalesce(1).write.csv(staging)  →  COPY → staging table  →  MERGE (delete+insert) → target
                  ▼
[data-model job only] dimensions (SCD1/SCD2) → facts (surrogate-key lookups)
                  ▼
audit (job_sts) → archive source → export logs
```

The data-model job (`glue_job_with_data_model.py`) is steps 3.1–3.6 of the simple job
**plus** config-driven star-schema population (dimensions then facts).

---

## 4. Schema evolution strategies (as implemented)

This realises "Solution 2 — Explicit Schema Reconciliation" from
[`scenario_based_questions.md`](../../scenario_based_questions/scenario_based_questions.md)
(Scenario 5).

### 4.1 New table — derive DDL from the source (`create_new_redshift_table`)

`_spark_to_redshift_type()` maps Spark types to Redshift DDL; upsert keys get `NOT NULL`:

| Spark type | Redshift type |
|------------|---------------|
| `StringType` | `VARCHAR(256)` |
| `IntegerType` / `LongType` | `INTEGER` / `BIGINT` |
| `DoubleType` | `DOUBLE PRECISION` |
| `DecimalType` | `DECIMAL(38,18)` |
| `BooleanType` | `BOOLEAN` |
| `DateType` / `TimestampType` | `DATE` / `TIMESTAMP` |
| `BinaryType` | `VARBYTE` |

### 4.2 New columns in source → `ALTER TABLE ADD COLUMN` (`alter_redshift_table`)

`glue_jobs/without_data_model/glue_job.py:331`. Because Redshift views bind to their
table, the dependent view is **dropped first and recreated after** the `ADD COLUMN`.
Existing rows get `NULL` for the new column (backward-compatible).

### 4.3 Columns missing from source → back-fill (`fill_missing_columns`)

`glue_jobs/without_data_model/glue_job.py:494`. Target-only columns are materialised in
the DataFrame with type-appropriate defaults (`get_default_value`) so the CSV→COPY
column count stays aligned with the table.

### 4.4 Widening to fit the data (`alter_varchar_columns`)

`glue_jobs/without_data_model/glue_job.py:373`. Value-driven evolution:

- **`VARCHAR` widening:** `max(length(col))` (one aggregate row); if it exceeds the
  current `character_maximum_length`, alter to `VARCHAR(min(maxlen+10, 65535))`.
- **Integer promotion:** `max(abs(col))`; if it overflows, promote
  `SMALLINT → INTEGER → BIGINT` via an add→update→drop→rename dance (Redshift can't
  alter an integer column's type in place).

> The single `.agg().collect()` returns **one** row of aggregates, not the dataset —
> it does not pull data to the driver (contrast with `df.collect()` on a full dataset,
> which can OOM the driver).

### 4.5 Aligning column order before COPY

Redshift `COPY` from CSV is **positional**. After all DDL the job re-reads the target
schema and reorders the DataFrame to match exactly
(`df.select(*[c.name for c in redshift_df.schema.fields])`, `glue_job.py:887`). Skipping
this silently loads values into the wrong columns.

### 4.6 The unsafe-type guard (`check_datatype_matching`)

Only in the simple job (`glue_job.py:698`, currently commented out at the call site): it
**refuses** to load when a non-numeric source column maps to a numeric target column
with non-null values — blocking a lossy load rather than failing opaquely. Worth
enabling.

### 4.7 What is **not** handled automatically (by design)

| Change | Behaviour | Recommended handling |
|--------|-----------|----------------------|
| Column **renamed** | Treated as add + back-fill | Rename map in config; apply before reconciliation |
| Column **dropped** from source | Back-filled (kept in target) | Acceptable; explicit `DROP COLUMN` only via reviewed migration |
| Type **narrowing** | Never auto-applied | Deliberate, reviewed migration |
| Incompatible type (text↔number) | `check_datatype_matching` can reject | Enable the guard; quarantine the file |
| `DECIMAL` precision/scale change | Standardised to `(38,18)` | Override per-column if needed |

**Stance:** additive, non-destructive evolution is automatic; destructive/lossy changes
require a human.

---

## 5. DynamicFrames: creation and management

### 5.1 What a DynamicFrame is

A distributed collection of **self-describing `DynamicRecord`s**. When the same field
appears with different types across records, Glue does **not** fail — it records a
**`choice` type** (e.g. `int|string`) for you to resolve later. That deferral is what
makes DynamicFrames resilient to schema drift.

### 5.2 Creating DynamicFrames

```python
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from pyspark.context import SparkContext

glueContext = GlueContext(SparkContext.getOrCreate())

# (a) From the Glue Data Catalog. transformation_ctx is REQUIRED for bookmarks (§9).
dyf = glueContext.create_dynamic_frame.from_catalog(
    database="ingestion_db", table_name="aum_revenue_raw",
    transformation_ctx="dyf_aum")

# (b) From S3/JDBC directly. format_options handles CSV edge cases
#     (quoteChar, escaper, multiline, withHeader, corrupt-record capture).
dyf = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": ["s3://bucket/data/in/"], "recurse": True},
    format="csv",
    format_options={"withHeader": True, "quoteChar": '"', "escaper": '"'},
    transformation_ctx="dyf_s3")

# (c) From an existing Spark DataFrame (the bridge into Glue transforms).
dyf = DynamicFrame.fromDF(df, glueContext, "dyf_from_df")

dyf.printSchema()    # may show e.g.  amount: choice (double | string)
```

### 5.3 Managing DynamicFrames — core transforms

| Transform | Purpose |
|-----------|---------|
| `resolveChoice` | Resolve `choice` (ambiguous) columns — the key schema tool (§6) |
| `apply_mapping` / `ApplyMapping` | Rename, reorder, **cast** in one step: `[("src","string","tgt","bigint"), …]` |
| `relationalize` | Flatten nested JSON/arrays into related flat frames |
| `unbox` | Parse a string column containing JSON/CSV into structured fields |
| `select_fields` / `drop_fields` | Project columns |
| `drop_nulls` / `DropNullFields` | Remove all-null columns |
| `rename_field` | Rename a single (possibly nested) field |
| `map` / `filter` | Per-record Python transform / predicate |
| `Join.apply` / `union` / `mergeDynamicFrame` | Combine frames; merge on keys |
| `split_fields` / `split_rows` | Partition a frame into multiple |

### 5.4 Per-record error handling

```python
resolved = dyf.resolveChoice(choice="make_struct")
errors_dyf = resolved.errorsAsDynamicFrame()     # rows that failed
print("errors:", resolved.stageErrorsCount())
# stageThreshold / totalThreshold let the job tolerate up to N bad records.
```

### 5.5 Writing DynamicFrames

```python
glueContext.write_dynamic_frame.from_options(
    frame=resolved, connection_type="s3",
    connection_options={"path": "s3://bucket/curated/aum/"}, format="parquet")

# Sink that can also evolve the Data Catalog (§6.3):
sink = glueContext.getSink(connection_type="s3", path="s3://bucket/curated/aum/",
                           enableUpdateCatalog=True, updateBehavior="UPDATE_IN_DATABASE")
sink.setCatalogInfo(catalogDatabase="curated_db", catalogTableName="aum")
sink.setFormat("glueparquet"); sink.writeFrame(resolved)
```

---

## 6. Schema evolution with DynamicFrames

### 6.1 Resolving type conflicts (`resolveChoice`)

| `choice` option | Effect |
|-----------------|--------|
| `make_cols` | Split the ambiguous column into one column per observed type |
| `make_struct` | Wrap both types in a struct — lossless |
| `cast:<type>` | Force a single type |
| `project:<type>` | Keep only values already of that type |

```python
resolved = dyf.resolveChoice(specs=[
    ("revenue", "cast:decimal"),   # mirrors this repo's DECIMAL-for-money intent
    ("flags",   "make_struct")])
```

### 6.2 Merging evolving datasets (`mergeDynamicFrame`)

```python
merged = base_dyf.mergeDynamicFrame(incremental_dyf, paths=["transaction_id", "report_date"])
```

### 6.3 Evolving the Glue Data Catalog automatically

For S3/lakehouse targets, the **sink** can grow the catalog schema as new columns
appear — the catalog analogue of this repo's `ALTER TABLE ADD COLUMN`:

| Mechanism | Where evolution lands |
|-----------|-----------------------|
| `resolveChoice` | In-memory type unification |
| `enableUpdateCatalog` + `updateBehavior=UPDATE_IN_DATABASE` | Glue Data Catalog table definition |
| Crawler with an update policy | Catalog (scheduled, out-of-band) |
| Parquet/Delta/Iceberg `mergeSchema` | The physical table format |

> **Important limitation:** `enableUpdateCatalog` evolves the **Glue Data Catalog**,
> *not* a Redshift table reached over JDBC. Against a Redshift target you still issue
> explicit `ALTER TABLE` (see §7 load group and §8.2).

### 6.4 Open table formats

Per [`glue_features.md`](../../glue_features/glue_features.md), Glue 5.0 ships Iceberg
1.7.1 / Delta 3.3.0 / Hudi 0.15.0 — native schema evolution (`mergeSchema`, `ADD
COLUMNS`, column mapping) plus time-travel/branching. Often the better fit **when the
target is a data lake** rather than Redshift.

---

## 7. Complete module reference (every function, today and on DynamicFrames)

Every function in both jobs, grouped by concern. **"Today"** = the current DataFrame
implementation; **"On DynamicFrames"** = how that module changes / how it helps schema
handling if rebuilt. Functions marked 🟰 are frame-agnostic (port unchanged); ✳️ change;
❌ are eliminated.

### 7.1 Initialisation, logging, resilience

| Function | Today | On DynamicFrames |
|----------|-------|------------------|
| `LogBuffer` (`info`/`warning`/`error`/`export_to_s3`, `_ts`) | Structured JSON logs → CloudWatch + S3, keyed by `run_id` | 🟰 Unchanged. Additionally log `dyf.stageErrorsCount()` / `errorsCount()` for per-record visibility |
| `retry_on_exception` | Exponential-backoff decorator (5→120s, 3 attempts) on Data API calls | 🟰 Unchanged — still wraps Data API/DDL; the connector handles COPY/merge retries internally |
| `initialize_glue` | `GlueContext` + `Job`; `job.init(name, args)` | ✳️ Same, but **`job.init` + `job.commit` + `transformation_ctx` are mandatory for bookmarks** (§9) |

### 7.2 Source read & column normalisation

| Function | Today | On DynamicFrames |
|----------|-------|------------------|
| `read_csv_file` | `spark.read.csv(header, inferSchema)` + rename + `cast_like` + audit cols | ✳️ `create_dynamic_frame.from_options(... format="csv", transformation_ctx=...)`. Gains: **bookmarks**, **malformed-record capture**, deferred typing via `choice` |
| `_clean_colname` | Lowercase, non-alphanumeric→`_`, collapse repeats | ✳️ Folded into `apply_mapping` target names (rename lives in the mapping spec) |
| `cast_like` (inner) | Non-key `double → decimal(38,18)`; cast to inferred types | ✳️ `resolveChoice` settles ambiguity, then `apply_mapping` casts/reorders declaratively (§8.1) |

```python
# read_csv_file → DynamicFrame equivalent
dyf = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": [f"s3://{bkt}/data/in/{fname}"]},
    format="csv", format_options={"withHeader": True},
    transformation_ctx=f"read_{target_table}")          # ← enables bookmarks
dyf = dyf.resolveChoice(choice="cast:string")           # or make_struct to keep both
```

### 7.3 Schema discovery & evolution

| Function | Today | On DynamicFrames |
|----------|-------|------------------|
| `read_redshift_table_schema` (+ `map_dtype`) | `SELECT … WHERE 1=0` → `ColumnMetadata` → typed empty DF | 🟰 Still the schema authority; compare against `dyf.toDF().schema` instead of `df.schema` |
| `check_table_exists` | `information_schema.tables` lookup | 🟰 Unchanged |
| `_spark_to_redshift_type` | Spark type → Redshift DDL string | 🟰 Still needed to render `CREATE`/`ALTER` DDL (Glue types map 1:1) |
| `create_new_redshift_table` | DDL from schema; `NOT NULL` keys; create view | ✳️ Drive DDL from the **resolved** frame schema; or let a `CREATE TABLE AS` preaction do it |
| `alter_redshift_table` | `ADD COLUMN` for new source cols (drop/recreate view) | ✳️ Emit the same `ADD COLUMN` as connector **`preactions`** — connector won't auto-`ALTER` Redshift |
| `get_metadata` | `SVV_COLUMNS` → lengths/types for widening | 🟰 Unchanged (introspection for §4.4) |
| `alter_varchar_columns` | Widen `VARCHAR`; promote `SMALLINT→INT→BIGINT` | ✳️ Same aggregates; widening `ALTER`s become `preactions`; `resolveChoice` first-classes type ambiguity |
| `get_default_value` | Type → default literal for back-fill | 🟰 Reused by back-fill (`withColumn`/`apply_mapping`) |
| `fill_missing_columns` | Add target-only columns with defaults | ✳️ Same logic on `toDF()` then back to a frame, or via `apply_mapping` |

### 7.4 Staging, COPY, and merge (the load path — biggest change)

| Function | Today | On DynamicFrames |
|----------|-------|------------------|
| `df.coalesce(1).write.csv` | Single-file CSV to S3 staging prefix | ❌ Eliminated — the connector writes to `redshiftTmpDir` itself |
| `_find_single_csv_in_prefix` | Locate the one part-file for COPY | ❌ Eliminated |
| `create_staging_table` | `CALL sp_create_staging_table(...)` | ✳️ Becomes a connector **`preaction`** (`CREATE TABLE … LIKE …`) |
| `copy_to_redshift` | `CALL sp_copy_from_s3(...)` | ❌ Eliminated — connector runs `COPY` internally |
| `run_merge` | `CALL sp_merge_from_staging(...)` (DELETE+INSERT) | ✳️ Becomes a connector **`postaction`** (same SP, or inline DELETE/INSERT) |
| `get_row_count` | `COUNT(*)` for audit deltas | 🟰 Unchanged; or use `dyf.count()` |
| `_poll_statement` / `execute_sql` | Redshift Data API submit + poll | ✳️ Still used for DDL/SP via Data API; load SQL moves into pre/postactions |

> One `write_dynamic_frame` call (§8.2) replaces `write` + `_find_single_csv_in_prefix`
> + `create_staging_table` + `copy_to_redshift` + `run_merge`.

### 7.5 Reporting views

| Function | Today | On DynamicFrames |
|----------|-------|------------------|
| `_load_view_config` | Load `config_view.json` from S3 | 🟰 Unchanged |
| `create_views` / `drop_views` | `CREATE/DROP VIEW` via Data API around DDL | 🟰 Unchanged. (For a catalog target, multi-dialect Glue Catalog views are an alternative — `glue_features.md` §L) |

### 7.6 Data quality

| Function | Today | On DynamicFrames |
|----------|-------|------------------|
| `check_datatype_matching` (simple job) | Reject lossy text→number loads | ✳️ `resolveChoice("make_struct")` + `errorsAsDynamicFrame` to quarantine, plus **Glue Data Quality** (`EvaluateDataQuality`, DQDL) as a declarative gate |

### 7.7 Audit & S3 lifecycle

| Function | Today | On DynamicFrames |
|----------|-------|------------------|
| `update_job_sts_table` | Write run row to `job_sts` | 🟰 Unchanged; can add `dyf` error counts |
| `move_s3_file_to_archive` | Copy to `data/archive/…`, delete source | 🟰 Unchanged (boto3). Note Glue's `purge_s3_path`/`purge_table` as alternatives |
| `delete_staging_s3_files` | Remove staging CSVs | ✳️ Largely redundant — the connector manages `redshiftTmpDir` |
| `move_s3_file_to_unprocessed` | Move failed file to `data/unprocessed/…` | 🟰 Unchanged |

### 7.8 Dimensional model (data-model job only)

| Function | Today | On DynamicFrames |
|----------|-------|------------------|
| `load_dimensional_config` | Load `dimensional_mappings.json` | 🟰 Unchanged |
| `process_dimension_from_config` (SCD2) | Select + dedupe + SCD meta + COPY + `sp_process_scd_type2` | ✳️ `apply_mapping` (column_mappings) + `select_fields` + `dropDuplicates`; SCD2 SP as **`postaction`** |
| `process_scd_type1_dimension` (SCD1) | Same shape, `sp_process_scd_type1` | ✳️ Same; SCD1 SP as `postaction` |
| `process_fact_from_config` | Stage natural keys + `sp_load_fact_table` (joins in Redshift) | ✳️ `Join.apply` for surrogate lookups, **or** keep the in-Redshift join SP (simpler); load via connector |

> **Honest call:** DynamicFrames add little to the **star-schema SQL**. Keep
> `03_scd_type2.sql` / `04_scd_type1.sql` / `05_fact_loader.sql` and invoke them as
> `postactions`; use DynamicFrames for ingest/typing/loading.

### 7.9 Entry point

| Function | Today | On DynamicFrames |
|----------|-------|------------------|
| `main` | Orchestrates the flow; `job.commit()` | ✳️ See `main_dynamic()` skeleton (§8.4); ensure a stable `transformation_ctx` and `job.commit()` so bookmarks advance |

---

## 8. Building the entire ELT on DynamicFrames

A forward-looking **design**. The biggest shift is the load path; schema evolution
against Redshift stays explicit (§6.3 limitation). This section is a blueprint — the
code blocks below are illustrative, not a deployed job.

### 8.0 The two flows side by side

```
 CURRENT  (DataFrame — glue_job.py)            VARIANT  (DynamicFrame design)
 ─────────────────────────────────            ────────────────────────────────────────────────
 spark.read.csv          (NO bookmarks)        create_dynamic_frame.from_options
   │  inferSchema · rename · cast                │  transformation_ctx ........... BOOKMARKS ON
   ▼                                             ▼  resolveChoice ................ choice types
 read_redshift_table_schema  ◄── Data API      apply_mapping / cast ............ rename+cast+reorder
   │  reconcile: ALTER · backfill · widen        │
   ▼                                             ▼  errorsAsDynamicFrame ......... quarantine bad rows
 df.select(target order)                       reconcile drift → preactions (ALTER/CREATE/backfill)
   │                                             │
   ▼  coalesce(1).write.csv ──► S3 staging       ▼  write_dynamic_frame(connection_type="redshift")
 create_staging_table    ◄── Data API             ├─ preactions  : CREATE/ALTER staging + target
   ▼  copy_to_redshift (COPY) ◄── Data API         ├─ (connector writes tmpDir + runs COPY)
   ▼  run_merge (DELETE+INSERT) ◄── Data API       └─ postactions : CALL sp_merge_from_staging
   ▼                                             ▼
 job.commit()        (no-op for bookmarks)      job.commit() .................... BOOKMARK advances
```

> Left is the current, target-anchored DataFrame pipeline. Right replaces the read +
> five load functions with a bookmarked DynamicFrame read and **one** connector write
> carrying `preactions`/`postactions`; the schema-reconciliation logic survives as the
> `preactions` (§6.3 — Redshift DDL is never auto-evolved).

### 8.1 Type conform — `apply_mapping` + `resolveChoice`

```python
def conform(dyf, config):
    dyf = dyf.resolveChoice(choice="cast:string")        # settle ambiguity first
    upsert = {k.lower() for k in config["upsert_keys"]}
    mappings = []
    for f in dyf.toDF().schema.fields:
        tgt = _clean_colname(f.name)
        tgt_type = "decimal(38,18)" if (_is_double(f.dataType) and tgt not in upsert) \
                   else _glue_type(f.dataType)
        mappings.append((f.name, _glue_type(f.dataType), tgt, tgt_type))
    return dyf.apply_mapping(mappings)                   # rename + cast + reorder in one step
```

### 8.2 Schema evolution + load + merge — one connector call

```python
def load_and_merge(dyf, config, redshift_conn, client):
    schema, table = redshift_conn["schema_name"], config["target_table"]
    target = read_redshift_table_schema(config, redshift_conn, spark, client)  # reuse probe
    tgt = {f.name: f.dataType for f in target.schema.fields}
    src = {f.name: f.dataType for f in dyf.toDF().schema.fields}

    pre = [f"CREATE TABLE IF NOT EXISTS {schema}.{table}_stg (LIKE {schema}.{table});",
           f"TRUNCATE TABLE {schema}.{table}_stg;"]
    for name, dt in src.items():                          # additive evolution → preactions
        if name not in tgt and _valid_ident(name):        # validate identifier before DDL interpolation
            rtype = _spark_to_redshift_type(dt)
            pre += [f"ALTER TABLE {schema}.{table} ADD COLUMN {name} {rtype};",
                    f"ALTER TABLE {schema}.{table}_stg ADD COLUMN {name} {rtype};"]

    df = dyf.toDF()                                        # back-fill target-only columns
    for name, dt in tgt.items():
        if name not in src:
            df = df.withColumn(name, F.lit(get_default_value(dt)))
    aligned = DynamicFrame.fromDF(df.select(*tgt.keys()), glueContext, "aligned")

    keys = config["upsert_keys"]
    on = " AND ".join(f"{table}.{k} = {table}_stg.{k}" for k in keys)
    post = (f"BEGIN;"
            f"DELETE FROM {schema}.{table} USING {schema}.{table}_stg WHERE {on};"
            f"INSERT INTO {schema}.{table} SELECT * FROM {schema}.{table}_stg;"
            f"DROP TABLE {schema}.{table}_stg; END;")
    # …or: postactions = f"CALL public.sp_merge_from_staging('{schema}','{table}','{table}_stg','{','.join(keys)}')"

    glueContext.write_dynamic_frame.from_options(
        frame=aligned, connection_type="redshift",
        connection_options={
            "redshiftTmpDir": redshift_conn["tmp_dir"],
            "connectionName": "redshift-glue-connection",
            "dbtable": f"{schema}.{table}_stg",
            "preactions":  ";\n".join(pre),
            "postactions": post,
        },
        transformation_ctx=f"load_{table}")
```

### 8.3 Pure-Glue variant — S3/Catalog target = *automatic* schema evolution

If the target could be an S3/Data-Catalog table (Parquet/Iceberg/Delta) instead of
Redshift, the §8.2 reconciliation disappears entirely:

```python
sink = glueContext.getSink(
    connection_type="s3", path="s3://bucket/curated/aum/",
    enableUpdateCatalog=True, updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=["report_date"])
sink.setCatalogInfo(catalogDatabase="curated_db", catalogTableName="aum")
sink.setFormat("glueparquet"); sink.writeFrame(resolved)
# Redshift reads via Spectrum / Lakehouse (glue_features.md §S, §L)
```

This is the **only** configuration with hands-off, end-to-end schema evolution.

### 8.4 End-to-end skeleton

A simple-job (`without_data_model`) shape. Note the ordering: **schema evolution runs
via the Data API *before* the load**, because integer promotion reorders Redshift columns
and `COPY` is positional — so the frame must be aligned to the post-evolution schema
before the connector writes it.

```python
def main_dynamic():
    spark, glueContext, job = initialize_glue(config["job_name"])
    dyf = read_source(config, glueContext)               # §7.2 ingest + bookmarks
    dyf = quality_gate(dyf, config, glueContext, log)     # §7.6 errors → quarantine (raw frame)
    df  = conform_to_df(dyf, config, log)                 # §8.1 resolveChoice + cast + audit cols

    if check_table_exists(redshift_conn, config, client):
        redshift_df = read_redshift_table_schema(config, redshift_conn, spark, client)
        if set(df.columns) != set(redshift_df.columns):   # §4.2–4.3 additive evolution
            df = fill_missing_columns(df, redshift_df, log)
            alter_redshift_table(config, redshift_conn, df, redshift_df, client, log)
    else:
        create_new_redshift_table(config, redshift_conn, df, client, log)

    alter_varchar_columns(config, redshift_conn, df, client, log)   # §4.4 widen / promote ints
    redshift_df = read_redshift_table_schema(config, redshift_conn, spark, client)
    df = df.select(*[c.name for c in redshift_df.schema.fields])    # §4.5 positional alignment

    aligned = DynamicFrame.fromDF(df, glueContext, "aligned")
    load_and_merge(aligned, config, redshift_conn, glueContext, log)  # §8.2 staging+COPY+merge
    update_job_sts_table(...)                             # §7.7 unchanged
    job.commit()                                          # §9 persist bookmark
    # data-model variant would additionally call process_dimensional_model(...) here (§7.8)
```

### 8.5 Gains vs costs of the rebuild

| You gain | You pay |
|----------|---------|
| `resolveChoice` first-classes mixed types the DataFrame path can't see | `choice` types can mask real data issues if blindly `cast` |
| Connector-managed `COPY` — delete ~5 functions of S3/COPY plumbing | Connector needs a **Glue JDBC Connection + VPC route**, losing the **Data API's no-VPC simplicity** the repo chose (`data_pipeline_logic.md`) |
| Per-record error capture + quarantine | Manage `redshiftTmpDir` lifecycle/permissions |
| Job bookmarks via `transformation_ctx` (§9) | Redshift DDL evolution **still not automatic** — keep §8.2 as `preactions` |
| **Automatic** catalog evolution iff target is S3/Catalog (§8.3) | Slight per-record overhead |

---

## 9. Glue job bookmarks: schema, keys, and failure modes

Job bookmarks let a job **process only new data** across runs by persisting state
between runs. This section covers how they're enabled, **how schema/keys are defined**,
and **when they fail** — with the repo-specific reality up front.

### 9.1 The reality in this repo (read this first)

Both jobs read with **`spark.read.csv`** (`glue_job.py:142`) and call `job.init` /
`job.commit` — but with **no `transformation_ctx` and no DynamicFrame reader**.

> **Consequence: bookmarks are inert here.** Bookmarks only track sources read through
> the Glue readers (`create_dynamic_frame.from_options` / `from_catalog`, or
> `create_data_frame.from_catalog`) **with a `transformation_ctx`**. A plain
> `spark.read` does **not** participate, so `job.commit()` advances no source state.

Incrementality today is achieved differently and correctly: **one S3 `ObjectCreated`
event → one file processed → file archived/deleted**, so a file is never seen twice.
You only need bookmarks if you switch to **batch-reading a folder** of many files.

### 9.2 Enabling bookmarks

Three modes via the `--job-bookmark-option` job parameter:

| Value | Behaviour |
|-------|-----------|
| `job-bookmark-enable` | Track state; skip already-processed data |
| `job-bookmark-disable` | Off (default) |
| `job-bookmark-pause` | Process the same increment again **without** advancing state |

Required code: `job.init(jobName, args)` at start, **`transformation_ctx` on each
source/sink**, and `job.commit()` at the end. State identity = *(job name +
`transformation_ctx` + run number)*.

### 9.3 How "schema" / keys are defined on bookmarks

Bookmarks don't store a row schema; they store a **position**. How that position is
defined depends on the source type:

**S3 sources** — no key columns. Glue tracks **object key + last-modified timestamp**.
On each run it processes objects whose modification time is **newer** than the last
committed timestamp.

```python
dyf = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": ["s3://bucket/data/in/"], "recurse": True},
    format="csv", format_options={"withHeader": True},
    transformation_ctx="ingest_aum")        # ← the bookmark identity for this source
```

**JDBC / relational sources** — you **define bookmark keys** (the columns that act as
the high-water mark). These must be **unique and monotonically increasing**:

```python
dyf = glueContext.create_dynamic_frame.from_catalog(
    database="src_db", table_name="transactions",
    transformation_ctx="ingest_txn",
    additional_options={
        "jobBookmarkKeys": ["transaction_id"],     # one or more columns
        "jobBookmarkKeysSortOrder": "asc"})        # 'asc' or 'desc'
```

> The bookmark "schema" *is* `jobBookmarkKeys` + `jobBookmarkKeysSortOrder`. Pick a
> column that only ever increases (identity/sequence or an append-only timestamp). A
> composite key is allowed but every component must preserve monotonic order.

### 9.4 When bookmarks fail (failure modes)

| # | Failure mode | Why it happens | Mitigation |
|---|--------------|----------------|------------|
| 1 | **No tracking at all** | Source read with `spark.read` (not a Glue reader) — *this repo's case* | Read via `create_dynamic_frame…`/`create_data_frame.from_catalog` with `transformation_ctx` |
| 2 | **Everything reprocessed** | `transformation_ctx` missing, or **changes between runs** (e.g. embeds `run_id`/timestamp) | Use a **stable, unique** ctx per source (e.g. `f"read_{target_table}"`) |
| 3 | **Same data reprocessed each run** | `job.commit()` not called, or job failed before commit | Always `job.commit()`; commit only after successful writes |
| 4 | **Late/out-of-order files skipped (S3)** | A file lands with a modified time **older** than the last bookmark high-water mark | Don't backdate uploads; for late data, reset/pause or use a manifest |
| 5 | **File reprocessed unexpectedly (S3)** | Same key **re-uploaded/overwritten** → new modified time | Treat S3 input as immutable; write new keys, never overwrite |
| 6 | **Missed rows / duplicates (JDBC)** | Bookmark key not truly unique/monotonic, or has gaps/out-of-order inserts | Use a strict sequence/identity column; never a mutable column |
| 7 | **Updates never captured (JDBC)** | Bookmarks see only **new** rows by increasing key — they are not CDC | Use a CDC tool (DMS/Debezium) or full-snapshot + MERGE for updates |
| 8 | **State corruption under concurrency** | Multiple concurrent runs of the same job contend on bookmark state | Set **max concurrency = 1** for bookmark-enabled jobs (this repo already serialises) |
| 9 | **Stale/incorrect state after code change** | Changing `transformation_ctx`, partitioning, or keys orphans old state | **Reset** the bookmark (`--job-bookmark-option job-bookmark-reset` or `reset-job-bookmark`) |
| 10 | **Left in `pause`** | `job-bookmark-pause` reprocesses without advancing — easy to forget | Use `pause` only for deliberate replays; return to `enable` |
| 11 | **Bounded run cut data short** | `boundedSize`/`boundedFiles` limits per run; remaining files wait | Expected with bounded execution; ensure the job re-runs to drain the backlog |

### 9.5 Bookmarks + schema evolution

A bookmark is path/timestamp- or key-based, so **adding a column to the source does not
break the bookmark itself**. But the *downstream transform* can break if it assumes the
old schema — which is exactly where DynamicFrame `resolveChoice` / `apply_mapping`
(§6) help: new/ambiguous columns are absorbed as `choice` rather than throwing. For
JDBC, **do not** put an evolving/renamed column in `jobBookmarkKeys`; keep the key a
stable surrogate.

### 9.6 Bookmark best practices

- One **stable, unique** `transformation_ctx` per logical source; never embed run-specific values.
- `job.commit()` **once**, at the very end, after successful writes.
- Bookmark-enabled jobs → **max concurrency 1** (already true here via FIFO + serial Step Functions).
- Treat S3 inputs as **immutable**; never overwrite a processed key.
- JDBC keys: strictly increasing, unique, immutable.
- Keep a documented **reset** runbook for re-platforming/backfills.

---

## 10. Design considerations and trade-offs

| Decision in this repo | Benefit | Trade-off / when to revisit |
|-----------------------|---------|------------------------------|
| `DataFrame` + Redshift-anchored reconciliation | Precise DDL, typed warehouse, simple model | More bespoke code than DynamicFrame auto-resolution; less tolerant of chaotic input |
| Target table = schema authority | Stable downstream contracts | Source additions need a Redshift write per drift |
| Additive-only evolution | No accidental data loss | Renames/drops/narrowing need manual migration |
| `DECIMAL(38,18)` for non-key doubles | No float drift on money | Larger storage; uniform precision may over-provision |
| `COPY` + staging + delete/insert MERGE | Fast, atomic, idempotent | `COPY` is positional → column alignment mandatory |
| Redshift Data API | No VPC/driver, easy retries | Async polling; not for high-frequency tiny writes |
| Serial execution (FIFO + concurrency 1) | No merge conflicts; safe for bookmarks | Lower throughput; parallelise per-table if needed |
| `spark.read` (not DynamicFrame reader) | Simple, fast, predictable | **No bookmarks**, no per-record error capture, no `choice` resolution (§9.1) |
| DynamicFrames not used | Less abstraction, faster | Adopt at the ingest edge (§7.2) if inputs get messy or you need bookmarks |

---

## 11. Quick reference

### Complete module inventory (both jobs)

| Group | Functions |
|-------|-----------|
| Init / logging / resilience | `initialize_glue`, `LogBuffer.*`, `retry_on_exception` |
| Read & normalise | `read_csv_file`, `_clean_colname` (+ inner `cast_like`) |
| Redshift Data API | `_poll_statement`, `execute_sql` |
| Schema discovery | `read_redshift_table_schema` (+ `map_dtype`), `check_table_exists`, `_spark_to_redshift_type`, `get_metadata` |
| Schema evolution | `create_new_redshift_table`, `alter_redshift_table`, `alter_varchar_columns`, `get_default_value`, `fill_missing_columns` |
| Load (stage/COPY/merge) | `create_staging_table`, `_find_single_csv_in_prefix`, `copy_to_redshift`, `run_merge`, `get_row_count` |
| Views | `_load_view_config`, `create_views`, `drop_views` |
| Data quality | `check_datatype_matching` *(simple job only)* |
| Audit & S3 lifecycle | `update_job_sts_table`, `move_s3_file_to_archive`, `delete_staging_s3_files`, `move_s3_file_to_unprocessed` |
| Dimensional *(data-model job only)* | `load_dimensional_config`, `process_dimension_from_config` (SCD2), `process_scd_type1_dimension` (SCD1), `process_fact_from_config` |
| Entry | `main` |

Detailed "today vs DynamicFrame" notes for each: see §7.

### Glossary

- **`choice` type** — a DynamicFrame column whose type varies across records; resolved with `resolveChoice`.
- **Target-anchored evolution** — the destination schema, not the file, is the contract.
- **Additive evolution** — only add columns / widen types; never drop or narrow automatically.
- **Positional COPY** — Redshift `COPY` maps CSV columns by order; DataFrame column order must match the table.
- **Bookmark key** — the unique, monotonically increasing column(s) (`jobBookmarkKeys`) that define a JDBC bookmark's high-water mark.
- **`transformation_ctx`** — the identifier that ties a source/sink to its bookmark state; must be stable and unique.

### See also

- [`data_pipeline_logic.md`](./data_pipeline_logic.md) — end-to-end pipeline walkthrough
- [`scenario_based_questions.md`](../../scenario_based_questions/scenario_based_questions.md) — schema-evolution & bookmark scenarios
- [`glue_features.md`](../../glue_features/glue_features.md) — Glue 5.0 / table formats / FGAC
- [`docs/operations/cost_estimation.md`](../operations/cost_estimation.md) — worker sizing & cost
- AWS docs: *Glue DynamicFrame class*, *ResolveChoice transform*, *Updating the Data Catalog from a job*, *Tracking processed data with job bookmarks*, *Redshift Data API*
