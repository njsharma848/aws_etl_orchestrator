# Code Review: AWS ETL Orchestrator

## Critical Bugs (Runtime Failures)

### 1. Undefined type aliases `T.DoubleType` / `T.DecimalType`
- **File:** `glue_jobs/without_data_model/glue_job.py:146-150`
- **Issue:** `cast_like` references `T.DoubleType` and `T.DecimalType`, but `pyspark.sql.types` is never imported as `T`. Causes `NameError` at runtime.
- **Fix:** Replace `T.DoubleType` with `DoubleType` and `T.DecimalType` with `DecimalType(38, 18)` (already imported).

### 2. Missing imports: `ShortType`, `ByteType`, `count`
- **File:** `glue_jobs/without_data_model/glue_job.py:371, 733, 741`
- **Issue:** `alter_varchar_columns` uses `ShortType`; `check_datatype_matching` uses `ByteType`, `ShortType`, and `count()` -- none are imported.
- **Fix:** Add `ShortType, ByteType` to the `pyspark.sql.types` import and `count` to the `pyspark.sql.functions` import.

### 3. Missing `log` argument in `create_new_redshift_table` call
- **File:** `glue_jobs/without_data_model/glue_job.py:899`
- **Issue:** Called with 4 arguments but function signature at line 286 requires 5 (`config, redshift_conn, df, client, log`).
- **Fix:** Change to `create_new_redshift_table(config, redshift_conn, df, client, log)`.

### 4. State machine / Glue job argument mismatch
- **File:** `state_machines/state_machine.json:20` vs `glue_jobs/without_data_model/glue_job.py:831`
- **Issue:** Step Function passes `--source_file_path` but Glue job expects `--source_file_name`.
- **Fix:** Align the parameter name in either the state machine or the Glue job.

### 5. `fill_missing_columns` uses `log` as implicit global
- **File:** `glue_jobs/without_data_model/glue_job.py:472-483`
- **Issue:** Function references `log` but doesn't accept it as a parameter.
- **Fix:** Add `log` as a function parameter.

### 6. COPY command ignores resolved single-file path
- **File:** `glue_jobs/without_data_model/glue_job.py:517-533`
- **Issue:** `_find_single_csv_in_prefix` resolves the CSV path but COPY still uses the directory prefix.
- **Fix:** Use the resolved `s3_single_file` path in the COPY command.

---

## Logic Bugs

### 7. Duplicate type imports
- **File:** `glue_jobs/without_data_model/glue_job.py:12-18`
- **Issue:** `LongType`, `DecimalType`, `BooleanType`, `TimestampType`, `BinaryType` imported twice.

### 8. Redundant double-read of CSV
- **File:** `glue_jobs/without_data_model/glue_job.py:126-139`
- **Issue:** Source CSV read twice (`df` and `df1`) with identical options. `df1` only used for schema reference that `df` already has.

### 9. `alter_redshift_table` shadows `df` parameter
- **File:** `glue_jobs/without_data_model/glue_job.py:314`
- **Issue:** `df` is overwritten immediately with `read_redshift_table_schema(...)`.

### 10. Double-append in integer column alteration
- **File:** `glue_jobs/without_data_model/glue_job.py:421, 448`
- **Issue:** Same column appended as tuple and dict, producing a mixed-type list.

---

## Security Concerns

### 11. Hardcoded bucket name
- **File:** `lambda_functions/logs_to_smb.py:15`
- **Issue:** `S3_BUCKET = 'nyl-invqai-dev-anaplan'` exposes internal naming.

### 12. SSH host-key validation disabled
- **File:** `lambda_functions/logs_to_smb.py:48`
- **Issue:** `paramiko.AutoAddPolicy()` accepts any host key (MITM vulnerability).

### 13. SQL string interpolation
- **File:** `glue_jobs/without_data_model/glue_job.py:244-249, 763-775`
- **Issue:** Values interpolated directly into SQL. The `error_message` field uses basic quote escaping.

### 14. Dead validation code
- **File:** `glue_jobs/without_data_model/glue_job.py:594, 656-661`
- **Issue:** `if not key:` check on a hardcoded string will never be true.

---

## Architecture Improvements

### 15. No Infrastructure as Code
- No Terraform, CDK, or CloudFormation templates for any AWS resources.

### 16. Monolithic Glue job (965 lines)
- `glue_job.py` handles logging, retry, schema, views, staging, merge, audit, and file management.

### 17. Duplicated view config loading
- `create_views` and `drop_views` duplicate ~30 lines of S3 config fetch/parse logic.

### 18. Data quality framework unused
- Comprehensive `DataQualityFramework` exists in toolkit but is never integrated into the pipeline.

### 19. Incremental loading unused
- Four loading patterns exist but the pipeline always does full-file MERGE.

---

## Operational Gaps

### 20. No tests
- No unit tests, integration tests, or CI/CD pipeline.

### 21. No CloudWatch alarms
- Logs are emitted but no alarms or dashboards are configured.

### 22. Fixed retry delays
- 300-second fixed delay instead of exponential backoff.

### 23. Error handler re-reads CSV on failure
- `glue_job.py:948-949` re-reads the source file in the error handler, which may fail again.

---

## Summary

| Category | Count | Severity |
|---|---|---|
| Runtime bugs | 6 | Critical |
| Logic bugs | 4 | Medium |
| Security issues | 4 | High |
| Architecture gaps | 5 | Medium |
| Operational gaps | 4 | Medium |

## Recommended Priority

1. Fix the 6 critical runtime bugs
2. Add Infrastructure as Code (CDK/Terraform)
3. Add unit tests for schema reconciliation and merge logic
4. Integrate data quality framework into pipeline
5. Decompose `glue_job.py` into focused modules
6. Address security issues (AutoAddPolicy, hardcoded bucket)
