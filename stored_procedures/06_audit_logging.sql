-- ============================================================================
-- 06 - Audit / Job Status Logging
-- ============================================================================
-- Procedures for recording ETL job execution status and metrics.
-- ============================================================================

-- ---------------------------------------------------------------------------
-- sp_log_job_status
-- Records a job execution result in the job_sts audit table.
--
-- Usage:
--   CALL public.sp_log_job_status(
--       'public', 'abc123', 'job_01',
--       '2025-01-15 10:00:00', '2025-01-15 10:05:30',
--       'Products.csv', 'products',
--       1000, 50, 950, 'SUCCESS', NULL
--   );
-- ---------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE public.sp_log_job_status(
    p_schema_name       VARCHAR(256),
    p_run_id            VARCHAR(64),
    p_job_id            VARCHAR(64),
    p_run_start_ts      VARCHAR(64),
    p_run_end_ts        VARCHAR(64),
    p_source_filename   VARCHAR(512),
    p_target_table      VARCHAR(256),
    p_records_read      BIGINT,
    p_records_updated   BIGINT,
    p_records_inserted  BIGINT,
    p_status            VARCHAR(20),
    p_error_message     VARCHAR(4096)
)
LANGUAGE plpgsql
AS $$
BEGIN
    EXECUTE 'INSERT INTO ' || p_schema_name || '.job_sts '
         || '(run_id, job_id, run_start_ts, run_end_ts, '
         ||  'source_filename, target_table_name, '
         ||  'records_read, records_updated, records_inserted, '
         ||  'status, error_message) '
         || 'VALUES ('
         ||     '''' || p_run_id || ''', '
         ||     '''' || p_job_id || ''', '
         ||     '''' || p_run_start_ts || '''::TIMESTAMP, '
         ||     '''' || p_run_end_ts   || '''::TIMESTAMP, '
         ||     '''' || REPLACE(p_source_filename, '''', '''''') || ''', '
         ||     '''' || p_target_table || ''', '
         ||     p_records_read || ', '
         ||     p_records_updated || ', '
         ||     p_records_inserted || ', '
         ||     '''' || p_status || ''', '
         ||     CASE
                    WHEN p_error_message IS NULL THEN 'NULL'
                    ELSE '''' || REPLACE(LEFT(p_error_message, 4096), '''', '''''') || ''''
                END
         || ')';
END;
$$;


-- ---------------------------------------------------------------------------
-- sp_log_job_start
-- Convenience procedure to mark a job as RUNNING.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE public.sp_log_job_start(
    p_schema_name       VARCHAR(256),
    p_run_id            VARCHAR(64),
    p_job_id            VARCHAR(64),
    p_source_filename   VARCHAR(512),
    p_target_table      VARCHAR(256)
)
LANGUAGE plpgsql
AS $$
BEGIN
    CALL public.sp_log_job_status(
        p_schema_name, p_run_id, p_job_id,
        TO_CHAR(GETDATE(), 'YYYY-MM-DD HH24:MI:SS'),
        TO_CHAR(GETDATE(), 'YYYY-MM-DD HH24:MI:SS'),
        p_source_filename, p_target_table,
        0, 0, 0, 'RUNNING', NULL
    );
END;
$$;


-- ---------------------------------------------------------------------------
-- sp_get_job_history
-- Returns recent job executions for monitoring.
-- Usage: SELECT * FROM public.fn_get_job_history('products', 10);
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW public.v_job_history AS
SELECT
    run_id,
    job_id,
    source_filename,
    target_table_name,
    status,
    records_read,
    records_inserted,
    records_updated,
    run_start_ts,
    run_end_ts,
    DATEDIFF(second, run_start_ts, run_end_ts) AS duration_seconds,
    error_message,
    created_at
FROM public.job_sts
ORDER BY run_start_ts DESC;


-- ---------------------------------------------------------------------------
-- v_job_summary
-- Aggregated view of job health for dashboards.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW public.v_job_summary AS
SELECT
    target_table_name,
    COUNT(*)                                          AS total_runs,
    SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) AS success_count,
    SUM(CASE WHEN status = 'FAILED'  THEN 1 ELSE 0 END) AS failure_count,
    ROUND(
        100.0 * SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0),
        1
    )                                                   AS success_rate_pct,
    SUM(records_read)                                   AS total_records_read,
    SUM(records_inserted)                               AS total_records_inserted,
    MAX(run_start_ts)                                   AS last_run,
    AVG(DATEDIFF(second, run_start_ts, run_end_ts))     AS avg_duration_seconds
FROM public.job_sts
WHERE run_start_ts >= DATEADD(day, -30, GETDATE())
GROUP BY target_table_name
ORDER BY last_run DESC;
