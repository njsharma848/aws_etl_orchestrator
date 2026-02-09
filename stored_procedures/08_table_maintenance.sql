-- ============================================================================
-- 08 - Table Maintenance Procedures
-- ============================================================================
-- Scheduled maintenance operations for Redshift tables.
-- ============================================================================

-- ---------------------------------------------------------------------------
-- sp_vacuum_table
-- Runs VACUUM on a specific table to reclaim space and re-sort.
--
-- Redshift Note: VACUUM cannot run inside a transaction block and requires
-- exclusive table access. Schedule during low-usage windows.
--
-- Usage:
--   CALL public.sp_vacuum_table('public', 'products', 'FULL');
-- ---------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE public.sp_vacuum_table(
    p_schema_name   VARCHAR(256),
    p_table_name    VARCHAR(256),
    p_vacuum_type   VARCHAR(64)   -- FULL | SORT ONLY | DELETE ONLY | REINDEX
)
LANGUAGE plpgsql
AS $$
BEGIN
    EXECUTE 'VACUUM ' || p_vacuum_type || ' ' || p_schema_name || '.' || p_table_name;
    EXECUTE 'ANALYZE ' || p_schema_name || '.' || p_table_name;
END;
$$;


-- ---------------------------------------------------------------------------
-- sp_analyze_table
-- Updates table statistics for the query optimizer.
--
-- Usage:
--   CALL public.sp_analyze_table('public', 'products');
-- ---------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE public.sp_analyze_table(
    p_schema_name   VARCHAR(256),
    p_table_name    VARCHAR(256)
)
LANGUAGE plpgsql
AS $$
BEGIN
    EXECUTE 'ANALYZE ' || p_schema_name || '.' || p_table_name;
END;
$$;


-- ---------------------------------------------------------------------------
-- sp_maintenance_all_etl_tables
-- Runs VACUUM and ANALYZE on all tables that received data recently.
-- Uses the job_sts audit table to identify active tables.
--
-- Usage:
--   CALL public.sp_maintenance_all_etl_tables('public', 7);
-- ---------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE public.sp_maintenance_all_etl_tables(
    p_schema_name    VARCHAR(256),
    p_lookback_days  INTEGER
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_table_name VARCHAR(256);
    v_cursor CURSOR FOR
        SELECT DISTINCT target_table_name
        FROM public.job_sts
        WHERE status = 'SUCCESS'
          AND run_start_ts >= DATEADD(day, -p_lookback_days, GETDATE());
BEGIN
    OPEN v_cursor;

    LOOP
        FETCH v_cursor INTO v_table_name;
        EXIT WHEN NOT FOUND;

        BEGIN
            EXECUTE 'VACUUM FULL ' || p_schema_name || '.' || v_table_name;
            EXECUTE 'ANALYZE ' || p_schema_name || '.' || v_table_name;
            RAISE INFO 'Maintained: %.%', p_schema_name, v_table_name;
        EXCEPTION WHEN OTHERS THEN
            RAISE WARNING 'Failed to maintain %.%: %', p_schema_name, v_table_name, SQLERRM;
        END;
    END LOOP;

    CLOSE v_cursor;
END;
$$;


-- ---------------------------------------------------------------------------
-- sp_purge_old_audit_records
-- Removes job_sts and data_quality_results records older than N days.
--
-- Usage:
--   CALL public.sp_purge_old_audit_records(90);
-- ---------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE public.sp_purge_old_audit_records(
    p_retention_days INTEGER
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_deleted_jobs  BIGINT;
    v_deleted_dq    BIGINT;
BEGIN
    DELETE FROM public.job_sts
    WHERE created_at < DATEADD(day, -p_retention_days, GETDATE());
    GET DIAGNOSTICS v_deleted_jobs = ROW_COUNT;

    DELETE FROM public.data_quality_results
    WHERE checked_at < DATEADD(day, -p_retention_days, GETDATE());
    GET DIAGNOSTICS v_deleted_dq = ROW_COUNT;

    RAISE INFO 'Purged % job_sts records and % data_quality_results records older than % days',
        v_deleted_jobs, v_deleted_dq, p_retention_days;
END;
$$;


-- ---------------------------------------------------------------------------
-- v_table_health
-- Overview of table sizes, row counts, and sort/vacuum status.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW public.v_table_health AS
SELECT
    schema                  AS schema_name,
    "table"                 AS table_name,
    size                    AS size_mb,
    tbl_rows                AS row_count,
    unsorted                AS unsorted_pct,
    stats_off               AS stats_off_pct,
    CASE
        WHEN unsorted > 20  THEN 'NEEDS VACUUM'
        WHEN stats_off > 10 THEN 'NEEDS ANALYZE'
        ELSE 'HEALTHY'
    END                     AS health_status
FROM svv_table_info
WHERE schema NOT IN ('pg_catalog', 'information_schema', 'pg_internal')
ORDER BY size DESC;
