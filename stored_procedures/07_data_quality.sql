-- ============================================================================
-- 07 - Data Quality Checks
-- ============================================================================
-- Stored procedures for automated data quality validation.
-- Results are logged to public.data_quality_results for monitoring.
-- ============================================================================

-- ---------------------------------------------------------------------------
-- sp_dq_check_nulls
-- Checks for NULL values in specified columns and logs results.
--
-- Parameters:
--   p_schema_name  - Schema containing the table
--   p_table_name   - Table to check
--   p_column_name  - Column to check for NULLs
--   p_run_id       - ETL run identifier for correlation
--
-- Usage:
--   CALL public.sp_dq_check_nulls('public', 'products', 'product_id', 'run_abc123');
-- ---------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE public.sp_dq_check_nulls(
    p_schema_name   VARCHAR(256),
    p_table_name    VARCHAR(256),
    p_column_name   VARCHAR(256),
    p_run_id        VARCHAR(64)
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_null_count BIGINT;
    v_total      BIGINT;
    v_passed     BOOLEAN;
BEGIN
    EXECUTE 'SELECT COUNT(*) FROM ' || p_schema_name || '.' || p_table_name
         || ' WHERE ' || p_column_name || ' IS NULL'
    INTO v_null_count;

    EXECUTE 'SELECT COUNT(*) FROM ' || p_schema_name || '.' || p_table_name
    INTO v_total;

    v_passed := (v_null_count = 0);

    INSERT INTO public.data_quality_results
        (run_id, check_name, table_name, column_name, check_type,
         expected_value, actual_value, passed, details)
    VALUES (
        p_run_id,
        'null_check_' || p_column_name,
        p_schema_name || '.' || p_table_name,
        p_column_name,
        'null_check',
        '0',
        v_null_count::VARCHAR,
        v_passed,
        v_null_count || ' NULLs found out of ' || v_total || ' rows'
    );
END;
$$;


-- ---------------------------------------------------------------------------
-- sp_dq_check_duplicates
-- Checks for duplicate rows based on key columns.
--
-- Usage:
--   CALL public.sp_dq_check_duplicates('public', 'products', 'product_id,time', 'run_abc123');
-- ---------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE public.sp_dq_check_duplicates(
    p_schema_name   VARCHAR(256),
    p_table_name    VARCHAR(256),
    p_key_columns   VARCHAR(2048),
    p_run_id        VARCHAR(64)
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_dup_count BIGINT;
    v_passed    BOOLEAN;
BEGIN
    EXECUTE 'SELECT COUNT(*) FROM ('
         || '  SELECT ' || p_key_columns || ', COUNT(*) AS cnt'
         || '  FROM ' || p_schema_name || '.' || p_table_name
         || '  GROUP BY ' || p_key_columns
         || '  HAVING COUNT(*) > 1'
         || ') dupes'
    INTO v_dup_count;

    v_passed := (v_dup_count = 0);

    INSERT INTO public.data_quality_results
        (run_id, check_name, table_name, column_name, check_type,
         expected_value, actual_value, passed, details)
    VALUES (
        p_run_id,
        'duplicate_check',
        p_schema_name || '.' || p_table_name,
        p_key_columns,
        'duplicate_check',
        '0',
        v_dup_count::VARCHAR,
        v_passed,
        v_dup_count || ' duplicate key combinations found'
    );
END;
$$;


-- ---------------------------------------------------------------------------
-- sp_dq_check_row_count
-- Validates row count is within an expected range.
--
-- Usage:
--   CALL public.sp_dq_check_row_count('public', 'products', 1, 1000000, 'run_abc123');
-- ---------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE public.sp_dq_check_row_count(
    p_schema_name   VARCHAR(256),
    p_table_name    VARCHAR(256),
    p_min_expected  BIGINT,
    p_max_expected  BIGINT,
    p_run_id        VARCHAR(64)
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_row_count BIGINT;
    v_passed    BOOLEAN;
BEGIN
    EXECUTE 'SELECT COUNT(*) FROM ' || p_schema_name || '.' || p_table_name
    INTO v_row_count;

    v_passed := (v_row_count >= p_min_expected AND v_row_count <= p_max_expected);

    INSERT INTO public.data_quality_results
        (run_id, check_name, table_name, column_name, check_type,
         expected_value, actual_value, passed, details)
    VALUES (
        p_run_id,
        'row_count_check',
        p_schema_name || '.' || p_table_name,
        NULL,
        'row_count',
        p_min_expected || '-' || p_max_expected,
        v_row_count::VARCHAR,
        v_passed,
        'Row count ' || v_row_count || ' (expected ' || p_min_expected || ' to ' || p_max_expected || ')'
    );
END;
$$;


-- ---------------------------------------------------------------------------
-- sp_dq_check_freshness
-- Validates that the most recent record is within an acceptable age.
--
-- Usage:
--   CALL public.sp_dq_check_freshness('public', 'products', 'updated_at', 24, 'run_abc123');
-- ---------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE public.sp_dq_check_freshness(
    p_schema_name       VARCHAR(256),
    p_table_name        VARCHAR(256),
    p_timestamp_column  VARCHAR(256),
    p_max_age_hours     INTEGER,
    p_run_id            VARCHAR(64)
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_max_ts    TIMESTAMP;
    v_age_hours INTEGER;
    v_passed    BOOLEAN;
BEGIN
    EXECUTE 'SELECT MAX(' || p_timestamp_column || ') FROM '
         || p_schema_name || '.' || p_table_name
    INTO v_max_ts;

    IF v_max_ts IS NULL THEN
        v_age_hours := -1;
        v_passed := FALSE;
    ELSE
        v_age_hours := DATEDIFF(hour, v_max_ts, GETDATE());
        v_passed := (v_age_hours <= p_max_age_hours);
    END IF;

    INSERT INTO public.data_quality_results
        (run_id, check_name, table_name, column_name, check_type,
         expected_value, actual_value, passed, details)
    VALUES (
        p_run_id,
        'freshness_check',
        p_schema_name || '.' || p_table_name,
        p_timestamp_column,
        'freshness',
        '<= ' || p_max_age_hours || ' hours',
        CASE WHEN v_age_hours = -1 THEN 'NO DATA' ELSE v_age_hours || ' hours' END,
        v_passed,
        CASE
            WHEN v_max_ts IS NULL THEN 'Table has no data'
            ELSE 'Latest record: ' || v_max_ts::VARCHAR || ' (' || v_age_hours || ' hours ago)'
        END
    );
END;
$$;


-- ---------------------------------------------------------------------------
-- sp_dq_check_referential_integrity
-- Validates that foreign keys in a fact table exist in their dimension table.
--
-- Usage:
--   CALL public.sp_dq_check_referential_integrity(
--       'public', 'fact_aum_revenue', 'fund_key',
--       'public', 'dim_fund', 'fund_key', 'run_abc123'
--   );
-- ---------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE public.sp_dq_check_referential_integrity(
    p_child_schema      VARCHAR(256),
    p_child_table       VARCHAR(256),
    p_child_column      VARCHAR(256),
    p_parent_schema     VARCHAR(256),
    p_parent_table      VARCHAR(256),
    p_parent_column     VARCHAR(256),
    p_run_id            VARCHAR(64)
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_orphan_count BIGINT;
    v_passed       BOOLEAN;
BEGIN
    EXECUTE 'SELECT COUNT(*) FROM ' || p_child_schema || '.' || p_child_table || ' c '
         || 'WHERE c.' || p_child_column || ' IS NOT NULL '
         || 'AND NOT EXISTS ('
         ||     'SELECT 1 FROM ' || p_parent_schema || '.' || p_parent_table || ' p '
         ||     'WHERE p.' || p_parent_column || ' = c.' || p_child_column
         || ')'
    INTO v_orphan_count;

    v_passed := (v_orphan_count = 0);

    INSERT INTO public.data_quality_results
        (run_id, check_name, table_name, column_name, check_type,
         expected_value, actual_value, passed, details)
    VALUES (
        p_run_id,
        'referential_integrity_' || p_child_column,
        p_child_schema || '.' || p_child_table,
        p_child_column,
        'referential_integrity',
        '0 orphans',
        v_orphan_count::VARCHAR,
        v_passed,
        v_orphan_count || ' orphan records in ' || p_child_table || '.' || p_child_column
        || ' not found in ' || p_parent_table || '.' || p_parent_column
    );
END;
$$;


-- ---------------------------------------------------------------------------
-- v_data_quality_dashboard
-- Aggregated view for monitoring data quality over time.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW public.v_data_quality_dashboard AS
SELECT
    table_name,
    check_type,
    COUNT(*)                                              AS total_checks,
    SUM(CASE WHEN passed THEN 1 ELSE 0 END)              AS passed_count,
    SUM(CASE WHEN NOT passed THEN 1 ELSE 0 END)          AS failed_count,
    ROUND(
        100.0 * SUM(CASE WHEN passed THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0),
        1
    )                                                       AS pass_rate_pct,
    MAX(checked_at)                                         AS last_checked
FROM public.data_quality_results
WHERE checked_at >= DATEADD(day, -7, GETDATE())
GROUP BY table_name, check_type
ORDER BY pass_rate_pct ASC, last_checked DESC;
