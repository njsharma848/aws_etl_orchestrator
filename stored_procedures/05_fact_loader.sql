-- ============================================================================
-- 05 - Fact Table Loader
-- ============================================================================
-- Loads fact tables with surrogate key lookups from dimension tables.
--
-- The staging table contains natural keys + measures + degenerate dimensions.
-- This procedure JOINs staging to dimension tables to resolve surrogate keys,
-- then inserts the fully-keyed rows into the fact table.
-- ============================================================================

-- ---------------------------------------------------------------------------
-- sp_load_fact_table
--
-- Parameters:
--   p_schema_name       - Redshift schema
--   p_fact_table        - Target fact table
--   p_staging_table     - Staging table with raw fact data
--   p_surrogate_selects - Comma-separated surrogate key expressions
--                         e.g. 'd_fund.fund_key, d_advisor.advisor_key'
--   p_measure_cols      - Comma-separated measure columns prefixed with stg.
--                         e.g. 'stg.aum_amount, stg.revenue_amount'
--   p_join_clauses      - Complete JOIN clause string including LEFT JOINs
--                         to all dimension tables
--
-- Usage:
--   CALL public.sp_load_fact_table(
--       'public',
--       'fact_aum_revenue',
--       'stg_fact_aum_revenue_abc123',
--       'd_fund.fund_key, d_advisor.advisor_key',
--       'stg.aum_amount, stg.revenue_amount, stg.report_date, stg.load_timestamp',
--       'LEFT JOIN public.dim_fund d_fund ON stg.fund_code = d_fund.fund_code AND d_fund.is_current = TRUE
--        LEFT JOIN public.dim_advisor d_advisor ON stg.advisor_id = d_advisor.advisor_id AND d_advisor.is_current = TRUE'
--   );
-- ---------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE public.sp_load_fact_table(
    p_schema_name       VARCHAR(256),
    p_fact_table        VARCHAR(256),
    p_staging_table     VARCHAR(256),
    p_surrogate_selects VARCHAR(4096),
    p_measure_cols      VARCHAR(4096),
    p_join_clauses      VARCHAR(8192)
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_fact_fqn  VARCHAR(512);
    v_stg_fqn   VARCHAR(512);
BEGIN
    v_fact_fqn := p_schema_name || '.' || p_fact_table;
    v_stg_fqn  := p_schema_name || '.' || p_staging_table;

    -- Insert fact rows with dimension surrogate keys
    EXECUTE 'INSERT INTO ' || v_fact_fqn || ' '
         || 'SELECT ' || p_surrogate_selects || ', ' || p_measure_cols || ' '
         || 'FROM ' || v_stg_fqn || ' stg '
         || p_join_clauses;

    -- Cleanup staging
    EXECUTE 'DROP TABLE IF EXISTS ' || v_stg_fqn;
END;
$$;
