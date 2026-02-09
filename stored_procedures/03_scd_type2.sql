-- ============================================================================
-- 03 - SCD Type 2 Dimension Processing
-- ============================================================================
-- Slowly Changing Dimension Type 2: track full history of changes.
--
-- When an attribute changes:
--   1. The current record is expired (end_date set, is_current = FALSE)
--   2. A new record is inserted with version = old_version + 1
--
-- New dimension members are inserted with version = 1.
--
-- Expected dimension table structure:
--   {surrogate_key}  BIGINT IDENTITY or managed via this procedure
--   {natural_keys}   business key columns
--   {attributes}     tracked dimension attributes
--   effective_date   DATE
--   end_date         DATE (NULL = current)
--   is_current       BOOLEAN
--   version          INTEGER
--   created_date     TIMESTAMP
--   updated_date     TIMESTAMP
-- ============================================================================

-- ---------------------------------------------------------------------------
-- sp_process_scd_type2
--
-- Parameters:
--   p_schema_name      - Redshift schema
--   p_dim_table        - Target dimension table
--   p_staging_table    - Staging table with new/changed dimension data
--   p_surrogate_key    - Name of the surrogate key column
--   p_natural_keys     - Comma-separated natural key columns
--   p_scd_attributes   - Comma-separated SCD-tracked attribute columns
--   p_all_dim_columns  - Comma-separated list of ALL dimension columns
--                        (natural keys + attributes, EXCLUDING surrogate key
--                         and SCD metadata)
--
-- Usage:
--   CALL public.sp_process_scd_type2(
--       'public', 'dim_fund', 'stg_dim_fund_abc123',
--       'fund_key',
--       'fund_code',
--       'fund_name,fund_category',
--       'fund_code,fund_name,fund_category'
--   );
-- ---------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE public.sp_process_scd_type2(
    p_schema_name       VARCHAR(256),
    p_dim_table         VARCHAR(256),
    p_staging_table     VARCHAR(256),
    p_surrogate_key     VARCHAR(256),
    p_natural_keys      VARCHAR(2048),
    p_scd_attributes    VARCHAR(4096),
    p_all_dim_columns   VARCHAR(4096)
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_dim_fqn           VARCHAR(512);
    v_stg_fqn           VARCHAR(512);
    v_natural_join      VARCHAR(4096) := '';
    v_scd_condition     VARCHAR(8192) := '';
    v_stg_cols          VARCHAR(4096) := '';
    v_insert_cols       VARCHAR(4096) := '';
    v_key               VARCHAR(256);
    v_remaining         VARCHAR(4096);
    v_pos               INTEGER;
    v_first             BOOLEAN;
BEGIN
    v_dim_fqn := p_schema_name || '.' || p_dim_table;
    v_stg_fqn := p_schema_name || '.' || p_staging_table;

    -- ----------------------------------------------------------------
    -- Build natural key JOIN clause: curr.key1 = stg.key1 AND ...
    -- ----------------------------------------------------------------
    v_remaining := TRIM(p_natural_keys);
    v_first := TRUE;

    WHILE LENGTH(v_remaining) > 0 LOOP
        v_pos := POSITION(',' IN v_remaining);
        IF v_pos = 0 THEN
            v_key := TRIM(v_remaining);
            v_remaining := '';
        ELSE
            v_key := TRIM(LEFT(v_remaining, v_pos - 1));
            v_remaining := TRIM(SUBSTRING(v_remaining, v_pos + 1));
        END IF;

        IF NOT v_first THEN
            v_natural_join := v_natural_join || ' AND ';
        END IF;
        v_natural_join := v_natural_join || 'curr.' || v_key || ' = stg.' || v_key;
        v_first := FALSE;
    END LOOP;

    -- ----------------------------------------------------------------
    -- Build SCD change detection condition
    -- ----------------------------------------------------------------
    v_remaining := TRIM(p_scd_attributes);
    v_first := TRUE;

    IF LENGTH(v_remaining) = 0 THEN
        v_scd_condition := '1=0';  -- No SCD tracking
    ELSE
        WHILE LENGTH(v_remaining) > 0 LOOP
            v_pos := POSITION(',' IN v_remaining);
            IF v_pos = 0 THEN
                v_key := TRIM(v_remaining);
                v_remaining := '';
            ELSE
                v_key := TRIM(LEFT(v_remaining, v_pos - 1));
                v_remaining := TRIM(SUBSTRING(v_remaining, v_pos + 1));
            END IF;

            IF NOT v_first THEN
                v_scd_condition := v_scd_condition || ' OR ';
            END IF;
            v_scd_condition := v_scd_condition ||
                'COALESCE(CAST(curr.' || v_key || ' AS VARCHAR(1024)), ''__NULL__'') <> ' ||
                'COALESCE(CAST(stg.'  || v_key || ' AS VARCHAR(1024)), ''__NULL__'')';
            v_first := FALSE;
        END LOOP;
    END IF;

    -- ----------------------------------------------------------------
    -- Build column references for INSERT:
    --   stg.col1, stg.col2, ...
    -- ----------------------------------------------------------------
    v_remaining := TRIM(p_all_dim_columns);
    v_stg_cols  := '';
    v_insert_cols := '';
    v_first := TRUE;

    WHILE LENGTH(v_remaining) > 0 LOOP
        v_pos := POSITION(',' IN v_remaining);
        IF v_pos = 0 THEN
            v_key := TRIM(v_remaining);
            v_remaining := '';
        ELSE
            v_key := TRIM(LEFT(v_remaining, v_pos - 1));
            v_remaining := TRIM(SUBSTRING(v_remaining, v_pos + 1));
        END IF;

        IF NOT v_first THEN
            v_stg_cols    := v_stg_cols    || ', ';
            v_insert_cols := v_insert_cols || ', ';
        END IF;
        v_stg_cols    := v_stg_cols    || 'stg.' || v_key;
        v_insert_cols := v_insert_cols || v_key;
        v_first := FALSE;
    END LOOP;

    -- ================================================================
    -- SCD TYPE 2 MERGE (all within caller's transaction)
    -- ================================================================

    -- Step 1: Snapshot max surrogate key
    EXECUTE 'DROP TABLE IF EXISTS __scd2_max_key__';
    EXECUTE 'CREATE TEMP TABLE __scd2_max_key__ AS '
         || 'SELECT COALESCE(MAX(' || p_surrogate_key || '), 0) AS max_key '
         || 'FROM ' || v_dim_fqn;

    -- Step 2: Expire changed records
    EXECUTE 'UPDATE ' || v_dim_fqn || ' curr '
         || 'SET end_date = CURRENT_DATE - 1, '
         ||     'is_current = FALSE, '
         ||     'updated_date = GETDATE() '
         || 'FROM ' || v_stg_fqn || ' stg '
         || 'WHERE ' || v_natural_join
         || '  AND curr.is_current = TRUE'
         || '  AND (' || v_scd_condition || ')';

    -- Step 3: Insert new versions for changed records (version + 1)
    EXECUTE 'INSERT INTO ' || v_dim_fqn
         || ' (' || p_surrogate_key || ', ' || v_insert_cols
         || ', effective_date, end_date, is_current, version, created_date, updated_date) '
         || 'SELECT '
         ||     'ROW_NUMBER() OVER (ORDER BY ' || v_stg_cols || ') + '
         ||         '(SELECT max_key FROM __scd2_max_key__), '
         ||     v_stg_cols || ', '
         ||     'stg.effective_date, stg.end_date, stg.is_current, '
         ||     'expired.version + 1, '
         ||     'stg.created_date, stg.updated_date '
         || 'FROM ' || v_stg_fqn || ' stg '
         || 'JOIN ' || v_dim_fqn || ' expired '
         ||     'ON ' || REPLACE(v_natural_join, 'curr.', 'expired.')
         ||     ' AND expired.is_current = FALSE'
         ||     ' AND expired.end_date = CURRENT_DATE - 1';

    -- Step 4: Refresh max key after first insert
    EXECUTE 'DROP TABLE __scd2_max_key__';
    EXECUTE 'CREATE TEMP TABLE __scd2_max_key__ AS '
         || 'SELECT COALESCE(MAX(' || p_surrogate_key || '), 0) AS max_key '
         || 'FROM ' || v_dim_fqn;

    -- Step 5: Insert completely new dimension members (version = 1)
    EXECUTE 'INSERT INTO ' || v_dim_fqn
         || ' (' || p_surrogate_key || ', ' || v_insert_cols
         || ', effective_date, end_date, is_current, version, created_date, updated_date) '
         || 'SELECT '
         ||     'ROW_NUMBER() OVER (ORDER BY ' || v_stg_cols || ') + '
         ||         '(SELECT max_key FROM __scd2_max_key__), '
         ||     v_stg_cols || ', '
         ||     'stg.effective_date, stg.end_date, stg.is_current, '
         ||     '1, '
         ||     'stg.created_date, stg.updated_date '
         || 'FROM ' || v_stg_fqn || ' stg '
         || 'WHERE NOT EXISTS ('
         ||     'SELECT 1 FROM ' || v_dim_fqn || ' curr '
         ||     'WHERE ' || v_natural_join
         || ')';

    -- Cleanup
    EXECUTE 'DROP TABLE IF EXISTS ' || v_stg_fqn;
    EXECUTE 'DROP TABLE IF EXISTS __scd2_max_key__';
END;
$$;
