-- ============================================================================
-- 04 - SCD Type 1 Dimension Processing
-- ============================================================================
-- Slowly Changing Dimension Type 1: overwrite on change (no history).
--
-- When an attribute changes:
--   The existing record is updated in place with updated_date = GETDATE()
--
-- New dimension members are inserted with a generated surrogate key.
--
-- Expected dimension table structure:
--   {surrogate_key}  BIGINT
--   {natural_keys}   business key columns
--   {attributes}     dimension attributes
--   created_date     TIMESTAMP
--   updated_date     TIMESTAMP (optional, updated on change)
-- ============================================================================

-- ---------------------------------------------------------------------------
-- sp_process_scd_type1
--
-- Parameters:
--   p_schema_name      - Redshift schema
--   p_dim_table        - Target dimension table
--   p_staging_table    - Staging table with new/changed dimension data
--   p_surrogate_key    - Name of the surrogate key column
--   p_natural_keys     - Comma-separated natural key columns
--   p_update_columns   - Comma-separated columns to update on change
--                        (attributes only -- excludes natural keys and surrogate key)
--   p_all_dim_columns  - Comma-separated list of ALL dimension columns
--                        (natural keys + attributes, EXCLUDING surrogate key
--                         and audit metadata)
--
-- Usage:
--   CALL public.sp_process_scd_type1(
--       'public', 'dim_cost_center', 'stg_dim_cost_center_abc123',
--       'cost_center_key',
--       'cost_center_code',
--       'cost_center_name,department',
--       'cost_center_code,cost_center_name,department'
--   );
-- ---------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE public.sp_process_scd_type1(
    p_schema_name       VARCHAR(256),
    p_dim_table         VARCHAR(256),
    p_staging_table     VARCHAR(256),
    p_surrogate_key     VARCHAR(256),
    p_natural_keys      VARCHAR(2048),
    p_update_columns    VARCHAR(4096),
    p_all_dim_columns   VARCHAR(4096)
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_dim_fqn           VARCHAR(512);
    v_stg_fqn           VARCHAR(512);
    v_natural_join      VARCHAR(4096) := '';
    v_update_clause     VARCHAR(8192) := '';
    v_src_cols          VARCHAR(4096) := '';
    v_insert_cols       VARCHAR(4096) := '';
    v_key               VARCHAR(256);
    v_remaining         VARCHAR(4096);
    v_pos               INTEGER;
    v_first             BOOLEAN;
BEGIN
    v_dim_fqn := p_schema_name || '.' || p_dim_table;
    v_stg_fqn := p_schema_name || '.' || p_staging_table;

    -- ----------------------------------------------------------------
    -- Build natural key JOIN: tgt.key1 = src.key1 AND ...
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
        v_natural_join := v_natural_join || 'tgt.' || v_key || ' = src.' || v_key;
        v_first := FALSE;
    END LOOP;

    -- ----------------------------------------------------------------
    -- Build UPDATE SET clause: col1 = src.col1, col2 = src.col2, ...
    -- ----------------------------------------------------------------
    v_remaining := TRIM(p_update_columns);
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
            v_update_clause := v_update_clause || ', ';
        END IF;
        v_update_clause := v_update_clause || v_key || ' = src.' || v_key;
        v_first := FALSE;
    END LOOP;

    -- ----------------------------------------------------------------
    -- Build column references for INSERT
    -- ----------------------------------------------------------------
    v_remaining := TRIM(p_all_dim_columns);
    v_src_cols  := '';
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
            v_src_cols    := v_src_cols    || ', ';
            v_insert_cols := v_insert_cols || ', ';
        END IF;
        v_src_cols    := v_src_cols    || 'src.' || v_key;
        v_insert_cols := v_insert_cols || v_key;
        v_first := FALSE;
    END LOOP;

    -- ================================================================
    -- SCD TYPE 1 MERGE
    -- ================================================================

    -- Step 1: Update existing records (overwrite attributes)
    EXECUTE 'UPDATE ' || v_dim_fqn || ' tgt '
         || 'SET ' || v_update_clause || ', '
         ||     'updated_date = GETDATE() '
         || 'FROM ' || v_stg_fqn || ' src '
         || 'WHERE ' || v_natural_join;

    -- Step 2: Insert new records
    EXECUTE 'INSERT INTO ' || v_dim_fqn
         || ' (' || p_surrogate_key || ', ' || v_insert_cols || ', created_date) '
         || 'SELECT '
         ||     'ROW_NUMBER() OVER (ORDER BY ' || v_src_cols || ') + '
         ||         'COALESCE((SELECT MAX(' || p_surrogate_key || ') FROM ' || v_dim_fqn || '), 0), '
         ||     v_src_cols || ', '
         ||     'src.created_date '
         || 'FROM ' || v_stg_fqn || ' src '
         || 'WHERE NOT EXISTS ('
         ||     'SELECT 1 FROM ' || v_dim_fqn || ' tgt '
         ||     'WHERE ' || v_natural_join
         || ')';

    -- Cleanup
    EXECUTE 'DROP TABLE IF EXISTS ' || v_stg_fqn;
END;
$$;
