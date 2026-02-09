-- ============================================================================
-- 02 - Merge / Upsert Stored Procedure
-- ============================================================================
-- Core ETL merge: delete-then-insert within a transaction.
-- This replaces the inline SQL in run_merge() of the Glue jobs.
-- ============================================================================

-- ---------------------------------------------------------------------------
-- sp_merge_from_staging
-- Performs a transactional upsert from a staging table into the target table
-- using the supplied key columns.
--
-- The merge is a DELETE+INSERT pattern:
--   1. DELETE rows in target that match staging on the upsert keys
--   2. INSERT all rows from staging into target
--   3. DROP the staging table
--
-- Parameters:
--   p_schema_name   - Redshift schema (e.g. 'public')
--   p_target_table  - Destination table name
--   p_staging_table - Source staging table name (will be dropped)
--   p_upsert_keys   - Comma-separated list of key columns (e.g. 'id,date')
--
-- Usage:
--   CALL public.sp_merge_from_staging('public', 'products',
--        'products_stg_abc123', 'product_id,time');
-- ---------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE public.sp_merge_from_staging(
    p_schema_name   VARCHAR(256),
    p_target_table  VARCHAR(256),
    p_staging_table VARCHAR(256),
    p_upsert_keys   VARCHAR(2048)
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_key           VARCHAR(256);
    v_join_clause   VARCHAR(4096) := '';
    v_target_fqn    VARCHAR(512);
    v_staging_fqn   VARCHAR(512);
    v_pos           INTEGER;
    v_remaining     VARCHAR(2048);
    v_first         BOOLEAN := TRUE;
BEGIN
    v_target_fqn  := p_schema_name || '.' || p_target_table;
    v_staging_fqn := p_schema_name || '.' || p_staging_table;

    -- Build JOIN clause from comma-separated key list
    v_remaining := TRIM(p_upsert_keys);

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
            v_join_clause := v_join_clause || ' AND ';
        END IF;

        v_join_clause := v_join_clause ||
            v_target_fqn || '.' || v_key || ' = ' ||
            v_staging_fqn || '.' || v_key;
        v_first := FALSE;
    END LOOP;

    -- Transactional merge: delete matching + insert all + drop staging
    EXECUTE 'DELETE FROM ' || v_target_fqn ||
            ' USING ' || v_staging_fqn ||
            ' WHERE ' || v_join_clause;

    EXECUTE 'INSERT INTO ' || v_target_fqn ||
            ' SELECT * FROM ' || v_staging_fqn;

    EXECUTE 'DROP TABLE IF EXISTS ' || v_staging_fqn;
END;
$$;
