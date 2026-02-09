-- ============================================================================
-- 01 - Utility Stored Procedures
-- ============================================================================
-- Lightweight helper procedures used by the ETL pipeline.
-- ============================================================================

-- ---------------------------------------------------------------------------
-- sp_check_table_exists
-- Returns 1 if the table exists, 0 otherwise.
-- Usage: CALL public.sp_check_table_exists('public', 'my_table');
-- ---------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE public.sp_check_table_exists(
    p_schema_name   VARCHAR(256),
    p_table_name    VARCHAR(256),
    INOUT p_exists   INTEGER
)
LANGUAGE plpgsql
AS $$
BEGIN
    SELECT COUNT(*)
    INTO p_exists
    FROM information_schema.tables
    WHERE table_schema = p_schema_name
      AND table_name   = p_table_name;

    IF p_exists > 0 THEN
        p_exists := 1;
    END IF;
END;
$$;


-- ---------------------------------------------------------------------------
-- sp_get_row_count
-- Returns the current row count of a table.
-- Usage: CALL public.sp_get_row_count('public', 'products', 0);
-- ---------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE public.sp_get_row_count(
    p_schema_name   VARCHAR(256),
    p_table_name    VARCHAR(256),
    INOUT p_row_count BIGINT
)
LANGUAGE plpgsql
AS $$
BEGIN
    EXECUTE 'SELECT COUNT(*) FROM ' || p_schema_name || '.' || p_table_name
    INTO p_row_count;
END;
$$;


-- ---------------------------------------------------------------------------
-- sp_create_staging_table
-- Creates an empty staging table matching the target table's structure.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE public.sp_create_staging_table(
    p_schema_name       VARCHAR(256),
    p_staging_table     VARCHAR(256),
    p_target_table      VARCHAR(256)
)
LANGUAGE plpgsql
AS $$
BEGIN
    EXECUTE 'DROP TABLE IF EXISTS ' || p_schema_name || '.' || p_staging_table;

    EXECUTE 'CREATE TABLE ' || p_schema_name || '.' || p_staging_table ||
            ' AS SELECT * FROM ' || p_schema_name || '.' || p_target_table ||
            ' WHERE 1=0';
END;
$$;


-- ---------------------------------------------------------------------------
-- sp_copy_from_s3
-- Bulk-loads a CSV file from S3 into a staging table.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE public.sp_copy_from_s3(
    p_schema_name       VARCHAR(256),
    p_table_name        VARCHAR(256),
    p_s3_path           VARCHAR(2048),
    p_iam_role          VARCHAR(512)
)
LANGUAGE plpgsql
AS $$
BEGIN
    EXECUTE 'COPY ' || p_schema_name || '.' || p_table_name ||
            ' FROM ''' || p_s3_path || '''' ||
            ' IAM_ROLE ''' || p_iam_role || '''' ||
            ' FORMAT AS CSV' ||
            ' TIMEFORMAT ''auto''' ||
            ' DELIMITER '',''' ||
            ' QUOTE ''"''' ||
            ' IGNOREHEADER 1';
END;
$$;


-- ---------------------------------------------------------------------------
-- sp_drop_view_if_exists
-- Safely drops a view if it exists.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE public.sp_drop_view_if_exists(
    p_schema_name   VARCHAR(256),
    p_view_name     VARCHAR(256)
)
LANGUAGE plpgsql
AS $$
BEGIN
    EXECUTE 'DROP VIEW IF EXISTS ' || p_schema_name || '.' || p_view_name;
END;
$$;


-- ---------------------------------------------------------------------------
-- sp_alter_table_add_column
-- Adds a new column to an existing table if it doesn't already exist.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE public.sp_alter_table_add_column(
    p_schema_name   VARCHAR(256),
    p_table_name    VARCHAR(256),
    p_column_name   VARCHAR(256),
    p_data_type     VARCHAR(256)
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_exists INTEGER;
BEGIN
    SELECT COUNT(*)
    INTO v_exists
    FROM information_schema.columns
    WHERE table_schema = p_schema_name
      AND table_name   = p_table_name
      AND column_name  = p_column_name;

    IF v_exists = 0 THEN
        EXECUTE 'ALTER TABLE ' || p_schema_name || '.' || p_table_name ||
                ' ADD COLUMN ' || p_column_name || ' ' || p_data_type;
    END IF;
END;
$$;


-- ---------------------------------------------------------------------------
-- sp_alter_varchar_length
-- Expands a VARCHAR column to a new length.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE public.sp_alter_varchar_length(
    p_schema_name   VARCHAR(256),
    p_table_name    VARCHAR(256),
    p_column_name   VARCHAR(256),
    p_new_length    INTEGER
)
LANGUAGE plpgsql
AS $$
BEGIN
    EXECUTE 'ALTER TABLE ' || p_schema_name || '.' || p_table_name ||
            ' ALTER COLUMN ' || p_column_name ||
            ' TYPE VARCHAR(' || p_new_length::VARCHAR || ')';
END;
$$;


-- ---------------------------------------------------------------------------
-- sp_widen_integer_column
-- Widens an integer column (e.g. INTEGER -> BIGINT) using the
-- add-copy-drop-rename pattern required by Redshift.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE public.sp_widen_integer_column(
    p_schema_name   VARCHAR(256),
    p_table_name    VARCHAR(256),
    p_column_name   VARCHAR(256),
    p_new_type      VARCHAR(64)
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_fqn VARCHAR(512);
BEGIN
    v_fqn := p_schema_name || '.' || p_table_name;

    -- Step 1: Add temporary column with wider type
    EXECUTE 'ALTER TABLE ' || v_fqn ||
            ' ADD COLUMN __tmp_widen__ ' || p_new_type;

    -- Step 2: Copy data with explicit cast
    EXECUTE 'UPDATE ' || v_fqn ||
            ' SET __tmp_widen__ = ' || p_column_name || '::' || p_new_type;

    -- Step 3: Drop old column
    EXECUTE 'ALTER TABLE ' || v_fqn ||
            ' DROP COLUMN ' || p_column_name;

    -- Step 4: Rename temporary column
    EXECUTE 'ALTER TABLE ' || v_fqn ||
            ' RENAME COLUMN __tmp_widen__ TO ' || p_column_name;
END;
$$;
