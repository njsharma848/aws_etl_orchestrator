-- ============================================================================
-- AWS ETL Orchestrator: Stored Procedures Setup
-- ============================================================================
-- Run this file FIRST to create the audit table and schemas required
-- by all other stored procedures.
-- ============================================================================

-- Audit / job status tracking table
CREATE TABLE IF NOT EXISTS public.job_sts (
    run_id          VARCHAR(64)   NOT NULL,
    job_id          VARCHAR(64)   NOT NULL,
    run_start_ts    TIMESTAMP     NOT NULL,
    run_end_ts      TIMESTAMP,
    source_filename VARCHAR(512),
    target_table_name VARCHAR(256),
    records_read    BIGINT        DEFAULT 0,
    records_updated BIGINT        DEFAULT 0,
    records_inserted BIGINT       DEFAULT 0,
    status          VARCHAR(20)   NOT NULL,  -- SUCCESS | FAILED | RUNNING
    error_message   VARCHAR(4096),
    created_at      TIMESTAMP     DEFAULT GETDATE(),
    PRIMARY KEY (run_id)
)
DISTSTYLE AUTO
SORTKEY (run_start_ts);

-- Data quality results table
CREATE TABLE IF NOT EXISTS public.data_quality_results (
    check_id        BIGINT        IDENTITY(1,1),
    run_id          VARCHAR(64)   NOT NULL,
    check_name      VARCHAR(256)  NOT NULL,
    table_name      VARCHAR(256)  NOT NULL,
    column_name     VARCHAR(256),
    check_type      VARCHAR(64)   NOT NULL,  -- null_check | duplicate_check | freshness | row_count | custom
    expected_value  VARCHAR(256),
    actual_value    VARCHAR(256),
    passed          BOOLEAN       NOT NULL,
    details         VARCHAR(4096),
    checked_at      TIMESTAMP     DEFAULT GETDATE(),
    PRIMARY KEY (check_id)
)
DISTSTYLE AUTO
SORTKEY (checked_at);
