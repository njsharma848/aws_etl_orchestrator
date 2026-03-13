# SCD Type 2 Implementation Guide

A practical, interview-ready guide to Slowly Changing Dimension Type 2 (SCD2) using real tables and data from this project.

---

## What Is SCD Type 2?

SCD Type 2 **preserves the full history** of changes in a dimension table. When an attribute changes, the old row is **expired** and a **new row** is inserted. No data is ever overwritten.

Every SCD2 dimension table includes these metadata columns:

| Column | Purpose |
|--------|---------|
| `effective_date` | When this version became active |
| `end_date` | When this version was superseded (`NULL` = still current) |
| `is_current` | `TRUE` for the active row, `FALSE` for historical rows |
| `version` | Integer that increments with each change (1, 2, 3, ...) |

**Key rule:** For any given natural key, exactly **one** row has `is_current = TRUE` at all times.

---

## Real Example: `dim_fund` Table

In this project, the `aum_revenue.csv` source file feeds the `dim_fund` dimension. The table tracks investment fund attributes over time.

### Table Structure

```sql
CREATE TABLE dim_fund (
    fund_key            INT NOT NULL,      -- Surrogate key (auto-assigned)
    fund_code           VARCHAR(50),       -- Natural key (part 1)
    share_class         VARCHAR(10),       -- Natural key (part 2)
    fund_name           VARCHAR(256),      -- SCD2 tracked attribute
    fund_family_name    VARCHAR(256),      -- SCD2 tracked attribute
    management_fee      NUMERIC(5,4),      -- SCD2 tracked attribute
    asset_class         VARCHAR(100),      -- SCD2 tracked attribute
    status              VARCHAR(20),       -- SCD2 tracked attribute
    effective_date      DATE NOT NULL,     -- SCD2 metadata
    end_date            DATE,              -- SCD2 metadata (NULL = current)
    is_current          BOOLEAN NOT NULL,  -- SCD2 metadata
    version             INT NOT NULL,      -- SCD2 metadata
    created_date        TIMESTAMP,
    updated_date        TIMESTAMP,
    PRIMARY KEY (fund_key)
);
```

**What each column category means:**

- **Natural keys** (`fund_code`, `share_class`): The business identifier. These never change. Together they uniquely identify a real-world fund.
- **SCD2 tracked attributes** (`fund_name`, `management_fee`, etc.): If any of these change, a new version row is created.
- **Surrogate key** (`fund_key`): A system-generated integer that uniquely identifies each version of a dimension row. Fact tables reference this key.

This mapping comes from `configs/dimensional_mappings.json`:

```json
{
    "dimension_table": "dim_fund",
    "natural_keys": ["fund_code", "share_class"],
    "scd_type": 2,
    "scd_attributes": ["fund_name", "fund_family_name", "management_fee", "asset_class", "status"],
    "column_mappings": {
        "fund_code": "fund_code",
        "share_class": "share_class",
        "fund_name": "fund_name",
        "fund_family_name": "fund_family",
        "management_fee": "mgmt_fee_rate",
        "asset_class": "asset_class",
        "status": "fund_status"
    }
}
```

---

## Step-by-Step Walkthrough with Real Data

### Day 1 -- Initial Load (January 15, 2025)

The first `aum_revenue.csv` arrives with these fund records:

```
fund_code  share_class  fund_name             fund_family       mgmt_fee_rate  asset_class    fund_status
FND001     A            Growth Equity Fund    Equity Funds      0.0075         Equity         Active
FND002     I            Bond Income Fund      Fixed Income      0.0050         Fixed Income   Active
FND003     C            International Fund    International     0.0085         Equity         Active
```

Since `dim_fund` is empty, all three are **new dimension members**. The procedure inserts them with `version = 1`:

**`dim_fund` after initial load:**

| fund_key | fund_code | share_class | fund_name | management_fee | asset_class | status | effective_date | end_date | is_current | version |
|----------|-----------|-------------|-----------|----------------|-------------|--------|----------------|----------|------------|---------|
| 1 | FND001 | A | Growth Equity Fund | 0.0075 | Equity | Active | 2025-01-15 | NULL | TRUE | 1 |
| 2 | FND002 | I | Bond Income Fund | 0.0050 | Fixed Income | Active | 2025-01-15 | NULL | TRUE | 1 |
| 3 | FND003 | C | International Fund | 0.0085 | Equity | Active | 2025-01-15 | NULL | TRUE | 1 |

All rows have `is_current = TRUE`, `end_date = NULL`, and `version = 1`.

---

### Day 2 -- A Change Arrives (February 15, 2025)

A new `aum_revenue.csv` arrives. Fund `FND001` share class `A` now has an **increased management fee** (`0.0075` -> `0.0080`) and `FND003` share class `C` changed its **status** from `Active` to `Closed`. Fund `FND002` is unchanged.

```
fund_code  share_class  fund_name             fund_family       mgmt_fee_rate  asset_class    fund_status
FND001     A            Growth Equity Fund    Equity Funds      0.0080         Equity         Active
FND002     I            Bond Income Fund      Fixed Income      0.0050         Fixed Income   Active
FND003     C            International Fund    International     0.0085         Equity         Closed
```

The SCD2 procedure runs three steps:

#### Step 1: Detect Changes

Compare staging data against current dimension rows (where `is_current = TRUE`) on the tracked attributes:

```sql
-- Change detection condition (auto-generated by the procedure):
COALESCE(CAST(curr.fund_name AS VARCHAR), '__NULL__')    <> COALESCE(CAST(stg.fund_name AS VARCHAR), '__NULL__')
OR COALESCE(CAST(curr.management_fee AS VARCHAR), '__NULL__') <> COALESCE(CAST(stg.management_fee AS VARCHAR), '__NULL__')
OR COALESCE(CAST(curr.asset_class AS VARCHAR), '__NULL__')    <> COALESCE(CAST(stg.asset_class AS VARCHAR), '__NULL__')
OR COALESCE(CAST(curr.status AS VARCHAR), '__NULL__')         <> COALESCE(CAST(stg.status AS VARCHAR), '__NULL__')
-- ...and so on for each SCD attribute
```

**Result:** FND001/A changed (`management_fee`), FND003/C changed (`status`), FND002/I unchanged.

#### Step 2: Expire Changed Rows

The existing current rows for FND001/A and FND003/C are expired:

```sql
UPDATE dim_fund curr
SET end_date     = CURRENT_DATE - 1,   -- 2025-02-14
    is_current   = FALSE,
    updated_date = GETDATE()
FROM stg_dim_fund stg
WHERE curr.fund_code   = stg.fund_code
  AND curr.share_class = stg.share_class
  AND curr.is_current  = TRUE
  AND (/* change detected in any SCD attribute */);
```

#### Step 3: Insert New Versions

New rows are inserted for the changed records with `version = old_version + 1`:

```sql
INSERT INTO dim_fund (fund_key, fund_code, share_class, ..., effective_date, end_date, is_current, version)
SELECT
    ROW_NUMBER() OVER (...) + (SELECT MAX(fund_key) FROM dim_fund),  -- new surrogate key
    stg.fund_code, stg.share_class, ...,
    CURRENT_DATE,    -- effective_date = today
    NULL,            -- end_date = NULL (this is now current)
    TRUE,            -- is_current = TRUE
    expired.version + 1
FROM stg_dim_fund stg
JOIN dim_fund expired ON expired.fund_code = stg.fund_code
                     AND expired.share_class = stg.share_class
                     AND expired.is_current = FALSE
                     AND expired.end_date = CURRENT_DATE - 1;  -- the row we just expired
```

**`dim_fund` after Day 2:**

| fund_key | fund_code | share_class | fund_name | management_fee | status | effective_date | end_date | is_current | version |
|----------|-----------|-------------|-----------|----------------|--------|----------------|----------|------------|---------|
| 1 | FND001 | A | Growth Equity Fund | 0.0075 | Active | 2025-01-15 | 2025-02-14 | **FALSE** | 1 |
| 2 | FND002 | I | Bond Income Fund | 0.0050 | Active | 2025-01-15 | NULL | TRUE | 1 |
| 3 | FND003 | C | International Fund | 0.0085 | **Active** | 2025-01-15 | 2025-02-14 | **FALSE** | 1 |
| **4** | FND001 | A | Growth Equity Fund | **0.0080** | Active | **2025-02-15** | NULL | **TRUE** | **2** |
| **5** | FND003 | C | International Fund | 0.0085 | **Closed** | **2025-02-15** | NULL | **TRUE** | **2** |

Notice:
- **Row 1** (FND001/A v1): expired -- `end_date = 2025-02-14`, `is_current = FALSE`
- **Row 4** (FND001/A v2): new current row with updated `management_fee = 0.0080`
- **Row 3** (FND003/C v1): expired -- `end_date = 2025-02-14`, `is_current = FALSE`
- **Row 5** (FND003/C v2): new current row with `status = Closed`
- **Row 2** (FND002/I): untouched -- nothing changed

---

### Day 3 -- A New Fund Appears (March 15, 2025)

A brand-new fund `FND004` arrives that does not exist in `dim_fund` at all:

```
fund_code  share_class  fund_name        fund_family      mgmt_fee_rate  asset_class    fund_status
FND004     R            Tech Growth      Sector Funds     0.0095         Equity         Active
```

The procedure detects no matching natural key in `dim_fund` and inserts as a brand-new member with `version = 1`:

```sql
INSERT INTO dim_fund (...)
SELECT ...
FROM stg_dim_fund stg
WHERE NOT EXISTS (
    SELECT 1 FROM dim_fund curr
    WHERE curr.fund_code = stg.fund_code
      AND curr.share_class = stg.share_class
);
```

**New row added:**

| fund_key | fund_code | share_class | fund_name | management_fee | status | effective_date | end_date | is_current | version |
|----------|-----------|-------------|-----------|----------------|--------|----------------|----------|------------|---------|
| 6 | FND004 | R | Tech Growth | 0.0095 | Active | 2025-03-15 | NULL | TRUE | 1 |

---

## How Fact Tables Use SCD2 Surrogate Keys

The fact table `fact_aum_revenue` stores the `fund_key` surrogate key, **not** the natural key. This is critical because it links each fact row to the **exact version** of the dimension that was active at that point in time.

```
fact_aum_revenue
-----------------
fund_key = 1   --> Links to FND001/A version 1 (fee = 0.0075)
fund_key = 4   --> Links to FND001/A version 2 (fee = 0.0080)
```

### Query: Current Fund Performance

```sql
-- Only join to current dimension rows
SELECT f.fund_code, f.fund_name, f.management_fee,
       SUM(fa.revenue) AS total_revenue
FROM fact_aum_revenue fa
JOIN dim_fund f ON fa.fund_key = f.fund_key AND f.is_current = TRUE
JOIN dim_version v ON fa.version_key = v.version_key
WHERE v.version_code = 'ACTUAL'
GROUP BY 1, 2, 3;
```

### Query: Point-in-Time Analysis

```sql
-- What was the fund's fee rate when a specific transaction happened?
SELECT fa.transaction_id, f.fund_code, f.management_fee, f.version,
       fa.fee_paying_aum, fa.revenue
FROM fact_aum_revenue fa
JOIN dim_fund f ON fa.fund_key = f.fund_key
WHERE f.fund_code = 'FND001' AND f.share_class = 'A';
```

Because each fact row points to a **specific version** of `dim_fund`, this query automatically returns the fee rate that was in effect at the time of each transaction -- no date filtering needed.

### Query: Complete History for a Fund

```sql
-- All versions of a fund, ordered chronologically
SELECT fund_key, fund_code, share_class, fund_name,
       management_fee, status, effective_date, end_date, is_current, version
FROM dim_fund
WHERE fund_code = 'FND001' AND share_class = 'A'
ORDER BY version;
```

**Result:**

| fund_key | fund_code | management_fee | status | effective_date | end_date | is_current | version |
|----------|-----------|----------------|--------|----------------|----------|------------|---------|
| 1 | FND001 | 0.0075 | Active | 2025-01-15 | 2025-02-14 | FALSE | 1 |
| 4 | FND001 | 0.0080 | Active | 2025-02-15 | NULL | TRUE | 2 |

---

## The Stored Procedure: `sp_process_scd_type2`

The project implements SCD2 in `stored_procedures/03_scd_type2.sql`. Here is how it works internally:

### Parameters

```sql
CALL public.sp_process_scd_type2(
    'public',              -- p_schema_name:     Target schema
    'dim_fund',            -- p_dim_table:       Target dimension table
    'stg_dim_fund_abc123', -- p_staging_table:   Staging table with incoming data
    'fund_key',            -- p_surrogate_key:   Surrogate key column name
    'fund_code,share_class', -- p_natural_keys:  Comma-separated natural keys
    'fund_name,fund_family_name,management_fee,asset_class,status', -- p_scd_attributes
    'fund_code,share_class,fund_name,fund_family_name,management_fee,asset_class,status' -- p_all_dim_columns
);
```

### Internal Steps

```
Step 1: Snapshot max surrogate key
         └─ SELECT MAX(fund_key) FROM dim_fund  →  stores in temp table

Step 2: Expire changed records
         └─ UPDATE dim_fund SET end_date = yesterday, is_current = FALSE
            WHERE natural keys match AND any SCD attribute differs

Step 3: Insert new versions (changed records)
         └─ INSERT INTO dim_fund with version = old_version + 1
            surrogate_key = ROW_NUMBER() + max_key

Step 4: Refresh max surrogate key
         └─ Re-read MAX(fund_key) after the insert above

Step 5: Insert brand-new dimension members
         └─ INSERT INTO dim_fund with version = 1
            WHERE natural key does NOT exist in dim_fund at all

Step 6: Cleanup
         └─ DROP staging table and temp table
```

All steps run inside a single transaction to ensure consistency.

---

## Another Example: `dim_advisor`

The `dim_advisor` table is also SCD2 in this project, tracking advisor reassignments.

**Natural key:** `advisor_code`
**SCD2 tracked attributes:** `advisor_name`, `team_name`, `region_name`, `status`

### Scenario: Advisor Transfers Teams

Source data from `aum_revenue.csv`:

**January 2025:**
```
advisor_code=ADV123, advisor_name=John Smith, team=East Coast, region=Northeast, status=Active
```

**March 2025 -- John moves to West Coast:**
```
advisor_code=ADV123, advisor_name=John Smith, team=West Coast, region=Pacific, status=Active
```

**`dim_advisor` result:**

| advisor_key | advisor_code | advisor_name | team_name | region_name | effective_date | end_date | is_current | version |
|-------------|--------------|--------------|-----------|-------------|----------------|----------|------------|---------|
| 1 | ADV123 | John Smith | East Coast | Northeast | 2025-01-15 | 2025-03-14 | FALSE | 1 |
| 4 | ADV123 | John Smith | **West Coast** | **Pacific** | 2025-03-15 | NULL | TRUE | 2 |

Now any fact rows loaded in January link to `advisor_key = 1` (East Coast), and facts loaded in March link to `advisor_key = 4` (West Coast). Historical reporting stays accurate.

---

## Another Example: `dim_cost_center`

The `dim_cost_center` table tracks organizational hierarchy changes.

**Natural key:** `l4_cost_center_code`
**SCD2 tracked attributes:** `l4_cost_center_name`, `l3_cost_center_code`, `l2_cost_center_code`, `department`, and more

### Scenario: Department Reorganization

Source data from `expense.csv`:

**January 2025:**
```
cost_center_l4=CC-1001, cost_center_l4_name=Portfolio Management Team, cost_center_l3=CC-100, department=Portfolio Management
```

**April 2025 -- The team is renamed:**
```
cost_center_l4=CC-1001, cost_center_l4_name=Investment Strategy Team, cost_center_l3=CC-100, department=Investment Strategy
```

**`dim_cost_center` result:**

| cost_center_key | l4_code | l4_name | department | effective_date | end_date | is_current | version |
|-----------------|---------|---------|------------|----------------|----------|------------|---------|
| 1 | CC-1001 | Portfolio Management Team | Portfolio Management | 2025-01-15 | 2025-04-14 | FALSE | 1 |
| 5 | CC-1001 | **Investment Strategy Team** | **Investment Strategy** | 2025-04-15 | NULL | TRUE | 2 |

Expense facts from Q1 still correctly link to the "Portfolio Management" version, while Q2 expenses link to "Investment Strategy".

---

## End-to-End Data Flow in This Project

```
                        CSV lands in S3
                             │
                             ▼
                     EventBridge detects
                             │
                             ▼
                     SQS FIFO buffers
                             │
                             ▼
                    Lambda routes to
                    Glue job: "data_model"
                             │
                             ▼
              ┌──────────────┴──────────────┐
              │    Glue PySpark Job          │
              │                             │
              │  1. Read CSV from S3         │
              │  2. Reconcile schema         │
              │  3. COPY to staging table    │
              │  4. Read dimensional_        │
              │     mappings.json            │
              │  5. For each dimension:      │
              │     CALL sp_process_scd_     │
              │     type2(...)               │
              │  6. For each fact:           │
              │     CALL sp_load_fact_       │
              │     table(...)              │
              └─────────────────────────────┘
                             │
                             ▼
                Redshift dimension + fact
                    tables updated
```

The Glue job (`glue_jobs/with_data_model/glue_job_with_data_model.py`) reads `configs/dimensional_mappings.json` to determine:
- Which dimensions to populate and their SCD type
- Which attributes to track for changes
- How source columns map to dimension columns
- Which fact tables to load and their dimension lookups

---

## SCD2 vs SCD1 -- When to Use Which

This project uses **both** SCD types. The choice is configured per dimension in `dimensional_mappings.json`:

| Dimension | SCD Type | Why |
|-----------|----------|-----|
| `dim_fund` | **Type 2** | Management fees change and we need to know what fee was active for historical revenue calculations |
| `dim_advisor` | **Type 2** | Advisors transfer between teams; need accurate historical AUM attribution per region |
| `dim_cost_center` | **Type 2** | Departments reorganize; expense history must reflect the org structure at the time |
| `dim_account_type` | **Type 1** | Account types are corrections, not meaningful business changes -- just overwrite |
| `dim_version` | **Type 1** | Static reference data (ACTUAL, PLAN, FORECAST) that does not change |
| `dim_date` | **Type 1** | Pre-populated calendar, no historical tracking needed |

**Rule of thumb:** Use SCD2 when the attribute change is a **real business event** that you need to report on historically. Use SCD1 when changes are **corrections** or the dimension is **static reference data**.

---

## Common Interview Questions

### Q: What happens if the same natural key has two current rows?

**A:** This is a data quality issue. The SCD2 procedure prevents this by design -- it only expires rows where `is_current = TRUE` and the change condition is met, then inserts the new row as current. You can verify with:

```sql
SELECT fund_code, share_class, COUNT(*) AS current_count
FROM dim_fund
WHERE is_current = TRUE
GROUP BY 1, 2
HAVING COUNT(*) > 1;  -- Should return zero rows
```

### Q: Why use a surrogate key instead of the natural key in fact tables?

**A:** Three reasons:
1. **History linking** -- The natural key `FND001/A` has multiple rows in `dim_fund` (one per version). The surrogate key `fund_key` uniquely identifies the exact version.
2. **Join performance** -- Integer joins are faster than multi-column VARCHAR joins.
3. **Insulation** -- If the source system changes how it generates `fund_code`, the warehouse is unaffected.

### Q: How does the procedure handle NULLs in SCD attributes?

**A:** It uses `COALESCE(CAST(column AS VARCHAR), '__NULL__')` so that a change from NULL to a value (or vice versa) is correctly detected as a change.

### Q: What if a record changes back to its original value?

**A:** A new version is still created. For example, if `management_fee` goes from 0.0075 to 0.0080 and back to 0.0075, three versions exist:

| version | management_fee | effective_date | end_date |
|---------|----------------|----------------|----------|
| 1 | 0.0075 | 2025-01-15 | 2025-02-14 |
| 2 | 0.0080 | 2025-02-15 | 2025-03-14 |
| 3 | 0.0075 | 2025-03-15 | NULL |

This is correct behavior -- the fee was actually different during that period.

### Q: How do you handle late-arriving dimension data?

**A:** When a fact record arrives before its dimension member exists, you insert a placeholder dimension row (often called an "inferred member") with a default surrogate key. When the real dimension data arrives later, the SCD2 process creates the proper row and the placeholder can be expired.

### Q: What is the difference between `end_date` and `is_current`?

**A:** They are redundant by design for query convenience:
- `is_current = TRUE` is simpler in `WHERE` clauses: `JOIN dim_fund f ON ... AND f.is_current = TRUE`
- `end_date IS NULL` achieves the same result but `end_date` is also useful for **date-range queries**: "What was the fund's fee on March 1?" → `WHERE '2025-03-01' BETWEEN effective_date AND COALESCE(end_date, '9999-12-31')`

### Q: What are the performance implications of SCD2?

**A:** The dimension table grows with every change. Mitigations:
- **Sort key** on `(natural_key, effective_date)` for efficient range scans
- **`is_current` filter** eliminates historical rows for most queries
- **VACUUM and ANALYZE** (handled by `stored_procedures/08_table_maintenance.sql`) keep Redshift statistics current
- Dimension tables are typically small compared to fact tables, so the growth is manageable

---

## Summary

| Concept | In This Project |
|---------|-----------------|
| SCD2 procedure | `stored_procedures/03_scd_type2.sql` → `sp_process_scd_type2` |
| Dimension config | `configs/dimensional_mappings.json` → `scd_type: 2` per dimension |
| Glue job | `glue_jobs/with_data_model/glue_job_with_data_model.py` |
| SCD2 dimensions | `dim_fund`, `dim_advisor`, `dim_cost_center` |
| SCD1 dimensions | `dim_account_type`, `dim_version`, `dim_date` |
| Surrogate key | Auto-managed via `ROW_NUMBER() + MAX(key)` in the procedure |
| Change detection | Column-by-column comparison with NULL handling |
| Fact table linkage | Fact rows store surrogate keys, preserving point-in-time accuracy |
