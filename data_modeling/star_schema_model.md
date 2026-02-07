# Dimensional Data Model for NYL Investment Management Data Warehouse

## Executive Summary

Based on your PySpark implementation and the Anaplan data sources, here's a complete **star schema dimensional model** using Kimball methodology.

---

## 1. Bus Matrix

| Fact Table | Fund | Advisor | Cost Center | Date | Version | Share Class | Account Type |
|------------|------|---------|-------------|------|---------|-------------|--------------|
| Fact_AUM_Revenue | ✓ | ✓ | | ✓ | ✓ | ✓ | |
| Fact_Revenue_Plan | ✓ | | | ✓ | ✓ | ✓ | |
| Fact_Expense | | | ✓ | ✓ | ✓ | | ✓ |
| Fact_Headcount | | | ✓ | ✓ | ✓ | | |
| Fact_ZAUM | ✓ | | | ✓ | ✓ | | |
| Fact_GA_Invoice | | | | ✓ | ✓ | | ✓ |

---

## 2. Dimension Tables (Conformed Dimensions)

### 2.1 Dim_Date (Date Dimension)

**Type:** Type 1 (Static)  
**Grain:** One row per day  
**Refresh:** Pre-populated, no updates

```sql
CREATE TABLE dim_date (
    date_key                INT NOT NULL,              -- YYYYMMDD format
    full_date               DATE NOT NULL,
    day_of_week             SMALLINT,                  -- 1-7
    day_name                VARCHAR(10),               -- Monday, Tuesday...
    day_of_month            SMALLINT,                  -- 1-31
    day_of_year             SMALLINT,                  -- 1-366
    week_of_year            SMALLINT,                  -- 1-53
    month_number            SMALLINT,                  -- 1-12
    month_name              VARCHAR(10),               -- January, February...
    month_abbr              VARCHAR(3),                -- Jan, Feb...
    quarter                 SMALLINT,                  -- 1-4
    quarter_name            VARCHAR(2),                -- Q1, Q2, Q3, Q4
    year                    SMALLINT,
    fiscal_year             SMALLINT,
    fiscal_quarter          SMALLINT,
    fiscal_month            SMALLINT,
    is_weekend              BOOLEAN,
    is_holiday              BOOLEAN,
    holiday_name            VARCHAR(50),
    is_business_day         BOOLEAN,
    prior_day_key           INT,
    next_day_key            INT,
    same_day_prior_year_key INT,
    
    -- Metadata
    created_date            TIMESTAMP DEFAULT GETDATE(),
    
    PRIMARY KEY (date_key)
)
DISTSTYLE ALL                                          -- Small, replicate to all nodes
SORTKEY (date_key);
```

**Sample Data:**
```
date_key  | full_date  | month_name | quarter | fiscal_year | is_business_day
----------|------------|------------|---------|-------------|----------------
20250115  | 2025-01-15 | January    | 1       | 2025        | TRUE
20250116  | 2025-01-16 | January    | 1       | 2025        | TRUE
```

---

### 2.2 Dim_Fund (Fund/Share Class Dimension)

**Type:** SCD Type 2 (Track history of fee changes)  
**Grain:** One row per fund + share class + version  
**Natural Key:** fund_code + share_class  
**Business Key:** fund_key (surrogate)

```sql
CREATE TABLE dim_fund (
    fund_key                    INT NOT NULL,          -- Surrogate key
    
    -- Natural Keys
    fund_code                   VARCHAR(50) NOT NULL,
    share_class                 VARCHAR(10) NOT NULL,
    
    -- Attributes (SCD Type 2)
    fund_name                   VARCHAR(256),
    fund_family_code            VARCHAR(50),
    fund_family_name            VARCHAR(256),
    share_class_code            VARCHAR(50),
    
    -- Financial Attributes (Track changes)
    management_fee              NUMERIC(5,4),
    expense_ratio               NUMERIC(5,4),
    
    -- Classifications (Type 2)
    asset_class                 VARCHAR(100),          -- Equity, Fixed Income, etc.
    strategy                    VARCHAR(100),          -- Growth, Value, Index, etc.
    
    -- Additional Attributes
    inception_date              DATE,
    status                      VARCHAR(20),           -- Active, Closed, Liquidated
    
    -- SCD Type 2 Metadata
    effective_date              DATE NOT NULL,
    end_date                    DATE,                  -- NULL for current
    is_current                  BOOLEAN NOT NULL,
    version                     INT NOT NULL,
    
    -- Audit
    created_date                TIMESTAMP DEFAULT GETDATE(),
    updated_date                TIMESTAMP DEFAULT GETDATE(),
    created_by                  VARCHAR(50) DEFAULT 'glue_etl',
    
    PRIMARY KEY (fund_key)
)
DISTSTYLE KEY
DISTKEY (fund_key)
SORTKEY (fund_code, share_class, effective_date);

-- Index for SCD lookups
CREATE INDEX idx_fund_natural_key ON dim_fund(fund_code, share_class, is_current);
```

**Sample Data:**
```
fund_key | fund_code | share_class | fund_name        | mgmt_fee | effective_date | end_date   | is_current | version
---------|-----------|-------------|------------------|----------|----------------|------------|------------|--------
1        | FND001    | A           | Growth Fund      | 0.0075   | 2024-01-01     | 2024-12-31 | FALSE      | 1
2        | FND001    | A           | Growth Fund      | 0.0080   | 2025-01-01     | NULL       | TRUE       | 2
3        | FND002    | I           | Bond Fund        | 0.0050   | 2024-01-01     | NULL       | TRUE       | 1
```

---

### 2.3 Dim_Advisor (Advisor Dimension)

**Type:** SCD Type 2 (Track advisor reassignments)  
**Grain:** One row per advisor + version  
**Natural Key:** advisor_code  
**Business Key:** advisor_key

```sql
CREATE TABLE dim_advisor (
    advisor_key                 INT NOT NULL,          -- Surrogate key
    
    -- Natural Key
    advisor_code                VARCHAR(50) NOT NULL,
    
    -- Attributes
    advisor_name                VARCHAR(256),
    advisor_email               VARCHAR(100),
    advisor_phone               VARCHAR(20),
    
    -- Hierarchies
    team_code                   VARCHAR(50),
    team_name                   VARCHAR(100),
    region_code                 VARCHAR(50),
    region_name                 VARCHAR(100),
    division_code               VARCHAR(50),
    division_name               VARCHAR(100),
    
    -- Status
    status                      VARCHAR(20),           -- Active, Inactive, Terminated
    hire_date                   DATE,
    termination_date            DATE,
    
    -- SCD Type 2
    effective_date              DATE NOT NULL,
    end_date                    DATE,
    is_current                  BOOLEAN NOT NULL,
    version                     INT NOT NULL,
    
    -- Audit
    created_date                TIMESTAMP DEFAULT GETDATE(),
    updated_date                TIMESTAMP DEFAULT GETDATE(),
    
    PRIMARY KEY (advisor_key)
)
DISTSTYLE KEY
DISTKEY (advisor_key)
SORTKEY (advisor_code, effective_date);
```

---

### 2.4 Dim_Cost_Center (Cost Center Hierarchy)

**Type:** SCD Type 2 (Track reorganizations)  
**Grain:** One row per cost center + version  
**Natural Key:** l4_cost_center  
**Business Key:** cost_center_key

```sql
CREATE TABLE dim_cost_center (
    cost_center_key             INT NOT NULL,          -- Surrogate key
    
    -- Natural Key (Lowest level)
    l4_cost_center_code         VARCHAR(50) NOT NULL,
    l4_cost_center_name         VARCHAR(500),
    
    -- Hierarchy (Bottom-up)
    l3_cost_center_code         VARCHAR(50),
    l3_cost_center_name         VARCHAR(500),
    l2_cost_center_code         VARCHAR(50),
    l2_cost_center_name         VARCHAR(500),
    l1_cost_center_code         VARCHAR(50),
    l1_cost_center_name         VARCHAR(500),
    
    -- Attributes
    department                  VARCHAR(100),
    business_unit               VARCHAR(100),
    cost_type                   VARCHAR(50),           -- Direct, Indirect, Overhead
    budget_owner                VARCHAR(100),
    
    -- Status
    status                      VARCHAR(20),           -- Active, Inactive
    
    -- SCD Type 2
    effective_date              DATE NOT NULL,
    end_date                    DATE,
    is_current                  BOOLEAN NOT NULL,
    version                     INT NOT NULL,
    
    -- Audit
    created_date                TIMESTAMP DEFAULT GETDATE(),
    updated_date                TIMESTAMP DEFAULT GETDATE(),
    
    PRIMARY KEY (cost_center_key)
)
DISTSTYLE KEY
DISTKEY (cost_center_key)
SORTKEY (l4_cost_center_code, effective_date);
```

---

### 2.5 Dim_Version (Plan/Forecast/Actual)

**Type:** Type 1 (Static reference)  
**Grain:** One row per version type  
**Natural Key:** version_code

```sql
CREATE TABLE dim_version (
    version_key                 INT NOT NULL,          -- Surrogate key
    version_code                VARCHAR(50) NOT NULL,  -- Natural key
    version_name                VARCHAR(100),
    version_type                VARCHAR(20),           -- Plan, Forecast, Actual
    version_category            VARCHAR(50),           -- Budget, Revised, Final
    display_order               INT,
    is_active                   BOOLEAN,
    
    -- Audit
    created_date                TIMESTAMP DEFAULT GETDATE(),
    
    PRIMARY KEY (version_key)
)
DISTSTYLE ALL;                                         -- Small, replicate

-- Seed data
INSERT INTO dim_version VALUES
(1, 'ACTUAL', 'Actual', 'Actual', 'Final', 1, TRUE),
(2, 'PLAN', 'Plan', 'Plan', 'Budget', 2, TRUE),
(3, 'FORECAST', 'Forecast', 'Forecast', 'Revised', 3, TRUE),
(4, 'REVISED_PLAN', 'Revised Plan', 'Plan', 'Revised', 4, TRUE);
```

---

### 2.6 Dim_Account_Type (General Account Categories)

**Type:** Type 1  
**Grain:** One row per account type

```sql
CREATE TABLE dim_account_type (
    account_type_key            INT NOT NULL,
    account_type_code           VARCHAR(50) NOT NULL,
    account_type_name           VARCHAR(100),
    account_category            VARCHAR(50),           -- General Account, Separate Account
    
    PRIMARY KEY (account_type_key)
)
DISTSTYLE ALL;
```

---

### 2.7 Dim_Data_Source (Source System Tracking)

**Type:** Type 1  
**Grain:** One row per data source

```sql
CREATE TABLE dim_data_source (
    data_source_key             INT NOT NULL,
    data_source_code            VARCHAR(50) NOT NULL,
    data_source_name            VARCHAR(100),
    source_system               VARCHAR(50),           -- Anaplan, Manual, etc.
    
    PRIMARY KEY (data_source_key)
)
DISTSTYLE ALL;
```

---

## 3. Fact Tables

### 3.1 Fact_AUM_Revenue (AUM and Revenue Facts)

**Type:** Transaction Fact  
**Grain:** One row per Fund + Share Class + Advisor + Date + Version  
**Source:** `aum_revenue` table

```sql
CREATE TABLE fact_aum_revenue (
    -- Dimension Keys (Composite Primary Key)
    fund_key                    INT NOT NULL,
    advisor_key                 INT NOT NULL,
    date_key                    INT NOT NULL,
    version_key                 INT NOT NULL,
    data_source_key             INT NOT NULL,
    
    -- Degenerate Dimensions
    data_index                  VARCHAR(70),           -- Original business key
    
    -- Measures (Additive)
    fee_paying_aum              NUMERIC(18,2),
    reportable_aum              NUMERIC(18,2),
    revenue                     NUMERIC(18,2),
    
    -- Measures (Non-Additive)
    effective_fee_rate          NUMERIC(7,6),          -- Calculated: revenue / fee_paying_aum
    
    -- Additional Attributes
    expense_aum_revenue_key     VARCHAR(256),          -- Business identifier
    
    -- Audit Columns
    load_timestamp              TIMESTAMP DEFAULT GETDATE(),
    source_file                 VARCHAR(500),
    
    PRIMARY KEY (fund_key, advisor_key, date_key, version_key)
)
DISTSTYLE KEY
DISTKEY (fund_key)
SORTKEY (date_key, fund_key);

-- Foreign Keys (Enforced in ETL, not in Redshift)
-- FOREIGN KEY (fund_key) REFERENCES dim_fund(fund_key)
-- FOREIGN KEY (advisor_key) REFERENCES dim_advisor(advisor_key)
-- FOREIGN KEY (date_key) REFERENCES dim_date(date_key)
-- FOREIGN KEY (version_key) REFERENCES dim_version(version_key)
```

**Aggregation Example:**
```sql
-- Total AUM by Fund Family and Month
SELECT 
    f.fund_family_name,
    d.year,
    d.month_name,
    v.version_name,
    SUM(fa.fee_paying_aum) as total_aum,
    SUM(fa.revenue) as total_revenue
FROM fact_aum_revenue fa
JOIN dim_fund f ON fa.fund_key = f.fund_key AND f.is_current = TRUE
JOIN dim_date d ON fa.date_key = d.date_key
JOIN dim_version v ON fa.version_key = v.version_key
WHERE d.year = 2025
  AND v.version_code = 'ACTUAL'
GROUP BY 1, 2, 3, 4;
```

---

### 3.2 Fact_Revenue_Plan (Revenue Plan/Forecast)

**Type:** Periodic Snapshot Fact  
**Grain:** One row per Fund + Share Class + Date + Version + DC Tier  
**Source:** `revenue_plan_forecast` table

```sql
CREATE TABLE fact_revenue_plan (
    -- Dimension Keys
    fund_key                    INT NOT NULL,
    date_key                    INT NOT NULL,
    version_key                 INT NOT NULL,
    data_source_key             INT NOT NULL,
    
    -- Degenerate Dimensions
    data_index                  VARCHAR(70),
    
    -- Additional Dimensions (Degenerate or Mini-dimensions)
    dc_tier1                    VARCHAR(256),
    dc_tier2                    VARCHAR(256),
    dc_tier3                    VARCHAR(256),
    dc_tier4                    VARCHAR(256),
    data_source                 VARCHAR(256),          -- Original source field
    reporting_currency          VARCHAR(10),
    assets_under_admin          VARCHAR(256),
    
    -- Measures
    fee_paying_aum              NUMERIC(18,2),
    reportable_aum              NUMERIC(18,2),
    revenue                     NUMERIC(18,2),
    
    -- Business Keys
    expense_aum_revenue_key     VARCHAR(256),
    
    -- Audit
    load_timestamp              TIMESTAMP DEFAULT GETDATE(),
    
    PRIMARY KEY (fund_key, date_key, version_key, data_index)
)
DISTSTYLE KEY
DISTKEY (fund_key)
SORTKEY (date_key, version_key);
```

---

### 3.3 Fact_Expense (Expense Facts)

**Type:** Transaction Fact  
**Grain:** One row per Cost Center + Date + Version  
**Source:** `expense` table

```sql
CREATE TABLE fact_expense (
    -- Dimension Keys
    cost_center_key             INT NOT NULL,
    date_key                    INT NOT NULL,
    version_key                 INT NOT NULL,
    data_source_key             INT NOT NULL,
    
    -- Degenerate Dimensions
    data_index                  VARCHAR(70),
    
    -- Measures (Additive)
    expense_amount              NUMERIC(18,2),
    
    -- Audit
    load_timestamp              TIMESTAMP DEFAULT GETDATE(),
    
    PRIMARY KEY (cost_center_key, date_key, version_key)
)
DISTSTYLE KEY
DISTKEY (cost_center_key)
SORTKEY (date_key, cost_center_key);
```

**Budget vs Actual Analysis:**
```sql
-- Compare Budget to Actual by Department
SELECT 
    cc.l2_cost_center_name as department,
    d.year,
    d.quarter_name,
    SUM(CASE WHEN v.version_code = 'PLAN' THEN fe.expense_amount END) as budgeted,
    SUM(CASE WHEN v.version_code = 'ACTUAL' THEN fe.expense_amount END) as actual,
    SUM(CASE WHEN v.version_code = 'ACTUAL' THEN fe.expense_amount END) - 
    SUM(CASE WHEN v.version_code = 'PLAN' THEN fe.expense_amount END) as variance
FROM fact_expense fe
JOIN dim_cost_center cc ON fe.cost_center_key = cc.cost_center_key AND cc.is_current = TRUE
JOIN dim_date d ON fe.date_key = d.date_key
JOIN dim_version v ON fe.version_key = v.version_key
WHERE d.year = 2025
GROUP BY 1, 2, 3;
```

---

### 3.4 Fact_Headcount (Headcount Facts)

**Type:** Periodic Snapshot Fact  
**Grain:** One row per Cost Center + Date + Version  
**Source:** `headcount` table

```sql
CREATE TABLE fact_headcount (
    -- Dimension Keys
    cost_center_key             INT NOT NULL,
    date_key                    INT NOT NULL,
    version_key                 INT NOT NULL,
    data_source_key             INT NOT NULL,
    
    -- Degenerate Dimensions
    cc_period_id                VARCHAR(200),          -- Original business key
    
    -- Measures (Semi-Additive - sum across cost centers, not time)
    headcount                   INT,
    
    -- Audit
    load_timestamp              TIMESTAMP DEFAULT GETDATE(),
    
    PRIMARY KEY (cost_center_key, date_key, version_key)
)
DISTSTYLE KEY
DISTKEY (cost_center_key)
SORTKEY (date_key);
```

**Headcount Trend:**
```sql
-- Headcount by Department Over Time
SELECT 
    cc.l2_cost_center_name,
    d.year,
    d.month_name,
    SUM(fh.headcount) as total_headcount
FROM fact_headcount fh
JOIN dim_cost_center cc ON fh.cost_center_key = cc.cost_center_key AND cc.is_current = TRUE
JOIN dim_date d ON fh.date_key = d.date_key
JOIN dim_version v ON fh.version_key = v.version_key
WHERE v.version_code = 'ACTUAL'
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3;
```

---

### 3.5 Fact_ZAUM (Zero-AUM Facts)

**Type:** Periodic Snapshot  
**Grain:** One row per Date + Version  
**Source:** `zaum` table

```sql
CREATE TABLE fact_zaum (
    -- Dimension Keys
    date_key                    INT NOT NULL,
    version_key                 INT NOT NULL,
    data_source_key             INT NOT NULL,
    
    -- Degenerate Dimensions
    data_index                  VARCHAR(70),
    
    -- Measures
    zaum_value                  NUMERIC(18,2),
    
    -- Audit
    load_timestamp              TIMESTAMP DEFAULT GETDATE(),
    
    PRIMARY KEY (date_key, version_key)
)
DISTSTYLE ALL                                          -- Small table
SORTKEY (date_key);
```

---

### 3.6 Fact_GA_Invoice (General Account Invoice)

**Type:** Transaction Fact  
**Grain:** One row per Invoice + Date + Version  
**Source:** `general_account_invoice` table

```sql
CREATE TABLE fact_ga_invoice (
    -- Dimension Keys
    date_key                    INT NOT NULL,
    version_key                 INT NOT NULL,
    account_type_key            INT NOT NULL,
    data_source_key             INT NOT NULL,
    
    -- Degenerate Dimensions
    data_index                  VARCHAR(70),
    invoice_number              VARCHAR(50),
    
    -- Measures
    invoice_amount              NUMERIC(18,2),
    
    -- Audit
    load_timestamp              TIMESTAMP DEFAULT GETDATE(),
    
    PRIMARY KEY (data_index, date_key, version_key)
)
DISTSTYLE KEY
DISTKEY (date_key)
SORTKEY (date_key);
```

---

## 4. Staging Tables (for MERGE operations)

Each dimension and fact needs a staging table:

```sql
-- Example: Staging for Dim_Fund
CREATE TABLE stg_dim_fund (
    LIKE dim_fund INCLUDING DEFAULTS
);

-- Example: Staging for Fact_AUM_Revenue
CREATE TABLE stg_fact_aum_revenue (
    LIKE fact_aum_revenue INCLUDING DEFAULTS
);
```

---

## 5. Entity Relationship Diagram (ERD)

```
                    ┌──────────────┐
                    │  Dim_Date    │
                    │  (date_key)  │
                    └──────┬───────┘
                           │
        ┌──────────────────┼──────────────────┬─────────────────┐
        │                  │                  │                 │
   ┌────▼─────┐       ┌────▼─────┐      ┌────▼─────┐    ┌─────▼──────┐
   │  Fact_   │       │  Fact_   │      │  Fact_   │    │   Fact_    │
   │   AUM    │       │ Revenue  │      │ Expense  │    │ Headcount  │
   │ Revenue  │       │   Plan   │      └────┬─────┘    └─────┬──────┘
   └────┬─────┘       └────┬─────┘           │                │
        │                  │                 │                │
   ┌────▼─────┐       ┌────▼─────┐      ┌────▼─────┐    ┌────▼──────┐
   │ Dim_Fund │       │ Dim_Fund │      │  Dim_    │    │   Dim_    │
   │ (SCD2)   │       │ (SCD2)   │      │   Cost   │    │   Cost    │
   └────┬─────┘       └──────────┘      │  Center  │    │  Center   │
        │                               │  (SCD2)  │    │  (SCD2)   │
   ┌────▼─────┐                        └──────────┘    └───────────┘
   │   Dim_   │
   │ Advisor  │
   │ (SCD2)   │
   └──────────┘

   All Facts connect to:
   - Dim_Version
   - Dim_Data_Source
```

---

## 6. Dimensional Model Validation Queries

### 6.1 Check Dimension Integrity

```sql
-- Verify no overlapping SCD Type 2 records
SELECT 
    fund_code,
    share_class,
    COUNT(*) as version_count,
    SUM(CASE WHEN is_current = TRUE THEN 1 ELSE 0 END) as current_count
FROM dim_fund
GROUP BY 1, 2
HAVING current_count != 1  -- Should always be exactly 1
   OR COUNT(*) < 1;        -- Should have at least 1 version
```

### 6.2 Orphan Fact Records

```sql
-- Find facts without valid dimension keys
SELECT 
    'Missing Fund' as issue,
    COUNT(*) as record_count
FROM fact_aum_revenue fa
LEFT JOIN dim_fund f ON fa.fund_key = f.fund_key
WHERE f.fund_key IS NULL

UNION ALL

SELECT 
    'Missing Advisor',
    COUNT(*)
FROM fact_aum_revenue fa
LEFT JOIN dim_advisor a ON fa.advisor_key = a.advisor_key
WHERE a.advisor_key IS NULL;
```

---

## 7. Implementation Checklist

- [ ] **Phase 1: Date Dimension**
  - [ ] Generate date records (2020-2030)
  - [ ] Populate fiscal calendar
  - [ ] Add holiday calendar

- [ ] **Phase 2: Conformed Dimensions**
  - [ ] Dim_Version (seed static data)
  - [ ] Dim_Data_Source (seed static data)
  - [ ] Dim_Account_Type (seed static data)

- [ ] **Phase 3: SCD Type 2 Dimensions**
  - [ ] Dim_Fund (implement full SCD2 in Glue)
  - [ ] Dim_Advisor (implement full SCD2 in Glue)
  - [ ] Dim_Cost_Center (implement full SCD2 in Glue)

- [ ] **Phase 4: Fact Tables**
  - [ ] Fact_AUM_Revenue
  - [ ] Fact_Revenue_Plan
  - [ ] Fact_Expense
  - [ ] Fact_Headcount
  - [ ] Fact_ZAUM
  - [ ] Fact_GA_Invoice

- [ ] **Phase 5: Validation**
  - [ ] Referential integrity checks
  - [ ] Data quality tests
  - [ ] Performance testing

---

## 8. Next Steps

1. **Review and validate grain definitions** with business users
2. **Confirm SCD Type 2 attributes** for each dimension
3. **Create DDL scripts** for all tables
4. **Implement Glue jobs** following the PySpark patterns
5. **Set up data quality monitoring**

Would you like me to:
1. Generate the complete DDL for all tables?
2. Create specific Glue job implementations for any dimension?
3. Design aggregate fact tables for performance?