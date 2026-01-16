# Investment Management Dimensional Data Model Design
## Complete Guide: Star vs Snowflake, Grain, SCD, and Best Practices

---

## Table of Contents

1. [Star Schema vs Snowflake Schema Analysis](#schema-analysis)
2. [Recommended Dimensional Model](#recommended-model)
3. [Grain Definition](#grain-definition)
4. [Cardinality Analysis](#cardinality)
5. [Slowly Changing Dimensions (SCD)](#scd-patterns)
6. [Data Quality Constraints](#data-quality)
7. [Normalization vs Denormalization](#normalization)
8. [Late Arriving Facts and Dimensions](#late-arriving)
9. [Incremental Loading Strategy](#incremental-loading)
10. [Complete DDL Scripts](#ddl-scripts)

---

## 1. Star Schema vs Snowflake Schema Analysis {#schema-analysis}

### Current State Analysis

Your current tables have **embedded hierarchies**:
- Cost Center: L1 → L2 → L3 → L4
- Distribution Channel: dc_tier1 → dc_tier2 → dc_tier3 → dc_tier4
- Fund Family → Fund → Share Class

This is **denormalized** (Star Schema characteristics)

### Star Schema (Recommended for Your Use Case)

**Structure:**
```
Fact Table (center)
    ├─ Dimension 1 (denormalized - all attributes in one table)
    ├─ Dimension 2 (denormalized)
    └─ Dimension 3 (denormalized)
```

**Pros:**
- ✅ **Fast query performance** (fewer JOINs)
- ✅ Simpler for BI tools to query
- ✅ Easier for business users to understand
- ✅ Better for aggregations and reporting

**Cons:**
- ❌ Data redundancy
- ❌ Larger dimension tables
- ❌ More storage space

**When to Use:**
- Query performance is critical
- Business users run ad-hoc queries
- Dimension updates are infrequent
- **← Your investment management scenario fits here**

---

### Snowflake Schema (Alternative)

**Structure:**
```
Fact Table (center)
    ├─ Dimension 1 (normalized)
    │   └─ Sub-dimension 1a
    │       └─ Sub-dimension 1b
    └─ Dimension 2 (normalized)
        └─ Sub-dimension 2a
```

**Pros:**
- ✅ Less data redundancy
- ✅ Smaller dimension tables
- ✅ Easier to maintain hierarchies
- ✅ Better data integrity

**Cons:**
- ❌ More complex queries (more JOINs)
- ❌ Slower query performance
- ❌ Harder for business users

**When to Use:**
- Storage is a concern
- Dimension hierarchies change frequently
- Data integrity is paramount

---

### Recommendation for Your Data: **Hybrid Star Schema**

Use **Star Schema as the foundation** with **selective normalization** for:
1. **Cost Center Dimension** - Keep denormalized (L1-L4 in one table)
2. **Fund Dimension** - Keep denormalized (Family → Fund → Share Class)
3. **Date Dimension** - Standard denormalized
4. **Account/Client Dimension** - Can normalize if very large

**Why Hybrid?**
- Primary use case: Financial reporting and analysis → Star Schema
- Some dimensions (like Advisor hierarchy) might benefit from normalization
- Flexibility to optimize specific dimensions

---

## 2. Recommended Dimensional Model {#recommended-model}

### High-Level Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     FACT TABLES (Star Center)               │
├─────────────────────────────────────────────────────────────┤
│  • Fact_AUM_Revenue (grain: fund/share class/advisor/day)  │
│  • Fact_Expense (grain: cost center/GL account/day)        │
│  • Fact_Headcount (grain: cost center/month)               │
│  • Fact_Invoice (grain: invoice line item)                 │
└─────────────────────────────────────────────────────────────┘
                            │
        ┌───────────────────┼───────────────────┐
        ▼                   ▼                   ▼
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│ Dim_Date    │     │ Dim_Fund    │     │ Dim_Cost    │
│ (Type 0)    │     │ (Type 2)    │     │ Center      │
└─────────────┘     └─────────────┘     │ (Type 2)    │
        ▼                   ▼            └─────────────┘
┌─────────────┐     ┌─────────────┐            ▼
│ Dim_Period  │     │ Dim_Advisor │     ┌─────────────┐
│ (Type 0)    │     │ (Type 2)    │     │ Dim_GL      │
└─────────────┘     └─────────────┘     │ Account     │
                            ▼            │ (Type 1)    │
                    ┌─────────────┐     └─────────────┘
                    │ Dim_         │
                    │ Distribution│
                    │ Channel     │
                    │ (Type 2)    │
                    └─────────────┘
```

---

## 3. Grain Definition {#grain-definition}

**Grain = The atomic level of detail stored in a fact table**

### Fact_AUM_Revenue

**Grain:** One row per **Fund/Share Class/Advisor/Date/Version**

```
Business Question: 
"What was the AUM and revenue for ABC Growth Fund, Class A shares, 
 managed by Advisor John Smith, on December 15, 2024?"

Grain Components:
- Fund Key (identifies fund + share class)
- Advisor Key
- Date Key
- Version (Plan/Forecast/Actual)
```

**Why this grain?**
- Supports daily reporting
- Allows advisor-level analysis
- Can aggregate up to any level (fund family, month, etc.)

**Grain too fine?** ❌ No - can't go below daily for AUM
**Grain too coarse?** ❌ No - need advisor-level detail

---

### Fact_Expense

**Grain:** One row per **Cost Center (L4)/GL Account/Date/Version**

```
Business Question:
"What expenses did the Technology Department (L4 cost center) 
 incur for Software Licenses (GL Account 6000) on Oct 5, 2024?"

Grain Components:
- Cost Center Key (L4 level)
- GL Account Key
- Date Key
- Version (Plan/Forecast/Actual)
```

---

### Fact_Headcount

**Grain:** One row per **Cost Center (L4)/Month/Version**

```
Business Question:
"How many employees were in the Investment Operations department 
 in Q3 2024 according to the Plan?"

Grain Components:
- Cost Center Key (L4 level)
- Period Key (Month level)
- Version (Plan/Forecast/Actual)

Note: Headcount is typically monthly, not daily
```

---

### Fact_Invoice

**Grain:** One row per **Invoice Line Item**

```
Business Question:
"What was the charge for Custom Reporting services 
 on Invoice INV-2024-12345?"

Grain Components:
- Invoice Key
- Invoice Line Number
- Date Key
- Customer Key
```

---

### Critical Grain Rules

1. **Consistency:** All measures in a fact table must be at the same grain
2. **Atomic:** Store at the lowest sensible level of detail
3. **No mixing:** Don't mix daily and monthly facts in same table
4. **Additive:** Grain should support SUM aggregation across dimensions

**Example of WRONG grain:**
```sql
-- BAD: Mixing grains
Fact_Revenue (
    fund_key,      -- Daily grain
    date_key,      -- Daily grain
    month_key,     -- Monthly grain ← WRONG! Mixed grain
    aum,           -- Daily measure
    monthly_total  -- Monthly measure ← WRONG! Mixed grain
)
```

**Correct approach:**
```sql
-- GOOD: Consistent grain
Fact_AUM_Revenue_Daily (
    fund_key,
    date_key,
    aum,
    revenue
)

-- Separate table or aggregate from daily
Fact_AUM_Revenue_Monthly (
    fund_key,
    period_key,
    avg_aum,
    total_revenue
)
```

---

## 4. Cardinality Analysis {#cardinality}

**Cardinality = Relationship between tables (1:1, 1:Many, Many:Many)**

### Dimension to Fact Relationships

#### Date to Facts: **1:Many**
```
One Date (e.g., Jan 15, 2025)
    → Many AUM transactions
    → Many Expense records
    → Many Invoice line items
```

#### Fund to AUM Revenue: **1:Many**
```
One Fund (e.g., "Growth Fund")
    → Many daily AUM records (one per date)
    → Many advisors managing it
```

#### Cost Center to Expense: **1:Many**
```
One Cost Center (e.g., "Technology - L4")
    → Many expense transactions
    → Multiple GL accounts
```

#### Advisor to AUM Revenue: **1:Many**
```
One Advisor
    → Manages multiple funds
    → Has revenue records across many dates
```

---

### Dimension Hierarchy Cardinality

#### Cost Center Hierarchy (L1 → L2 → L3 → L4)
```
L1: Company                    [1 record]
  └─ L2: Division              [1:Many]  (5-10 divisions)
      └─ L3: Department        [1:Many]  (50-100 departments)
          └─ L4: Team          [1:Many]  (200-500 teams)
```

**Cardinality:**
- 1 L1 : Many L2 (1:N)
- 1 L2 : Many L3 (1:N)
- 1 L3 : Many L4 (1:N)
- Overall: 1 L1 : 500 L4s (1:N relationship through hierarchy)

---

#### Fund Hierarchy (Family → Fund → Share Class)
```
Fund Family                     [1:Many]  (10-20 families)
  └─ Fund                       [1:Many]  (100-200 funds)
      └─ Share Class           [1:Many]  (300-600 share classes)
```

**Cardinality:**
- 1 Family : Many Funds (1:N)
- 1 Fund : Many Share Classes (1:N)
- Overall: 1 Family : 30 Share Classes average (1:N)

---

### Fact to Fact Relationships (Rare but possible)

#### AUM Revenue to Expense: **Many:Many through Dimensions**
```
One fund's AUM revenue → Allocated across multiple cost centers
One cost center's expenses → Supports multiple funds

Solution: Use bridge table or allocate through dimensions
```

---

### Cardinality Impact on Design

**High Cardinality Dimensions (100K+ rows):**
- Customer/Client dimension → Consider partitioning
- Account dimension → Index properly
- Date dimension → Standard (365 × years)

**Low Cardinality Dimensions (< 1000 rows):**
- Version (Plan/Forecast/Actual) → Only 3 values
- Fund Family → 10-20 values
- Can denormalize into facts if very stable

---

## 5. Slowly Changing Dimensions (SCD) {#scd-patterns}

### SCD Type 0: No Changes Allowed

**Use for:** Reference data that never changes

**Example: Dim_Date**
```sql
CREATE TABLE Dim_Date (
    date_key        INT PRIMARY KEY,  -- 20250115
    full_date       DATE,
    year            INT,
    quarter         INT,
    month           INT,
    day             INT,
    day_of_week     VARCHAR(10),
    is_weekend      BOOLEAN,
    fiscal_year     INT,
    fiscal_quarter  INT
);
```

**Why Type 0?** 
- January 15, 2025 will always be January 15, 2025
- Properties never change
- No history needed

---

### SCD Type 1: Overwrite (No History)

**Use for:** Corrections or attributes where history doesn't matter

**Example: Dim_GL_Account (update description)**
```sql
CREATE TABLE Dim_GL_Account (
    gl_account_key      INT PRIMARY KEY,
    gl_account_code     VARCHAR(20) NOT NULL,
    gl_account_name     VARCHAR(200),      -- Type 1: Overwrite
    gl_category         VARCHAR(100),       -- Type 1: Overwrite
    is_active           BOOLEAN,
    last_updated        TIMESTAMP
);
```

**Behavior:**
```
Before:
GL_Account_Key: 123
GL_Account_Code: 6000
GL_Account_Name: "Software Licenses"  ← Old value

After UPDATE:
GL_Account_Key: 123
GL_Account_Code: 6000
GL_Account_Name: "Software & Cloud Licenses"  ← Overwrites old value

Result: History is lost, all facts now point to new description
```

**When to use:**
- Typo corrections
- Formatting changes
- Non-critical attribute updates
- When you DON'T need historical values

---

### SCD Type 2: Add New Row (Full History)

**Use for:** Critical attributes where history matters

**Example: Dim_Fund (tracking fee changes)**
```sql
CREATE TABLE Dim_Fund (
    fund_key            INT PRIMARY KEY,         -- Surrogate key
    fund_code           VARCHAR(50) NOT NULL,    -- Natural key
    fund_name           VARCHAR(200),
    fund_family         VARCHAR(200),
    share_class         VARCHAR(50),
    management_fee      NUMERIC(5,4),            -- Type 2: Track changes
    
    -- SCD Type 2 tracking columns
    effective_date      DATE NOT NULL,
    end_date            DATE,                    -- NULL = current
    is_current          BOOLEAN DEFAULT TRUE,
    version             INT DEFAULT 1,
    
    -- Audit columns
    created_date        TIMESTAMP DEFAULT GETDATE(),
    updated_date        TIMESTAMP DEFAULT GETDATE()
);
```

**Behavior:**
```
January 2024: Fund ABC has 0.75% fee

fund_key | fund_code | management_fee | effective_date | end_date | is_current
---------|-----------|----------------|----------------|----------|------------
101      | ABC       | 0.0075         | 2024-01-01     | NULL     | TRUE

June 2024: Fee reduced to 0.65%

fund_key | fund_code | management_fee | effective_date | end_date | is_current
---------|-----------|----------------|----------------|----------|------------
101      | ABC       | 0.0075         | 2024-01-01     | 2024-05-31| FALSE  ← Closed
102      | ABC       | 0.0065         | 2024-06-01     | NULL      | TRUE   ← New row
```

**Historical Query:**
```sql
-- Revenue in March 2024 (uses 0.75% fee)
SELECT f.management_fee, fct.revenue
FROM Fact_AUM_Revenue fct
JOIN Dim_Fund f ON fct.fund_key = f.fund_key
WHERE fct.date_key = 20240315
  AND f.effective_date <= '2024-03-15'
  AND (f.end_date > '2024-03-15' OR f.end_date IS NULL);

-- Revenue in July 2024 (uses 0.65% fee)
SELECT f.management_fee, fct.revenue
FROM Fact_AUM_Revenue fct
JOIN Dim_Fund f ON fct.fund_key = f.fund_key
WHERE fct.date_key = 20240715
  AND f.effective_date <= '2024-07-15'
  AND (f.end_date > '2024-07-15' OR f.end_date IS NULL);
```

**When to use:**
- Fee changes
- Organizational changes (cost center restructuring)
- Advisor team changes
- Anything where you need to answer: "What was it THEN vs NOW?"

---

### SCD Type 3: Add New Column (Limited History)

**Use for:** When you only need to track current and previous value

**Example: Dim_Cost_Center (track previous manager)**
```sql
CREATE TABLE Dim_Cost_Center (
    cost_center_key     INT PRIMARY KEY,
    cost_center_code    VARCHAR(50),
    cost_center_name    VARCHAR(200),
    
    -- Current manager
    current_manager     VARCHAR(200),
    current_manager_date DATE,
    
    -- Previous manager (Type 3)
    previous_manager    VARCHAR(200),
    previous_manager_date DATE,
    
    l1_name             VARCHAR(200),
    l2_name             VARCHAR(200),
    l3_name             VARCHAR(200),
    l4_name             VARCHAR(200)
);
```

**Behavior:**
```
Before:
current_manager: "John Smith"
previous_manager: "Jane Doe"

After manager change:
current_manager: "Bob Johnson"    ← New manager
previous_manager: "John Smith"    ← Previous current becomes previous

Lost forever: "Jane Doe" (only keep 2 versions)
```

**When to use:**
- "Current vs Previous" comparisons
- Limited history is acceptable
- Don't want multiple dimension rows

**Limitations:**
- Only tracks 2 versions (can add more columns for more)
- Queries are harder
- Most projects use Type 2 instead

---

### SCD Type 6 (Hybrid): Type 1 + Type 2 + Type 3

**Combines all approaches for critical dimensions**

```sql
CREATE TABLE Dim_Advisor (
    advisor_key         INT PRIMARY KEY,
    advisor_code        VARCHAR(50),
    
    -- Type 1: Always current
    current_phone       VARCHAR(20),         -- Overwrite always
    current_email       VARCHAR(200),        -- Overwrite always
    
    -- Type 2: Full history
    advisor_name        VARCHAR(200),        -- Track changes
    team                VARCHAR(100),        -- Track changes
    effective_date      DATE,
    end_date            DATE,
    is_current          BOOLEAN,
    
    -- Type 3: Current + Previous
    current_manager     VARCHAR(200),
    previous_manager    VARCHAR(200)
);
```

---

### SCD Implementation for Your Tables

#### Dim_Fund: **Type 2** (Track fee changes, fund restructuring)
```sql
-- Track: Management fees, fund family changes, share class changes
effective_date, end_date, is_current, version
```

#### Dim_Cost_Center: **Type 2** (Track org restructuring)
```sql
-- Track: Department moves, hierarchy changes, manager changes
effective_date, end_date, is_current
```

#### Dim_Advisor: **Type 2** (Track team changes)
```sql
-- Track: Team assignments, manager changes
effective_date, end_date, is_current
```

#### Dim_GL_Account: **Type 1** (Just fix descriptions)
```sql
-- Overwrite: Account names, categories (corrections only)
last_updated
```

#### Dim_Date: **Type 0** (Never changes)
```sql
-- No tracking needed
```

---

## 6. Data Quality Constraints {#data-quality}

### Primary Key Constraints

**Every table must have a PRIMARY KEY:**

```sql
-- Dimension: Surrogate key
CREATE TABLE Dim_Fund (
    fund_key INT PRIMARY KEY,  -- Auto-incrementing surrogate
    ...
);

-- Fact: Composite key
CREATE TABLE Fact_AUM_Revenue (
    fund_key INT NOT NULL,
    advisor_key INT NOT NULL,
    date_key INT NOT NULL,
    version VARCHAR(20) NOT NULL,
    PRIMARY KEY (fund_key, advisor_key, date_key, version)
);
```

---

### Foreign Key Constraints

**Every dimension key in fact must exist in dimension:**

```sql
CREATE TABLE Fact_AUM_Revenue (
    fund_key INT NOT NULL,
    advisor_key INT NOT NULL,
    date_key INT NOT NULL,
    
    FOREIGN KEY (fund_key) REFERENCES Dim_Fund(fund_key),
    FOREIGN KEY (advisor_key) REFERENCES Dim_Advisor(advisor_key),
    FOREIGN KEY (date_key) REFERENCES Dim_Date(date_key)
);
```

**Problem:** What if dimension record doesn't exist yet?

**Solution:** Unknown/Default dimension members

```sql
-- Insert default "Unknown" record
INSERT INTO Dim_Fund (fund_key, fund_code, fund_name, is_current)
VALUES (-1, 'UNKNOWN', 'Unknown Fund', TRUE);

-- If fund not found, use -1
INSERT INTO Fact_AUM_Revenue (fund_key, ...)
VALUES (
    COALESCE(found_fund_key, -1),  -- Use -1 if NULL
    ...
);
```

---

### NOT NULL Constraints

**Critical fields must never be NULL:**

```sql
CREATE TABLE Fact_AUM_Revenue (
    fund_key INT NOT NULL,              -- Must have fund
    date_key INT NOT NULL,               -- Must have date
    version VARCHAR(20) NOT NULL,        -- Must have version
    
    fee_paying_aum NUMERIC(18,2),       -- Can be NULL (maybe no assets)
    revenue NUMERIC(18,2)                -- Can be NULL (maybe no fees)
);
```

**Rule:** Keys and business-critical attributes = NOT NULL

---

### CHECK Constraints

**Business rules enforced in database:**

```sql
CREATE TABLE Fact_AUM_Revenue (
    fee_paying_aum NUMERIC(18,2) CHECK (fee_paying_aum >= 0),  -- Can't be negative
    revenue NUMERIC(18,2) CHECK (revenue >= 0),                 -- Can't be negative
    management_fee NUMERIC(5,4) CHECK (management_fee BETWEEN 0 AND 0.05),  -- 0-5%
    
    version VARCHAR(20) CHECK (version IN ('Plan', 'Forecast', 'Actual'))
);

CREATE TABLE Dim_Fund (
    effective_date DATE NOT NULL,
    end_date DATE,
    CHECK (end_date IS NULL OR end_date > effective_date)  -- End must be after start
);
```

---

### UNIQUE Constraints

**Natural keys must be unique within active records:**

```sql
CREATE TABLE Dim_Fund (
    fund_key INT PRIMARY KEY,
    fund_code VARCHAR(50) NOT NULL,
    share_class VARCHAR(50) NOT NULL,
    effective_date DATE NOT NULL,
    end_date DATE,
    is_current BOOLEAN,
    
    -- Only one current record per fund_code + share_class
    UNIQUE (fund_code, share_class, is_current)
    WHERE is_current = TRUE
);
```

---

### Data Validation Rules

**Create validation table:**

```sql
CREATE TABLE Data_Quality_Rules (
    rule_id INT PRIMARY KEY,
    table_name VARCHAR(100),
    rule_description TEXT,
    validation_sql TEXT,
    severity VARCHAR(20)  -- ERROR, WARNING, INFO
);

-- Example rules
INSERT INTO Data_Quality_Rules VALUES
(1, 'Fact_AUM_Revenue', 'Revenue should match fee formula', 
 'SELECT * FROM Fact_AUM_Revenue WHERE ABS(revenue - (fee_paying_aum * management_fee / 100)) > 0.01',
 'ERROR'),

(2, 'Fact_Expense', 'Actual should not exceed Plan by >20%',
 'SELECT * FROM Fact_Expense WHERE version=''Actual'' AND expense_amount > (plan_amount * 1.2)',
 'WARNING');
```

---

### Referential Integrity for SCD Type 2

**Problem:** Facts might point to old dimension versions

**Solution:** Use effective date joins

```sql
-- CORRECT: Join with date awareness
SELECT 
    f.fund_code,
    f.management_fee,
    fct.revenue,
    fct.date_key
FROM Fact_AUM_Revenue fct
JOIN Dim_Fund f 
    ON fct.fund_key = f.fund_key
    AND fct.date_key BETWEEN f.effective_date AND COALESCE(f.end_date, '9999-12-31');
```

---

## 7. Normalization vs Denormalization {#normalization}

### Normalization (3NF) - Traditional OLTP

**Goal:** Eliminate redundancy

```sql
-- Normalized (3NF):
Table: Fund
- fund_id (PK)
- fund_name
- fund_family_id (FK)

Table: Fund_Family
- fund_family_id (PK)
- fund_family_name

Table: Share_Class
- share_class_id (PK)
- fund_id (FK)
- class_name
```

**Pros:** No redundancy, easy updates
**Cons:** Many JOINs, slower queries

---

### Denormalization - Star Schema (Recommended)

**Goal:** Optimize for query performance

```sql
-- Denormalized (Star):
Table: Dim_Fund
- fund_key (PK)
- fund_code
- fund_name
- fund_family_name        ← Denormalized
- fund_family_code        ← Denormalized
- share_class             ← Denormalized
- share_class_code        ← Denormalized
```

**Pros:** Fast queries, fewer JOINs
**Cons:** Redundant data, larger tables

---

### Decision Matrix: When to Normalize vs Denormalize

| Factor | Normalize (Snowflake) | Denormalize (Star) |
|--------|----------------------|-------------------|
| **Query Performance** | Lower | ✅ Higher |
| **Storage Space** | ✅ Less | More |
| **Update Frequency** | ✅ Frequent | Infrequent |
| **Dimension Size** | ✅ Very Large (millions) | Small-Medium (thousands) |
| **User Complexity** | Complex | ✅ Simple |
| **Business Users** | Technical | ✅ Non-technical |

---

### Your Investment Management Use Case

**Recommended Approach:**

#### Denormalize (Star Schema):
1. **Dim_Fund** - Keep fund family, fund, share class in one table
   - Why? Only 300-600 total share classes, rarely changes, frequently queried
   
2. **Dim_Cost_Center** - Keep L1-L4 hierarchy in one table
   - Why? Only 200-500 cost centers, stable hierarchy, common in reports

3. **Dim_Date** - Standard denormalized
   - Why? Industry standard, high performance

#### Normalize (Snowflake Schema):
1. **Dim_Advisor** hierarchy (if complex reporting structure)
   ```sql
   Dim_Advisor
   Dim_Advisor_Team
   Dim_Advisor_Region
   ```
   - Why? If you need to report by team/region independently

2. **Dim_Customer** (if very large)
   ```sql
   Dim_Customer
   Dim_Customer_Segment
   Dim_Customer_Geography
   ```
   - Why? If 100,000+ customers, reduce dimension size

---

### Example: Cost Center Dimension Design

#### Option 1: Fully Denormalized (Star) ✅ RECOMMENDED
```sql
CREATE TABLE Dim_Cost_Center (
    cost_center_key INT PRIMARY KEY,
    
    -- L4 (most granular)
    l4_code VARCHAR(50),
    l4_name VARCHAR(200),
    
    -- L3 (denormalized - redundant)
    l3_code VARCHAR(50),
    l3_name VARCHAR(200),
    
    -- L2 (denormalized - redundant)
    l2_code VARCHAR(50),
    l2_name VARCHAR(200),
    
    -- L1 (denormalized - redundant)
    l1_code VARCHAR(50),
    l1_name VARCHAR(200),
    
    -- SCD Type 2
    effective_date DATE,
    end_date DATE,
    is_current BOOLEAN
);

-- Query: Simple, fast
SELECT 
    l1_name, l2_name, SUM(expense_amount)
FROM Fact_Expense f
JOIN Dim_Cost_Center c ON f.cost_center_key = c.cost_center_key
GROUP BY l1_name, l2_name;
```

**Pros:**
- ✅ One JOIN
- ✅ Fast aggregation
- ✅ Simple for users

**Cons:**
- ❌ Redundant data (L1, L2, L3 repeated for each L4)
- ❌ Larger dimension table

---

#### Option 2: Normalized (Snowflake)
```sql
CREATE TABLE Dim_Cost_Center_L4 (
    l4_key INT PRIMARY KEY,
    l4_code VARCHAR(50),
    l4_name VARCHAR(200),
    l3_key INT REFERENCES Dim_Cost_Center_L3(l3_key)
);

CREATE TABLE Dim_Cost_Center_L3 (
    l3_key INT PRIMARY KEY,
    l3_code VARCHAR(50),
    l3_name VARCHAR(200),
    l2_key INT REFERENCES Dim_Cost_Center_L2(l2_key)
);

CREATE TABLE Dim_Cost_Center_L2 (
    l2_key INT PRIMARY KEY,
    l2_code VARCHAR(50),
    l2_name VARCHAR(200),
    l1_key INT REFERENCES Dim_Cost_Center_L1(l1_key)
);

CREATE TABLE Dim_Cost_Center_L1 (
    l1_key INT PRIMARY KEY,
    l1_code VARCHAR(50),
    l1_name VARCHAR(200)
);

-- Query: Complex, slower
SELECT 
    l1.l1_name, l2.l2_name, SUM(expense_amount)
FROM Fact_Expense f
JOIN Dim_Cost_Center_L4 l4 ON f.cost_center_key = l4.l4_key
JOIN Dim_Cost_Center_L3 l3 ON l4.l3_key = l3.l3_key
JOIN Dim_Cost_Center_L2 l2 ON l3.l2_key = l2.l2_key
JOIN Dim_Cost_Center_L1 l1 ON l2.l1_key = l1.l1_key
GROUP BY l1.l1_name, l2.l2_name;
```

**Pros:**
- ✅ No redundancy
- ✅ Easy to maintain hierarchy

**Cons:**
- ❌ Four JOINs
- ❌ Slower queries
- ❌ Complex for users

---

### Hybrid Approach (Best of Both)

**Strategy:** Denormalize dimensions, but create hierarchy bridge tables

```sql
-- Main dimension (denormalized for queries)
CREATE TABLE Dim_Cost_Center (
    cost_center_key INT PRIMARY KEY,
    l4_code VARCHAR(50),
    l4_name VARCHAR(200),
    l3_code VARCHAR(50),
    l3_name VARCHAR(200),
    l2_code VARCHAR(50),
    l2_name VARCHAR(200),
    l1_code VARCHAR(50),
    l1_name VARCHAR(200)
);

-- Hierarchy table (for hierarchy maintenance)
CREATE TABLE Cost_Center_Hierarchy (
    l4_key INT REFERENCES Dim_Cost_Center(cost_center_key),
    l3_key INT,
    l2_key INT,
    l1_key INT,
    hierarchy_level INT
);
```

**Benefits:**
- Fast queries (denormalized)
- Easy hierarchy updates (normalized bridge)

---

## 8. Late Arriving Facts and Dimensions {#late-arriving}

### Problem: Late Arriving Dimension

**Scenario:**
```
Day 1: Fact arrives for new Fund "XYZ"
       But Dim_Fund record doesn't exist yet!
       
Day 3: Dim_Fund record for "XYZ" arrives
```

### Solution 1: Default "Unknown" Dimension Member

```sql
-- Always maintain an "Unknown" record
INSERT INTO Dim_Fund (fund_key, fund_code, fund_name, is_current)
VALUES (-1, 'UNKNOWN', 'Unknown Fund - Pending', TRUE);

-- When fact arrives early, use -1
INSERT INTO Fact_AUM_Revenue (fund_key, advisor_key, date_key, aum, revenue)
VALUES (
    -1,  -- Use Unknown until real dimension arrives
    123,
    20250115,
    1000000,
    5000
);

-- When dimension arrives (Day 3), update fact
UPDATE Fact_AUM_Revenue
SET fund_key = 456  -- Real fund_key
WHERE fund_key = -1 
  AND date_key = 20250115
  AND aum = 1000000;  -- Additional filters to target specific row
```

**Pros:**
- ✅ Preserves referential integrity
- ✅ Facts never rejected
- ✅ Can report on "Unknown" items

**Cons:**
- ❌ Requires update after dimension arrives
- ❌ Can't query by fund attributes until update

---

### Solution 2: Inferred Dimension Member

**Create partial dimension record from fact:**

```sql
-- When fact arrives with fund_code "XYZ" but no dimension
-- Create "inferred" dimension record
INSERT INTO Dim_Fund (
    fund_key, 
    fund_code, 
    fund_name, 
    is_current,
    is_inferred  -- Flag as inferred
)
VALUES (
    456,
    'XYZ',  -- From fact
    'XYZ - Inferred',
    TRUE,
    TRUE  -- Mark as inferred
);

-- Insert fact with inferred dimension
INSERT INTO Fact_AUM_Revenue (fund_key, ...)
VALUES (456, ...);

-- When real dimension arrives, UPDATE inferred record
UPDATE Dim_Fund
SET 
    fund_name = 'XYZ Growth Fund',
    fund_family = 'Equity Funds',
    is_inferred = FALSE
WHERE fund_key = 456 AND is_inferred = TRUE;
```

**Pros:**
- ✅ No fact updates needed
- ✅ Can query by natural key (fund_code)

**Cons:**
- ❌ Dimension attributes incomplete until update
- ❌ Reports may show "Inferred" items

---

### Problem: Late Arriving Fact

**Scenario:**
```
Today: Jan 15, 2025 - Load facts for Jan 14
Tomorrow: Jan 16 - Late fact arrives for Jan 10 (5 days late!)

Problem: Dimension might have changed between Jan 10 and Jan 14
```

### Solution: SCD Type 2 with Effective Dating

```sql
-- Fact arrives late for Jan 10, 2025
-- Need dimension AS OF Jan 10

INSERT INTO Fact_AUM_Revenue (
    fund_key,
    date_key,
    aum,
    revenue
)
SELECT 
    f.fund_key,  -- Get fund_key that was valid on Jan 10
    20250110,
    1000000,
    5000
FROM Dim_Fund f
WHERE f.fund_code = 'ABC'
  AND f.effective_date <= '2025-01-10'  -- Valid on Jan 10
  AND (f.end_date > '2025-01-10' OR f.end_date IS NULL);
```

**Key Points:**
1. Always use **date of transaction** to find dimension
2. Don't use "current" dimension for historical facts
3. SCD Type 2 handles this automatically

---

### Late Arrival Workflow

```sql
-- Step 1: Stage incoming facts
CREATE TABLE Staging_AUM_Revenue (
    fund_code VARCHAR(50),
    transaction_date DATE,
    aum NUMERIC(18,2),
    revenue NUMERIC(18,2),
    load_timestamp TIMESTAMP DEFAULT GETDATE()
);

-- Step 2: Check for missing dimensions
SELECT DISTINCT s.fund_code
FROM Staging_AUM_Revenue s
LEFT JOIN Dim_Fund f ON s.fund_code = f.fund_code AND f.is_current = TRUE
WHERE f.fund_key IS NULL;

-- Step 3a: Create inferred dimensions (or use Unknown -1)
INSERT INTO Dim_Fund (fund_key, fund_code, fund_name, is_inferred, is_current)
SELECT 
    NEXTVAL('dim_fund_seq'),
    fund_code,
    fund_code || ' - Inferred',
    TRUE,
    TRUE
FROM (
    SELECT DISTINCT s.fund_code
    FROM Staging_AUM_Revenue s
    LEFT JOIN Dim_Fund f ON s.fund_code = f.fund_code AND f.is_current = TRUE
    WHERE f.fund_key IS NULL
) missing;

-- Step 4: Load facts with proper dimension keys
INSERT INTO Fact_AUM_Revenue (fund_key, date_key, aum, revenue)
SELECT 
    f.fund_key,
    TO_CHAR(s.transaction_date, 'YYYYMMDD')::INT,
    s.aum,
    s.revenue
FROM Staging_AUM_Revenue s
JOIN Dim_Fund f 
    ON s.fund_code = f.fund_code
    AND f.effective_date <= s.transaction_date
    AND (f.end_date > s.transaction_date OR f.end_date IS NULL);
```

---

## 9. Incremental Loading Strategy {#incremental-loading}

### Full Load vs Incremental Load

#### Full Load (Simple but slow)
```sql
-- Truncate and reload entire table
TRUNCATE TABLE Fact_AUM_Revenue;

INSERT INTO Fact_AUM_Revenue
SELECT * FROM Source_AUM_Revenue;
```

**Pros:** Simple, guarantees consistency
**Cons:** Slow for large tables, downtime required

---

#### Incremental Load (Recommended)

**Only load new/changed records**

### Strategy 1: Timestamp-Based (Most Common)

```sql
-- Track last load time
CREATE TABLE ETL_Control (
    table_name VARCHAR(100) PRIMARY KEY,
    last_load_timestamp TIMESTAMP,
    last_load_date DATE,
    rows_loaded INT
);

-- Incremental load query
INSERT INTO Fact_AUM_Revenue (fund_key, date_key, aum, revenue, load_timestamp)
SELECT 
    f.fund_key,
    TO_CHAR(s.transaction_date, 'YYYYMMDD')::INT,
    s.aum,
    s.revenue,
    GETDATE()
FROM Source_AUM_Revenue s
JOIN Dim_Fund f ON s.fund_code = f.fund_code AND f.is_current = TRUE
WHERE s.last_modified > (
    SELECT last_load_timestamp 
    FROM ETL_Control 
    WHERE table_name = 'Fact_AUM_Revenue'
);

-- Update control table
UPDATE ETL_Control
SET 
    last_load_timestamp = GETDATE(),
    last_load_date = CURRENT_DATE,
    rows_loaded = @@ROWCOUNT
WHERE table_name = 'Fact_AUM_Revenue';
```

---

### Strategy 2: Date-Based (For Daily Loads)

```sql
-- Load yesterday's data
INSERT INTO Fact_AUM_Revenue (fund_key, date_key, aum, revenue)
SELECT 
    f.fund_key,
    TO_CHAR(s.transaction_date, 'YYYYMMDD')::INT,
    s.aum,
    s.revenue
FROM Source_AUM_Revenue s
JOIN Dim_Fund f ON s.fund_code = f.fund_code AND f.is_current = TRUE
WHERE s.transaction_date = CURRENT_DATE - 1  -- Yesterday only
  AND NOT EXISTS (
      -- Prevent duplicates
      SELECT 1 FROM Fact_AUM_Revenue fct
      WHERE fct.fund_key = f.fund_key
        AND fct.date_key = TO_CHAR(s.transaction_date, 'YYYYMMDD')::INT
  );
```

---

### Strategy 3: CDC (Change Data Capture) - Advanced

**Capture only INSERT/UPDATE/DELETE from source**

```sql
-- Source system tracks changes
CREATE TABLE Source_CDC_Log (
    operation VARCHAR(10),  -- INSERT, UPDATE, DELETE
    table_name VARCHAR(100),
    primary_key VARCHAR(100),
    change_timestamp TIMESTAMP,
    change_data JSONB
);

-- Process CDC log
-- INSERT
INSERT INTO Fact_AUM_Revenue (...)
SELECT ... FROM Source_CDC_Log
WHERE operation = 'INSERT' AND table_name = 'AUM_Revenue'
  AND change_timestamp > last_load;

-- UPDATE
UPDATE Fact_AUM_Revenue
SET aum = change_data->>'aum', revenue = change_data->>'revenue'
FROM Source_CDC_Log
WHERE operation = 'UPDATE' 
  AND fact_key = change_data->>'fact_key';

-- DELETE
DELETE FROM Fact_AUM_Revenue
WHERE fact_key IN (
    SELECT change_data->>'fact_key'
    FROM Source_CDC_Log
    WHERE operation = 'DELETE'
);
```

---

### Delta Load Pattern for Your Tables

#### Fact_AUM_Revenue (Daily Incremental)
```sql
-- Delete yesterday's data (if reprocessing)
DELETE FROM Fact_AUM_Revenue
WHERE date_key = TO_CHAR(CURRENT_DATE - 1, 'YYYYMMDD')::INT;

-- Insert yesterday's data
INSERT INTO Fact_AUM_Revenue
SELECT ... FROM Staging_AUM_Revenue
WHERE transaction_date = CURRENT_DATE - 1;
```

#### Dim_Fund (SCD Type 2 Incremental)
```sql
-- Detect changes
WITH Changes AS (
    SELECT 
        s.fund_code,
        s.management_fee,
        d.fund_key,
        d.management_fee as old_fee
    FROM Staging_Fund s
    LEFT JOIN Dim_Fund d 
        ON s.fund_code = d.fund_code 
        AND d.is_current = TRUE
    WHERE s.management_fee != d.management_fee  -- Change detected
       OR d.fund_key IS NULL  -- New fund
)
-- Close old record
UPDATE Dim_Fund
SET 
    end_date = CURRENT_DATE - 1,
    is_current = FALSE
FROM Changes c
WHERE Dim_Fund.fund_key = c.fund_key;

-- Insert new record
INSERT INTO Dim_Fund (fund_code, management_fee, effective_date, is_current)
SELECT fund_code, management_fee, CURRENT_DATE, TRUE
FROM Changes;
```

---

### Handling Late Data with Incremental Loads

**Problem:** What if data arrives for 3 days ago?

**Solution: Sliding Window Approach**

```sql
-- Load last 7 days (to catch late arrivals)
DELETE FROM Fact_AUM_Revenue
WHERE date_key >= TO_CHAR(CURRENT_DATE - 7, 'YYYYMMDD')::INT;

INSERT INTO Fact_AUM_Revenue
SELECT ...
FROM Source_AUM_Revenue
WHERE transaction_date >= CURRENT_DATE - 7;
```

**Trade-off:**
- Wider window = catches more late data
- Wider window = more deletes/reinserts

---

### Incremental Load Best Practices

1. **Idempotency:** Running twice produces same result
   ```sql
   -- Use MERGE or DELETE + INSERT
   -- NOT just INSERT (causes duplicates)
   ```

2. **Transactional:** All or nothing
   ```sql
   BEGIN TRANSACTION;
       DELETE FROM Fact_AUM_Revenue WHERE ...;
       INSERT INTO Fact_AUM_Revenue ...;
   COMMIT;
   ```

3. **Logging:** Track every load
   ```sql
   INSERT INTO ETL_Log (job_name, start_time, status, rows_processed)
   VALUES ('AUM_Revenue_Daily', GETDATE(), 'SUCCESS', 10000);
   ```

4. **Validation:** Check counts
   ```sql
   -- Source count should match target count
   SELECT COUNT(*) FROM Source WHERE date = yesterday;
   SELECT COUNT(*) FROM Fact WHERE date_key = yesterday;
   ```

---

## 10. Complete DDL Scripts {#ddl-scripts}

### Dimension Tables

#### Dim_Date (Type 0)
```sql
CREATE TABLE Dim_Date (
    date_key            INT PRIMARY KEY,        -- 20250115
    full_date           DATE NOT NULL UNIQUE,
    year                INT NOT NULL,
    quarter             INT NOT NULL,
    quarter_name        VARCHAR(10),            -- Q1, Q2, Q3, Q4
    month               INT NOT NULL,
    month_name          VARCHAR(20),
    day                 INT NOT NULL,
    day_of_week         INT NOT NULL,           -- 1=Monday, 7=Sunday
    day_of_week_name    VARCHAR(20),
    week_of_year        INT,
    is_weekend          BOOLEAN NOT NULL,
    is_holiday          BOOLEAN DEFAULT FALSE,
    holiday_name        VARCHAR(100),
    
    -- Fiscal calendar
    fiscal_year         INT,
    fiscal_quarter      INT,
    fiscal_period       INT                     -- Month within fiscal year
);

-- Index for queries
CREATE INDEX idx_date_full_date ON Dim_Date(full_date);
CREATE INDEX idx_date_year_month ON Dim_Date(year, month);
```

#### Dim_Fund (Type 2 SCD)
```sql
CREATE TABLE Dim_Fund (
    fund_key            INT PRIMARY KEY,        -- Surrogate key
    fund_code           VARCHAR(50) NOT NULL,   -- Natural key (business key)
    fund_name           VARCHAR(200) NOT NULL,
    fund_family_code    VARCHAR(50),
    fund_family_name    VARCHAR(200),
    share_class         VARCHAR(50),
    share_class_code    VARCHAR(20),
    
    -- Financial attributes
    management_fee      NUMERIC(5,4),           -- 0.0075 = 0.75%
    performance_fee     NUMERIC(5,4),
    expense_ratio       NUMERIC(5,4),
    
    -- Categorization
    asset_class         VARCHAR(100),           -- Equity, Fixed Income, etc.
    strategy            VARCHAR(100),           -- Growth, Value, etc.
    is_active           BOOLEAN DEFAULT TRUE,
    
    -- SCD Type 2 tracking
    effective_date      DATE NOT NULL,
    end_date            DATE,                   -- NULL = current
    is_current          BOOLEAN DEFAULT TRUE,
    version             INT DEFAULT 1,
    
    -- Inferred dimension support
    is_inferred         BOOLEAN DEFAULT FALSE,
    
    -- Audit columns
    created_date        TIMESTAMP DEFAULT GETDATE(),
    created_by          VARCHAR(100),
    updated_date        TIMESTAMP DEFAULT GETDATE(),
    updated_by          VARCHAR(100),
    
    -- Business rule constraints
    CHECK (end_date IS NULL OR end_date > effective_date),
    CHECK (management_fee >= 0 AND management_fee <= 0.05),  -- Max 5%
    
    -- Only one current record per natural key
    UNIQUE (fund_code, share_class, is_current) WHERE is_current = TRUE
);

-- Indexes
CREATE INDEX idx_fund_code ON Dim_Fund(fund_code);
CREATE INDEX idx_fund_current ON Dim_Fund(is_current) WHERE is_current = TRUE;
CREATE INDEX idx_fund_effective_date ON Dim_Fund(effective_date, end_date);
```

#### Dim_Cost_Center (Type 2 SCD)
```sql
CREATE TABLE Dim_Cost_Center (
    cost_center_key     INT PRIMARY KEY,
    cost_center_code    VARCHAR(50) NOT NULL,   -- Natural key
    
    -- L4 - Most granular level
    l4_code             VARCHAR(50),
    l4_name             VARCHAR(200),
    l4_manager          VARCHAR(200),
    
    -- L3 - Department level (denormalized)
    l3_code             VARCHAR(50),
    l3_name             VARCHAR(200),
    
    -- L2 - Division level (denormalized)
    l2_code             VARCHAR(50),
    l2_name             VARCHAR(200),
    
    -- L1 - Company level (denormalized)
    l1_code             VARCHAR(50),
    l1_name             VARCHAR(200),
    
    -- Attributes
    cost_center_type    VARCHAR(50),            -- Direct, Indirect, Overhead
    is_active           BOOLEAN DEFAULT TRUE,
    
    -- SCD Type 2
    effective_date      DATE NOT NULL,
    end_date            DATE,
    is_current          BOOLEAN DEFAULT TRUE,
    version             INT DEFAULT 1,
    
    -- Audit
    created_date        TIMESTAMP DEFAULT GETDATE(),
    updated_date        TIMESTAMP DEFAULT GETDATE(),
    
    CHECK (end_date IS NULL OR end_date > effective_date),
    UNIQUE (cost_center_code, is_current) WHERE is_current = TRUE
);

CREATE INDEX idx_cc_code ON Dim_Cost_Center(cost_center_code);
CREATE INDEX idx_cc_current ON Dim_Cost_Center(is_current) WHERE is_current = TRUE;
CREATE INDEX idx_cc_l1 ON Dim_Cost_Center(l1_code);
CREATE INDEX idx_cc_l2 ON Dim_Cost_Center(l2_code);
```

#### Dim_Advisor (Type 2 SCD)
```sql
CREATE TABLE Dim_Advisor (
    advisor_key         INT PRIMARY KEY,
    advisor_code        VARCHAR(50) NOT NULL,
    advisor_name        VARCHAR(200) NOT NULL,
    
    -- Team/Hierarchy
    team_code           VARCHAR(50),
    team_name           VARCHAR(200),
    region_code         VARCHAR(50),
    region_name         VARCHAR(200),
    manager_code        VARCHAR(50),
    manager_name        VARCHAR(200),
    
    -- Contact (Type 1 - always current)
    email               VARCHAR(200),
    phone               VARCHAR(20),
    
    -- Attributes
    license_number      VARCHAR(50),
    hire_date           DATE,
    termination_date    DATE,
    is_active           BOOLEAN DEFAULT TRUE,
    
    -- SCD Type 2
    effective_date      DATE NOT NULL,
    end_date            DATE,
    is_current          BOOLEAN DEFAULT TRUE,
    version             INT DEFAULT 1,
    
    created_date        TIMESTAMP DEFAULT GETDATE(),
    updated_date        TIMESTAMP DEFAULT GETDATE(),
    
    CHECK (end_date IS NULL OR end_date > effective_date),
    UNIQUE (advisor_code, is_current) WHERE is_current = TRUE
);

CREATE INDEX idx_advisor_code ON Dim_Advisor(advisor_code);
CREATE INDEX idx_advisor_current ON Dim_Advisor(is_current) WHERE is_current = TRUE;
```

#### Dim_GL_Account (Type 1 - Overwrite)
```sql
CREATE TABLE Dim_GL_Account (
    gl_account_key      INT PRIMARY KEY,
    gl_account_code     VARCHAR(20) NOT NULL UNIQUE,
    gl_account_name     VARCHAR(200) NOT NULL,
    
    -- Hierarchy
    gl_category         VARCHAR(100),           -- Revenue, Expense, Asset, etc.
    gl_subcategory      VARCHAR(100),
    gl_type             VARCHAR(50),            -- Operating, Capital, etc.
    
    -- Attributes
    is_active           BOOLEAN DEFAULT TRUE,
    allows_posting      BOOLEAN DEFAULT TRUE,
    
    -- Type 1 - just track last update
    last_updated        TIMESTAMP DEFAULT GETDATE(),
    updated_by          VARCHAR(100)
);

CREATE INDEX idx_gl_code ON Dim_GL_Account(gl_account_code);
CREATE INDEX idx_gl_category ON Dim_GL_Account(gl_category);
```

#### Dim_Version (Type 0 - Lookup table)
```sql
CREATE TABLE Dim_Version (
    version_key         INT PRIMARY KEY,
    version_code        VARCHAR(20) NOT NULL UNIQUE,
    version_name        VARCHAR(100),
    version_description TEXT,
    display_order       INT
);

-- Preload standard versions
INSERT INTO Dim_Version VALUES 
(1, 'PLAN', 'Budget Plan', 'Annual budget approved by management', 1),
(2, 'FORECAST', 'Latest Forecast', 'Updated forecast based on current trends', 2),
(3, 'ACTUAL', 'Actual Results', 'Actual financial results', 3);
```

---

### Fact Tables

#### Fact_AUM_Revenue (Daily Grain)
```sql
CREATE TABLE Fact_AUM_Revenue (
    -- Dimension Keys
    fund_key            INT NOT NULL,
    advisor_key         INT NOT NULL,
    date_key            INT NOT NULL,
    version_key         INT NOT NULL,
    
    -- Measures (Additive)
    fee_paying_aum      NUMERIC(18,2),          -- Can be summed
    reportable_aum      NUMERIC(18,2),          -- Can be summed
    revenue             NUMERIC(18,2),          -- Can be summed
    
    -- Measures (Semi-Additive - average across time)
    management_fee_rate NUMERIC(5,4),           -- Cannot sum across time
    
    -- Measures (Non-Additive - ratios)
    aum_growth_rate     NUMERIC(10,4),          -- Cannot sum
    
    -- Degenerate Dimensions (no separate dimension table)
    transaction_id      VARCHAR(100),
    
    -- Audit
    load_timestamp      TIMESTAMP DEFAULT GETDATE(),
    source_system       VARCHAR(50),
    
    -- Composite Primary Key (Grain)
    PRIMARY KEY (fund_key, advisor_key, date_key, version_key),
    
    -- Foreign Keys
    FOREIGN KEY (fund_key) REFERENCES Dim_Fund(fund_key),
    FOREIGN KEY (advisor_key) REFERENCES Dim_Advisor(advisor_key),
    FOREIGN KEY (date_key) REFERENCES Dim_Date(date_key),
    FOREIGN KEY (version_key) REFERENCES Dim_Version(version_key),
    
    -- Business Rules
    CHECK (fee_paying_aum >= 0),
    CHECK (reportable_aum >= 0),
    CHECK (revenue >= 0),
    CHECK (management_fee_rate >= 0 AND management_fee_rate <= 0.05)
);

-- Indexes for query performance
CREATE INDEX idx_fact_aum_date ON Fact_AUM_Revenue(date_key);
CREATE INDEX idx_fact_aum_fund ON Fact_AUM_Revenue(fund_key);
CREATE INDEX idx_fact_aum_advisor ON Fact_AUM_Revenue(advisor_key);
CREATE INDEX idx_fact_aum_version ON Fact_AUM_Revenue(version_key);

-- Columnstore index for analytics (Redshift specific)
-- CREATE INDEX idx_fact_aum_columnstore ON Fact_AUM_Revenue
-- USING COLUMNSTORE (date_key, fund_key, version_key);
```

#### Fact_Expense (Daily Grain)
```sql
CREATE TABLE Fact_Expense (
    -- Dimension Keys
    cost_center_key     INT NOT NULL,
    gl_account_key      INT NOT NULL,
    date_key            INT NOT NULL,
    version_key         INT NOT NULL,
    
    -- Measures
    expense_amount      NUMERIC(18,2),
    quantity            NUMERIC(18,2),          -- Optional: units, hours, etc.
    unit_price          NUMERIC(18,2),          -- Optional
    
    -- Degenerate Dimensions
    transaction_id      VARCHAR(100),
    vendor_code         VARCHAR(50),
    vendor_name         VARCHAR(200),
    description         VARCHAR(500),
    
    -- Audit
    load_timestamp      TIMESTAMP DEFAULT GETDATE(),
    
    PRIMARY KEY (cost_center_key, gl_account_key, date_key, version_key),
    
    FOREIGN KEY (cost_center_key) REFERENCES Dim_Cost_Center(cost_center_key),
    FOREIGN KEY (gl_account_key) REFERENCES Dim_GL_Account(gl_account_key),
    FOREIGN KEY (date_key) REFERENCES Dim_Date(date_key),
    FOREIGN KEY (version_key) REFERENCES Dim_Version(version_key),
    
    CHECK (expense_amount >= 0),
    CHECK (quantity IS NULL OR quantity >= 0)
);

CREATE INDEX idx_fact_exp_date ON Fact_Expense(date_key);
CREATE INDEX idx_fact_exp_cc ON Fact_Expense(cost_center_key);
CREATE INDEX idx_fact_exp_gl ON Fact_Expense(gl_account_key);
```

#### Fact_Headcount (Monthly Grain)
```sql
CREATE TABLE Fact_Headcount (
    -- Dimension Keys
    cost_center_key     INT NOT NULL,
    period_key          INT NOT NULL,           -- YYYYMM (202501)
    version_key         INT NOT NULL,
    
    -- Measures
    headcount           INT,                    -- Count of employees
    fte_count           NUMERIC(10,2),          -- Full-time equivalent
    
    -- Demographic breakdowns (optional)
    permanent_count     INT,
    contract_count      INT,
    intern_count        INT,
    
    -- Audit
    load_timestamp      TIMESTAMP DEFAULT GETDATE(),
    
    PRIMARY KEY (cost_center_key, period_key, version_key),
    
    FOREIGN KEY (cost_center_key) REFERENCES Dim_Cost_Center(cost_center_key),
    FOREIGN KEY (version_key) REFERENCES Dim_Version(version_key),
    
    CHECK (headcount >= 0),
    CHECK (fte_count >= 0)
);

CREATE INDEX idx_fact_hc_period ON Fact_Headcount(period_key);
CREATE INDEX idx_fact_hc_cc ON Fact_Headcount(cost_center_key);
```

#### Fact_Invoice (Transaction Grain)
```sql
CREATE TABLE Fact_Invoice (
    -- Dimension Keys
    invoice_key         INT NOT NULL,           -- Link to invoice dimension
    invoice_line_number INT NOT NULL,
    customer_key        INT,
    date_key            INT NOT NULL,
    gl_account_key      INT NOT NULL,
    
    -- Measures
    invoice_amount      NUMERIC(18,2),
    paid_amount         NUMERIC(18,2),
    outstanding_amount  NUMERIC(18,2),
    tax_amount          NUMERIC(18,2),
    discount_amount     NUMERIC(18,2),
    
    -- Degenerate Dimensions
    invoice_number      VARCHAR(50),
    payment_status      VARCHAR(20),            -- Paid, Pending, Overdue
    payment_terms       VARCHAR(50),
    
    -- Audit
    load_timestamp      TIMESTAMP DEFAULT GETDATE(),
    
    PRIMARY KEY (invoice_key, invoice_line_number),
    
    FOREIGN KEY (date_key) REFERENCES Dim_Date(date_key),
    FOREIGN KEY (gl_account_key) REFERENCES Dim_GL_Account(gl_account_key),
    
    CHECK (invoice_amount >= 0),
    CHECK (paid_amount >= 0),
    CHECK (outstanding_amount >= 0)
);

CREATE INDEX idx_fact_inv_date ON Fact_Invoice(date_key);
CREATE INDEX idx_fact_inv_status ON Fact_Invoice(payment_status);
```

---

### ETL Control and Logging Tables

```sql
-- ETL Control for incremental loads
CREATE TABLE ETL_Control (
    table_name              VARCHAR(100) PRIMARY KEY,
    last_load_timestamp     TIMESTAMP,
    last_load_date          DATE,
    last_successful_load    TIMESTAMP,
    rows_loaded             INT,
    status                  VARCHAR(20)         -- SUCCESS, FAILED, RUNNING
);

-- ETL Job Log
CREATE TABLE ETL_Log (
    log_id                  INT PRIMARY KEY,
    job_name                VARCHAR(100),
    start_time              TIMESTAMP,
    end_time                TIMESTAMP,
    status                  VARCHAR(20),
    rows_processed          INT,
    rows_inserted           INT,
    rows_updated            INT,
    rows_deleted            INT,
    error_message           TEXT
);

-- Data Quality Results
CREATE TABLE Data_Quality_Log (
    check_id                INT PRIMARY KEY,
    table_name              VARCHAR(100),
    rule_name               VARCHAR(200),
    check_timestamp         TIMESTAMP,
    records_checked         INT,
    records_failed          INT,
    severity                VARCHAR(20),
    status                  VARCHAR(20)
);
```

---

### Sample Stored Procedure: SCD Type 2 Update

```sql
CREATE OR REPLACE PROCEDURE sp_update_dim_fund_scd2()
AS $$
DECLARE
    v_rows_updated INT;
    v_rows_inserted INT;
BEGIN
    -- Step 1: Close expired records
    UPDATE Dim_Fund d
    SET 
        end_date = CURRENT_DATE - 1,
        is_current = FALSE,
        updated_date = GETDATE()
    FROM Staging_Fund s
    WHERE d.fund_code = s.fund_code
      AND d.is_current = TRUE
      AND (
          d.management_fee != s.management_fee OR
          d.fund_name != s.fund_name OR
          d.fund_family_name != s.fund_family_name
      );
    
    GET DIAGNOSTICS v_rows_updated = ROW_COUNT;
    
    -- Step 2: Insert new records for changes
    INSERT INTO Dim_Fund (
        fund_key,
        fund_code,
        fund_name,
        fund_family_code,
        fund_family_name,
        share_class,
        management_fee,
        effective_date,
        end_date,
        is_current,
        version,
        created_date
    )
    SELECT 
        NEXTVAL('dim_fund_seq'),
        s.fund_code,
        s.fund_name,
        s.fund_family_code,
        s.fund_family_name,
        s.share_class,
        s.management_fee,
        CURRENT_DATE,
        NULL,
        TRUE,
        COALESCE(d.version, 0) + 1,
        GETDATE()
    FROM Staging_Fund s
    LEFT JOIN Dim_Fund d 
        ON s.fund_code = d.fund_code 
        AND d.end_date = CURRENT_DATE - 1  -- Just closed
    WHERE d.fund_key IS NOT NULL  -- Changed records
       OR NOT EXISTS (  -- New records
           SELECT 1 FROM Dim_Fund d2
           WHERE d2.fund_code = s.fund_code
       );
    
    GET DIAGNOSTICS v_rows_inserted = ROW_COUNT;
    
    -- Step 3: Log results
    INSERT INTO ETL_Log (job_name, start_time, end_time, status, rows_updated, rows_inserted)
    VALUES ('sp_update_dim_fund_scd2', GETDATE(), GETDATE(), 'SUCCESS', 
            v_rows_updated, v_rows_inserted);
    
    COMMIT;
    
EXCEPTION WHEN OTHERS THEN
    ROLLBACK;
    INSERT INTO ETL_Log (job_name, start_time, end_time, status, error_message)
    VALUES ('sp_update_dim_fund_scd2', GETDATE(), GETDATE(), 'FAILED', SQLERRM);
    RAISE;
END;
$$ LANGUAGE plpgsql;
```

---

## Summary: Recommended Approach

### For Your Investment Management Data Warehouse:

1. **Schema Type:** **Hybrid Star Schema**
   - Primary approach: Star (denormalized dimensions)
   - Selective normalization for very large or frequently changing dimensions

2. **Grain:**
   - Fact_AUM_Revenue: Daily by Fund/Advisor
   - Fact_Expense: Daily by Cost Center/GL Account
   - Fact_Headcount: Monthly by Cost Center

3. **SCD Patterns:**
   - Dim_Fund: Type 2 (track fee changes)
   - Dim_Cost_Center: Type 2 (track org changes)
   - Dim_Advisor: Type 2 (track team changes)
   - Dim_GL_Account: Type 1 (corrections only)
   - Dim_Date: Type 0 (never changes)

4. **Loading Strategy:**
   - Incremental loads with timestamp/date tracking
   - 7-day sliding window for late arrivals
   - CDC for high-volume tables

5. **Data Quality:**
   - Primary keys on all tables
   - Foreign keys with "Unknown" defaults
   - CHECK constraints for business rules
   - Data quality logging

---

**Next Steps:**
1. Review and approve dimensional model
2. Create development environment DDL
3. Build ETL framework
4. Implement SCD Type 2 logic
5. Set up incremental loading
6. Configure data quality checks

---

**Document Version:** 1.0  
**Last Updated:** January 2026  
**Project:** Investment Management Data Warehouse