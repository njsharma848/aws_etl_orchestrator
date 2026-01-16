You're right. Let me provide **only the verified columns** from our conversations, and add **only essential missing pieces**:

---

## **1. HEADCOUNT** ✓ (Complete)

```sql
CREATE TABLE headcount (
    cc_period_id      VARCHAR(200) NOT NULL,
    version_name      VARCHAR(100),
    l4_cost_center    VARCHAR(500),
    time1             DATE NOT NULL,
    data_source2      VARCHAR(100),
    time2             INT NOT NULL,
    headcount         INT,
    last_updated      TIMESTAMP,
    PRIMARY KEY (cc_period_id)
);
```

---

## **2. AUM REVENUE** ✓ (Complete)

```sql
CREATE TABLE aum_revenue (
    data_index                    VARCHAR(70) NOT NULL,
    version_name                  VARCHAR(256),
    advisor                       VARCHAR(256),
    fund_family                   VARCHAR(256),
    fund_name                     VARCHAR(256),
    share_class                   VARCHAR(256),
    time1                         DATE,
    fee_paying_aum                NUMERIC(18,2),
    reportable_aum                NUMERIC(18,2),
    revenue                       NUMERIC(18,2),
    expense_aum_revenue_key       VARCHAR(256),
    period_month                  INTEGER,
    period_year                   INTEGER,
    time2                         INTEGER,
    data_source2                  VARCHAR(256),
    last_updated                  TIMESTAMP,
    PRIMARY KEY (data_index, time2)
);
```

---

## **3. REVENUE PLAN/FORECAST** ✓ (Complete)

```sql
CREATE TABLE revenue_plan_forecast (
    data_index                    VARCHAR(70) NOT NULL,
    version_name                  VARCHAR(256),
    fund_name                     VARCHAR(256),
    share_class                   VARCHAR(256),
    dc_tier1                      VARCHAR(256),
    dc_tier2                      VARCHAR(256),
    dc_tier3                      VARCHAR(256),
    dc_tier4                      VARCHAR(256),
    data_source                   VARCHAR(256),
    reporting_currency            VARCHAR(256),
    assets_under_administration   VARCHAR(256),
    time1                         DATE,
    fee_paying_aum                NUMERIC(18,2),
    reportable_aum                NUMERIC(18,2),
    revenue                       NUMERIC(18,2),
    expense_aum_revenue_key       VARCHAR(256),
    period_month                  INTEGER,
    period_year                   INTEGER,
    time2                         INTEGER,
    data_source2                  VARCHAR(256),
    last_updated                  TIMESTAMP,
    PRIMARY KEY (data_index, time2)
);
```

---

## **4. REVENUE ACTUALS** (Adding minimal structure)

```sql
CREATE TABLE revenue_actuals (
    data_index           VARCHAR(70) NOT NULL,
    version_name         VARCHAR(256),
    -- [Similar dimensional columns as revenue_plan_forecast]
    time1                DATE,
    revenue              NUMERIC(18,2),
    period_month         INTEGER,
    period_year          INTEGER,
    time2                INTEGER,
    data_source2         VARCHAR(256),
    last_updated         TIMESTAMP,
    PRIMARY KEY (data_index, time2)
);
```

---

## **5. EXPENSE** (Adding minimal structure)

```sql
CREATE TABLE expense (
    data_index           VARCHAR(70) NOT NULL,
    version_name         VARCHAR(256),
    l1_cost_center       VARCHAR(500),
    l2_cost_center       VARCHAR(500),
    l3_cost_center       VARCHAR(500),
    l4_cost_center       VARCHAR(500),
    time1                DATE,
    expense_amount       NUMERIC(18,2),
    time2                INTEGER,
    period_month         INTEGER,
    period_year          INTEGER,
    data_source2         VARCHAR(256),
    last_updated         TIMESTAMP,
    PRIMARY KEY (data_index, time2)
);
```

---

## **6. ZAUM** (Adding minimal structure)

```sql
CREATE TABLE zaum (
    data_index           VARCHAR(70) NOT NULL,
    version_name         VARCHAR(256),  -- Plan/Forecast/Actual
    time1                DATE,
    zaum_value           NUMERIC(18,2),
    time2                INTEGER,
    period_month         INTEGER,
    period_year          INTEGER,
    data_source2         VARCHAR(256),
    last_updated         TIMESTAMP,
    PRIMARY KEY (data_index, time2)
);
```

---

## **7. GENERAL ACCOUNT INVOICE** (Adding minimal structure)

```sql
CREATE TABLE general_account_invoice (
    data_index           VARCHAR(70) NOT NULL,
    version_name         VARCHAR(256),
    time1                DATE,
    invoice_amount       NUMERIC(18,2),
    time2                INTEGER,
    period_month         INTEGER,
    period_year          INTEGER,
    data_source2         VARCHAR(256),
    last_updated         TIMESTAMP,
    PRIMARY KEY (data_index, time2)
);
```

---

**Note:** Tables 4-7 have minimal schemas because I don't have detailed column information from our conversations. To complete these properly, you'll need to:

1. Check the actual CSV files from S3
2. Review the Glue job schema inference
3. Or provide sample data

Would you like me to help create a script to extract actual column names from your S3 CSV files?