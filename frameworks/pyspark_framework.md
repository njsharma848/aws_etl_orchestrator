# PySpark Problem-Solving Framework

> A systematic approach to solving PySpark interview questions.
> Adapted from the UDACT framework for distributed data processing scenarios.

---

## Why a Framework?

PySpark interview questions test whether you can think in **distributed data transformations** — not just write syntactically correct code. Having a structured approach is MORE important than memorizing the API.

**Interview evaluation breakdown (approximate):**

| Aspect | Weight | What They're Looking For |
|--------|--------|-------------------------|
| Structured thinking | 40% | Systematic decomposition, identifying transformation steps |
| Communication | 30% | Explaining approach, trade-offs, distributed thinking |
| Correctness | 20% | Working solution, handles edge cases |
| Optimization | 10% | Performance awareness, avoiding common anti-patterns |

---

## The UDACT Framework (Adapted for PySpark)

```
U → Understand the Problem
D → Design the Transformation Pipeline
A → Approach Selection
C → Code Incrementally
T → Test, Optimize & Communicate
```

---

## Phase 1: UNDERSTAND the Problem

### Goal: Clarify inputs, outputs, and constraints before writing ANY code

### Questions to Ask

**Input Data:**
- "What does the input DataFrame look like?" (schema, sample rows)
- "How large is the dataset?" (thousands, millions, billions of rows)
- "Are there NULLs in the data? How should they be handled?"
- "Are there duplicate rows? How do we define a duplicate?"
- "Is the data already partitioned? On which column?"

**Expected Output:**
- "What should the output look like?" (columns, granularity)
- "Should the output be a DataFrame, a scalar value, or written to a table?"
- "What's the expected row count relative to input?" (same, fewer, more)
- "Does the output order matter?"

**Constraints:**
- "Are there performance requirements?" (time SLA, cluster size)
- "Can we use UDFs, or should we stick to built-in functions?"
- "Is this a one-time computation or part of a recurring pipeline?"

### Example Clarification

> **Interviewer:** "Find the top 3 products by revenue for each category."
>
> **You:** "Before I dive in, let me clarify:
> 1. By 'revenue' — is that a column in the data, or do I compute it as price * quantity?
> 2. By 'top 3' — if two products tie for 3rd place, should I include both (rank) or exactly 3 (row_number)?
> 3. What columns should the output have? Category, product, revenue, and the rank?
> 4. Are there categories with fewer than 3 products? Should I still include them?
> 5. How should I handle NULL revenue values — exclude or treat as 0?"

---

## Phase 2: DESIGN the Transformation Pipeline

### Goal: Break down the problem into a sequence of DataFrame operations

### Step 1: Identify the Logical Steps

Think of the problem as a series of transformations:

```
Input DataFrame
    ↓  (filter)       — Remove unwanted rows
    ↓  (transform)    — Add/modify columns
    ↓  (aggregate)    — Group and summarize
    ↓  (window)       — Rank, running totals, comparisons
    ↓  (join)         — Combine with other data
    ↓  (select)       — Choose output columns
Output DataFrame
```

### Step 2: Sketch the Plan

Before coding, describe your approach in plain English:

> "Here's my plan:
> 1. First, I'll compute revenue as `price * quantity`
> 2. Then, I'll create a window partitioned by category, ordered by revenue descending
> 3. Apply `dense_rank()` to handle ties
> 4. Filter to keep only rows where rank <= 3
> 5. Select the final output columns"

### Step 3: Identify the PySpark Building Blocks

| Problem Component | PySpark Tool |
|-------------------|-------------|
| Filter rows | `.filter()` / `.where()` |
| Add/modify columns | `.withColumn()` |
| Aggregate | `.groupBy().agg()` |
| Rank within groups | `Window.partitionBy().orderBy()` + `row_number()/rank()` |
| Running total | `Window` + `sum().over(rowsBetween(...))` |
| Compare to group avg | `avg().over(Window.partitionBy())` |
| Join tables | `.join(other_df, key, type)` |
| Reshape | `.groupBy().pivot()` / `selectExpr("stack(...)")` |
| Remove duplicates | `.dropDuplicates()` or window + `row_number() == 1` |
| Previous/next row | `lag()` / `lead()` over window |

---

## Phase 3: APPROACH Selection

### Goal: Choose the right operations and evaluate trade-offs

### Decision Matrix: Common Problem Types

```
What type of problem is this?
├── "Top N per group"
│   └── Window function: partitionBy + orderBy + row_number/dense_rank
├── "Running total / cumulative"
│   └── Window function: sum/avg over rowsBetween(unboundedPreceding, currentRow)
├── "Previous/next value comparison"
│   └── Window function: lag() / lead()
├── "Aggregate per group"
│   └── groupBy().agg()
├── "Find rows above/below group average"
│   └── Window avg() compared to row value
├── "Join two datasets"
│   └── .join() — choose inner/left/anti based on need
├── "Remove duplicates"
│   └── dropDuplicates() or window row_number() == 1
├── "Pivot / reshape"
│   └── groupBy().pivot().agg() or stack()
├── "Fill missing dates/values"
│   └── Generate date series + left join + fillna
└── "Complex conditional logic"
    └── when().when().otherwise()
```

### Window Function vs GroupBy Decision

```
Need to keep individual rows?
├── YES → Use Window function
│   Examples: rank per group, running total, compare to average
│   Pattern: df.withColumn("result", func().over(Window.partitionBy(...)))
│
└── NO → Use GroupBy + Agg
    Examples: total per group, count per category, average by region
    Pattern: df.groupBy("col").agg(sum("val").alias("total"))
```

### Join Type Decision

```
What rows do you need?
├── Only matching rows from both → "inner"
├── All from left + matches from right → "left"
├── All from both, even unmatched → "outer"
├── Left rows that HAVE a match → "left_semi" (like EXISTS)
├── Left rows that DON'T have a match → "left_anti" (like NOT EXISTS)
└── Every combination → "cross" (Cartesian — use carefully!)
```

### Communicate Your Choice

> "I'll use a window function here rather than groupBy because we need to keep individual rows — we want to see each product alongside its rank. I'll use `dense_rank()` instead of `row_number()` to handle ties fairly — if two products have the same revenue, they should share the same rank."

---

## Phase 4: CODE Incrementally

### Goal: Build step by step, verifying at each stage

### Coding Principles

1. **Import first** — show you know the modules
2. **Build one transformation at a time** — don't chain everything at once
3. **Name intermediate DataFrames** — makes debugging and explaining easier
4. **Use meaningful aliases** — `F.col("salary").alias("annual_salary")`
5. **Comment your reasoning** — especially for non-obvious choices
6. **Prefer built-in functions** — over UDFs (10-100x faster)

### Step-by-Step Example

> **Problem:** "For each department, find the employees whose salary is above the department average. Show their name, department, salary, department average, and how much above average they are."

```python
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# STEP 1: Understand the data
# df has columns: employee_id, name, department, salary
df.printSchema()
df.show(5)

# STEP 2: Compute department average using a window function
# (Window because we need to keep individual rows)
dept_window = Window.partitionBy("department")

df_with_avg = df.withColumn(
    "dept_avg_salary",
    F.round(F.avg("salary").over(dept_window), 2)
)

# Verify: "Let me check this intermediate result..."
df_with_avg.show(5)

# STEP 3: Compute the difference
df_with_diff = df_with_avg.withColumn(
    "above_avg_by",
    F.round(F.col("salary") - F.col("dept_avg_salary"), 2)
)

# STEP 4: Filter to above-average employees
above_avg = df_with_diff.filter(F.col("salary") > F.col("dept_avg_salary"))

# STEP 5: Select final output columns
result = above_avg.select(
    "name",
    "department",
    "salary",
    "dept_avg_salary",
    "above_avg_by"
).orderBy("department", F.col("above_avg_by").desc())

result.show()
```

### Communication During Coding

Narrate as you write:

- "I'll use a window function partitioned by department — this lets me compute the department average while keeping individual rows..."
- "I'm using `F.round()` to keep the output clean..."
- "Let me verify this intermediate result before adding the filter..."
- "I'll order by department first, then by how much above average descending, so we see the biggest outliers first..."

---

## Phase 5: TEST, Optimize & Communicate

### Goal: Verify correctness, discuss edge cases, suggest improvements

### Testing Checklist

| Check | How to Verify |
|-------|---------------|
| **Correct output** | Sample rows — do the numbers make sense? |
| **NULL handling** | `df.filter(F.col("key_col").isNull()).count()` |
| **Duplicate check** | `df.groupBy("key").count().filter("count > 1").count()` |
| **Row count** | Does the output have the expected number of rows? |
| **Data types** | `df.printSchema()` — are types correct? |
| **Ordering** | Is the output ordered as required? |

### Edge Cases to Mention

```python
# Edge case 1: NULLs in the group-by or order-by column
# "Window functions treat NULLs as a single group. I should filter them
#  or handle them explicitly."
df.filter(F.col("department").isNotNull())

# Edge case 2: Ties in ranking
# "With row_number(), ties get arbitrary ordering. I used dense_rank()
#  to ensure tied values get the same rank."

# Edge case 3: Empty groups
# "If a department has no employees above average, it won't appear in
#  the result — which is correct behavior for this problem."

# Edge case 4: Division by zero
# "When computing percentages, I'll use when() to guard against zero denominators."
df.withColumn(
    "pct",
    F.when(F.col("total") != 0, F.col("value") / F.col("total") * 100)
     .otherwise(0)
)

# Edge case 5: Data skew
# "If one department has 90% of the rows, the window function will be slow
#  for that partition. In production, I might repartition or salt the key."

# Edge case 6: Very large dataset
# "For billions of rows, I'd avoid collect(), prefer approximate functions
#  like approx_count_distinct(), and ensure we're filtering early to reduce
#  the amount of data being shuffled."
```

### Optimization Discussion

After your solution works, proactively discuss performance:

> "A few optimization considerations for production:
> 1. **Filter early** — if we only care about active employees, filter before the window function to reduce data
> 2. **Broadcast small tables** — if joining with a small lookup table, use `broadcast()` to avoid shuffle
> 3. **Avoid UDFs** — my solution uses only built-in functions, which run in the JVM; UDFs serialize to Python and are 10-100x slower
> 4. **Caching** — if we're going to reuse `df_with_avg` for multiple analyses, I'd `.cache()` it
> 5. **Partitioning** — if this data is stored on disk, partitioning by department would speed up per-department queries
> 6. **Column pruning** — I selected only needed columns early to reduce memory usage"

---

## Worked Examples by Problem Type

### Example 1: Top N Per Group

> **Problem:** "Find the 3 highest-paid employees in each department."

```python
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# U: Top 3 by salary per department. Use dense_rank for ties.
# D: Add rank column → filter → select
# A: Window function with dense_rank (handles ties)

window_spec = Window.partitionBy("department").orderBy(F.col("salary").desc())

result = (df
    .withColumn("rank", F.dense_rank().over(window_spec))
    .filter(F.col("rank") <= 3)
    .select("department", "employee_name", "salary", "rank")
    .orderBy("department", "rank")
)
```

### Example 2: Running Total

> **Problem:** "Calculate the running total of sales for each product, ordered by date."

```python
# U: Cumulative sum of sales per product, chronologically.
# D: Window with unboundedPreceding → running sum
# A: sum() over window with rowsBetween

window_spec = (Window
    .partitionBy("product_id")
    .orderBy("sale_date")
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)
)

result = df.withColumn(
    "running_total",
    F.sum("sale_amount").over(window_spec)
)
```

### Example 3: Compare to Group Average

> **Problem:** "Find all orders that are more than 2x the average order value for their category."

```python
# U: Compare each order to its category average. Keep rows > 2x avg.
# D: Window avg → compute ratio → filter
# A: Window function (need individual rows with group stat)

category_window = Window.partitionBy("category")

result = (df
    .withColumn("category_avg", F.avg("order_value").over(category_window))
    .withColumn("ratio", F.col("order_value") / F.col("category_avg"))
    .filter(F.col("ratio") > 2)
    .select("order_id", "category", "order_value", "category_avg", "ratio")
)
```

### Example 4: Gap Detection (Consecutive Days)

> **Problem:** "Find the longest streak of consecutive login days for each user."

```python
# U: Consecutive login dates per user. Longest streak.
# D: Deduplicate dates → detect breaks → group islands → find max
# A: row_number trick (date - row_num = constant for consecutive days)

window_spec = Window.partitionBy("user_id").orderBy("login_date")

streaks = (df
    .select("user_id", F.to_date("login_timestamp").alias("login_date"))
    .distinct()  # One row per user per day
    .withColumn("rn", F.row_number().over(window_spec))
    .withColumn("grp", F.date_sub("login_date", F.col("rn")))
    # Consecutive dates produce the same grp value
    .groupBy("user_id", "grp")
    .agg(
        F.count("*").alias("streak_length"),
        F.min("login_date").alias("streak_start"),
        F.max("login_date").alias("streak_end")
    )
)

longest_streaks = (streaks
    .withColumn("rn", F.row_number().over(
        Window.partitionBy("user_id").orderBy(F.col("streak_length").desc())
    ))
    .filter(F.col("rn") == 1)
    .select("user_id", "streak_length", "streak_start", "streak_end")
)
```

### Example 5: Pivot and Reshape

> **Problem:** "Create a summary showing total sales per product per quarter."

```python
# U: Pivot quarter values into columns, sum sales per product.
# D: Extract quarter → pivot → aggregate
# A: groupBy().pivot().agg()

result = (df
    .withColumn("quarter", F.concat(F.lit("Q"), F.quarter("sale_date")))
    .groupBy("product_name")
    .pivot("quarter", ["Q1", "Q2", "Q3", "Q4"])  # Specify values for performance
    .agg(F.sum("sale_amount"))
    .fillna(0)
)
```

### Example 6: Deduplication (Keep Latest)

> **Problem:** "Remove duplicate customer records, keeping the most recently updated one."

```python
# U: Deduplicate by customer_id, keep latest updated_at.
# D: Window → row_number → filter → drop helper column
# A: row_number (deterministic, exactly 1 winner per group)

window_spec = Window.partitionBy("customer_id").orderBy(F.col("updated_at").desc())

result = (df
    .withColumn("rn", F.row_number().over(window_spec))
    .filter(F.col("rn") == 1)
    .drop("rn")
)
```

### Example 7: Self-Join (Employees and Managers)

> **Problem:** "Show each employee alongside their manager's name."

```python
# U: Self-join on manager_id = employee_id.
# D: Alias both sides → left join → select
# A: Left join (some employees may have no manager)

employees = df.alias("emp")
managers = df.alias("mgr")

result = (employees
    .join(
        managers,
        F.col("emp.manager_id") == F.col("mgr.employee_id"),
        "left"
    )
    .select(
        F.col("emp.employee_id"),
        F.col("emp.name").alias("employee_name"),
        F.col("emp.department"),
        F.col("mgr.name").alias("manager_name")
    )
)
```

### Example 8: Conditional Aggregation

> **Problem:** "For each department, count total employees, employees earning above 100K, and the average salary."

```python
# U: Multiple aggregations per department, some conditional.
# D: groupBy → agg with when() conditions
# A: Single groupBy with conditional sums (avoids multiple passes)

result = (df
    .groupBy("department")
    .agg(
        F.count("*").alias("total_employees"),
        F.sum(F.when(F.col("salary") > 100000, 1).otherwise(0)).alias("high_earners"),
        F.round(F.avg("salary"), 2).alias("avg_salary"),
        F.min("salary").alias("min_salary"),
        F.max("salary").alias("max_salary")
    )
    .orderBy(F.col("total_employees").desc())
)
```

---

## Framework Quick Reference Card

```
┌─────────────────────────────────────────────────────────────────┐
│                     UDACT FRAMEWORK                              │
├──────────┬──────────────────────────────────────────────────────┤
│ UNDERSTAND│ Ask about: input schema, output format, NULLs,       │
│           │ duplicates, ties, data volume, edge cases            │
├──────────┼──────────────────────────────────────────────────────┤
│ DESIGN    │ Map: input → filter → transform → aggregate/window   │
│           │      → join → select → output                        │
│           │ Identify the building blocks needed                  │
├──────────┼──────────────────────────────────────────────────────┤
│ APPROACH  │ Choose: Window vs GroupBy, join type, rank function   │
│           │ Consider: built-in vs UDF, broadcast vs shuffle      │
│           │ Communicate WHY you chose each                       │
├──────────┼──────────────────────────────────────────────────────┤
│ CODE      │ Build one step at a time                             │
│           │ Name intermediate DataFrames                         │
│           │ Verify at each step: .show(), .count()               │
│           │ Narrate your reasoning                               │
├──────────┼──────────────────────────────────────────────────────┤
│ TEST &    │ Check: NULLs, duplicates, row counts, data types     │
│ OPTIMIZE  │ Discuss: edge cases, skew, performance               │
│           │ Mention: filter early, broadcast, avoid UDFs          │
└──────────┴──────────────────────────────────────────────────────┘
```

---

## Performance Principles to Mention

### The Big Five

| Principle | Why | Example |
|-----------|-----|---------|
| **Filter early** | Reduce data before expensive ops | `.filter(...)` before `.join(...)` |
| **Select only needed columns** | Less memory, less shuffle | `.select("a", "b")` not `.select("*")` |
| **Broadcast small tables** | Avoid shuffle join | `join(broadcast(small_df), ...)` |
| **Use built-in functions** | JVM-native, vectorized | `F.when()` not `udf(...)` |
| **Cache strategically** | Avoid recomputation | `.cache()` after expensive transforms |

### Anti-Patterns to Avoid

| Anti-Pattern | Problem | Better Approach |
|-------------|---------|-----------------|
| `df.collect()` on large data | OOM on driver | Aggregate first, then collect |
| Python UDF for simple logic | 10-100x slower | Use `F.when()`, `F.regexp_extract()`, etc. |
| `df.count()` to check emptiness | Triggers full computation | Use `df.head(1)` or `df.isEmpty()` |
| Joining without filtering | Shuffles entire datasets | Filter both sides before join |
| Multiple separate aggregations | Multiple passes over data | Single `.agg()` with multiple expressions |
| `limit(N)` for "top N per group" | Only gives top N overall | Window function + filter |

---

## Key Phrases to Use in Interviews

| Situation | What to Say |
|-----------|-------------|
| Starting | "Let me make sure I understand the input schema and expected output..." |
| Choosing window vs groupBy | "I need a window function here because I want to keep individual rows while computing a group-level metric..." |
| Handling NULLs | "I'll explicitly handle NULLs with `isNotNull()` to avoid unexpected behavior in aggregations..." |
| Choosing rank function | "I'll use `dense_rank()` to handle ties fairly — `row_number()` would arbitrarily break ties..." |
| Deduplication | "I'll use `row_number()` partitioned by the key to deterministically pick one record per group..." |
| Join choice | "A `left_anti` join here efficiently finds records in A that don't exist in B — like a NOT EXISTS..." |
| Performance | "I'll filter before the join to reduce the shuffle size, and broadcast the smaller table..." |
| After coding | "Let me verify by checking a few edge cases: what if there are NULLs? What about ties? What about empty groups?" |

---

## Common Imports Cheat Sheet

```python
# Always import these
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# For specific needs
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, DateType, TimestampType, BooleanType,
    ArrayType, MapType, DecimalType, LongType
)

# For Delta Lake operations
from delta.tables import DeltaTable

# For broadcast joins
from pyspark.sql.functions import broadcast
```

---

## PySpark Interview Pattern Recognition

| Interview Phrase | Immediate Thought | Pattern |
|------------------|-------------------|---------|
| "for each group" | Window or GroupBy | `Window.partitionBy()` |
| "top N per" | Window | `row_number().over(Window...).filter(rn <= N)` |
| "running total" | Window with frame | `sum().over(rowsBetween(unboundedPreceding, currentRow))` |
| "previous value" | Window | `lag().over(Window...)` |
| "compared to average" | Window | `avg().over(Window.partitionBy())` |
| "consecutive" | Window + gap detect | `lag()` + diff, or row_number date trick |
| "remove duplicates" | Window or built-in | `dropDuplicates()` or `row_number() == 1` |
| "pivot table" | Pivot | `groupBy().pivot().agg()` |
| "not in" / "doesn't exist" | Anti-join | `.join(..., "left_anti")` |
| "exists in" | Semi-join | `.join(..., "left_semi")` |
| "fill missing" | Join + fillna | Generate series + left join + `.fillna()` |
| "combine tables" | Join | `.join(df2, key, join_type)` |

---

Remember: Interviewers care MORE about your thinking process than the final code. Show structured reasoning, explain your trade-offs, and demonstrate awareness of how PySpark operations work in a distributed context.
