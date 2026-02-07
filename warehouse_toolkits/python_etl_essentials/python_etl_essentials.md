# **Python Proficiency for AWS Glue & Data Engineering**

Complete guide covering essential Python concepts for ETL development.

---

## **Table of Contents**

1. [Data Structures](#data-structures)
2. [File Handling](#file-handling)
3. [Exception Handling](#exception-handling)
4. [Lambda Functions](#lambda-functions)
5. [Decorators](#decorators)
6. [Generators](#generators)
7. [Iterators](#iterators)
8. [Memory Management](#memory-management)
9. [Multithreading vs Multiprocessing](#multithreading-vs-multiprocessing)

---

## **1. Data Structures** {#data-structures}

### **1.1 Lists**

**Characteristics:**
- Ordered, mutable, allows duplicates
- Dynamic sizing
- Can contain mixed data types

```python
# Creating lists
records = []
records = [1, 2, 3, 4, 5]
records = ["fund_001", "fund_002", "fund_003"]
mixed = [1, "text", 3.14, True, None]

# Common operations
records = ["fund_001", "fund_002", "fund_003"]

# Append (add to end)
records.append("fund_004")
print(records)  # ['fund_001', 'fund_002', 'fund_003', 'fund_004']

# Insert at specific position
records.insert(1, "fund_new")
print(records)  # ['fund_001', 'fund_new', 'fund_002', 'fund_003', 'fund_004']

# Remove by value
records.remove("fund_new")

# Remove by index
popped = records.pop(0)  # Returns and removes first element
print(popped)  # 'fund_001'

# Extend (add multiple items)
records.extend(["fund_005", "fund_006"])

# List comprehension
numbers = [1, 2, 3, 4, 5]
squared = [x**2 for x in numbers]
print(squared)  # [1, 4, 9, 16, 25]

# Filter with list comprehension
even_numbers = [x for x in numbers if x % 2 == 0]
print(even_numbers)  # [2, 4]

# Slicing
records = ["fund_001", "fund_002", "fund_003", "fund_004", "fund_005"]
first_three = records[:3]      # ['fund_001', 'fund_002', 'fund_003']
last_two = records[-2:]        # ['fund_004', 'fund_005']
every_other = records[::2]     # ['fund_001', 'fund_003', 'fund_005']
reversed_list = records[::-1]  # Reverse order

# Sorting
numbers = [5, 2, 8, 1, 9]
numbers.sort()              # In-place sort
print(numbers)              # [1, 2, 5, 8, 9]

sorted_numbers = sorted([5, 2, 8, 1, 9])  # Returns new sorted list

# Sort with key
funds = [
    {"code": "FND001", "aum": 5000000},
    {"code": "FND002", "aum": 3000000},
    {"code": "FND003", "aum": 8000000}
]
sorted_funds = sorted(funds, key=lambda x: x["aum"], reverse=True)
```

**ETL Use Cases:**
```python
# Collecting error records
error_records = []

for record in source_data:
    if not validate(record):
        error_records.append(record)

# Building dynamic column lists
source_cols = ["fund_code", "fund_name", "aum", "revenue"]
dimension_cols = ["fund_key"] + source_cols + ["effective_date", "is_current"]

# Filtering data
valid_records = [r for r in records if r["amount"] > 0]
```

---

### **1.2 Tuples**

**Characteristics:**
- Ordered, **immutable**, allows duplicates
- Faster than lists
- Can be used as dictionary keys
- Memory efficient

```python
# Creating tuples
empty_tuple = ()
single_item = (1,)  # Note the comma!
coordinates = (10.5, 20.3)
record = ("FND001", "Growth Fund", 1000000)

# Unpacking
fund_code, fund_name, aum = record
print(fund_code)  # "FND001"

# Named tuple unpacking
x, y = coordinates

# Can't modify (immutable)
# record[0] = "FND002"  # TypeError!

# But can contain mutable objects
tuple_with_list = (1, 2, [3, 4])
tuple_with_list[2].append(5)  # This works!
print(tuple_with_list)  # (1, 2, [3, 4, 5])

# Tuple methods
numbers = (1, 2, 3, 2, 4, 2)
count_of_2 = numbers.count(2)   # 3
index_of_3 = numbers.index(3)   # 2

# Multiple return values
def get_stats(numbers):
    return len(numbers), sum(numbers), max(numbers)

count, total, maximum = get_stats([1, 2, 3, 4, 5])

# Tuple as dictionary key
location_data = {
    (40.7128, -74.0060): "New York",
    (51.5074, -0.1278): "London"
}
```

**ETL Use Cases:**
```python
# Composite keys
upsert_keys = ("fund_code", "share_class", "date")

# Configuration that shouldn't change
REDSHIFT_ENDPOINTS = (
    "cluster1.redshift.amazonaws.com",
    "cluster2.redshift.amazonaws.com"
)

# Multiple return values
def process_file(filepath):
    records_read = 1000
    records_valid = 950
    records_error = 50
    return records_read, records_valid, records_error

total, valid, errors = process_file("data.csv")

# Spark row representation
row = ("FND001", "A", 1000000, "2025-01-15")
fund_code, share_class, aum, date = row
```

---

### **1.3 Named Tuples**

**Characteristics:**
- Immutable like regular tuples
- Fields accessible by name (like class attributes)
- Memory efficient (lighter than classes)
- Self-documenting code

```python
from collections import namedtuple

# Define a named tuple
Fund = namedtuple('Fund', ['code', 'name', 'aum', 'management_fee'])

# Create instances
fund1 = Fund(code="FND001", name="Growth Fund", aum=5000000, management_fee=0.0075)
fund2 = Fund("FND002", "Value Fund", 3000000, 0.0050)  # Positional args

# Access by name (much clearer!)
print(fund1.code)              # "FND001"
print(fund1.management_fee)    # 0.0075

# Still works like a tuple
print(fund1[0])                # "FND001"
print(fund1[:2])               # ('FND001', 'Growth Fund')

# Convert to dictionary
fund_dict = fund1._asdict()
print(fund_dict)
# {'code': 'FND001', 'name': 'Growth Fund', 'aum': 5000000, 'management_fee': 0.0075}

# Replace values (returns new instance - immutable!)
fund1_updated = fund1._replace(aum=6000000)
print(fund1.aum)          # 5000000 (original unchanged)
print(fund1_updated.aum)  # 6000000

# Create from iterable
fields = ["FND003", "Income Fund", 2000000, 0.0060]
fund3 = Fund._make(fields)

# Named tuple with defaults (Python 3.7+)
FundWithDefaults = namedtuple('Fund', ['code', 'name', 'aum'], defaults=[0])
fund4 = FundWithDefaults("FND004", "New Fund")  # aum defaults to 0
```

**ETL Use Cases:**
```python
# Configuration objects
RedshiftConfig = namedtuple('RedshiftConfig', [
    'workgroup', 'database', 'schema', 'secret_arn', 'iam_role'
])

config = RedshiftConfig(
    workgroup='nyl-invgai-wg',
    database='nyl_anaplan_db',
    schema='public',
    secret_arn='arn:aws:secretsmanager:...',
    iam_role='arn:aws:iam::...'
)

# Clear, self-documenting access
print(config.workgroup)
print(config.secret_arn)

# Data validation results
ValidationResult = namedtuple('ValidationResult', [
    'is_valid', 'record_count', 'error_count', 'errors'
])

result = ValidationResult(
    is_valid=False,
    record_count=1000,
    error_count=50,
    errors=["Missing fund_code", "Invalid date format"]
)

if not result.is_valid:
    print(f"Validation failed: {result.error_count} errors")
    for error in result.errors:
        print(f"  - {error}")

# Job metrics
JobMetrics = namedtuple('JobMetrics', [
    'job_id', 'start_time', 'end_time', 'records_processed',
    'records_inserted', 'records_updated', 'duration_seconds'
])

metrics = JobMetrics(
    job_id="job_001",
    start_time="2025-01-15 10:00:00",
    end_time="2025-01-15 10:05:30",
    records_processed=10000,
    records_inserted=2000,
    records_updated=8000,
    duration_seconds=330
)

print(f"Job {metrics.job_id} processed {metrics.records_processed} records in {metrics.duration_seconds}s")
```

---

### **1.4 Sets**

**Characteristics:**
- Unordered, mutable, **no duplicates**
- Fast membership testing
- Supports mathematical set operations

```python
# Creating sets
empty_set = set()  # Note: {} creates empty dict!
numbers = {1, 2, 3, 4, 5}
funds = {"FND001", "FND002", "FND003"}

# Add elements
funds.add("FND004")
funds.add("FND001")  # Duplicate ignored
print(funds)  # {'FND001', 'FND002', 'FND003', 'FND004'}

# Remove elements
funds.remove("FND004")      # Raises KeyError if not found
funds.discard("FND999")     # No error if not found
popped = funds.pop()        # Remove and return arbitrary element

# Set operations
set1 = {1, 2, 3, 4, 5}
set2 = {4, 5, 6, 7, 8}

# Union (all elements from both sets)
union = set1 | set2
# or: union = set1.union(set2)
print(union)  # {1, 2, 3, 4, 5, 6, 7, 8}

# Intersection (common elements)
intersection = set1 & set2
# or: intersection = set1.intersection(set2)
print(intersection)  # {4, 5}

# Difference (in set1 but not in set2)
difference = set1 - set2
# or: difference = set1.difference(set2)
print(difference)  # {1, 2, 3}

# Symmetric difference (in either set but not both)
sym_diff = set1 ^ set2
# or: sym_diff = set1.symmetric_difference(set2)
print(sym_diff)  # {1, 2, 3, 6, 7, 8}

# Membership testing (very fast!)
if "FND001" in funds:
    print("Fund found!")

# Remove duplicates from list
numbers_with_dupes = [1, 2, 2, 3, 3, 3, 4, 4, 4, 4]
unique_numbers = list(set(numbers_with_dupes))
print(unique_numbers)  # [1, 2, 3, 4] (order may vary)

# Subset and superset
set_a = {1, 2, 3}
set_b = {1, 2, 3, 4, 5}

print(set_a.issubset(set_b))    # True
print(set_b.issuperset(set_a))  # True
print(set_a.isdisjoint(set_b))  # False (they have common elements)
```

**ETL Use Cases:**
```python
# Find new records
current_fund_codes = {"FND001", "FND002", "FND003"}
incoming_fund_codes = {"FND002", "FND003", "FND004", "FND005"}

# New funds to insert
new_funds = incoming_fund_codes - current_fund_codes
print(new_funds)  # {'FND004', 'FND005'}

# Funds to update (already exist)
existing_funds = incoming_fund_codes & current_fund_codes
print(existing_funds)  # {'FND002', 'FND003'}

# Funds to deactivate (no longer in source)
funds_to_deactivate = current_fund_codes - incoming_fund_codes
print(funds_to_deactivate)  # {'FND001'}

# Remove duplicates while preserving order
def remove_duplicates_ordered(items):
    seen = set()
    result = []
    for item in items:
        if item not in seen:
            seen.add(item)
            result.append(item)
    return result

records = ["FND001", "FND002", "FND001", "FND003", "FND002"]
unique_records = remove_duplicates_ordered(records)
print(unique_records)  # ['FND001', 'FND002', 'FND003']

# Check for required columns
required_columns = {"fund_code", "share_class", "aum"}
actual_columns = {"fund_code", "share_class", "aum", "revenue"}

missing_columns = required_columns - actual_columns
if missing_columns:
    raise ValueError(f"Missing required columns: {missing_columns}")

# Validate upsert keys exist
upsert_keys = {"fund_code", "share_class"}
df_columns = set(df.columns)

if not upsert_keys.issubset(df_columns):
    missing = upsert_keys - df_columns
    raise ValueError(f"Upsert keys not found in DataFrame: {missing}")
```

---

### **1.5 Dictionaries**

**Characteristics:**
- Unordered (ordered in Python 3.7+), mutable
- Key-value pairs
- Keys must be immutable (strings, numbers, tuples)
- Fast lookups by key

```python
# Creating dictionaries
empty_dict = {}
empty_dict = dict()

fund = {
    "code": "FND001",
    "name": "Growth Fund",
    "aum": 5000000,
    "management_fee": 0.0075
}

# Accessing values
print(fund["code"])           # "FND001"
print(fund.get("code"))       # "FND001"
print(fund.get("missing"))    # None (doesn't raise error)
print(fund.get("missing", 0)) # 0 (default value)

# Adding/updating
fund["asset_class"] = "Equity"
fund["aum"] = 6000000  # Update existing

# Removing
del fund["asset_class"]
popped_value = fund.pop("name")  # Returns value and removes key
popped_item = fund.popitem()     # Removes and returns (key, value)

# Check key existence
if "code" in fund:
    print("Code exists")

# Get all keys, values, items
keys = fund.keys()      # dict_keys(['code', 'aum', 'management_fee'])
values = fund.values()  # dict_values(['FND001', 6000000, 0.0075])
items = fund.items()    # dict_items([('code', 'FND001'), ...])

# Iterate over dictionary
for key, value in fund.items():
    print(f"{key}: {value}")

# Dictionary comprehension
numbers = [1, 2, 3, 4, 5]
squares = {x: x**2 for x in numbers}
print(squares)  # {1: 1, 2: 4, 3: 9, 4: 16, 5: 25}

# Filter dictionary
fund_data = {
    "code": "FND001",
    "name": "Growth Fund",
    "aum": 5000000,
    "temp_field": None
}
cleaned = {k: v for k, v in fund_data.items() if v is not None}

# Merge dictionaries
dict1 = {"a": 1, "b": 2}
dict2 = {"c": 3, "d": 4}

# Python 3.9+
merged = dict1 | dict2
print(merged)  # {'a': 1, 'b': 2, 'c': 3, 'd': 4}

# Python 3.5+
merged = {**dict1, **dict2}

# Update method (modifies dict1)
dict1.update(dict2)

# setdefault (get or set default)
counts = {}
counts.setdefault("fund_001", 0)
counts["fund_001"] += 1

# Better: defaultdict
from collections import defaultdict

counts = defaultdict(int)  # Default value is 0
counts["fund_001"] += 1    # No need to check if key exists!

# Nested dictionaries
data = {
    "dimensions": {
        "fund": ["FND001", "FND002"],
        "advisor": ["ADV001", "ADV002"]
    },
    "facts": {
        "aum_revenue": 1000,
        "expense": 500
    }
}

print(data["dimensions"]["fund"][0])  # "FND001"
```

**ETL Use Cases:**
```python
# Configuration management
config = {
    "source": {
        "bucket": "my-bucket",
        "path": "data/input/",
        "file_pattern": "*.csv"
    },
    "target": {
        "workgroup": "my-workgroup",
        "database": "my_db",
        "schema": "public",
        "table": "dim_fund"
    },
    "processing": {
        "batch_size": 10000,
        "max_retries": 3
    }
}

# Accessing nested config
source_bucket = config["source"]["bucket"]
max_retries = config["processing"]["max_retries"]

# Lookup tables
fund_lookup = {
    "FND001": {"name": "Growth Fund", "asset_class": "Equity"},
    "FND002": {"name": "Income Fund", "asset_class": "Fixed Income"}
}

# Fast lookups during processing
for record in source_data:
    fund_code = record["fund_code"]
    if fund_code in fund_lookup:
        record["fund_name"] = fund_lookup[fund_code]["name"]

# Counting occurrences
from collections import Counter

fund_codes = ["FND001", "FND002", "FND001", "FND003", "FND001"]
counts = Counter(fund_codes)
print(counts)  # Counter({'FND001': 3, 'FND002': 1, 'FND003': 1})

# Most common
most_common = counts.most_common(2)
print(most_common)  # [('FND001', 3), ('FND002', 1)]

# Grouping data
from collections import defaultdict

records = [
    {"fund": "FND001", "amount": 100},
    {"fund": "FND002", "amount": 200},
    {"fund": "FND001", "amount": 150}
]

grouped = defaultdict(list)
for record in records:
    grouped[record["fund"]].append(record["amount"])

print(dict(grouped))
# {'FND001': [100, 150], 'FND002': [200]}

# Data quality metrics
quality_metrics = {
    "total_records": 10000,
    "valid_records": 9500,
    "null_keys": 50,
    "duplicate_keys": 30,
    "invalid_dates": 20,
    "negative_amounts": 400
}

# Job parameters
job_params = {
    "SOURCE_FILE": "s3://bucket/input/fund.csv",
    "TARGET_TABLE": "dim_fund",
    "NATURAL_KEYS": ["fund_code", "share_class"],
    "SCD_ATTRIBUTES": ["fund_name", "management_fee"],
    "SURROGATE_KEY": "fund_key"
}
```

---

## **2. File Handling** {#file-handling}

### **2.1 Opening and Closing Files**

```python
# Basic file operations
# Method 1: Manual close (not recommended)
file = open("data.txt", "r")
content = file.read()
file.close()  # Must remember to close!

# Method 2: Context manager (RECOMMENDED)
with open("data.txt", "r") as file:
    content = file.read()
# File automatically closed when exiting the 'with' block

# File modes
# 'r'  - Read (default)
# 'w'  - Write (overwrites existing file)
# 'a'  - Append
# 'r+' - Read and write
# 'rb' - Read binary
# 'wb' - Write binary
```

### **2.2 Reading Files**

```python
# Read entire file
with open("data.txt", "r") as f:
    content = f.read()
    print(content)

# Read line by line (memory efficient for large files)
with open("data.txt", "r") as f:
    for line in f:
        print(line.strip())  # strip() removes newline

# Read all lines into list
with open("data.txt", "r") as f:
    lines = f.readlines()  # ['line1\n', 'line2\n', ...]

# Read specific number of characters
with open("data.txt", "r") as f:
    first_100_chars = f.read(100)

# Read one line at a time
with open("data.txt", "r") as f:
    line1 = f.readline()
    line2 = f.readline()
```

### **2.3 Writing Files**

```python
# Write to file (overwrites)
with open("output.txt", "w") as f:
    f.write("Hello World\n")
    f.write("Second line\n")

# Write multiple lines
lines = ["Line 1\n", "Line 2\n", "Line 3\n"]
with open("output.txt", "w") as f:
    f.writelines(lines)

# Append to file
with open("log.txt", "a") as f:
    f.write("New log entry\n")

# Writing with print (automatically adds newline)
with open("output.txt", "w") as f:
    print("Hello World", file=f)
    print("Second line", file=f)
```

### **2.4 Working with CSV Files**

```python
import csv

# Reading CSV
with open("data.csv", "r") as f:
    reader = csv.reader(f)
    header = next(reader)  # Skip header
    for row in reader:
        print(row)  # row is a list

# Reading CSV as dictionaries
with open("data.csv", "r") as f:
    reader = csv.DictReader(f)
    for row in reader:
        print(row["fund_code"])  # Access by column name
        print(row["aum"])

# Writing CSV
data = [
    ["fund_code", "fund_name", "aum"],
    ["FND001", "Growth Fund", 5000000],
    ["FND002", "Value Fund", 3000000]
]

with open("output.csv", "w", newline='') as f:
    writer = csv.writer(f)
    writer.writerows(data)

# Writing CSV from dictionaries
data = [
    {"fund_code": "FND001", "fund_name": "Growth Fund", "aum": 5000000},
    {"fund_code": "FND002", "fund_name": "Value Fund", "aum": 3000000}
]

with open("output.csv", "w", newline='') as f:
    fieldnames = ["fund_code", "fund_name", "aum"]
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(data)
```

### **2.5 Working with JSON Files**

```python
import json

# Reading JSON
with open("data.json", "r") as f:
    data = json.load(f)  # Returns dict or list

# Writing JSON
data = {
    "fund_code": "FND001",
    "fund_name": "Growth Fund",
    "aum": 5000000
}

with open("output.json", "w") as f:
    json.dump(data, f, indent=2)  # Pretty print with indent

# JSON string conversion
json_string = json.dumps(data)  # Dict to JSON string
data = json.loads(json_string)  # JSON string to dict
```

### **2.6 File Path Operations**

```python
import os
from pathlib import Path

# Using os module
current_dir = os.getcwd()
file_path = os.path.join(current_dir, "data", "input.csv")

if os.path.exists(file_path):
    print("File exists")

if os.path.isfile(file_path):
    print("It's a file")

if os.path.isdir("/path/to/dir"):
    print("It's a directory")

# List files in directory
files = os.listdir("/path/to/dir")

# Get file info
file_size = os.path.getsize(file_path)
mod_time = os.path.getmtime(file_path)

# Using pathlib (modern, recommended)
path = Path("data/input.csv")

if path.exists():
    print("File exists")

if path.is_file():
    print("It's a file")

# Read/write with pathlib
content = path.read_text()
path.write_text("Hello World")

# Path operations
parent = path.parent        # data/
name = path.name           # input.csv
stem = path.stem           # input
suffix = path.suffix       # .csv

# Create directories
Path("data/output").mkdir(parents=True, exist_ok=True)

# List files with pattern
csv_files = list(Path("data").glob("*.csv"))
all_csv_files = list(Path("data").rglob("*.csv"))  # Recursive
```

### **2.7 ETL File Handling Examples**

```python
# Processing large CSV in chunks
def process_large_csv(filepath, chunk_size=10000):
    """Process large CSV file in chunks"""
    with open(filepath, 'r') as f:
        reader = csv.DictReader(f)
        chunk = []
        
        for i, row in enumerate(reader, 1):
            chunk.append(row)
            
            if i % chunk_size == 0:
                # Process chunk
                process_chunk(chunk)
                chunk = []
        
        # Process remaining records
        if chunk:
            process_chunk(chunk)

# Reading configuration file
def load_config(config_path):
    """Load JSON configuration"""
    with open(config_path, 'r') as f:
        config = json.load(f)
    
    # Validate required keys
    required = ["source_bucket", "target_table", "upsert_keys"]
    for key in required:
        if key not in config:
            raise ValueError(f"Missing required config key: {key}")
    
    return config

# Writing job metrics
def write_job_metrics(metrics, output_path):
    """Write job metrics to JSON"""
    metrics_data = {
        "job_id": metrics.job_id,
        "start_time": metrics.start_time,
        "end_time": metrics.end_time,
        "records_processed": metrics.records_processed,
        "records_inserted": metrics.records_inserted,
        "records_updated": metrics.records_updated,
        "duration_seconds": metrics.duration_seconds
    }
    
    with open(output_path, 'w') as f:
        json.dump(metrics_data, f, indent=2)

# Log file rotation
def append_to_log(message, log_path, max_size_mb=10):
    """Append to log file with rotation"""
    max_size_bytes = max_size_mb * 1024 * 1024
    
    # Check if rotation needed
    if os.path.exists(log_path):
        if os.path.getsize(log_path) > max_size_bytes:
            # Rotate log file
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            archive_path = f"{log_path}.{timestamp}"
            os.rename(log_path, archive_path)
    
    # Append message
    with open(log_path, 'a') as f:
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        f.write(f"[{timestamp}] {message}\n")
```

---

## **3. Exception Handling** {#exception-handling}

### **3.1 Basic Try-Except**

```python
# Basic exception handling
try:
    result = 10 / 0
except ZeroDivisionError:
    print("Cannot divide by zero!")

# Catching multiple exceptions
try:
    file = open("nonexistent.txt", "r")
    data = file.read()
except FileNotFoundError:
    print("File not found")
except PermissionError:
    print("Permission denied")

# Catching any exception
try:
    risky_operation()
except Exception as e:
    print(f"An error occurred: {e}")

# Multiple exceptions in one line
try:
    operation()
except (ValueError, TypeError, KeyError) as e:
    print(f"Error: {e}")
```

### **3.2 Else and Finally**

```python
try:
    file = open("data.txt", "r")
    content = file.read()
except FileNotFoundError:
    print("File not found")
else:
    # Executes if no exception occurred
    print(f"File read successfully: {len(content)} characters")
finally:
    # Always executes (cleanup code)
    if 'file' in locals():
        file.close()
    print("Cleanup complete")

# Better with context manager
try:
    with open("data.txt", "r") as f:
        content = f.read()
except FileNotFoundError:
    print("File not found")
else:
    print("Success")
finally:
    print("Done")
```

### **3.3 Raising Exceptions**

```python
# Raise built-in exception
def validate_amount(amount):
    if amount < 0:
        raise ValueError("Amount cannot be negative")
    if not isinstance(amount, (int, float)):
        raise TypeError("Amount must be a number")
    return amount

# Re-raise exception
try:
    validate_amount(-100)
except ValueError:
    print("Validation failed")
    raise  # Re-raise the same exception

# Raise from another exception
try:
    int("invalid")
except ValueError as e:
    raise TypeError("Type conversion failed") from e
```

### **3.4 Custom Exceptions**

```python
# Define custom exception
class DataValidationError(Exception):
    """Raised when data validation fails"""
    pass

class InsufficientDataError(Exception):
    """Raised when not enough data is available"""
    def __init__(self, required, actual):
        self.required = required
        self.actual = actual
        super().__init__(f"Required {required} records, got {actual}")

# Using custom exceptions
def validate_data(records):
    if len(records) == 0:
        raise InsufficientDataError(required=1, actual=0)
    
    for record in records:
        if "fund_code" not in record:
            raise DataValidationError("Missing fund_code in record")

# Catching custom exceptions
try:
    validate_data([])
except InsufficientDataError as e:
    print(f"Error: {e}")
    print(f"Required: {e.required}, Actual: {e.actual}")
except DataValidationError as e:
    print(f"Validation error: {e}")
```

### **3.5 Exception Chaining**

```python
# Exception context (automatic)
try:
    result = 1 / 0
except ZeroDivisionError as e:
    try:
        log_error(e)
    except Exception as logging_error:
        # The original exception (e) is preserved in __context__
        print(f"Original error: {logging_error.__context__}")

# Explicit exception chaining
try:
    data = json.loads(invalid_json)
except json.JSONDecodeError as e:
    raise DataValidationError("Invalid JSON in config file") from e
```

### **3.6 ETL Exception Handling Patterns**

```python
# Retry logic with exponential backoff
import time

def retry_on_failure(func, max_attempts=3, delay=1):
    """Retry a function on failure"""
    for attempt in range(1, max_attempts + 1):
        try:
            return func()
        except Exception as e:
            if attempt == max_attempts:
                raise
            
            wait_time = delay * (2 ** (attempt - 1))
            print(f"Attempt {attempt} failed: {e}")
            print(f"Retrying in {wait_time} seconds...")
            time.sleep(wait_time)

# Usage
def connect_to_database():
    # Connection logic that might fail
    pass

result = retry_on_failure(connect_to_database, max_attempts=3, delay=2)

# Graceful degradation
def process_record(record):
    """Process record with graceful degradation"""
    try:
        # Main processing
        validate(record)
        transform(record)
        load(record)
        return {"status": "success", "record": record}
    
    except ValidationError as e:
        # Log validation error but continue
        log.warning(f"Validation failed for record {record['id']}: {e}")
        return {"status": "validation_failed", "record": record, "error": str(e)}
    
    except TransformationError as e:
        # Log transformation error
        log.error(f"Transformation failed for record {record['id']}: {e}")
        return {"status": "transformation_failed", "record": record, "error": str(e)}
    
    except LoadError as e:
        # Critical error - needs retry
        log.critical(f"Load failed for record {record['id']}: {e}")
        raise  # Re-raise for retry logic

# Batch processing with error collection
def process_batch(records):
    """Process batch and collect errors"""
    successful = []
    failed = []
    
    for record in records:
        try:
            result = process_record(record)
            successful.append(result)
        except Exception as e:
            failed.append({
                "record": record,
                "error": str(e),
                "traceback": traceback.format_exc()
            })
    
    return {
        "successful_count": len(successful),
        "failed_count": len(failed),
        "successful": successful,
        "failed": failed
    }

# Context managers for cleanup
from contextlib import contextmanager

@contextmanager
def database_connection(config):
    """Context manager for database connections"""
    conn = None
    try:
        conn = connect(config)
        yield conn
    except Exception as e:
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            conn.close()

# Usage
with database_connection(config) as conn:
    conn.execute("INSERT INTO table VALUES (...)")
# Connection automatically closed even if exception occurs

# Comprehensive ETL error handling
import traceback
import boto3

def etl_main():
    """Main ETL function with comprehensive error handling"""
    job_id = generate_job_id()
    start_time = datetime.now()
    status = "FAILED"
    error_message = None
    
    try:
        # Step 1: Read source
        try:
            df = read_source_file(config['source_file'])
        except FileNotFoundError as e:
            raise ETLError(f"Source file not found: {config['source_file']}") from e
        except Exception as e:
            raise ETLError(f"Failed to read source file: {e}") from e
        
        # Step 2: Validate
        try:
            validation_results = validate_data(df)
            if not validation_results.is_valid:
                raise DataValidationError(f"Validation failed: {validation_results.errors}")
        except DataValidationError as e:
            # Write validation report
            write_validation_report(validation_results, job_id)
            raise
        
        # Step 3: Transform
        try:
            transformed_df = transform_data(df)
        except Exception as e:
            raise ETLError(f"Transformation failed: {e}") from e
        
        # Step 4: Load
        try:
            load_to_redshift(transformed_df, config['target_table'])
        except Exception as e:
            raise ETLError(f"Load to Redshift failed: {e}") from e
        
        # Success
        status = "SUCCESS"
        
    except ETLError as e:
        error_message = str(e)
        log.error(f"ETL failed: {e}")
        log.error(f"Traceback: {traceback.format_exc()}")
        
        # Send alert
        send_sns_alert(
            subject=f"ETL Job Failed: {job_id}",
            message=f"Error: {error_message}\n\nTraceback:\n{traceback.format_exc()}"
        )
        
        raise
    
    except Exception as e:
        error_message = f"Unexpected error: {e}"
        log.critical(f"Unexpected error: {e}")
        log.critical(f"Traceback: {traceback.format_exc()}")
        
        send_sns_alert(
            subject=f"ETL Job Critical Failure: {job_id}",
            message=f"Unexpected error: {error_message}\n\nTraceback:\n{traceback.format_exc()}"
        )
        
        raise
    
    finally:
        # Always update job status
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        try:
            update_job_status(
                job_id=job_id,
                start_time=start_time,
                end_time=end_time,
                duration=duration,
                status=status,
                error_message=error_message or "NULL"
            )
        except Exception as e:
            log.error(f"Failed to update job status: {e}")
        
        # Export logs
        try:
            export_logs_to_s3(job_id)
        except Exception as e:
            log.error(f"Failed to export logs: {e}")
```

---

## **4. Lambda Functions** {#lambda-functions}

### **4.1 Basic Lambda Syntax**

```python
# Regular function
def add(x, y):
    return x + y

# Lambda equivalent
add = lambda x, y: x + y

# Calling lambda
result = add(5, 3)  # 8

# Anonymous lambda (no name)
result = (lambda x, y: x + y)(5, 3)  # 8
```

### **4.2 Lambda with Built-in Functions**

```python
# map() - apply function to each item
numbers = [1, 2, 3, 4, 5]
squared = list(map(lambda x: x**2, numbers))
print(squared)  # [1, 4, 9, 16, 25]

# filter() - keep items that return True
numbers = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
even = list(filter(lambda x: x % 2 == 0, numbers))
print(even)  # [2, 4, 6, 8, 10]

# sorted() - sort with custom key
funds = [
    {"code": "FND001", "aum": 5000000},
    {"code": "FND002", "aum": 3000000},
    {"code": "FND003", "aum": 8000000}
]
sorted_funds = sorted(funds, key=lambda x: x["aum"], reverse=True)

# max/min with key
largest_fund = max(funds, key=lambda x: x["aum"])
smallest_fund = min(funds, key=lambda x: x["aum"])

# reduce() - accumulate values
from functools import reduce

numbers = [1, 2, 3, 4, 5]
sum_all = reduce(lambda x, y: x + y, numbers)
print(sum_all)  # 15

product = reduce(lambda x, y: x * y, numbers)
print(product)  # 120
```

### **4.3 Lambda Limitations**

```python
# Lambda can only contain expressions, not statements

# Valid lambda (expression)
add = lambda x, y: x + y

# Invalid lambda (statement)
# invalid = lambda x: print(x)  # SyntaxError

# Can't use multiple lines
# invalid = lambda x: 
#     result = x * 2
#     return result  # SyntaxError

# Can use conditional expressions (ternary operator)
abs_value = lambda x: x if x >= 0 else -x
print(abs_value(-5))  # 5

# Can nest lambdas (but don't - hard to read!)
multiply_then_add = lambda x: (lambda y: x * y + 10)
result = multiply_then_add(5)(3)  # (5 * 3) + 10 = 25
```

### **4.4 ETL Lambda Examples**

```python
# Column transformation
df_spark = df_spark.withColumn(
    "management_fee_pct",
    (col("management_fee") * 100).cast("decimal(5,2)")
)

# PySpark - filter with lambda
from pyspark.sql.functions import udf
from pyspark.sql.types import BooleanType

# Define UDF with lambda
is_valid_code = udf(lambda code: len(code) == 6, BooleanType())
df_filtered = df_spark.filter(is_valid_code(col("fund_code")))

# Data cleaning
records = [
    {"fund_code": "FND001", "aum": 5000000},
    {"fund_code": "", "aum": 3000000},  # Invalid
    {"fund_code": "FND003", "aum": None}  # Invalid
]

# Filter valid records
valid_records = list(filter(
    lambda r: r["fund_code"] and r["aum"] is not None,
    records
))

# Transform column names
df_columns = ["Fund Code", "Fund-Name", "AUM Value"]
clean_columns = list(map(
    lambda col: col.lower().replace(" ", "_").replace("-", "_"),
    df_columns
))
print(clean_columns)  # ['fund_code', 'fund_name', 'aum_value']

# Sort by multiple keys
records = [
    {"fund": "FND001", "date": "2025-01-15", "aum": 5000000},
    {"fund": "FND001", "date": "2025-01-14", "aum": 4900000},
    {"fund": "FND002", "date": "2025-01-15", "aum": 3000000}
]

# Sort by fund, then by date (descending)
sorted_records = sorted(
    records,
    key=lambda r: (r["fund"], r["date"]),
    reverse=True
)

# Dictionary transformation
config = {
    "SOURCE_FILE": "fund.csv",
    "TARGET_TABLE": "dim_fund",
    "NATURAL_KEYS": "fund_code,share_class"
}

# Parse comma-separated values
parsed_config = {
    k: v.split(",") if k == "NATURAL_KEYS" else v
    for k, v in config.items()
}

# Group by key
from itertools import groupby

records = [
    {"fund": "FND001", "amount": 100},
    {"fund": "FND001", "amount": 150},
    {"fund": "FND002", "amount": 200}
]

# Must sort first for groupby
records_sorted = sorted(records, key=lambda r: r["fund"])

grouped = {
    key: list(group)
    for key, group in groupby(records_sorted, key=lambda r: r["fund"])
}
```

### **4.5 When to Use Lambda (and When Not To)**

```python
# GOOD: Simple, one-time use
numbers = [1, 2, 3, 4, 5]
squares = list(map(lambda x: x**2, numbers))

# GOOD: Sorting key
funds.sort(key=lambda f: f["aum"])

# BAD: Complex logic (use regular function)
# Don't do this:
process = lambda x: x.strip().lower().replace(" ", "_") if x else "unknown"

# Do this instead:
def clean_text(text):
    """Clean and normalize text"""
    if not text:
        return "unknown"
    return text.strip().lower().replace(" ", "_")

# BAD: Reusing lambda (use regular function)
# Don't do this:
calculate_fee = lambda aum, rate: aum * rate
fee1 = calculate_fee(1000000, 0.0075)
fee2 = calculate_fee(2000000, 0.0075)
fee3 = calculate_fee(3000000, 0.0075)

# Do this instead:
def calculate_management_fee(aum, rate):
    """Calculate management fee"""
    return aum * rate
```

---

## **5. Decorators** {#decorators}

### **5.1 Basic Decorator Concept**

```python
# Simple function
def greet(name):
    return f"Hello, {name}!"

# Decorator function
def uppercase_decorator(func):
    def wrapper(*args, **kwargs):
        result = func(*args, **kwargs)
        return result.upper()
    return wrapper

# Apply decorator
@uppercase_decorator
def greet(name):
    return f"Hello, {name}!"

print(greet("Alice"))  # HELLO, ALICE!

# Without @ syntax (equivalent)
def greet(name):
    return f"Hello, {name}!"

greet = uppercase_decorator(greet)
```

### **5.2 Decorator with functools.wraps**

```python
import functools

# Without functools.wraps
def my_decorator(func):
    def wrapper(*args, **kwargs):
        return func(*args, **kwargs)
    return wrapper

@my_decorator
def greet(name):
    """Greet someone"""
    return f"Hello, {name}!"

print(greet.__name__)  # wrapper (wrong!)
print(greet.__doc__)   # None (lost!)

# With functools.wraps (preserves metadata)
def my_decorator(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        return func(*args, **kwargs)
    return wrapper

@my_decorator
def greet(name):
    """Greet someone"""
    return f"Hello, {name}!"

print(greet.__name__)  # greet (correct!)
print(greet.__doc__)   # Greet someone (preserved!)
```

### **5.3 Decorator with Arguments**

```python
# Decorator that takes arguments
def repeat(times):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            for _ in range(times):
                result = func(*args, **kwargs)
            return result
        return wrapper
    return decorator

@repeat(times=3)
def greet(name):
    print(f"Hello, {name}!")

greet("Alice")
# Output:
# Hello, Alice!
# Hello, Alice!
# Hello, Alice!
```

### **5.4 Common Decorator Patterns**

#### **Timing Decorator**
```python
import time
import functools

def timer(func):
    """Measure execution time"""
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        duration = end_time - start_time
        print(f"{func.__name__} took {duration:.2f} seconds")
        return result
    return wrapper

@timer
def process_large_file(filepath):
    # Processing logic
    time.sleep(2)  # Simulate work
    return "Done"

result = process_large_file("data.csv")
# Output: process_large_file took 2.00 seconds
```

#### **Logging Decorator**
```python
import functools
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def log_calls(func):
    """Log function calls"""
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        logger.info(f"Calling {func.__name__} with args={args}, kwargs={kwargs}")
        try:
            result = func(*args, **kwargs)
            logger.info(f"{func.__name__} returned {result}")
            return result
        except Exception as e:
            logger.error(f"{func.__name__} raised {type(e).__name__}: {e}")
            raise
    return wrapper

@log_calls
def divide(a, b):
    return a / b

result = divide(10, 2)  # Logs call and result
```

#### **Retry Decorator**
```python
import time
import functools

def retry(max_attempts=3, delay=1):
    """Retry function on failure"""
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            attempt = 0
            while attempt < max_attempts:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    attempt += 1
                    if attempt >= max_attempts:
                        raise
                    print(f"Attempt {attempt} failed: {e}. Retrying in {delay}s...")
                    time.sleep(delay)
        return wrapper
    return decorator

@retry(max_attempts=3, delay=2)
def connect_to_database():
    # Connection logic that might fail
    import random
    if random.random() < 0.7:
        raise ConnectionError("Connection failed")
    return "Connected!"

result = connect_to_database()
```

#### **Cache Decorator**
```python
import functools

def memoize(func):
    """Cache function results"""
    cache = {}
    
    @functools.wraps(func)
    def wrapper(*args):
        if args in cache:
            print(f"Cache hit for {args}")
            return cache[args]
        
        print(f"Computing result for {args}")
        result = func(*args)
        cache[args] = result
        return result
    
    return wrapper

@memoize
def fibonacci(n):
    if n < 2:
        return n
    return fibonacci(n-1) + fibonacci(n-2)

print(fibonacci(10))  # Much faster with caching!

# Or use built-in lru_cache
from functools import lru_cache

@lru_cache(maxsize=128)
def fibonacci(n):
    if n < 2:
        return n
    return fibonacci(n-1) + fibonacci(n-2)
```

### **5.5 ETL Decorator Examples**

```python
# Validation decorator
def validate_dataframe(required_columns):
    """Validate DataFrame has required columns"""
    def decorator(func):
        @functools.wraps(func)
        def wrapper(df, *args, **kwargs):
            missing = set(required_columns) - set(df.columns)
            if missing:
                raise ValueError(f"Missing required columns: {missing}")
            return func(df, *args, **kwargs)
        return wrapper
    return decorator

@validate_dataframe(required_columns=["fund_code", "share_class", "aum"])
def transform_fund_data(df):
    # Transformation logic
    return df

# Metrics tracking decorator
import time

def track_metrics(metric_name):
    """Track execution metrics"""
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            start_time = time.time()
            start_memory = get_memory_usage()
            
            try:
                result = func(*args, **kwargs)
                status = "SUCCESS"
                error = None
            except Exception as e:
                status = "FAILED"
                error = str(e)
                raise
            finally:
                end_time = time.time()
                end_memory = get_memory_usage()
                duration = end_time - start_time
                memory_delta = end_memory - start_memory
                
                # Log metrics
                log_metric({
                    "metric_name": metric_name,
                    "function": func.__name__,
                    "duration_seconds": duration,
                    "memory_delta_mb": memory_delta,
                    "status": status,
                    "error": error
                })
            
            return result
        return wrapper
    return decorator

@track_metrics(metric_name="dimension_load")
def load_dimension(df, table_name):
    # Load logic
    pass

# Transaction decorator (for database operations)
def transaction(func):
    """Wrap function in database transaction"""
    @functools.wraps(func)
    def wrapper(conn, *args, **kwargs):
        try:
            result = func(conn, *args, **kwargs)
            conn.commit()
            return result
        except Exception as e:
            conn.rollback()
            raise
    return wrapper

@transaction
def update_records(conn, records):
    cursor = conn.cursor()
    for record in records:
        cursor.execute("UPDATE table SET ... WHERE id = ?", record)
```

### **5.6 Stacking Multiple Decorators**

```python
@timer
@log_calls
@retry(max_attempts=3)
def critical_etl_function():
    # Will be retried, logged, and timed
    pass

# Execution order (bottom to top):
# 1. retry wraps the original function
# 2. log_calls wraps the retry wrapper
# 3. timer wraps the log_calls wrapper
```

---

## **6. Generators** {#generators}

### **6.1 What are Generators?**

Generators are functions that return an iterator. They generate values on-the-fly and don't store the entire sequence in memory.

```python
# Regular function - returns list (all in memory)
def get_numbers(n):
    result = []
    for i in range(n):
        result.append(i)
    return result

numbers = get_numbers(1000000)  # Uses lots of memory!

# Generator function - yields values one at a time
def get_numbers_generator(n):
    for i in range(n):
        yield i  # yield instead of return

numbers_gen = get_numbers_generator(1000000)  # Uses minimal memory!

# Generator produces values lazily
for num in numbers_gen:
    print(num)  # Values generated one at a time
```

### **6.2 Creating Generators**

```python
# Method 1: Generator function
def countdown(n):
    while n > 0:
        yield n
        n -= 1

for num in countdown(5):
    print(num)  # 5, 4, 3, 2, 1

# Method 2: Generator expression
# Similar to list comprehension but with ()
numbers = (x**2 for x in range(10))  # Generator
numbers_list = [x**2 for x in range(10)]  # List

print(type(numbers))       # <class 'generator'>
print(type(numbers_list))  # <class 'list'>

# Generator is consumed once
gen = (x for x in range(3))
print(list(gen))  # [0, 1, 2]
print(list(gen))  # [] (empty - already consumed!)
```

### **6.3 Generator Methods**

```python
def my_generator():
    yield 1
    yield 2
    yield 3

gen = my_generator()

# next() - get next value
print(next(gen))  # 1
print(next(gen))  # 2
print(next(gen))  # 3
# next(gen)  # StopIteration exception

# send() - send value to generator
def accumulator():
    total = 0
    while True:
        value = yield total
        if value is None:
            break
        total += value

acc = accumulator()
next(acc)  # Initialize generator
print(acc.send(10))  # 10
print(acc.send(20))  # 30
print(acc.send(5))   # 35

# close() - stop generator
gen = my_generator()
print(next(gen))  # 1
gen.close()
# next(gen)  # StopIteration

# throw() - raise exception in generator
def my_gen():
    try:
        yield 1
        yield 2
    except ValueError:
        yield "Caught ValueError"

gen = my_gen()
print(next(gen))              # 1
print(gen.throw(ValueError))  # Caught ValueError
```

### **6.4 Generator Benefits**

```python
# Memory efficiency
import sys

# List - all in memory
numbers_list = [x for x in range(1000000)]
print(f"List size: {sys.getsizeof(numbers_list)} bytes")

# Generator - minimal memory
numbers_gen = (x for x in range(1000000))
print(f"Generator size: {sys.getsizeof(numbers_gen)} bytes")

# List size: 8448728 bytes
# Generator size: 112 bytes

# Infinite sequences
def infinite_sequence():
    num = 0
    while True:
        yield num
        num += 1

# Can iterate forever without running out of memory
gen = infinite_sequence()
for i in gen:
    print(i)
    if i >= 10:
        break

# Pipeline processing
def read_large_file(filepath):
    """Generator to read file line by line"""
    with open(filepath, 'r') as f:
        for line in f:
            yield line.strip()

def filter_non_empty(lines):
    """Generator to filter empty lines"""
    for line in lines:
        if line:
            yield line

def parse_csv_line(lines):
    """Generator to parse CSV lines"""
    for line in lines:
        yield line.split(',')

# Chain generators (memory efficient pipeline)
lines = read_large_file('huge_file.csv')
non_empty = filter_non_empty(lines)
parsed = parse_csv_line(non_empty)

for record in parsed:
    process(record)  # Process one record at a time
```

### **6.5 ETL Generator Examples**

```python
# Read large CSV in chunks
def read_csv_chunks(filepath, chunk_size=10000):
    """Generator to read CSV in chunks"""
    chunk = []
    with open(filepath, 'r') as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader, 1):
            chunk.append(row)
            if i % chunk_size == 0:
                yield chunk
                chunk = []
        
        # Yield remaining records
        if chunk:
            yield chunk

# Usage
for chunk in read_csv_chunks('large_file.csv', chunk_size=10000):
    process_chunk(chunk)  # Process 10,000 records at a time

# Stream data transformation
def transform_records(records):
    """Generator for record transformation"""
    for record in records:
        # Clean data
        record['fund_code'] = record['fund_code'].strip().upper()
        record['aum'] = float(record['aum'])
        
        # Validate
        if record['aum'] > 0:
            yield record

# Process large dataset without loading all into memory
source_data = read_csv_chunks('large_file.csv')
for chunk in source_data:
    transformed = transform_records(chunk)
    load_to_database(list(transformed))

# S3 file streaming
import boto3

def stream_s3_file(bucket, key):
    """Generator to stream S3 file line by line"""
    s3 = boto3.client('s3')
    obj = s3.get_object(Bucket=bucket, Key=key)
    
    for line in obj['Body'].iter_lines():
        yield line.decode('utf-8')

# Usage
for line in stream_s3_file('my-bucket', 'data/large_file.csv'):
    process_line(line)

# Batch generator
def batch_generator(iterable, batch_size):
    """Generator to create batches from iterable"""
    batch = []
    for item in iterable:
        batch.append(item)
        if len(batch) == batch_size:
            yield batch
            batch = []
    
    # Yield remaining items
    if batch:
        yield batch

# Usage
records = range(100)  # Large dataset
for batch in batch_generator(records, batch_size=10):
    insert_batch_to_db(batch)

# Data quality check generator
def validate_records(records):
    """Generator that yields validation results"""
    for i, record in enumerate(records):
        errors = []
        
        if not record.get('fund_code'):
            errors.append("Missing fund_code")
        
        if record.get('aum', 0) < 0:
            errors.append("Negative AUM")
        
        yield {
            "record_number": i + 1,
            "record": record,
            "is_valid": len(errors) == 0,
            "errors": errors
        }

# Usage
for validation_result in validate_records(source_data):
    if not validation_result['is_valid']:
        log_validation_error(validation_result)
```

---

## **7. Iterators** {#iterators}

### **7.1 What are Iterators?**

An iterator is an object that implements:
- `__iter__()` - returns the iterator object
- `__next__()` - returns the next value

```python
# Lists are iterable but not iterators
numbers = [1, 2, 3]

# Get iterator from iterable
iterator = iter(numbers)

# Iterate manually
print(next(iterator))  # 1
print(next(iterator))  # 2
print(next(iterator))  # 3
# next(iterator)  # StopIteration

# for loop uses iterators internally
for num in numbers:
    print(num)

# Equivalent to:
iterator = iter(numbers)
while True:
    try:
        num = next(iterator)
        print(num)
    except StopIteration:
        break
```

### **7.2 Creating Custom Iterators**

```python
class CountDown:
    """Custom iterator for countdown"""
    def __init__(self, start):
        self.start = start
        self.current = start
    
    def __iter__(self):
        """Return the iterator object (self)"""
        return self
    
    def __next__(self):
        """Return next value"""
        if self.current <= 0:
            raise StopIteration
        
        self.current -= 1
        return self.current + 1

# Usage
countdown = CountDown(5)
for num in countdown:
    print(num)  # 5, 4, 3, 2, 1

# Manual iteration
countdown = CountDown(3)
print(next(countdown))  # 3
print(next(countdown))  # 2
print(next(countdown))  # 1
# next(countdown)  # StopIteration
```

### **7.3 Iterator vs Iterable**

```python
# Iterable: Can be converted to iterator
# Examples: list, tuple, dict, set, string, file, generator

# Check if iterable
from collections.abc import Iterable, Iterator

numbers = [1, 2, 3]
print(isinstance(numbers, Iterable))   # True
print(isinstance(numbers, Iterator))   # False

numbers_iter = iter(numbers)
print(isinstance(numbers_iter, Iterator))  # True

# Key difference:
# - Iterable can be iterated over multiple times
# - Iterator can only be consumed once

numbers = [1, 2, 3]  # Iterable
print(list(numbers))  # [1, 2, 3]
print(list(numbers))  # [1, 2, 3] - Works again!

numbers_iter = iter(numbers)  # Iterator
print(list(numbers_iter))  # [1, 2, 3]
print(list(numbers_iter))  # [] - Already consumed!
```

### **7.4 Built-in Iterator Functions**

```python
# itertools module - powerful iterator tools
import itertools

# count() - infinite counter
counter = itertools.count(start=10, step=2)
print(next(counter))  # 10
print(next(counter))  # 12
print(next(counter))  # 14

# cycle() - cycle through iterable infinitely
colors = itertools.cycle(['red', 'green', 'blue'])
print(next(colors))  # red
print(next(colors))  # green
print(next(colors))  # blue
print(next(colors))  # red (cycles back)

# chain() - chain multiple iterables
list1 = [1, 2, 3]
list2 = [4, 5, 6]
chained = itertools.chain(list1, list2)
print(list(chained))  # [1, 2, 3, 4, 5, 6]

# islice() - slice an iterator
numbers = itertools.count()  # Infinite
first_10 = itertools.islice(numbers, 10)
print(list(first_10))  # [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]

# takewhile() - take while condition is true
numbers = [1, 2, 3, 4, 5, 1, 2]
result = itertools.takewhile(lambda x: x < 4, numbers)
print(list(result))  # [1, 2, 3]

# dropwhile() - drop while condition is true
numbers = [1, 2, 3, 4, 5, 1, 2]
result = itertools.dropwhile(lambda x: x < 4, numbers)
print(list(result))  # [4, 5, 1, 2]

# groupby() - group consecutive elements
data = [1, 1, 2, 2, 2, 3, 1, 1]
grouped = itertools.groupby(data)
for key, group in grouped:
    print(f"{key}: {list(group)}")
# 1: [1, 1]
# 2: [2, 2, 2]
# 3: [3]
# 1: [1, 1]

# product() - Cartesian product
colors = ['red', 'blue']
sizes = ['S', 'M', 'L']
combinations = itertools.product(colors, sizes)
print(list(combinations))
# [('red', 'S'), ('red', 'M'), ('red', 'L'),
#  ('blue', 'S'), ('blue', 'M'), ('blue', 'L')]

# combinations() - all combinations
items = ['A', 'B', 'C']
combos = itertools.combinations(items, 2)
print(list(combos))  # [('A', 'B'), ('A', 'C'), ('B', 'C')]

# permutations() - all permutations
items = ['A', 'B', 'C']
perms = itertools.permutations(items, 2)
print(list(perms))
# [('A', 'B'), ('A', 'C'), ('B', 'A'), ('B', 'C'), ('C', 'A'), ('C', 'B')]

# zip() - combine multiple iterables
names = ['Alice', 'Bob', 'Charlie']
ages = [25, 30, 35]
cities = ['NYC', 'LA', 'Chicago']

for name, age, city in zip(names, ages, cities):
    print(f"{name} ({age}) from {city}")

# zip_longest() - doesn't stop at shortest
from itertools import zip_longest

list1 = [1, 2, 3]
list2 = ['a', 'b']
result = zip_longest(list1, list2, fillvalue='N/A')
print(list(result))  # [(1, 'a'), (2, 'b'), (3, 'N/A')]

# enumerate() - add counter
items = ['apple', 'banana', 'cherry']
for index, item in enumerate(items, start=1):
    print(f"{index}. {item}")
# 1. apple
# 2. banana
# 3. cherry
```

### **7.5 ETL Iterator Examples**

```python
# Efficient data processing pipeline
class DataProcessor:
    """Iterator for processing data in pipeline"""
    def __init__(self, filepath):
        self.filepath = filepath
        self.file = None
        self.reader = None
    
    def __iter__(self):
        self.file = open(self.filepath, 'r')
        self.reader = csv.DictReader(self.file)
        return self
    
    def __next__(self):
        try:
            row = next(self.reader)
            # Clean and validate
            row['fund_code'] = row['fund_code'].strip().upper()
            row['aum'] = float(row['aum'])
            
            if row['aum'] <= 0:
                return self.__next__()  # Skip invalid, get next
            
            return row
        except StopIteration:
            self.file.close()
            raise
    
    def __del__(self):
        if self.file:
            self.file.close()

# Usage
for record in DataProcessor('data.csv'):
    load_to_database(record)

# Batch iterator for database operations
class BatchIterator:
    """Iterator that yields batches"""
    def __init__(self, data, batch_size=1000):
        self.data = data
        self.batch_size = batch_size
        self.index = 0
    
    def __iter__(self):
        return self
    
    def __next__(self):
        if self.index >= len(self.data):
            raise StopIteration
        
        batch = self.data[self.index:self.index + self.batch_size]
        self.index += self.batch_size
        
        return batch

# Usage
records = range(10000)
for batch in BatchIterator(records, batch_size=1000):
    insert_batch(batch)  # Insert 1000 records at a time

# Parallel processing with itertools
import concurrent.futures
import itertools

def process_record(record):
    # Processing logic
    return transformed_record

# Process in parallel batches
records = read_large_dataset()
batch_size = 1000

with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
    # Create batches
    batches = itertools.batched(records, batch_size)  # Python 3.12+
    
    # Process batches in parallel
    for results in executor.map(process_batch, batches):
        load_to_database(results)

# Chain multiple data sources
import itertools

def read_source1():
    # Read from source 1
    for record in data1:
        yield record

def read_source2():
    # Read from source 2
    for record in data2:
        yield record

# Combine all sources
all_records = itertools.chain(read_source1(), read_source2())

for record in all_records:
    process(record)
```

---

## **8. Memory Management** {#memory-management}

### **8.1 How Python Manages Memory**

```python
# Reference counting
x = [1, 2, 3]  # Reference count = 1
y = x          # Reference count = 2
z = x          # Reference count = 3

del x          # Reference count = 2
del y          # Reference count = 1
del z          # Reference count = 0 -> object deleted

# Check reference count
import sys

numbers = [1, 2, 3]
print(sys.getrefcount(numbers))  # Actual count + 1 (function argument)

# Memory address
x = 1000
y = 1000
print(id(x))  # Memory address
print(id(y))  # Different address (large integers)

# But small integers are cached
a = 5
b = 5
print(id(a) == id(b))  # True (Python caches -5 to 256)

# Memory size
import sys

numbers = [1, 2, 3, 4, 5]
print(sys.getsizeof(numbers))  # Size in bytes

text = "Hello World"
print(sys.getsizeof(text))
```

### **8.2 Garbage Collection**

```python
import gc

# Garbage collection is automatic
# But you can control it:

# Check if GC is enabled
print(gc.isenabled())  # True

# Disable GC (not recommended)
gc.disable()

# Enable GC
gc.enable()

# Force garbage collection
gc.collect()  # Returns number of objects collected

# Get garbage collection stats
print(gc.get_stats())

# Get threshold values
print(gc.get_threshold())  # (700, 10, 10)

# Set threshold
gc.set_threshold(700, 10, 10)

# Track objects
class MyClass:
    pass

obj = MyClass()
gc.collect()  # Clean up

# Check tracked objects
print(len(gc.get_objects()))
```

### **8.3 Memory-Efficient Practices**

```python
# 1. Use generators instead of lists
# Bad - loads everything in memory
def get_squares(n):
    return [x**2 for x in range(n)]

numbers = get_squares(1000000)  # Uses lots of memory

# Good - generates on the fly
def get_squares_gen(n):
    for x in range(n):
        yield x**2

numbers_gen = get_squares_gen(1000000)  # Minimal memory

# 2. Delete large objects when done
import pandas as pd

df = pd.read_csv('huge_file.csv')
# Process df
result = process(df)

del df  # Free memory
gc.collect()  # Force garbage collection

# 3. Use __slots__ to reduce memory
# Regular class
class RegularPerson:
    def __init__(self, name, age):
        self.name = name
        self.age = age

# Class with __slots__
class OptimizedPerson:
    __slots__ = ['name', 'age']
    
    def __init__(self, name, age):
        self.name = name
        self.age = age

# Memory comparison
regular = RegularPerson("Alice", 30)
optimized = OptimizedPerson("Alice", 30)

print(sys.getsizeof(regular))    # More memory
print(sys.getsizeof(optimized))  # Less memory

# 4. Use itertools for large datasets
import itertools

# Instead of creating large lists
big_list = list(range(1000000))

# Use itertools
big_iter = itertools.count(0, 1)
first_million = itertools.islice(big_iter, 1000000)

# 5. Context managers for automatic cleanup
class DatabaseConnection:
    def __enter__(self):
        self.conn = connect_to_db()
        return self.conn
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.conn.close()  # Always closes, even on exception
        return False

with DatabaseConnection() as conn:
    conn.execute("SELECT * FROM table")
# Connection automatically closed

# 6. Avoid circular references
class Node:
    def __init__(self, value):
        self.value = value
        self.next = None

# Circular reference
node1 = Node(1)
node2 = Node(2)
node1.next = node2
node2.next = node1  # Circular!

# Break the cycle
node1.next = None
node2.next = None
del node1
del node2

# 7. Use weakref for cache
import weakref

class DataCache:
    def __init__(self):
        self.cache = weakref.WeakValueDictionary()
    
    def get(self, key):
        return self.cache.get(key)
    
    def set(self, key, value):
        self.cache[key] = value

# Objects can be garbage collected even if in cache
cache = DataCache()
data = [1, 2, 3]
cache.set('key', data)

del data  # Object can be collected
gc.collect()

print(cache.get('key'))  # May be None if collected
```

### **8.4 Memory Profiling**

```python
# memory_profiler package
# Install: pip install memory-profiler

from memory_profiler import profile

@profile
def my_function():
    a = [1] * (10 ** 6)
    b = [2] * (2 * 10 ** 7)
    del b
    return a

# Run with: python -m memory_profiler script.py

# tracemalloc (built-in)
import tracemalloc

tracemalloc.start()

# Your code here
data = [i for i in range(1000000)]

current, peak = tracemalloc.get_traced_memory()
print(f"Current memory usage: {current / 10**6:.2f} MB")
print(f"Peak memory usage: {peak / 10**6:.2f} MB")

tracemalloc.stop()

# objgraph for visualization
# Install: pip install objgraph
import objgraph

# Show most common types
objgraph.show_most_common_types()

# Track object growth
objgraph.show_growth()
```

### **8.5 ETL Memory Management Examples**

```python
# Process large CSV without loading into memory
def process_large_csv_efficiently(filepath):
    """Process large CSV file with minimal memory"""
    with open(filepath, 'r') as f:
        reader = csv.DictReader(f)
        
        batch = []
        batch_size = 10000
        
        for row in reader:
            # Transform row
            transformed = transform_row(row)
            batch.append(transformed)
            
            if len(batch) >= batch_size:
                # Process batch and free memory
                load_batch_to_db(batch)
                batch = []  # Clear batch
                gc.collect()  # Optional: force GC
        
        # Process remaining
        if batch:
            load_batch_to_db(batch)

# PySpark memory management
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("ETL Job") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "8g") \
    .config("spark.sql.shuffle.partitions", "200") \
    .getOrCreate()

# Cache strategically
df_dimension = spark.read.parquet("s3://bucket/dimension/")
df_dimension.cache()  # Keep in memory if used multiple times

# Use df multiple times
result1 = df_fact.join(df_dimension, "key")
result2 = another_fact.join(df_dimension, "key")

# Unpersist when done
df_dimension.unpersist()

# Repartition to control memory usage
large_df = spark.read.csv("huge_file.csv")

# Too many partitions = too much overhead
# Too few partitions = OOM errors

# Repartition based on data size
optimal_partitions = max(large_df.count() // 1000000, 1)
large_df = large_df.repartition(optimal_partitions)

# Memory-efficient aggregation
# Instead of:
df_grouped = df.groupBy("fund_code").agg(sum("aum"))  # May OOM

# Use window functions for partial aggregates
from pyspark.sql.window import Window

window_spec = Window.partitionBy("fund_code")
df_with_running_total = df.withColumn(
    "running_total",
    F.sum("aum").over(window_spec)
)

# Broadcast small dimensions
from pyspark.sql.functions import broadcast

small_dim = spark.read.parquet("s3://bucket/small_dim/")
large_fact = spark.read.parquet("s3://bucket/large_fact/")

# Broadcast join (efficient for small table)
result = large_fact.join(broadcast(small_dim), "key")
```

---

## **9. Multithreading vs Multiprocessing** {#multithreading-vs-multiprocessing}

### **9.1 Understanding the Difference**

**Threading:**
- Multiple threads in single process
- Shared memory space
- Good for I/O-bound tasks (file reading, network calls)
- Limited by GIL (Global Interpreter Lock) for CPU-bound tasks

**Multiprocessing:**
- Multiple processes
- Separate memory space
- Good for CPU-bound tasks (computations, data processing)
- No GIL limitation

```python
# Visual representation:

# Threading: Single process, multiple threads
# Process 1
#   ├── Thread 1 (main)
#   ├── Thread 2
#   └── Thread 3
# All share same memory

# Multiprocessing: Multiple processes
# Process 1 (main)
# Process 2 (worker)
# Process 3 (worker)
# Each has its own memory
```

### **9.2 Multithreading**

```python
import threading
import time

# Basic thread creation
def print_numbers():
    for i in range(5):
        print(f"Number: {i}")
        time.sleep(0.5)

# Create and start thread
thread = threading.Thread(target=print_numbers)
thread.start()
thread.join()  # Wait for thread to complete

# Thread with arguments
def print_message(message, count):
    for i in range(count):
        print(f"{message} - {i}")
        time.sleep(0.3)

thread = threading.Thread(
    target=print_message,
    args=("Hello", 3)
)
thread.start()
thread.join()

# Multiple threads
threads = []
for i in range(3):
    thread = threading.Thread(
        target=print_message,
        args=(f"Thread-{i}", 3)
    )
    threads.append(thread)
    thread.start()

# Wait for all threads
for thread in threads:
    thread.join()

# Thread with return value
from concurrent.futures import ThreadPoolExecutor

def process_data(data):
    time.sleep(1)
    return data * 2

with ThreadPoolExecutor(max_workers=4) as executor:
    # Submit tasks
    future1 = executor.submit(process_data, 5)
    future2 = executor.submit(process_data, 10)
    
    # Get results
    print(future1.result())  # 10
    print(future2.result())  # 20

# Map function (like map() but parallel)
with ThreadPoolExecutor(max_workers=4) as executor:
    data = [1, 2, 3, 4, 5]
    results = executor.map(process_data, data)
    print(list(results))  # [2, 4, 6, 8, 10]

# Thread safety - Lock
balance = 0
lock = threading.Lock()

def deposit(amount):
    global balance
    with lock:  # Acquire lock
        temp = balance
        time.sleep(0.001)  # Simulate processing
        balance = temp + amount
    # Lock automatically released

threads = []
for i in range(10):
    thread = threading.Thread(target=deposit, args=(100,))
    threads.append(thread)
    thread.start()

for thread in threads:
    thread.join()

print(f"Final balance: {balance}")  # Should be 1000
```

### **9.3 Multiprocessing**

```python
import multiprocessing
import time

# Basic process creation
def worker(name):
    print(f"Worker {name} starting")
    time.sleep(2)
    print(f"Worker {name} finished")

# Create and start process
process = multiprocessing.Process(target=worker, args=("A",))
process.start()
process.join()  # Wait for completion

# Multiple processes
processes = []
for i in range(3):
    process = multiprocessing.Process(
        target=worker,
        args=(f"Worker-{i}",)
    )
    processes.append(process)
    process.start()

# Wait for all
for process in processes:
    process.join()

# Process Pool
from multiprocessing import Pool

def square(x):
    return x ** 2

# Create pool of workers
with Pool(processes=4) as pool:
    numbers = [1, 2, 3, 4, 5, 6, 7, 8]
    results = pool.map(square, numbers)
    print(results)  # [1, 4, 9, 16, 25, 36, 49, 64]

# Async processing
with Pool(processes=4) as pool:
    # Submit tasks asynchronously
    result1 = pool.apply_async(square, (5,))
    result2 = pool.apply_async(square, (10,))
    
    # Get results
    print(result1.get())  # 25
    print(result2.get())  # 100

# Share data between processes - Queue
from multiprocessing import Queue, Process

def producer(queue):
    for i in range(5):
        queue.put(i)
        print(f"Produced: {i}")

def consumer(queue):
    while True:
        item = queue.get()
        if item is None:
            break
        print(f"Consumed: {item}")

queue = Queue()

p1 = Process(target=producer, args=(queue,))
p2 = Process(target=consumer, args=(queue,))

p1.start()
p2.start()

p1.join()
queue.put(None)  # Signal consumer to stop
p2.join()

# Share data - Manager
from multiprocessing import Manager

def worker(shared_dict, key, value):
    shared_dict[key] = value

manager = Manager()
shared_dict = manager.dict()

processes = []
for i in range(5):
    p = Process(target=worker, args=(shared_dict, f"key_{i}", i * 10))
    processes.append(p)
    p.start()

for p in processes:
    p.join()

print(dict(shared_dict))  # {'key_0': 0, 'key_1': 10, ...}
```

### **9.4 Comparison: Threading vs Multiprocessing**

```python
import time
import threading
from multiprocessing import Pool

# CPU-bound task
def cpu_bound_task(n):
    """Calculate sum of squares"""
    return sum(i * i for i in range(n))

# I/O-bound task
def io_bound_task(url):
    """Simulate network request"""
    time.sleep(1)  # Simulate I/O delay
    return f"Downloaded {url}"

# Benchmark CPU-bound: Threading
def benchmark_threading_cpu():
    start = time.time()
    
    with ThreadPoolExecutor(max_workers=4) as executor:
        results = list(executor.map(cpu_bound_task, [10**7] * 4))
    
    end = time.time()
    print(f"Threading (CPU-bound): {end - start:.2f}s")

# Benchmark CPU-bound: Multiprocessing
def benchmark_multiprocessing_cpu():
    start = time.time()
    
    with Pool(processes=4) as pool:
        results = pool.map(cpu_bound_task, [10**7] * 4)
    
    end = time.time()
    print(f"Multiprocessing (CPU-bound): {end - start:.2f}s")

# Benchmark I/O-bound: Threading
def benchmark_threading_io():
    start = time.time()
    
    urls = [f"http://example.com/{i}" for i in range(10)]
    with ThreadPoolExecutor(max_workers=10) as executor:
        results = list(executor.map(io_bound_task, urls))
    
    end = time.time()
    print(f"Threading (I/O-bound): {end - start:.2f}s")

# Benchmark I/O-bound: Multiprocessing
def benchmark_multiprocessing_io():
    start = time.time()
    
    urls = [f"http://example.com/{i}" for i in range(10)]
    with Pool(processes=10) as pool:
        results = pool.map(io_bound_task, urls)
    
    end = time.time()
    print(f"Multiprocessing (I/O-bound): {end - start:.2f}s")

# Results (approximate):
# Threading (CPU-bound): 8.5s (limited by GIL)
# Multiprocessing (CPU-bound): 2.5s (true parallelism)
# Threading (I/O-bound): 1.0s (efficient for I/O)
# Multiprocessing (I/O-bound): 1.2s (overhead from process creation)
```

### **9.5 ETL Use Cases**

```python
# Use Case 1: Parallel file downloads (I/O-bound - use Threading)
from concurrent.futures import ThreadPoolExecutor
import boto3

def download_s3_file(s3_path):
    """Download file from S3"""
    s3 = boto3.client('s3')
    bucket, key = parse_s3_path(s3_path)
    
    local_path = f"/tmp/{key.split('/')[-1]}"
    s3.download_file(bucket, key, local_path)
    return local_path

# Parallel downloads
s3_files = [
    "s3://bucket/data1.csv",
    "s3://bucket/data2.csv",
    "s3://bucket/data3.csv",
    "s3://bucket/data4.csv"
]

with ThreadPoolExecutor(max_workers=4) as executor:
    local_files = list(executor.map(download_s3_file, s3_files))

# Use Case 2: Parallel data transformation (CPU-bound - use Multiprocessing)
from multiprocessing import Pool

def transform_chunk(chunk):
    """Heavy transformation on data chunk"""
    # Complex calculations
    result = []
    for record in chunk:
        # CPU-intensive operations
        transformed = complex_calculation(record)
        result.append(transformed)
    return result

# Split data into chunks
data = read_large_dataset()
chunk_size = len(data) // 4
chunks = [data[i:i+chunk_size] for i in range(0, len(data), chunk_size)]

# Process in parallel
with Pool(processes=4) as pool:
    results = pool.map(transform_chunk, chunks)

# Combine results
final_data = []
for result in results:
    final_data.extend(result)

# Use Case 3: Parallel database operations (I/O-bound - use Threading)
from concurrent.futures import ThreadPoolExecutor
import psycopg2

def insert_batch(batch, config):
    """Insert batch of records"""
    conn = psycopg2.connect(**config)
    cursor = conn.cursor()
    
    for record in batch:
        cursor.execute("INSERT INTO table VALUES (%s, %s)", record)
    
    conn.commit()
    conn.close()

# Split records into batches
records = generate_records(100000)
batch_size = 10000
batches = [records[i:i+batch_size] for i in range(0, len(records), batch_size)]

# Insert in parallel
with ThreadPoolExecutor(max_workers=5) as executor:
    futures = [executor.submit(insert_batch, batch, db_config) for batch in batches]
    
    for future in futures:
        future.result()  # Wait for completion

# Use Case 4: Hybrid approach (both)
def process_file_parallel(filepath):
    """
    1. Download file (I/O-bound - threading)
    2. Transform data (CPU-bound - multiprocessing)
    3. Upload result (I/O-bound - threading)
    """
    
    # Step 1: Download (threading)
    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(download_file, filepath)
        local_file = future.result()
    
    # Step 2: Transform (multiprocessing)
    data = read_file(local_file)
    chunks = split_into_chunks(data, num_chunks=4)
    
    with Pool(processes=4) as pool:
        transformed_chunks = pool.map(transform_data, chunks)
    
    transformed_data = merge_chunks(transformed_chunks)
    
    # Step 3: Upload (threading)
    output_file = write_file(transformed_data)
    
    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(upload_file, output_file, "s3://bucket/output/")
        result = future.result()
    
    return result
```

### **9.6 Best Practices**

```python
# 1. Choose based on task type
"""
I/O-bound (network, file I/O, database):
- Use Threading (ThreadPoolExecutor)
- Example: API calls, file downloads, database queries

CPU-bound (calculations, data processing):
- Use Multiprocessing (Pool)
- Example: data transformations, computations, encoding

Mixed workload:
- Use both strategically
"""

# 2. Limit number of workers
import os

# Don't create too many workers
cpu_count = os.cpu_count()

# For CPU-bound: match CPU cores
with Pool(processes=cpu_count) as pool:
    results = pool.map(cpu_task, data)

# For I/O-bound: can exceed CPU count
with ThreadPoolExecutor(max_workers=cpu_count * 2) as executor:
    results = executor.map(io_task, data)

# 3. Handle exceptions properly
from concurrent.futures import ThreadPoolExecutor, as_completed

def task_with_error_handling(data):
    try:
        return process(data)
    except Exception as e:
        return {"error": str(e), "data": data}

with ThreadPoolExecutor(max_workers=4) as executor:
    futures = [executor.submit(task_with_error_handling, item) for item in data]
    
    for future in as_completed(futures):
        result = future.result()
        if "error" in result:
            log_error(result)
        else:
            process_result(result)

# 4. Use context managers
# Good - automatic cleanup
with ThreadPoolExecutor(max_workers=4) as executor:
    results = executor.map(process, data)

# Bad - must manually shutdown
executor = ThreadPoolExecutor(max_workers=4)
results = executor.map(process, data)
executor.shutdown()  # Must remember this!

# 5. Monitor resource usage
import psutil

def monitor_resources():
    cpu_percent = psutil.cpu_percent(interval=1)
    memory = psutil.virtual_memory()
    
    print(f"CPU Usage: {cpu_percent}%")
    print(f"Memory Usage: {memory.percent}%")

# Monitor while processing
monitor_resources()
```

---

## **Summary: Key Takeaways**

### **Data Structures**
- **Lists**: Ordered, mutable, for sequences
- **Tuples**: Immutable, for fixed data
- **Sets**: No duplicates, for membership testing
- **Dicts**: Key-value pairs, for lookups
- **NamedTuples**: Self-documenting, immutable data

### **File & Exception Handling**
- Always use context managers (`with`)
- Handle specific exceptions first
- Use custom exceptions for clarity
- Implement retry logic for resilience

### **Functional Programming**
- **Lambda**: Quick, anonymous functions
- **Decorators**: Add behavior to functions
- **Generators**: Memory-efficient iteration
- **Iterators**: Custom iteration logic

### **Performance**
- **Memory**: Use generators, delete large objects
- **Threading**: I/O-bound tasks (files, network)
- **Multiprocessing**: CPU-bound tasks (calculations)

---

**These concepts form the foundation of efficient, production-ready Python code for data engineering!** 🚀