# **Object-Oriented Programming (OOP) Concepts in Your Glue Scripts**

Great question! Let me break down all the OOP concepts used in your scripts with clear explanations and examples.

---

## **1. Classes and Objects**

### **What is a Class?**
A class is a blueprint for creating objects. Think of it as a template or a cookie cutter.

### **Example from Your Script:**
```python
class LogBuffer:
    """This is a class - a blueprint for creating log objects"""
    
    def __init__(self, run_id: str):
        """Constructor - runs when you create a new object"""
        self.lines = []           # Instance variable
        self.run_id = run_id      # Instance variable
```

### **Creating Objects (Instances):**
```python
# Creating an object from the class
log = LogBuffer("abc123")  # 'log' is an object/instance of LogBuffer

# Each object has its own data
log1 = LogBuffer("run_001")
log2 = LogBuffer("run_002")

# They are independent
log1.lines.append("First log")
log2.lines.append("Second log")

print(log1.lines)  # ['First log']
print(log2.lines)  # ['Second log']
```

---

## **2. Encapsulation**

### **What is Encapsulation?**
Bundling data (variables) and methods (functions) that operate on that data within a single unit (class). It also means hiding internal details and providing a clean interface.

### **Example from Your Script:**
```python
class LogBuffer:
    def __init__(self, run_id: str):
        self.lines = []          # Data (state)
        self.run_id = run_id     # Data (state)
    
    def info(self, msg: str, **kv):
        """Method that operates on the data"""
        payload = {
            "level": "INFO",
            "ts": self._ts(),
            "run_id": self.run_id,  # Using encapsulated data
            "msg": msg
        }
        if kv:
            payload.update(kv)
        self.lines.append(json.dumps(payload))  # Modifying encapsulated data
        print(json.dumps(payload))
```

### **Benefits:**
```python
# Users don't need to know HOW logs are stored
log = LogBuffer("run_001")
log.info("Starting job")  # Clean interface

# Instead of manually doing:
# lines = []
# lines.append(json.dumps({"level": "INFO", ...}))  # Messy!
```

---

## **3. The `self` Keyword**

### **What is `self`?**
`self` refers to the current instance of the class. It's how methods access the object's own data.

### **Understanding `self`:**
```python
class LogBuffer:
    def __init__(self, run_id: str):
        self.run_id = run_id      # Setting THIS object's run_id
        self.lines = []           # Setting THIS object's lines
    
    def info(self, msg: str):
        # self.run_id means "THIS object's run_id"
        payload = {"run_id": self.run_id, "msg": msg}
        self.lines.append(json.dumps(payload))

# When you call:
log = LogBuffer("abc123")
log.info("Hello")  # Python automatically passes 'log' as 'self'

# It's like Python is doing:
# LogBuffer.info(log, "Hello")  # 'log' becomes 'self' inside the method
```

---

## **4. Constructor (`__init__`)**

### **What is a Constructor?**
A special method that runs automatically when you create a new object. Used to initialize the object's state.

### **Example:**
```python
class LogBuffer:
    def __init__(self, run_id: str):
        """Constructor - sets up the initial state"""
        print(f"Creating new LogBuffer with run_id: {run_id}")
        self.lines = []           # Initialize empty list
        self.run_id = run_id      # Store the run_id

# When you create an object:
log = LogBuffer("run_001")
# Output: Creating new LogBuffer with run_id: run_001
```

### **Real-World Analogy:**
```python
class BankAccount:
    def __init__(self, account_number, initial_balance=0):
        self.account_number = account_number
        self.balance = initial_balance
        self.transactions = []
        print(f"Account {account_number} created with ${initial_balance}")

# Creating accounts
checking = BankAccount("123456", 1000)
savings = BankAccount("789012", 5000)
```

---

## **5. Instance Methods vs Static Methods**

### **Instance Methods**
Methods that operate on instance data (use `self`).

```python
class LogBuffer:
    def __init__(self, run_id: str):
        self.run_id = run_id
        self.lines = []
    
    def info(self, msg: str):
        """Instance method - uses self"""
        payload = {"run_id": self.run_id, "msg": msg}
        self.lines.append(json.dumps(payload))
        
    def get_log_count(self):
        """Instance method - returns instance data"""
        return len(self.lines)

# Usage:
log = LogBuffer("run_001")
log.info("Test message")
print(log.get_log_count())  # 1
```

### **Static Methods**
Methods that don't need instance data (no `self`). They're utility functions.

```python
class LogBuffer:
    @staticmethod
    def _ts() -> str:
        """Static method - doesn't use self"""
        return datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
    
    def info(self, msg: str):
        # Calling static method
        timestamp = self._ts()  # or LogBuffer._ts()
        payload = {"ts": timestamp, "msg": msg}
        self.lines.append(json.dumps(payload))

# Can call without an instance:
timestamp = LogBuffer._ts()
print(timestamp)  # Works without creating an object!
```

### **When to Use Each:**
```python
class MathHelper:
    def __init__(self, name):
        self.name = name
        self.calculations = []
    
    # Instance method - needs object data
    def add_calculation(self, result):
        self.calculations.append(result)
        print(f"{self.name} added calculation: {result}")
    
    # Static method - pure utility, no object data needed
    @staticmethod
    def calculate_area(length, width):
        return length * width

# Usage:
helper = MathHelper("Calculator_1")
area = MathHelper.calculate_area(5, 10)  # Static - no object needed
helper.add_calculation(area)              # Instance - needs object
```

---

## **6. Decorators**

### **What are Decorators?**
Functions that modify or enhance other functions. They "wrap" a function with additional behavior.

### **`@staticmethod` Decorator:**
```python
class LogBuffer:
    # Without @staticmethod - would need self
    def timestamp_bad(self):
        return datetime.now().strftime('%Y-%m-%d')
    
    # With @staticmethod - no self needed
    @staticmethod
    def timestamp_good():
        return datetime.now().strftime('%Y-%m-%d')

# Usage:
log = LogBuffer("run_001")
# log.timestamp_bad()   # Must create object first
LogBuffer.timestamp_good()  # Can use directly!
```

### **`@functools.wraps` Decorator:**
```python
import functools

def retry_on_exception(max_attempts=3):
    def decorator(func):
        @functools.wraps(func)  # Preserves function metadata
        def wrapper(*args, **kwargs):
            attempt = 0
            while attempt < max_attempts:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    attempt += 1
                    if attempt >= max_attempts:
                        raise
                    print(f"Retry {attempt}/{max_attempts}")
        return wrapper
    return decorator

# Usage:
@retry_on_exception(max_attempts=3)
def connect_to_database():
    print("Connecting...")
    # connection logic here

# The decorator "wraps" the function with retry logic
connect_to_database()  # Will retry 3 times if it fails
```

---

## **7. Method Chaining**

### **What is Method Chaining?**
Calling multiple methods in sequence on the same object.

### **Example:**
```python
# In your script:
df = df \
    .withColumn("run_date", lit(run_ts)) \
    .withColumn("file_name", lit(file_name)) \
    .withColumn("year", lit(2025))

# This works because each method returns the DataFrame object,
# allowing you to call the next method on it.
```

### **How to Implement:**
```python
class QueryBuilder:
    def __init__(self):
        self.conditions = []
        self.columns = []
    
    def select(self, *cols):
        self.columns.extend(cols)
        return self  # Return self to enable chaining
    
    def where(self, condition):
        self.conditions.append(condition)
        return self  # Return self to enable chaining
    
    def build(self):
        cols = ", ".join(self.columns)
        conds = " AND ".join(self.conditions)
        return f"SELECT {cols} FROM table WHERE {conds}"

# Usage with chaining:
query = QueryBuilder() \
    .select("name", "age", "city") \
    .where("age > 18") \
    .where("city = 'NYC'") \
    .build()

print(query)
# SELECT name, age, city FROM table WHERE age > 18 AND city = 'NYC'
```

---

## **8. Inheritance (Bonus - Not in Your Script)**

### **What is Inheritance?**
Creating new classes based on existing classes, inheriting their properties and methods.

### **Example:**
```python
class Logger:
    """Base class"""
    def __init__(self, name):
        self.name = name
    
    def log(self, msg):
        print(f"[{self.name}] {msg}")

class FileLogger(Logger):
    """Child class - inherits from Logger"""
    def __init__(self, name, filepath):
        super().__init__(name)  # Call parent constructor
        self.filepath = filepath
    
    def log(self, msg):
        """Override parent method"""
        super().log(msg)  # Call parent's log
        with open(self.filepath, 'a') as f:
            f.write(f"{msg}\n")

# Usage:
console_log = Logger("Console")
console_log.log("Hello")  # [Console] Hello

file_log = FileLogger("File", "app.log")
file_log.log("Hello")     # [File] Hello + writes to file
```

---

## **Complete Example: OOP in Your Glue Script**

Let me create a comprehensive example showing all OOP concepts together:

```python
import json
from datetime import datetime, timezone
import boto3

class ETLLogger:
    """
    Complete OOP example with all concepts
    """
    
    # Class variable (shared by all instances)
    log_format = "JSON"
    
    def __init__(self, run_id: str, job_name: str):
        """Constructor - initializes instance variables"""
        # Instance variables (unique to each object)
        self.run_id = run_id
        self.job_name = job_name
        self.lines = []
        self.start_time = datetime.now(timezone.utc)
    
    @staticmethod
    def _get_timestamp() -> str:
        """Static method - utility function, no self needed"""
        return datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
    
    @staticmethod
    def _format_size(bytes_size: int) -> str:
        """Static method - another utility"""
        for unit in ['B', 'KB', 'MB', 'GB']:
            if bytes_size < 1024.0:
                return f"{bytes_size:.2f} {unit}"
            bytes_size /= 1024.0
        return f"{bytes_size:.2f} TB"
    
    def _create_payload(self, level: str, msg: str, **kwargs) -> dict:
        """Private instance method - used internally"""
        payload = {
            "level": level,
            "timestamp": self._get_timestamp(),
            "run_id": self.run_id,
            "job_name": self.job_name,
            "message": msg
        }
        if kwargs:
            payload.update(kwargs)
        return payload
    
    def info(self, msg: str, **kwargs):
        """Public instance method - encapsulates logging logic"""
        payload = self._create_payload("INFO", msg, **kwargs)
        self.lines.append(json.dumps(payload))
        print(json.dumps(payload))
        return self  # Enable method chaining
    
    def warning(self, msg: str, **kwargs):
        """Public instance method"""
        payload = self._create_payload("WARNING", msg, **kwargs)
        self.lines.append(json.dumps(payload))
        print(json.dumps(payload))
        return self  # Enable method chaining
    
    def error(self, msg: str, **kwargs):
        """Public instance method"""
        payload = self._create_payload("ERROR", msg, **kwargs)
        self.lines.append(json.dumps(payload))
        print(json.dumps(payload))
        return self  # Enable method chaining
    
    def get_stats(self) -> dict:
        """Public instance method - returns statistics"""
        duration = (datetime.now(timezone.utc) - self.start_time).total_seconds()
        return {
            "run_id": self.run_id,
            "job_name": self.job_name,
            "total_logs": len(self.lines),
            "duration_seconds": duration
        }
    
    def export_to_s3(self, bucket: str, key: str) -> str:
        """Public instance method - exports logs"""
        s3 = boto3.client('s3')
        body = "\n".join(self.lines).encode('utf-8')
        s3.put_object(Bucket=bucket, Key=key, Body=body)
        
        size = len(body)
        self.info(f"Logs exported to S3", 
                  bucket=bucket, 
                  key=key, 
                  size=self._format_size(size))
        
        return f"s3://{bucket}/{key}"
    
    def __str__(self):
        """Magic method - defines string representation"""
        return f"ETLLogger(run_id={self.run_id}, logs={len(self.lines)})"
    
    def __len__(self):
        """Magic method - defines length"""
        return len(self.lines)


# ============================================================================
# USAGE EXAMPLES
# ============================================================================

# 1. Creating objects (Instantiation)
log1 = ETLLogger("run_001", "dimension_loader")
log2 = ETLLogger("run_002", "fact_loader")

# 2. Using instance methods (Encapsulation)
log1.info("Starting ETL job")
log1.info("Reading source file", records=1000)
log1.warning("Found duplicate records", count=5)

# 3. Method chaining
log1.info("Step 1 complete") \
    .info("Step 2 complete") \
    .info("Step 3 complete")

# 4. Using static methods (no object needed)
timestamp = ETLLogger._get_timestamp()
print(f"Current time: {timestamp}")

size_str = ETLLogger._format_size(1024 * 1024 * 500)  # 500 MB
print(f"File size: {size_str}")

# 5. Each object maintains its own state
print(f"Log1 has {len(log1)} entries")  # Uses __len__
print(f"Log2 has {len(log2)} entries")

# 6. Getting statistics
stats = log1.get_stats()
print(f"Job stats: {stats}")

# 7. String representation
print(log1)  # Uses __str__
# Output: ETLLogger(run_id=run_001, logs=5)

# 8. Exporting logs
s3_path = log1.export_to_s3("my-bucket", "logs/run_001.txt")
print(f"Logs saved to: {s3_path}")
```

---

## **Key OOP Principles Summary:**

| **Concept** | **Purpose** | **Example in Your Script** |
|------------|-------------|---------------------------|
| **Class** | Blueprint for objects | `class LogBuffer` |
| **Object/Instance** | Actual entity created from class | `log = LogBuffer("run_001")` |
| **Encapsulation** | Bundle data + methods together | `self.lines` and `self.info()` |
| **Constructor** | Initialize object state | `__init__(self, run_id)` |
| **`self`** | Reference to current object | `self.lines`, `self.run_id` |
| **Instance Method** | Operates on object data | `def info(self, msg)` |
| **Static Method** | Utility, no object data needed | `@staticmethod def _ts()` |
| **Method Chaining** | Call methods in sequence | `df.withColumn().withColumn()` |
| **Decorator** | Modify function behavior | `@retry_on_exception` |

---

## **Why Use OOP in Your Glue Scripts?**

### **1. Organization**
```python
# Without OOP - messy
lines = []
run_id = "abc123"

def log_info(msg):
    lines.append(json.dumps({"level": "INFO", "msg": msg}))

# With OOP - clean
log = LogBuffer("abc123")
log.info("Message")
```

### **2. Reusability**
```python
# Create multiple independent loggers
dim_log = LogBuffer("dim_job_001")
fact_log = LogBuffer("fact_job_001")

dim_log.info("Processing dimensions")
fact_log.info("Processing facts")
```

### **3. Maintainability**
```python
# Easy to add features to the class
class LogBuffer:
    def info(self, msg):
        # If we need to change log format, change it once here
        payload = {"level": "INFO", "msg": msg}
        self.lines.append(json.dumps(payload))
```

---

**The OOP approach makes your code more organized, reusable, and easier to maintain - perfect for complex ETL workflows!** 🎯