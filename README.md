# 🚀 ParallelDB: Lightweight Parallel Database 

A high-performance, in-memory parallel database system designed specifically for Google Colab notebooks. Built for data science workflows with parallel processing capabilities.


## ✨ Features

### 🏗️ **Parallel Architecture**
- **Multi-partition sharding** for distributed data storage
- **Configurable parallelism** (threads or processes)
- **Consistent hashing** for balanced data distribution
- **Automatic load balancing** across partitions

### ⚡ **High Performance**
- **Parallel CRUD operations** with bulk processing
- **In-memory storage** for lightning-fast queries
- **LRU query caching** with configurable cache size
- **Indexed queries** for O(1) lookups on indexed fields

### 🔍 **Advanced Querying**
- **Flexible filter system** with QueryBuilder helper class
- **Parallel map-reduce** operations
- **Complex query composition** (AND/OR conditions)
- **Range queries, equality checks, and substring searches**

### 🔄 **Data Science Integration**
- **Pandas DataFrame** interoperability
- **Auto-save/load** functionality for Colab sessions
- **Built-in benchmarking** tools
- **Real-time statistics** and monitoring

### 🛡️ **Robust & Reliable**
- **Thread-safe operations** with proper locking
- **Data persistence** to disk
- **Configurable sharding** keys
- **Graceful shutdown** and resource cleanup

## 📋 Installation

```python
# In Google Colab, simply copy the database code into a cell and run it
# Or upload the colab_parallel_db.py file and import it
```

## 🚀 Quick Start

### 1. Basic Setup

```python
from colab_parallel_db import ParallelDatabase, DatabaseConfig, QueryBuilder

# Create database with default settings (optimized for Colab)
db = ParallelDatabase()

# Or customize configuration
config = DatabaseConfig(
    num_partitions=4,      # Number of parallel partitions
    max_workers=8,         # Max worker threads/processes
    use_threads=True,      # Use threads (lighter) instead of processes
    cache_size=1000,       # Max cached queries
    persist_file="colab_db_backup.pkl"  # Auto-save file
)
db = ParallelDatabase(config)
```

### 2. Create Tables

```python
# Create tables with optional sharding keys
db.create_table("users", shard_key="id")        # Shard by user ID
db.create_table("products", shard_key="category")  # Shard by category
db.create_table("orders")                       # No sharding (default)
```

### 3. Insert Data

```python
# Single insert
db.insert("users", {"id": 1, "name": "Alice", "age": 30, "city": "NY"})

# Bulk insert (parallel execution)
users = [
    {"id": i, "name": f"User{i}", "age": 20 + (i % 30), "city": ["NY", "LA", "CHI"][i % 3]}
    for i in range(1000)
]
db.bulk_insert("users", users)
```

### 4. Query Data

```python
# Basic query
all_users = db.query("users")

# Filtered query
young_users = db.query("users", QueryBuilder.less_than("age", 25))
ny_users = db.query("users", QueryBuilder.equals("city", "NY"))

# Complex queries
query = QueryBuilder.and_(
    QueryBuilder.greater_than("age", 30),
    QueryBuilder.equals("city", "LA"),
    QueryBuilder.contains("name", "Admin")
)
results = db.query("users", query)
```

### 5. Create Indexes

```python
# Create indexes for faster queries
db.create_index("users", "age_index", "age")
db.create_index("users", "city_index", "city")

# Query using index (faster)
ny_users_fast = db.indexed_query("users", "city_index", "NY")
```

### 6. Map-Reduce Operations

```python
# Calculate average age
def extract_age(record):
    return record.get("age", 0)

def calculate_average(ages):
    return sum(ages) / len(ages) if ages else 0

avg_age = db.parallel_map("users", extract_age, calculate_average)
print(f"Average age: {avg_age:.2f}")
```

### 7. Pandas Integration

```python
# Convert to DataFrame
df_users = db.to_dataframe("users")
print(df_users.head())
print(f"DataFrame shape: {df_users.shape}")

# Load from DataFrame
import pandas as pd
new_data = pd.DataFrame([
    {"id": 1001, "name": "Bob", "age": 25},
    {"id": 1002, "name": "Charlie", "age": 35}
])
db.from_dataframe("users", new_data)
```

### 8. Persistence

```python
# Save database to disk
db.save("my_database.pkl")

# Load database from disk (in next Colab session)
db.load("my_database.pkl")
```

### 9. Monitor Database

```python
# Get statistics
stats = db.stats()
print(f"Total partitions: {stats['num_partitions']}")
for table, table_stats in stats['tables'].items():
    print(f"{table}: {table_stats['record_count']} records")

# Clear cache
db.clear_cache()
```

### 10. Clean Shutdown

```python
# Properly close database
db.close()
```

## 📊 Query Builder Reference

```python
# Equality
QueryBuilder.equals("field", value)

# Comparisons
QueryBuilder.greater_than("field", value)
QueryBuilder.less_than("field", value)
QueryBuilder.greater_equal("field", value)
QueryBuilder.less_equal("field", value)

# Set operations
QueryBuilder.in_list("field", [value1, value2, value3])

# String operations
QueryBuilder.contains("field", "substring")
QueryBuilder.starts_with("field", "prefix")
QueryBuilder.ends_with("field", "suffix")

# Logical operations
QueryBuilder.and_(condition1, condition2, condition3)
QueryBuilder.or_(condition1, condition2, condition3)
QueryBuilder.not_(condition)

# Custom conditions
QueryBuilder.custom(lambda record: record["age"] > 25 and "NY" in record["city"])
```

## 🎯 Performance Tuning

### For Small Datasets (<10K records)
```python
config = DatabaseConfig(
    num_partitions=2,
    max_workers=4,
    use_threads=True,
    cache_size=500
)
```

### For Medium Datasets (10K-100K records)
```python
config = DatabaseConfig(
    num_partitions=4,
    max_workers=8,
    use_threads=True,
    cache_size=2000
)
```

### For Large Datasets (>100K records)
```python
config = DatabaseConfig(
    num_partitions=8,
    max_workers=16,
    use_threads=False,  # Use processes for CPU-intensive tasks
    cache_size=5000
)
```

## 📈 Benchmarks

```python
# Run built-in benchmark
from colab_parallel_db import benchmark_performance
results = benchmark_performance()

# Custom benchmark
import time

db = ParallelDatabase()
db.create_table("benchmark")

# Test insert performance
start = time.time()
data = [{"id": i, "value": i*2} for i in range(10000)]
db.bulk_insert("benchmark", data)
insert_time = time.time() - start

# Test query performance
start = time.time()
results = db.query("benchmark", QueryBuilder.greater_than("value", 5000))
query_time = time.time() - start

print(f"Insert: {insert_time:.4f}s, Query: {query_time:.4f}s")
```

## 🚨 Best Practices

### 1. **Choose Appropriate Shard Keys**
```python
# Good: High cardinality fields
db.create_table("users", shard_key="id")        # Excellent
db.create_table("logs", shard_key="timestamp")  # Good

# Avoid: Low cardinality fields
db.create_table("users", shard_key="gender")    # Poor - only 2-3 shards
```

### 2. **Use Indexes Wisely**
```python
# Create indexes on frequently queried fields
db.create_index("users", "email_index", "email")
db.create_index("orders", "date_index", "order_date")
db.create_index("products", "price_index", "price")

# Don't over-index - each index consumes memory
```

### 3. **Leverage Bulk Operations**
```python
# ✅ DO: Use bulk_insert for multiple records
db.bulk_insert("table", large_dataset)

# ❌ DON'T: Insert records one by one in a loop
for record in large_dataset:
    db.insert("table", record)  # Slow!
```

### 4. **Manage Cache Effectively**
```python
# Clear cache when memory is low
db.clear_cache()

# Disable cache for one-time queries
results = db.query("table", filter_func, use_cache=False)
```

### 5. **Save Regularly in Colab**
```python
# Colab sessions can timeout - save frequently
db.save("autosave.pkl")

# Or use auto-save
config = DatabaseConfig(persist_file="colab_backup.pkl")
db = ParallelDatabase(config)
# Database will auto-save to this file
```

## 🔧 Troubleshooting

### Common Issues and Solutions

#### 1. **High Memory Usage**
```python
# Reduce cache size
config = DatabaseConfig(cache_size=500)
db = ParallelDatabase(config)

# Clear cache
db.clear_cache()

# Use fewer partitions
config = DatabaseConfig(num_partitions=2)
```

#### 2. **Slow Queries**
```python
# Create indexes on queried fields
db.create_index("table", "field_index", "field_name")

# Use indexed queries
db.indexed_query("table", "field_index", value)

# Check partition count (too many can cause overhead)
config = DatabaseConfig(num_partitions=4)  # Optimal for most cases
```

#### 3. **Data Persistence in Colab**
```python
# Save before session ends
db.save("last_backup.pkl")

# Download from Colab
from google.colab import files
files.download("last_backup.pkl")

# Upload in next session
from google.colab import files
uploaded = files.upload()
db.load("last_backup.pkl")
```

#### 4. **Thread/Process Issues**
```python
# Switch to threads (lighter, better for I/O)
config = DatabaseConfig(use_threads=True)

# Or use processes (better for CPU-intensive)
config = DatabaseConfig(use_threads=False)

# Adjust worker count
config = DatabaseConfig(max_workers=mp.cpu_count() * 2)
```

## 📚 API Reference

### Core Classes

#### `ParallelDatabase`
Main database class with all operations.

**Key Methods:**
- `create_table(table_name, shard_key=None)`
- `insert(table, record, key_field='id')`
- `bulk_insert(table, records, key_field='id')`
- `query(table, filter_func=None, use_cache=True)`
- `parallel_map(table, map_func, reduce_func=None)`
- `create_index(table, index_name, field)`
- `indexed_query(table, index_name, value)`
- `save(filename=None)`
- `load(filename=None)`
- `to_dataframe(table)`
- `from_dataframe(table, df, key_field='id')`
- `stats()`
- `close()`

#### `DatabaseConfig`
Configuration class for database settings.

**Parameters:**
- `num_partitions`: Number of parallel partitions (default: 4)
- `max_workers`: Max worker processes (default: CPU count)
- `use_threads`: Use threads instead of processes (default: True)
- `cache_size`: Max cached queries (default: 1000)
- `persist_file`: Auto-save file (default: "colab_db_backup.pkl")

#### `QueryBuilder`
Helper class for building query conditions.

**Static Methods:**
- `equals(field, value)`
- `greater_than(field, value)`
- `less_than(field, value)`
- `in_list(field, values)`
- `contains(field, substring)`
- `and_(*conditions)`
- `or_(*conditions)`

## 🎓 Examples

### Example 1: E-commerce Analytics

```python
# Initialize database
db = ParallelDatabase()

# Create tables
db.create_table("customers", shard_key="customer_id")
db.create_table("products", shard_key="category")
db.create_table("orders", shard_key="order_date")

# Load data
customers = [...]  # Load from CSV/API
products = [...]   # Load from CSV/API
orders = [...]     # Load from CSV/API

db.bulk_insert("customers", customers)
db.bulk_insert("products", products)
db.bulk_insert("orders", orders)

# Create indexes
db.create_index("orders", "customer_index", "customer_id")
db.create_index("orders", "product_index", "product_id")

# Analyze: Top spending customers
def map_order_value(order):
    return (order["customer_id"], order["total_amount"])

def reduce_top_customers(values):
    from collections import defaultdict
    customer_totals = defaultdict(float)
    for customer_id, amount in values:
        customer_totals[customer_id] += amount
    return sorted(customer_totals.items(), key=lambda x: x[1], reverse=True)[:10]

top_customers = db.parallel_map("orders", map_order_value, reduce_top_customers)
```

### Example 2: Log Analysis

```python
# Analyze server logs
db = ParallelDatabase(DatabaseConfig(num_partitions=8))

db.create_table("logs", shard_key="timestamp")

# Query: Errors in last hour
from datetime import datetime, timedelta
one_hour_ago = datetime.now() - timedelta(hours=1)

errors = db.query("logs", QueryBuilder.and_(
    QueryBuilder.equals("level", "ERROR"),
    QueryBuilder.greater_than("timestamp", one_hour_ago)
))

# Count by error type
error_counts = {}
for log in errors:
    error_type = log.get("error_type", "unknown")
    error_counts[error_type] = error_counts.get(error_type, 0) + 1
```

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request


---

**Happy parallel processing in Colab! 🎉**

*For issues, questions, or feature requests, please open an issue on GitHub.*
