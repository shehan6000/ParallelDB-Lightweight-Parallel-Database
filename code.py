import multiprocessing as mp
import threading
import json
import pickle
import hashlib
import time
from typing import Any, Dict, List, Tuple, Optional
from dataclasses import dataclass, field
from collections import defaultdict
import numpy as np
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import pandas as pd

# ============================================
# Global Helpers (Required for Multiprocessing)
# ============================================

def _global_insert_task(args):
    """Worker task for parallel insertion"""
    db_instance, table, record, key_field = args
    return db_instance.insert(table, record, key_field)

def _global_scan_task(args):
    """Worker task for parallel scanning"""
    partition, table, filter_func = args
    return partition.scan(table, filter_func)

def _global_map_task(args):
    """Worker task for parallel map operations"""
    partition, table, map_func = args
    records = partition.scan(table)
    return [map_func(record) for record in records]

# ============================================
# Configuration
# ============================================

@dataclass
class DatabaseConfig:
    num_partitions: int = 4
    max_workers: int = None
    use_threads: bool = True
    cache_size: int = 1000
    persist_file: str = "colab_db_backup.pkl"
    
    def __post_init__(self):
        if self.max_workers is None:
            self.max_workers = mp.cpu_count()

# ============================================
# Core Database Classes
# ============================================

class Partition:
    """Single partition/shard of the database"""
    def __init__(self, partition_id: int):
        self.partition_id = partition_id
        self.data = {}  # {table_name: {key: value}}
        self.indexes = defaultdict(dict)
        self._lock = None # Initialized lazily for pickle compatibility

    @property
    def lock(self):
        """Lazy loader for RLock (Locks cannot be pickled)"""
        if self._lock is None:
            self._lock = threading.RLock()
        return self._lock
        
    def insert(self, table: str, key: str, value: dict) -> bool:
        with self.lock:
            if table not in self.data:
                self.data[table] = {}
            if key in self.data[table]:
                return False
            self.data[table][key] = value
            return True
    
    def update(self, table: str, key: str, value: dict) -> bool:
        with self.lock:
            if table not in self.data or key not in self.data[table]:
                return False
            self.data[table][key].update(value)
            return True
    
    def get(self, table: str, key: str) -> Optional[dict]:
        with self.lock:
            return self.data.get(table, {}).get(key)
    
    def delete(self, table: str, key: str) -> bool:
        with self.lock:
            if table in self.data and key in self.data[table]:
                del self.data[table][key]
                return True
            return False
    
    def scan(self, table: str, filter_func=None) -> List[dict]:
        with self.lock:
            if table not in self.data:
                return []
            records = list(self.data[table].values())
            if filter_func:
                records = [r for r in records if filter_func(r)]
            return records

    def create_index(self, table: str, index_name: str, field: str):
        with self.lock:
            if table not in self.data: return
            if table not in self.indexes: self.indexes[table] = {}
            index = defaultdict(list)
            for key, record in self.data[table].items():
                if field in record:
                    index[record[field]].append(key)
            self.indexes[table][index_name] = dict(index)
    
    def query_index(self, table: str, index_name: str, value: Any) -> List[dict]:
        with self.lock:
            if table not in self.indexes or index_name not in self.indexes[table]:
                return []
            keys = self.indexes[table][index_name].get(value, [])
            return [self.data[table][key] for key in keys if key in self.data[table]]
    
    def count(self, table: str) -> int:
        with self.lock:
            return len(self.data.get(table, {}))

    def __getstate__(self):
        """Remove lock before pickling for multiprocessing"""
        state = self.__dict__.copy()
        state['_lock'] = None
        return state

class ParallelDatabase:
    def __init__(self, config: DatabaseConfig = None):
        self.config = config or DatabaseConfig()
        self.partitions = [Partition(i) for i in range(self.config.num_partitions)]
        self.query_cache = {}
        self.cache_keys = []
        self.table_metadata = {}
        
        if self.config.use_threads:
            self.executor = ThreadPoolExecutor(max_workers=self.config.max_workers)
        else:
            self.executor = ProcessPoolExecutor(max_workers=self.config.max_workers)
    
    def _get_partition(self, key: str) -> Partition:
        hash_val = int(hashlib.md5(key.encode()).hexdigest(), 16)
        return self.partitions[hash_val % self.config.num_partitions]
    
    def _get_partition_for_shard_key(self, table: str, record: dict) -> Partition:
        if table in self.table_metadata and 'shard_key' in self.table_metadata[table]:
            shard_field = self.table_metadata[table]['shard_key']
            if shard_field in record:
                return self._get_partition(str(record[shard_field]))
        record_str = json.dumps(record, sort_keys=True)
        return self._get_partition(record_str)
    
    def create_table(self, table: str, shard_key: str = None):
        self.table_metadata[table] = {'shard_key': shard_key}
    
    def insert(self, table: str, record: dict, key_field: str = 'id') -> bool:
        if key_field not in record:
            raise ValueError(f"Record must contain key field: {key_field}")
        key = str(record[key_field])
        partition = self._get_partition_for_shard_key(table, record)
        return partition.insert(table, key, record)
    
    def bulk_insert(self, table: str, records: List[dict], key_field: str = 'id') -> List[bool]:
        tasks = [(self, table, record, key_field) for record in records]
        return list(self.executor.map(_global_insert_task, tasks))
    
    def get(self, table: str, key: str) -> Optional[dict]:
        for partition in self.partitions:
            result = partition.get(table, key)
            if result is not None: return result
        return None

    def query(self, table: str, filter_func=None, use_cache: bool = True) -> List[dict]:
        cache_key = f"query_{table}_{hash(filter_func) if filter_func else 'all'}"
        if use_cache and cache_key in self.query_cache:
            self._update_cache_lru(cache_key)
            return self.query_cache[cache_key]
        
        tasks = [(p, table, filter_func) for p in self.partitions]
        results = []
        for res in self.executor.map(_global_scan_task, tasks):
            results.extend(res)
        
        if use_cache: self._cache_result(cache_key, results)
        return results

    def parallel_map(self, table: str, map_func, reduce_func=None):
        tasks = [(p, table, map_func) for p in self.partitions]
        mapped_results = []
        for res in self.executor.map(_global_map_task, tasks):
            mapped_results.extend(res)
        
        return reduce_func(mapped_results) if reduce_func else mapped_results

    def create_index(self, table: str, index_name: str, field: str):
        futures = [self.executor.submit(p.create_index, table, index_name, field) for p in self.partitions]
        for f in futures: f.result()

    def indexed_query(self, table: str, index_name: str, value: Any) -> List[dict]:
        futures = [self.executor.submit(p.query_index, table, index_name, value) for p in self.partitions]
        results = []
        for f in futures: results.extend(f.result())
        return results

    def count(self, table: str) -> int:
        return sum(p.count(table) for p in self.partitions)

    def _cache_result(self, key: str, result: Any):
        if len(self.query_cache) >= self.config.cache_size:
            lru_key = self.cache_keys.pop(0)
            del self.query_cache[lru_key]
        self.query_cache[key] = result
        self.cache_keys.append(key)

    def _update_cache_lru(self, key: str):
        if key in self.cache_keys:
            self.cache_keys.remove(key)
            self.cache_keys.append(key)

    def save(self, filename: str = None):
        filename = filename or self.config.persist_file
        data = {
            'partitions': [p.data for p in self.partitions],
            'indexes': [p.indexes for p in self.partitions],
            'table_metadata': self.table_metadata,
            'config': self.config
        }
        with open(filename, 'wb') as f:
            pickle.dump(data, f)
        print(f"Database saved to {filename}")

    def load(self, filename: str = None):
        filename = filename or self.config.persist_file
        try:
            with open(filename, 'rb') as f:
                data = pickle.load(f)
            for i, p_data in enumerate(data['partitions']):
                if i < len(self.partitions): self.partitions[i].data = p_data
            for i, idx_data in enumerate(data['indexes']):
                if i < len(self.partitions): self.partitions[i].indexes = idx_data
            self.table_metadata = data.get('table_metadata', {})
            return True
        except FileNotFoundError:
            return False

    def to_dataframe(self, table: str) -> pd.DataFrame:
        return pd.DataFrame(self.query(table, use_cache=False))

    def stats(self) -> Dict:
        return {
            'num_partitions': self.config.num_partitions,
            'tables': {t: {'record_count': self.count(t)} for t in self.table_metadata}
        }

    def close(self):
        self.executor.shutdown(wait=True)

# ============================================
# Query Builder
# ============================================

class QueryBuilder:
    @staticmethod
    def equals(field: str, value: Any): return lambda r: r.get(field) == value
    @staticmethod
    def greater_than(field: str, value: Any): return lambda r: r.get(field) > value
    @staticmethod
    def less_than(field: str, value: Any): return lambda r: r.get(field) < value
    @staticmethod
    def and_(*conds): return lambda r: all(c(r) for c in conds)
    @staticmethod
    def or_(*conds): return lambda r: any(c(r) for c in conds)

# ============================================
# Benchmark and Demo
# ============================================

def demonstrate_database():
    print("=" * 60)
    print("PARALLEL DATABASE DEMONSTRATION")
    print("=" * 60)
    
    db = ParallelDatabase(DatabaseConfig(num_partitions=4, use_threads=True))
    db.create_table("users", shard_key="id")
    
    users = [{"id": i, "name": f"User{i}", "age": 20 + (i % 30)} for i in range(1000)]
    db.bulk_insert("users", users)
    
    print(f"✓ Inserted {db.count('users')} users")
    
    # Query Example
    young_users = db.query("users", QueryBuilder.less_than("age", 25))
    print(f"Users under 25: {len(young_users)}")
    
    # Map-Reduce Example
    avg_age = db.parallel_map("users", lambda r: r['age'], lambda ages: sum(ages)/len(ages))
    print(f"Average user age: {avg_age:.2f}")
    
    db.close()
    print("✓ Demonstration complete!")

if __name__ == "__main__":
    demonstrate_database()
