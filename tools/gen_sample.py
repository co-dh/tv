#!/usr/bin/env python3
"""Generate sample CSV and Parquet with all primary Arrow types."""
import sys
sys.path.insert(0, '.venv/lib/python3.13/site-packages')

import polars as pl
import numpy as np
from datetime import datetime, date, time, timedelta

np.random.seed(42)
n = 10_000

data = {
    # === Integer types ===
    # Note: Int8/Int16/UInt8/UInt16 not included - polars gather not implemented for these
    'id': range(1, n + 1),                                            # Int64 (default)
    'age': np.random.normal(40, 15, n).clip(18, 80).astype(int),      # Int64
    'year': np.random.randint(2000, 2025, n),                         # Int64
    'count': np.random.randint(0, 1000, n),                           # Int64
    'quantity': np.random.randint(1, 100, n),                         # Int64
    'i32_val': np.random.randint(-2**31, 2**31-1, n).astype(np.int32),# Int32
    'u32_val': np.random.randint(0, 2**32-1, n).astype(np.uint32),    # UInt32
    'u64_val': np.random.randint(0, 2**63-1, n).astype(np.uint64),    # UInt64
    # === Float types ===
    'score': np.random.normal(70, 15, n).clip(0, 100),                # Float64
    'f32_val': np.random.normal(50, 10, n).astype(np.float32),        # Float32
    'price': np.random.lognormal(5, 1, n).clip(10, 1000),             # Float64
    'rating': np.random.normal(3.5, 0.8, n).clip(1, 5),               # Float64
    'percentage': np.random.uniform(0, 100, n),                       # Float64
    'weight': np.random.normal(120, 30, n).clip(50, 200),             # Float64
    # === String type ===
    'name': [f'Person_{i}' for i in range(n)],                        # String
    'city': np.random.choice(['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix',
                              'Philadelphia', 'San Antonio', 'San Diego', 'Dallas', 'San Jose'], n),
    'status': np.random.choice(['Active', 'Inactive', 'Pending', 'Completed', 'Failed'], n),
    'category': np.random.choice(['A', 'B', 'C', 'D', 'E'], n),
    'product': np.random.choice([f'Product_{i}' for i in range(1, 21)], n),
    'email': [f'user{i}@example.com' for i in range(n)],
    'country': np.random.choice(['USA', 'UK', 'Canada', 'Australia', 'Germany',
                                 'France', 'Japan', 'China', 'India', 'Brazil'], n),
    'department': np.random.choice(['Sales', 'Marketing', 'Engineering', 'HR', 'Finance'], n),
    'level': np.random.choice(['Junior', 'Mid', 'Senior', 'Lead', 'Manager'], n),
    'region': np.random.choice(['North', 'South', 'East', 'West', 'Central'], n),
    'nullable': [None if i % 10 == 0 else f'val_{i}' for i in range(n)],  # nullable String
    # === Boolean type ===
    'is_active': np.random.choice([True, False], n),                  # Boolean
    'has_discount': np.random.choice([True, False], n),               # Boolean
    'verified': np.random.choice([True, False], n),                   # Boolean
    # === Date/Time types ===
    'date': [date(2020, 1, 1) + timedelta(days=int(x)) for x in np.random.randint(0, 1826, n)],  # Date
    'time': [time(h % 24, m % 60, s % 60) for h, m, s in zip(
        np.random.randint(0, 24, n), np.random.randint(0, 60, n), np.random.randint(0, 60, n))],  # Time
    'created_at': [datetime(2022, 1, 1) + timedelta(hours=int(x)) for x in np.random.randint(0, 17520, n)],  # Datetime
    # === Duration type ===
    'duration': [timedelta(seconds=int(x)) for x in np.random.randint(0, 86400, n)],  # Duration (0-24h)
    # === Binary type ===
    'binary': [bytes([i % 256 for i in range(4)]) for _ in range(n)],  # Binary (4 bytes each)
    # === Float (more columns) ===
    'revenue': np.random.lognormal(7, 1, n).clip(100, 10000),         # Float64
    'cost': np.random.lognormal(6, 1, n).clip(50, 5000),              # Float64
    'latitude': np.random.uniform(-90, 90, n),                        # Float64
    'longitude': np.random.uniform(-180, 180, n),                     # Float64
    'temperature': np.random.normal(15, 12, n).clip(-20, 40),         # Float64
}

df = pl.DataFrame(data)
# CSV doesn't support duration/binary - write parquet first, then drop those cols for CSV
df.write_parquet('tests/data/sample.parquet')
df.drop(['duration', 'binary']).write_csv('tests/data/sample.csv')

print(f"Created tests/data/sample.{{csv,parquet}} with {len(df)} rows, {len(df.columns)} cols")
print(f"Types: {[(c, str(df[c].dtype)) for c in df.columns]}")

# Generate hive-partitioned parquet (5 days)
import os
hive_base = 'tests/data/hive'
os.makedirs(hive_base, exist_ok=True)
dates = [date(2024, 1, d) for d in range(1, 6)]  # 5 days
for d in dates:
    part_dir = f'{hive_base}/date={d}'
    os.makedirs(part_dir, exist_ok=True)
    hive_df = pl.DataFrame({
        'id': range(1, 101),
        'value': np.random.randint(1, 100, 100),
        'name': [f'Item_{i}' for i in range(100)],
    })
    hive_df.write_parquet(f'{part_dir}/data.parquet')
print(f"Created hive partitions: {hive_base}/date={{2024-01-01..2024-01-05}}/data.parquet")
