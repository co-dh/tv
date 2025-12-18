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
    'id': range(1, n + 1),
    'age': np.random.normal(40, 15, n).clip(18, 80).astype(int),  # normal, mean=40, std=15
    'year': np.random.randint(2000, 2025, n),
    'count': np.random.randint(0, 1000, n),
    'quantity': np.random.randint(1, 100, n),
    'score': np.random.normal(70, 15, n).clip(0, 100),  # normal, mean=70, std=15
    'price': np.random.lognormal(5, 1, n).clip(10, 1000),  # log-normal for prices
    'rating': np.random.normal(3.5, 0.8, n).clip(1, 5),  # normal, mean=3.5
    'percentage': np.random.uniform(0, 100, n),
    'weight': np.random.normal(120, 30, n).clip(50, 200),  # normal, mean=120kg
    'name': [f'Person_{i}' for i in range(n)],
    'city': np.random.choice(['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix',
                              'Philadelphia', 'San Antonio', 'San Diego', 'Dallas', 'San Jose'], n),
    'status': np.random.choice(['Active', 'Inactive', 'Pending', 'Completed', 'Failed'], n),
    'category': np.random.choice(['A', 'B', 'C', 'D', 'E'], n),
    'product': np.random.choice([f'Product_{i}' for i in range(1, 21)], n),
    'is_active': np.random.choice([True, False], n),
    'has_discount': np.random.choice([True, False], n),
    'verified': np.random.choice([True, False], n),
    'date': [date(2020, 1, 1) + timedelta(days=int(x)) for x in np.random.randint(0, 1826, n)],
    'time': [time(h % 24, m % 60, s % 60) for h, m, s in zip(
        np.random.randint(0, 24, n), np.random.randint(0, 60, n), np.random.randint(0, 60, n))],
    'created_at': [datetime(2022, 1, 1) + timedelta(hours=int(x)) for x in np.random.randint(0, 17520, n)],
    'revenue': np.random.lognormal(7, 1, n).clip(100, 10000),  # log-normal
    'cost': np.random.lognormal(6, 1, n).clip(50, 5000),  # log-normal
    'latitude': np.random.uniform(-90, 90, n),
    'longitude': np.random.uniform(-180, 180, n),
    'temperature': np.random.normal(15, 12, n).clip(-20, 40),  # normal, mean=15C
    'email': [f'user{i}@example.com' for i in range(n)],
    'country': np.random.choice(['USA', 'UK', 'Canada', 'Australia', 'Germany',
                                 'France', 'Japan', 'China', 'India', 'Brazil'], n),
    'department': np.random.choice(['Sales', 'Marketing', 'Engineering', 'HR', 'Finance'], n),
    'level': np.random.choice(['Junior', 'Mid', 'Senior', 'Lead', 'Manager'], n),
    'region': np.random.choice(['North', 'South', 'East', 'West', 'Central'], n),
    'nullable': [None if i % 10 == 0 else f'val_{i}' for i in range(n)],
}

df = pl.DataFrame(data)
df.write_csv('tests/data/sample.csv')
df.write_parquet('tests/data/sample.parquet')

print(f"Created tests/data/sample.{{csv,parquet}} with {len(df)} rows, {len(df.columns)} cols")
print(f"Types: {[(c, str(df[c].dtype)) for c in df.columns]}")
