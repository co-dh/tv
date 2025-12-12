import polars as pl
import numpy as np
from datetime import datetime, timedelta

np.random.seed(42)
n_rows = 100_000

data = {
    'id': range(1, n_rows + 1),
    'age': np.random.randint(18, 80, n_rows),
    'year': np.random.randint(2000, 2025, n_rows),
    'count': np.random.randint(0, 1000, n_rows),
    'quantity': np.random.randint(1, 100, n_rows),
    'score': np.random.uniform(0, 100, n_rows),
    'price': np.random.uniform(10, 1000, n_rows),
    'rating': np.random.uniform(1, 5, n_rows),
    'percentage': np.random.uniform(0, 100, n_rows),
    'weight': np.random.uniform(50, 200, n_rows),
    'name': [f'Person_{i}' for i in range(n_rows)],
    'city': np.random.choice(['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix',
                              'Philadelphia', 'San Antonio', 'San Diego', 'Dallas', 'San Jose'], n_rows),
    'status': np.random.choice(['Active', 'Inactive', 'Pending', 'Completed', 'Failed'], n_rows),
    'category': np.random.choice(['A', 'B', 'C', 'D', 'E'], n_rows),
    'product': np.random.choice([f'Product_{i}' for i in range(1, 21)], n_rows),
    'is_active': np.random.choice([True, False], n_rows),
    'has_discount': np.random.choice([True, False], n_rows),
    'verified': np.random.choice([True, False], n_rows),
    'date': [datetime(2020, 1, 1) + timedelta(days=int(x)) for x in np.random.randint(0, 1826, n_rows)],
    'created_at': [datetime(2022, 1, 1) + timedelta(hours=int(x)) for x in np.random.randint(0, 17520, n_rows)],
    'revenue': np.random.uniform(100, 10000, n_rows),
    'cost': np.random.uniform(50, 5000, n_rows),
    'latitude': np.random.uniform(-90, 90, n_rows),
    'longitude': np.random.uniform(-180, 180, n_rows),
    'temperature': np.random.uniform(-20, 40, n_rows),
    'email': [f'user{i}@example.com' for i in range(n_rows)],
    'country': np.random.choice(['USA', 'UK', 'Canada', 'Australia', 'Germany',
                                 'France', 'Japan', 'China', 'India', 'Brazil'], n_rows),
    'department': np.random.choice(['Sales', 'Marketing', 'Engineering', 'HR', 'Finance'], n_rows),
    'level': np.random.choice(['Junior', 'Mid', 'Senior', 'Lead', 'Manager'], n_rows),
    'region': np.random.choice(['North', 'South', 'East', 'West', 'Central'], n_rows),
}

df = pl.DataFrame(data)
df.write_parquet('sample.parquet')

print(f"Created sample.parquet with {len(df)} rows and {len(df.columns)} columns")
print(f"Columns: {df.columns}")
