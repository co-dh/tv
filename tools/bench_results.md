# Benchmark Results

## 2025-12-14: Polars vs DuckDB Backend

**Test file:** tests/data/nyse/1.parquet (3.7G, 304M rows, 11k symbols)

### Single Process

| Backend         |    Real  |
|-----------------|----------|
| Polars freq     |   0.787s |
| DuckDB freq     |   0.761s |
| Polars filter   |   0.257s |
| DuckDB filter   |   0.253s |

### Multi Process (4 parallel)

| Backend         | Total(s) | Avg(s)  |
|-----------------|----------|---------|
| Polars freq x4  |     2.58s |    0.64s |
| DuckDB freq x4  |     2.56s |    0.64s |

### Summary

- Both backends comparable performance (~0.76s for freq)
- DuckDB slightly faster in all tests
- Linear scaling with parallel processes (4x processes = ~3.3x time)
- Filter much faster than freq (~0.25s vs ~0.76s)

### Historical Comparison

| Engine | Freq Time |
|--------|-----------|
| Polars 0.45 default | 7.0s |
| Polars 0.52 default | 5.0s |
| Polars 0.52 streaming | 0.79s |
| DuckDB native API | 0.76s |
| DuckDB CLI | 0.10s |

Note: DuckDB CLI is faster because it outputs directly without DataFrame conversion overhead.
