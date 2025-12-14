#!/bin/bash
# Benchmark polars vs duckapi vs duckcli vs raw duckdb
# Tests internal thread scaling (1,2,4,8,16 threads in single process)
# Usage: ./tools/bench.sh [parquet|all]

set -e
TV=${TV:-./target/release/tv}
PARQUET=${PARQUET:-tests/data/nyse/1.parquet}
COL=${COL:-Symbol}
SYMBOL=${SYMBOL:-AAPL}
THREADS="${THREADS:-1 2 4 8 16}"

# Build if needed
[ -f "$TV" ] || cargo build --release

# Time a command, return seconds
timeit() {
    local start=$(date +%s%N)
    "$@" >/dev/null 2>&1
    local end=$(date +%s%N)
    awk "BEGIN {printf \"%.3f\", ($end-$start)/1000000000}"
}

# Header for operation
op_header() {
    echo ""
    echo "### $1"
    echo "| Backend      |   1T |   2T |   4T |   8T |  16T |"
    echo "|--------------|------|------|------|------|------|"
}

# Run all thread counts for one backend, print one row
bench_row() {
    local name="$1"
    shift
    printf "| %-12s |" "$name"
    for t in $THREADS; do
        export POLARS_MAX_THREADS=$t RAYON_NUM_THREADS=$t
        local time=$(timeit "$@")
        printf " %4ss |" "$time"
    done
    echo ""
}

# Raw duckdb with thread setting
bench_row_raw() {
    local name="$1" sql="$2"
    printf "| %-12s |" "$name"
    for t in $THREADS; do
        local time=$(timeit duckdb -c "SET threads=$t; $sql")
        printf " %4ss |" "$time"
    done
    echo ""
}

# Main benchmark
bench_parquet() {
    local f="$PARQUET"
    [ -f "$f" ] || { echo "File not found: $f"; return 1; }
    local size=$(ls -lh "$f" | awk '{print $5}')
    local rows=$(duckdb -c "SELECT COUNT(*) FROM '$f'" 2>/dev/null | tail -1 | tr -d ' ')

    echo "# Benchmark: $f"
    echo "Size: $size, Rows: $rows"
    echo "Threads: $THREADS"
    echo ""

    # freq
    op_header "freq $COL"
    bench_row "polars" $TV "$f" -c "from $f | freq $COL"
    bench_row "duckapi" $TV --duckapi "$f" -c "from $f | freq $COL"
    bench_row "duckcli" $TV --duckcli "$f" -c "from $f | freq $COL"
    bench_row_raw "duckdb raw" "SELECT \"$COL\", COUNT(*) as Cnt FROM '$f' GROUP BY \"$COL\" ORDER BY Cnt DESC"

    # filter
    op_header "filter $COL='$SYMBOL'"
    bench_row "polars" $TV "$f" -c "from $f | filter $COL = '$SYMBOL'"
    bench_row "duckapi" $TV --duckapi "$f" -c "from $f | filter $COL = '$SYMBOL'"
    bench_row "duckcli" $TV --duckcli "$f" -c "from $f | filter $COL = '$SYMBOL'"
    bench_row_raw "duckdb raw" "SELECT * FROM '$f' WHERE \"$COL\" = '$SYMBOL'"

    # count
    op_header "count"
    bench_row "polars" $TV "$f" -c "from $f | count"
    bench_row "duckapi" $TV --duckapi "$f" -c "from $f | count"
    bench_row "duckcli" $TV --duckcli "$f" -c "from $f | count"
    bench_row_raw "duckdb raw" "SELECT COUNT(*) FROM '$f'"

    # head
    op_header "head 100"
    bench_row "polars" $TV "$f" -c "from $f | take 100"
    bench_row "duckapi" $TV --duckapi "$f" -c "from $f | take 100"
    bench_row "duckcli" $TV --duckcli "$f" -c "from $f | take 100"
    bench_row_raw "duckdb raw" "SELECT * FROM '$f' LIMIT 100"

    # meta
    op_header "meta"
    bench_row "polars" $TV "$f" -c "from $f | meta"
    bench_row "duckapi" $TV --duckapi "$f" -c "from $f | meta"
    bench_row "duckcli" $TV --duckcli "$f" -c "from $f | meta"
    bench_row_raw "duckdb raw" "DESCRIBE SELECT * FROM '$f'"

    echo ""
    echo "Done."
}

bench_parquet
