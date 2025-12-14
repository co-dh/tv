#!/bin/bash
# Benchmark polars vs duckdb backends
# Usage: ./tools/bench.sh [parquet_file] [column] [mode]
# Modes: single (default), multi (parallel processes)

TV=${TV:-./target/release/tv}
FILE=${1:-tests/data/nyse/1.parquet}
COL=${2:-Symbol}
MODE=${3:-single}
NPROC=${NPROC:-4}

if [ ! -f "$TV" ]; then
    echo "Building release..."
    cargo build --release
fi

if [ ! -f "$FILE" ]; then
    echo "File not found: $FILE"
    exit 1
fi

SIZE=$(ls -lh "$FILE" | awk '{print $5}')

# Run benchmark: captures real time
run_bench() {
    local name="$1"
    shift
    local start=$(date +%s%N)
    "$@" >/dev/null 2>&1
    local end=$(date +%s%N)
    local ms=$(( (end - start) / 1000000 ))
    local sec=$(awk "BEGIN {printf \"%.3f\", $ms/1000}")
    printf "| %-15s | %7ss |\n" "$name" "$sec"
}

echo "=== Benchmark: $FILE ($SIZE) ==="
echo "Column: $COL | Mode: $MODE"
echo ""

if [ "$MODE" = "single" ]; then
    echo "| Backend         |    Real  |"
    echo "|-----------------|----------|"

    run_bench "Polars freq" $TV "$FILE" -c "from $FILE | freq $COL"
    run_bench "DuckDB freq" $TV --duckdb "$FILE" -c "from $FILE | freq $COL"
    run_bench "Polars filter" $TV "$FILE" -c "from $FILE | filter $COL = 'NVDA'"
    run_bench "DuckDB filter" $TV --duckdb "$FILE" -c "from $FILE | filter $COL = 'NVDA'"

elif [ "$MODE" = "multi" ]; then
    echo "Running $NPROC parallel processes..."
    echo ""
    echo "| Backend         | Total(s) | Avg(s)  |"
    echo "|-----------------|----------|---------|"

    # Polars multi-process
    start=$(date +%s%N)
    for i in $(seq 1 $NPROC); do
        $TV "$FILE" -c "from $FILE | freq $COL" >/dev/null 2>&1 &
    done
    wait
    end=$(date +%s%N)
    ms=$(( (end - start) / 1000000 ))
    total=$(awk "BEGIN {printf \"%.2f\", $ms/1000}")
    avg=$(awk "BEGIN {printf \"%.2f\", $ms/1000/$NPROC}")
    printf "| Polars freq x%-2d | %8ss | %7ss |\n" "$NPROC" "$total" "$avg"

    # DuckDB multi-process
    start=$(date +%s%N)
    for i in $(seq 1 $NPROC); do
        $TV --duckdb "$FILE" -c "from $FILE | freq $COL" >/dev/null 2>&1 &
    done
    wait
    end=$(date +%s%N)
    ms=$(( (end - start) / 1000000 ))
    total=$(awk "BEGIN {printf \"%.2f\", $ms/1000}")
    avg=$(awk "BEGIN {printf \"%.2f\", $ms/1000/$NPROC}")
    printf "| DuckDB freq x%-2d | %8ss | %7ss |\n" "$NPROC" "$total" "$avg"
fi

echo ""
echo "Done."
