#!/bin/bash
# Test frequency table functionality

set -e
cd "$(dirname "$0")/.."

TV=./target/release/tv
mkdir -p ./tmp

cat > ./tmp/freq_test.csv << 'EOF'
city,sales
NYC,100
LA,200
NYC,150
Chicago,300
LA,250
NYC,175
EOF

echo "Testing frequency table..."

# Test freq command shows value counts
$TV -c 'load ./tmp/freq_test.csv | freq city' 2>/dev/null | grep -q 'NYC' && echo "  freq city contains NYC: PASS" || exit 1
$TV -c 'load ./tmp/freq_test.csv | freq city' 2>/dev/null | grep -q '(3 rows)' && echo "  freq city has 3 unique values: PASS" || exit 1

# Test freq shows Cnt column
$TV -c 'load ./tmp/freq_test.csv | freq city' 2>/dev/null | grep -q 'Cnt' && echo "  freq has Cnt column: PASS" || exit 1

echo "All frequency tests PASSED"

echo ""
echo "Testing freq + filter workflow (simulates Enter key behavior)..."

# Filter by single value (what Enter does on freq view)
$TV -c "load ./tmp/freq_test.csv | filter city IN ('NYC')" 2>/dev/null | grep -q '(3 rows)' && echo "  filter city='NYC': 3 rows: PASS" || exit 1
$TV -c "load ./tmp/freq_test.csv | filter city IN ('LA')" 2>/dev/null | grep -q '(2 rows)' && echo "  filter city='LA': 2 rows: PASS" || exit 1

# Filter by multiple values (what Enter does with multi-select on freq view)
$TV -c "load ./tmp/freq_test.csv | filter city IN ('NYC','LA')" 2>/dev/null | grep -q '(5 rows)' && echo "  filter city IN (NYC,LA): 5 rows: PASS" || exit 1

echo "All freq + filter workflow tests PASSED"

rm -rf ./tmp
