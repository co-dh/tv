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

rm -rf ./tmp
