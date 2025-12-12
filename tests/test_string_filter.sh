#!/bin/bash
# Test string filter with SQL WHERE syntax

set -e
cd "$(dirname "$0")/.."

TV=./target/release/tv
mkdir -p ./tmp

cat > ./tmp/strings.csv << 'EOF'
name,value
apple,10
banana,20
cherry,30
pineapple,40
grape,50
blueberry,60
EOF

echo "Testing SQL filter patterns..."

# exact match: 1 row (apple)
$TV -c "load ./tmp/strings.csv | filter name = 'apple'" 2>/dev/null | grep -q '(1 rows)' && echo "  name = 'apple': PASS" || exit 1

# contains: 2 rows (apple, pineapple)
$TV -c "load ./tmp/strings.csv | filter name LIKE '%apple%'" 2>/dev/null | grep -q '(2 rows)' && echo "  name LIKE '%apple%': PASS" || exit 1

# ends with: 2 rows (cherry, blueberry)
$TV -c "load ./tmp/strings.csv | filter name LIKE '%rry'" 2>/dev/null | grep -q '(2 rows)' && echo "  name LIKE '%rry': PASS" || exit 1

# starts with: 2 rows (banana, blueberry)
$TV -c "load ./tmp/strings.csv | filter name LIKE 'b%'" 2>/dev/null | grep -q '(2 rows)' && echo "  name LIKE 'b%': PASS" || exit 1

# numeric comparison
$TV -c "load ./tmp/strings.csv | filter value > 30" 2>/dev/null | grep -q '(3 rows)' && echo "  value > 30: PASS" || exit 1

echo "All SQL filter tests PASSED"
