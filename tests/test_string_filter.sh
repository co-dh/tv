#!/bin/bash
# Test string filter and search glob patterns

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

echo "Testing string filter patterns..."

# exact match: 1 row (apple)
$TV -c 'load ./tmp/strings.csv | filter name==apple' 2>/dev/null | grep -q '(1 rows)' && echo "  filter name==apple: PASS" || exit 1

# contains: 2 rows (apple, pineapple)
$TV -c 'load ./tmp/strings.csv | filter name==*apple*' 2>/dev/null | grep -q '(2 rows)' && echo "  filter name==*apple*: PASS" || exit 1

# ends with: 2 rows (cherry, blueberry)
$TV -c 'load ./tmp/strings.csv | filter name==*rry' 2>/dev/null | grep -q '(2 rows)' && echo "  filter name==*rry: PASS" || exit 1

# starts with: 2 rows (banana, blueberry)
$TV -c 'load ./tmp/strings.csv | filter name==b*' 2>/dev/null | grep -q '(2 rows)' && echo "  filter name==b*: PASS" || exit 1

echo "All string filter tests PASSED"

echo ""
echo "Note: Interactive skim hints for / and \\ show glob examples based on current cell:"
echo "  For cell 'Finance': Fi* (starts with), *ce (ends with)"
