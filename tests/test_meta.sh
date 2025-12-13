#!/bin/bash
# Test metadata view displays empty instead of -

set -e
cd "$(dirname "$0")/.."

TV=./target/release/tv
mkdir -p ./tmp

# Create test CSV with mixed types
cat > ./tmp/meta_test.csv << 'EOF'
num,str,empty
1,apple,
2,banana,
3,,
EOF

echo "Testing metadata empty display..."

# Check that meta doesn't show "-" for empty/non-numeric fields
OUTPUT=$($TV -c 'load ./tmp/meta_test.csv | meta' 2>/dev/null)

# String columns should have empty median/sigma, not "-"
echo "$OUTPUT" | grep 'str' | grep -qv ' - ' && echo "  str row has no '-': PASS" || exit 1

# Empty column should have empty values, not "-"
echo "$OUTPUT" | grep 'empty' | grep -qv ' - ' && echo "  empty row has no '-': PASS" || exit 1

# Numeric column should have actual values
echo "$OUTPUT" | grep 'num' | grep -q 'median\|2.00' && echo "  num row has values: PASS" || exit 1

echo "All metadata tests PASSED"

rm -rf ./tmp
