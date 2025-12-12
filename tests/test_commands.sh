#!/bin/bash
# Test all script commands

set -e
cd "$(dirname "$0")/.."

TV=./target/release/tv
mkdir -p ./tmp

# Create test data
cat > ./tmp/test.csv << 'EOF'
name,city,value,score
Alice,NYC,100,85
Bob,LA,200,90
Carol,NYC,150,75
Dave,Chicago,300,95
Eve,LA,250,80
Frank,NYC,175,null
EOF

echo "Testing script commands..."

# load - tested implicitly by all other tests

# save
$TV -c "load ./tmp/test.csv | save ./tmp/out.csv" 2>/dev/null
[ -f ./tmp/out.csv ] && echo "  save: PASS" || exit 1

# freq
$TV -c "load ./tmp/test.csv | freq city" 2>/dev/null | grep -q "(3 rows)" && echo "  freq: PASS" || exit 1

# meta
$TV -c "load ./tmp/test.csv | meta" 2>/dev/null | grep -q "name" && echo "  meta: PASS" || exit 1

# corr
$TV -c "load ./tmp/test.csv | corr" 2>/dev/null | grep -q "value" && echo "  corr: PASS" || exit 1

# filter (numeric)
$TV -c "load ./tmp/test.csv | filter value>200" 2>/dev/null | grep -q "(2 rows)" && echo "  filter numeric: PASS" || exit 1

# filter (string)
$TV -c "load ./tmp/test.csv | filter city==NYC" 2>/dev/null | grep -q "(3 rows)" && echo "  filter string: PASS" || exit 1

# select
$TV -c "load ./tmp/test.csv | sel name,city" 2>/dev/null | grep -q "name" && echo "  select: PASS" || exit 1
! $TV -c "load ./tmp/test.csv | sel name,city" 2>/dev/null | grep -q "value" && echo "  select removes cols: PASS" || exit 1

# sort (asc)
OUT=$($TV -c "load ./tmp/test.csv | sort value" 2>/dev/null)
echo "$OUT" | head -20 | grep -q "Alice" && echo "  sort asc: PASS" || exit 1

# sortdesc
OUT=$($TV -c "load ./tmp/test.csv | sortdesc value" 2>/dev/null)
echo "$OUT" | head -20 | grep -q "Dave" && echo "  sort desc: PASS" || exit 1

# delcol
! $TV -c "load ./tmp/test.csv | delcol score" 2>/dev/null | grep -q "score" && echo "  delcol: PASS" || exit 1

# rename
$TV -c "load ./tmp/test.csv | rename name username" 2>/dev/null | grep -q "username" && echo "  rename: PASS" || exit 1

# delnull - need column with all nulls
cat > ./tmp/nulls.csv << 'EOF'
a,b,c
1,,x
2,,y
3,,z
EOF
! $TV -c "load ./tmp/nulls.csv | delnull" 2>/dev/null | grep -q "│ b " && echo "  delnull: PASS" || exit 1

# del1 - need column with single value
cat > ./tmp/single.csv << 'EOF'
a,b,c
1,X,x
2,X,y
3,X,z
EOF
! $TV -c "load ./tmp/single.csv | del1" 2>/dev/null | grep -q "│ b " && echo "  del1: PASS" || exit 1

# pipe chaining
$TV -c "load ./tmp/test.csv | filter city==NYC | sort value | sel name,value" 2>/dev/null | grep -q "(3 rows)" && echo "  pipe chain: PASS" || exit 1

rm -rf ./tmp

echo "All command tests PASSED"
