#!/bin/bash
# Test ls and lr directory listing

set -e
cd "$(dirname "$0")/.."

TV=./target/release/tv
mkdir -p ./tmp/testdir/subdir
touch ./tmp/testdir/file1.txt
touch ./tmp/testdir/file2.txt

echo "Testing ls dir column..."

# dir column should show 'x' for directories, empty for files
$TV -c 'ls ./tmp/testdir' 2>/dev/null | grep -q 'subdir' && echo "  ls shows subdir: PASS" || exit 1
# Check subdir line has 'x' in the dir column (last column before │)
$TV -c 'ls ./tmp/testdir' 2>/dev/null | grep -q 'subdir.*x' && echo "  dir=x for subdir: PASS" || exit 1
# Check file line has empty dir column (spaces only before final │)
$TV -c 'ls ./tmp/testdir' 2>/dev/null | grep 'file1' | grep -q '     │$' && echo "  dir empty for file: PASS" || exit 1

echo "Testing lr recursive..."

$TV -c 'lr ./tmp/testdir' 2>/dev/null | grep -q 'subdir' && echo "  lr shows subdir: PASS" || exit 1

echo "All ls/lr tests PASSED"

rm -rf ./tmp/testdir
