#!/usr/bin/env python3
"""Dump cargo dependencies to parquet. Only updates if Cargo.lock changed."""
import subprocess
import json
from pathlib import Path
import hashlib

OUT = Path.home() / ".tv" / "cargo.parquet"
OUT.parent.mkdir(parents=True, exist_ok=True)
STAMP = OUT.parent / ".cargo_stamp"

# Check if Cargo.lock changed
lock = Path("Cargo.lock")
if not lock.exists():
    exit(0)
cur_hash = hashlib.md5(lock.read_bytes()).hexdigest()
last_hash = STAMP.read_text().strip() if STAMP.exists() else ""
if cur_hash == last_hash and OUT.exists():
    exit(0)

# Get metadata
meta = json.loads(subprocess.run(
    ["cargo", "metadata", "--format-version=1"],
    capture_output=True, text=True).stdout)

# Build rows as tuples
rows = []
for pkg in meta.get("packages", []):
    name = pkg.get("name", "")
    ver = pkg.get("version", "")
    deps = len(pkg.get("dependencies", []))
    targets = pkg.get("targets", [])
    kind = targets[0].get("kind", [""])[0] if targets else ""
    rows.append((name, ver, deps, kind))

# Write parquet
import duckdb
con = duckdb.connect()
con.execute("CREATE TABLE t(name VARCHAR, version VARCHAR, deps BIGINT, kind VARCHAR)")
con.executemany("INSERT INTO t VALUES(?,?,?,?)", rows)
con.execute(f"COPY t TO '{OUT}' (FORMAT PARQUET)")
print(f"Wrote {OUT} ({len(rows)} packages)")
STAMP.write_text(cur_hash)
