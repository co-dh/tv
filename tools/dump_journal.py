#!/usr/bin/env python3
"""Dump journal logs to parquet files, one per day. Only updates if changed."""
import subprocess
import json
from pathlib import Path
from datetime import datetime

OUT_DIR = Path.home() / ".tv" / "journal"
OUT_DIR.mkdir(parents=True, exist_ok=True)

# Get last dump time
STAMP = OUT_DIR / ".stamp"
last = STAMP.read_text().strip() if STAMP.exists() else ""

# Get current journal cursor
cur = subprocess.run(["journalctl", "-n1", "-o", "json", "--no-pager"],
                     capture_output=True, text=True).stdout.strip()
if cur:
    cursor = json.loads(cur).get("__CURSOR", "")
    if cursor == last:
        exit(0)  # No changes

# Dump all entries as JSON
proc = subprocess.run(["journalctl", "-o", "json", "--no-pager"],
                      capture_output=True, text=True)

# Group by date
days = {}
for line in proc.stdout.strip().split('\n'):
    if not line:
        continue
    try:
        e = json.loads(line)
        ts = int(e.get("__REALTIME_TIMESTAMP", 0)) // 1_000_000
        dt = datetime.fromtimestamp(ts)
        date = dt.strftime("%Y-%m-%d")
        time = dt.strftime("%H:%M:%S")
        boot = e.get("_BOOT_ID", "")[:8]
        unit = e.get("SYSLOG_IDENTIFIER", "")
        msg = e.get("MESSAGE", "") if isinstance(e.get("MESSAGE"), str) else ""
        if date not in days:
            days[date] = []
        days[date].append((date, time, boot, unit, msg))
    except:
        pass

# Write parquet per day (only today or missing)
import duckdb
today = datetime.now().strftime("%Y-%m-%d")
for date, rows in days.items():
    pq = OUT_DIR / f"{date}.parquet"
    if date == today or not pq.exists():
        con = duckdb.connect()
        con.execute("CREATE TABLE t(date VARCHAR, time VARCHAR, boot VARCHAR, unit VARCHAR, message VARCHAR)")
        con.executemany("INSERT INTO t VALUES(?,?,?,?,?)", rows)
        con.execute(f"COPY t TO '{pq}' (FORMAT PARQUET)")
        print(f"Wrote {pq} ({len(rows)} rows)")

# Save cursor
if cur:
    STAMP.write_text(json.loads(cur).get("__CURSOR", ""))
