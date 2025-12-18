#!/usr/bin/env python3
# Measure Halstead metrics for all source files
import json, subprocess, glob, sys

files = glob.glob("./src/**/*.rs", recursive=True)
results, tl, tb = [], 0, 0
for f in sorted(files):
    r = subprocess.run(["rust-code-analysis-cli", "--metrics", "-p", f, "-O", "json"], capture_output=True, text=True)
    try:
        m = json.loads(r.stdout)["metrics"]["halstead"]
        results.append({"file": f, "length": m["length"], "bugs": m["bugs"], "volume": m.get("volume", 0)})
        print(f"{f:50} len={m['length']:6.0f} bugs={m['bugs']:.3f}")
        tl += m["length"]; tb += m["bugs"]
    except: pass
print(f"{'TOTAL':50} len={tl:6.0f} bugs={tb:.3f}")

# Save to JSON
with open("halstead.json", "w") as f:
    json.dump({"total_length": tl, "total_bugs": tb, "files": results}, f, indent=2)
print("Saved to halstead.json")
