#!/usr/bin/env python3
# Measure Halstead metrics for all source files
import json, subprocess

files = [
    "./src/main.rs", "./src/app.rs", "./src/state.rs", "./src/os.rs",
    "./src/command/transform.rs", "./src/command/view.rs",
    "./src/command/io.rs", "./src/picker.rs",
    "./src/command/mod.rs", "./src/command/executor.rs"
]
tl, tb = 0, 0
for f in files:
    r = subprocess.run(["rust-code-analysis-cli", "--metrics", "-p", f, "-O", "json"], capture_output=True, text=True)
    m = json.loads(r.stdout)["metrics"]["halstead"]
    print(f"{f:45} len={m['length']:.0f} bugs={m['bugs']:.3f}")
    tl += m["length"]; tb += m["bugs"]
print(f"{'TOTAL':45} len={tl:.0f} bugs={tb:.3f}")
