# DRY Skill - Reduce Code Duplication

Run Halstead metrics, find duplication, refactor, measure improvement.

## Steps

1. **Measure baseline** - Run `python3 tool/measure.py` to get current metrics
2. **Find duplication** - Search for:
   - Repeated code patterns (grep for similar blocks)
   - Functions that could be unified
   - Copy-pasted logic across files
3. **Refactor** - Extract common code to shared helpers
4. **Measure again** - Run metrics and compare to baseline
5. **Report** - Show before/after: length, bugs, % improvement

## Common DRY targets

- Similar match patterns (e.g., DataType checks → is_numeric)
- Repeated SQL query building
- Duplicate parsing/formatting logic
- Similar error handling patterns

## Run

```bash
python3 tool/measure.py
```

Focus on files with highest bug counts first (main.rs, system.rs, renderer.rs).
