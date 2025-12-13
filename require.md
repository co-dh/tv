# Requirements - 2025-12-13 Session

## Performance
- Large CSV (5M+ rows) keyboard scrolling must be responsive
- Stats caching to avoid recomputing n_unique/value_counts on every render

## xkey Command
- `xkey col1,col2` moves columns to front as key columns
- Modifies column order in place
- Draws vertical separator bar after key columns
- Selects the key columns

## Meta View Enter
- Pop meta view and return to parent table
- If columns selected: run xkey on selected columns in parent (moves them to front as key columns)
- If no selection: focus parent on current row's column

## Command History
- Log commands to `~/.tv/history`
- Create `~/.tv/` directory if needed

## Delete Feedback
- Show "N columns deleted" message after delete

## View Handler Refactor
- Each view (Meta, Freq, Folder) is a separate handler module
- ViewKind enum for identifying view types
- ViewHandler trait for view-specific command dispatch

## Save Command
- Prompt to create directory if parent doesn't exist
- Large gz save runs in background thread
- Save from full file on disk (gz_source), not just what's in memory
- Streaming gz to parquet for large files

## Type Conversion (Streaming Save)
- Read all columns as String first to avoid parse errors
- Conversion must be lossless and conservative
- Only convert to i64 if ALL values are pure integers that round-trip exactly
- Only convert to f64 if ALL values round-trip correctly
- Keep as String when in doubt
- First chunk is 100k rows for better schema detection
- Schema from first chunk applied to all subsequent chunks
- `--raw` flag skips type detection, keeps all columns as String

## Background Save Status
- Status updates via channel, not stdout/stderr (TUI mode)
- Status shown in status bar
- No eprintln or stdout in TUI mode

## Multi-Parquet/Glob Support
- Accept glob patterns for parquet files (e.g., "data/*.parquet")
- Use polars scan_parquet for lazy loading
- Supports patterns with * and ?

## Meta/Freq Status Bar
- Show parent table row count in status bar
- Format: "metadata (1,234,567)" or "Freq:col (1,234,567)"

## Forth-style User Functions (cfg/funcs.4th)
- Syntax: `: funcname body ... ;`
- Comments: `( comment text )` - ignored during parsing
- Functions expand recursively (max 10 levels)
- Loaded from `cfg/funcs.4th` on startup
- Built-in functions:
  - `sel_null`: `sel_rows \`null%\` == '100.0'` - Meta view only
  - `sel_single`: `sel_rows distinct == '1'` - Meta view only
- Key bindings (meta view only in cfg/key.csv):
  - `0` -> `sel_null`
  - `1` -> `sel_single`
- Commands:
  - `sel_all`: selects all rows (Meta/Freq) or columns (table)
  - `sel_rows <expr>`: selects rows matching filter expression (like filter but highlights instead)
- Column names with special chars need backticks in PRQL: `` `null%` ``
