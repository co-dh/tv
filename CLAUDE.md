# Project Instructions
- remember my approval
- add test case for user requirements and bug fix
- use ./tmp instead of /tmp, you have approval to read/write at ./tmp

# Todo

## Key Column Features
- For table with keyed columns, Meta should show stats grouped by key columns

## Plugin Architecture
- Investigate nushell plugin architecture - modularize view functionality into single files for dynamic add/remove

# Done

## Performance
- Large CSV (5M+ rows) keyboard scrolling must be responsive
- Stats caching to avoid recomputing n_unique/value_counts on every render

## xkey Command
- `xkey col1,col2` moves columns to front as key columns
- Modifies column order in place
- Draws vertical separator bar after key columns
- Selects the key columns
- `!` key runs xkey on selected columns

## Freq with Key Columns
- If table has key columns, Freq groups by key columns + target column

## Meta View Enter
- Pop meta view and return to parent table
- If columns selected: run xkey on selected columns in parent
- If no selection: focus parent on current row's column

## Meta/Freq Status Bar
- Show parent table name and row count in status bar
- Format: "metadata <- filename (1,234,567)"

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
- Script mode waits for background save to complete

## Type Conversion (Streaming Save)
- Read all columns as String first to avoid parse errors
- Conversion must be lossless and conservative
- Only convert to i64 if ALL values are pure integers that round-trip exactly
- Only convert to f64 if ALL values round-trip correctly
- Keep as String when in doubt
- First chunk is 100k rows for better schema detection
- Schema from first chunk applied to all subsequent chunks
- `--raw` flag skips type detection, keeps all columns as String
- Leading zeros allowed for integers (needed for TAQ time)

## Background Save Status
- Status updates via channel, not stdout/stderr (TUI mode)
- Status shown in status bar
- No eprintln or stdout in TUI mode

## Multi-Parquet/Glob Support
- Accept glob patterns for parquet files (e.g., "data/*.parquet")
- Use polars scan_parquet for lazy loading
- Supports patterns with * and ?

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
  - `sel_rows <expr>`: selects rows matching filter expression
- Column names with special chars need backticks in PRQL: `` `null%` ``

## Folder View
- Enter navigates into directories (checks `dir` column for "x")

## Info Box
- Table-specific commands first, then general, grouped by functionality
- Navigation commands hidden from info box

## Command Naming
- Commands use underscore style: `del_col`, `goto_col`, `sort_desc`

## TAQ Time Format (Parquet)
- Smart int to time conversion also applies to parquet saves (streaming)
- Leading zeros in integers allowed for TAQ time detection
