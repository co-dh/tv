# Done

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


## DRY Refactoring and Halstead Improvement
- Added dispatch() helper for plugin action dispatch
- Added run() helper for command execution with error handling
- Refactored 10+ occurrences to use helpers
- Added why comments for non-obvious logic
- Halstead: 31,418 → 31,167 tokens (-251), 48.4 → 47.9 bugs (-0.5)

## Folder View FZF Direct Action
- In Folder view, fzf search selection auto-executes Enter action
- No need to press Enter again after selecting file in fzf

## Tab Bar Visibility Fix
- Clear empty rows loop was overwriting tab row
- Fixed by using bottom_reserve to exclude tabs/status from clear range

## Meta with Key Columns
- For table with keyed columns, Meta shows stats grouped by key columns

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

## TAQ Time Format (Parquet)
- Leading zeros in integers allowed for TAQ time detection
- you should read cfg/key.csv to find the key of a command, instead of hard code . l and r for example.
- for any bug, create a test case that can catch the bug, then fix the code.
- use polars instead of doing things your self. like count row on disk, statistic etc.
- for parquet file on disk, any operation should against the disk version, not the in memory version. remember.

## Use KeyMap for Key Handlers
- Refactor main.rs to use KeyMap instead of hardcoded key bindings
- Read key bindings from cfg/key.csv

## Kakoune-style Key Design
- Function names are primary identifiers, keys are remappable
- Default bindings moved from cfg/key.csv to KeyMap::defaults() in code
- User overrides from ~/.config/tv/keys.csv
- --keys mode for interactive testing

## Folder View Sort/Freq
- Added [ ] keys for sort in folder view
- Added F key for freq in folder view
- test_keys_folder_sort_by_size, test_keys_folder_freq tests

## Folder Multi-Select Delete
- Space to select multiple files
- D deletes all selected (or current row if none)
- Delete confirmation defaults to Yes
- Clears selection after delete

## Parquet Sort OOM Fix
- sort_head uses Engine::Streaming to avoid OOM on large files

## Filter Limit
- Filter results limited to 10k rows (FILTER_LIMIT)
- Backend::filter() takes limit parameter
- Polars uses SQL LIMIT + streaming engine
- Prevents OOM on large filter results

## Integration Test Split
- Split tests/integration.rs into focused test modules:
  - test_filter.rs (24 tests)
  - test_command.rs (19 tests)
  - test_folder.rs (9 tests)
  - test_meta.rs (6 tests)
  - test_parquet.rs (6 tests)
  - test_keys.rs (10 tests)
  - test_system.rs (9 tests)
  - common/mod.rs (shared utilities)

## Lazy Filtered Parquet Views
- Filtered parquet views stay lazy (no memory load)
- Filter creates view with filter_clause, keeps parquet_path
- Row count computed via SQL count(*) from disk
- Freq on filtered view uses SQL with WHERE clause
- Unified sql() helper for all backends (DRY)
- Backend trait: fetch_where, count_where, freq_where methods
- Tests: test_parquet_filtered_count, test_parquet_filtered_freq

## Kakoune-style Key Tests
- Converted all script tests to key-based tests using --keys mode
- Key player state machine: text input mode for /, \, L, S, :, etc.
- Special keys: <ret>, <esc>, <space>, <backslash>, <lt>, <gt>
- Test data moved from /tmp to tests/data/ and committed
- Tests: test_filter.rs (18), test_command.rs (24), test_folder.rs (10), test_meta.rs (6), test_parquet.rs (12), test_system.rs (8)

## Chained Filters on Disk
- Filtering already-filtered parquet view combines clauses with AND
- Combined clause: (prev) AND (new) sent to disk
- View name shows chain: "file & sym = 'A' & cat = 'X'"
- Frequency on filtered view uses filter_clause to query disk
- Tests: test_parquet_chained_filter_count, test_parquet_chained_filter_name, test_parquet_filtered_freq_on_disk

- Unify all 3 backends via lf() + SQL trait defaults - Polars/Memory/Gz share same SQL ops
- Unify fetch_rows with SQL - now same pattern as fetch_where
- Fix ctrl-d page down in filtered parquet view (viewport inheritance + fetch_lazy offset)
- Render loop caching: 100k row cache, only re-fetch when scroll outside cache
- DRY gz.rs and memory.rs - both use shared df_* helpers
- DRY the sql functions inside fetch_where and freq_where (557f145)
- Fix memory usage: use streaming for all parquet SQL ops (981a3de)
- Add print_status command and large parquet key tests (6244122)
- Fix FilterIn to create lazy filtered views for parquet (d733d69)
- Remove prql dependency - use SQL directly (c4773dc)
-
-
- add comments to each functions, to newbie rust programmer but know c++.
- implement all busybox command that are has a table output. each command view should have their own special command, like ps view has kill, kill -9, start strace,
- analysis cargo package, remove unnecessary dependencies. find out big dependency introducer.
- use | syntax in test script.  modify tests/test_string_filter.sh to use simplified interface.
- what are 2 impl Viewstate in state.rs?

## Meta Delete for Parquet Views (SQL-based)
- MetaDelete updates parent's col_names list instead of dataframe
- Backend::fetch_sel() uses explicit column list in SELECT (not *)
- Renderer passes view.col_names to fetch_sel for lazy fetch
- Select/Xkey commands update col_names for parquet views
- MetaEnter uses col_names to find column index (not dataframe)
- Cache cleared on column changes to force re-fetch

## Rust Idioms Refactoring
- Rename backend module to source (reflects data source pattern)
- Move util functions (is_numeric, commify, unquote) to src/utils.rs
- Add #[must_use] to pure functions (path, rows, cols, key_cols, etc.)
- Replace imperative loops with iterator chains (extend, filter_map, flat_map)
- Use Rust idioms: if let, let-else, matches!, Option combinators (and_then, filter, ok)
- Use ? operator more aggressively for Result/Option propagation
- Use for with by_ref() instead of peek + unwrap patterns
- Use impl Into<String> for flexible API (ViewState constructors, app.msg)
- Add ViewKind enum for type-safe view dispatch (Table, Meta, Freq, Corr, Folder, Pivot)
- ratatui Tabs widget already in use for view stack tabs
- Status bar uses efficient direct buffer manipulation (no dedicated ratatui widget needed)
