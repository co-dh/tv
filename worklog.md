# Worklog

## 2025-12-16: Dependency Deduplication - rustix Unified

### Problem
22 duplicate dependencies, including `linux-raw-sys` (0.4.15 and 0.11.0) via two `rustix` versions.

### Root Cause (Before)
```
ratatui 0.29 → crossterm 0.28 → rustix 0.38 → linux-raw-sys 0.4
polars → polars-io → fs4 → rustix 1.1 → linux-raw-sys 0.11
```

### Solution
1. **Removed direct crossterm dep** - use `ratatui::crossterm` re-export
2. **Updated ratatui 0.29 → 0.30.0-beta.0** - uses crossterm 0.29 → rustix 1.x
3. **Disabled polars `fmt` feature** - removed comfy-table dependency chain
4. **Custom print()** - outputs CSV format without polars Display trait

### Results
| Metric | Before | After |
|--------|--------|-------|
| Duplicates | 22 | 10 |
| rustix | 0.38 + 1.1 | **1.1 only** |
| linux-raw-sys | 0.4 + 0.11 | **0.11 only** |
| Binary | 97MB | 97MB |

### Remaining Duplicates (polars internals)
- foldhash, getrandom, hashbrown, itertools, libc, memchr

### Why Binary Size Unchanged
polars dominates everything:

| Crate | rlib Size |
|-------|-----------|
| polars_core | 65MB |
| sqlparser | 55MB |
| polars_expr | 29MB |
| polars_ops | 27MB |
| **Total polars** | **~176MB** |

vs removed duplicates:

| Crate | rlib Size |
|-------|-----------|
| rustix (was 2 copies) | ~5MB each |
| linux-raw-sys | <1MB |

The ~5MB saved from removing rustix duplicate is <1% of the 97MB binary - lost in link-time optimization.

### Perspective: kdb+/q

| Engine | Binary Size | Ratio |
|--------|-------------|-------|
| kdb+/q | **870KB** | 1x |
| polars | 97MB | 114x |
| DuckDB | 152MB | 179x |

870KB handles petabyte-scale time-series data, vector operations, SQL, IPC, persistence - everything. Written in K, which Arthur Whitney wrote in C. Every character earns its place.

### Files Changed
- `Cargo.toml` - ratatui 0.30.0-beta.0, removed crossterm
- `src/*.rs` - `use ratatui::crossterm::*` instead of `use crossterm::*`
- `src/main.rs` - custom `print()` function (no polars fmt)
- `tests/test_keys.rs` - updated assertions for CSV output format

---

## 2025-12-16: Frequency View Aggregates

### Feature
Frequency view (F key) now shows min/max/sum for all numeric columns, not just count.

| Column | Description |
|--------|-------------|
| col | Grouping column value |
| Cnt | Count of rows |
| x_min | Minimum of numeric column x |
| x_max | Maximum of numeric column x |
| x_sum | Sum of numeric column x |
| Pct | Percentage of total |
| Bar | Visual bar |

### Implementation
- `freq_agg()` in Backend trait builds GROUP BY with MIN/MAX/SUM for each numeric column
- `freq_agg_df()` in freq.rs handles in-memory DataFrames
- Runs synchronously (bg thread had rayon deadlock with polars)

### Files
- `src/backend/mod.rs` - freq_agg() method
- `src/plugin/freq.rs` - Frequency command updated, freq_agg_df() helper
- `src/backend/memory.rs` - test_memory_freq_agg, test_freq_agg_bg_thread

### Known Issue
Background thread execution causes rayon deadlock with polars. Unit tests pass but integration with TUI event loop deadlocks. Running synchronously for now.

---

## 2025-12-16: Cargo Dependencies Command

### New Command
Added `:cargo` to analyze current project's Cargo.toml dependencies (like pacman for Rust).

| Column | Description |
|--------|-------------|
| name | Package name |
| version | Current version in use |
| latest | Latest version on crates.io (cached) |
| size(k) | Source size in KB |
| rsize(k) | Removal size (pkg + exclusive deps) |
| deps | Number of dependencies |
| req_by | Number of packages depending on this |
| platform | linux/windows/macos/android/wasm or empty |

### Implementation
- Uses `cargo metadata --format-version 1` for package info
- Uses `cargo metadata --filter-platform x86_64-unknown-linux-gnu` to detect linux-compiled packages
- Platform detection: infers from package name (windows-sys→windows, core-foundation→macos, etc.)
- Latest version: fetches via `cargo search` in background thread

### Background Version Fetching
- Cache stored at `~/.cache/tv/cargo_versions.csv`
- Format: `name,version,timestamp`
- Re-fetches entries older than 1 day
- Saves every 10 fetches (incremental)
- Fully detached from terminal via `setsid()` to avoid garbage on quit

### Dependencies Added
- `serde_json = "1.0"` - parse cargo metadata JSON
- `nix` feature `process` - for setsid() to detach background processes

### Files
- `src/plugin/system.rs` - cargo(), fetch_latest(), load_ver_cache(), save_ver_cache(), update_ver_cache_bg()
- `src/main.rs` - add "cargo" to command picker
- `Cargo.toml` - add serde_json, nix process feature
- `tests/test_system.rs` - test_cargo_command

---

## 2025-12-15: SQL-based Stats Unification

### Problem
- `compute_stats()`, `pq_stats()`, `grp_stats()` had separate implementations
- Polars API streaming was slow for large parquet (42.74s for 304M rows)
- Code duplication across 3 stats functions

### Solution
Unified all stats computation via single `col_stats()` function using SQL:
```rust
fn col_stats(lf: LazyFrame, col: &str, n: f64, is_num: bool) -> ColStats
```

### Performance (304M rows, 30 cols)
| Version | Time |
|---------|------|
| SQL-based | **23.35s** |
| Polars API | 42.74s |

**1.8x faster** with SQL approach.

### Changes
- `col_stats()` - SQL query for nulls, distinct, min, max, mean, std
- Separate queries for numeric vs string columns (AVG/STDDEV fail on strings)
- `is_numeric()`, `is_numeric_str()` - type checking helpers
- Removed unused `get_f64()`, `get_u32()`, `get_str()` helpers
- All 3 stats functions now use same code path

### Halstead Metrics
| File | Before | After | Δ len | Δ bugs |
|------|--------|-------|-------|--------|
| meta.rs | 3361 | 2765 | -596 | -1.13 |
| **Total** | 36885 | 36289 | **-596** | **-1.13** |

### Refactoring
- Removed BG_THRESHOLD - always use background compute
- Unified 3-way branch → 2 branches (grouped vs non-grouped)
- `lf_stats()` - in-memory df via LazyFrame + SQL
- `lf_stats_path()` - parquet via LazyFrame + SQL
- `grp_stats()` - grouped stats (needs in-memory df)

### Files
- `src/plugin/meta.rs` - unified stats via col_stats()

---

## 2025-12-15: DRY Refactoring & Feature Comparison

### Halstead Metrics
| File | Before | After | Δ len | Δ bugs |
|------|--------|-------|-------|--------|
| gz.rs | 2106 | 2092 | -14 | -0.06 |
| meta.rs | 3464 | 3361 | -103 | -0.47 |
| system.rs | 4078 | 4034 | -44 | -0.34 |
| **Total** | 37046 | 36885 | **-161** | **-0.87** |

### DRY Helpers Extracted
- **gz.rs**: `require_complete()` - partial-data check (6 uses)
- **meta.rs**: `stats_df()`, `placeholder_df()` - DataFrame construction (5 uses)
- **system.rs**: `run_cmd()` - command execution (5 uses), `parse_deps()`, `calc_rsize()`, `push_pkg` closure

### Code Consolidation
- Merged `os.rs` into `plugin/system.rs` (deleted 562 lines, all OS functions now in one place)
- Removed low-output OS commands: df, lsblk, who

### Feature Comparison Docs
- `visidata.md` - VisiData vs tv comparison, nested JSON handling
- `nushell.md` - Nushell vs tv comparison

---

## 2025-12-15: OS Commands & Rendering Fixes

### New Commands
Added systemctl, journalctl, pacman to `:` command picker.

| Command | Columns |
|---------|---------|
| `systemctl` | unit, load, active, sub, description |
| `journalctl [n]` | time, host, unit, message |
| `pacman` | name, version, size, deps, req_by, orphan, reason, installed, description |

### Pacman Enhancements
- `deps` - number of dependencies
- `req_by` - number of packages requiring this
- `orphan` - "x" if orphaned (dep no longer needed, from `pacman -Qdt`)
- `reason` - "dep" or "explicit"

### Rendering Fixes
- **Last column fills screen**: Expands to remaining width (cap 200 chars)
- **Unicode crash fix**: Use `.chars().take(n)` instead of byte-slicing `&s[..n]`
  - Crashed on pacman descriptions with fancy quotes like `"`

### Removed
- `-c` command mode (unused, only `--keys` test mode remains)

### Tests Added
- `test_pacman_command` - basic pacman view
- `test_pacman_sort_deps` - sort ascending on deps
- `test_pacman_sort_deps_desc` - sort descending
- `test_pacman_sort_unicode_description` - unicode in description
- `test_systemctl_command` - systemctl view
- `test_journalctl_command` - journalctl view

### Files
- `src/os.rs` - systemctl(), journalctl(), pacman()
- `src/plugin/system.rs` - register commands
- `src/main.rs` - add to `:` picker, remove `-c`
- `src/render/renderer.rs` - last col fills screen, unicode fix
- `tests/test_system.rs` - 6 new tests
- `unix.md` - command list (50 unix commands with table output)

---

## 2025-12-15: Backend Unification & Dependency Analysis

### Backend Refactoring
Unified all 3 backends (Polars, Memory, Gz) to use single SQL path via `lf()` trait method.

**Before:** Each backend had custom implementations for cols, schema, metadata, fetch, freq, filter, sort, distinct.

**After:** Backend trait requires only `lf()` - all operations use SQL defaults:
- `metadata()` - `SELECT COUNT(*) FROM df` + schema
- `cols()` - `lf().collect_schema()`
- `schema()` - `lf().collect_schema()` with types
- `fetch_rows/where()` - `SELECT * LIMIT OFFSET`
- `freq/freq_where()` - `SELECT col, COUNT(*) GROUP BY`
- `filter()` - `SELECT * WHERE`
- `sort_head()` - `SELECT * ORDER BY LIMIT`
- `distinct()` - `SELECT DISTINCT`
- `count_where()` - `SELECT COUNT(*) WHERE`

**Commits:**
- `3fb3606` Unify backends via lf() + SQL trait defaults
- `2856742` Backend: metadata() via SQL, remove convert_epoch_cols
- `a3439b9` Add 100k row cache and fix filtered view page down
- `2d25332` DRY ViewState constructors with base() helper

### Dependency Analysis

| Dependency | Version | Usage | Status |
|------------|---------|-------|--------|
| polars | 0.52 | Core dataframe engine | Required (391 deps, 96MB) |
| polars-ops | 0.52 | pearson_corr for correlation | Already transitive dep |
| crossterm | 0.28 | Terminal I/O | Required |
| ratatui | 0.29 | TUI framework | Required |
| anyhow | 1.0 | Error handling | Required |
| chrono | 0.4 | 3 date parses | Already polars transitive |
| dirs | 5 | 1 home_dir() call | Tiny, keep |
| nix | 0.29 | 1 statvfs call | Tiny, keep |

**Findings:**
- 391 total dependencies, ~380 from polars
- Binary size: 96MB (polars dominates)
- Removed unused "strings" polars feature (no size change - already transitive)
- polars-ops already pulled by polars, only adds "cov" feature for correlation
- dirs/nix are <1KB code each, not worth removing
- chrono already a polars transitive dependency

**Conclusion:** Dependencies are minimal. Polars is unavoidable size driver for dataframe operations.

---

## 2025-12-15: Lazy Filtered Parquet Views

### Problem
Filter on parquet loaded entire filtered result into memory, losing lazy benefits.
Freq on filtered view ran against in-memory data, not disk.

### Solution
- Add `filter_clause` field to ViewState for lazy filtered views
- Add unified `sql()` helper in `backend/mod.rs` (DRY SQL execution)
- Add Backend trait methods: `fetch_where`, `count_where`, `freq_where`
- Implement in all backends (Polars, Memory, Gz) using same SQL path

### Changes
- Filter on parquet creates lazy view (keeps `parquet_path` + `filter_clause`)
- Row count computed via `SELECT COUNT(*) WHERE filter` from disk
- Viewport fetched via `SELECT * WHERE filter LIMIT/OFFSET` from disk
- Freq on filtered view uses `SELECT col, COUNT(*) WHERE filter GROUP BY`
- Pre-generated test parquet files in `tests/data/` (filtered_test, freq_test, sort_test, meta_test)

### Files
- `src/backend/mod.rs` - `sql()` helper, trait methods
- `src/backend/polars.rs` - `lf()` helper, implementations
- `src/backend/memory.rs` - implementations using `sql()`
- `src/backend/gz.rs` - implementations (blocked if partial)
- `src/state.rs` - `filter_clause` field, `new_filtered()` constructor
- `src/command/transform.rs` - Filter creates lazy view for parquet
- `src/plugin/freq.rs` - uses `freq_where` when filter_clause set
- `src/render/renderer.rs` - uses `fetch_where` for filtered views
- `tests/data/*.parquet` - pre-generated test files

---

## 2025-12-15: Split Integration Tests by View

### Changes
Split `tests/integration.rs` (1386 lines, 105 tests) into focused test modules:

```
tests/
├── common/mod.rs     - Shared utilities (unique_id, run_script, run_keys, setup_test_csv)
├── test_filter.rs    - Filter command tests (24 tests)
├── test_command.rs   - Command tests (freq, sort, select, save, xkey - 19 tests)
├── test_folder.rs    - Folder view tests (ls, delete - 9 tests)
├── test_meta.rs      - Meta view tests (6 tests)
├── test_parquet.rs   - Parquet backend tests (6 tests)
├── test_keys.rs      - Key play tests (10 tests)
└── test_system.rs    - System tests (ps, forth funcs, history - 9 tests)
```

### Benefits
- Each test file is focused and easier to navigate
- Can run tests by category: `cargo test test_filter`
- Parallel test execution per file
- Easier to maintain and extend

---

## 2025-12-14: Consolidate I/O into Backend

### Analysis
- `io/parquet.rs` duplicated `backend/polars.rs` (both read parquet metadata, use LazyFrame)
- `csv.rs` (57 lines): detect_sep, parse_buf, load, save
- `gz.rs` (284 lines): background streaming, memory-limited loading, gz→parquet conversion

### Decision
- Move parquet functions to Backend trait (metadata, fetch_rows, distinct, save)
- Move CSV functions to backend/polars.rs (detect_sep, load_csv, save_csv, parse_csv_buf)
- Move gz.rs to backend/gz.rs as Gz backend (refuses expensive ops on partial data)
- Remove csv.rs - polars handles CSV natively
- Let polars fail on ragged lines (no more truncate_ragged_lines)

### Changes
- Backend trait: +4 methods (metadata, fetch_rows, distinct, save)
- `backend/polars.rs`: +CSV functions (detect_sep, load_csv, save_csv, parse_csv_buf)
- `backend/gz.rs`: Gz backend - refuses freq/filter/distinct on partial (memory-limited) data
- Deleted `io/parquet.rs`, `io/csv.rs`, `io/gz.rs`
- ViewState::backend() returns Gz when gz_source is set

### Architecture
```
src/backend/
├── mod.rs      - Backend trait (8 methods)
├── polars.rs   - Polars impl + CSV/parquet helpers
├── gz.rs       - Gz backend (streaming load, refuses expensive ops if partial)
└── memory.rs   - In-memory impl

src/command/io/
├── mod.rs      - From/Save commands
└── convert.rs  - epoch conversion
```

### Investigation: polars pipe support
**Q:** Can polars scan_csv from zcat pipe/fifo?
**A:** No. Polars requires `MmapBytesReader` trait. Pipes (ChildStdout) don't implement it.
Must buffer to `Cursor<Vec<u8>>` first - which is what gz.rs already does.
Cannot simplify further.

---

## 2025-12-14: Simplify to Polars-only Backend

### Benchmark Results (8 threads, 3.7GB parquet)
| Operation | Polars | DuckApi | DuckDB Raw |
|-----------|--------|---------|------------|
| freq      | 0.581s | 0.345s  | 0.127s     |
| filter    | 0.200s | 0.574s  | 0.153s     |
| count     | 0.159s | 0.161s  | 0.064s     |
| head 100k | 0.164s | 0.159s  | 0.180s     |
| meta      | 0.229s | 0.291s  | 0.058s     |

### Analysis
- **DuckApi**: Only wins at freq (1.7x faster), loses at filter (2.9x slower due to Arrow transfer of 1.2M rows)
- **DuckCli**: 10x slower at filter due to CSV serialization overhead
- **Polars**: Best overall balance, streaming handles large results well

### Decision
Remove DuckDB backends (duckapi, duckcli) - complexity not justified:
- DuckApi Arrow transfer slow for large results
- DuckCli CSV serialization too slow
- Polars handles all operations reasonably well
- Simpler codebase with single backend

### Changes
- Removed `src/backend/duckapi.rs`
- Removed `src/backend/duckcli.rs`
- Removed `--duckapi` CLI flag
- Removed `duckdb` crate from Cargo.toml
- Fixed keymap fallback: freq/meta/corr views now inherit table keys (sort works)
- Disabled LTO/strip for faster builds
- Binary: 108MB (was 152MB with duckdb+LTO)
- Build: 3s incremental (was 4-7min with duckdb)

---

## 2025-12-14: Backend Trait & DuckDB Integration

### Commits
- `791f836` - Remove duplicate freq_from_disk, use Backend trait
- `dde7798` - Fix backend flag ignored with -c, Arrow conversion
- `04a94d0` - Add 3 backend options: polars, duckapi, duckcli
- `8778c6f` - Remove unused code
- `e7ba58e` - Fix polars 0.52 API changes
- `9f53404` - Add Backend trait for polars/duckdb switching

### Changes
- **Backend trait**: Abstract interface for data engines
  - `Polars` - streaming engine with LazyFrame
  - `DuckApi` - native duckdb crate with Arrow transfer
  - `DuckCli` - shell out to duckdb CLI, parse CSV output
  - CLI flags: `--duckapi`, `--duckcli` (default: polars)

- **Arrow conversion**: Fast DuckDB to polars transfer
  - Use `query_arrow()` instead of row-by-row iteration
  - Handle Int32/Int64/Float64/Utf8/LargeUtf8 types
  - 2x faster than polars for freq on 3.7GB parquet

- **Bug fix**: Backend flag was ignored with `-c`
  - Parse `--duckapi`/`--duckcli` before `-c` check
  - Pass `BackendType` to `run_commands()`/`run_batch()`

- **Code cleanup**: Removed 584+ lines of unused code
  - Deleted `src/connector.rs` (unused module)
  - Deleted `src/command/io/duckdb.rs` (replaced by backend::DuckApi)
  - Removed duplicate `freq_from_disk` from parquet.rs
  - Consolidated all duckdb queries to use Arrow-based Backend trait

### Performance (8 threads, 3.7GB parquet)
| Operation | Polars | DuckApi | Raw DuckDB |
|-----------|--------|---------|------------|
| freq      | 0.59s  | 0.33s   | 0.13s      |
| filter    | 0.21s  | 0.58s   | 0.15s      |
| count     | 0.18s  | 0.18s   | 0.06s      |

### Files Modified
- `src/backend/` - Backend module split into separate files:
  - `mod.rs` - Backend trait with freq, filter, freq_df methods
  - `polars.rs` - Polars streaming engine for parquet
  - `duckapi.rs` - DuckDB API with Arrow transfer
  - `duckcli.rs` - DuckDB CLI backend
  - `memory.rs` - In-memory DataFrame operations
- `src/main.rs` - CLI flag parsing, pass backend to batch
- `src/app.rs` - backend field, set_backend()
- `src/plugin/freq.rs` - use backend.freq() and freq_df()
- `tools/bench.sh` - benchmark script for backends

## 2025-12-13: Performance, Rendering & xkey

### Commits
- `635250b` - Migrate renderer to ratatui for flicker-free updates
- `7175298` - Add TAQ time format conversion (HHMMSSNNNNNNNN)
- `2bd3ab9` - Auto-convert epoch integers to datetime in columns with time-like names
- `68a0be0` - Fix streaming gz save with TAQ time columns
- `5c5545f` - Add tests/data/ to gitignore for large test files

### Changes
- **Ratatui migration**: Replaced raw crossterm rendering with ratatui's diff-based terminal update
  - Eliminates screen flicker when moving cursor
  - Uses `Terminal<CrosstermBackend>` with `frame.draw()` API
  - New `src/render/terminal.rs` for terminal lifecycle

- **Background gz streaming**: Large .csv.gz files now stream in background
  - Shows first 1,000 rows immediately
  - Continues loading in background up to 10% of system memory
  - Configurable via `gz_mem_pct` in `cfg/config.csv`
  - Uses mpsc channels for thread communication

- **Stats caching**: Column statistics cached to avoid expensive recomputation
  - `n_unique()` and `value_counts()` only called when column changes
  - Makes scrolling through 5M+ rows responsive

- **Streaming save fix**: Fixed "could not find an appropriate format to parse times" error
  - Don't use converted schema for streaming save
  - Apply `convert_epoch_cols` per chunk during streaming

- **xkey command**: Move columns to front as key columns
  - `xkey col1,col2` reorders dataframe with specified columns first
  - Draws vertical separator bar after key columns
  - Selects the key columns
  - In Meta view, Enter on multiple selected rows applies xkey

- **Command history**: Log commands to `~/.tv/history`
  - Creates `~/.tv/` directory if needed
  - Appends commands via executor

- **Delete feedback**: Shows "N columns deleted" message

- **View handler refactor**: Extracted view-specific logic to modules
  - New `src/view/` module with ViewKind enum and ViewHandler trait
  - `meta.rs` - MetaEnter, MetaDelete commands
  - `freq.rs` - FreqEnter command
  - `folder.rs` - opens file on Enter
  - `table.rs` - placeholder for table-specific commands
  - Dispatch system routes commands to view handlers

### Files Modified
- `src/render/mod.rs` - new module with init/restore
- `src/render/terminal.rs` - ratatui terminal lifecycle
- `src/render/renderer.rs` - Frame-based rendering, stats cache
- `src/command/io.rs` - background streaming, streaming save fix
- `src/app.rs` - bg_loader field, merge_bg_data()
- `src/state.rs` - stats_cache field
- `src/os.rs` - mem_total() function
- `src/main.rs` - polling loop with 100ms timeout
- `cfg/config.csv` - gz_mem_pct setting

## 2025-12-12: Arthur Whitney Style Refactor

### Commits
- `892e1a3` - Use SQL WHERE syntax for filter command
- `87077e8` - Add multi-column support to delcol command
- `daac2cb` - Remove DelNull/DelSingle commands (use 0/1 keys + D instead)
- `92e2110` - Refactor transform.rs: leverage polars, reduce duplication
- `6162c7e` - Remove unnecessary success messages (silence is golden)
- `e26a802` - Refactor: Arthur Whitney style with short names
- `b1646bd` - Inline single-use variables in renderer.rs
- `ea84d1f` - Remove dead code
- `00722dd` - DRY refactor: reduce code duplication and complexity

### Style Rules Applied
1. Use short names (e.g., `exec` for `execute`)
2. Arthur Whitney style: terse, dense code with inline comments
3. Local variables 1-letter when obvious from context
4. Don't introduce local variables used only once - inline them
5. Silence is golden - no messages when everything works
6. Leverage polars library instead of implementing manually

### Method Renamings
| Before | After |
|--------|-------|
| `execute` | `exec` |
| `to_command_string` | `to_str` |
| `should_record` | `record` |
| `current_view` | `view` |
| `current_view_mut` | `view_mut` |
| `require_view` | `req` |
| `require_view_mut` | `req_mut` |
| `record_command` | `record` |
| `set_message` | `msg` |
| `set_error` | `err` |
| `update_viewport` | `viewport` |
| `page_size` | `page` |
| `needs_width_recalc` | `need_widths` |
| `current_column` | `cur_col` |
| `move_down/up/left/right` | `down/up/left/right` |
| `goto_top/bottom` | `top/bot` |
| `ensure_visible` | `visible` |
| `row_count/col_count` | `rows/cols` |
| `add_to_history` | `add_hist` |
| `new_frequency` | `new_freq` |
| `with_initial` | `init` |
| `find_by_id` | `find` |
| `swap_top` | `swap` |
| `input_with_hints` | `input` |
| `widths_calc_row` | `widths_row` |

### Halstead Metrics
| Metric | Start | After Style | After Silent | After Polars | After SQL | Unified | No Regex | Polars Corr | Commands | Total Change |
|--------|-------|-------------|--------------|--------------|-----------|---------|----------|-------------|----------|--------------|
| Length | 12,664 | 12,427 | 12,122 | 11,698 | 11,003 | 10,638 | 9,904 | 9,622 | 10,284 | -2,380 (-18.8%) |
| Bugs | 20.808 | 20.525 | 19.950 | 19.054 | 17.897 | 16.997 | 15.557 | 14.764 | 16.166 | -4.642 (-22.3%) |

### Architecture
- Only command executor can modify stack (push/pop/swap)
- Key handlers send commands, not direct stack manipulation
- New commands: Pop, Swap, Dup, Ls, Lr, Agg, FilterIn
- New module: `src/os.rs` for directory operations

### Line Count
- 11 files changed
- +440 / -814 lines
- Net: -374 lines (-32%)

### Files Modified
- `src/app.rs` - terse one-liners, short method names
- `src/state.rs` - compact TableState/ViewState/StateStack
- `src/picker.rs` - condensed skim wrappers
- `src/command/mod.rs` - minimal Command trait
- `src/command/executor.rs` - 10-line exec function
- `src/command/io.rs` - Load/Save commands
- `src/command/transform.rs` - Filter/Select/Sort/etc
- `src/command/view.rs` - Frequency/Metadata/Correlation
- `src/main.rs` - key handlers, helpers
- `src/render/renderer.rs` - updated method calls

### Tools
- `tool/measure.py` - Halstead metrics measurement script

## 2024-12-14: Backend refactoring & binary optimization

### Backend moved from app to view level
- Each view now owns its backend via `ViewState::backend()`
- File-based views (parquet) use Polars/DuckApi/DuckCli
- In-memory views (ls, ps, csv) use Memory backend
- Simplified freq.rs, transform.rs, meta.rs - no more if/else for file vs memory

### Memory backend simplified to newtype
- `Memory<'a>(pub &'a DataFrame, pub Vec<String>)` - tuple struct (df, keys)
- Added Rust documentation for newbie programmers
- Explains lifetimes, tuple struct access, ? operator, map_err, lazy/collect

### Binary size reduced
- Added release profile: lto=true, strip=true, codegen-units=1
- Binary: 152MB → 95MB (38% reduction)
- Startup time: 3ms (unchanged, fast)

### Documentation
- Documented all backend module functions
- Added module-level docs explaining 4 backends and usage pattern
