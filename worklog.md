# Worklog

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
