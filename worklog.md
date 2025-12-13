# Worklog

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
