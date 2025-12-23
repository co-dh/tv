# Project Instructions
- the year is almost 2026, do not use 2024 in your web search.
- there is no -c cli, use --keys
- remember my approval
- add test case for user requirements and bug fix
- use ./tmp instead of /tmp, you have approval to read/write at ./tmp
- ask for approval if delete unit test that covers requirements.
- do not remove or change test case just to fit the code. ask for approval of changing tests.
- For bugs, implement a test to catch it first, then fix.
- use short module imports: `use crate::foo::bar;` then `bar::func()`, not `crate::foo::bar::func()`
- unify similar funciton like freq and freq_where, the former is just empyt where condition.
- Add #[must_use] to pure functions (path, rows, cols, key_cols, etc.)
- Replace imperative loops with iterator chains (extend, filter_map, flat_map)
- Use Rust idioms: if let, let-else, matches!, Option combinators (and_then, filter, ok)
- Use ? operator more aggressively for Result/Option propagation
- Use for with by_ref() instead of peek + unwrap patterns
- Use impl Into<String> for flexible API (ViewState constructors, app.msg)
- use sum type
- use prql to implement views, so filter just append filter to current prql, sel is also append to prql.
- prql is essential to the design, do not replace it with sql without approval.
- use sqlite's virtual table for in memory table, like from ls, lr, and other other system source.
-

# Architecture

```
src/
├── main.rs            # Entry, event loop, -c flag
├── app.rs             # AppContext: global state, view stack
├── state.rs           # ViewState, ViewStack, cursor/viewport, PRQL chain
│
├── input/             # Key handling and parsing
│   ├── handler.rs     #   Main dispatch: key → command → exec
│   ├── keyhandler.rs  #   Key → command translation (context-aware)
│   ├── keymap.rs      #   Kakoune-style keybindings (tab → key → cmd)
│   ├── parser.rs      #   Command string → Command object
│   └── prompt.rs      #   fzf prompts: filter, search, command picker
│
├── command/           # Command pattern
│   ├── mod.rs         #   Command trait
│   ├── executor.rs    #   CommandExecutor
│   ├── transform.rs   #   Filter, Sort, Take, Select - all lazy PRQL
│   ├── nav.rs         #   Goto, GotoCol, Page navigation
│   ├── view.rs        #   Pop, Swap, Dup
│   └── io/mod.rs      #   From (load via plugin), Save
│
├── data/              # Data abstraction
│   ├── dynload.rs     #   Plugin loader (C ABI), wraps .so in Rust API
│   └── table.rs       #   Table trait, Cell/ColType, SimpleTable
│
├── plugin/            # View-specific handlers (internal)
│   ├── mod.rs         #   Plugin trait, Registry
│   ├── meta.rs        #   Metadata view (via dynload plugin)
│   ├── freq.rs        #   Frequency distribution (via dynload plugin)
│   ├── folder.rs      #   ls/lr file browser
│   ├── corr.rs        #   Correlation matrix (stub)
│   ├── pivot.rs       #   Pivot table (stub)
│   └── system.rs      #   OS commands (ps, pacman, systemctl, etc.)
│
├── render/
│   ├── terminal.rs    #   Terminal init/restore
│   └── renderer.rs    #   TUI rendering, lazy fetch via dynload
│
├── util/
│   ├── picker.rs      #   fzf integration for fuzzy selection
│   ├── pure.rs        #   Pure functions (PRQL compile, helpers)
│   └── theme.rs       #   Config loading, colors
│
crates/
├── tv-plugin-api/     # C ABI types (PluginVtable, CellValue, etc.)
├── tv-polars/         # Polars plugin (~100MB .so) - parquet/CSV/gzip
└── tv-sqlite/         # SQLite plugin (~2MB .so) - in-memory tables, sources
```

## Plugin Architecture
```
main.rs → dynload::get() → Plugin { vt: PluginVtable }
                              ├── query(sql, path) → PluginTable
                              ├── fetch(path, offset, limit)
                              ├── fetch_where(path, filter, offset, limit)
                              ├── count(path), count_where(path, filter)
                              ├── freq(path, cols, filter)
                              ├── distinct(path, col)
                              └── schema(path)
```

## PRQL Chain
All transforms are lazy PRQL that get appended to ViewState.prql:
- Filter: `{prql} | filter {expr}`
- Sort: `{prql} | sort {col}` or `{prql} | sort {-col}` (desc)
- Select: `{prql} | select {col1, col2}`
- Take: `{prql} | take {n}`
- Derive: `{prql} | derive {new = old}`

PRQL compiles to SQL for execution via plugin.

## Command Flow
```
KeyEvent → key_str() → keymap.get_command() → keyhandler::to_cmd() → parse() → exec()
                                                    ↓ (None for interactive)
                                              handle_cmd() with prompts
```

## CLI Modes
- TUI: `tv file.parquet` - interactive table viewer
- Script: `tv --script script.tv` - run commands from file
- Inline: `tv -c "from data.csv filter x > 5"` - run commands directly
- Keys: `tv --keys "F<ret>" file` - replay key sequence (testing)

## Key Patterns

1. **View Stack** - Each operation pushes a new view (freq, filter, meta). `q` pops back.

2. **Lazy Loading** - Views store PRQL chain, data fetched on render via plugin

3. **Commands** - Transform operations implement Command trait, append to PRQL

4. **Plugins (internal)** - Handle view-specific behavior (enter on folder opens file)

5. **Keymap** - Tab-based bindings (table/folder/freq/meta) with common fallback

## Design Principles
- Main crate has no polars dependency - all data ops via plugin .so
- Small CSV loads to memory, parquet stays on disk (lazy)
- PRQL for query chaining, compiles to SQL for plugin execution
- DRY: similar functions share code (e.g., freq is freq_where with empty filter)

## Filter Logic (fzf)
- fzf shows hints (column values), supports multi-select
- If result is 1 item from hints → `col = 'value'`
- If result is N items from hints → `col IN ('a', 'b', ...)`
- Otherwise (not in hints) → raw SQL WHERE clause

# Testing
- Use unique static IDs (10000+) for `new_memory()` in tests to avoid parallel test conflicts with sqlite registry

# Known Issues (Fixed)
- Space key in TUI must map to `<space>` in key_str(), not `" "`. Keymap expects `<space>`.

# Freq Behavior
- `!` toggles selected columns (or current col) as key columns
- `F` freq: GROUP BY key columns (if set) or current column
- Aggregates (min/max/sum) computed for selected columns, or current column if no selection
- Key columns excluded from aggregation

# Idea
GPU? cache meta.

# Todo
- load tests/data/nyse/1.parquet M0D is not working, M1<ret> neither.
- :cargo background fetch still leaks package descriptions to terminal (setsid not enough)
- Add is_loading() to plugin interface so main crate knows if plugin is still loading
