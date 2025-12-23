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
├── main.rs            # Entry, event loop, --keys flag
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
│   └── io/mod.rs      #   From (load file), Save
│
├── data/              # Data layer (ADBC+DuckDB)
│   ├── backend.rs     #   DuckDB via ADBC: query, save, register_table
│   ├── source.rs      #   Source generators: ls, lr, ps, pacman, etc.
│   └── table.rs       #   Table trait, Cell/ColType, SimpleTable
│
├── plugin/            # View-specific handlers
│   ├── mod.rs         #   Plugin trait, Registry
│   ├── meta.rs        #   Metadata view (column stats)
│   ├── freq.rs        #   Frequency distribution
│   ├── folder.rs      #   ls/lr file browser, bat viewer
│   ├── corr.rs        #   Correlation matrix
│   └── system.rs      #   OS commands (ps, pacman, systemctl)
│
├── render/
│   ├── terminal.rs    #   Terminal init/restore
│   └── renderer.rs    #   TUI rendering, status bar
│
└── util/
    ├── picker.rs      #   fzf integration for fuzzy selection
    ├── pure.rs        #   Pure functions (PRQL compile, helpers)
    └── theme.rs       #   Config loading, colors
```

## Data Backend (ADBC+DuckDB)
```
backend.rs
├── query(prql, path) → Box<dyn Table>  # Cached, PRQL→SQL→DuckDB
├── save(prql, path_in, path_out)       # Export to CSV/Parquet
├── register_table(id, data) → "mem:id" # In-memory table for views
├── unregister_table(id)                # Free on view drop
└── compile_prql(prql) → SQL            # With built-in funcs (freq, stats, etc.)

source.rs
├── source:ls:{path}   # Directory listing
├── source:lr:{path}   # Recursive listing
├── source:ps          # Process list
├── source:pacman      # Installed packages
├── source:systemctl   # Systemd services
└── source:mounts      # Mount points
```

## PRQL Chain
All transforms are lazy PRQL that get appended to ViewState.prql:
- Filter: `{prql} | filter {expr}`
- Sort: `{prql} | sort {col}` or `{prql} | sort {-col}` (desc)
- Select: `{prql} | select {col1, col2}`
- Take: `{prql} | take {n}`
- Derive: `{prql} | derive {new = old}`

Built-in PRQL functions (defined in backend.rs):
- `freq{col}` - frequency count with Cnt, Pct, Bar
- `stats col` - n, min, max, avg, std
- `cnt` - row count
- `uniq{col}` - distinct values
- `meta col` - count, distinct, total, min, max

PRQL compiles to SQL for execution via DuckDB.

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
- Single binary with ADBC+DuckDB (libduckdb.so at runtime)
- Parquet/CSV/JSON read directly via DuckDB (lazy, no memory load)
- PRQL for query chaining, compiles to SQL for DuckDB execution
- Source paths (ls, ps, pacman) generate SQL via source.rs
- Views freed on pop (Drop calls unregister_table)

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
