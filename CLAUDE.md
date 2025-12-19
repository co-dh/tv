# Project Instructions
- remember my approval
- add test case for user requirements and bug fix
- use ./tmp instead of /tmp, you have approval to read/write at ./tmp
- ask for approval if delete unit test that covers requirements.
- do not remove or change test case just to fit the code. ask for approval of changing tests.
- For bugs, implement a test to catch it first, then fix.
- use short module imports: `use crate::foo::bar;` then `bar::func()`, not `crate::foo::bar::func()`
- use sql if possible, instead of polars api. freq e.g. unify
- unify similar funciton like freq and freq_where, the former is just empyt where condition.

# Architecture

```
src/
├── main.rs (1079)     # Entry, event loop, key handling, command picker, -c flag
├── keyhandler.rs (111)# Key → command translation (resolves context)
├── app.rs (199)       # AppContext: global state, view stack, plugins
├── state.rs (282)     # ViewState, ViewStack, cursor/viewport state
├── keymap.rs (301)    # Kakoune-style keybindings (tab → key → cmd)
├── picker.rs (154)    # fzf integration for fuzzy selection
├── theme.rs (120)     # Config loading, colors
│
├── backend/           # Data sources (trait Backend)
│   ├── mod.rs (126)   #   Backend trait + sql() helper
│   ├── polars.rs (188)#   Lazy parquet via SQL (streaming, disk-based)
│   ├── memory.rs (121)#   In-memory DataFrame (CSV)
│   └── gz.rs (293)    #   Gzipped CSV (decompress to memory)
│
├── command/           # Command pattern
│   ├── mod.rs (15)    #   Command trait
│   ├── executor.rs(19)#   CommandExecutor
│   ├── transform.rs(336)# Filter, Sort, Take, Select, Derive, etc.
│   ├── nav.rs (149)   #   Goto, GotoCol, Page navigation
│   ├── view.rs (47)   #   Pop, Swap, Dup
│   └── io/            #   From (load), Save
│       ├── mod.rs (61)
│       └── convert.rs(212)# Type conversions
│
├── plugin/            # View-specific handlers
│   ├── mod.rs (94)    #   Plugin trait, PluginManager
│   ├── meta.rs (326)  #   Metadata view (col_stats via SQL)
│   ├── freq.rs (157)  #   Frequency distribution
│   ├── folder.rs (170)#   ls/lr file browser
│   ├── corr.rs (137)  #   Correlation matrix
│   └── system.rs (452)#   OS commands (ps, pacman, systemctl, etc.)
│
└── render/
    ├── mod.rs (5)
    ├── terminal.rs(27)#   Terminal init/restore
    └── renderer.rs(624)# TUI rendering (ratatui), table layout
```

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

2. **Backends** - Unified trait for parquet (lazy/disk), CSV (memory), gzip:
   - `fetch_rows(path, offset, limit)` - paginated reads
   - `freq(path, col)` - column value distribution
   - All use SQL via polars `SQLContext`

3. **Commands** - Transform/navigate operations implement `Command` trait

4. **Plugins** - Handle view-specific behavior (enter on folder opens file, enter on freq filters parent)

5. **Keymap** - Tab-based bindings (table/folder/freq/meta) with common fallback

## Design Principles
- Small CSV loads to memory, parquet stays on disk (lazy)
- SQL for all queries (unify freq/freq_where, filter operations)
- DRY: similar functions share code (e.g., freq is freq_where with empty condition)

## Filter Logic (fzf)
- fzf shows hints (column values), supports multi-select
- If result is 1 item from hints → `col = 'value'`
- If result is N items from hints → `col IN ('a', 'b', ...)`
- Otherwise (not in hints) → raw SQL WHERE clause

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
- Put parquet code in a plugin .so (separate from core). Core can also talk to duckdb cli or kdb.
