# tv - Terminal Table Viewer

A fast, interactive terminal-based table viewer for CSV and Parquet files, built with Rust.

## Installation

```bash
cargo build --release
```

The binary will be at `./target/release/tv`.

## Usage

```bash
# Open a file
tv data.csv
tv data.parquet
tv large.csv.gz  # streams 1k rows instantly, continues loading in background

# Run inline commands (PRQL/SQL syntax)
tv -c "from data.csv | filter age > 30 | save filtered.parquet"
tv -c "from data.csv | filter (name | text.starts_with 'A') | select name,age"

# Run script file
tv --script commands.txt
```

## Keyboard Shortcuts

### Navigation

| Key | Action |
|-----|--------|
| `Up/Down` | Move cursor up/down one row |
| `Left/Right` | Move cursor left/right one column |
| `g` | Go to first row |
| `G` | Go to last row |
| `Ctrl+D` / `PageDown` | Page down |
| `Ctrl+U` / `PageUp` | Page up |
| `Home` | Go to first row |
| `End` | Go to last row |
| `:` | Jump to row number |
| `@` | Jump to column by name (fuzzy finder) |
| `m` | Toggle bookmark on current row |
| `'` | Jump to next bookmark (cycles) |

### Search & Filter

Both search and filter use **PRQL/SQL syntax**:

| Key | Action |
|-----|--------|
| `/` | Search (navigate to matching row) |
| `\` | Filter (create new view with matching rows) |
| `n` | Find next match |
| `N` | Find previous match |
| `*` | Search for current cell value |

PRQL syntax:
- `col == 100` - Numeric equality
- `col == 'NYC'` - String equality
- `col > 100`, `col <= 50` - Comparisons
- `(col | text.starts_with 'abc')` - Starts with
- `(col | text.ends_with 'abc')` - Ends with
- `(col | text.contains 'abc')` - Contains
- `col >= @2020-01-01 && col < @2021-01-01` - Date range
- `col > 10 && col < 100` - Combined conditions (use `&&` for AND, `||` for OR)

SQL syntax also supported:
- `col LIKE 'abc%'` - Starts with
- `col BETWEEN 10 AND 100` - Range
- `col IN ('a', 'b', 'c')` - In list

Filtering pushes a new view onto the stack. Press `q` to return.

### Column Operations

| Key | Action |
|-----|--------|
| `Space` | Toggle selection (columns in normal view, rows in Meta/Freq views) |
| `Esc` | Clear selection |
| `0` | Select all-null columns (in Meta: select rows with 100% null) |
| `1` | Select single-value columns (in Meta: select rows with 1 distinct) |
| `c` | Copy current column |
| `D` | Delete selected columns (in Meta view: deletes from parent table) |
| `^` | Rename current column |
| `$` | Convert column type (String/Int64/Float64/Boolean) |
| `s` | Select columns (comma-separated) |
| `[` | Sort ascending by current column |
| `]` | Sort descending by current column |

### Views

| Key | Action |
|-----|--------|
| `F` | Frequency table for current column |
| `M` | Metadata/data profile (column types, null%, distinct, min/max, median, sigma) |
| `C` | Correlation matrix (selected columns if >=2, else all numeric) |
| `b` | Aggregate by current column (count/sum/mean/min/max/std) |
| `T` | Duplicate current view |
| `W` | Swap top two views on stack |
| `q` | Pop view from stack (quit if only one view) |

### File Operations

| Key | Action |
|-----|--------|
| `L` | Load file |
| `S` | Save to file |
| `l` | List current directory (name, size, modified, dir) |
| `r` | List directory recursively (path, size, modified, dir) |

### Display

| Key | Action |
|-----|--------|
| `I` | Toggle info box |
| `,` | Decrease float decimal places |
| `.` | Increase float decimal places |

Numbers display with comma separators (e.g., 1,000,000). Floats show 3 decimal places by default.

### Other

| Key | Action |
|-----|--------|
| `Ctrl+C` | Force quit |
| `Enter` | (In Freq view) Filter parent by selected value(s); (In Meta view) Focus/xkey selected column(s) |

In Frequency view: use `Space` to select multiple values, then `Enter` to filter parent table by all selected values.

In Meta view: `Enter` on single row focuses that column in parent; multiple selected rows applies `xkey` to move them to front with separator.

## Status Bar

The status bar shows:
- Left: View name and column info
- Center: Column statistics
  - Categorical columns: null%, distinct count, mode
  - Numerical columns: min, mean, max, std
- Right: Current row / total rows

## View Stack

tv uses a view stack for navigation:
- Operations like `F` (frequency), `\` (filter), `b` (aggregate) push new views
- Press `q` to pop back to the previous view
- Press `q` on the last view to quit

## Script Mode

Create a script file with commands:

```
# Comments start with #
load data.csv
filter age > 30
sort name
save filtered.csv
quit
```

Commands can be separated by `|` on a single line:
```
load data.csv | filter age > 30 AND city = 'NYC' | sort name | save filtered.csv
```

Run with:
```bash
tv --script myscript.txt
```

### Script Commands

| Command | Description |
|---------|-------------|
| `from <path>` | Load CSV, Parquet, or gzipped CSV file |
| `save <path>` | Save to CSV or Parquet file |
| `filter <expr>` | Filter rows (PRQL/SQL syntax) |
| `take <n>` | Limit to first n rows |
| `sort <col>` | Sort ascending (use `-col` for descending) |
| `freq <col>` | Frequency table |
| `meta` | Metadata view |
| `corr` | Correlation matrix (all numeric columns) |
| `delcol <col1,col2>` | Delete column(s) |
| `select <col1,col2>` | Select columns |
| `xkey <col1,col2>` | Move columns to front as key columns (with separator) |
| `rename <old> <new>` | Rename column |
| `quit` | Exit script |

## Examples

### Basic Workflow

1. Open a CSV: `tv sales.csv`
2. Navigate to a column with arrow keys
3. Press `F` to see value distribution
4. Press `Enter` on a value to filter the original data
5. Press `q` to go back

### Data Exploration

1. `tv large_dataset.parquet`
2. Press `M` for metadata overview
3. Press `q` to return
4. Navigate to a numeric column
5. Press `]` to sort descending (find highest values)
6. Press `/` to search for specific values

### Data Transformation

1. Load file: `tv input.csv`
2. Delete unwanted columns: navigate + `D`
3. Rename columns: `^`
4. Filter rows: `\` then type `status == 'active'`
5. Save result: `S` then enter filename

## Large Gzipped CSV Files

tv supports streaming large `.csv.gz` files:

```bash
# Load gzipped CSV (shows 1k rows instantly, streams more in background)
tv large_data.csv.gz

# Save to parquet (streams entire file, creates sequential chunks)
# Creates: output_001.parquet, output_002.parquet, ...
tv -c "from large.csv.gz | save output.parquet"
```

Background streaming behavior:
- Shows first 1,000 rows immediately for fast startup
- Continues loading in background up to 10% of system memory (configurable via `gz_mem_pct` in `cfg/config.csv`)
- Data automatically merges as it loads

When saving a gzipped CSV to parquet:
- Streams through the entire file using `zcat`
- Writes ~1GB chunks as sequential parquet files
- Schema is inferred from the preview data

## Themes

tv supports color themes via `cfg/themes.csv` and `cfg/config.csv`.

To change theme, edit `cfg/config.csv`:
```csv
key,value
theme,light
```

Available themes: `default`, `light`

Theme colors are defined in `cfg/themes.csv` (long format):
```csv
theme,name,color
default,header_bg,#282832
default,header_fg,#ffffff
...
```

## Key Replay Mode

Test keyboard interactions without TUI using `--keys`:

```bash
tv --keys 'F<ret>' data.csv              # Freq view, press Enter
tv --keys 'l<right><right>]' .           # ls, move right 2x, sort desc
tv --keys 'M<down><space>D' x.csv        # Meta, select row, delete col
tv --keys '<backslash>age > 30<ret>' x   # Filter with expression
tv --keys '/hello<ret>n' x.csv           # Search "hello", next match
```

Key names follow [Kakoune](https://kakoune.org) style (no commas between keys):

| Key | Name |
|-----|------|
| Enter | `<ret>` |
| Escape | `<esc>` |
| Space | `<space>` |
| Backspace | `<backspace>` |
| Tab | `<tab>` |
| Arrows | `<up>` `<down>` `<left>` `<right>` |
| Home/End | `<home>` `<end>` |
| Page Up/Down | `<pageup>` `<pagedown>` |
| Delete | `<del>` |
| Backslash | `<backslash>` |
| Ctrl+x | `<c-x>` |
| Shift+Tab | `<s-tab>` |

Regular keys are just the character: `F`, `M`, `[`, `]`, `/`, etc.

**Text input mode**: Keys like `/`, `<backslash>`, `L`, `S`, `:` enter text mode.
Type text normally, `<ret>` executes, `<esc>` cancels.

## Command History

Commands are logged to `~/.tv/history`. This file records all commands executed through the command executor.
