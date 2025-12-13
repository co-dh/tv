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

# Run inline commands (SQL WHERE syntax)
tv -c "load data.csv | filter age > 30 | save filtered.csv"
tv -c "load data.csv | filter name LIKE 'A%' | sel name,age"

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

Both search and filter use **SQL WHERE syntax**:

| Key | Action |
|-----|--------|
| `/` | Search (navigate to matching row) |
| `\` | Filter (create new view with matching rows) |
| `n` | Find next match |
| `N` | Find previous match |
| `*` | Search for current cell value |

SQL WHERE syntax:
- `col = 100` - Numeric equality
- `col = 'NYC'` - String equality (use quotes)
- `col > 100`, `col <= 50` - Comparisons
- `col LIKE 'abc%'` - Starts with
- `col LIKE '%abc'` - Ends with
- `col LIKE '%abc%'` - Contains
- `col BETWEEN 10 AND 100` - Range
- `col > 10 AND col < 100` - Combined conditions

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
| `Enter` | (In frequency view) Filter parent table by selected value(s) |

In Frequency view: use `Space` to select multiple values, then `Enter` to filter parent table by all selected values.

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
| `load <path>` | Load CSV or Parquet file |
| `save <path>` | Save to CSV or Parquet file |
| `filter <sql>` | Filter rows (SQL WHERE syntax) |
| `freq <col>` | Frequency table |
| `meta` | Metadata view |
| `corr` | Correlation matrix (all numeric columns) |
| `delcol <col1,col2>` | Delete column(s) |
| `sel <col1,col2>` | Select columns |
| `sort <col>` | Sort ascending |
| `sortdesc <col>` | Sort descending |
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
4. Filter rows: `\` then type `status = 'active'`
5. Save result: `S` then enter filename
