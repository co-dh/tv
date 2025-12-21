# tabv - Terminal Table Viewer

A fast, interactive terminal table viewer for CSV and Parquet files. Built for data exploration, tabv lets you navigate million-row datasets instantly, filter with PRQL queries, and explore your data with frequency tables, correlation matrices, and pivot tables — all without leaving the terminal.

## Features

- **Instant loading**: Streams gzipped CSV files and lazy-loads Parquet, so you can start exploring immediately even with huge datasets
- **PRQL queries**: Filter, sort, and derive columns using PRQL, a modern query language that compiles to SQL
- **Data profiling**: The metadata view shows column types, null percentages, distinct value counts, and statistical summaries
- **Frequency analysis**: Quickly see value distributions with counts and aggregates for any column
- **Pivot tables**: Reshape your data with cross-tabulations and custom aggregation functions
- **System explorer**: Browse running processes, network connections, installed packages, and more as interactive tables
- **Vim-style navigation**: Use familiar hjkl keys for quick exploration

## Installation

Install from crates.io:

```bash
cargo install tabv
```

Or build from source:

```bash
git clone https://github.com/xxx/tabv
cd tabv
cargo build --release
./target/release/tabv
```

## Quick Start

Open any CSV or Parquet file:

```bash
tabv data.csv
tabv sales.parquet
```

Once inside tabv:

1. **Navigate** with arrow keys or vim-style `hjkl`
2. **Press `F`** on any column to see value distribution (frequency table)
3. **Press `\`** to filter rows — try typing `price > 100`
4. **Press `M`** to see column statistics and data types
5. **Press `q`** to go back to the previous view, or quit if you're at the root

That's the basic workflow: navigate, explore with views, filter to drill down, and press `q` to back out.

## User Guide

### Opening Files

tabv supports several file formats:

```bash
tabv customers.csv        # Plain CSV file
tabv orders.parquet       # Parquet file (lazy-loaded, handles huge files efficiently)
tabv logs.csv.gz          # Gzipped CSV (streams first 1000 rows immediately, loads rest in background)
tabv                      # Start with empty view, press L to load a file
```

### Navigating Your Data

Use arrow keys or vim-style movement:

- **Arrow keys** or **hjkl**: Move one cell at a time
- **g** / **G**: Jump to the first or last row
- **Ctrl+D** / **Ctrl+U**: Page down or up through the data
- **@**: Open a fuzzy finder to jump to a column by name

### Filtering Your Data

Press `\` (backslash) to open the filter prompt. Type a PRQL expression to filter rows:

```
price > 100                      # Keep rows where price is greater than 100
status == "active"               # Keep rows where status equals "active"
name ~= "john"                   # Keep rows where name contains "john" (regex match)
age >= 18 && age <= 65           # Combine conditions with && (and)
quantity > 0 || discount > 0     # Use || for or conditions
```

Each filter creates a new view on the stack. Press `q` to return to the unfiltered data.

### Working with Columns

- **Space**: Toggle selection on the current column (selected columns are highlighted)
- **Esc**: Clear all selections
- **s**: Keep only specific columns (like SQL SELECT) — enter column names or use the fuzzy picker
- **D**: Delete selected columns from the view
- **^**: Rename the current column
- **[** / **]**: Sort by current column ascending or descending
- **!**: Mark columns as "key columns" (used for pivot tables and shown first with a separator)


### Aggregating Data

Press `b` to aggregate the data by the current column. You'll be prompted to choose an aggregation function:

- **count**: Number of rows in each group
- **sum**: Sum of values
- **mean**: Average value
- **min** / **max**: Minimum or maximum value
- **std**: Standard deviation

### Analytical Views

tabv provides several views for data analysis:

| Key | View | What it shows |
|-----|------|---------------|
| **F** | Frequency | Value counts for the current column, plus min/max/sum for numeric columns. Press Enter on any value to filter the parent table to just that value. |
| **M** | Metadata | One row per column showing: data type, null percentage, distinct count, min, max, mean, median, and standard deviation. Press `0` to select all-null columns, `1` to select single-value columns, then `D` to delete them. |
| **C** | Correlation | Pearson correlation matrix for all numeric columns. Helps identify relationships between variables. |
| **P** | Pivot | Cross-tabulation table. First mark your row key columns with `!`, then press `P` and select the pivot column and value column. |

### Understanding the View Stack

Every operation that creates a new perspective on your data (filtering, frequency table, metadata view) pushes a new view onto a stack. This lets you drill down into your data while keeping the ability to go back.

- Press **q** to pop the current view and return to the previous one
- Press **q** on the last view to quit tabv
- Press **T** to duplicate the current view (useful for comparing different filters)
- Press **S** to swap the top two views on the stack

### Exploring System Information

Press `:` to enter command mode, then type a command to view system information as an interactive table:

| Command | What it shows |
|---------|---------------|
| **ps** | Running processes with user, PID, CPU usage, memory usage, and command line. Sort by CPU with `]` to find resource hogs. |
| **tcp** | Active TCP connections showing local and remote addresses, ports, and connection state. |
| **udp** | Active UDP sockets with local and remote endpoints. |
| **env** | Environment variables as a two-column table (name and value). Filter with `\` to find specific variables. |
| **mounts** | Mounted filesystems showing device, mount point, filesystem type, and mount options. |
| **df** | Disk usage for each filesystem with total, used, and available space. |
| **pacman** | Installed Arch Linux packages with name, installed size, and number of dependencies. |
| **cargo** | Rust dependencies for the current project showing name, version, and dependency relationships. |
| **lsof** | Open file descriptors for all processes, or specify a PID like `lsof 1234` for a specific process. |
| **journalctl** | Recent system log entries. Specify a count like `journalctl 500` to see more lines. |

All system views support the same navigation and filtering as regular data files.

### Files and Directories

- **L**: Load a new file (opens a file picker)
- **r**: List the current directory recursively as a table with file paths, sizes, and modification times

## Key Reference

### Navigation
| Key | Action |
|-----|--------|
| `↑↓←→` / `hjkl` | Move cursor |
| `g` / `G` | First / last row |
| `Ctrl+D` / `Ctrl+U` | Page down / up |
| `@` | Jump to column by name |

### Data Operations
| Key | Action |
|-----|--------|
| `\` | Filter rows with PRQL expression |
| `/` | Search in current column |
| `n` / `N` | Next / previous search match |
| `*` | Search for current cell value |
| `s` | Select columns to keep |
| `D` | Delete selected columns |
| `[` / `]` | Sort ascending / descending |
| `c` | Create computed column |
| `b` | Aggregate by current column |
| `^` | Rename current column |
| `!` | Set key columns for pivot |

### Views
| Key | Action |
|-----|--------|
| `F` | Frequency table |
| `M` | Metadata / column statistics |
| `C` | Correlation matrix |
| `P` | Pivot table |
| `T` | Duplicate current view |
| `S` | Swap top two views |
| `q` | Go back / quit |

### Other
| Key | Action |
|-----|--------|
| `L` | Load file |
| `r` | List directory |
| `:` | Command mode |
| `I` | Toggle info panel |
| `,` / `.` | Decrease / increase decimal places |
| `Space` | Toggle column selection |
| `Esc` | Clear selection |

## Configuration

tabv reads configuration from `~/.tv/config.csv`:

```csv
key,value
theme,dark
```

Available themes: `default`, `dark`, `light`

## Automated Testing

tabv supports a key replay mode for automated testing and scripting:

```bash
tabv --keys 'F<ret>' data.csv           # Open freq view, press enter
tabv --keys 'M0D' data.csv              # Open meta, select null columns, delete them
tabv --keys '\price > 100<ret>' x.csv   # Filter to price > 100
```

Special key names: `<ret>`, `<esc>`, `<space>`, `<up>`, `<down>`, `<left>`, `<right>`, `<c-d>`, `<c-u>`, `<backslash>`
