# Arthur Whitney Review

6478 lines. Should be 1500.

## 1. ViewState: 28 fields → 8

```rust
// Current: 28 fields, most Option<T>
pub struct ViewState {
    pub id: usize,
    pub name: String,
    pub kind: ViewKind,
    pub prql: String,
    pub dataframe: DataFrame,
    pub state: TableState,
    pub history: Vec<String>,
    pub filename: Option<String>,
    pub show_row_numbers: bool,
    pub parent_id: Option<usize>,
    pub parent_rows: Option<usize>,
    pub parent_name: Option<String>,
    pub freq_col: Option<String>,
    pub selected_cols: HashSet<usize>,
    pub selected_rows: HashSet<usize>,
    pub gz_source: Option<String>,
    pub stats_cache: Option<(usize, String)>,
    pub col_separator: Option<usize>,
    pub meta_cache: Option<DataFrame>,
    pub partial: bool,
    pub disk_rows: Option<usize>,
    pub parquet_path: Option<String>,
    pub col_names: Vec<String>,
    pub sort_col: Option<String>,
    pub sort_desc: bool,
    pub filter_clause: Option<String>,
    pub fetch_cache: Option<(usize, usize)>,
}

// Better: core data only
pub struct View {
    pub df: DataFrame,       // data
    pub r: usize,            // cursor row
    pub c: usize,            // cursor col
    pub r0: usize,           // scroll offset
    pub sel: HashSet<usize>, // selection
    pub path: String,        // source path
    pub filter: String,      // SQL where
    pub parent: Option<usize>,
}
```

## 2. Kill Row/Col newtypes

```rust
// 38 lines for nothing
pub struct Row(pub usize);
impl Row { ... }
impl Add<usize> for Row { ... }
impl Display for Row { ... }

// Just use usize. You won't confuse row and col.
```

## 3. ViewKind → &str

```rust
// Current: enum + Display impl = 24 lines
pub enum ViewKind { Table, Meta, Freq, Corr, Folder, Pivot }
impl Display for ViewKind { ... }

// Better: just a string
pub kind: &'static str,  // "table", "meta", "freq"
```

## 4. Kill Plugin System

```rust
// Current: trait + registry + 6 plugin files = 1500+ lines
pub trait Plugin { ... }
pub struct Registry { ... }

// Better: single match in handle_key
fn handle(k: &str, v: &mut View) {
    match (v.kind, k) {
        ("freq", "enter") => filter_parent(v),
        ("meta", "enter") => goto_col(v),
        ("meta", "D") => del_cols(v),
        _ => {}
    }
}
```

## 5. Kill Command Pattern

```rust
// Current: trait + executor + 10 command structs = 500+ lines
pub trait Command { fn exec(&mut self, app: &mut App) -> Result<()>; }
pub struct Filter { pub expr: String }
impl Command for Filter { ... }

// Better: just functions
fn filter(app: &mut App, expr: &str) -> Result<()> { ... }
fn sort(app: &mut App, col: &str, desc: bool) -> Result<()> { ... }
```

## 6. One Source, Not Three

```rust
// Current: trait + 3 impls = 600+ lines
pub trait Source { fn lf(&self) -> LazyFrame; ... }
pub struct Polars;
pub struct Memory<'a>(&'a DataFrame);
pub struct Gz<'a> { ... }

// Better: everything is LazyFrame
fn lf(v: &View) -> LazyFrame {
    if v.path.ends_with(".parquet") {
        LazyFrame::scan_parquet(&v.path, ..)
    } else {
        v.df.clone().lazy()
    }
}
```

## 7. Kill error.rs

```rust
// 43 lines for unused error types
pub enum TvError { NoTable, ColumnNotFound, ... }

// Just use anyhow. It's already there.
```

## 8. Merge Files

```
// Current: 13 source files
src/
  app.rs command/ error.rs keyhandler.rs keymap.rs
  main.rs picker.rs plugin/ pure.rs render/
  source/ state.rs theme.rs utils.rs

// Better: 3 files
src/
  main.rs   // entry, key handling, commands
  view.rs   // View struct, cursor, render
  sql.rs    // polars/SQL helpers
```

## 9. Inline Small Functions

```rust
// Current: many 1-line wrapper functions
pub fn has_view(&self) -> bool { self.stack.has_view() }
pub fn view(&self) -> Option<&ViewState> { self.stack.cur() }
pub fn is_loading(&self) -> bool { self.bg_loader.is_some() }

// Just access fields directly
if app.stack.last().is_some() { ... }
if app.bg.is_some() { ... }
```

## 10. Simplify Keymap

```rust
// Current: HashMap<String, HashMap<String, KeyBinding>> + CSV files
pub struct KeyMap {
    bindings: HashMap<String, HashMap<String, KeyBinding>>,
    key_to_cmd: HashMap<String, HashMap<String, String>>,
}

// Better: static array
const KEYS: &[(&str, &str)] = &[
    ("q", "quit"), ("j", "down"), ("k", "up"),
    ("\\", "filter"), ("F", "freq"), ("M", "meta"),
];
```

## Summary

| Current | Target | Reduction |
|---------|--------|-----------|
| 6478 lines | 1500 lines | -77% |
| 13 files | 3 files | -77% |
| 28 ViewState fields | 8 fields | -71% |
| 6 plugins | 1 match | -83% |
| Command trait | functions | -100% |

The code works. But it's 4x bigger than needed.
