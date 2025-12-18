# similarity-rs Analysis Summary

## Key Highlights (>90% similar)

| File                 | Functions                            | Similarity |
|------                |-----------                           |------------|
| backend/mod.rs       | `cols` ↔ `schema`                    | 96.4%      |
| backend/mod.rs       | `cols` ↔ `distinct`                  | 94.8%      |
| backend/mod.rs       | `schema` ↔ `distinct`                | 94.5%      |
| backend/mod.rs       | `count_where` ↔ `distinct`           | 94.5%      |
| backend/mod.rs       | `fetch_sel` ↔ `distinct`             | 93.6%      |
| backend/mod.rs       | `cols` ↔ `fetch_sel`                 | 92.9%      |
| backend/mod.rs       | `schema` ↔ `fetch_sel`               | 92.9%      |
| backend/mod.rs       | `fetch_sel` ↔ `count_where`          | 92.9%      |
| backend/polars.rs    | `detect_sep` ↔ `save_csv`            | 95.1%      |
| backend/polars.rs    | `save_csv` ↔ `load_glob`             | 94.6%      |
| backend/polars.rs    | `detect_sep` ↔ `load_glob`           | 93.6%      |
| backend/polars.rs    | `load` ↔ `schema_diff`               | 91.1%      |
| plugin/folder.rs     | `handle` ↔ `exec` (DelFiles)         | 92.1%      |
| plugin/folder.rs     | `handle` ↔ `exec` (BatView)          | 91.3%      |
| command/transform.rs | `exec` (FilterIn) ↔ `exec` (EpochMs) | 85.3%      |

## Tool Comparison

| Tool | Detection Level | Best For |
|------|-----------------|----------|
| **similarity-rs** | Function-level | Finding functions to consolidate |
| **dedup_rust** | Token pattern | Finding macro/helper opportunities |

## Interpretation

The Backend trait methods (`cols`, `schema`, `distinct`, `count_where`, `fetch_sel`) share structure:
1. Call `self.lf(path)?`
2. Execute SQL via `sql()`
3. Extract/transform result

This is **intentional** - the SQL-based Backend design. Not worth refactoring as each method has different SQL and result extraction. A macro would add complexity for marginal gain.

**Actionable items** from dedup_rust (already done):
- `ser!` macro for `Series::new().into()` (50 sites)
- `df_cols()` helper for column name extraction (12 sites)
- `test_df!` macro for test DataFrame creation (11 sites)

## DRY Refactoring Results

**Baseline:** 4926 lines, 12315 len, 4.105 bugs
**After:** 4928 lines, 12320 len, 4.107 bugs

**What was done:**
- Extracted `get_schema()` helper in Backend trait
- `cols()` and `schema()` now share schema fetching via `get_schema()`

**Similarity change:**

| Functions | Before | After |
|-----------|--------|-------|
| `cols` ↔ `schema` | 96.4% | 94.4% |

**Analysis of Key Highlights:**

| Key Highlight | Actionable? | Verdict |
|--------------|-------------|---------|
| backend/mod.rs `cols`↔`schema` | ✓ Yes | Fixed - share `get_schema()` |
| backend/mod.rs other methods | ✗ No | Already DRY (all use `sql()`) |
| backend/polars.rs functions | ✗ No | False positive - different purposes |
| plugin/folder.rs `handle`↔`exec` | ✗ No | False positive - Plugin vs Command |
| command/transform.rs exec methods | ✗ No | False positive - trait impl pattern |

**Conclusion:** similarity-rs reports structural similarity (same error handling, same trait patterns) - not actual duplication. Only 1 of 15 highlights was actionable. dedup_rust's token patterns are more actionable for DRY.

---

## Raw Output

Analyzing Rust code similarity...

=== Function Similarity ===
Checking 27 files for duplicates...

Duplicates in src/plugin/folder.rs:
------------------------------------------------------------
  src/plugin/folder.rs:33-72 method handle <-> src/plugin/folder.rs:105-134 method exec
  Similarity: 92.08%
  Classes: Plugin <-> Command

[36m--- src/plugin/folder.rs:handle (lines 33-72) ---[0m
    fn handle(&self, cmd: &str, app: &mut AppContext) -> Option<Box<dyn Command>> {
        let v = app.view()?;
        let df = &v.dataframe;
        // Extract parent dir from view name (ls:path or ls -r:path)
        let dir = v.name.split(':').nth(1).map(|s| PathBuf::from(s)).unwrap_or_else(|| PathBuf::from("."));

        // For delete: get all selected paths (or current row)
        if cmd == "delete" {
            let rows: Vec<usize> = if v.selected_rows.is_empty() { vec![v.state.cr] }
                else { v.selected_rows.iter().copied().collect() };
            let paths: Vec<String> = rows.iter().filter_map(|&r| {
                df.column("path").ok()?.get(r).ok().map(|v| unquote(&v.to_string()))
            }).collect();
            if !paths.is_empty() {
                return Some(Box::new(DelFiles { paths, dir }));
            }
            return None;
        }

        // For enter: get current row info
        let path = df.column("path").ok()?.get(v.state.cr).ok()
            .map(|v| unquote(&v.to_string()))?;
        let is_dir = df.column("dir").ok()
            .and_then(|c| c.get(v.state.cr).ok())
            .map(|v| unquote(&v.to_string()) == "x")
            .unwrap_or(false);

        match cmd {
            "enter" => {
                if is_dir {
                    Some(Box::new(Ls { dir: PathBuf::from(&path), recursive: false }))
                } else if is_text_file(Path::new(&path)) {
                    Some(Box::new(BatView { path }))
                } else {
                    Some(Box::new(From { file_path: path }))
                }
            }
            _ => None,
        }
    }

[36m--- src/plugin/folder.rs:exec (lines 105-134) ---[0m
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        use crate::picker;
        let n = self.paths.len();
        let prompt = if n == 1 {
            let name = Path::new(&self.paths[0]).file_name().and_then(|s| s.to_str()).unwrap_or(&self.paths[0]);
            format!("Delete '{}'? ", name)
        } else { format!("Delete {} files? ", n) };
        let result = picker::fzf(vec!["Yes".into(), "No".into()], &prompt)?;
        app.needs_redraw = true;
        match result {
            Some(s) if s == "Yes" => {
                let mut deleted = 0;
                for path in &self.paths {
                    if std::fs::remove_file(path).is_ok() { deleted += 1; }
                }
                app.msg(format!("Deleted {} file(s)", deleted));
                // Refresh by re-running ls on parent dir
                let df = super::system::ls(&self.dir)?;
                if let Some(view) = app.view_mut() {
                    view.dataframe = df;
                    view.selected_rows.clear();
                    if view.state.cr >= view.dataframe.height() {
                        view.state.cr = view.dataframe.height().saturating_sub(1);
                    }
                }
            }
            _ => app.msg("Cancelled".into()),
        }
        Ok(())
    }

  src/plugin/folder.rs:33-72 method handle <-> src/plugin/folder.rs:142-168 method exec
  Similarity: 91.29%
  Classes: Plugin <-> Command

[36m--- src/plugin/folder.rs:handle (lines 33-72) ---[0m
    fn handle(&self, cmd: &str, app: &mut AppContext) -> Option<Box<dyn Command>> {
        let v = app.view()?;
        let df = &v.dataframe;
        // Extract parent dir from view name (ls:path or ls -r:path)
        let dir = v.name.split(':').nth(1).map(|s| PathBuf::from(s)).unwrap_or_else(|| PathBuf::from("."));

        // For delete: get all selected paths (or current row)
        if cmd == "delete" {
            let rows: Vec<usize> = if v.selected_rows.is_empty() { vec![v.state.cr] }
                else { v.selected_rows.iter().copied().collect() };
            let paths: Vec<String> = rows.iter().filter_map(|&r| {
                df.column("path").ok()?.get(r).ok().map(|v| unquote(&v.to_string()))
            }).collect();
            if !paths.is_empty() {
                return Some(Box::new(DelFiles { paths, dir }));
            }
            return None;
        }

        // For enter: get current row info
        let path = df.column("path").ok()?.get(v.state.cr).ok()
            .map(|v| unquote(&v.to_string()))?;
        let is_dir = df.column("dir").ok()
            .and_then(|c| c.get(v.state.cr).ok())
            .map(|v| unquote(&v.to_string()) == "x")
            .unwrap_or(false);

        match cmd {
            "enter" => {
                if is_dir {
                    Some(Box::new(Ls { dir: PathBuf::from(&path), recursive: false }))
                } else if is_text_file(Path::new(&path)) {
                    Some(Box::new(BatView { path }))
                } else {
                    Some(Box::new(From { file_path: path }))
                }
            }
            _ => None,
        }
    }

[36m--- src/plugin/folder.rs:exec (lines 142-168) ---[0m
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        use ratatui::crossterm::{execute, terminal::{LeaveAlternateScreen, EnterAlternateScreen, disable_raw_mode, enable_raw_mode}};
        use std::io::stdout;
        use std::process::Command as Cmd;

        // Leave alternate screen and disable raw mode so bat receives input
        execute!(stdout(), LeaveAlternateScreen)?;
        disable_raw_mode()?;

        // Run bat (or cat as fallback)
        let status = Cmd::new("bat")
            .args(["--paging=always", "--style=numbers", &self.path])
            .status()
            .or_else(|_| Cmd::new("less").arg(&self.path).status())
            .or_else(|_| Cmd::new("cat").arg(&self.path).status());

        // Re-enable raw mode and re-enter alternate screen
        enable_raw_mode()?;
        execute!(stdout(), EnterAlternateScreen)?;
        app.needs_redraw = true;  // force ratatui to redraw

        match status {
            Ok(s) if s.success() => { app.msg(format!("Viewed: {}", self.path)); Ok(()) }
            Ok(s) => Err(anyhow!("bat exited with: {}", s)),
            Err(e) => Err(anyhow!("Failed to view file: {}", e)),
        }
    }

  src/plugin/folder.rs:105-134 method exec <-> src/plugin/folder.rs:142-168 method exec
  Similarity: 85.32%
  Classes: Command <-> Command

[36m--- src/plugin/folder.rs:exec (lines 105-134) ---[0m
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        use crate::picker;
        let n = self.paths.len();
        let prompt = if n == 1 {
            let name = Path::new(&self.paths[0]).file_name().and_then(|s| s.to_str()).unwrap_or(&self.paths[0]);
            format!("Delete '{}'? ", name)
        } else { format!("Delete {} files? ", n) };
        let result = picker::fzf(vec!["Yes".into(), "No".into()], &prompt)?;
        app.needs_redraw = true;
        match result {
            Some(s) if s == "Yes" => {
                let mut deleted = 0;
                for path in &self.paths {
                    if std::fs::remove_file(path).is_ok() { deleted += 1; }
                }
                app.msg(format!("Deleted {} file(s)", deleted));
                // Refresh by re-running ls on parent dir
                let df = super::system::ls(&self.dir)?;
                if let Some(view) = app.view_mut() {
                    view.dataframe = df;
                    view.selected_rows.clear();
                    if view.state.cr >= view.dataframe.height() {
                        view.state.cr = view.dataframe.height().saturating_sub(1);
                    }
                }
            }
            _ => app.msg("Cancelled".into()),
        }
        Ok(())
    }

[36m--- src/plugin/folder.rs:exec (lines 142-168) ---[0m
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        use ratatui::crossterm::{execute, terminal::{LeaveAlternateScreen, EnterAlternateScreen, disable_raw_mode, enable_raw_mode}};
        use std::io::stdout;
        use std::process::Command as Cmd;

        // Leave alternate screen and disable raw mode so bat receives input
        execute!(stdout(), LeaveAlternateScreen)?;
        disable_raw_mode()?;

        // Run bat (or cat as fallback)
        let status = Cmd::new("bat")
            .args(["--paging=always", "--style=numbers", &self.path])
            .status()
            .or_else(|_| Cmd::new("less").arg(&self.path).status())
            .or_else(|_| Cmd::new("cat").arg(&self.path).status());

        // Re-enable raw mode and re-enter alternate screen
        enable_raw_mode()?;
        execute!(stdout(), EnterAlternateScreen)?;
        app.needs_redraw = true;  // force ratatui to redraw

        match status {
            Ok(s) if s.success() => { app.msg(format!("Viewed: {}", self.path)); Ok(()) }
            Ok(s) => Err(anyhow!("bat exited with: {}", s)),
            Err(e) => Err(anyhow!("Failed to view file: {}", e)),
        }
    }


Duplicates in src/command/io/convert.rs:
------------------------------------------------------------
  src/command/io/convert.rs:44-70 function convert_epoch_cols <-> src/command/io/convert.rs:128-168 function apply_schema
  Similarity: 90.15%

[36m--- src/command/io/convert.rs:convert_epoch_cols (lines 44-70) ---[0m
pub fn convert_epoch_cols(df: DataFrame) -> DataFrame {
    let mut cols: Vec<Column> = Vec::with_capacity(df.width());
    for c in df.get_columns() {
        let name = c.name().as_str();
        let is_numeric = c.dtype().is_integer() || c.dtype().is_float();
        if !is_datetime_name(name) || !is_numeric {
            cols.push(c.clone());
            continue;
        }
        let s = c.as_materialized_series();
        let Ok(i64_s) = s.cast(&DataType::Int64) else { cols.push(c.clone()); continue; };
        let Ok(i64_ca) = i64_s.i64() else { cols.push(c.clone()); continue; };
        let Some(v) = i64_ca.into_iter().flatten().next() else { cols.push(c.clone()); continue; };

        // Try epoch conversion (not TAQ time - that requires explicit command)
        if let Some(unit) = epoch_unit(v) {
            let mult = if v.abs() < 10_000_000_000 { 1000i64 } else { 1 };
            let scaled = i64_ca.clone() * mult;
            if let Ok(dt) = scaled.into_series().cast(&DataType::Datetime(unit, None)) {
                cols.push(dt.into_column());
                continue;
            }
        }
        cols.push(c.clone());
    }
    DataFrame::new(cols).unwrap_or(df)
}

[36m--- src/command/io/convert.rs:apply_schema (lines 128-168) ---[0m
pub fn apply_schema(df: DataFrame, schema: &Schema) -> (DataFrame, Option<String>) {
    let mut cols: Vec<Column> = Vec::with_capacity(df.width());
    let mut err_msg: Option<String> = None;
    let n_rows = df.height();

    for col in df.get_columns() {
        let name = col.name();
        let target = schema.get(name);
        if target.is_none() || col.dtype() == target.unwrap() {
            cols.push(col.clone());
            continue;
        }
        let target = target.unwrap();

        // Try epoch conversion: String → i64 → Datetime
        if matches!(target, DataType::Datetime(_, _)) && col.dtype() == &DataType::String {
            if let Ok(i64_s) = col.cast(&DataType::Int64) {
                if let Ok(i64_ca) = i64_s.as_materialized_series().i64() {
                    if let Some(v) = i64_ca.iter().flatten().next() {
                        if let Some(unit) = epoch_unit(v) {
                            let mult = if v.abs() < 10_000_000_000 { 1000i64 } else { 1 };
                            let scaled = i64_ca.clone() * mult;
                            if let Ok(dt) = scaled.into_series().cast(&DataType::Datetime(unit, None)) {
                                if dt.len() == n_rows { cols.push(dt.into_column()); continue; }
                            }
                        }
                    }
                }
            }
        }
        // Standard cast
        if let Ok(casted) = col.cast(target) {
            if casted.len() == n_rows { cols.push(casted); continue; }
        }
        if err_msg.is_none() {
            err_msg = Some(format!("Column '{}': failed to convert {:?} to {:?}", name, col.dtype(), target));
        }
        cols.push(col.clone());
    }
    (DataFrame::new(cols).unwrap_or(df), err_msg)
}

  src/command/io/convert.rs:100-125 function convert_types <-> src/command/io/convert.rs:128-168 function apply_schema
  Similarity: 90.56%

[36m--- src/command/io/convert.rs:convert_types (lines 100-125) ---[0m
pub fn convert_types(df: DataFrame) -> DataFrame {
    let mut cols: Vec<Column> = Vec::with_capacity(df.width());
    for col in df.get_columns() {
        let s = col.as_materialized_series();
        let Ok(str_ca) = s.str() else { cols.push(col.clone()); continue; };

        // Try i64: must be pure integers that round-trip exactly
        let all_int = str_ca.iter().all(|v| v.is_none() || (is_pure_int(v.unwrap()) && int_roundtrip(v.unwrap())));
        if all_int {
            if let Ok(int_s) = s.cast(&DataType::Int64) {
                cols.push(int_s.into_column());
                continue;
            }
        }
        // Try f64: must round-trip
        let all_float = str_ca.iter().all(|v| v.is_none() || float_roundtrip(v.unwrap()));
        if all_float {
            if let Ok(float_s) = s.cast(&DataType::Float64) {
                cols.push(float_s.into_column());
                continue;
            }
        }
        cols.push(col.clone());
    }
    convert_epoch_cols(DataFrame::new(cols).unwrap_or(df))
}

[36m--- src/command/io/convert.rs:apply_schema (lines 128-168) ---[0m
pub fn apply_schema(df: DataFrame, schema: &Schema) -> (DataFrame, Option<String>) {
    let mut cols: Vec<Column> = Vec::with_capacity(df.width());
    let mut err_msg: Option<String> = None;
    let n_rows = df.height();

    for col in df.get_columns() {
        let name = col.name();
        let target = schema.get(name);
        if target.is_none() || col.dtype() == target.unwrap() {
            cols.push(col.clone());
            continue;
        }
        let target = target.unwrap();

        // Try epoch conversion: String → i64 → Datetime
        if matches!(target, DataType::Datetime(_, _)) && col.dtype() == &DataType::String {
            if let Ok(i64_s) = col.cast(&DataType::Int64) {
                if let Ok(i64_ca) = i64_s.as_materialized_series().i64() {
                    if let Some(v) = i64_ca.iter().flatten().next() {
                        if let Some(unit) = epoch_unit(v) {
                            let mult = if v.abs() < 10_000_000_000 { 1000i64 } else { 1 };
                            let scaled = i64_ca.clone() * mult;
                            if let Ok(dt) = scaled.into_series().cast(&DataType::Datetime(unit, None)) {
                                if dt.len() == n_rows { cols.push(dt.into_column()); continue; }
                            }
                        }
                    }
                }
            }
        }
        // Standard cast
        if let Ok(casted) = col.cast(target) {
            if casted.len() == n_rows { cols.push(casted); continue; }
        }
        if err_msg.is_none() {
            err_msg = Some(format!("Column '{}': failed to convert {:?} to {:?}", name, col.dtype(), target));
        }
        cols.push(col.clone());
    }
    (DataFrame::new(cols).unwrap_or(df), err_msg)
}


Duplicates in src/keymap.rs:
------------------------------------------------------------
  src/keymap.rs:97-106 method from_defaults <-> src/keymap.rs:124-141 method load
  Similarity: 87.46%
  Classes: KeyMap <-> KeyMap

[36m--- src/keymap.rs:from_defaults (lines 97-106) ---[0m
    fn from_defaults() -> Self {
        let mut bindings: HashMap<String, HashMap<String, KeyBinding>> = HashMap::new();
        let mut key_to_cmd: HashMap<String, HashMap<String, String>> = HashMap::new();
        for (tab, key, cmd) in Self::defaults() {
            let binding = KeyBinding { key: key.to_string(), command: cmd.to_string() };
            bindings.entry(tab.to_string()).or_default().insert(cmd.to_string(), binding);
            key_to_cmd.entry(tab.to_string()).or_default().insert(key.to_string(), cmd.to_string());
        }
        Self { bindings, key_to_cmd }
    }

[36m--- src/keymap.rs:load (lines 124-141) ---[0m
    pub fn load(path: &Path) -> anyhow::Result<Self> {
        let content = fs::read_to_string(path)?;
        let mut bindings: HashMap<String, HashMap<String, KeyBinding>> = HashMap::new();
        let mut key_to_cmd: HashMap<String, HashMap<String, String>> = HashMap::new();
        for line in content.lines().skip(1) {
            let parts: Vec<&str> = line.splitn(4, ',').collect();
            if parts.len() >= 3 {
                let (tab, key, cmd) = (parts[0].to_string(), parts[1].to_string(), parts[2].to_string());
                if let Some(existing) = key_to_cmd.get(&tab).and_then(|m| m.get(&key)) {
                    return Err(anyhow::anyhow!("Key conflict: '{}' mapped to both '{}' and '{}'", key, existing, cmd));
                }
                let binding = KeyBinding { key: key.clone(), command: cmd.clone() };
                bindings.entry(tab.clone()).or_default().insert(cmd.clone(), binding);
                key_to_cmd.entry(tab).or_default().insert(key, cmd);
            }
        }
        Ok(Self { bindings, key_to_cmd })
    }

  src/keymap.rs:144-157 method get_command <-> src/keymap.rs:160-172 method get_key
  Similarity: 90.46%
  Classes: KeyMap <-> KeyMap

[36m--- src/keymap.rs:get_command (lines 144-157) ---[0m
    pub fn get_command(&self, tab: &str, key: &str) -> Option<&str> {
        // Check specific tab first
        if let Some(cmds) = self.key_to_cmd.get(tab) {
            if let Some(cmd) = cmds.get(key) { return Some(cmd); }
        }
        // Fall back to table (all views inherit table keys)
        if tab != "table" {
            if let Some(cmds) = self.key_to_cmd.get("table") {
                if let Some(cmd) = cmds.get(key) { return Some(cmd); }
            }
        }
        // Fall back to common
        self.key_to_cmd.get("common").and_then(|m| m.get(key)).map(|s| s.as_str())
    }

[36m--- src/keymap.rs:get_key (lines 160-172) ---[0m
    pub fn get_key(&self, tab: &str, command: &str) -> Option<&str> {
        if let Some(cmds) = self.bindings.get(tab) {
            if let Some(binding) = cmds.get(command) {
                return Some(&binding.key);
            }
        }
        if let Some(cmds) = self.bindings.get("common") {
            if let Some(binding) = cmds.get(command) {
                return Some(&binding.key);
            }
        }
        None
    }

  src/keymap.rs:86-94 method new <-> src/keymap.rs:144-157 method get_command
  Similarity: 89.64%
  Classes: KeyMap <-> KeyMap

[36m--- src/keymap.rs:new (lines 86-94) ---[0m
    pub fn new() -> Self {
        let mut km = Self::from_defaults();
        // Try user override: ~/.config/tv/keys.csv
        if let Some(home) = std::env::var_os("HOME") {
            let path = Path::new(&home).join(".config/tv/keys.csv");
            if path.exists() { let _ = km.load_overrides(&path); }
        }
        km
    }

[36m--- src/keymap.rs:get_command (lines 144-157) ---[0m
    pub fn get_command(&self, tab: &str, key: &str) -> Option<&str> {
        // Check specific tab first
        if let Some(cmds) = self.key_to_cmd.get(tab) {
            if let Some(cmd) = cmds.get(key) { return Some(cmd); }
        }
        // Fall back to table (all views inherit table keys)
        if tab != "table" {
            if let Some(cmds) = self.key_to_cmd.get("table") {
                if let Some(cmd) = cmds.get(key) { return Some(cmd); }
            }
        }
        // Fall back to common
        self.key_to_cmd.get("common").and_then(|m| m.get(key)).map(|s| s.as_str())
    }

  src/keymap.rs:86-94 method new <-> src/keymap.rs:160-172 method get_key
  Similarity: 87.89%
  Classes: KeyMap <-> KeyMap

[36m--- src/keymap.rs:new (lines 86-94) ---[0m
    pub fn new() -> Self {
        let mut km = Self::from_defaults();
        // Try user override: ~/.config/tv/keys.csv
        if let Some(home) = std::env::var_os("HOME") {
            let path = Path::new(&home).join(".config/tv/keys.csv");
            if path.exists() { let _ = km.load_overrides(&path); }
        }
        km
    }

[36m--- src/keymap.rs:get_key (lines 160-172) ---[0m
    pub fn get_key(&self, tab: &str, command: &str) -> Option<&str> {
        if let Some(cmds) = self.bindings.get(tab) {
            if let Some(binding) = cmds.get(command) {
                return Some(&binding.key);
            }
        }
        if let Some(cmds) = self.bindings.get("common") {
            if let Some(binding) = cmds.get(command) {
                return Some(&binding.key);
            }
        }
        None
    }


Duplicates in src/main.rs:
------------------------------------------------------------
  src/main.rs:30-122 function main <-> src/main.rs:378-434 function parse
  Similarity: 92.28%

[36m--- src/main.rs:main (lines 30-122) ---[0m
fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();

    // Parse flags first (before early returns)
    let raw_save = args.iter().any(|a| a == "--raw");

    // Check for --script argument
    if let Some(idx) = args.iter().position(|a| a == "--script") {
        if args.len() <= idx + 1 {
            eprintln!("Usage: tv --script <script_file>");
            std::process::exit(1);
        }
        return run_script(&args[idx + 1]);
    }

    // Check for --keys argument (key replay mode with immutable keymap)
    if let Some(idx) = args.iter().position(|a| a == "--keys") {
        if args.len() <= idx + 1 {
            eprintln!("Usage: tv --keys 'F<ret>' file.parquet");
            std::process::exit(1);
        }
        let file = args.get(idx + 2).map(|s| s.as_str());
        return run_keys(&args[idx + 1], file);
    }

    // Initialize ratatui terminal
    let mut tui = render::init()?;

    // Get file path (first non-flag argument after program name)
    let file_arg = args.iter().skip(1).find(|a| !a.starts_with('-'));

    // Create app context
    let mut app = if let Some(path) = file_arg {
        // Load file from CLI argument
        let mut temp_app = AppContext::new();
        temp_app.raw_save = raw_save;
        match CommandExecutor::exec(&mut temp_app, Box::new(From { file_path: path.clone() })) {
            Ok(_) => temp_app,
            Err(e) => {
                render::restore()?;
                eprintln!("Error loading file: {}", e);
                std::process::exit(1);
            }
        }
    } else {
        let mut temp_app = AppContext::new();
        temp_app.raw_save = raw_save;
        temp_app
    };

    // Update viewport
    let size = tui.size()?;
    app.viewport(size.height, size.width);

    // Main event loop
    loop {
        // Check background tasks
        app.merge_bg_data();
        app.check_bg_saver();
        app.check_bg_meta();
        app.check_bg_freq();

        // Force full redraw if needed (after bat/less/fzf return)
        if app.needs_redraw {
            tui.clear()?;
            // Update viewport in case terminal size changed
            let size = tui.size()?;
            app.viewport(size.height, size.width);
            app.needs_redraw = false;
        }
        // Center cursor if needed (after search, with fresh viewport)
        if app.needs_center {
            if let Some(view) = app.view_mut() {
                view.state.center_if_needed();
            }
            app.needs_center = false;
        }
        // Render with ratatui diff-based update
        tui.draw(|frame| Renderer::render(frame, &mut app))?;

        // Poll for events with timeout (allows background data merge)
        if event::poll(std::time::Duration::from_millis(100))? {
            if let Event::Key(key) = event::read()? {
                if !on_key(&mut app, key)? {
                    break;
                }
            }
        }
    }

    render::restore()?;
    Ok(())
}

[36m--- src/main.rs:parse (lines 378-434) ---[0m
fn parse(line: &str, app: &mut AppContext) -> Option<Box<dyn command::Command>> {
    let parts: Vec<&str> = line.splitn(2, ' ').collect();
    let cmd = parts[0].to_lowercase();
    let arg = parts.get(1).map(|s| s.trim()).unwrap_or("");

    // Core commands (not in plugins)
    match cmd.as_str() {
        "load" | "from" => return Some(Box::new(From { file_path: arg.to_string() })),
        "save" => return Some(Box::new(Save { file_path: arg.to_string() })),
        "corr" | "correlation" => return Some(Box::new(Correlation { selected_cols: vec![] })),
        "del_col" | "delcol" => return Some(Box::new(DelCol { col_names: arg.split(',').map(|s| s.trim().to_string()).collect() })),
        "filter" => return Some(Box::new(Filter { expr: arg.to_string() })),
        "select" | "sel" => return Some(Box::new(Select {
            col_names: arg.split(',').map(|s| s.trim().to_string()).collect()
        })),
        "sort" => {
            let (col, desc) = if let Some(c) = arg.strip_prefix('-') { (c, true) } else { (arg, false) };
            return Some(Box::new(Sort { col_name: col.to_string(), descending: desc }));
        }
        "sort_desc" | "sortdesc" => return Some(Box::new(Sort { col_name: arg.to_string(), descending: true })),
        "take" => return arg.parse().ok().map(|n| Box::new(Take { n }) as Box<dyn command::Command>),
        "to_time" => return Some(Box::new(ToTime { col_name: arg.to_string() })),
        "xkey" => return Some(Box::new(Xkey { col_names: arg.split(',').map(|s| s.trim().to_string()).collect() })),
        "rename" => {
            let rename_parts: Vec<&str> = arg.splitn(2, ' ').collect();
            if rename_parts.len() == 2 {
                return Some(Box::new(RenameCol {
                    old_name: rename_parts[0].to_string(),
                    new_name: rename_parts[1].to_string(),
                }));
            }
            return None;
        }
        "goto" => return Some(Box::new(Goto { arg: arg.to_string() })),
        "goto_col" | "gotocol" => return Some(Box::new(GotoCol { arg: arg.to_string() })),
        "toggle_info" => return Some(Box::new(ToggleInfo)),
        "decimals" => return arg.parse().ok().map(|d| Box::new(Decimals { delta: d }) as Box<dyn command::Command>),
        "toggle_sel" => return Some(Box::new(ToggleSel)),
        "clear_sel" => return Some(Box::new(ClearSel)),
        "sel_all" => return Some(Box::new(SelAll)),
        "sel_rows" => return Some(Box::new(SelRows { expr: arg.to_string() })),
        "pop" => return Some(Box::new(Pop)),
        _ => {}
    }

    // Try plugin commands (parse method)
    if let Some(c) = app.plugins.parse(&cmd, arg) { return Some(c); }

    // Try plugin handle for context-dependent commands (enter, delete_sel, etc.)
    if let Some(name) = app.view().map(|v| v.name.clone()) {
        let plugins = std::mem::take(&mut app.plugins);
        let result = plugins.handle(&name, &cmd, app);
        app.plugins = plugins;
        if result.is_some() { return result; }
    }
    None
}

  src/main.rs:30-122 function main <-> src/main.rs:179-237 function run_keys
  Similarity: 89.79%

[36m--- src/main.rs:main (lines 30-122) ---[0m
fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();

    // Parse flags first (before early returns)
    let raw_save = args.iter().any(|a| a == "--raw");

    // Check for --script argument
    if let Some(idx) = args.iter().position(|a| a == "--script") {
        if args.len() <= idx + 1 {
            eprintln!("Usage: tv --script <script_file>");
            std::process::exit(1);
        }
        return run_script(&args[idx + 1]);
    }

    // Check for --keys argument (key replay mode with immutable keymap)
    if let Some(idx) = args.iter().position(|a| a == "--keys") {
        if args.len() <= idx + 1 {
            eprintln!("Usage: tv --keys 'F<ret>' file.parquet");
            std::process::exit(1);
        }
        let file = args.get(idx + 2).map(|s| s.as_str());
        return run_keys(&args[idx + 1], file);
    }

    // Initialize ratatui terminal
    let mut tui = render::init()?;

    // Get file path (first non-flag argument after program name)
    let file_arg = args.iter().skip(1).find(|a| !a.starts_with('-'));

    // Create app context
    let mut app = if let Some(path) = file_arg {
        // Load file from CLI argument
        let mut temp_app = AppContext::new();
        temp_app.raw_save = raw_save;
        match CommandExecutor::exec(&mut temp_app, Box::new(From { file_path: path.clone() })) {
            Ok(_) => temp_app,
            Err(e) => {
                render::restore()?;
                eprintln!("Error loading file: {}", e);
                std::process::exit(1);
            }
        }
    } else {
        let mut temp_app = AppContext::new();
        temp_app.raw_save = raw_save;
        temp_app
    };

    // Update viewport
    let size = tui.size()?;
    app.viewport(size.height, size.width);

    // Main event loop
    loop {
        // Check background tasks
        app.merge_bg_data();
        app.check_bg_saver();
        app.check_bg_meta();
        app.check_bg_freq();

        // Force full redraw if needed (after bat/less/fzf return)
        if app.needs_redraw {
            tui.clear()?;
            // Update viewport in case terminal size changed
            let size = tui.size()?;
            app.viewport(size.height, size.width);
            app.needs_redraw = false;
        }
        // Center cursor if needed (after search, with fresh viewport)
        if app.needs_center {
            if let Some(view) = app.view_mut() {
                view.state.center_if_needed();
            }
            app.needs_center = false;
        }
        // Render with ratatui diff-based update
        tui.draw(|frame| Renderer::render(frame, &mut app))?;

        // Poll for events with timeout (allows background data merge)
        if event::poll(std::time::Duration::from_millis(100))? {
            if let Event::Key(key) = event::read()? {
                if !on_key(&mut app, key)? {
                    break;
                }
            }
        }
    }

    render::restore()?;
    Ok(())
}

[36m--- src/main.rs:run_keys (lines 179-237) ---[0m
fn run_keys(keys: &str, file: Option<&str>) -> Result<()> {
    let mut app = AppContext::new();
    if let Some(path) = file {
        if let Err(e) = CommandExecutor::exec(&mut app, Box::new(From { file_path: path.to_string() })) {
            eprintln!("Error loading {}: {}", path, e);
        }
    }
    app.viewport(50, 120);  // set after load so first view gets viewport
    let mut mode = InputMode::None;
    let mut buf = String::new();

    for key in parse_keys(keys) {
        if mode != InputMode::None {
            // Text input mode - <lt> → <, <gt> → >, <space> → space
            if key == "<ret>" {
                exec_input(&mut app, &mode, &buf);
                mode = InputMode::None;
                buf.clear();
            } else if key == "<esc>" {
                mode = InputMode::None;
                buf.clear();
            } else if key == "<backspace>" {
                buf.pop();
            } else if key == "<lt>" {
                buf.push('<');
            } else if key == "<gt>" {
                buf.push('>');
            } else if key == "<space>" {
                buf.push(' ');
            } else if !key.starts_with('<') {
                buf.push_str(&key);
            }
        } else {
            // Normal mode - check for input-triggering keys
            let tab = cur_tab(&app);
            let cmd = app.keymap.get_command(tab, &key).map(|s| s.to_string());
            if let Some(cmd) = cmd {
                mode = match cmd.as_str() {
                    "search" => InputMode::Search,
                    "filter" => InputMode::Filter,
                    "from" => InputMode::Load,
                    "save" => InputMode::Save,
                    "command" => InputMode::Command,
                    "goto_row" => InputMode::Goto,
                    "goto_col" => InputMode::GotoCol,
                    "select_cols" => InputMode::Select,
                    "rename" => InputMode::Rename,
                    _ => { let _ = handle_cmd(&mut app, &cmd); InputMode::None }
                };
            } else {
                eprintln!("No binding for key '{}' in tab '{}'", key, tab);
            }
        }
    }
    wait_bg_save(&mut app);
    wait_bg_meta(&mut app);
    print(&mut app);
    Ok(())
}

  src/main.rs:179-237 function run_keys <-> src/main.rs:911-957 function prompt
  Similarity: 88.34%

[36m--- src/main.rs:run_keys (lines 179-237) ---[0m
fn run_keys(keys: &str, file: Option<&str>) -> Result<()> {
    let mut app = AppContext::new();
    if let Some(path) = file {
        if let Err(e) = CommandExecutor::exec(&mut app, Box::new(From { file_path: path.to_string() })) {
            eprintln!("Error loading {}: {}", path, e);
        }
    }
    app.viewport(50, 120);  // set after load so first view gets viewport
    let mut mode = InputMode::None;
    let mut buf = String::new();

    for key in parse_keys(keys) {
        if mode != InputMode::None {
            // Text input mode - <lt> → <, <gt> → >, <space> → space
            if key == "<ret>" {
                exec_input(&mut app, &mode, &buf);
                mode = InputMode::None;
                buf.clear();
            } else if key == "<esc>" {
                mode = InputMode::None;
                buf.clear();
            } else if key == "<backspace>" {
                buf.pop();
            } else if key == "<lt>" {
                buf.push('<');
            } else if key == "<gt>" {
                buf.push('>');
            } else if key == "<space>" {
                buf.push(' ');
            } else if !key.starts_with('<') {
                buf.push_str(&key);
            }
        } else {
            // Normal mode - check for input-triggering keys
            let tab = cur_tab(&app);
            let cmd = app.keymap.get_command(tab, &key).map(|s| s.to_string());
            if let Some(cmd) = cmd {
                mode = match cmd.as_str() {
                    "search" => InputMode::Search,
                    "filter" => InputMode::Filter,
                    "from" => InputMode::Load,
                    "save" => InputMode::Save,
                    "command" => InputMode::Command,
                    "goto_row" => InputMode::Goto,
                    "goto_col" => InputMode::GotoCol,
                    "select_cols" => InputMode::Select,
                    "rename" => InputMode::Rename,
                    _ => { let _ = handle_cmd(&mut app, &cmd); InputMode::None }
                };
            } else {
                eprintln!("No binding for key '{}' in tab '{}'", key, tab);
            }
        }
    }
    wait_bg_save(&mut app);
    wait_bg_meta(&mut app);
    print(&mut app);
    Ok(())
}

[36m--- src/main.rs:prompt (lines 911-957) ---[0m
fn prompt(_app: &mut AppContext, prompt: &str) -> Result<Option<String>> {
    // Show prompt at bottom (screen already rendered by main loop)
    let (_cols, rows) = terminal::size()?;
    execute!(
        io::stdout(),
        cursor::MoveTo(0, rows - 1),
        terminal::Clear(terminal::ClearType::CurrentLine),
        Print(prompt),
        cursor::Show
    )?;
    io::stdout().flush()?;

    let mut input = String::new();

    loop {
        if let Event::Key(key) = event::read()? {
            match key.code {
                KeyCode::Enter => {
                    execute!(io::stdout(), cursor::Hide)?;
                    return Ok(Some(input));
                }
                KeyCode::Esc => {
                    execute!(io::stdout(), cursor::Hide)?;
                    return Ok(None);
                }
                KeyCode::Backspace => {
                    input.pop();
                    // Re-render prompt
                    execute!(
                        io::stdout(),
                        cursor::MoveTo(0, rows - 1),
                        terminal::Clear(terminal::ClearType::CurrentLine),
                        Print(prompt),
                        Print(&input)
                    )?;
                    io::stdout().flush()?;
                }
                KeyCode::Char(c) => {
                    input.push(c);
                    execute!(io::stdout(), Print(c))?;
                    io::stdout().flush()?;
                }
                _ => {}
            }
        }
    }
}

  src/main.rs:179-237 function run_keys <-> src/main.rs:811-842 function hints
  Similarity: 90.43%

[36m--- src/main.rs:run_keys (lines 179-237) ---[0m
fn run_keys(keys: &str, file: Option<&str>) -> Result<()> {
    let mut app = AppContext::new();
    if let Some(path) = file {
        if let Err(e) = CommandExecutor::exec(&mut app, Box::new(From { file_path: path.to_string() })) {
            eprintln!("Error loading {}: {}", path, e);
        }
    }
    app.viewport(50, 120);  // set after load so first view gets viewport
    let mut mode = InputMode::None;
    let mut buf = String::new();

    for key in parse_keys(keys) {
        if mode != InputMode::None {
            // Text input mode - <lt> → <, <gt> → >, <space> → space
            if key == "<ret>" {
                exec_input(&mut app, &mode, &buf);
                mode = InputMode::None;
                buf.clear();
            } else if key == "<esc>" {
                mode = InputMode::None;
                buf.clear();
            } else if key == "<backspace>" {
                buf.pop();
            } else if key == "<lt>" {
                buf.push('<');
            } else if key == "<gt>" {
                buf.push('>');
            } else if key == "<space>" {
                buf.push(' ');
            } else if !key.starts_with('<') {
                buf.push_str(&key);
            }
        } else {
            // Normal mode - check for input-triggering keys
            let tab = cur_tab(&app);
            let cmd = app.keymap.get_command(tab, &key).map(|s| s.to_string());
            if let Some(cmd) = cmd {
                mode = match cmd.as_str() {
                    "search" => InputMode::Search,
                    "filter" => InputMode::Filter,
                    "from" => InputMode::Load,
                    "save" => InputMode::Save,
                    "command" => InputMode::Command,
                    "goto_row" => InputMode::Goto,
                    "goto_col" => InputMode::GotoCol,
                    "select_cols" => InputMode::Select,
                    "rename" => InputMode::Rename,
                    _ => { let _ = handle_cmd(&mut app, &cmd); InputMode::None }
                };
            } else {
                eprintln!("No binding for key '{}' in tab '{}'", key, tab);
            }
        }
    }
    wait_bg_save(&mut app);
    wait_bg_meta(&mut app);
    print(&mut app);
    Ok(())
}

[36m--- src/main.rs:hints (lines 811-842) ---[0m
fn hints(df: &polars::prelude::DataFrame, col_name: &str, row: usize, file: Option<&str>) -> Vec<String> {
    use polars::prelude::DataType;
    let mut items = Vec::new();
    let Ok(col) = df.column(col_name) else { return items };
    let dtype = col.dtype();
    let is_str = matches!(dtype, DataType::String);
    let is_datetime = matches!(dtype, DataType::Date | DataType::Datetime(_, _) | DataType::Time);

    // Distinct values: from disk for parquet, else from memory
    if let Some(path) = file.filter(|f| f.ends_with(".parquet")) {
        if let Ok(vals) = backend::Polars.distinct(path, col_name) {
            items.extend(vals.into_iter().map(|v| unquote(&v)).filter(|v| v != "null"));
        }
    } else if let Ok(uniq) = col.unique() {
        for i in 0..uniq.len() {
            if let Ok(v) = uniq.get(i) {
                let val = unquote(&v.to_string());
                if val == "null" { continue; }
                items.push(val);
            }
        }
    }

    // Sort distinct values
    items.sort();

    // PRQL hints only if enabled in cfg/config.csv
    if theme::load_config_value("prql_hints").map(|v| v == "true").unwrap_or(false) {
        items.extend(prql_hints(col, col_name, row, is_str, is_datetime));
    }
    items
}

  src/main.rs:179-237 function run_keys <-> src/main.rs:845-880 function prql_hints
  Similarity: 86.60%

[36m--- src/main.rs:run_keys (lines 179-237) ---[0m
fn run_keys(keys: &str, file: Option<&str>) -> Result<()> {
    let mut app = AppContext::new();
    if let Some(path) = file {
        if let Err(e) = CommandExecutor::exec(&mut app, Box::new(From { file_path: path.to_string() })) {
            eprintln!("Error loading {}: {}", path, e);
        }
    }
    app.viewport(50, 120);  // set after load so first view gets viewport
    let mut mode = InputMode::None;
    let mut buf = String::new();

    for key in parse_keys(keys) {
        if mode != InputMode::None {
            // Text input mode - <lt> → <, <gt> → >, <space> → space
            if key == "<ret>" {
                exec_input(&mut app, &mode, &buf);
                mode = InputMode::None;
                buf.clear();
            } else if key == "<esc>" {
                mode = InputMode::None;
                buf.clear();
            } else if key == "<backspace>" {
                buf.pop();
            } else if key == "<lt>" {
                buf.push('<');
            } else if key == "<gt>" {
                buf.push('>');
            } else if key == "<space>" {
                buf.push(' ');
            } else if !key.starts_with('<') {
                buf.push_str(&key);
            }
        } else {
            // Normal mode - check for input-triggering keys
            let tab = cur_tab(&app);
            let cmd = app.keymap.get_command(tab, &key).map(|s| s.to_string());
            if let Some(cmd) = cmd {
                mode = match cmd.as_str() {
                    "search" => InputMode::Search,
                    "filter" => InputMode::Filter,
                    "from" => InputMode::Load,
                    "save" => InputMode::Save,
                    "command" => InputMode::Command,
                    "goto_row" => InputMode::Goto,
                    "goto_col" => InputMode::GotoCol,
                    "select_cols" => InputMode::Select,
                    "rename" => InputMode::Rename,
                    _ => { let _ = handle_cmd(&mut app, &cmd); InputMode::None }
                };
            } else {
                eprintln!("No binding for key '{}' in tab '{}'", key, tab);
            }
        }
    }
    wait_bg_save(&mut app);
    wait_bg_meta(&mut app);
    print(&mut app);
    Ok(())
}

[36m--- src/main.rs:prql_hints (lines 845-880) ---[0m
fn prql_hints(col: &polars::prelude::Column, col_name: &str, row: usize, is_str: bool, is_datetime: bool) -> Vec<String> {
    let mut items = Vec::new();
    if is_str {
        if let Ok(val) = col.get(row) {
            let v = unquote(&val.to_string());
            if v.len() >= 2 {
                items.push(format!("({} | text.starts_with '{}')", col_name, &v[..2]));
                items.push(format!("({} | text.ends_with '{}')", col_name, &v[v.len()-2..]));
                items.push(format!("({} | text.contains '{}')", col_name, &v[..v.len().min(4)]));
            }
        }
    } else if is_datetime {
        if let Ok(val) = col.get(row) {
            let v = val.to_string();
            if let Some((year, rest)) = v.split_once('-') {
                if let Ok(y) = year.parse::<i32>() {
                    items.push(format!("{} >= @{}-01-01 && {} < @{}-01-01", col_name, y, col_name, y + 1));
                    if let Some((month, _)) = rest.split_once('-') {
                        if let Ok(m) = month.parse::<u32>() {
                            let (ny, nm) = if m >= 12 { (y + 1, 1) } else { (y, m + 1) };
                            items.push(format!("{} >= @{}-{:02}-01 && {} < @{}-{:02}-01", col_name, y, m, col_name, ny, nm));
                        }
                    }
                }
            }
        }
    } else if let Ok(val) = col.get(row) {
        let v = val.to_string();
        if v != "null" {
            items.push(format!("{} > {}", col_name, v));
            items.push(format!("{} < {}", col_name, v));
            items.push(format!("{} >= {} && {} <= {}", col_name, v, col_name, v));
        }
    }
    items
}

  src/main.rs:845-880 function prql_hints <-> src/main.rs:911-957 function prompt
  Similarity: 91.82%

[36m--- src/main.rs:prql_hints (lines 845-880) ---[0m
fn prql_hints(col: &polars::prelude::Column, col_name: &str, row: usize, is_str: bool, is_datetime: bool) -> Vec<String> {
    let mut items = Vec::new();
    if is_str {
        if let Ok(val) = col.get(row) {
            let v = unquote(&val.to_string());
            if v.len() >= 2 {
                items.push(format!("({} | text.starts_with '{}')", col_name, &v[..2]));
                items.push(format!("({} | text.ends_with '{}')", col_name, &v[v.len()-2..]));
                items.push(format!("({} | text.contains '{}')", col_name, &v[..v.len().min(4)]));
            }
        }
    } else if is_datetime {
        if let Ok(val) = col.get(row) {
            let v = val.to_string();
            if let Some((year, rest)) = v.split_once('-') {
                if let Ok(y) = year.parse::<i32>() {
                    items.push(format!("{} >= @{}-01-01 && {} < @{}-01-01", col_name, y, col_name, y + 1));
                    if let Some((month, _)) = rest.split_once('-') {
                        if let Ok(m) = month.parse::<u32>() {
                            let (ny, nm) = if m >= 12 { (y + 1, 1) } else { (y, m + 1) };
                            items.push(format!("{} >= @{}-{:02}-01 && {} < @{}-{:02}-01", col_name, y, m, col_name, ny, nm));
                        }
                    }
                }
            }
        }
    } else if let Ok(val) = col.get(row) {
        let v = val.to_string();
        if v != "null" {
            items.push(format!("{} > {}", col_name, v));
            items.push(format!("{} < {}", col_name, v));
            items.push(format!("{} >= {} && {} <= {}", col_name, v, col_name, v));
        }
    }
    items
}

[36m--- src/main.rs:prompt (lines 911-957) ---[0m
fn prompt(_app: &mut AppContext, prompt: &str) -> Result<Option<String>> {
    // Show prompt at bottom (screen already rendered by main loop)
    let (_cols, rows) = terminal::size()?;
    execute!(
        io::stdout(),
        cursor::MoveTo(0, rows - 1),
        terminal::Clear(terminal::ClearType::CurrentLine),
        Print(prompt),
        cursor::Show
    )?;
    io::stdout().flush()?;

    let mut input = String::new();

    loop {
        if let Event::Key(key) = event::read()? {
            match key.code {
                KeyCode::Enter => {
                    execute!(io::stdout(), cursor::Hide)?;
                    return Ok(Some(input));
                }
                KeyCode::Esc => {
                    execute!(io::stdout(), cursor::Hide)?;
                    return Ok(None);
                }
                KeyCode::Backspace => {
                    input.pop();
                    // Re-render prompt
                    execute!(
                        io::stdout(),
                        cursor::MoveTo(0, rows - 1),
                        terminal::Clear(terminal::ClearType::CurrentLine),
                        Print(prompt),
                        Print(&input)
                    )?;
                    io::stdout().flush()?;
                }
                KeyCode::Char(c) => {
                    input.push(c);
                    execute!(io::stdout(), Print(c))?;
                    io::stdout().flush()?;
                }
                _ => {}
            }
        }
    }
}

  src/main.rs:700-728 function do_search <-> src/main.rs:911-957 function prompt
  Similarity: 89.84%

[36m--- src/main.rs:do_search (lines 700-728) ---[0m
fn do_search(app: &mut AppContext) -> Result<()> {
    let info = app.view().and_then(|v| {
        let col_name = v.col_name(v.state.cc)?;
        let file = v.filename.as_deref();
        Some((hints(&v.dataframe, &col_name, v.state.cr, file), col_name, v.name.starts_with("ls")))
    });
    if let Some((hint_list, col_name, is_folder)) = info {
        let expr_opt = picker::fzf(hint_list, "Search> ");
        app.needs_redraw = true;
        if let Ok(Some(expr)) = expr_opt {
            let prql_mode = theme::load_config_value("prql_hints").map(|v| v == "true").unwrap_or(false);
            let expr = if !prql_mode && is_plain_value(&expr) {
                format!("{} LIKE '%{}%'", col_name, expr)
            } else { expr.to_string() };
            let matches = app.view().map(|v| find(&v.dataframe, &expr)).unwrap_or_default();
            app.search.col_name = None;
            app.search.value = Some(expr.clone());
            let found = if let Some(view) = app.view_mut() {
                if let Some(&pos) = matches.first() {
                    view.state.cr = pos;
                    app.needs_center = true;
                    true
                } else { app.msg(format!("Not found: {}", expr)); false }
            } else { false };
            if found && is_folder { dispatch(app, "enter"); }
        }
    }
    Ok(())
}

[36m--- src/main.rs:prompt (lines 911-957) ---[0m
fn prompt(_app: &mut AppContext, prompt: &str) -> Result<Option<String>> {
    // Show prompt at bottom (screen already rendered by main loop)
    let (_cols, rows) = terminal::size()?;
    execute!(
        io::stdout(),
        cursor::MoveTo(0, rows - 1),
        terminal::Clear(terminal::ClearType::CurrentLine),
        Print(prompt),
        cursor::Show
    )?;
    io::stdout().flush()?;

    let mut input = String::new();

    loop {
        if let Event::Key(key) = event::read()? {
            match key.code {
                KeyCode::Enter => {
                    execute!(io::stdout(), cursor::Hide)?;
                    return Ok(Some(input));
                }
                KeyCode::Esc => {
                    execute!(io::stdout(), cursor::Hide)?;
                    return Ok(None);
                }
                KeyCode::Backspace => {
                    input.pop();
                    // Re-render prompt
                    execute!(
                        io::stdout(),
                        cursor::MoveTo(0, rows - 1),
                        terminal::Clear(terminal::ClearType::CurrentLine),
                        Print(prompt),
                        Print(&input)
                    )?;
                    io::stdout().flush()?;
                }
                KeyCode::Char(c) => {
                    input.push(c);
                    execute!(io::stdout(), Print(c))?;
                    io::stdout().flush()?;
                }
                _ => {}
            }
        }
    }
}

  src/main.rs:179-237 function run_keys <-> src/main.rs:764-781 function do_command_picker
  Similarity: 86.18%

[36m--- src/main.rs:run_keys (lines 179-237) ---[0m
fn run_keys(keys: &str, file: Option<&str>) -> Result<()> {
    let mut app = AppContext::new();
    if let Some(path) = file {
        if let Err(e) = CommandExecutor::exec(&mut app, Box::new(From { file_path: path.to_string() })) {
            eprintln!("Error loading {}: {}", path, e);
        }
    }
    app.viewport(50, 120);  // set after load so first view gets viewport
    let mut mode = InputMode::None;
    let mut buf = String::new();

    for key in parse_keys(keys) {
        if mode != InputMode::None {
            // Text input mode - <lt> → <, <gt> → >, <space> → space
            if key == "<ret>" {
                exec_input(&mut app, &mode, &buf);
                mode = InputMode::None;
                buf.clear();
            } else if key == "<esc>" {
                mode = InputMode::None;
                buf.clear();
            } else if key == "<backspace>" {
                buf.pop();
            } else if key == "<lt>" {
                buf.push('<');
            } else if key == "<gt>" {
                buf.push('>');
            } else if key == "<space>" {
                buf.push(' ');
            } else if !key.starts_with('<') {
                buf.push_str(&key);
            }
        } else {
            // Normal mode - check for input-triggering keys
            let tab = cur_tab(&app);
            let cmd = app.keymap.get_command(tab, &key).map(|s| s.to_string());
            if let Some(cmd) = cmd {
                mode = match cmd.as_str() {
                    "search" => InputMode::Search,
                    "filter" => InputMode::Filter,
                    "from" => InputMode::Load,
                    "save" => InputMode::Save,
                    "command" => InputMode::Command,
                    "goto_row" => InputMode::Goto,
                    "goto_col" => InputMode::GotoCol,
                    "select_cols" => InputMode::Select,
                    "rename" => InputMode::Rename,
                    _ => { let _ = handle_cmd(&mut app, &cmd); InputMode::None }
                };
            } else {
                eprintln!("No binding for key '{}' in tab '{}'", key, tab);
            }
        }
    }
    wait_bg_save(&mut app);
    wait_bg_meta(&mut app);
    print(&mut app);
    Ok(())
}

[36m--- src/main.rs:do_command_picker (lines 764-781) ---[0m
fn do_command_picker(app: &mut AppContext) -> Result<()> {
    let cmd_list: Vec<String> = vec![
        "from <file>", "save <file>", "ls [dir]", "lr [dir]",
        "ps", "mounts", "tcp", "udp", "lsof [pid]", "env",
        "systemctl", "journalctl [n]", "pacman", "cargo",
        "filter <expr>", "freq <col>", "meta", "corr",
        "select <cols>", "delcol <cols>", "sort <col>", "sort -<col>", "take <n>", "rename <old> <new>",
    ].iter().map(|s| s.to_string()).collect();
    let result = picker::fzf(cmd_list, ": ");
    app.needs_redraw = true;
    if let Ok(Some(selected)) = result {
        let cmd_str = selected.split_whitespace().next().unwrap_or(&selected);
        if let Some(cmd) = parse(cmd_str, app).or_else(|| parse(&selected, app)) {
            if let Err(e) = CommandExecutor::exec(app, cmd) { app.err(e); }
        } else { app.msg(format!("Unknown command: {}", selected)); }
    }
    Ok(())
}

  src/main.rs:125-147 function run_batch <-> src/main.rs:911-957 function prompt
  Similarity: 89.00%

[36m--- src/main.rs:run_batch (lines 125-147) ---[0m
fn run_batch<I: Iterator<Item = String>>(lines: I) -> Result<()> {
    let mut app = AppContext::new();
    app.viewport(50, 120);
    'outer: for line in lines {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') { continue; }
        for cmd_str in line.split('|').map(str::trim) {
            if cmd_str.is_empty() { continue; }
            if cmd_str == "quit" { break 'outer; }
            if let Some(cmd) = parse(cmd_str, &mut app) {
                if let Err(e) = CommandExecutor::exec(&mut app, cmd) {
                    eprintln!("Error executing '{}': {}", cmd_str, e);
                }
            } else {
                eprintln!("Unknown command: {}", cmd_str);
            }
        }
    }
    wait_bg_save(&mut app);
    wait_bg_meta(&mut app);
    print(&mut app);
    Ok(())
}

[36m--- src/main.rs:prompt (lines 911-957) ---[0m
fn prompt(_app: &mut AppContext, prompt: &str) -> Result<Option<String>> {
    // Show prompt at bottom (screen already rendered by main loop)
    let (_cols, rows) = terminal::size()?;
    execute!(
        io::stdout(),
        cursor::MoveTo(0, rows - 1),
        terminal::Clear(terminal::ClearType::CurrentLine),
        Print(prompt),
        cursor::Show
    )?;
    io::stdout().flush()?;

    let mut input = String::new();

    loop {
        if let Event::Key(key) = event::read()? {
            match key.code {
                KeyCode::Enter => {
                    execute!(io::stdout(), cursor::Hide)?;
                    return Ok(Some(input));
                }
                KeyCode::Esc => {
                    execute!(io::stdout(), cursor::Hide)?;
                    return Ok(None);
                }
                KeyCode::Backspace => {
                    input.pop();
                    // Re-render prompt
                    execute!(
                        io::stdout(),
                        cursor::MoveTo(0, rows - 1),
                        terminal::Clear(terminal::ClearType::CurrentLine),
                        Print(prompt),
                        Print(&input)
                    )?;
                    io::stdout().flush()?;
                }
                KeyCode::Char(c) => {
                    input.push(c);
                    execute!(io::stdout(), Print(c))?;
                    io::stdout().flush()?;
                }
                _ => {}
            }
        }
    }
}

  src/main.rs:700-728 function do_search <-> src/main.rs:845-880 function prql_hints
  Similarity: 93.85%

[36m--- src/main.rs:do_search (lines 700-728) ---[0m
fn do_search(app: &mut AppContext) -> Result<()> {
    let info = app.view().and_then(|v| {
        let col_name = v.col_name(v.state.cc)?;
        let file = v.filename.as_deref();
        Some((hints(&v.dataframe, &col_name, v.state.cr, file), col_name, v.name.starts_with("ls")))
    });
    if let Some((hint_list, col_name, is_folder)) = info {
        let expr_opt = picker::fzf(hint_list, "Search> ");
        app.needs_redraw = true;
        if let Ok(Some(expr)) = expr_opt {
            let prql_mode = theme::load_config_value("prql_hints").map(|v| v == "true").unwrap_or(false);
            let expr = if !prql_mode && is_plain_value(&expr) {
                format!("{} LIKE '%{}%'", col_name, expr)
            } else { expr.to_string() };
            let matches = app.view().map(|v| find(&v.dataframe, &expr)).unwrap_or_default();
            app.search.col_name = None;
            app.search.value = Some(expr.clone());
            let found = if let Some(view) = app.view_mut() {
                if let Some(&pos) = matches.first() {
                    view.state.cr = pos;
                    app.needs_center = true;
                    true
                } else { app.msg(format!("Not found: {}", expr)); false }
            } else { false };
            if found && is_folder { dispatch(app, "enter"); }
        }
    }
    Ok(())
}

[36m--- src/main.rs:prql_hints (lines 845-880) ---[0m
fn prql_hints(col: &polars::prelude::Column, col_name: &str, row: usize, is_str: bool, is_datetime: bool) -> Vec<String> {
    let mut items = Vec::new();
    if is_str {
        if let Ok(val) = col.get(row) {
            let v = unquote(&val.to_string());
            if v.len() >= 2 {
                items.push(format!("({} | text.starts_with '{}')", col_name, &v[..2]));
                items.push(format!("({} | text.ends_with '{}')", col_name, &v[v.len()-2..]));
                items.push(format!("({} | text.contains '{}')", col_name, &v[..v.len().min(4)]));
            }
        }
    } else if is_datetime {
        if let Ok(val) = col.get(row) {
            let v = val.to_string();
            if let Some((year, rest)) = v.split_once('-') {
                if let Ok(y) = year.parse::<i32>() {
                    items.push(format!("{} >= @{}-01-01 && {} < @{}-01-01", col_name, y, col_name, y + 1));
                    if let Some((month, _)) = rest.split_once('-') {
                        if let Ok(m) = month.parse::<u32>() {
                            let (ny, nm) = if m >= 12 { (y + 1, 1) } else { (y, m + 1) };
                            items.push(format!("{} >= @{}-{:02}-01 && {} < @{}-{:02}-01", col_name, y, m, col_name, ny, nm));
                        }
                    }
                }
            }
        }
    } else if let Ok(val) = col.get(row) {
        let v = val.to_string();
        if v != "null" {
            items.push(format!("{} > {}", col_name, v));
            items.push(format!("{} < {}", col_name, v));
            items.push(format!("{} >= {} && {} <= {}", col_name, v, col_name, v));
        }
    }
    items
}

  src/main.rs:764-781 function do_command_picker <-> src/main.rs:911-957 function prompt
  Similarity: 90.98%

[36m--- src/main.rs:do_command_picker (lines 764-781) ---[0m
fn do_command_picker(app: &mut AppContext) -> Result<()> {
    let cmd_list: Vec<String> = vec![
        "from <file>", "save <file>", "ls [dir]", "lr [dir]",
        "ps", "mounts", "tcp", "udp", "lsof [pid]", "env",
        "systemctl", "journalctl [n]", "pacman", "cargo",
        "filter <expr>", "freq <col>", "meta", "corr",
        "select <cols>", "delcol <cols>", "sort <col>", "sort -<col>", "take <n>", "rename <old> <new>",
    ].iter().map(|s| s.to_string()).collect();
    let result = picker::fzf(cmd_list, ": ");
    app.needs_redraw = true;
    if let Ok(Some(selected)) = result {
        let cmd_str = selected.split_whitespace().next().unwrap_or(&selected);
        if let Some(cmd) = parse(cmd_str, app).or_else(|| parse(&selected, app)) {
            if let Err(e) = CommandExecutor::exec(app, cmd) { app.err(e); }
        } else { app.msg(format!("Unknown command: {}", selected)); }
    }
    Ok(())
}

[36m--- src/main.rs:prompt (lines 911-957) ---[0m
fn prompt(_app: &mut AppContext, prompt: &str) -> Result<Option<String>> {
    // Show prompt at bottom (screen already rendered by main loop)
    let (_cols, rows) = terminal::size()?;
    execute!(
        io::stdout(),
        cursor::MoveTo(0, rows - 1),
        terminal::Clear(terminal::ClearType::CurrentLine),
        Print(prompt),
        cursor::Show
    )?;
    io::stdout().flush()?;

    let mut input = String::new();

    loop {
        if let Event::Key(key) = event::read()? {
            match key.code {
                KeyCode::Enter => {
                    execute!(io::stdout(), cursor::Hide)?;
                    return Ok(Some(input));
                }
                KeyCode::Esc => {
                    execute!(io::stdout(), cursor::Hide)?;
                    return Ok(None);
                }
                KeyCode::Backspace => {
                    input.pop();
                    // Re-render prompt
                    execute!(
                        io::stdout(),
                        cursor::MoveTo(0, rows - 1),
                        terminal::Clear(terminal::ClearType::CurrentLine),
                        Print(prompt),
                        Print(&input)
                    )?;
                    io::stdout().flush()?;
                }
                KeyCode::Char(c) => {
                    input.push(c);
                    execute!(io::stdout(), Print(c))?;
                    io::stdout().flush()?;
                }
                _ => {}
            }
        }
    }
}

  src/main.rs:894-908 function find <-> src/main.rs:911-957 function prompt
  Similarity: 92.03%

[36m--- src/main.rs:find (lines 894-908) ---[0m
fn find(df: &polars::prelude::DataFrame, expr: &str) -> Vec<usize> {
    use polars::prelude::*;
    let mut ctx = polars::sql::SQLContext::new();
    let with_idx = df.clone().lazy().with_row_index("__idx__", None);
    ctx.register("df", with_idx);
    ctx.execute(&format!("SELECT __idx__ FROM df WHERE {}", expr))
        .and_then(|lf| lf.collect())
        .map(|result| {
            result.column("__idx__").ok()
                .and_then(|c| c.idx().ok())
                .map(|idx| idx.into_iter().filter_map(|v| v.map(|i| i as usize)).collect())
                .unwrap_or_default()
        })
        .unwrap_or_default()
}

[36m--- src/main.rs:prompt (lines 911-957) ---[0m
fn prompt(_app: &mut AppContext, prompt: &str) -> Result<Option<String>> {
    // Show prompt at bottom (screen already rendered by main loop)
    let (_cols, rows) = terminal::size()?;
    execute!(
        io::stdout(),
        cursor::MoveTo(0, rows - 1),
        terminal::Clear(terminal::ClearType::CurrentLine),
        Print(prompt),
        cursor::Show
    )?;
    io::stdout().flush()?;

    let mut input = String::new();

    loop {
        if let Event::Key(key) = event::read()? {
            match key.code {
                KeyCode::Enter => {
                    execute!(io::stdout(), cursor::Hide)?;
                    return Ok(Some(input));
                }
                KeyCode::Esc => {
                    execute!(io::stdout(), cursor::Hide)?;
                    return Ok(None);
                }
                KeyCode::Backspace => {
                    input.pop();
                    // Re-render prompt
                    execute!(
                        io::stdout(),
                        cursor::MoveTo(0, rows - 1),
                        terminal::Clear(terminal::ClearType::CurrentLine),
                        Print(prompt),
                        Print(&input)
                    )?;
                    io::stdout().flush()?;
                }
                KeyCode::Char(c) => {
                    input.push(c);
                    execute!(io::stdout(), Print(c))?;
                    io::stdout().flush()?;
                }
                _ => {}
            }
        }
    }
}

  src/main.rs:472-493 function key_str <-> src/main.rs:845-880 function prql_hints
  Similarity: 95.23%

[36m--- src/main.rs:key_str (lines 472-493) ---[0m
fn key_str(key: &KeyEvent) -> String {
    let ctrl = key.modifiers.contains(KeyModifiers::CONTROL);
    match key.code {
        KeyCode::Char(c) if ctrl => format!("<c-{}>", c.to_ascii_lowercase()),
        KeyCode::Char(c) => c.to_string(),
        KeyCode::Enter => "<ret>".into(),
        KeyCode::Esc => "<esc>".into(),
        KeyCode::Up => "<up>".into(),
        KeyCode::Down => "<down>".into(),
        KeyCode::Left => "<left>".into(),
        KeyCode::Right => "<right>".into(),
        KeyCode::Home => "<home>".into(),
        KeyCode::End => "<end>".into(),
        KeyCode::PageUp => "<pageup>".into(),
        KeyCode::PageDown => "<pagedown>".into(),
        KeyCode::Tab => "<tab>".into(),
        KeyCode::BackTab => "<s-tab>".into(),
        KeyCode::Delete => "<del>".into(),
        KeyCode::Backspace => "<backspace>".into(),
        _ => "?".into(),
    }
}

[36m--- src/main.rs:prql_hints (lines 845-880) ---[0m
fn prql_hints(col: &polars::prelude::Column, col_name: &str, row: usize, is_str: bool, is_datetime: bool) -> Vec<String> {
    let mut items = Vec::new();
    if is_str {
        if let Ok(val) = col.get(row) {
            let v = unquote(&val.to_string());
            if v.len() >= 2 {
                items.push(format!("({} | text.starts_with '{}')", col_name, &v[..2]));
                items.push(format!("({} | text.ends_with '{}')", col_name, &v[v.len()-2..]));
                items.push(format!("({} | text.contains '{}')", col_name, &v[..v.len().min(4)]));
            }
        }
    } else if is_datetime {
        if let Ok(val) = col.get(row) {
            let v = val.to_string();
            if let Some((year, rest)) = v.split_once('-') {
                if let Ok(y) = year.parse::<i32>() {
                    items.push(format!("{} >= @{}-01-01 && {} < @{}-01-01", col_name, y, col_name, y + 1));
                    if let Some((month, _)) = rest.split_once('-') {
                        if let Ok(m) = month.parse::<u32>() {
                            let (ny, nm) = if m >= 12 { (y + 1, 1) } else { (y, m + 1) };
                            items.push(format!("{} >= @{}-{:02}-01 && {} < @{}-{:02}-01", col_name, y, m, col_name, ny, nm));
                        }
                    }
                }
            }
        }
    } else if let Ok(val) = col.get(row) {
        let v = val.to_string();
        if v != "null" {
            items.push(format!("{} > {}", col_name, v));
            items.push(format!("{} < {}", col_name, v));
            items.push(format!("{} >= {} && {} <= {}", col_name, v, col_name, v));
        }
    }
    items
}

  src/main.rs:731-747 function do_filter <-> src/main.rs:845-880 function prql_hints
  Similarity: 96.40%

[36m--- src/main.rs:do_filter (lines 731-747) ---[0m
fn do_filter(app: &mut AppContext) -> Result<()> {
    let info = app.view().and_then(|v| {
        let col_name = v.col_name(v.state.cc)?;
        let is_str = v.dataframe.column(&col_name).ok()
            .map(|c| matches!(c.dtype(), polars::prelude::DataType::String)).unwrap_or(false);
        let file = v.filename.as_deref();
        Some((hints(&v.dataframe, &col_name, v.state.cr, file), col_name, is_str))
    });
    if let Some((hint_list, col_name, is_str)) = info {
        let expr_opt = picker::fzf_filter(hint_list, "WHERE> ", &col_name, is_str);
        app.needs_redraw = true;
        if let Ok(Some(expr)) = expr_opt {
            run(app, Box::new(Filter { expr }));
        }
    } else { app.no_table(); }
    Ok(())
}

[36m--- src/main.rs:prql_hints (lines 845-880) ---[0m
fn prql_hints(col: &polars::prelude::Column, col_name: &str, row: usize, is_str: bool, is_datetime: bool) -> Vec<String> {
    let mut items = Vec::new();
    if is_str {
        if let Ok(val) = col.get(row) {
            let v = unquote(&val.to_string());
            if v.len() >= 2 {
                items.push(format!("({} | text.starts_with '{}')", col_name, &v[..2]));
                items.push(format!("({} | text.ends_with '{}')", col_name, &v[v.len()-2..]));
                items.push(format!("({} | text.contains '{}')", col_name, &v[..v.len().min(4)]));
            }
        }
    } else if is_datetime {
        if let Ok(val) = col.get(row) {
            let v = val.to_string();
            if let Some((year, rest)) = v.split_once('-') {
                if let Ok(y) = year.parse::<i32>() {
                    items.push(format!("{} >= @{}-01-01 && {} < @{}-01-01", col_name, y, col_name, y + 1));
                    if let Some((month, _)) = rest.split_once('-') {
                        if let Ok(m) = month.parse::<u32>() {
                            let (ny, nm) = if m >= 12 { (y + 1, 1) } else { (y, m + 1) };
                            items.push(format!("{} >= @{}-{:02}-01 && {} < @{}-{:02}-01", col_name, y, m, col_name, ny, nm));
                        }
                    }
                }
            }
        }
    } else if let Ok(val) = col.get(row) {
        let v = val.to_string();
        if v != "null" {
            items.push(format!("{} > {}", col_name, v));
            items.push(format!("{} < {}", col_name, v));
            items.push(format!("{} >= {} && {} <= {}", col_name, v, col_name, v));
        }
    }
    items
}

  src/main.rs:764-781 function do_command_picker <-> src/main.rs:845-880 function prql_hints
  Similarity: 93.59%

[36m--- src/main.rs:do_command_picker (lines 764-781) ---[0m
fn do_command_picker(app: &mut AppContext) -> Result<()> {
    let cmd_list: Vec<String> = vec![
        "from <file>", "save <file>", "ls [dir]", "lr [dir]",
        "ps", "mounts", "tcp", "udp", "lsof [pid]", "env",
        "systemctl", "journalctl [n]", "pacman", "cargo",
        "filter <expr>", "freq <col>", "meta", "corr",
        "select <cols>", "delcol <cols>", "sort <col>", "sort -<col>", "take <n>", "rename <old> <new>",
    ].iter().map(|s| s.to_string()).collect();
    let result = picker::fzf(cmd_list, ": ");
    app.needs_redraw = true;
    if let Ok(Some(selected)) = result {
        let cmd_str = selected.split_whitespace().next().unwrap_or(&selected);
        if let Some(cmd) = parse(cmd_str, app).or_else(|| parse(&selected, app)) {
            if let Err(e) = CommandExecutor::exec(app, cmd) { app.err(e); }
        } else { app.msg(format!("Unknown command: {}", selected)); }
    }
    Ok(())
}

[36m--- src/main.rs:prql_hints (lines 845-880) ---[0m
fn prql_hints(col: &polars::prelude::Column, col_name: &str, row: usize, is_str: bool, is_datetime: bool) -> Vec<String> {
    let mut items = Vec::new();
    if is_str {
        if let Ok(val) = col.get(row) {
            let v = unquote(&val.to_string());
            if v.len() >= 2 {
                items.push(format!("({} | text.starts_with '{}')", col_name, &v[..2]));
                items.push(format!("({} | text.ends_with '{}')", col_name, &v[v.len()-2..]));
                items.push(format!("({} | text.contains '{}')", col_name, &v[..v.len().min(4)]));
            }
        }
    } else if is_datetime {
        if let Ok(val) = col.get(row) {
            let v = val.to_string();
            if let Some((year, rest)) = v.split_once('-') {
                if let Ok(y) = year.parse::<i32>() {
                    items.push(format!("{} >= @{}-01-01 && {} < @{}-01-01", col_name, y, col_name, y + 1));
                    if let Some((month, _)) = rest.split_once('-') {
                        if let Ok(m) = month.parse::<u32>() {
                            let (ny, nm) = if m >= 12 { (y + 1, 1) } else { (y, m + 1) };
                            items.push(format!("{} >= @{}-{:02}-01 && {} < @{}-{:02}-01", col_name, y, m, col_name, ny, nm));
                        }
                    }
                }
            }
        }
    } else if let Ok(val) = col.get(row) {
        let v = val.to_string();
        if v != "null" {
            items.push(format!("{} > {}", col_name, v));
            items.push(format!("{} < {}", col_name, v));
            items.push(format!("{} >= {} && {} <= {}", col_name, v, col_name, v));
        }
    }
    items
}

  src/main.rs:125-147 function run_batch <-> src/main.rs:700-728 function do_search
  Similarity: 87.97%

[36m--- src/main.rs:run_batch (lines 125-147) ---[0m
fn run_batch<I: Iterator<Item = String>>(lines: I) -> Result<()> {
    let mut app = AppContext::new();
    app.viewport(50, 120);
    'outer: for line in lines {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') { continue; }
        for cmd_str in line.split('|').map(str::trim) {
            if cmd_str.is_empty() { continue; }
            if cmd_str == "quit" { break 'outer; }
            if let Some(cmd) = parse(cmd_str, &mut app) {
                if let Err(e) = CommandExecutor::exec(&mut app, cmd) {
                    eprintln!("Error executing '{}': {}", cmd_str, e);
                }
            } else {
                eprintln!("Unknown command: {}", cmd_str);
            }
        }
    }
    wait_bg_save(&mut app);
    wait_bg_meta(&mut app);
    print(&mut app);
    Ok(())
}

[36m--- src/main.rs:do_search (lines 700-728) ---[0m
fn do_search(app: &mut AppContext) -> Result<()> {
    let info = app.view().and_then(|v| {
        let col_name = v.col_name(v.state.cc)?;
        let file = v.filename.as_deref();
        Some((hints(&v.dataframe, &col_name, v.state.cr, file), col_name, v.name.starts_with("ls")))
    });
    if let Some((hint_list, col_name, is_folder)) = info {
        let expr_opt = picker::fzf(hint_list, "Search> ");
        app.needs_redraw = true;
        if let Ok(Some(expr)) = expr_opt {
            let prql_mode = theme::load_config_value("prql_hints").map(|v| v == "true").unwrap_or(false);
            let expr = if !prql_mode && is_plain_value(&expr) {
                format!("{} LIKE '%{}%'", col_name, expr)
            } else { expr.to_string() };
            let matches = app.view().map(|v| find(&v.dataframe, &expr)).unwrap_or_default();
            app.search.col_name = None;
            app.search.value = Some(expr.clone());
            let found = if let Some(view) = app.view_mut() {
                if let Some(&pos) = matches.first() {
                    view.state.cr = pos;
                    app.needs_center = true;
                    true
                } else { app.msg(format!("Not found: {}", expr)); false }
            } else { false };
            if found && is_folder { dispatch(app, "enter"); }
        }
    }
    Ok(())
}

  src/main.rs:700-728 function do_search <-> src/main.rs:731-747 function do_filter
  Similarity: 94.38%

[36m--- src/main.rs:do_search (lines 700-728) ---[0m
fn do_search(app: &mut AppContext) -> Result<()> {
    let info = app.view().and_then(|v| {
        let col_name = v.col_name(v.state.cc)?;
        let file = v.filename.as_deref();
        Some((hints(&v.dataframe, &col_name, v.state.cr, file), col_name, v.name.starts_with("ls")))
    });
    if let Some((hint_list, col_name, is_folder)) = info {
        let expr_opt = picker::fzf(hint_list, "Search> ");
        app.needs_redraw = true;
        if let Ok(Some(expr)) = expr_opt {
            let prql_mode = theme::load_config_value("prql_hints").map(|v| v == "true").unwrap_or(false);
            let expr = if !prql_mode && is_plain_value(&expr) {
                format!("{} LIKE '%{}%'", col_name, expr)
            } else { expr.to_string() };
            let matches = app.view().map(|v| find(&v.dataframe, &expr)).unwrap_or_default();
            app.search.col_name = None;
            app.search.value = Some(expr.clone());
            let found = if let Some(view) = app.view_mut() {
                if let Some(&pos) = matches.first() {
                    view.state.cr = pos;
                    app.needs_center = true;
                    true
                } else { app.msg(format!("Not found: {}", expr)); false }
            } else { false };
            if found && is_folder { dispatch(app, "enter"); }
        }
    }
    Ok(())
}

[36m--- src/main.rs:do_filter (lines 731-747) ---[0m
fn do_filter(app: &mut AppContext) -> Result<()> {
    let info = app.view().and_then(|v| {
        let col_name = v.col_name(v.state.cc)?;
        let is_str = v.dataframe.column(&col_name).ok()
            .map(|c| matches!(c.dtype(), polars::prelude::DataType::String)).unwrap_or(false);
        let file = v.filename.as_deref();
        Some((hints(&v.dataframe, &col_name, v.state.cr, file), col_name, is_str))
    });
    if let Some((hint_list, col_name, is_str)) = info {
        let expr_opt = picker::fzf_filter(hint_list, "WHERE> ", &col_name, is_str);
        app.needs_redraw = true;
        if let Ok(Some(expr)) = expr_opt {
            run(app, Box::new(Filter { expr }));
        }
    } else { app.no_table(); }
    Ok(())
}

  src/main.rs:700-728 function do_search <-> src/main.rs:764-781 function do_command_picker
  Similarity: 91.42%

[36m--- src/main.rs:do_search (lines 700-728) ---[0m
fn do_search(app: &mut AppContext) -> Result<()> {
    let info = app.view().and_then(|v| {
        let col_name = v.col_name(v.state.cc)?;
        let file = v.filename.as_deref();
        Some((hints(&v.dataframe, &col_name, v.state.cr, file), col_name, v.name.starts_with("ls")))
    });
    if let Some((hint_list, col_name, is_folder)) = info {
        let expr_opt = picker::fzf(hint_list, "Search> ");
        app.needs_redraw = true;
        if let Ok(Some(expr)) = expr_opt {
            let prql_mode = theme::load_config_value("prql_hints").map(|v| v == "true").unwrap_or(false);
            let expr = if !prql_mode && is_plain_value(&expr) {
                format!("{} LIKE '%{}%'", col_name, expr)
            } else { expr.to_string() };
            let matches = app.view().map(|v| find(&v.dataframe, &expr)).unwrap_or_default();
            app.search.col_name = None;
            app.search.value = Some(expr.clone());
            let found = if let Some(view) = app.view_mut() {
                if let Some(&pos) = matches.first() {
                    view.state.cr = pos;
                    app.needs_center = true;
                    true
                } else { app.msg(format!("Not found: {}", expr)); false }
            } else { false };
            if found && is_folder { dispatch(app, "enter"); }
        }
    }
    Ok(())
}

[36m--- src/main.rs:do_command_picker (lines 764-781) ---[0m
fn do_command_picker(app: &mut AppContext) -> Result<()> {
    let cmd_list: Vec<String> = vec![
        "from <file>", "save <file>", "ls [dir]", "lr [dir]",
        "ps", "mounts", "tcp", "udp", "lsof [pid]", "env",
        "systemctl", "journalctl [n]", "pacman", "cargo",
        "filter <expr>", "freq <col>", "meta", "corr",
        "select <cols>", "delcol <cols>", "sort <col>", "sort -<col>", "take <n>", "rename <old> <new>",
    ].iter().map(|s| s.to_string()).collect();
    let result = picker::fzf(cmd_list, ": ");
    app.needs_redraw = true;
    if let Ok(Some(selected)) = result {
        let cmd_str = selected.split_whitespace().next().unwrap_or(&selected);
        if let Some(cmd) = parse(cmd_str, app).or_else(|| parse(&selected, app)) {
            if let Err(e) = CommandExecutor::exec(app, cmd) { app.err(e); }
        } else { app.msg(format!("Unknown command: {}", selected)); }
    }
    Ok(())
}

  src/main.rs:322-340 function print <-> src/main.rs:472-493 function key_str
  Similarity: 88.10%

[36m--- src/main.rs:print (lines 322-340) ---[0m
fn print(app: &mut AppContext) {
    if let Some(view) = app.view_mut() {
        println!("=== {} ({} rows) ===", view.name, view.rows());
        fetch_lazy(view);
        // Print columns
        let cols = df_cols(&view.dataframe);
        println!("{}", cols.join(","));
        // Print first few rows
        let n = view.dataframe.height().min(10);
        for r in 0..n {
            let row: Vec<String> = (0..cols.len()).map(|c| {
                view.dataframe.get_columns()[c].get(r).map(|v| v.to_string()).unwrap_or_default()
            }).collect();
            println!("{}", row.join(","));
        }
    } else {
        println!("No table loaded");
    }
}

[36m--- src/main.rs:key_str (lines 472-493) ---[0m
fn key_str(key: &KeyEvent) -> String {
    let ctrl = key.modifiers.contains(KeyModifiers::CONTROL);
    match key.code {
        KeyCode::Char(c) if ctrl => format!("<c-{}>", c.to_ascii_lowercase()),
        KeyCode::Char(c) => c.to_string(),
        KeyCode::Enter => "<ret>".into(),
        KeyCode::Esc => "<esc>".into(),
        KeyCode::Up => "<up>".into(),
        KeyCode::Down => "<down>".into(),
        KeyCode::Left => "<left>".into(),
        KeyCode::Right => "<right>".into(),
        KeyCode::Home => "<home>".into(),
        KeyCode::End => "<end>".into(),
        KeyCode::PageUp => "<pageup>".into(),
        KeyCode::PageDown => "<pagedown>".into(),
        KeyCode::Tab => "<tab>".into(),
        KeyCode::BackTab => "<s-tab>".into(),
        KeyCode::Delete => "<del>".into(),
        KeyCode::Backspace => "<backspace>".into(),
        _ => "?".into(),
    }
}

  src/main.rs:472-493 function key_str <-> src/main.rs:784-799 function do_goto_col
  Similarity: 94.04%

[36m--- src/main.rs:key_str (lines 472-493) ---[0m
fn key_str(key: &KeyEvent) -> String {
    let ctrl = key.modifiers.contains(KeyModifiers::CONTROL);
    match key.code {
        KeyCode::Char(c) if ctrl => format!("<c-{}>", c.to_ascii_lowercase()),
        KeyCode::Char(c) => c.to_string(),
        KeyCode::Enter => "<ret>".into(),
        KeyCode::Esc => "<esc>".into(),
        KeyCode::Up => "<up>".into(),
        KeyCode::Down => "<down>".into(),
        KeyCode::Left => "<left>".into(),
        KeyCode::Right => "<right>".into(),
        KeyCode::Home => "<home>".into(),
        KeyCode::End => "<end>".into(),
        KeyCode::PageUp => "<pageup>".into(),
        KeyCode::PageDown => "<pagedown>".into(),
        KeyCode::Tab => "<tab>".into(),
        KeyCode::BackTab => "<s-tab>".into(),
        KeyCode::Delete => "<del>".into(),
        KeyCode::Backspace => "<backspace>".into(),
        _ => "?".into(),
    }
}

[36m--- src/main.rs:do_goto_col (lines 784-799) ---[0m
fn do_goto_col(app: &mut AppContext) -> Result<()> {
    if let Some(view) = app.view() {
        let col_names = df_cols(&view.dataframe);
        let result = picker::fzf(col_names.clone(), "Column: ");
        app.needs_redraw = true;
        if let Ok(Some(selected)) = result {
            if let Some(idx) = col_names.iter().position(|c| c == &selected) {
                if let Some(v) = app.view_mut() {
                    v.state.cc = idx;
                    app.msg(format!("Column: {}", selected));
                }
            }
        }
    }
    Ok(())
}

  src/main.rs:125-147 function run_batch <-> src/main.rs:764-781 function do_command_picker
  Similarity: 86.58%

[36m--- src/main.rs:run_batch (lines 125-147) ---[0m
fn run_batch<I: Iterator<Item = String>>(lines: I) -> Result<()> {
    let mut app = AppContext::new();
    app.viewport(50, 120);
    'outer: for line in lines {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') { continue; }
        for cmd_str in line.split('|').map(str::trim) {
            if cmd_str.is_empty() { continue; }
            if cmd_str == "quit" { break 'outer; }
            if let Some(cmd) = parse(cmd_str, &mut app) {
                if let Err(e) = CommandExecutor::exec(&mut app, cmd) {
                    eprintln!("Error executing '{}': {}", cmd_str, e);
                }
            } else {
                eprintln!("Unknown command: {}", cmd_str);
            }
        }
    }
    wait_bg_save(&mut app);
    wait_bg_meta(&mut app);
    print(&mut app);
    Ok(())
}

[36m--- src/main.rs:do_command_picker (lines 764-781) ---[0m
fn do_command_picker(app: &mut AppContext) -> Result<()> {
    let cmd_list: Vec<String> = vec![
        "from <file>", "save <file>", "ls [dir]", "lr [dir]",
        "ps", "mounts", "tcp", "udp", "lsof [pid]", "env",
        "systemctl", "journalctl [n]", "pacman", "cargo",
        "filter <expr>", "freq <col>", "meta", "corr",
        "select <cols>", "delcol <cols>", "sort <col>", "sort -<col>", "take <n>", "rename <old> <new>",
    ].iter().map(|s| s.to_string()).collect();
    let result = picker::fzf(cmd_list, ": ");
    app.needs_redraw = true;
    if let Ok(Some(selected)) = result {
        let cmd_str = selected.split_whitespace().next().unwrap_or(&selected);
        if let Some(cmd) = parse(cmd_str, app).or_else(|| parse(&selected, app)) {
            if let Err(e) = CommandExecutor::exec(app, cmd) { app.err(e); }
        } else { app.msg(format!("Unknown command: {}", selected)); }
    }
    Ok(())
}

  src/main.rs:472-493 function key_str <-> src/main.rs:731-747 function do_filter
  Similarity: 90.77%

[36m--- src/main.rs:key_str (lines 472-493) ---[0m
fn key_str(key: &KeyEvent) -> String {
    let ctrl = key.modifiers.contains(KeyModifiers::CONTROL);
    match key.code {
        KeyCode::Char(c) if ctrl => format!("<c-{}>", c.to_ascii_lowercase()),
        KeyCode::Char(c) => c.to_string(),
        KeyCode::Enter => "<ret>".into(),
        KeyCode::Esc => "<esc>".into(),
        KeyCode::Up => "<up>".into(),
        KeyCode::Down => "<down>".into(),
        KeyCode::Left => "<left>".into(),
        KeyCode::Right => "<right>".into(),
        KeyCode::Home => "<home>".into(),
        KeyCode::End => "<end>".into(),
        KeyCode::PageUp => "<pageup>".into(),
        KeyCode::PageDown => "<pagedown>".into(),
        KeyCode::Tab => "<tab>".into(),
        KeyCode::BackTab => "<s-tab>".into(),
        KeyCode::Delete => "<del>".into(),
        KeyCode::Backspace => "<backspace>".into(),
        _ => "?".into(),
    }
}

[36m--- src/main.rs:do_filter (lines 731-747) ---[0m
fn do_filter(app: &mut AppContext) -> Result<()> {
    let info = app.view().and_then(|v| {
        let col_name = v.col_name(v.state.cc)?;
        let is_str = v.dataframe.column(&col_name).ok()
            .map(|c| matches!(c.dtype(), polars::prelude::DataType::String)).unwrap_or(false);
        let file = v.filename.as_deref();
        Some((hints(&v.dataframe, &col_name, v.state.cr, file), col_name, is_str))
    });
    if let Some((hint_list, col_name, is_str)) = info {
        let expr_opt = picker::fzf_filter(hint_list, "WHERE> ", &col_name, is_str);
        app.needs_redraw = true;
        if let Ok(Some(expr)) = expr_opt {
            run(app, Box::new(Filter { expr }));
        }
    } else { app.no_table(); }
    Ok(())
}

  src/main.rs:156-172 function parse_keys <-> src/main.rs:472-493 function key_str
  Similarity: 86.24%

[36m--- src/main.rs:parse_keys (lines 156-172) ---[0m
fn parse_keys(s: &str) -> Vec<String> {
    let mut keys = Vec::new();
    let mut chars = s.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '<' {
            let mut key = String::from("<");
            while let Some(&ch) = chars.peek() {
                key.push(chars.next().unwrap());
                if ch == '>' { break; }
            }
            keys.push(key);
        } else {
            keys.push(c.to_string());
        }
    }
    keys
}

[36m--- src/main.rs:key_str (lines 472-493) ---[0m
fn key_str(key: &KeyEvent) -> String {
    let ctrl = key.modifiers.contains(KeyModifiers::CONTROL);
    match key.code {
        KeyCode::Char(c) if ctrl => format!("<c-{}>", c.to_ascii_lowercase()),
        KeyCode::Char(c) => c.to_string(),
        KeyCode::Enter => "<ret>".into(),
        KeyCode::Esc => "<esc>".into(),
        KeyCode::Up => "<up>".into(),
        KeyCode::Down => "<down>".into(),
        KeyCode::Left => "<left>".into(),
        KeyCode::Right => "<right>".into(),
        KeyCode::Home => "<home>".into(),
        KeyCode::End => "<end>".into(),
        KeyCode::PageUp => "<pageup>".into(),
        KeyCode::PageDown => "<pagedown>".into(),
        KeyCode::Tab => "<tab>".into(),
        KeyCode::BackTab => "<s-tab>".into(),
        KeyCode::Delete => "<del>".into(),
        KeyCode::Backspace => "<backspace>".into(),
        _ => "?".into(),
    }
}

  src/main.rs:156-172 function parse_keys <-> src/main.rs:764-781 function do_command_picker
  Similarity: 93.95%

[36m--- src/main.rs:parse_keys (lines 156-172) ---[0m
fn parse_keys(s: &str) -> Vec<String> {
    let mut keys = Vec::new();
    let mut chars = s.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '<' {
            let mut key = String::from("<");
            while let Some(&ch) = chars.peek() {
                key.push(chars.next().unwrap());
                if ch == '>' { break; }
            }
            keys.push(key);
        } else {
            keys.push(c.to_string());
        }
    }
    keys
}

[36m--- src/main.rs:do_command_picker (lines 764-781) ---[0m
fn do_command_picker(app: &mut AppContext) -> Result<()> {
    let cmd_list: Vec<String> = vec![
        "from <file>", "save <file>", "ls [dir]", "lr [dir]",
        "ps", "mounts", "tcp", "udp", "lsof [pid]", "env",
        "systemctl", "journalctl [n]", "pacman", "cargo",
        "filter <expr>", "freq <col>", "meta", "corr",
        "select <cols>", "delcol <cols>", "sort <col>", "sort -<col>", "take <n>", "rename <old> <new>",
    ].iter().map(|s| s.to_string()).collect();
    let result = picker::fzf(cmd_list, ": ");
    app.needs_redraw = true;
    if let Ok(Some(selected)) = result {
        let cmd_str = selected.split_whitespace().next().unwrap_or(&selected);
        if let Some(cmd) = parse(cmd_str, app).or_else(|| parse(&selected, app)) {
            if let Err(e) = CommandExecutor::exec(app, cmd) { app.err(e); }
        } else { app.msg(format!("Unknown command: {}", selected)); }
    }
    Ok(())
}

  src/main.rs:731-747 function do_filter <-> src/main.rs:764-781 function do_command_picker
  Similarity: 87.29%

[36m--- src/main.rs:do_filter (lines 731-747) ---[0m
fn do_filter(app: &mut AppContext) -> Result<()> {
    let info = app.view().and_then(|v| {
        let col_name = v.col_name(v.state.cc)?;
        let is_str = v.dataframe.column(&col_name).ok()
            .map(|c| matches!(c.dtype(), polars::prelude::DataType::String)).unwrap_or(false);
        let file = v.filename.as_deref();
        Some((hints(&v.dataframe, &col_name, v.state.cr, file), col_name, is_str))
    });
    if let Some((hint_list, col_name, is_str)) = info {
        let expr_opt = picker::fzf_filter(hint_list, "WHERE> ", &col_name, is_str);
        app.needs_redraw = true;
        if let Ok(Some(expr)) = expr_opt {
            run(app, Box::new(Filter { expr }));
        }
    } else { app.no_table(); }
    Ok(())
}

[36m--- src/main.rs:do_command_picker (lines 764-781) ---[0m
fn do_command_picker(app: &mut AppContext) -> Result<()> {
    let cmd_list: Vec<String> = vec![
        "from <file>", "save <file>", "ls [dir]", "lr [dir]",
        "ps", "mounts", "tcp", "udp", "lsof [pid]", "env",
        "systemctl", "journalctl [n]", "pacman", "cargo",
        "filter <expr>", "freq <col>", "meta", "corr",
        "select <cols>", "delcol <cols>", "sort <col>", "sort -<col>", "take <n>", "rename <old> <new>",
    ].iter().map(|s| s.to_string()).collect();
    let result = picker::fzf(cmd_list, ": ");
    app.needs_redraw = true;
    if let Ok(Some(selected)) = result {
        let cmd_str = selected.split_whitespace().next().unwrap_or(&selected);
        if let Some(cmd) = parse(cmd_str, app).or_else(|| parse(&selected, app)) {
            if let Err(e) = CommandExecutor::exec(app, cmd) { app.err(e); }
        } else { app.msg(format!("Unknown command: {}", selected)); }
    }
    Ok(())
}

  src/main.rs:156-172 function parse_keys <-> src/main.rs:731-747 function do_filter
  Similarity: 89.52%

[36m--- src/main.rs:parse_keys (lines 156-172) ---[0m
fn parse_keys(s: &str) -> Vec<String> {
    let mut keys = Vec::new();
    let mut chars = s.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '<' {
            let mut key = String::from("<");
            while let Some(&ch) = chars.peek() {
                key.push(chars.next().unwrap());
                if ch == '>' { break; }
            }
            keys.push(key);
        } else {
            keys.push(c.to_string());
        }
    }
    keys
}

[36m--- src/main.rs:do_filter (lines 731-747) ---[0m
fn do_filter(app: &mut AppContext) -> Result<()> {
    let info = app.view().and_then(|v| {
        let col_name = v.col_name(v.state.cc)?;
        let is_str = v.dataframe.column(&col_name).ok()
            .map(|c| matches!(c.dtype(), polars::prelude::DataType::String)).unwrap_or(false);
        let file = v.filename.as_deref();
        Some((hints(&v.dataframe, &col_name, v.state.cr, file), col_name, is_str))
    });
    if let Some((hint_list, col_name, is_str)) = info {
        let expr_opt = picker::fzf_filter(hint_list, "WHERE> ", &col_name, is_str);
        app.needs_redraw = true;
        if let Ok(Some(expr)) = expr_opt {
            run(app, Box::new(Filter { expr }));
        }
    } else { app.no_table(); }
    Ok(())
}

  src/main.rs:764-781 function do_command_picker <-> src/main.rs:894-908 function find
  Similarity: 92.05%

[36m--- src/main.rs:do_command_picker (lines 764-781) ---[0m
fn do_command_picker(app: &mut AppContext) -> Result<()> {
    let cmd_list: Vec<String> = vec![
        "from <file>", "save <file>", "ls [dir]", "lr [dir]",
        "ps", "mounts", "tcp", "udp", "lsof [pid]", "env",
        "systemctl", "journalctl [n]", "pacman", "cargo",
        "filter <expr>", "freq <col>", "meta", "corr",
        "select <cols>", "delcol <cols>", "sort <col>", "sort -<col>", "take <n>", "rename <old> <new>",
    ].iter().map(|s| s.to_string()).collect();
    let result = picker::fzf(cmd_list, ": ");
    app.needs_redraw = true;
    if let Ok(Some(selected)) = result {
        let cmd_str = selected.split_whitespace().next().unwrap_or(&selected);
        if let Some(cmd) = parse(cmd_str, app).or_else(|| parse(&selected, app)) {
            if let Err(e) = CommandExecutor::exec(app, cmd) { app.err(e); }
        } else { app.msg(format!("Unknown command: {}", selected)); }
    }
    Ok(())
}

[36m--- src/main.rs:find (lines 894-908) ---[0m
fn find(df: &polars::prelude::DataFrame, expr: &str) -> Vec<usize> {
    use polars::prelude::*;
    let mut ctx = polars::sql::SQLContext::new();
    let with_idx = df.clone().lazy().with_row_index("__idx__", None);
    ctx.register("df", with_idx);
    ctx.execute(&format!("SELECT __idx__ FROM df WHERE {}", expr))
        .and_then(|lf| lf.collect())
        .map(|result| {
            result.column("__idx__").ok()
                .and_then(|c| c.idx().ok())
                .map(|idx| idx.into_iter().filter_map(|v| v.map(|i| i as usize)).collect())
                .unwrap_or_default()
        })
        .unwrap_or_default()
}

  src/main.rs:156-172 function parse_keys <-> src/main.rs:894-908 function find
  Similarity: 89.57%

[36m--- src/main.rs:parse_keys (lines 156-172) ---[0m
fn parse_keys(s: &str) -> Vec<String> {
    let mut keys = Vec::new();
    let mut chars = s.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '<' {
            let mut key = String::from("<");
            while let Some(&ch) = chars.peek() {
                key.push(chars.next().unwrap());
                if ch == '>' { break; }
            }
            keys.push(key);
        } else {
            keys.push(c.to_string());
        }
    }
    keys
}

[36m--- src/main.rs:find (lines 894-908) ---[0m
fn find(df: &polars::prelude::DataFrame, expr: &str) -> Vec<usize> {
    use polars::prelude::*;
    let mut ctx = polars::sql::SQLContext::new();
    let with_idx = df.clone().lazy().with_row_index("__idx__", None);
    ctx.register("df", with_idx);
    ctx.execute(&format!("SELECT __idx__ FROM df WHERE {}", expr))
        .and_then(|lf| lf.collect())
        .map(|result| {
            result.column("__idx__").ok()
                .and_then(|c| c.idx().ok())
                .map(|idx| idx.into_iter().filter_map(|v| v.map(|i| i as usize)).collect())
                .unwrap_or_default()
        })
        .unwrap_or_default()
}

  src/main.rs:459-469 function find_match <-> src/main.rs:472-493 function key_str
  Similarity: 86.21%

[36m--- src/main.rs:find_match (lines 459-469) ---[0m
fn find_match(app: &mut AppContext, forward: bool) {
    if let Some(expr) = app.search.value.clone() {
        if let Some(view) = app.view_mut() {
            let m = find(&view.dataframe, &expr);
            let cur = view.state.cr;
            let pos = if forward { m.iter().find(|&&i| i > cur) } else { m.iter().rev().find(|&&i| i < cur) };
            if let Some(&p) = pos { view.state.cr = p; view.state.visible(); }
            else { app.msg("No more matches".into()); }
        }
    } else { app.msg("No search active".into()); }
}

[36m--- src/main.rs:key_str (lines 472-493) ---[0m
fn key_str(key: &KeyEvent) -> String {
    let ctrl = key.modifiers.contains(KeyModifiers::CONTROL);
    match key.code {
        KeyCode::Char(c) if ctrl => format!("<c-{}>", c.to_ascii_lowercase()),
        KeyCode::Char(c) => c.to_string(),
        KeyCode::Enter => "<ret>".into(),
        KeyCode::Esc => "<esc>".into(),
        KeyCode::Up => "<up>".into(),
        KeyCode::Down => "<down>".into(),
        KeyCode::Left => "<left>".into(),
        KeyCode::Right => "<right>".into(),
        KeyCode::Home => "<home>".into(),
        KeyCode::End => "<end>".into(),
        KeyCode::PageUp => "<pageup>".into(),
        KeyCode::PageDown => "<pagedown>".into(),
        KeyCode::Tab => "<tab>".into(),
        KeyCode::BackTab => "<s-tab>".into(),
        KeyCode::Delete => "<del>".into(),
        KeyCode::Backspace => "<backspace>".into(),
        _ => "?".into(),
    }
}

  src/main.rs:125-147 function run_batch <-> src/main.rs:883-891 function is_plain_value
  Similarity: 87.81%

[36m--- src/main.rs:run_batch (lines 125-147) ---[0m
fn run_batch<I: Iterator<Item = String>>(lines: I) -> Result<()> {
    let mut app = AppContext::new();
    app.viewport(50, 120);
    'outer: for line in lines {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') { continue; }
        for cmd_str in line.split('|').map(str::trim) {
            if cmd_str.is_empty() { continue; }
            if cmd_str == "quit" { break 'outer; }
            if let Some(cmd) = parse(cmd_str, &mut app) {
                if let Err(e) = CommandExecutor::exec(&mut app, cmd) {
                    eprintln!("Error executing '{}': {}", cmd_str, e);
                }
            } else {
                eprintln!("Unknown command: {}", cmd_str);
            }
        }
    }
    wait_bg_save(&mut app);
    wait_bg_meta(&mut app);
    print(&mut app);
    Ok(())
}

[36m--- src/main.rs:is_plain_value (lines 883-891) ---[0m
fn is_plain_value(expr: &str) -> bool {
    let e = expr.trim();
    // Empty or has spaces (likely SQL) → not plain
    if e.is_empty() || e.contains(' ') { return false; }
    // Quoted string literal → plain
    if (e.starts_with('\'') && e.ends_with('\'')) || (e.starts_with('"') && e.ends_with('"')) { return true; }
    // Alphanumeric/underscore (identifier or number) → plain
    e.chars().all(|c| c.is_alphanumeric() || c == '_' || c == '.' || c == '-')
}

  src/main.rs:351-362 function print_status <-> src/main.rs:731-747 function do_filter
  Similarity: 94.93%

[36m--- src/main.rs:print_status (lines 351-362) ---[0m
fn print_status(app: &mut AppContext) {
    // Wait for background tasks to complete
    wait_bg(app);
    if let Some(view) = app.view_mut() {
        fetch_lazy(view);  // simulate render fetch
        let col_name = view.col_name(view.state.cc).unwrap_or_default();
        let disk = view.disk_rows.map(|n| n.to_string()).unwrap_or("-".into());
        let df = view.dataframe.height();
        println!("STATUS: view={} rows={} disk={} df={} col={} col_name={} mem={}MB",
            view.name, view.rows(), disk, df, view.state.cc, col_name, mem_mb());
    }
}

[36m--- src/main.rs:do_filter (lines 731-747) ---[0m
fn do_filter(app: &mut AppContext) -> Result<()> {
    let info = app.view().and_then(|v| {
        let col_name = v.col_name(v.state.cc)?;
        let is_str = v.dataframe.column(&col_name).ok()
            .map(|c| matches!(c.dtype(), polars::prelude::DataType::String)).unwrap_or(false);
        let file = v.filename.as_deref();
        Some((hints(&v.dataframe, &col_name, v.state.cr, file), col_name, is_str))
    });
    if let Some((hint_list, col_name, is_str)) = info {
        let expr_opt = picker::fzf_filter(hint_list, "WHERE> ", &col_name, is_str);
        app.needs_redraw = true;
        if let Ok(Some(expr)) = expr_opt {
            run(app, Box::new(Filter { expr }));
        }
    } else { app.no_table(); }
    Ok(())
}

  src/main.rs:750-761 function do_convert <-> src/main.rs:764-781 function do_command_picker
  Similarity: 88.08%

[36m--- src/main.rs:do_convert (lines 750-761) ---[0m
fn do_convert(app: &mut AppContext) -> Result<()> {
    let col_name = app.view().and_then(|v| v.col_name(v.state.cc));
    if let Some(col_name) = col_name {
        let types = vec!["String".into(), "Int64".into(), "Float64".into(), "Boolean".into()];
        let result = picker::fzf(types, "Convert to: ");
        app.needs_redraw = true;
        if let Ok(Some(dtype)) = result {
            run(app, Box::new(Cast { col_name, dtype }));
        }
    }
    Ok(())
}

[36m--- src/main.rs:do_command_picker (lines 764-781) ---[0m
fn do_command_picker(app: &mut AppContext) -> Result<()> {
    let cmd_list: Vec<String> = vec![
        "from <file>", "save <file>", "ls [dir]", "lr [dir]",
        "ps", "mounts", "tcp", "udp", "lsof [pid]", "env",
        "systemctl", "journalctl [n]", "pacman", "cargo",
        "filter <expr>", "freq <col>", "meta", "corr",
        "select <cols>", "delcol <cols>", "sort <col>", "sort -<col>", "take <n>", "rename <old> <new>",
    ].iter().map(|s| s.to_string()).collect();
    let result = picker::fzf(cmd_list, ": ");
    app.needs_redraw = true;
    if let Ok(Some(selected)) = result {
        let cmd_str = selected.split_whitespace().next().unwrap_or(&selected);
        if let Some(cmd) = parse(cmd_str, app).or_else(|| parse(&selected, app)) {
            if let Err(e) = CommandExecutor::exec(app, cmd) { app.err(e); }
        } else { app.msg(format!("Unknown command: {}", selected)); }
    }
    Ok(())
}

  src/main.rs:731-747 function do_filter <-> src/main.rs:750-761 function do_convert
  Similarity: 90.44%

[36m--- src/main.rs:do_filter (lines 731-747) ---[0m
fn do_filter(app: &mut AppContext) -> Result<()> {
    let info = app.view().and_then(|v| {
        let col_name = v.col_name(v.state.cc)?;
        let is_str = v.dataframe.column(&col_name).ok()
            .map(|c| matches!(c.dtype(), polars::prelude::DataType::String)).unwrap_or(false);
        let file = v.filename.as_deref();
        Some((hints(&v.dataframe, &col_name, v.state.cr, file), col_name, is_str))
    });
    if let Some((hint_list, col_name, is_str)) = info {
        let expr_opt = picker::fzf_filter(hint_list, "WHERE> ", &col_name, is_str);
        app.needs_redraw = true;
        if let Ok(Some(expr)) = expr_opt {
            run(app, Box::new(Filter { expr }));
        }
    } else { app.no_table(); }
    Ok(())
}

[36m--- src/main.rs:do_convert (lines 750-761) ---[0m
fn do_convert(app: &mut AppContext) -> Result<()> {
    let col_name = app.view().and_then(|v| v.col_name(v.state.cc));
    if let Some(col_name) = col_name {
        let types = vec!["String".into(), "Int64".into(), "Float64".into(), "Boolean".into()];
        let result = picker::fzf(types, "Convert to: ");
        app.needs_redraw = true;
        if let Ok(Some(dtype)) = result {
            run(app, Box::new(Cast { col_name, dtype }));
        }
    }
    Ok(())
}

  src/main.rs:351-362 function print_status <-> src/main.rs:784-799 function do_goto_col
  Similarity: 92.47%

[36m--- src/main.rs:print_status (lines 351-362) ---[0m
fn print_status(app: &mut AppContext) {
    // Wait for background tasks to complete
    wait_bg(app);
    if let Some(view) = app.view_mut() {
        fetch_lazy(view);  // simulate render fetch
        let col_name = view.col_name(view.state.cc).unwrap_or_default();
        let disk = view.disk_rows.map(|n| n.to_string()).unwrap_or("-".into());
        let df = view.dataframe.height();
        println!("STATUS: view={} rows={} disk={} df={} col={} col_name={} mem={}MB",
            view.name, view.rows(), disk, df, view.state.cc, col_name, mem_mb());
    }
}

[36m--- src/main.rs:do_goto_col (lines 784-799) ---[0m
fn do_goto_col(app: &mut AppContext) -> Result<()> {
    if let Some(view) = app.view() {
        let col_names = df_cols(&view.dataframe);
        let result = picker::fzf(col_names.clone(), "Column: ");
        app.needs_redraw = true;
        if let Ok(Some(selected)) = result {
            if let Some(idx) = col_names.iter().position(|c| c == &selected) {
                if let Some(v) = app.view_mut() {
                    v.state.cc = idx;
                    app.msg(format!("Column: {}", selected));
                }
            }
        }
    }
    Ok(())
}

  src/main.rs:322-340 function print <-> src/main.rs:496-504 function cur_tab
  Similarity: 92.36%

[36m--- src/main.rs:print (lines 322-340) ---[0m
fn print(app: &mut AppContext) {
    if let Some(view) = app.view_mut() {
        println!("=== {} ({} rows) ===", view.name, view.rows());
        fetch_lazy(view);
        // Print columns
        let cols = df_cols(&view.dataframe);
        println!("{}", cols.join(","));
        // Print first few rows
        let n = view.dataframe.height().min(10);
        for r in 0..n {
            let row: Vec<String> = (0..cols.len()).map(|c| {
                view.dataframe.get_columns()[c].get(r).map(|v| v.to_string()).unwrap_or_default()
            }).collect();
            println!("{}", row.join(","));
        }
    } else {
        println!("No table loaded");
    }
}

[36m--- src/main.rs:cur_tab (lines 496-504) ---[0m
fn cur_tab(app: &AppContext) -> &'static str {
    app.view().map(|v| {
        if v.name.starts_with("ls") { "folder" }
        else if v.name.starts_with("Freq:") { "freq" }
        else if v.name == "metadata" { "meta" }
        else if v.name == "correlation" { "corr" }
        else { "table" }
    }).unwrap_or("table")
}

  src/main.rs:156-172 function parse_keys <-> src/main.rs:351-362 function print_status
  Similarity: 87.93%

[36m--- src/main.rs:parse_keys (lines 156-172) ---[0m
fn parse_keys(s: &str) -> Vec<String> {
    let mut keys = Vec::new();
    let mut chars = s.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '<' {
            let mut key = String::from("<");
            while let Some(&ch) = chars.peek() {
                key.push(chars.next().unwrap());
                if ch == '>' { break; }
            }
            keys.push(key);
        } else {
            keys.push(c.to_string());
        }
    }
    keys
}

[36m--- src/main.rs:print_status (lines 351-362) ---[0m
fn print_status(app: &mut AppContext) {
    // Wait for background tasks to complete
    wait_bg(app);
    if let Some(view) = app.view_mut() {
        fetch_lazy(view);  // simulate render fetch
        let col_name = view.col_name(view.state.cc).unwrap_or_default();
        let disk = view.disk_rows.map(|n| n.to_string()).unwrap_or("-".into());
        let df = view.dataframe.height();
        println!("STATUS: view={} rows={} disk={} df={} col={} col_name={} mem={}MB",
            view.name, view.rows(), disk, df, view.state.cc, col_name, mem_mb());
    }
}

  src/main.rs:156-172 function parse_keys <-> src/main.rs:750-761 function do_convert
  Similarity: 87.46%

[36m--- src/main.rs:parse_keys (lines 156-172) ---[0m
fn parse_keys(s: &str) -> Vec<String> {
    let mut keys = Vec::new();
    let mut chars = s.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '<' {
            let mut key = String::from("<");
            while let Some(&ch) = chars.peek() {
                key.push(chars.next().unwrap());
                if ch == '>' { break; }
            }
            keys.push(key);
        } else {
            keys.push(c.to_string());
        }
    }
    keys
}

[36m--- src/main.rs:do_convert (lines 750-761) ---[0m
fn do_convert(app: &mut AppContext) -> Result<()> {
    let col_name = app.view().and_then(|v| v.col_name(v.state.cc));
    if let Some(col_name) = col_name {
        let types = vec!["String".into(), "Int64".into(), "Float64".into(), "Boolean".into()];
        let result = picker::fzf(types, "Convert to: ");
        app.needs_redraw = true;
        if let Ok(Some(dtype)) = result {
            run(app, Box::new(Cast { col_name, dtype }));
        }
    }
    Ok(())
}

  src/main.rs:365-375 function wait_bg <-> src/main.rs:784-799 function do_goto_col
  Similarity: 92.73%

[36m--- src/main.rs:wait_bg (lines 365-375) ---[0m
fn wait_bg(app: &mut AppContext) {
    use std::time::Duration;
    // Poll until all bg tasks done (max 5s)
    for _ in 0..50 {
        app.check_bg_freq();
        app.check_bg_meta();
        app.merge_bg_data();
        if app.bg_freq.is_none() && app.bg_meta.is_none() && app.bg_loader.is_none() { break; }
        std::thread::sleep(Duration::from_millis(100));
    }
}

[36m--- src/main.rs:do_goto_col (lines 784-799) ---[0m
fn do_goto_col(app: &mut AppContext) -> Result<()> {
    if let Some(view) = app.view() {
        let col_names = df_cols(&view.dataframe);
        let result = picker::fzf(col_names.clone(), "Column: ");
        app.needs_redraw = true;
        if let Ok(Some(selected)) = result {
            if let Some(idx) = col_names.iter().position(|c| c == &selected) {
                if let Some(v) = app.view_mut() {
                    v.state.cc = idx;
                    app.msg(format!("Column: {}", selected));
                }
            }
        }
    }
    Ok(())
}

  src/main.rs:156-172 function parse_keys <-> src/main.rs:365-375 function wait_bg
  Similarity: 88.10%

[36m--- src/main.rs:parse_keys (lines 156-172) ---[0m
fn parse_keys(s: &str) -> Vec<String> {
    let mut keys = Vec::new();
    let mut chars = s.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '<' {
            let mut key = String::from("<");
            while let Some(&ch) = chars.peek() {
                key.push(chars.next().unwrap());
                if ch == '>' { break; }
            }
            keys.push(key);
        } else {
            keys.push(c.to_string());
        }
    }
    keys
}

[36m--- src/main.rs:wait_bg (lines 365-375) ---[0m
fn wait_bg(app: &mut AppContext) {
    use std::time::Duration;
    // Poll until all bg tasks done (max 5s)
    for _ in 0..50 {
        app.check_bg_freq();
        app.check_bg_meta();
        app.merge_bg_data();
        if app.bg_freq.is_none() && app.bg_meta.is_none() && app.bg_loader.is_none() { break; }
        std::thread::sleep(Duration::from_millis(100));
    }
}

  src/main.rs:156-172 function parse_keys <-> src/main.rs:507-516 function on_key
  Similarity: 89.08%

[36m--- src/main.rs:parse_keys (lines 156-172) ---[0m
fn parse_keys(s: &str) -> Vec<String> {
    let mut keys = Vec::new();
    let mut chars = s.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '<' {
            let mut key = String::from("<");
            while let Some(&ch) = chars.peek() {
                key.push(chars.next().unwrap());
                if ch == '>' { break; }
            }
            keys.push(key);
        } else {
            keys.push(c.to_string());
        }
    }
    keys
}

[36m--- src/main.rs:on_key (lines 507-516) ---[0m
fn on_key(app: &mut AppContext, key: KeyEvent) -> Result<bool> {
    // Look up command from keymap
    let ks = key_str(&key);
    let tab = cur_tab(app);
    if let Some(cmd) = app.keymap.get_command(tab, &ks).map(|s| s.to_string()) {
        return handle_cmd(app, &cmd);
    }
    // Fallback for unmapped keys
    Ok(true)
}

  src/main.rs:322-340 function print <-> src/main.rs:343-348 function mem_mb
  Similarity: 93.48%

[36m--- src/main.rs:print (lines 322-340) ---[0m
fn print(app: &mut AppContext) {
    if let Some(view) = app.view_mut() {
        println!("=== {} ({} rows) ===", view.name, view.rows());
        fetch_lazy(view);
        // Print columns
        let cols = df_cols(&view.dataframe);
        println!("{}", cols.join(","));
        // Print first few rows
        let n = view.dataframe.height().min(10);
        for r in 0..n {
            let row: Vec<String> = (0..cols.len()).map(|c| {
                view.dataframe.get_columns()[c].get(r).map(|v| v.to_string()).unwrap_or_default()
            }).collect();
            println!("{}", row.join(","));
        }
    } else {
        println!("No table loaded");
    }
}

[36m--- src/main.rs:mem_mb (lines 343-348) ---[0m
fn mem_mb() -> usize {
    fs::read_to_string("/proc/self/status").ok()
        .and_then(|s| s.lines().find(|l| l.starts_with("VmRSS:"))
            .and_then(|l| l.split_whitespace().nth(1)?.parse::<usize>().ok()))
        .map(|kb| kb / 1024).unwrap_or(0)
}

  src/main.rs:309-319 function fetch_lazy <-> src/main.rs:784-799 function do_goto_col
  Similarity: 86.11%

[36m--- src/main.rs:fetch_lazy (lines 309-319) ---[0m
fn fetch_lazy(view: &mut state::ViewState) {
    if let Some(ref path) = view.parquet_path {
        let offset = view.state.r0;
        let df = if let Some(ref w) = view.filter_clause {
            backend::Polars.fetch_where(path, w, offset, 50)
        } else {
            backend::Polars.fetch_rows(path, offset, 50)
        };
        if let Ok(df) = df { view.dataframe = df; }
    }
}

[36m--- src/main.rs:do_goto_col (lines 784-799) ---[0m
fn do_goto_col(app: &mut AppContext) -> Result<()> {
    if let Some(view) = app.view() {
        let col_names = df_cols(&view.dataframe);
        let result = picker::fzf(col_names.clone(), "Column: ");
        app.needs_redraw = true;
        if let Ok(Some(selected)) = result {
            if let Some(idx) = col_names.iter().position(|c| c == &selected) {
                if let Some(v) = app.view_mut() {
                    v.state.cc = idx;
                    app.msg(format!("Column: {}", selected));
                }
            }
        }
    }
    Ok(())
}

  src/main.rs:496-504 function cur_tab <-> src/main.rs:784-799 function do_goto_col
  Similarity: 90.61%

[36m--- src/main.rs:cur_tab (lines 496-504) ---[0m
fn cur_tab(app: &AppContext) -> &'static str {
    app.view().map(|v| {
        if v.name.starts_with("ls") { "folder" }
        else if v.name.starts_with("Freq:") { "freq" }
        else if v.name == "metadata" { "meta" }
        else if v.name == "correlation" { "corr" }
        else { "table" }
    }).unwrap_or("table")
}

[36m--- src/main.rs:do_goto_col (lines 784-799) ---[0m
fn do_goto_col(app: &mut AppContext) -> Result<()> {
    if let Some(view) = app.view() {
        let col_names = df_cols(&view.dataframe);
        let result = picker::fzf(col_names.clone(), "Column: ");
        app.needs_redraw = true;
        if let Ok(Some(selected)) = result {
            if let Some(idx) = col_names.iter().position(|c| c == &selected) {
                if let Some(v) = app.view_mut() {
                    v.state.cc = idx;
                    app.msg(format!("Column: {}", selected));
                }
            }
        }
    }
    Ok(())
}

  src/main.rs:351-362 function print_status <-> src/main.rs:750-761 function do_convert
  Similarity: 92.12%

[36m--- src/main.rs:print_status (lines 351-362) ---[0m
fn print_status(app: &mut AppContext) {
    // Wait for background tasks to complete
    wait_bg(app);
    if let Some(view) = app.view_mut() {
        fetch_lazy(view);  // simulate render fetch
        let col_name = view.col_name(view.state.cc).unwrap_or_default();
        let disk = view.disk_rows.map(|n| n.to_string()).unwrap_or("-".into());
        let df = view.dataframe.height();
        println!("STATUS: view={} rows={} disk={} df={} col={} col_name={} mem={}MB",
            view.name, view.rows(), disk, df, view.state.cc, col_name, mem_mb());
    }
}

[36m--- src/main.rs:do_convert (lines 750-761) ---[0m
fn do_convert(app: &mut AppContext) -> Result<()> {
    let col_name = app.view().and_then(|v| v.col_name(v.state.cc));
    if let Some(col_name) = col_name {
        let types = vec!["String".into(), "Int64".into(), "Float64".into(), "Boolean".into()];
        let result = picker::fzf(types, "Convert to: ");
        app.needs_redraw = true;
        if let Ok(Some(dtype)) = result {
            run(app, Box::new(Cast { col_name, dtype }));
        }
    }
    Ok(())
}

  src/main.rs:365-375 function wait_bg <-> src/main.rs:750-761 function do_convert
  Similarity: 93.28%

[36m--- src/main.rs:wait_bg (lines 365-375) ---[0m
fn wait_bg(app: &mut AppContext) {
    use std::time::Duration;
    // Poll until all bg tasks done (max 5s)
    for _ in 0..50 {
        app.check_bg_freq();
        app.check_bg_meta();
        app.merge_bg_data();
        if app.bg_freq.is_none() && app.bg_meta.is_none() && app.bg_loader.is_none() { break; }
        std::thread::sleep(Duration::from_millis(100));
    }
}

[36m--- src/main.rs:do_convert (lines 750-761) ---[0m
fn do_convert(app: &mut AppContext) -> Result<()> {
    let col_name = app.view().and_then(|v| v.col_name(v.state.cc));
    if let Some(col_name) = col_name {
        let types = vec!["String".into(), "Int64".into(), "Float64".into(), "Boolean".into()];
        let result = picker::fzf(types, "Convert to: ");
        app.needs_redraw = true;
        if let Ok(Some(dtype)) = result {
            run(app, Box::new(Cast { col_name, dtype }));
        }
    }
    Ok(())
}

  src/main.rs:444-451 function dispatch <-> src/main.rs:894-908 function find
  Similarity: 88.30%

[36m--- src/main.rs:dispatch (lines 444-451) ---[0m
fn dispatch(app: &mut AppContext, action: &str) -> bool {
    let name = match app.view() { Some(v) => v.name.clone(), None => return false };
    // mem::take to avoid borrow conflict: plugins.handle needs &mut app
    let plugins = std::mem::take(&mut app.plugins);
    let cmd = plugins.handle(&name, action, app);
    app.plugins = plugins;
    if let Some(cmd) = cmd { run(app, cmd); true } else { false }
}

[36m--- src/main.rs:find (lines 894-908) ---[0m
fn find(df: &polars::prelude::DataFrame, expr: &str) -> Vec<usize> {
    use polars::prelude::*;
    let mut ctx = polars::sql::SQLContext::new();
    let with_idx = df.clone().lazy().with_row_index("__idx__", None);
    ctx.register("df", with_idx);
    ctx.execute(&format!("SELECT __idx__ FROM df WHERE {}", expr))
        .and_then(|lf| lf.collect())
        .map(|result| {
            result.column("__idx__").ok()
                .and_then(|c| c.idx().ok())
                .map(|idx| idx.into_iter().filter_map(|v| v.map(|i| i as usize)).collect())
                .unwrap_or_default()
        })
        .unwrap_or_default()
}

  src/main.rs:343-348 function mem_mb <-> src/main.rs:784-799 function do_goto_col
  Similarity: 92.12%

[36m--- src/main.rs:mem_mb (lines 343-348) ---[0m
fn mem_mb() -> usize {
    fs::read_to_string("/proc/self/status").ok()
        .and_then(|s| s.lines().find(|l| l.starts_with("VmRSS:"))
            .and_then(|l| l.split_whitespace().nth(1)?.parse::<usize>().ok()))
        .map(|kb| kb / 1024).unwrap_or(0)
}

[36m--- src/main.rs:do_goto_col (lines 784-799) ---[0m
fn do_goto_col(app: &mut AppContext) -> Result<()> {
    if let Some(view) = app.view() {
        let col_names = df_cols(&view.dataframe);
        let result = picker::fzf(col_names.clone(), "Column: ");
        app.needs_redraw = true;
        if let Ok(Some(selected)) = result {
            if let Some(idx) = col_names.iter().position(|c| c == &selected) {
                if let Some(v) = app.view_mut() {
                    v.state.cc = idx;
                    app.msg(format!("Column: {}", selected));
                }
            }
        }
    }
    Ok(())
}

  src/main.rs:351-362 function print_status <-> src/main.rs:496-504 function cur_tab
  Similarity: 87.95%

[36m--- src/main.rs:print_status (lines 351-362) ---[0m
fn print_status(app: &mut AppContext) {
    // Wait for background tasks to complete
    wait_bg(app);
    if let Some(view) = app.view_mut() {
        fetch_lazy(view);  // simulate render fetch
        let col_name = view.col_name(view.state.cc).unwrap_or_default();
        let disk = view.disk_rows.map(|n| n.to_string()).unwrap_or("-".into());
        let df = view.dataframe.height();
        println!("STATUS: view={} rows={} disk={} df={} col={} col_name={} mem={}MB",
            view.name, view.rows(), disk, df, view.state.cc, col_name, mem_mb());
    }
}

[36m--- src/main.rs:cur_tab (lines 496-504) ---[0m
fn cur_tab(app: &AppContext) -> &'static str {
    app.view().map(|v| {
        if v.name.starts_with("ls") { "folder" }
        else if v.name.starts_with("Freq:") { "freq" }
        else if v.name == "metadata" { "meta" }
        else if v.name == "correlation" { "corr" }
        else { "table" }
    }).unwrap_or("table")
}

  src/main.rs:309-319 function fetch_lazy <-> src/main.rs:496-504 function cur_tab
  Similarity: 92.26%

[36m--- src/main.rs:fetch_lazy (lines 309-319) ---[0m
fn fetch_lazy(view: &mut state::ViewState) {
    if let Some(ref path) = view.parquet_path {
        let offset = view.state.r0;
        let df = if let Some(ref w) = view.filter_clause {
            backend::Polars.fetch_where(path, w, offset, 50)
        } else {
            backend::Polars.fetch_rows(path, offset, 50)
        };
        if let Ok(df) = df { view.dataframe = df; }
    }
}

[36m--- src/main.rs:cur_tab (lines 496-504) ---[0m
fn cur_tab(app: &AppContext) -> &'static str {
    app.view().map(|v| {
        if v.name.starts_with("ls") { "folder" }
        else if v.name.starts_with("Freq:") { "freq" }
        else if v.name == "metadata" { "meta" }
        else if v.name == "correlation" { "corr" }
        else { "table" }
    }).unwrap_or("table")
}

  src/main.rs:459-469 function find_match <-> src/main.rs:496-504 function cur_tab
  Similarity: 92.03%

[36m--- src/main.rs:find_match (lines 459-469) ---[0m
fn find_match(app: &mut AppContext, forward: bool) {
    if let Some(expr) = app.search.value.clone() {
        if let Some(view) = app.view_mut() {
            let m = find(&view.dataframe, &expr);
            let cur = view.state.cr;
            let pos = if forward { m.iter().find(|&&i| i > cur) } else { m.iter().rev().find(|&&i| i < cur) };
            if let Some(&p) = pos { view.state.cr = p; view.state.visible(); }
            else { app.msg("No more matches".into()); }
        }
    } else { app.msg("No search active".into()); }
}

[36m--- src/main.rs:cur_tab (lines 496-504) ---[0m
fn cur_tab(app: &AppContext) -> &'static str {
    app.view().map(|v| {
        if v.name.starts_with("ls") { "folder" }
        else if v.name.starts_with("Freq:") { "freq" }
        else if v.name == "metadata" { "meta" }
        else if v.name == "correlation" { "corr" }
        else { "table" }
    }).unwrap_or("table")
}

  src/main.rs:298-306 function wait_bg_meta <-> src/main.rs:496-504 function cur_tab
  Similarity: 92.26%

[36m--- src/main.rs:wait_bg_meta (lines 298-306) ---[0m
fn wait_bg_meta(app: &mut AppContext) {
    if let Some((pid, rx)) = app.bg_meta.take() {
        if let Ok(df) = rx.recv() {
            if let Some(v) = app.view_mut() {
                if v.name == "metadata" && v.parent_id == Some(pid) { v.dataframe = df; }
            }
        }
    }
}

[36m--- src/main.rs:cur_tab (lines 496-504) ---[0m
fn cur_tab(app: &AppContext) -> &'static str {
    app.view().map(|v| {
        if v.name.starts_with("ls") { "folder" }
        else if v.name.starts_with("Freq:") { "freq" }
        else if v.name == "metadata" { "meta" }
        else if v.name == "correlation" { "corr" }
        else { "table" }
    }).unwrap_or("table")
}

  src/main.rs:507-516 function on_key <-> src/main.rs:883-891 function is_plain_value
  Similarity: 87.20%

[36m--- src/main.rs:on_key (lines 507-516) ---[0m
fn on_key(app: &mut AppContext, key: KeyEvent) -> Result<bool> {
    // Look up command from keymap
    let ks = key_str(&key);
    let tab = cur_tab(app);
    if let Some(cmd) = app.keymap.get_command(tab, &ks).map(|s| s.to_string()) {
        return handle_cmd(app, &cmd);
    }
    // Fallback for unmapped keys
    Ok(true)
}

[36m--- src/main.rs:is_plain_value (lines 883-891) ---[0m
fn is_plain_value(expr: &str) -> bool {
    let e = expr.trim();
    // Empty or has spaces (likely SQL) → not plain
    if e.is_empty() || e.contains(' ') { return false; }
    // Quoted string literal → plain
    if (e.starts_with('\'') && e.ends_with('\'')) || (e.starts_with('"') && e.ends_with('"')) { return true; }
    // Alphanumeric/underscore (identifier or number) → plain
    e.chars().all(|c| c.is_alphanumeric() || c == '_' || c == '.' || c == '-')
}

  src/main.rs:343-348 function mem_mb <-> src/main.rs:351-362 function print_status
  Similarity: 89.71%

[36m--- src/main.rs:mem_mb (lines 343-348) ---[0m
fn mem_mb() -> usize {
    fs::read_to_string("/proc/self/status").ok()
        .and_then(|s| s.lines().find(|l| l.starts_with("VmRSS:"))
            .and_then(|l| l.split_whitespace().nth(1)?.parse::<usize>().ok()))
        .map(|kb| kb / 1024).unwrap_or(0)
}

[36m--- src/main.rs:print_status (lines 351-362) ---[0m
fn print_status(app: &mut AppContext) {
    // Wait for background tasks to complete
    wait_bg(app);
    if let Some(view) = app.view_mut() {
        fetch_lazy(view);  // simulate render fetch
        let col_name = view.col_name(view.state.cc).unwrap_or_default();
        let disk = view.disk_rows.map(|n| n.to_string()).unwrap_or("-".into());
        let df = view.dataframe.height();
        println!("STATUS: view={} rows={} disk={} df={} col={} col_name={} mem={}MB",
            view.name, view.rows(), disk, df, view.state.cc, col_name, mem_mb());
    }
}

  src/main.rs:309-319 function fetch_lazy <-> src/main.rs:343-348 function mem_mb
  Similarity: 94.49%

[36m--- src/main.rs:fetch_lazy (lines 309-319) ---[0m
fn fetch_lazy(view: &mut state::ViewState) {
    if let Some(ref path) = view.parquet_path {
        let offset = view.state.r0;
        let df = if let Some(ref w) = view.filter_clause {
            backend::Polars.fetch_where(path, w, offset, 50)
        } else {
            backend::Polars.fetch_rows(path, offset, 50)
        };
        if let Ok(df) = df { view.dataframe = df; }
    }
}

[36m--- src/main.rs:mem_mb (lines 343-348) ---[0m
fn mem_mb() -> usize {
    fs::read_to_string("/proc/self/status").ok()
        .and_then(|s| s.lines().find(|l| l.starts_with("VmRSS:"))
            .and_then(|l| l.split_whitespace().nth(1)?.parse::<usize>().ok()))
        .map(|kb| kb / 1024).unwrap_or(0)
}

  src/main.rs:343-348 function mem_mb <-> src/main.rs:459-469 function find_match
  Similarity: 93.20%

[36m--- src/main.rs:mem_mb (lines 343-348) ---[0m
fn mem_mb() -> usize {
    fs::read_to_string("/proc/self/status").ok()
        .and_then(|s| s.lines().find(|l| l.starts_with("VmRSS:"))
            .and_then(|l| l.split_whitespace().nth(1)?.parse::<usize>().ok()))
        .map(|kb| kb / 1024).unwrap_or(0)
}

[36m--- src/main.rs:find_match (lines 459-469) ---[0m
fn find_match(app: &mut AppContext, forward: bool) {
    if let Some(expr) = app.search.value.clone() {
        if let Some(view) = app.view_mut() {
            let m = find(&view.dataframe, &expr);
            let cur = view.state.cr;
            let pos = if forward { m.iter().find(|&&i| i > cur) } else { m.iter().rev().find(|&&i| i < cur) };
            if let Some(&p) = pos { view.state.cr = p; view.state.visible(); }
            else { app.msg("No more matches".into()); }
        }
    } else { app.msg("No search active".into()); }
}

  src/main.rs:298-306 function wait_bg_meta <-> src/main.rs:802-808 function unquote
  Similarity: 91.58%

[36m--- src/main.rs:wait_bg_meta (lines 298-306) ---[0m
fn wait_bg_meta(app: &mut AppContext) {
    if let Some((pid, rx)) = app.bg_meta.take() {
        if let Ok(df) = rx.recv() {
            if let Some(v) = app.view_mut() {
                if v.name == "metadata" && v.parent_id == Some(pid) { v.dataframe = df; }
            }
        }
    }
}

[36m--- src/main.rs:unquote (lines 802-808) ---[0m
fn unquote(s: &str) -> String {
    if s.starts_with('"') && s.ends_with('"') && s.len() >= 2 {
        s[1..s.len()-1].to_string()
    } else {
        s.to_string()
    }
}

  src/main.rs:343-348 function mem_mb <-> src/main.rs:496-504 function cur_tab
  Similarity: 95.29%

[36m--- src/main.rs:mem_mb (lines 343-348) ---[0m
fn mem_mb() -> usize {
    fs::read_to_string("/proc/self/status").ok()
        .and_then(|s| s.lines().find(|l| l.starts_with("VmRSS:"))
            .and_then(|l| l.split_whitespace().nth(1)?.parse::<usize>().ok()))
        .map(|kb| kb / 1024).unwrap_or(0)
}

[36m--- src/main.rs:cur_tab (lines 496-504) ---[0m
fn cur_tab(app: &AppContext) -> &'static str {
    app.view().map(|v| {
        if v.name.starts_with("ls") { "folder" }
        else if v.name.starts_with("Freq:") { "freq" }
        else if v.name == "metadata" { "meta" }
        else if v.name == "correlation" { "corr" }
        else { "table" }
    }).unwrap_or("table")
}

  src/main.rs:298-306 function wait_bg_meta <-> src/main.rs:343-348 function mem_mb
  Similarity: 94.49%

[36m--- src/main.rs:wait_bg_meta (lines 298-306) ---[0m
fn wait_bg_meta(app: &mut AppContext) {
    if let Some((pid, rx)) = app.bg_meta.take() {
        if let Ok(df) = rx.recv() {
            if let Some(v) = app.view_mut() {
                if v.name == "metadata" && v.parent_id == Some(pid) { v.dataframe = df; }
            }
        }
    }
}

[36m--- src/main.rs:mem_mb (lines 343-348) ---[0m
fn mem_mb() -> usize {
    fs::read_to_string("/proc/self/status").ok()
        .and_then(|s| s.lines().find(|l| l.starts_with("VmRSS:"))
            .and_then(|l| l.split_whitespace().nth(1)?.parse::<usize>().ok()))
        .map(|kb| kb / 1024).unwrap_or(0)
}

  src/main.rs:496-504 function cur_tab <-> src/main.rs:802-808 function unquote
  Similarity: 87.08%

[36m--- src/main.rs:cur_tab (lines 496-504) ---[0m
fn cur_tab(app: &AppContext) -> &'static str {
    app.view().map(|v| {
        if v.name.starts_with("ls") { "folder" }
        else if v.name.starts_with("Freq:") { "freq" }
        else if v.name == "metadata" { "meta" }
        else if v.name == "correlation" { "corr" }
        else { "table" }
    }).unwrap_or("table")
}

[36m--- src/main.rs:unquote (lines 802-808) ---[0m
fn unquote(s: &str) -> String {
    if s.starts_with('"') && s.ends_with('"') && s.len() >= 2 {
        s[1..s.len()-1].to_string()
    } else {
        s.to_string()
    }
}

  src/main.rs:437-441 function on_col <-> src/main.rs:496-504 function cur_tab
  Similarity: 92.26%

[36m--- src/main.rs:on_col (lines 437-441) ---[0m
fn on_col<F>(app: &mut AppContext, f: F) where F: FnOnce(String) -> Box<dyn command::Command> {
    if let Some(col) = app.view().and_then(|v| v.col_name(v.state.cc)) {
        if let Err(e) = CommandExecutor::exec(app, f(col)) { app.err(e); }
    }
}

[36m--- src/main.rs:cur_tab (lines 496-504) ---[0m
fn cur_tab(app: &AppContext) -> &'static str {
    app.view().map(|v| {
        if v.name.starts_with("ls") { "folder" }
        else if v.name.starts_with("Freq:") { "freq" }
        else if v.name == "metadata" { "meta" }
        else if v.name == "correlation" { "corr" }
        else { "table" }
    }).unwrap_or("table")
}

  src/main.rs:298-306 function wait_bg_meta <-> src/main.rs:437-441 function on_col
  Similarity: 90.70%

[36m--- src/main.rs:wait_bg_meta (lines 298-306) ---[0m
fn wait_bg_meta(app: &mut AppContext) {
    if let Some((pid, rx)) = app.bg_meta.take() {
        if let Ok(df) = rx.recv() {
            if let Some(v) = app.view_mut() {
                if v.name == "metadata" && v.parent_id == Some(pid) { v.dataframe = df; }
            }
        }
    }
}

[36m--- src/main.rs:on_col (lines 437-441) ---[0m
fn on_col<F>(app: &mut AppContext, f: F) where F: FnOnce(String) -> Box<dyn command::Command> {
    if let Some(col) = app.view().and_then(|v| v.col_name(v.state.cc)) {
        if let Err(e) = CommandExecutor::exec(app, f(col)) { app.err(e); }
    }
}

  src/main.rs:343-348 function mem_mb <-> src/main.rs:802-808 function unquote
  Similarity: 88.84%

[36m--- src/main.rs:mem_mb (lines 343-348) ---[0m
fn mem_mb() -> usize {
    fs::read_to_string("/proc/self/status").ok()
        .and_then(|s| s.lines().find(|l| l.starts_with("VmRSS:"))
            .and_then(|l| l.split_whitespace().nth(1)?.parse::<usize>().ok()))
        .map(|kb| kb / 1024).unwrap_or(0)
}

[36m--- src/main.rs:unquote (lines 802-808) ---[0m
fn unquote(s: &str) -> String {
    if s.starts_with('"') && s.ends_with('"') && s.len() >= 2 {
        s[1..s.len()-1].to_string()
    } else {
        s.to_string()
    }
}

  src/main.rs:437-441 function on_col <-> src/main.rs:802-808 function unquote
  Similarity: 89.45%

[36m--- src/main.rs:on_col (lines 437-441) ---[0m
fn on_col<F>(app: &mut AppContext, f: F) where F: FnOnce(String) -> Box<dyn command::Command> {
    if let Some(col) = app.view().and_then(|v| v.col_name(v.state.cc)) {
        if let Err(e) = CommandExecutor::exec(app, f(col)) { app.err(e); }
    }
}

[36m--- src/main.rs:unquote (lines 802-808) ---[0m
fn unquote(s: &str) -> String {
    if s.starts_with('"') && s.ends_with('"') && s.len() >= 2 {
        s[1..s.len()-1].to_string()
    } else {
        s.to_string()
    }
}

  src/main.rs:291-295 function wait_bg_save <-> src/main.rs:802-808 function unquote
  Similarity: 87.69%

[36m--- src/main.rs:wait_bg_save (lines 291-295) ---[0m
fn wait_bg_save(app: &mut AppContext) {
    if let Some(rx) = app.bg_saver.take() {
        for msg in rx { eprintln!("{}", msg); }
    }
}

[36m--- src/main.rs:unquote (lines 802-808) ---[0m
fn unquote(s: &str) -> String {
    if s.starts_with('"') && s.ends_with('"') && s.len() >= 2 {
        s[1..s.len()-1].to_string()
    } else {
        s.to_string()
    }
}

  src/main.rs:343-348 function mem_mb <-> src/main.rs:437-441 function on_col
  Similarity: 94.49%

[36m--- src/main.rs:mem_mb (lines 343-348) ---[0m
fn mem_mb() -> usize {
    fs::read_to_string("/proc/self/status").ok()
        .and_then(|s| s.lines().find(|l| l.starts_with("VmRSS:"))
            .and_then(|l| l.split_whitespace().nth(1)?.parse::<usize>().ok()))
        .map(|kb| kb / 1024).unwrap_or(0)
}

[36m--- src/main.rs:on_col (lines 437-441) ---[0m
fn on_col<F>(app: &mut AppContext, f: F) where F: FnOnce(String) -> Box<dyn command::Command> {
    if let Some(col) = app.view().and_then(|v| v.col_name(v.state.cc)) {
        if let Err(e) = CommandExecutor::exec(app, f(col)) { app.err(e); }
    }
}

  src/main.rs:437-441 function on_col <-> src/main.rs:964-968 function make_test_df
  Similarity: 92.75%

[36m--- src/main.rs:on_col (lines 437-441) ---[0m
fn on_col<F>(app: &mut AppContext, f: F) where F: FnOnce(String) -> Box<dyn command::Command> {
    if let Some(col) = app.view().and_then(|v| v.col_name(v.state.cc)) {
        if let Err(e) = CommandExecutor::exec(app, f(col)) { app.err(e); }
    }
}

[36m--- src/main.rs:make_test_df (lines 964-968) ---[0m
    fn make_test_df() -> DataFrame {
        df! {
            "name" => &["apple", "banana", "cherry", "pineapple", "grape", "blueberry"]
        }.unwrap()
    }

  src/main.rs:291-295 function wait_bg_save <-> src/main.rs:437-441 function on_col
  Similarity: 89.12%

[36m--- src/main.rs:wait_bg_save (lines 291-295) ---[0m
fn wait_bg_save(app: &mut AppContext) {
    if let Some(rx) = app.bg_saver.take() {
        for msg in rx { eprintln!("{}", msg); }
    }
}

[36m--- src/main.rs:on_col (lines 437-441) ---[0m
fn on_col<F>(app: &mut AppContext, f: F) where F: FnOnce(String) -> Box<dyn command::Command> {
    if let Some(col) = app.view().and_then(|v| v.col_name(v.state.cc)) {
        if let Err(e) = CommandExecutor::exec(app, f(col)) { app.err(e); }
    }
}

  src/main.rs:291-295 function wait_bg_save <-> src/main.rs:964-968 function make_test_df
  Similarity: 87.31%

[36m--- src/main.rs:wait_bg_save (lines 291-295) ---[0m
fn wait_bg_save(app: &mut AppContext) {
    if let Some(rx) = app.bg_saver.take() {
        for msg in rx { eprintln!("{}", msg); }
    }
}

[36m--- src/main.rs:make_test_df (lines 964-968) ---[0m
    fn make_test_df() -> DataFrame {
        df! {
            "name" => &["apple", "banana", "cherry", "pineapple", "grape", "blueberry"]
        }.unwrap()
    }

  src/main.rs:150-152 function run_script <-> src/main.rs:964-968 function make_test_df
  Similarity: 94.23%

[36m--- src/main.rs:run_script (lines 150-152) ---[0m
fn run_script(script_path: &str) -> Result<()> {
    run_batch(fs::read_to_string(script_path)?.lines().map(String::from))
}

[36m--- src/main.rs:make_test_df (lines 964-968) ---[0m
    fn make_test_df() -> DataFrame {
        df! {
            "name" => &["apple", "banana", "cherry", "pineapple", "grape", "blueberry"]
        }.unwrap()
    }

  src/main.rs:454-456 function run <-> src/main.rs:964-968 function make_test_df
  Similarity: 87.31%

[36m--- src/main.rs:run (lines 454-456) ---[0m
fn run(app: &mut AppContext, cmd: Box<dyn command::Command>) {
    if let Err(e) = CommandExecutor::exec(app, cmd) { app.err(e); }
}

[36m--- src/main.rs:make_test_df (lines 964-968) ---[0m
    fn make_test_df() -> DataFrame {
        df! {
            "name" => &["apple", "banana", "cherry", "pineapple", "grape", "blueberry"]
        }.unwrap()
    }


Duplicates in src/backend/gz.rs:
------------------------------------------------------------
  src/backend/gz.rs:81-122 function load_streaming <-> src/backend/gz.rs:180-244 function stream_save
  Similarity: 87.46%

[36m--- src/backend/gz.rs:load_streaming (lines 81-122) ---[0m
pub fn load_streaming(path: &Path, mem_limit: u64) -> Result<(DataFrame, Option<Receiver<GzChunk>>)> {
    let mut child = Command::new("zcat")
        .arg(path).stdout(Stdio::piped()).stderr(Stdio::null())
        .spawn().map_err(|e| anyhow!("Failed to spawn zcat: {}", e))?;

    let stdout = child.stdout.take().ok_or_else(|| anyhow!("No stdout"))?;
    let mut reader = BufReader::with_capacity(8 * 1024 * 1024, stdout);

    // Read header
    let mut header = String::new();
    reader.read_line(&mut header)?;
    let sep = detect_sep(&header);
    let header_bytes = header.as_bytes().to_vec();

    // Read first chunk
    let mut buf = header.clone().into_bytes();
    let (mut lines, mut total_bytes) = (0usize, 0u64);
    while lines < MIN_ROWS.max(CHUNK_ROWS) {
        let mut line = String::new();
        match reader.read_line(&mut line) {
            Ok(0) => break,
            Ok(n) => { buf.extend_from_slice(line.as_bytes()); total_bytes += n as u64; lines += 1; }
            Err(_) => break,
        }
    }
    if lines == 0 { return Err(anyhow!("Empty file")); }

    let raw_df = parse_csv_buf(buf, sep, 500)?;
    let schema = Arc::clone(raw_df.schema());
    let df = convert_epoch_cols(raw_df);

    // EOF or mem limit reached - no background loading
    if lines < CHUNK_ROWS || total_bytes * 2 > mem_limit {
        let _ = child.wait();
        return Ok((df, None));
    }

    // Background streaming
    let (tx, rx) = mpsc::channel();
    thread::spawn(move || stream_chunks(reader, header_bytes, sep, mem_limit, total_bytes, tx, child, schema));
    Ok((df, Some(rx)))
}

[36m--- src/backend/gz.rs:stream_save (lines 180-244) ---[0m
fn stream_save(gz_path: &str, out_path: &Path, raw: bool, tx: &Sender<String>) -> Result<()> {
    let mut child = Command::new("zcat")
        .arg(gz_path).stdout(Stdio::piped()).stderr(Stdio::null())
        .spawn().map_err(|e| anyhow!("zcat: {}", e))?;

    let stdout = child.stdout.take().ok_or_else(|| anyhow!("No stdout"))?;
    let mut reader = BufReader::with_capacity(64 * 1024 * 1024, stdout);

    let mut header = String::new();
    reader.read_line(&mut header)?;
    let sep = detect_sep(&header);
    let col_count = header.split(sep as char).count();
    let header_bytes = header.as_bytes().to_vec();

    let mut read_chunk = |n: usize| -> Result<(Vec<u8>, usize)> {
        let mut buf = header_bytes.clone();
        let mut lines = 0usize;
        while lines < n {
            let mut line = String::new();
            if reader.read_line(&mut line)? == 0 { break; }
            buf.extend_from_slice(line.as_bytes());
            lines += 1;
        }
        Ok((buf, lines))
    };

    let str_schema = Arc::new(Schema::from_iter((0..col_count).map(|i| {
        Field::new(header.split(sep as char).nth(i).unwrap_or("").trim().into(), DataType::String)
    })));
    let parse = |buf: Vec<u8>| -> Result<DataFrame> {
        CsvReadOptions::default().with_has_header(true).with_schema(Some(str_schema.clone()))
            .map_parse_options(|o| o.with_separator(sep))
            .into_reader_with_file_handle(std::io::Cursor::new(buf)).finish()
            .map_err(|e| anyhow!("Parse: {}", e))
    };

    let (buf, lines) = read_chunk(FIRST_CHUNK_ROWS)?;
    if lines == 0 { return Err(anyhow!("Empty file")); }
    let first_df = if raw { parse(buf)? } else { convert_types(parse(buf)?) };
    let schema = first_df.schema().to_owned();

    let file = std::fs::File::create(out_path)?;
    let mut writer = ParquetWriter::new(file).batched(&schema)?;
    let mut total_rows = first_df.height();
    let mut first_df = first_df;
    first_df.rechunk_mut();
    writer.write_batch(&first_df)?;
    let _ = tx.send(format!("Written {} rows", commify(&total_rows.to_string())));

    loop {
        let (buf, lines) = read_chunk(CHUNK_ROWS)?;
        if lines == 0 { break; }
        let df = parse(buf)?;
        let mut df = if raw { df } else { let (d, e) = apply_schema(df, &schema); if let Some(e) = e { let _ = tx.send(format!("Warning: {}", e)); } d };
        df.rechunk_mut();
        writer.write_batch(&df)?;
        total_rows += df.height();
        let _ = tx.send(format!("Written {} rows", commify(&total_rows.to_string())));
    }

    writer.finish()?;
    let _ = child.wait();
    let _ = tx.send(format!("Done: {} rows", commify(&total_rows.to_string())));
    Ok(())
}

  src/backend/gz.rs:50-54 method distinct <-> src/backend/gz.rs:172-177 function stream_to_parquet
  Similarity: 89.44%

[36m--- src/backend/gz.rs:distinct (lines 50-54) ---[0m
    fn distinct(&self, p: &str, col: &str) -> Result<Vec<String>> {
        self.require_complete()?;
        let df = sql(self.lf(p)?, &format!("SELECT DISTINCT \"{}\" FROM df ORDER BY \"{}\"", col, col))?;
        Ok(df.column(col).map(|c| (0..c.len()).filter_map(|i| c.get(i).ok().map(|v| v.to_string())).collect()).unwrap_or_default())
    }

[36m--- src/backend/gz.rs:stream_to_parquet (lines 172-177) ---[0m
pub fn stream_to_parquet(gz_path: &str, out_path: &Path, raw: bool) -> Receiver<String> {
    let (gz, out) = (gz_path.to_string(), out_path.to_path_buf());
    let (tx, rx) = mpsc::channel();
    thread::spawn(move || { if let Err(e) = stream_save(&gz, &out, raw, &tx) { let _ = tx.send(format!("Error: {}", e)); } });
    rx
}

  src/backend/gz.rs:63-67 method count_where <-> src/backend/gz.rs:172-177 function stream_to_parquet
  Similarity: 86.32%

[36m--- src/backend/gz.rs:count_where (lines 63-67) ---[0m
    fn count_where(&self, _: &str, w: &str) -> Result<usize> {
        self.require_complete()?;
        let r = sql(self.lf("")?, &format!("SELECT COUNT(*) as cnt FROM df WHERE {}", w))?;
        Ok(r.column("cnt")?.get(0)?.try_extract::<u32>().unwrap_or(0) as usize)
    }

[36m--- src/backend/gz.rs:stream_to_parquet (lines 172-177) ---[0m
pub fn stream_to_parquet(gz_path: &str, out_path: &Path, raw: bool) -> Receiver<String> {
    let (gz, out) = (gz_path.to_string(), out_path.to_path_buf());
    let (tx, rx) = mpsc::channel();
    thread::spawn(move || { if let Err(e) = stream_save(&gz, &out, raw, &tx) { let _ = tx.send(format!("Error: {}", e)); } });
    rx
}

  src/backend/gz.rs:50-54 method distinct <-> src/backend/gz.rs:63-67 method count_where
  Similarity: 94.88%
  Classes: Backend <-> Backend

[36m--- src/backend/gz.rs:distinct (lines 50-54) ---[0m
    fn distinct(&self, p: &str, col: &str) -> Result<Vec<String>> {
        self.require_complete()?;
        let df = sql(self.lf(p)?, &format!("SELECT DISTINCT \"{}\" FROM df ORDER BY \"{}\"", col, col))?;
        Ok(df.column(col).map(|c| (0..c.len()).filter_map(|i| c.get(i).ok().map(|v| v.to_string())).collect()).unwrap_or_default())
    }

[36m--- src/backend/gz.rs:count_where (lines 63-67) ---[0m
    fn count_where(&self, _: &str, w: &str) -> Result<usize> {
        self.require_complete()?;
        let r = sql(self.lf("")?, &format!("SELECT COUNT(*) as cnt FROM df WHERE {}", w))?;
        Ok(r.column("cnt")?.get(0)?.try_extract::<u32>().unwrap_or(0) as usize)
    }

  src/backend/gz.rs:58-61 method fetch_where <-> src/backend/gz.rs:69-72 method freq_where
  Similarity: 93.10%
  Classes: Backend <-> Backend

[36m--- src/backend/gz.rs:fetch_where (lines 58-61) ---[0m
    fn fetch_where(&self, _: &str, w: &str, offset: usize, limit: usize) -> Result<DataFrame> {
        self.require_complete()?;
        sql(self.lf("")?, &format!("SELECT * FROM df WHERE {} LIMIT {} OFFSET {}", w, limit, offset))
    }

[36m--- src/backend/gz.rs:freq_where (lines 69-72) ---[0m
    fn freq_where(&self, _: &str, col: &str, w: &str) -> Result<DataFrame> {
        self.require_complete()?;
        sql(self.lf("")?, &format!("SELECT \"{}\", COUNT(*) as Cnt FROM df WHERE {} GROUP BY \"{}\" ORDER BY Cnt DESC", col, w, col))
    }

  src/backend/gz.rs:44-47 method sort_head <-> src/backend/gz.rs:58-61 method fetch_where
  Similarity: 85.91%
  Classes: Backend <-> Backend

[36m--- src/backend/gz.rs:sort_head (lines 44-47) ---[0m
    fn sort_head(&self, _: &str, col: &str, desc: bool, limit: usize) -> Result<DataFrame> {
        let ord = if desc { "DESC" } else { "ASC" };
        sql(self.lf("")?, &format!("SELECT * FROM df ORDER BY \"{}\" {} LIMIT {}", col, ord, limit))
    }

[36m--- src/backend/gz.rs:fetch_where (lines 58-61) ---[0m
    fn fetch_where(&self, _: &str, w: &str, offset: usize, limit: usize) -> Result<DataFrame> {
        self.require_complete()?;
        sql(self.lf("")?, &format!("SELECT * FROM df WHERE {} LIMIT {} OFFSET {}", w, limit, offset))
    }

  src/backend/gz.rs:44-47 method sort_head <-> src/backend/gz.rs:69-72 method freq_where
  Similarity: 85.91%
  Classes: Backend <-> Backend

[36m--- src/backend/gz.rs:sort_head (lines 44-47) ---[0m
    fn sort_head(&self, _: &str, col: &str, desc: bool, limit: usize) -> Result<DataFrame> {
        let ord = if desc { "DESC" } else { "ASC" };
        sql(self.lf("")?, &format!("SELECT * FROM df ORDER BY \"{}\" {} LIMIT {}", col, ord, limit))
    }

[36m--- src/backend/gz.rs:freq_where (lines 69-72) ---[0m
    fn freq_where(&self, _: &str, col: &str, w: &str) -> Result<DataFrame> {
        self.require_complete()?;
        sql(self.lf("")?, &format!("SELECT \"{}\", COUNT(*) as Cnt FROM df WHERE {} GROUP BY \"{}\" ORDER BY Cnt DESC", col, w, col))
    }


Duplicates in src/command/io/mod.rs:
------------------------------------------------------------
  src/command/io/mod.rs:18-29 method exec <-> src/command/io/mod.rs:41-58 method exec
  Similarity: 87.49%
  Classes: Command <-> Command

[36m--- src/command/io/mod.rs:exec (lines 18-29) ---[0m
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        let p = &self.file_path;
        let id = app.next_id();

        // Dispatch: .gz -> gz backend, else -> Polars backend
        let is_gz = Path::new(p).file_name().and_then(|s| s.to_str()).map(|s| s.ends_with(".gz")).unwrap_or(false);
        let result = if is_gz { gz::load(p, id) } else { Polars.load(p, id) }?;

        app.stack.push(result.view);
        app.bg_loader = result.bg_loader;
        Ok(())
    }

[36m--- src/command/io/mod.rs:exec (lines 41-58) ---[0m
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        let view = app.req()?;
        let path = Path::new(&self.file_path);
        let is_parquet = !matches!(path.extension().and_then(|s| s.to_str()), Some("csv"));

        // Streaming save for gz source -> parquet (re-reads from disk)
        if is_parquet && view.gz_source.is_some() {
            let gz = view.gz_source.clone().unwrap();
            let raw = app.raw_save;
            app.msg(format!("Streaming {} to parquet{}...", gz, if raw { " (raw)" } else { "" }));
            app.bg_saver = Some(gz::stream_to_parquet(&gz, path, raw));
            return Ok(());
        }

        // Normal save via backend
        let df = if is_parquet { convert_epoch_cols(view.dataframe.clone()) } else { view.dataframe.clone() };
        view.backend().save(&df, path)
    }


Duplicates in src/render/renderer.rs:
------------------------------------------------------------
  src/render/renderer.rs:173-223 method render_headers_xs <-> src/render/renderer.rs:227-297 method render_row_xs
  Similarity: 91.68%
  Classes: Renderer <-> Renderer

[36m--- src/render/renderer.rs:render_headers_xs (lines 173-223) ---[0m
    fn render_headers_xs(frame: &mut Frame, df: &DataFrame, state: &TableState, xs: &[i32], screen_width: i32, row_num_width: u16, selected_cols: &HashSet<usize>, _col_sep: Option<usize>, theme: &Theme, area: Rect) {
        let buf = frame.buffer_mut();
        let header_style = Style::default().bg(to_rcolor(theme.header_bg)).fg(to_rcolor(theme.header_fg)).add_modifier(Modifier::BOLD);

        // Fill header row with header style first
        for x in 0..area.width { buf[(x, 0)].set_style(header_style); buf[(x, 0)].set_char(' '); }

        let mut x_pos = 0u16;

        // Row number header
        if row_num_width > 0 {
            let s = format!("{:>width$} ", "#", width = row_num_width as usize);
            for (i, ch) in s.chars().enumerate() {
                if x_pos + i as u16 >= area.width { break; }
                buf[(x_pos + i as u16, 0)].set_char(ch);
            }
            x_pos += row_num_width + 1;
        }

        for (col_idx, col_name) in df.get_column_names().iter().enumerate() {
            let x = xs[col_idx];
            let next_x = xs.get(col_idx + 1).copied().unwrap_or(x);
            if next_x <= 0 { continue; }
            if x >= screen_width { break; }

            let is_current = col_idx == state.cc;
            let is_selected = selected_cols.contains(&col_idx);
            let col_width = state.col_widths.get(col_idx).copied().unwrap_or(10) as usize;

            let style = if is_current {
                Style::default().bg(to_rcolor(theme.cursor_bg)).fg(to_rcolor(theme.cursor_fg)).add_modifier(Modifier::BOLD)
            } else if is_selected {
                Style::default().bg(to_rcolor(theme.header_bg)).fg(to_rcolor(theme.select_fg)).add_modifier(Modifier::BOLD)
            } else { header_style };

            let start_x = x.max(0) as u16 + x_pos;
            let display = format!("{:width$}", col_name, width = col_width);

            for (i, ch) in display.chars().take(col_width).enumerate() {
                let px = start_x + i as u16;
                if px >= area.width { break; }
                buf[(px, 0)].set_char(ch).set_style(style);
            }

            // Separator space
            let sep_x = start_x + col_width as u16;
            if sep_x < area.width {
                buf[(sep_x, 0)].set_char(' ').set_style(header_style);
            }
        }
    }

[36m--- src/render/renderer.rs:render_row_xs (lines 227-297) ---[0m
    fn render_row_xs(frame: &mut Frame, df: &DataFrame, df_idx: usize, row_idx: usize, state: &TableState, xs: &[i32], screen_width: i32, row_num_width: u16, is_correlation: bool, selected_cols: &HashSet<usize>, selected_rows: &HashSet<usize>, _col_sep: Option<usize>, decimals: usize, theme: &Theme, area: Rect, screen_row: u16) {
        let buf = frame.buffer_mut();
        let is_cur_row = row_idx == state.cr;
        let is_sel_row = selected_rows.contains(&row_idx);

        // Clear row first
        for x in 0..area.width { buf[(x, screen_row)].reset(); }

        let mut x_pos = 0u16;

        // Row number
        if row_num_width > 0 {
            let style = if is_cur_row { Style::default().fg(to_rcolor(theme.row_cur_fg)) }
                       else if is_sel_row { Style::default().fg(to_rcolor(theme.row_num_fg)) }
                       else { Style::default() };
            let s = format!("{:>width$} ", row_idx, width = row_num_width as usize);
            for (i, ch) in s.chars().enumerate() {
                if x_pos + i as u16 >= area.width { break; }
                buf[(x_pos + i as u16, screen_row)].set_char(ch).set_style(style);
            }
            x_pos += row_num_width + 1;
        }

        for col_idx in 0..df.width() {
            let x = xs[col_idx];
            let next_x = xs.get(col_idx + 1).copied().unwrap_or(x);
            if next_x <= 0 { continue; }
            if x >= screen_width { break; }

            let is_cur_col = col_idx == state.cc;
            let is_cur_cell = is_cur_row && is_cur_col;
            let is_sel = selected_cols.contains(&col_idx);

            let col_width = state.col_widths.get(col_idx).copied().unwrap_or(10) as usize;
            let value = Self::format_value(df, col_idx, df_idx, decimals);

            // Correlation color
            let corr_color = if is_correlation && col_idx > 0 { Self::correlation_color(&value) } else { None };

            let style = if is_cur_cell {
                Style::default().bg(to_rcolor(theme.cursor_bg)).fg(to_rcolor(theme.cursor_fg))
            } else if is_cur_col {
                let fg = corr_color.map(to_rcolor)
                    .or_else(|| if is_sel { Some(to_rcolor(theme.select_fg)) } else { None })
                    .or_else(|| if is_sel_row { Some(to_rcolor(theme.row_num_fg)) } else { None })
                    .unwrap_or(RColor::Reset);
                Style::default().bg(RColor::DarkGray).fg(fg)
            } else if is_sel_row {
                Style::default().fg(to_rcolor(theme.row_num_fg))
            } else if is_sel {
                Style::default().fg(to_rcolor(theme.select_fg))
            } else if let Some(c) = corr_color {
                Style::default().fg(to_rcolor(c))
            } else if is_cur_row {
                Style::default().fg(to_rcolor(theme.row_cur_fg))
            } else { Style::default() };

            let start_x = x.max(0) as u16 + x_pos;
            let display = format!("{:width$}", value, width = col_width);

            for (i, ch) in display.chars().take(col_width).enumerate() {
                let px = start_x + i as u16;
                if px >= area.width { break; }
                buf[(px, screen_row)].set_char(ch).set_style(style);
            }

            // Separator
            let sep_x = start_x + col_width as u16;
            if sep_x < area.width { buf[(sep_x, screen_row)].set_char(' '); }
        }
    }

  src/render/renderer.rs:435-471 method render_info_box <-> src/render/renderer.rs:498-546 method render_status_bar
  Similarity: 85.97%
  Classes: Renderer <-> Renderer

[36m--- src/render/renderer.rs:render_info_box (lines 435-471) ---[0m
    fn render_info_box(frame: &mut Frame, _view_name: &str, stack_len: usize, area: Rect, keys: &[(String, &'static str)], theme: &Theme) {
        use ratatui::widgets::{Block, Borders, Paragraph, Clear};
        use ratatui::text::{Line, Span};

        let max_desc_len = keys.iter().map(|(_, d)| d.len()).max().unwrap_or(10);
        let box_width = (max_desc_len + 11) as u16;
        let box_height = (keys.len() + 2) as u16;

        let box_x = area.width.saturating_sub(box_width + 1);
        let box_y = area.height.saturating_sub(box_height + 1);
        let box_area = Rect::new(box_x, box_y, box_width, box_height);

        // Clear area first
        frame.render_widget(Clear, box_area);

        // Block with border and title
        let title = if stack_len > 1 { format!(" [#{}] ", stack_len) } else { " [tv] ".to_string() };
        let border_style = Style::default().fg(to_rcolor(theme.info_border_fg));
        let block = Block::default()
            .borders(Borders::ALL)
            .border_style(border_style)
            .title(title);

        // Build styled lines for content
        let key_style = Style::default().fg(to_rcolor(theme.info_key_fg));
        let text_style = Style::default().fg(RColor::White);
        let lines: Vec<Line> = keys.iter().map(|(key, desc)| {
            Line::from(vec![
                Span::styled(format!("{:>5}", key), key_style),
                Span::raw(" "),
                Span::styled(*desc, text_style),
            ])
        }).collect();

        let para = Paragraph::new(lines).block(block);
        frame.render_widget(para, box_area);
    }

[36m--- src/render/renderer.rs:render_status_bar (lines 498-546) ---[0m
    fn render_status_bar(frame: &mut Frame, view: &mut ViewState, message: &str, is_loading: bool, area: Rect, theme: &Theme) {
        let row = area.height - 1;
        let buf = frame.buffer_mut();
        let style = Style::default().bg(to_rcolor(theme.status_bg)).fg(to_rcolor(theme.status_fg));

        // Fill status bar
        for x in 0..area.width { buf[(x, row)].set_style(style); buf[(x, row)].set_char(' '); }

        // Show total rows: just disk_rows if set, else dataframe height
        let total_str = commify(&view.rows().to_string());

        let left = if !message.is_empty() { message.to_string() }
        else if view.name.starts_with("Freq:") || view.name == "metadata" {
            // Show parent name and row count for Meta/Freq views
            let pn = view.parent_name.as_deref().unwrap_or("");
            let pr = view.parent_rows.map(|n| format!(" ({})", commify(&n.to_string()))).unwrap_or_default();
            format!("{} <- {}{}", view.name, pn, pr)
        }
        else { view.filename.as_deref().unwrap_or("(no file)").to_string() };

        // Use cached stats if column unchanged
        let col_stats = if view.cols() > 0 {
            let cc = view.state.cc;
            if let Some((cached_cc, ref s)) = view.stats_cache {
                if cached_cc == cc { s.clone() }
                else {
                    let s = Self::column_stats(&view.dataframe, cc);
                    view.stats_cache = Some((cc, s.clone()));
                    s
                }
            } else {
                let s = Self::column_stats(&view.dataframe, cc);
                view.stats_cache = Some((cc, s.clone()));
                s
            }
        } else { String::new() };

        let partial = if is_loading || view.partial { " Partial" } else { "" };
        let right = if col_stats.is_empty() { format!("{}/{}{}", view.state.cr, total_str, partial) }
        else { format!("{} {}/{}{}", col_stats, view.state.cr, total_str, partial) };

        let padding = (area.width as usize).saturating_sub(left.len() + right.len()).max(1);
        let status = format!("{}{:width$}{}", left, "", right, width = padding);

        for (i, ch) in status.chars().enumerate() {
            if i >= area.width as usize { break; }
            buf[(i as u16, row)].set_char(ch);
        }
    }

  src/render/renderer.rs:25-55 method render <-> src/render/renderer.rs:498-546 method render_status_bar
  Similarity: 89.86%
  Classes: Renderer <-> Renderer

[36m--- src/render/renderer.rs:render (lines 25-55) ---[0m
    pub fn render(frame: &mut Frame, app: &mut AppContext) {
        let area = frame.area();
        let message = app.message.clone();
        let stack_len = app.stack.len();
        let stack_names = app.stack.names();
        let show_info = app.show_info;
        let decimals = app.float_decimals;
        let is_loading = app.is_loading();

        // Get view name for keymap lookup
        let tab = app.view().map(|v| app.plugins.tab(&v.name)).unwrap_or("table");
        let hints = app.keymap.get_hints(tab);
        let theme = app.theme.clone();

        if let Some(view) = app.view_mut() {
            let selected_cols = view.selected_cols.clone();
            let selected_rows = view.selected_rows.clone();
            let view_name = view.name.clone();
            let show_tabs = stack_names.len() > 1;
            Self::render_table(frame, view, area, &selected_cols, &selected_rows, decimals, &theme, show_tabs);
            if show_info {
                Self::render_info_box(frame, &view_name, stack_len, area, &hints, &theme);
            }
            if show_tabs {
                Self::render_tabs(frame, &stack_names, area, &theme);
            }
            Self::render_status_bar(frame, view, &message, is_loading, area, &theme);
        } else {
            Self::empty_msg(frame, &message, area);
        }
    }

[36m--- src/render/renderer.rs:render_status_bar (lines 498-546) ---[0m
    fn render_status_bar(frame: &mut Frame, view: &mut ViewState, message: &str, is_loading: bool, area: Rect, theme: &Theme) {
        let row = area.height - 1;
        let buf = frame.buffer_mut();
        let style = Style::default().bg(to_rcolor(theme.status_bg)).fg(to_rcolor(theme.status_fg));

        // Fill status bar
        for x in 0..area.width { buf[(x, row)].set_style(style); buf[(x, row)].set_char(' '); }

        // Show total rows: just disk_rows if set, else dataframe height
        let total_str = commify(&view.rows().to_string());

        let left = if !message.is_empty() { message.to_string() }
        else if view.name.starts_with("Freq:") || view.name == "metadata" {
            // Show parent name and row count for Meta/Freq views
            let pn = view.parent_name.as_deref().unwrap_or("");
            let pr = view.parent_rows.map(|n| format!(" ({})", commify(&n.to_string()))).unwrap_or_default();
            format!("{} <- {}{}", view.name, pn, pr)
        }
        else { view.filename.as_deref().unwrap_or("(no file)").to_string() };

        // Use cached stats if column unchanged
        let col_stats = if view.cols() > 0 {
            let cc = view.state.cc;
            if let Some((cached_cc, ref s)) = view.stats_cache {
                if cached_cc == cc { s.clone() }
                else {
                    let s = Self::column_stats(&view.dataframe, cc);
                    view.stats_cache = Some((cc, s.clone()));
                    s
                }
            } else {
                let s = Self::column_stats(&view.dataframe, cc);
                view.stats_cache = Some((cc, s.clone()));
                s
            }
        } else { String::new() };

        let partial = if is_loading || view.partial { " Partial" } else { "" };
        let right = if col_stats.is_empty() { format!("{}/{}{}", view.state.cr, total_str, partial) }
        else { format!("{} {}/{}{}", col_stats, view.state.cr, total_str, partial) };

        let padding = (area.width as usize).saturating_sub(left.len() + right.len()).max(1);
        let status = format!("{}{:width$}{}", left, "", right, width = padding);

        for (i, ch) in status.chars().enumerate() {
            if i >= area.width as usize { break; }
            buf[(i as u16, row)].set_char(ch);
        }
    }

  src/render/renderer.rs:25-55 method render <-> src/render/renderer.rs:388-432 method column_stats
  Similarity: 86.79%
  Classes: Renderer <-> Renderer

[36m--- src/render/renderer.rs:render (lines 25-55) ---[0m
    pub fn render(frame: &mut Frame, app: &mut AppContext) {
        let area = frame.area();
        let message = app.message.clone();
        let stack_len = app.stack.len();
        let stack_names = app.stack.names();
        let show_info = app.show_info;
        let decimals = app.float_decimals;
        let is_loading = app.is_loading();

        // Get view name for keymap lookup
        let tab = app.view().map(|v| app.plugins.tab(&v.name)).unwrap_or("table");
        let hints = app.keymap.get_hints(tab);
        let theme = app.theme.clone();

        if let Some(view) = app.view_mut() {
            let selected_cols = view.selected_cols.clone();
            let selected_rows = view.selected_rows.clone();
            let view_name = view.name.clone();
            let show_tabs = stack_names.len() > 1;
            Self::render_table(frame, view, area, &selected_cols, &selected_rows, decimals, &theme, show_tabs);
            if show_info {
                Self::render_info_box(frame, &view_name, stack_len, area, &hints, &theme);
            }
            if show_tabs {
                Self::render_tabs(frame, &stack_names, area, &theme);
            }
            Self::render_status_bar(frame, view, &message, is_loading, area, &theme);
        } else {
            Self::empty_msg(frame, &message, area);
        }
    }

[36m--- src/render/renderer.rs:column_stats (lines 388-432) ---[0m
    fn column_stats(df: &DataFrame, col_idx: usize) -> String {
        let col = df.get_columns()[col_idx].as_materialized_series();
        let len = col.len();
        if len == 0 { return String::new(); }

        let null_count = if col.dtype() == &DataType::String {
            col.str().unwrap().into_iter()
                .filter(|v| v.is_none() || v.map(|s| s.is_empty()).unwrap_or(false))
                .count()
        } else { col.null_count() };
        let null_pct = 100.0 * null_count as f64 / len as f64;

        match col.dtype() {
            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
            | DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64
            | DataType::Float32 | DataType::Float64 => {
                let col_f64 = col.cast(&DataType::Float64).ok();
                if let Some(c) = col_f64 {
                    let min = c.min::<f64>().ok().flatten().unwrap_or(f64::NAN);
                    let max = c.max::<f64>().ok().flatten().unwrap_or(f64::NAN);
                    let mean = c.mean().unwrap_or(f64::NAN);
                    let std = c.std(1).unwrap_or(f64::NAN);
                    if null_pct > 0.0 {
                        format!("null:{:.0}% [{:.2},{:.2},{:.2}] σ{:.2}", null_pct, min, mean, max, std)
                    } else {
                        format!("[{:.2},{:.2},{:.2}] σ{:.2}", min, mean, max, std)
                    }
                } else { String::new() }
            }
            _ => {
                let n_unique = col.n_unique().unwrap_or(0);
                let mode = col.value_counts(true, false, "cnt".into(), false)
                    .ok()
                    .and_then(|vc| vc.column(col.name().as_str()).ok().cloned())
                    .and_then(|c| c.get(0).ok().map(|v| v.to_string()))
                    .unwrap_or_default();
                let mode_str = if mode.len() > 10 { &mode[..10] } else { &mode };
                if null_pct > 0.0 {
                    format!("null:{:.0}% #{}'{}'", null_pct, n_unique, mode_str)
                } else {
                    format!("#{}'{}'", n_unique, mode_str)
                }
            }
        }
    }

  src/render/renderer.rs:173-223 method render_headers_xs <-> src/render/renderer.rs:474-495 method render_tabs
  Similarity: 89.65%
  Classes: Renderer <-> Renderer

[36m--- src/render/renderer.rs:render_headers_xs (lines 173-223) ---[0m
    fn render_headers_xs(frame: &mut Frame, df: &DataFrame, state: &TableState, xs: &[i32], screen_width: i32, row_num_width: u16, selected_cols: &HashSet<usize>, _col_sep: Option<usize>, theme: &Theme, area: Rect) {
        let buf = frame.buffer_mut();
        let header_style = Style::default().bg(to_rcolor(theme.header_bg)).fg(to_rcolor(theme.header_fg)).add_modifier(Modifier::BOLD);

        // Fill header row with header style first
        for x in 0..area.width { buf[(x, 0)].set_style(header_style); buf[(x, 0)].set_char(' '); }

        let mut x_pos = 0u16;

        // Row number header
        if row_num_width > 0 {
            let s = format!("{:>width$} ", "#", width = row_num_width as usize);
            for (i, ch) in s.chars().enumerate() {
                if x_pos + i as u16 >= area.width { break; }
                buf[(x_pos + i as u16, 0)].set_char(ch);
            }
            x_pos += row_num_width + 1;
        }

        for (col_idx, col_name) in df.get_column_names().iter().enumerate() {
            let x = xs[col_idx];
            let next_x = xs.get(col_idx + 1).copied().unwrap_or(x);
            if next_x <= 0 { continue; }
            if x >= screen_width { break; }

            let is_current = col_idx == state.cc;
            let is_selected = selected_cols.contains(&col_idx);
            let col_width = state.col_widths.get(col_idx).copied().unwrap_or(10) as usize;

            let style = if is_current {
                Style::default().bg(to_rcolor(theme.cursor_bg)).fg(to_rcolor(theme.cursor_fg)).add_modifier(Modifier::BOLD)
            } else if is_selected {
                Style::default().bg(to_rcolor(theme.header_bg)).fg(to_rcolor(theme.select_fg)).add_modifier(Modifier::BOLD)
            } else { header_style };

            let start_x = x.max(0) as u16 + x_pos;
            let display = format!("{:width$}", col_name, width = col_width);

            for (i, ch) in display.chars().take(col_width).enumerate() {
                let px = start_x + i as u16;
                if px >= area.width { break; }
                buf[(px, 0)].set_char(ch).set_style(style);
            }

            // Separator space
            let sep_x = start_x + col_width as u16;
            if sep_x < area.width {
                buf[(sep_x, 0)].set_char(' ').set_style(header_style);
            }
        }
    }

[36m--- src/render/renderer.rs:render_tabs (lines 474-495) ---[0m
    fn render_tabs(frame: &mut Frame, names: &[String], area: Rect, theme: &Theme) {
        let row = area.height - 2;
        let tab_area = Rect::new(0, row, area.width, 1);
        // Shorten names: extract filename, truncate to 20 chars
        let short: Vec<String> = names.iter().map(|s| {
            let n = s.rsplit('/').next().unwrap_or(s);  // get filename
            let n = n.split(':').next().unwrap_or(n);   // remove :suffix
            if n.len() > 20 { format!("{}…", &n[..19]) } else { n.to_string() }
        }).collect();
        // Fill background (use header_bg for contrast)
        let buf = frame.buffer_mut();
        let bg = to_rcolor(theme.header_bg);
        for x in 0..area.width { buf[(x, row)].set_style(Style::default().bg(bg)).set_char(' '); }
        // Render tabs
        let selected = names.len().saturating_sub(1);
        let tabs = Tabs::new(short.iter().map(|s| s.as_str()))
            .select(selected)
            .style(Style::default().fg(to_rcolor(theme.status_fg)).bg(bg))
            .highlight_style(Style::default().fg(to_rcolor(theme.header_fg)).bg(bg).add_modifier(Modifier::BOLD))
            .divider("│");
        frame.render_widget(tabs, tab_area);
    }

  src/render/renderer.rs:474-495 method render_tabs <-> src/render/renderer.rs:498-546 method render_status_bar
  Similarity: 91.01%
  Classes: Renderer <-> Renderer

[36m--- src/render/renderer.rs:render_tabs (lines 474-495) ---[0m
    fn render_tabs(frame: &mut Frame, names: &[String], area: Rect, theme: &Theme) {
        let row = area.height - 2;
        let tab_area = Rect::new(0, row, area.width, 1);
        // Shorten names: extract filename, truncate to 20 chars
        let short: Vec<String> = names.iter().map(|s| {
            let n = s.rsplit('/').next().unwrap_or(s);  // get filename
            let n = n.split(':').next().unwrap_or(n);   // remove :suffix
            if n.len() > 20 { format!("{}…", &n[..19]) } else { n.to_string() }
        }).collect();
        // Fill background (use header_bg for contrast)
        let buf = frame.buffer_mut();
        let bg = to_rcolor(theme.header_bg);
        for x in 0..area.width { buf[(x, row)].set_style(Style::default().bg(bg)).set_char(' '); }
        // Render tabs
        let selected = names.len().saturating_sub(1);
        let tabs = Tabs::new(short.iter().map(|s| s.as_str()))
            .select(selected)
            .style(Style::default().fg(to_rcolor(theme.status_fg)).bg(bg))
            .highlight_style(Style::default().fg(to_rcolor(theme.header_fg)).bg(bg).add_modifier(Modifier::BOLD))
            .divider("│");
        frame.render_widget(tabs, tab_area);
    }

[36m--- src/render/renderer.rs:render_status_bar (lines 498-546) ---[0m
    fn render_status_bar(frame: &mut Frame, view: &mut ViewState, message: &str, is_loading: bool, area: Rect, theme: &Theme) {
        let row = area.height - 1;
        let buf = frame.buffer_mut();
        let style = Style::default().bg(to_rcolor(theme.status_bg)).fg(to_rcolor(theme.status_fg));

        // Fill status bar
        for x in 0..area.width { buf[(x, row)].set_style(style); buf[(x, row)].set_char(' '); }

        // Show total rows: just disk_rows if set, else dataframe height
        let total_str = commify(&view.rows().to_string());

        let left = if !message.is_empty() { message.to_string() }
        else if view.name.starts_with("Freq:") || view.name == "metadata" {
            // Show parent name and row count for Meta/Freq views
            let pn = view.parent_name.as_deref().unwrap_or("");
            let pr = view.parent_rows.map(|n| format!(" ({})", commify(&n.to_string()))).unwrap_or_default();
            format!("{} <- {}{}", view.name, pn, pr)
        }
        else { view.filename.as_deref().unwrap_or("(no file)").to_string() };

        // Use cached stats if column unchanged
        let col_stats = if view.cols() > 0 {
            let cc = view.state.cc;
            if let Some((cached_cc, ref s)) = view.stats_cache {
                if cached_cc == cc { s.clone() }
                else {
                    let s = Self::column_stats(&view.dataframe, cc);
                    view.stats_cache = Some((cc, s.clone()));
                    s
                }
            } else {
                let s = Self::column_stats(&view.dataframe, cc);
                view.stats_cache = Some((cc, s.clone()));
                s
            }
        } else { String::new() };

        let partial = if is_loading || view.partial { " Partial" } else { "" };
        let right = if col_stats.is_empty() { format!("{}/{}{}", view.state.cr, total_str, partial) }
        else { format!("{} {}/{}{}", col_stats, view.state.cr, total_str, partial) };

        let padding = (area.width as usize).saturating_sub(left.len() + right.len()).max(1);
        let status = format!("{}{:width$}{}", left, "", right, width = padding);

        for (i, ch) in status.chars().enumerate() {
            if i >= area.width as usize { break; }
            buf[(i as u16, row)].set_char(ch);
        }
    }

  src/render/renderer.rs:388-432 method column_stats <-> src/render/renderer.rs:474-495 method render_tabs
  Similarity: 89.65%
  Classes: Renderer <-> Renderer

[36m--- src/render/renderer.rs:column_stats (lines 388-432) ---[0m
    fn column_stats(df: &DataFrame, col_idx: usize) -> String {
        let col = df.get_columns()[col_idx].as_materialized_series();
        let len = col.len();
        if len == 0 { return String::new(); }

        let null_count = if col.dtype() == &DataType::String {
            col.str().unwrap().into_iter()
                .filter(|v| v.is_none() || v.map(|s| s.is_empty()).unwrap_or(false))
                .count()
        } else { col.null_count() };
        let null_pct = 100.0 * null_count as f64 / len as f64;

        match col.dtype() {
            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
            | DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64
            | DataType::Float32 | DataType::Float64 => {
                let col_f64 = col.cast(&DataType::Float64).ok();
                if let Some(c) = col_f64 {
                    let min = c.min::<f64>().ok().flatten().unwrap_or(f64::NAN);
                    let max = c.max::<f64>().ok().flatten().unwrap_or(f64::NAN);
                    let mean = c.mean().unwrap_or(f64::NAN);
                    let std = c.std(1).unwrap_or(f64::NAN);
                    if null_pct > 0.0 {
                        format!("null:{:.0}% [{:.2},{:.2},{:.2}] σ{:.2}", null_pct, min, mean, max, std)
                    } else {
                        format!("[{:.2},{:.2},{:.2}] σ{:.2}", min, mean, max, std)
                    }
                } else { String::new() }
            }
            _ => {
                let n_unique = col.n_unique().unwrap_or(0);
                let mode = col.value_counts(true, false, "cnt".into(), false)
                    .ok()
                    .and_then(|vc| vc.column(col.name().as_str()).ok().cloned())
                    .and_then(|c| c.get(0).ok().map(|v| v.to_string()))
                    .unwrap_or_default();
                let mode_str = if mode.len() > 10 { &mode[..10] } else { &mode };
                if null_pct > 0.0 {
                    format!("null:{:.0}% #{}'{}'", null_pct, n_unique, mode_str)
                } else {
                    format!("#{}'{}'", n_unique, mode_str)
                }
            }
        }
    }

[36m--- src/render/renderer.rs:render_tabs (lines 474-495) ---[0m
    fn render_tabs(frame: &mut Frame, names: &[String], area: Rect, theme: &Theme) {
        let row = area.height - 2;
        let tab_area = Rect::new(0, row, area.width, 1);
        // Shorten names: extract filename, truncate to 20 chars
        let short: Vec<String> = names.iter().map(|s| {
            let n = s.rsplit('/').next().unwrap_or(s);  // get filename
            let n = n.split(':').next().unwrap_or(n);   // remove :suffix
            if n.len() > 20 { format!("{}…", &n[..19]) } else { n.to_string() }
        }).collect();
        // Fill background (use header_bg for contrast)
        let buf = frame.buffer_mut();
        let bg = to_rcolor(theme.header_bg);
        for x in 0..area.width { buf[(x, row)].set_style(Style::default().bg(bg)).set_char(' '); }
        // Render tabs
        let selected = names.len().saturating_sub(1);
        let tabs = Tabs::new(short.iter().map(|s| s.as_str()))
            .select(selected)
            .style(Style::default().fg(to_rcolor(theme.status_fg)).bg(bg))
            .highlight_style(Style::default().fg(to_rcolor(theme.header_fg)).bg(bg).add_modifier(Modifier::BOLD))
            .divider("│");
        frame.render_widget(tabs, tab_area);
    }

  src/render/renderer.rs:25-55 method render <-> src/render/renderer.rs:474-495 method render_tabs
  Similarity: 89.63%
  Classes: Renderer <-> Renderer

[36m--- src/render/renderer.rs:render (lines 25-55) ---[0m
    pub fn render(frame: &mut Frame, app: &mut AppContext) {
        let area = frame.area();
        let message = app.message.clone();
        let stack_len = app.stack.len();
        let stack_names = app.stack.names();
        let show_info = app.show_info;
        let decimals = app.float_decimals;
        let is_loading = app.is_loading();

        // Get view name for keymap lookup
        let tab = app.view().map(|v| app.plugins.tab(&v.name)).unwrap_or("table");
        let hints = app.keymap.get_hints(tab);
        let theme = app.theme.clone();

        if let Some(view) = app.view_mut() {
            let selected_cols = view.selected_cols.clone();
            let selected_rows = view.selected_rows.clone();
            let view_name = view.name.clone();
            let show_tabs = stack_names.len() > 1;
            Self::render_table(frame, view, area, &selected_cols, &selected_rows, decimals, &theme, show_tabs);
            if show_info {
                Self::render_info_box(frame, &view_name, stack_len, area, &hints, &theme);
            }
            if show_tabs {
                Self::render_tabs(frame, &stack_names, area, &theme);
            }
            Self::render_status_bar(frame, view, &message, is_loading, area, &theme);
        } else {
            Self::empty_msg(frame, &message, area);
        }
    }

[36m--- src/render/renderer.rs:render_tabs (lines 474-495) ---[0m
    fn render_tabs(frame: &mut Frame, names: &[String], area: Rect, theme: &Theme) {
        let row = area.height - 2;
        let tab_area = Rect::new(0, row, area.width, 1);
        // Shorten names: extract filename, truncate to 20 chars
        let short: Vec<String> = names.iter().map(|s| {
            let n = s.rsplit('/').next().unwrap_or(s);  // get filename
            let n = n.split(':').next().unwrap_or(n);   // remove :suffix
            if n.len() > 20 { format!("{}…", &n[..19]) } else { n.to_string() }
        }).collect();
        // Fill background (use header_bg for contrast)
        let buf = frame.buffer_mut();
        let bg = to_rcolor(theme.header_bg);
        for x in 0..area.width { buf[(x, row)].set_style(Style::default().bg(bg)).set_char(' '); }
        // Render tabs
        let selected = names.len().saturating_sub(1);
        let tabs = Tabs::new(short.iter().map(|s| s.as_str()))
            .select(selected)
            .style(Style::default().fg(to_rcolor(theme.status_fg)).bg(bg))
            .highlight_style(Style::default().fg(to_rcolor(theme.header_fg)).bg(bg).add_modifier(Modifier::BOLD))
            .divider("│");
        frame.render_widget(tabs, tab_area);
    }

  src/render/renderer.rs:14-19 function dbg_log <-> src/render/renderer.rs:381-385 method commify_float
  Similarity: 89.08%

[36m--- src/render/renderer.rs:dbg_log (lines 14-19) ---[0m
fn dbg_log(msg: &str) {
    if let Ok(mut f) = std::fs::OpenOptions::new().create(true).append(true).open("/tmp/tv.debug.log") {
        let ts = SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis()).unwrap_or(0);
        let _ = writeln!(f, "{} {}", ts, msg);
    }
}

[36m--- src/render/renderer.rs:commify_float (lines 381-385) ---[0m
    fn commify_float(s: &str) -> String {
        if let Some(dot) = s.find('.') {
            format!("{}{}", Self::commify_str(&s[..dot]), &s[dot..])
        } else { Self::commify_str(s) }
    }


Duplicates in src/backend/polars.rs:
------------------------------------------------------------
  src/backend/polars.rs:19-43 method load <-> src/backend/polars.rs:109-136 function schema_diff
  Similarity: 91.14%

[36m--- src/backend/polars.rs:load (lines 19-43) ---[0m
    fn load(&self, path: &str, id: usize) -> Result<LoadResult> {
        const MAX_PREVIEW: u32 = 100_000;
        // Glob pattern
        if path.contains('*') || path.contains('?') {
            let df = load_glob(path, MAX_PREVIEW)?;
            if df.height() == 0 { return Err(anyhow!("No data found")); }
            return Ok(LoadResult { view: ViewState::new(id, path.into(), df, None), bg_loader: None });
        }
        let p = Path::new(path);
        if !p.exists() { return Err(anyhow!("File not found: {}", path)); }
        match p.extension().and_then(|s| s.to_str()) {
            Some("csv") => {
                let df = load_csv(p)?;
                if df.height() == 0 { return Err(anyhow!("File is empty")); }
                Ok(LoadResult { view: ViewState::new(id, path.into(), df, Some(path.into())), bg_loader: None })
            }
            Some("parquet") => {
                let (rows, cols) = self.metadata(path)?;
                if rows == 0 { return Err(anyhow!("File is empty")); }
                Ok(LoadResult { view: ViewState::new_parquet(id, path.into(), path.into(), rows, cols), bg_loader: None })
            }
            Some(ext) => Err(anyhow!("Unsupported: {}", ext)),
            None => Err(anyhow!("Unknown file type")),
        }
    }

[36m--- src/backend/polars.rs:schema_diff (lines 109-136) ---[0m
fn schema_diff(pattern: &str) -> Option<anyhow::Error> {
    let output = std::process::Command::new("sh")
        .args(["-c", &format!("ls -1 {} 2>/dev/null", pattern)])
        .output().ok()?;
    let paths: Vec<&str> = std::str::from_utf8(&output.stdout).ok()?
        .lines().filter(|l| !l.is_empty()).collect();
    if paths.len() < 2 { return None; }

    let first = paths[0];
    let base_schema = ParquetReader::new(std::fs::File::open(first).ok()?).schema().ok()?;

    for path in &paths[1..] {
        if let Ok(file) = std::fs::File::open(path) {
            if let Ok(schema) = ParquetReader::new(file).schema() {
                for (name, dtype) in schema.iter() {
                    let name_str = name.as_str();
                    if let Some((_, base_dtype)) = base_schema.iter().find(|(n, _)| n.as_str() == name_str) {
                        if dtype != base_dtype {
                            return Some(anyhow!("Schema mismatch for '{}': {} has {:?}, {} has {:?}",
                                name_str, first, base_dtype.dtype(), path, dtype.dtype()));
                        }
                    }
                }
            }
        }
    }
    None
}

  src/backend/polars.rs:49-55 function detect_sep <-> src/backend/polars.rs:97-106 function load_glob
  Similarity: 93.62%

[36m--- src/backend/polars.rs:detect_sep (lines 49-55) ---[0m
pub fn detect_sep(line: &str) -> u8 {
    let seps = [(b'|', line.matches('|').count()),
                (b'\t', line.matches('\t').count()),
                (b',', line.matches(',').count()),
                (b';', line.matches(';').count())];
    seps.into_iter().max_by_key(|&(_, n)| n).map(|(c, _)| c).unwrap_or(b',')
}

[36m--- src/backend/polars.rs:load_glob (lines 97-106) ---[0m
pub fn load_glob(pattern: &str, limit: u32) -> Result<DataFrame> {
    let lf = LazyFrame::scan_parquet(PlPath::new(pattern), ScanArgsParquet::default())
        .map_err(|e| anyhow!("Failed to scan parquet: {}", e))?;
    lf.limit(limit).collect()
        .map_err(|e| {
            if e.to_string().contains("mismatch") {
                schema_diff(pattern).unwrap_or_else(|| anyhow!("{}", e))
            } else { anyhow!("{}", e) }
        })
}

  src/backend/polars.rs:76-81 function save_csv <-> src/backend/polars.rs:97-106 function load_glob
  Similarity: 94.62%

[36m--- src/backend/polars.rs:save_csv (lines 76-81) ---[0m
pub fn save_csv(df: &DataFrame, path: &Path) -> Result<()> {
    CsvWriter::new(&mut std::fs::File::create(path)?)
        .finish(&mut df.clone())
        .map_err(|e| anyhow!("Failed to write CSV: {}", e))?;
    Ok(())
}

[36m--- src/backend/polars.rs:load_glob (lines 97-106) ---[0m
pub fn load_glob(pattern: &str, limit: u32) -> Result<DataFrame> {
    let lf = LazyFrame::scan_parquet(PlPath::new(pattern), ScanArgsParquet::default())
        .map_err(|e| anyhow!("Failed to scan parquet: {}", e))?;
    lf.limit(limit).collect()
        .map_err(|e| {
            if e.to_string().contains("mismatch") {
                schema_diff(pattern).unwrap_or_else(|| anyhow!("{}", e))
            } else { anyhow!("{}", e) }
        })
}

  src/backend/polars.rs:49-55 function detect_sep <-> src/backend/polars.rs:76-81 function save_csv
  Similarity: 95.13%

[36m--- src/backend/polars.rs:detect_sep (lines 49-55) ---[0m
pub fn detect_sep(line: &str) -> u8 {
    let seps = [(b'|', line.matches('|').count()),
                (b'\t', line.matches('\t').count()),
                (b',', line.matches(',').count()),
                (b';', line.matches(';').count())];
    seps.into_iter().max_by_key(|&(_, n)| n).map(|(c, _)| c).unwrap_or(b',')
}

[36m--- src/backend/polars.rs:save_csv (lines 76-81) ---[0m
pub fn save_csv(df: &DataFrame, path: &Path) -> Result<()> {
    CsvWriter::new(&mut std::fs::File::create(path)?)
        .finish(&mut df.clone())
        .map_err(|e| anyhow!("Failed to write CSV: {}", e))?;
    Ok(())
}


Duplicates in src/state.rs:
------------------------------------------------------------
  src/state.rs:101-109 method backend <-> src/state.rs:117-122 method key_cols
  Similarity: 90.71%
  Classes: ViewState <-> ViewState

[36m--- src/state.rs:backend (lines 101-109) ---[0m
    pub fn backend(&self) -> Box<dyn Backend + '_> {
        if self.parquet_path.is_some() {
            Box::new(Polars)
        } else if self.gz_source.is_some() {
            Box::new(Gz { df: &self.dataframe, partial: self.partial })
        } else {
            Box::new(Memory(&self.dataframe))
        }
    }

[36m--- src/state.rs:key_cols (lines 117-122) ---[0m
    pub fn key_cols(&self) -> Vec<String> {
        self.col_separator.map(|sep| {
            self.dataframe.get_column_names()[..sep].iter()
                .map(|s| s.to_string()).collect()
        }).unwrap_or_default()
    }

  src/state.rs:117-122 method key_cols <-> src/state.rs:161-163 method new_filtered
  Similarity: 94.62%
  Classes: ViewState <-> ViewState

[36m--- src/state.rs:key_cols (lines 117-122) ---[0m
    pub fn key_cols(&self) -> Vec<String> {
        self.col_separator.map(|sep| {
            self.dataframe.get_column_names()[..sep].iter()
                .map(|s| s.to_string()).collect()
        }).unwrap_or_default()
    }

[36m--- src/state.rs:new_filtered (lines 161-163) ---[0m
    pub fn new_filtered(id: usize, name: String, path: String, cols: Vec<String>, filter: String, count: usize) -> Self {
        Self { filename: Some(path.clone()), disk_rows: Some(count), parquet_path: Some(path), col_names: cols, filter_clause: Some(filter), ..Self::base(id, name, DataFrame::empty()) }
    }

  src/state.rs:18-20 method new <-> src/state.rs:117-122 method key_cols
  Similarity: 94.49%
  Classes: TableState <-> ViewState

[36m--- src/state.rs:new (lines 18-20) ---[0m
    pub fn new() -> Self {
        Self { r0: 0, cr: 0, cc: 0, viewport: (0, 0), col_widths: Vec::new(), widths_row: 0 }
    }

[36m--- src/state.rs:key_cols (lines 117-122) ---[0m
    pub fn key_cols(&self) -> Vec<String> {
        self.col_separator.map(|sep| {
            self.dataframe.get_column_names()[..sep].iter()
                .map(|s| s.to_string()).collect()
        }).unwrap_or_default()
    }

  src/state.rs:117-122 method key_cols <-> src/state.rs:141-143 method new_parquet
  Similarity: 94.49%
  Classes: ViewState <-> ViewState

[36m--- src/state.rs:key_cols (lines 117-122) ---[0m
    pub fn key_cols(&self) -> Vec<String> {
        self.col_separator.map(|sep| {
            self.dataframe.get_column_names()[..sep].iter()
                .map(|s| s.to_string()).collect()
        }).unwrap_or_default()
    }

[36m--- src/state.rs:new_parquet (lines 141-143) ---[0m
    pub fn new_parquet(id: usize, name: String, path: String, rows: usize, cols: Vec<String>) -> Self {
        Self { filename: Some(path.clone()), disk_rows: Some(rows), parquet_path: Some(path), col_names: cols, ..Self::base(id, name, DataFrame::empty()) }
    }

  src/state.rs:117-122 method key_cols <-> src/state.rs:151-153 method new_child
  Similarity: 94.49%
  Classes: ViewState <-> ViewState

[36m--- src/state.rs:key_cols (lines 117-122) ---[0m
    pub fn key_cols(&self) -> Vec<String> {
        self.col_separator.map(|sep| {
            self.dataframe.get_column_names()[..sep].iter()
                .map(|s| s.to_string()).collect()
        }).unwrap_or_default()
    }

[36m--- src/state.rs:new_child (lines 151-153) ---[0m
    pub fn new_child(id: usize, name: String, df: DataFrame, pid: usize, prows: usize, pname: String) -> Self {
        Self { parent_id: Some(pid), parent_rows: Some(prows), parent_name: Some(pname), ..Self::base(id, name, df) }
    }

  src/state.rs:117-122 method key_cols <-> src/state.rs:156-158 method new_freq
  Similarity: 94.49%
  Classes: ViewState <-> ViewState

[36m--- src/state.rs:key_cols (lines 117-122) ---[0m
    pub fn key_cols(&self) -> Vec<String> {
        self.col_separator.map(|sep| {
            self.dataframe.get_column_names()[..sep].iter()
                .map(|s| s.to_string()).collect()
        }).unwrap_or_default()
    }

[36m--- src/state.rs:new_freq (lines 156-158) ---[0m
    pub fn new_freq(id: usize, name: String, df: DataFrame, pid: usize, prows: usize, pname: String, col: String) -> Self {
        Self { parent_id: Some(pid), parent_rows: Some(prows), parent_name: Some(pname), freq_col: Some(col), ..Self::base(id, name, df) }
    }

  src/state.rs:42-44 method right <-> src/state.rs:117-122 method key_cols
  Similarity: 91.54%
  Classes: TableState <-> ViewState

[36m--- src/state.rs:right (lines 42-44) ---[0m
    pub fn right(&mut self, n: usize, max: usize) {
        if max > 0 { self.cc = (self.cc + n).min(max - 1); }
    }

[36m--- src/state.rs:key_cols (lines 117-122) ---[0m
    pub fn key_cols(&self) -> Vec<String> {
        self.col_separator.map(|sep| {
            self.dataframe.get_column_names()[..sep].iter()
                .map(|s| s.to_string()).collect()
        }).unwrap_or_default()
    }

  src/state.rs:117-122 method key_cols <-> src/state.rs:211-213 method names
  Similarity: 90.26%
  Classes: ViewState <-> StateStack

[36m--- src/state.rs:key_cols (lines 117-122) ---[0m
    pub fn key_cols(&self) -> Vec<String> {
        self.col_separator.map(|sep| {
            self.dataframe.get_column_names()[..sep].iter()
                .map(|s| s.to_string()).collect()
        }).unwrap_or_default()
    }

[36m--- src/state.rs:names (lines 211-213) ---[0m
    pub fn names(&self) -> Vec<String> {
        self.stack.iter().map(|v| v.name.clone()).collect()
    }

  src/state.rs:23-25 method need_widths <-> src/state.rs:117-122 method key_cols
  Similarity: 86.92%
  Classes: TableState <-> ViewState

[36m--- src/state.rs:need_widths (lines 23-25) ---[0m
    pub fn need_widths(&self) -> bool {
        self.col_widths.is_empty() || self.cr.abs_diff(self.widths_row) > self.viewport.0.saturating_sub(2) as usize
    }

[36m--- src/state.rs:key_cols (lines 117-122) ---[0m
    pub fn key_cols(&self) -> Vec<String> {
        self.col_separator.map(|sep| {
            self.dataframe.get_column_names()[..sep].iter()
                .map(|s| s.to_string()).collect()
        }).unwrap_or_default()
    }

  src/state.rs:141-143 method new_parquet <-> src/state.rs:161-163 method new_filtered
  Similarity: 95.88%
  Classes: ViewState <-> ViewState

[36m--- src/state.rs:new_parquet (lines 141-143) ---[0m
    pub fn new_parquet(id: usize, name: String, path: String, rows: usize, cols: Vec<String>) -> Self {
        Self { filename: Some(path.clone()), disk_rows: Some(rows), parquet_path: Some(path), col_names: cols, ..Self::base(id, name, DataFrame::empty()) }
    }

[36m--- src/state.rs:new_filtered (lines 161-163) ---[0m
    pub fn new_filtered(id: usize, name: String, path: String, cols: Vec<String>, filter: String, count: usize) -> Self {
        Self { filename: Some(path.clone()), disk_rows: Some(count), parquet_path: Some(path), col_names: cols, filter_clause: Some(filter), ..Self::base(id, name, DataFrame::empty()) }
    }

  src/state.rs:151-153 method new_child <-> src/state.rs:156-158 method new_freq
  Similarity: 95.08%
  Classes: ViewState <-> ViewState

[36m--- src/state.rs:new_child (lines 151-153) ---[0m
    pub fn new_child(id: usize, name: String, df: DataFrame, pid: usize, prows: usize, pname: String) -> Self {
        Self { parent_id: Some(pid), parent_rows: Some(prows), parent_name: Some(pname), ..Self::base(id, name, df) }
    }

[36m--- src/state.rs:new_freq (lines 156-158) ---[0m
    pub fn new_freq(id: usize, name: String, df: DataFrame, pid: usize, prows: usize, pname: String, col: String) -> Self {
        Self { parent_id: Some(pid), parent_rows: Some(prows), parent_name: Some(pname), freq_col: Some(col), ..Self::base(id, name, df) }
    }

  src/state.rs:161-163 method new_filtered <-> src/state.rs:211-213 method names
  Similarity: 94.62%
  Classes: ViewState <-> StateStack

[36m--- src/state.rs:new_filtered (lines 161-163) ---[0m
    pub fn new_filtered(id: usize, name: String, path: String, cols: Vec<String>, filter: String, count: usize) -> Self {
        Self { filename: Some(path.clone()), disk_rows: Some(count), parquet_path: Some(path), col_names: cols, filter_clause: Some(filter), ..Self::base(id, name, DataFrame::empty()) }
    }

[36m--- src/state.rs:names (lines 211-213) ---[0m
    pub fn names(&self) -> Vec<String> {
        self.stack.iter().map(|v| v.name.clone()).collect()
    }

  src/state.rs:141-143 method new_parquet <-> src/state.rs:211-213 method names
  Similarity: 93.86%
  Classes: ViewState <-> StateStack

[36m--- src/state.rs:new_parquet (lines 141-143) ---[0m
    pub fn new_parquet(id: usize, name: String, path: String, rows: usize, cols: Vec<String>) -> Self {
        Self { filename: Some(path.clone()), disk_rows: Some(rows), parquet_path: Some(path), col_names: cols, ..Self::base(id, name, DataFrame::empty()) }
    }

[36m--- src/state.rs:names (lines 211-213) ---[0m
    pub fn names(&self) -> Vec<String> {
        self.stack.iter().map(|v| v.name.clone()).collect()
    }

  src/state.rs:141-143 method new_parquet <-> src/state.rs:156-158 method new_freq
  Similarity: 93.14%
  Classes: ViewState <-> ViewState

[36m--- src/state.rs:new_parquet (lines 141-143) ---[0m
    pub fn new_parquet(id: usize, name: String, path: String, rows: usize, cols: Vec<String>) -> Self {
        Self { filename: Some(path.clone()), disk_rows: Some(rows), parquet_path: Some(path), col_names: cols, ..Self::base(id, name, DataFrame::empty()) }
    }

[36m--- src/state.rs:new_freq (lines 156-158) ---[0m
    pub fn new_freq(id: usize, name: String, df: DataFrame, pid: usize, prows: usize, pname: String, col: String) -> Self {
        Self { parent_id: Some(pid), parent_rows: Some(prows), parent_name: Some(pname), freq_col: Some(col), ..Self::base(id, name, df) }
    }

  src/state.rs:156-158 method new_freq <-> src/state.rs:211-213 method names
  Similarity: 92.95%
  Classes: ViewState <-> StateStack

[36m--- src/state.rs:new_freq (lines 156-158) ---[0m
    pub fn new_freq(id: usize, name: String, df: DataFrame, pid: usize, prows: usize, pname: String, col: String) -> Self {
        Self { parent_id: Some(pid), parent_rows: Some(prows), parent_name: Some(pname), freq_col: Some(col), ..Self::base(id, name, df) }
    }

[36m--- src/state.rs:names (lines 211-213) ---[0m
    pub fn names(&self) -> Vec<String> {
        self.stack.iter().map(|v| v.name.clone()).collect()
    }

  src/state.rs:151-153 method new_child <-> src/state.rs:211-213 method names
  Similarity: 91.57%
  Classes: ViewState <-> StateStack

[36m--- src/state.rs:new_child (lines 151-153) ---[0m
    pub fn new_child(id: usize, name: String, df: DataFrame, pid: usize, prows: usize, pname: String) -> Self {
        Self { parent_id: Some(pid), parent_rows: Some(prows), parent_name: Some(pname), ..Self::base(id, name, df) }
    }

[36m--- src/state.rs:names (lines 211-213) ---[0m
    pub fn names(&self) -> Vec<String> {
        self.stack.iter().map(|v| v.name.clone()).collect()
    }

  src/state.rs:156-158 method new_freq <-> src/state.rs:161-163 method new_filtered
  Similarity: 91.12%
  Classes: ViewState <-> ViewState

[36m--- src/state.rs:new_freq (lines 156-158) ---[0m
    pub fn new_freq(id: usize, name: String, df: DataFrame, pid: usize, prows: usize, pname: String, col: String) -> Self {
        Self { parent_id: Some(pid), parent_rows: Some(prows), parent_name: Some(pname), freq_col: Some(col), ..Self::base(id, name, df) }
    }

[36m--- src/state.rs:new_filtered (lines 161-163) ---[0m
    pub fn new_filtered(id: usize, name: String, path: String, cols: Vec<String>, filter: String, count: usize) -> Self {
        Self { filename: Some(path.clone()), disk_rows: Some(count), parquet_path: Some(path), col_names: cols, filter_clause: Some(filter), ..Self::base(id, name, DataFrame::empty()) }
    }

  src/state.rs:146-148 method new_gz <-> src/state.rs:151-153 method new_child
  Similarity: 90.98%
  Classes: ViewState <-> ViewState

[36m--- src/state.rs:new_gz (lines 146-148) ---[0m
    pub fn new_gz(id: usize, name: String, df: DataFrame, filename: Option<String>, gz: String, partial: bool) -> Self {
        Self { filename, gz_source: Some(gz), partial, ..Self::base(id, name, df) }
    }

[36m--- src/state.rs:new_child (lines 151-153) ---[0m
    pub fn new_child(id: usize, name: String, df: DataFrame, pid: usize, prows: usize, pname: String) -> Self {
        Self { parent_id: Some(pid), parent_rows: Some(prows), parent_name: Some(pname), ..Self::base(id, name, df) }
    }

  src/state.rs:18-20 method new <-> src/state.rs:211-213 method names
  Similarity: 90.85%
  Classes: TableState <-> StateStack

[36m--- src/state.rs:new (lines 18-20) ---[0m
    pub fn new() -> Self {
        Self { r0: 0, cr: 0, cc: 0, viewport: (0, 0), col_widths: Vec::new(), widths_row: 0 }
    }

[36m--- src/state.rs:names (lines 211-213) ---[0m
    pub fn names(&self) -> Vec<String> {
        self.stack.iter().map(|v| v.name.clone()).collect()
    }

  src/state.rs:141-143 method new_parquet <-> src/state.rs:151-153 method new_child
  Similarity: 90.71%
  Classes: ViewState <-> ViewState

[36m--- src/state.rs:new_parquet (lines 141-143) ---[0m
    pub fn new_parquet(id: usize, name: String, path: String, rows: usize, cols: Vec<String>) -> Self {
        Self { filename: Some(path.clone()), disk_rows: Some(rows), parquet_path: Some(path), col_names: cols, ..Self::base(id, name, DataFrame::empty()) }
    }

[36m--- src/state.rs:new_child (lines 151-153) ---[0m
    pub fn new_child(id: usize, name: String, df: DataFrame, pid: usize, prows: usize, pname: String) -> Self {
        Self { parent_id: Some(pid), parent_rows: Some(prows), parent_name: Some(pname), ..Self::base(id, name, df) }
    }

  src/state.rs:146-148 method new_gz <-> src/state.rs:211-213 method names
  Similarity: 89.76%
  Classes: ViewState <-> StateStack

[36m--- src/state.rs:new_gz (lines 146-148) ---[0m
    pub fn new_gz(id: usize, name: String, df: DataFrame, filename: Option<String>, gz: String, partial: bool) -> Self {
        Self { filename, gz_source: Some(gz), partial, ..Self::base(id, name, df) }
    }

[36m--- src/state.rs:names (lines 211-213) ---[0m
    pub fn names(&self) -> Vec<String> {
        self.stack.iter().map(|v| v.name.clone()).collect()
    }

  src/state.rs:151-153 method new_child <-> src/state.rs:161-163 method new_filtered
  Similarity: 89.00%
  Classes: ViewState <-> ViewState

[36m--- src/state.rs:new_child (lines 151-153) ---[0m
    pub fn new_child(id: usize, name: String, df: DataFrame, pid: usize, prows: usize, pname: String) -> Self {
        Self { parent_id: Some(pid), parent_rows: Some(prows), parent_name: Some(pname), ..Self::base(id, name, df) }
    }

[36m--- src/state.rs:new_filtered (lines 161-163) ---[0m
    pub fn new_filtered(id: usize, name: String, path: String, cols: Vec<String>, filter: String, count: usize) -> Self {
        Self { filename: Some(path.clone()), disk_rows: Some(count), parquet_path: Some(path), col_names: cols, filter_clause: Some(filter), ..Self::base(id, name, DataFrame::empty()) }
    }

  src/state.rs:141-143 method new_parquet <-> src/state.rs:146-148 method new_gz
  Similarity: 87.71%
  Classes: ViewState <-> ViewState

[36m--- src/state.rs:new_parquet (lines 141-143) ---[0m
    pub fn new_parquet(id: usize, name: String, path: String, rows: usize, cols: Vec<String>) -> Self {
        Self { filename: Some(path.clone()), disk_rows: Some(rows), parquet_path: Some(path), col_names: cols, ..Self::base(id, name, DataFrame::empty()) }
    }

[36m--- src/state.rs:new_gz (lines 146-148) ---[0m
    pub fn new_gz(id: usize, name: String, df: DataFrame, filename: Option<String>, gz: String, partial: bool) -> Self {
        Self { filename, gz_source: Some(gz), partial, ..Self::base(id, name, df) }
    }

  src/state.rs:146-148 method new_gz <-> src/state.rs:156-158 method new_freq
  Similarity: 87.54%
  Classes: ViewState <-> ViewState

[36m--- src/state.rs:new_gz (lines 146-148) ---[0m
    pub fn new_gz(id: usize, name: String, df: DataFrame, filename: Option<String>, gz: String, partial: bool) -> Self {
        Self { filename, gz_source: Some(gz), partial, ..Self::base(id, name, df) }
    }

[36m--- src/state.rs:new_freq (lines 156-158) ---[0m
    pub fn new_freq(id: usize, name: String, df: DataFrame, pid: usize, prows: usize, pname: String, col: String) -> Self {
        Self { parent_id: Some(pid), parent_rows: Some(prows), parent_name: Some(pname), freq_col: Some(col), ..Self::base(id, name, df) }
    }

  src/state.rs:18-20 method new <-> src/state.rs:161-163 method new_filtered
  Similarity: 87.38%
  Classes: TableState <-> ViewState

[36m--- src/state.rs:new (lines 18-20) ---[0m
    pub fn new() -> Self {
        Self { r0: 0, cr: 0, cc: 0, viewport: (0, 0), col_widths: Vec::new(), widths_row: 0 }
    }

[36m--- src/state.rs:new_filtered (lines 161-163) ---[0m
    pub fn new_filtered(id: usize, name: String, path: String, cols: Vec<String>, filter: String, count: usize) -> Self {
        Self { filename: Some(path.clone()), disk_rows: Some(count), parquet_path: Some(path), col_names: cols, filter_clause: Some(filter), ..Self::base(id, name, DataFrame::empty()) }
    }

  src/state.rs:18-20 method new <-> src/state.rs:141-143 method new_parquet
  Similarity: 85.43%
  Classes: TableState <-> ViewState

[36m--- src/state.rs:new (lines 18-20) ---[0m
    pub fn new() -> Self {
        Self { r0: 0, cr: 0, cc: 0, viewport: (0, 0), col_widths: Vec::new(), widths_row: 0 }
    }

[36m--- src/state.rs:new_parquet (lines 141-143) ---[0m
    pub fn new_parquet(id: usize, name: String, path: String, rows: usize, cols: Vec<String>) -> Self {
        Self { filename: Some(path.clone()), disk_rows: Some(rows), parquet_path: Some(path), col_names: cols, ..Self::base(id, name, DataFrame::empty()) }
    }


Duplicates in src/plugin/mod.rs:
------------------------------------------------------------
  src/plugin/mod.rs:62-68 method all <-> src/plugin/mod.rs:76-78 method tab
  Similarity: 94.53%
  Classes: Registry <-> Registry

[36m--- src/plugin/mod.rs:all (lines 62-68) ---[0m
    pub fn all() -> Self {
        Self { plugins: vec![
            Box::new(meta::MetaPlugin), Box::new(freq::FreqPlugin),
            Box::new(folder::FolderPlugin), Box::new(system::SystemPlugin),
            Box::new(corr::CorrPlugin), Box::new(pivot::PivotPlugin),
        ]}
    }

[36m--- src/plugin/mod.rs:tab (lines 76-78) ---[0m
    pub fn tab(&self, name: &str) -> &str {
        self.find(name).map(|p| p.tab()).unwrap_or("table")
    }

  src/plugin/mod.rs:62-68 method all <-> src/plugin/mod.rs:71-73 method find
  Similarity: 92.53%
  Classes: Registry <-> Registry

[36m--- src/plugin/mod.rs:all (lines 62-68) ---[0m
    pub fn all() -> Self {
        Self { plugins: vec![
            Box::new(meta::MetaPlugin), Box::new(freq::FreqPlugin),
            Box::new(folder::FolderPlugin), Box::new(system::SystemPlugin),
            Box::new(corr::CorrPlugin), Box::new(pivot::PivotPlugin),
        ]}
    }

[36m--- src/plugin/mod.rs:find (lines 71-73) ---[0m
    pub fn find(&self, name: &str) -> Option<&dyn Plugin> {
        self.plugins.iter().find(|p| p.matches(name)).map(|p| p.as_ref())
    }

  src/plugin/mod.rs:81-83 method handle <-> src/plugin/mod.rs:86-88 method parse
  Similarity: 90.86%
  Classes: Registry <-> Registry

[36m--- src/plugin/mod.rs:handle (lines 81-83) ---[0m
    pub fn handle(&self, view_name: &str, cmd: &str, app: &mut AppContext) -> Option<Box<dyn Command>> {
        self.find(view_name).and_then(|p| p.handle(cmd, app))
    }

[36m--- src/plugin/mod.rs:parse (lines 86-88) ---[0m
    pub fn parse(&self, cmd: &str, arg: &str) -> Option<Box<dyn Command>> {
        self.plugins.iter().find_map(|p| p.parse(cmd, arg))
    }

  src/plugin/mod.rs:71-73 method find <-> src/plugin/mod.rs:81-83 method handle
  Similarity: 89.43%
  Classes: Registry <-> Registry

[36m--- src/plugin/mod.rs:find (lines 71-73) ---[0m
    pub fn find(&self, name: &str) -> Option<&dyn Plugin> {
        self.plugins.iter().find(|p| p.matches(name)).map(|p| p.as_ref())
    }

[36m--- src/plugin/mod.rs:handle (lines 81-83) ---[0m
    pub fn handle(&self, view_name: &str, cmd: &str, app: &mut AppContext) -> Option<Box<dyn Command>> {
        self.find(view_name).and_then(|p| p.handle(cmd, app))
    }

  src/plugin/mod.rs:71-73 method find <-> src/plugin/mod.rs:76-78 method tab
  Similarity: 85.85%
  Classes: Registry <-> Registry

[36m--- src/plugin/mod.rs:find (lines 71-73) ---[0m
    pub fn find(&self, name: &str) -> Option<&dyn Plugin> {
        self.plugins.iter().find(|p| p.matches(name)).map(|p| p.as_ref())
    }

[36m--- src/plugin/mod.rs:tab (lines 76-78) ---[0m
    pub fn tab(&self, name: &str) -> &str {
        self.find(name).map(|p| p.tab()).unwrap_or("table")
    }

  src/plugin/mod.rs:76-78 method tab <-> src/plugin/mod.rs:81-83 method handle
  Similarity: 85.61%
  Classes: Registry <-> Registry

[36m--- src/plugin/mod.rs:tab (lines 76-78) ---[0m
    pub fn tab(&self, name: &str) -> &str {
        self.find(name).map(|p| p.tab()).unwrap_or("table")
    }

[36m--- src/plugin/mod.rs:handle (lines 81-83) ---[0m
    pub fn handle(&self, view_name: &str, cmd: &str, app: &mut AppContext) -> Option<Box<dyn Command>> {
        self.find(view_name).and_then(|p| p.handle(cmd, app))
    }


Duplicates in src/plugin/freq.rs:
------------------------------------------------------------
  src/plugin/freq.rs:138-151 method exec <-> src/plugin/freq.rs:184-202 function freq_agg_df
  Similarity: 95.94%

[36m--- src/plugin/freq.rs:exec (lines 138-151) ---[0m
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        // Extract col and values from current freq view
        let (col, values) = app.view().and_then(|v| {
            let col = v.freq_col.clone()?;
            let rows: Vec<usize> = if v.selected_rows.is_empty() { vec![v.state.cr] }
                else { v.selected_rows.iter().copied().collect() };
            let vals: Vec<String> = rows.iter()
                .filter_map(|&r| v.dataframe.get_columns()[0].get(r).ok().map(|x| unquote(&x.to_string())))
                .collect();
            Some((col, vals))
        }).ok_or_else(|| anyhow!("Not a freq view"))?;
        // Delegate to FreqEnter
        FreqEnter { col, values }.exec(app)
    }

[36m--- src/plugin/freq.rs:freq_agg_df (lines 184-202) ---[0m
fn freq_agg_df(df: &DataFrame, grp: &str, _w: &str) -> Result<DataFrame> {
    // Build aggregate expressions: COUNT + MIN/MAX/SUM for numeric columns
    let mut agg_exprs: Vec<Expr> = vec![len().alias("Cnt")];
    for (name, dt) in df.schema().iter() {
        let n = name.to_string();
        if n == grp { continue; }
        if is_numeric(dt) {
            agg_exprs.push(col(&n).min().alias(&format!("{}_min", n)));
            agg_exprs.push(col(&n).max().alias(&format!("{}_max", n)));
            agg_exprs.push(col(&n).sum().alias(&format!("{}_sum", n)));
        }
    }
    df.clone().lazy()
        .group_by([col(grp)])
        .agg(agg_exprs)
        .sort(["Cnt"], SortMultipleOptions::default().with_order_descending(true))
        .collect()
        .map_err(|e| anyhow!("{}", e))
}

  src/plugin/freq.rs:25-42 method handle <-> src/plugin/freq.rs:138-151 method exec
  Similarity: 88.47%
  Classes: Plugin <-> Command

[36m--- src/plugin/freq.rs:handle (lines 25-42) ---[0m
    fn handle(&self, cmd: &str, app: &mut AppContext) -> Option<Box<dyn Command>> {
        match cmd {
            "enter" | "filter_parent" => {
                // Extract column name and selected values from freq view
                let info = app.view().and_then(|v| {
                    let col = v.freq_col.clone()?;
                    let rows: Vec<usize> = if v.selected_rows.is_empty() { vec![v.state.cr] }
                        else { v.selected_rows.iter().copied().collect() };
                    let vals: Vec<String> = rows.iter()
                        .filter_map(|&r| v.dataframe.get_columns()[0].get(r).ok().map(|x| unquote(&x.to_string())))
                        .collect();
                    Some((col, vals))
                });
                info.map(|(col, values)| Box::new(FreqEnter { col, values }) as Box<dyn Command>)
            }
            _ => None,
        }
    }

[36m--- src/plugin/freq.rs:exec (lines 138-151) ---[0m
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        // Extract col and values from current freq view
        let (col, values) = app.view().and_then(|v| {
            let col = v.freq_col.clone()?;
            let rows: Vec<usize> = if v.selected_rows.is_empty() { vec![v.state.cr] }
                else { v.selected_rows.iter().copied().collect() };
            let vals: Vec<String> = rows.iter()
                .filter_map(|&r| v.dataframe.get_columns()[0].get(r).ok().map(|x| unquote(&x.to_string())))
                .collect();
            Some((col, vals))
        }).ok_or_else(|| anyhow!("Not a freq view"))?;
        // Delegate to FreqEnter
        FreqEnter { col, values }.exec(app)
    }

  src/plugin/freq.rs:119-129 method exec <-> src/plugin/freq.rs:184-202 function freq_agg_df
  Similarity: 90.82%

[36m--- src/plugin/freq.rs:exec (lines 119-129) ---[0m
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        let _ = CommandExecutor::exec(app, Box::new(Pop));
        if !self.values.is_empty() {
            let _ = CommandExecutor::exec(app, Box::new(FilterIn { col: self.col.clone(), values: self.values.clone() }));
            // Move cursor to filter column
            if let Some(v) = app.view_mut() {
                if let Some(i) = v.dataframe.get_column_names().iter().position(|c| c.as_str() == self.col) { v.state.cc = i; }
            }
        }
        Ok(())
    }

[36m--- src/plugin/freq.rs:freq_agg_df (lines 184-202) ---[0m
fn freq_agg_df(df: &DataFrame, grp: &str, _w: &str) -> Result<DataFrame> {
    // Build aggregate expressions: COUNT + MIN/MAX/SUM for numeric columns
    let mut agg_exprs: Vec<Expr> = vec![len().alias("Cnt")];
    for (name, dt) in df.schema().iter() {
        let n = name.to_string();
        if n == grp { continue; }
        if is_numeric(dt) {
            agg_exprs.push(col(&n).min().alias(&format!("{}_min", n)));
            agg_exprs.push(col(&n).max().alias(&format!("{}_max", n)));
            agg_exprs.push(col(&n).sum().alias(&format!("{}_sum", n)));
        }
    }
    df.clone().lazy()
        .group_by([col(grp)])
        .agg(agg_exprs)
        .sort(["Cnt"], SortMultipleOptions::default().with_order_descending(true))
        .collect()
        .map_err(|e| anyhow!("{}", e))
}

  src/plugin/freq.rs:25-42 method handle <-> src/plugin/freq.rs:119-129 method exec
  Similarity: 86.07%
  Classes: Plugin <-> Command

[36m--- src/plugin/freq.rs:handle (lines 25-42) ---[0m
    fn handle(&self, cmd: &str, app: &mut AppContext) -> Option<Box<dyn Command>> {
        match cmd {
            "enter" | "filter_parent" => {
                // Extract column name and selected values from freq view
                let info = app.view().and_then(|v| {
                    let col = v.freq_col.clone()?;
                    let rows: Vec<usize> = if v.selected_rows.is_empty() { vec![v.state.cr] }
                        else { v.selected_rows.iter().copied().collect() };
                    let vals: Vec<String> = rows.iter()
                        .filter_map(|&r| v.dataframe.get_columns()[0].get(r).ok().map(|x| unquote(&x.to_string())))
                        .collect();
                    Some((col, vals))
                });
                info.map(|(col, values)| Box::new(FreqEnter { col, values }) as Box<dyn Command>)
            }
            _ => None,
        }
    }

[36m--- src/plugin/freq.rs:exec (lines 119-129) ---[0m
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        let _ = CommandExecutor::exec(app, Box::new(Pop));
        if !self.values.is_empty() {
            let _ = CommandExecutor::exec(app, Box::new(FilterIn { col: self.col.clone(), values: self.values.clone() }));
            // Move cursor to filter column
            if let Some(v) = app.view_mut() {
                if let Some(i) = v.dataframe.get_column_names().iter().position(|c| c.as_str() == self.col) { v.state.cc = i; }
            }
        }
        Ok(())
    }

  src/plugin/freq.rs:119-129 method exec <-> src/plugin/freq.rs:138-151 method exec
  Similarity: 96.25%
  Classes: Command <-> Command

[36m--- src/plugin/freq.rs:exec (lines 119-129) ---[0m
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        let _ = CommandExecutor::exec(app, Box::new(Pop));
        if !self.values.is_empty() {
            let _ = CommandExecutor::exec(app, Box::new(FilterIn { col: self.col.clone(), values: self.values.clone() }));
            // Move cursor to filter column
            if let Some(v) = app.view_mut() {
                if let Some(i) = v.dataframe.get_column_names().iter().position(|c| c.as_str() == self.col) { v.state.cc = i; }
            }
        }
        Ok(())
    }

[36m--- src/plugin/freq.rs:exec (lines 138-151) ---[0m
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        // Extract col and values from current freq view
        let (col, values) = app.view().and_then(|v| {
            let col = v.freq_col.clone()?;
            let rows: Vec<usize> = if v.selected_rows.is_empty() { vec![v.state.cr] }
                else { v.selected_rows.iter().copied().collect() };
            let vals: Vec<String> = rows.iter()
                .filter_map(|&r| v.dataframe.get_columns()[0].get(r).ok().map(|x| unquote(&x.to_string())))
                .collect();
            Some((col, vals))
        }).ok_or_else(|| anyhow!("Not a freq view"))?;
        // Delegate to FreqEnter
        FreqEnter { col, values }.exec(app)
    }


Duplicates in src/plugin/meta.rs:
------------------------------------------------------------
  src/plugin/meta.rs:59-115 method exec <-> src/plugin/meta.rs:244-292 function grp_stats
  Similarity: 89.05%

[36m--- src/plugin/meta.rs:exec (lines 59-115) ---[0m
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        // Block meta while gz is still loading
        if app.is_loading() { return Err(anyhow!("Wait for loading to complete")); }
        let (parent_id, parent_col, parent_rows, parent_name, cached, df, col_sep, pq_path, col_names, schema) = {
            let view = app.req()?;
            let path = view.path().to_string();
            let cols = view.backend().cols(&path)?;
            let schema = view.backend().schema(&path)?;
            (view.id, view.state.cc, view.rows(), view.name.clone(),
             view.meta_cache.clone(), view.dataframe.clone(), view.col_separator, view.parquet_path.clone(), cols, schema)
        };
        let key_cols: Vec<String> = col_sep.map(|sep| col_names[..sep].to_vec()).unwrap_or_default();

        // Check cache (only for non-grouped)
        if key_cols.is_empty() {
            if let Some(cached_df) = cached {
                let id = app.next_id();
                let mut v = ViewState::new_child(id, "metadata".into(), cached_df, parent_id, parent_rows, parent_name);
                v.state.cr = parent_col;
                app.stack.push(v);
                return Ok(());
            }
        }

        // Grouped stats need in-memory df
        if !key_cols.is_empty() {
            let types: Vec<String> = df.dtypes().iter().map(|dt| format!("{:?}", dt)).collect();
            let placeholder = placeholder_df(col_names, types)?;
            let id = app.next_id();
            let mut v = ViewState::new_child(id, "metadata".into(), placeholder, parent_id, parent_rows, parent_name);
            v.state.cr = parent_col;
            v.col_separator = Some(key_cols.len());
            app.stack.push(v);

            let (tx, rx) = std::sync::mpsc::channel();
            std::thread::spawn(move || { if let Ok(r) = grp_stats(&df, &key_cols) { let _ = tx.send(r); } });
            app.bg_meta = Some((parent_id, rx));
            return Ok(());
        }

        // Non-grouped: unified LazyFrame path (parquet or in-memory)
        let types: Vec<String> = schema.iter().map(|(_, dt)| dt.clone()).collect();
        let placeholder = placeholder_df(col_names, types)?;
        let id = app.next_id();
        let mut v = ViewState::new_child(id, "metadata".into(), placeholder, parent_id, parent_rows, parent_name);
        v.state.cr = parent_col;
        app.stack.push(v);

        let (tx, rx) = std::sync::mpsc::channel();
        if let Some(path) = pq_path {
            std::thread::spawn(move || { if let Ok(r) = lf_stats_path(&path) { let _ = tx.send(r); } });
        } else {
            std::thread::spawn(move || { if let Ok(r) = lf_stats(&df) { let _ = tx.send(r); } });
        }
        app.bg_meta = Some((parent_id, rx));
        Ok(())
    }

[36m--- src/plugin/meta.rs:grp_stats (lines 244-292) ---[0m
fn grp_stats(df: &DataFrame, keys: &[String]) -> Result<DataFrame> {
    let all = df_cols(df);
    let non_keys: Vec<&String> = all.iter().filter(|c| !keys.contains(c)).collect();

    // Get unique key combinations
    let unique = df.clone().lazy()
        .select(keys.iter().map(|c| col(c)).collect::<Vec<_>>())
        .unique(None, UniqueKeepStrategy::First)
        .sort(keys.iter().map(|s| s.as_str()).collect::<Vec<_>>(), SortMultipleOptions::default())
        .collect()?;

    // Build key columns (repeated for each non-key column)
    let mut result: Vec<Column> = Vec::new();
    for k in keys {
        let mut vals = Vec::new();
        for r in 0..unique.height() {
            for _ in &non_keys { vals.push(unique.column(k).ok().and_then(|c| c.get(r).ok()).map(|v| fmt(&v)).unwrap_or_default()); }
        }
        result.push(ser!(k.as_str(), vals));
    }

    // Compute stats per group per column using SQL
    let (mut names, mut types, mut nulls, mut dists, mut mins, mut maxs, mut meds, mut sigs) =
        (vec![], vec![], vec![], vec![], vec![], vec![], vec![], vec![]);

    for r in 0..unique.height() {
        // Build filter for this key combo
        let filter = keys.iter().fold(lit(true), |acc, k| {
            let v = unique.column(k).unwrap().get(r).unwrap();
            acc.and(col(k).eq(lit(Scalar::new(unique.column(k).unwrap().dtype().clone(), v.into_static()))))
        });
        let grp = df.clone().lazy().filter(filter).collect()?;
        let n = grp.height() as f64;

        for &c in &non_keys {
            let dt = grp.column(c)?.dtype().clone();
            let s = col_stats(grp.clone().lazy(), c, n, is_numeric(&dt));
            names.push(c.clone()); types.push(format!("{:?}", dt));
            nulls.push(format!("{:.1}", s.nulls)); dists.push(format!("{}", s.distinct));
            mins.push(s.min); maxs.push(s.max); meds.push(s.mean); sigs.push(s.std);
        }
    }

    result.extend([
        ser!("column", names), ser!("type", types), ser!("null%", nulls), ser!("distinct", dists),
        ser!("min", mins), ser!("max", maxs), ser!("median", meds), ser!("sigma", sigs),
    ]);
    Ok(DataFrame::new(result)?)
}

  src/plugin/meta.rs:59-115 method exec <-> src/plugin/meta.rs:295-318 function lf_stats_path
  Similarity: 86.48%

[36m--- src/plugin/meta.rs:exec (lines 59-115) ---[0m
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        // Block meta while gz is still loading
        if app.is_loading() { return Err(anyhow!("Wait for loading to complete")); }
        let (parent_id, parent_col, parent_rows, parent_name, cached, df, col_sep, pq_path, col_names, schema) = {
            let view = app.req()?;
            let path = view.path().to_string();
            let cols = view.backend().cols(&path)?;
            let schema = view.backend().schema(&path)?;
            (view.id, view.state.cc, view.rows(), view.name.clone(),
             view.meta_cache.clone(), view.dataframe.clone(), view.col_separator, view.parquet_path.clone(), cols, schema)
        };
        let key_cols: Vec<String> = col_sep.map(|sep| col_names[..sep].to_vec()).unwrap_or_default();

        // Check cache (only for non-grouped)
        if key_cols.is_empty() {
            if let Some(cached_df) = cached {
                let id = app.next_id();
                let mut v = ViewState::new_child(id, "metadata".into(), cached_df, parent_id, parent_rows, parent_name);
                v.state.cr = parent_col;
                app.stack.push(v);
                return Ok(());
            }
        }

        // Grouped stats need in-memory df
        if !key_cols.is_empty() {
            let types: Vec<String> = df.dtypes().iter().map(|dt| format!("{:?}", dt)).collect();
            let placeholder = placeholder_df(col_names, types)?;
            let id = app.next_id();
            let mut v = ViewState::new_child(id, "metadata".into(), placeholder, parent_id, parent_rows, parent_name);
            v.state.cr = parent_col;
            v.col_separator = Some(key_cols.len());
            app.stack.push(v);

            let (tx, rx) = std::sync::mpsc::channel();
            std::thread::spawn(move || { if let Ok(r) = grp_stats(&df, &key_cols) { let _ = tx.send(r); } });
            app.bg_meta = Some((parent_id, rx));
            return Ok(());
        }

        // Non-grouped: unified LazyFrame path (parquet or in-memory)
        let types: Vec<String> = schema.iter().map(|(_, dt)| dt.clone()).collect();
        let placeholder = placeholder_df(col_names, types)?;
        let id = app.next_id();
        let mut v = ViewState::new_child(id, "metadata".into(), placeholder, parent_id, parent_rows, parent_name);
        v.state.cr = parent_col;
        app.stack.push(v);

        let (tx, rx) = std::sync::mpsc::channel();
        if let Some(path) = pq_path {
            std::thread::spawn(move || { if let Ok(r) = lf_stats_path(&path) { let _ = tx.send(r); } });
        } else {
            std::thread::spawn(move || { if let Ok(r) = lf_stats(&df) { let _ = tx.send(r); } });
        }
        app.bg_meta = Some((parent_id, rx));
        Ok(())
    }

[36m--- src/plugin/meta.rs:lf_stats_path (lines 295-318) ---[0m
fn lf_stats_path(path: &str) -> Result<DataFrame> {
    use crate::backend::{Backend, Polars};
    use polars::prelude::{ScanArgsParquet, PlPath};
    use std::io::Write;
    let t0 = std::time::Instant::now();
    let _ = std::fs::OpenOptions::new().create(true).append(true).open("/tmp/tv.debug.log")
        .and_then(|mut f| writeln!(f, "[lf_stats_path] START {}", path));

    let schema = Polars.schema(path)?;
    let (rows, _) = Polars.metadata(path)?;
    let cols: Vec<String> = schema.iter().map(|(name, _)| name.clone()).collect();
    let types: Vec<String> = schema.iter().map(|(_, dt)| dt.clone()).collect();
    let n = rows as f64;
    let (mut nulls, mut mins, mut maxs, mut dists, mut meds, mut sigs) = (vec![], vec![], vec![], vec![], vec![], vec![]);
    for (i, c) in cols.iter().enumerate() {
        let lf = LazyFrame::scan_parquet(PlPath::new(path), ScanArgsParquet::default())?;
        let s = col_stats(lf, c, n, is_numeric_str(&types[i]));
        nulls.push(format!("{:.1}", s.nulls)); dists.push(format!("{}", s.distinct));
        mins.push(s.min); maxs.push(s.max); meds.push(s.mean); sigs.push(s.std);
    }
    let _ = std::fs::OpenOptions::new().create(true).append(true).open("/tmp/tv.debug.log")
        .and_then(|mut f| writeln!(f, "[lf_stats_path] DONE {:.2}s {} cols", t0.elapsed().as_secs_f64(), cols.len()));
    stats_df(cols, types, nulls, dists, mins, maxs, meds, sigs)
}

  src/plugin/meta.rs:244-292 function grp_stats <-> src/plugin/meta.rs:295-318 function lf_stats_path
  Similarity: 85.45%

[36m--- src/plugin/meta.rs:grp_stats (lines 244-292) ---[0m
fn grp_stats(df: &DataFrame, keys: &[String]) -> Result<DataFrame> {
    let all = df_cols(df);
    let non_keys: Vec<&String> = all.iter().filter(|c| !keys.contains(c)).collect();

    // Get unique key combinations
    let unique = df.clone().lazy()
        .select(keys.iter().map(|c| col(c)).collect::<Vec<_>>())
        .unique(None, UniqueKeepStrategy::First)
        .sort(keys.iter().map(|s| s.as_str()).collect::<Vec<_>>(), SortMultipleOptions::default())
        .collect()?;

    // Build key columns (repeated for each non-key column)
    let mut result: Vec<Column> = Vec::new();
    for k in keys {
        let mut vals = Vec::new();
        for r in 0..unique.height() {
            for _ in &non_keys { vals.push(unique.column(k).ok().and_then(|c| c.get(r).ok()).map(|v| fmt(&v)).unwrap_or_default()); }
        }
        result.push(ser!(k.as_str(), vals));
    }

    // Compute stats per group per column using SQL
    let (mut names, mut types, mut nulls, mut dists, mut mins, mut maxs, mut meds, mut sigs) =
        (vec![], vec![], vec![], vec![], vec![], vec![], vec![], vec![]);

    for r in 0..unique.height() {
        // Build filter for this key combo
        let filter = keys.iter().fold(lit(true), |acc, k| {
            let v = unique.column(k).unwrap().get(r).unwrap();
            acc.and(col(k).eq(lit(Scalar::new(unique.column(k).unwrap().dtype().clone(), v.into_static()))))
        });
        let grp = df.clone().lazy().filter(filter).collect()?;
        let n = grp.height() as f64;

        for &c in &non_keys {
            let dt = grp.column(c)?.dtype().clone();
            let s = col_stats(grp.clone().lazy(), c, n, is_numeric(&dt));
            names.push(c.clone()); types.push(format!("{:?}", dt));
            nulls.push(format!("{:.1}", s.nulls)); dists.push(format!("{}", s.distinct));
            mins.push(s.min); maxs.push(s.max); meds.push(s.mean); sigs.push(s.std);
        }
    }

    result.extend([
        ser!("column", names), ser!("type", types), ser!("null%", nulls), ser!("distinct", dists),
        ser!("min", mins), ser!("max", maxs), ser!("median", meds), ser!("sigma", sigs),
    ]);
    Ok(DataFrame::new(result)?)
}

[36m--- src/plugin/meta.rs:lf_stats_path (lines 295-318) ---[0m
fn lf_stats_path(path: &str) -> Result<DataFrame> {
    use crate::backend::{Backend, Polars};
    use polars::prelude::{ScanArgsParquet, PlPath};
    use std::io::Write;
    let t0 = std::time::Instant::now();
    let _ = std::fs::OpenOptions::new().create(true).append(true).open("/tmp/tv.debug.log")
        .and_then(|mut f| writeln!(f, "[lf_stats_path] START {}", path));

    let schema = Polars.schema(path)?;
    let (rows, _) = Polars.metadata(path)?;
    let cols: Vec<String> = schema.iter().map(|(name, _)| name.clone()).collect();
    let types: Vec<String> = schema.iter().map(|(_, dt)| dt.clone()).collect();
    let n = rows as f64;
    let (mut nulls, mut mins, mut maxs, mut dists, mut meds, mut sigs) = (vec![], vec![], vec![], vec![], vec![], vec![]);
    for (i, c) in cols.iter().enumerate() {
        let lf = LazyFrame::scan_parquet(PlPath::new(path), ScanArgsParquet::default())?;
        let s = col_stats(lf, c, n, is_numeric_str(&types[i]));
        nulls.push(format!("{:.1}", s.nulls)); dists.push(format!("{}", s.distinct));
        mins.push(s.min); maxs.push(s.max); meds.push(s.mean); sigs.push(s.std);
    }
    let _ = std::fs::OpenOptions::new().create(true).append(true).open("/tmp/tv.debug.log")
        .and_then(|mut f| writeln!(f, "[lf_stats_path] DONE {:.2}s {} cols", t0.elapsed().as_secs_f64(), cols.len()));
    stats_df(cols, types, nulls, dists, mins, maxs, meds, sigs)
}

  src/plugin/meta.rs:146-170 method exec <-> src/plugin/meta.rs:199-219 function col_stats
  Similarity: 87.34%

[36m--- src/plugin/meta.rs:exec (lines 146-170) ---[0m
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        let n = self.col_names.len();
        if let Some(pid) = app.view().and_then(|v| v.parent_id) {
            if let Some(parent) = app.stack.find_mut(pid) {
                // Adjust col_separator if deleting key columns
                if let Some(sep) = parent.col_separator {
                    let cols = if parent.col_names.is_empty() { df_cols(&parent.dataframe) } else { parent.col_names.clone() };
                    let adj = self.col_names.iter().filter(|c| cols.iter().position(|x| x == *c).map(|i| i < sep).unwrap_or(false)).count();
                    parent.col_separator = Some(sep.saturating_sub(adj));
                }
                // Init col_names from df if empty, then remove deleted columns
                if parent.col_names.is_empty() { parent.col_names = df_cols(&parent.dataframe); }
                parent.col_names.retain(|c| !self.col_names.contains(c));
                // Clear cache to force re-fetch with new column list
                parent.fetch_cache = None;
                // For in-memory views, also drop from dataframe
                if parent.parquet_path.is_none() {
                    for c in &self.col_names { let _ = parent.dataframe.drop_in_place(c); }
                }
            }
        }
        let _ = CommandExecutor::exec(app, Box::new(Pop));
        app.msg(format!("{} columns deleted", n));
        Ok(())
    }

[36m--- src/plugin/meta.rs:col_stats (lines 199-219) ---[0m
fn col_stats(lf: LazyFrame, col: &str, n: f64, is_num: bool) -> ColStats {
    // SQL query - skip AVG/STDDEV for non-numeric columns (causes SQL error)
    let q = if is_num {
        format!(r#"SELECT COUNT(*) - COUNT("{}") as nulls, COUNT(DISTINCT "{}") as dist,
            MIN("{}") as min, MAX("{}") as max, AVG("{}") as mean, STDDEV("{}") as std FROM df"#,
            col, col, col, col, col, col)
    } else {
        format!(r#"SELECT COUNT(*) - COUNT("{}") as nulls, COUNT(DISTINCT "{}") as dist,
            MIN("{}") as min, MAX("{}") as max FROM df"#, col, col, col, col)
    };
    let df = sql(lf, &q).ok();
    let get = |c: &str| df.as_ref().and_then(|d| d.column(c).ok()?.get(0).ok()).map(|v| fmt(&v)).unwrap_or_default();
    let nulls = df.as_ref().and_then(|d| d.column("nulls").ok()?.get(0).ok()?.try_extract::<u32>().ok()).unwrap_or(0) as f64;
    let distinct = df.as_ref().and_then(|d| d.column("dist").ok()?.get(0).ok()?.try_extract::<u32>().ok()).unwrap_or(0);
    ColStats {
        nulls: 100.0 * nulls / n, distinct,
        min: get("min"), max: get("max"),
        mean: if is_num { get("mean") } else { String::new() },
        std: if is_num { get("std") } else { String::new() },
    }
}

  src/plugin/meta.rs:123-137 method exec <-> src/plugin/meta.rs:146-170 method exec
  Similarity: 90.97%
  Classes: Command <-> Command

[36m--- src/plugin/meta.rs:exec (lines 123-137) ---[0m
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        let _ = CommandExecutor::exec(app, Box::new(Pop));  // pop meta view
        if self.col_names.len() == 1 {  // single col: move cursor
            if let Some(v) = app.view_mut() {
                // Use col_names for parquet, dataframe for memory
                let cols = if v.col_names.is_empty() { df_cols(&v.dataframe) } else { v.col_names.clone() };
                if let Some(idx) = cols.iter().position(|c| c == &self.col_names[0]) {
                    v.state.cc = idx;
                }
            }
        } else {
            let _ = CommandExecutor::exec(app, Box::new(Xkey { col_names: self.col_names.clone() }));
        }
        Ok(())
    }

[36m--- src/plugin/meta.rs:exec (lines 146-170) ---[0m
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        let n = self.col_names.len();
        if let Some(pid) = app.view().and_then(|v| v.parent_id) {
            if let Some(parent) = app.stack.find_mut(pid) {
                // Adjust col_separator if deleting key columns
                if let Some(sep) = parent.col_separator {
                    let cols = if parent.col_names.is_empty() { df_cols(&parent.dataframe) } else { parent.col_names.clone() };
                    let adj = self.col_names.iter().filter(|c| cols.iter().position(|x| x == *c).map(|i| i < sep).unwrap_or(false)).count();
                    parent.col_separator = Some(sep.saturating_sub(adj));
                }
                // Init col_names from df if empty, then remove deleted columns
                if parent.col_names.is_empty() { parent.col_names = df_cols(&parent.dataframe); }
                parent.col_names.retain(|c| !self.col_names.contains(c));
                // Clear cache to force re-fetch with new column list
                parent.fetch_cache = None;
                // For in-memory views, also drop from dataframe
                if parent.parquet_path.is_none() {
                    for c in &self.col_names { let _ = parent.dataframe.drop_in_place(c); }
                }
            }
        }
        let _ = CommandExecutor::exec(app, Box::new(Pop));
        app.msg(format!("{} columns deleted", n));
        Ok(())
    }

  src/plugin/meta.rs:199-219 function col_stats <-> src/plugin/meta.rs:228-240 function lf_stats
  Similarity: 87.51%

[36m--- src/plugin/meta.rs:col_stats (lines 199-219) ---[0m
fn col_stats(lf: LazyFrame, col: &str, n: f64, is_num: bool) -> ColStats {
    // SQL query - skip AVG/STDDEV for non-numeric columns (causes SQL error)
    let q = if is_num {
        format!(r#"SELECT COUNT(*) - COUNT("{}") as nulls, COUNT(DISTINCT "{}") as dist,
            MIN("{}") as min, MAX("{}") as max, AVG("{}") as mean, STDDEV("{}") as std FROM df"#,
            col, col, col, col, col, col)
    } else {
        format!(r#"SELECT COUNT(*) - COUNT("{}") as nulls, COUNT(DISTINCT "{}") as dist,
            MIN("{}") as min, MAX("{}") as max FROM df"#, col, col, col, col)
    };
    let df = sql(lf, &q).ok();
    let get = |c: &str| df.as_ref().and_then(|d| d.column(c).ok()?.get(0).ok()).map(|v| fmt(&v)).unwrap_or_default();
    let nulls = df.as_ref().and_then(|d| d.column("nulls").ok()?.get(0).ok()?.try_extract::<u32>().ok()).unwrap_or(0) as f64;
    let distinct = df.as_ref().and_then(|d| d.column("dist").ok()?.get(0).ok()?.try_extract::<u32>().ok()).unwrap_or(0);
    ColStats {
        nulls: 100.0 * nulls / n, distinct,
        min: get("min"), max: get("max"),
        mean: if is_num { get("mean") } else { String::new() },
        std: if is_num { get("std") } else { String::new() },
    }
}

[36m--- src/plugin/meta.rs:lf_stats (lines 228-240) ---[0m
fn lf_stats(df: &DataFrame) -> Result<DataFrame> {
    let cols = df_cols(df);
    let dtypes = df.dtypes();
    let types: Vec<String> = dtypes.iter().map(|dt| format!("{:?}", dt)).collect();
    let n = df.height() as f64;
    let (mut nulls, mut mins, mut maxs, mut dists, mut meds, mut sigs) = (vec![], vec![], vec![], vec![], vec![], vec![]);
    for (i, c) in cols.iter().enumerate() {
        let s = col_stats(df.clone().lazy(), c, n, is_numeric(&dtypes[i]));
        nulls.push(format!("{:.1}", s.nulls)); dists.push(format!("{}", s.distinct));
        mins.push(s.min); maxs.push(s.max); meds.push(s.mean); sigs.push(s.std);
    }
    stats_df(cols, types, nulls, dists, mins, maxs, meds, sigs)
}

  src/plugin/meta.rs:180-186 function stats_df <-> src/plugin/meta.rs:321-328 function fmt
  Similarity: 96.90%

[36m--- src/plugin/meta.rs:stats_df (lines 180-186) ---[0m
fn stats_df(cols: Vec<String>, types: Vec<String>, nulls: Vec<String>, dists: Vec<String>,
            mins: Vec<String>, maxs: Vec<String>, meds: Vec<String>, sigs: Vec<String>) -> Result<DataFrame> {
    Ok(DataFrame::new(vec![
        ser!("column", cols), ser!("type", types), ser!("null%", nulls), ser!("distinct", dists),
        ser!("min", mins), ser!("max", maxs), ser!("median", meds), ser!("sigma", sigs),
    ])?)
}

[36m--- src/plugin/meta.rs:fmt (lines 321-328) ---[0m
fn fmt(v: &AnyValue) -> String {
    match v {
        AnyValue::Null => String::new(),
        AnyValue::Float64(f) => format!("{:.2}", f),
        AnyValue::Float32(f) => format!("{:.2}", f),
        _ => { let s = v.to_string(); if s == "null" { String::new() } else { unquote(&s) } }
    }
}


Duplicates in src/command/nav.rs:
------------------------------------------------------------
  src/command/nav.rs:66-80 method exec <-> src/command/nav.rs:102-114 method exec
  Similarity: 98.42%
  Classes: Command <-> Command

[36m--- src/command/nav.rs:exec (lines 66-80) ---[0m
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        let msg = if let Some(v) = app.view_mut() {
            if v.is_row_sel() {
                let cr = v.state.cr;
                if v.selected_rows.contains(&cr) { v.selected_rows.remove(&cr); } else { v.selected_rows.insert(cr); }
                format!("{} row(s) selected", v.selected_rows.len())
            } else {
                let cc = v.state.cc;
                if v.selected_cols.contains(&cc) { v.selected_cols.remove(&cc); } else { v.selected_cols.insert(cc); }
                format!("{} column(s) selected", v.selected_cols.len())
            }
        } else { "No view".into() };
        app.msg(msg);
        Ok(())
    }

[36m--- src/command/nav.rs:exec (lines 102-114) ---[0m
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        let msg = if let Some(v) = app.view_mut() {
            if v.is_row_sel() {
                for i in 0..v.rows() { v.selected_rows.insert(i); }
                format!("Selected all {} row(s)", v.selected_rows.len())
            } else {
                for i in 0..v.cols() { v.selected_cols.insert(i); }
                format!("Selected all {} column(s)", v.selected_cols.len())
            }
        } else { "No view".into() };
        app.msg(msg);
        Ok(())
    }

  src/command/nav.rs:49-58 method exec <-> src/command/nav.rs:66-80 method exec
  Similarity: 86.28%
  Classes: Command <-> Command

[36m--- src/command/nav.rs:exec (lines 49-58) ---[0m
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        if self.delta > 0 {
            app.float_decimals = (app.float_decimals + self.delta as usize).min(17);
        } else {
            app.float_decimals = app.float_decimals.saturating_sub((-self.delta) as usize);
        }
        if let Some(v) = app.view_mut() { v.state.col_widths.clear(); }
        app.msg(format!("Float decimals: {}", app.float_decimals));
        Ok(())
    }

[36m--- src/command/nav.rs:exec (lines 66-80) ---[0m
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        let msg = if let Some(v) = app.view_mut() {
            if v.is_row_sel() {
                let cr = v.state.cr;
                if v.selected_rows.contains(&cr) { v.selected_rows.remove(&cr); } else { v.selected_rows.insert(cr); }
                format!("{} row(s) selected", v.selected_rows.len())
            } else {
                let cc = v.state.cc;
                if v.selected_cols.contains(&cc) { v.selected_cols.remove(&cc); } else { v.selected_cols.insert(cc); }
                format!("{} column(s) selected", v.selected_cols.len())
            }
        } else { "No view".into() };
        app.msg(msg);
        Ok(())
    }

  src/command/nav.rs:10-16 method exec <-> src/command/nav.rs:102-114 method exec
  Similarity: 88.16%
  Classes: Command <-> Command

[36m--- src/command/nav.rs:exec (lines 10-16) ---[0m
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        let a = self.arg.trim();
        // Parse: 0->-BIG(top), max->BIG(bot), +n/-n as-is
        let n = match a { "0" => -BIG, "max" => BIG, _ => a.trim_start_matches('+').parse().unwrap_or(0) };
        app.nav_row(n);
        Ok(())
    }

[36m--- src/command/nav.rs:exec (lines 102-114) ---[0m
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        let msg = if let Some(v) = app.view_mut() {
            if v.is_row_sel() {
                for i in 0..v.rows() { v.selected_rows.insert(i); }
                format!("Selected all {} row(s)", v.selected_rows.len())
            } else {
                for i in 0..v.cols() { v.selected_cols.insert(i); }
                format!("Selected all {} column(s)", v.selected_cols.len())
            }
        } else { "No view".into() };
        app.msg(msg);
        Ok(())
    }

  src/command/nav.rs:24-30 method exec <-> src/command/nav.rs:102-114 method exec
  Similarity: 88.16%
  Classes: Command <-> Command

[36m--- src/command/nav.rs:exec (lines 24-30) ---[0m
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        let a = self.arg.trim();
        // Parse: 0->-BIG(left), max->BIG(right), +n/-n as-is
        let n = match a { "0" => -BIG, "max" => BIG, _ => a.trim_start_matches('+').parse().unwrap_or(0) };
        app.nav_col(n);
        Ok(())
    }

[36m--- src/command/nav.rs:exec (lines 102-114) ---[0m
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        let msg = if let Some(v) = app.view_mut() {
            if v.is_row_sel() {
                for i in 0..v.rows() { v.selected_rows.insert(i); }
                format!("Selected all {} row(s)", v.selected_rows.len())
            } else {
                for i in 0..v.cols() { v.selected_cols.insert(i); }
                format!("Selected all {} column(s)", v.selected_cols.len())
            }
        } else { "No view".into() };
        app.msg(msg);
        Ok(())
    }

  src/command/nav.rs:10-16 method exec <-> src/command/nav.rs:24-30 method exec
  Similarity: 99.69%
  Classes: Command <-> Command

[36m--- src/command/nav.rs:exec (lines 10-16) ---[0m
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        let a = self.arg.trim();
        // Parse: 0->-BIG(top), max->BIG(bot), +n/-n as-is
        let n = match a { "0" => -BIG, "max" => BIG, _ => a.trim_start_matches('+').parse().unwrap_or(0) };
        app.nav_row(n);
        Ok(())
    }

[36m--- src/command/nav.rs:exec (lines 24-30) ---[0m
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        let a = self.arg.trim();
        // Parse: 0->-BIG(left), max->BIG(right), +n/-n as-is
        let n = match a { "0" => -BIG, "max" => BIG, _ => a.trim_start_matches('+').parse().unwrap_or(0) };
        app.nav_col(n);
        Ok(())
    }


Duplicates in src/command/transform.rs:
------------------------------------------------------------
  src/command/transform.rs:145-172 method exec <-> src/command/transform.rs:219-235 method exec
  Similarity: 85.25%
  Classes: Command <-> Command

[36m--- src/command/transform.rs:exec (lines 145-172) ---[0m
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        if app.is_loading() { return Err(anyhow!("Wait for loading to complete")); }
        let id = app.next_id();
        let v = app.req()?;
        let path = v.path().to_string();
        let schema = v.backend().schema(&path)?;
        let is_str = schema.iter().find(|(n, _)| n == &self.col)
            .map(|(_, t)| t.contains("String") || t.contains("Utf8")).unwrap_or(true);
        let vals = self.values.iter().map(|v| if is_str { format!("'{}'", v) } else { v.clone() }).collect::<Vec<_>>().join(",");
        let new_clause = format!("\"{}\" IN ({})", self.col, vals);
        let name = if self.values.len() == 1 { format!("{}={}", self.col, self.values[0]) }
                   else { format!("{}∈{{{}}}", self.col, self.values.len()) };
        // Lazy filtered view for parquet: combine with existing filter
        if v.parquet_path.is_some() {
            let combined = match &v.filter_clause {
                Some(prev) => format!("({}) AND ({})", prev, new_clause),
                None => new_clause,
            };
            let count = v.backend().count_where(&path, &combined)?;
            let cols = v.col_names.clone();
            app.stack.push(crate::state::ViewState::new_filtered(id, name, path, cols, combined, count));
        } else {
            let filtered = v.backend().filter(&path, &new_clause, FILTER_LIMIT)?;
            let filename = v.filename.clone();
            app.stack.push(crate::state::ViewState::new(id, name, filtered, filename));
        }
        Ok(())
    }

[36m--- src/command/transform.rs:exec (lines 219-235) ---[0m
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        use crate::command::io::convert::{is_taq_time, taq_to_ns};
        let v = app.req_mut()?;
        let c = v.dataframe.column(&self.col_name)?;
        if !c.dtype().is_integer() { return Err(anyhow!("Column must be integer type")); }
        let i64_s = c.cast(&DataType::Int64)?;
        let i64_ca = i64_s.i64()?;
        // Check first non-null value is TAQ format
        let first = i64_ca.into_iter().flatten().next().ok_or_else(|| anyhow!("Column is empty"))?;
        if !is_taq_time(first) { return Err(anyhow!("Value {} doesn't look like TAQ time (HHMMSSNNNNNNNN)", first)); }
        // Convert to nanoseconds since midnight, then to Time
        let ns: Vec<Option<i64>> = i64_ca.into_iter().map(|v| v.map(taq_to_ns)).collect();
        let time_s = Series::new(self.col_name.as_str().into(), ns).cast(&DataType::Time)?;
        v.dataframe.replace(&self.col_name, time_s)?;
        v.state.col_widths.clear();
        Ok(())
    }

  src/command/transform.rs:34-56 method exec <-> src/command/transform.rs:118-136 method exec
  Similarity: 85.32%
  Classes: Command <-> Command

[36m--- src/command/transform.rs:exec (lines 34-56) ---[0m
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        if app.is_loading() { return Err(anyhow!("Wait for loading to complete")); }
        let id = app.next_id();
        let v = app.req()?;
        let path = v.path().to_string();
        // Lazy filtered view for parquet: keep parquet_path + filter_clause
        if v.parquet_path.is_some() {
            // Chain filters with AND
            let combined = match &v.filter_clause {
                Some(prev) => format!("({}) AND ({})", prev, self.expr),
                None => self.expr.clone(),
            };
            let count = v.backend().count_where(&path, &combined)?;
            let cols = v.col_names.clone();
            let name = format!("{} & {}", v.name, self.expr);
            app.stack.push(crate::state::ViewState::new_filtered(id, name, path, cols, combined, count));
        } else {
            let filtered = v.backend().filter(&path, &self.expr, FILTER_LIMIT)?;
            let filename = v.filename.clone();
            app.stack.push(crate::state::ViewState::new(id, self.expr.clone(), filtered, filename));
        }
        Ok(())
    }

[36m--- src/command/transform.rs:exec (lines 118-136) ---[0m
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        let (agg_df, filename) = {
            let v = app.req()?;
            let grouped = v.dataframe.clone().lazy().group_by([col(&self.col)]);
            let result = match self.func.as_str() {
                "count" => grouped.agg([col("*").count().alias("count")]),
                "sum" => grouped.agg([col("*").sum()]),
                "mean" => grouped.agg([col("*").mean()]),
                "min" => grouped.agg([col("*").min()]),
                "max" => grouped.agg([col("*").max()]),
                "std" => grouped.agg([col("*").std(1)]),
                _ => return Err(anyhow::anyhow!("Unknown aggregation: {}", self.func)),
            };
            (result.collect()?, v.filename.clone())
        };
        let id = app.next_id();
        app.stack.push(crate::state::ViewState::new(id, format!("{}:{}", self.func, self.col), agg_df, filename));
        Ok(())
    }

  src/command/transform.rs:34-56 method exec <-> src/command/transform.rs:243-257 method exec
  Similarity: 90.27%
  Classes: Command <-> Command

[36m--- src/command/transform.rs:exec (lines 34-56) ---[0m
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        if app.is_loading() { return Err(anyhow!("Wait for loading to complete")); }
        let id = app.next_id();
        let v = app.req()?;
        let path = v.path().to_string();
        // Lazy filtered view for parquet: keep parquet_path + filter_clause
        if v.parquet_path.is_some() {
            // Chain filters with AND
            let combined = match &v.filter_clause {
                Some(prev) => format!("({}) AND ({})", prev, self.expr),
                None => self.expr.clone(),
            };
            let count = v.backend().count_where(&path, &combined)?;
            let cols = v.col_names.clone();
            let name = format!("{} & {}", v.name, self.expr);
            app.stack.push(crate::state::ViewState::new_filtered(id, name, path, cols, combined, count));
        } else {
            let filtered = v.backend().filter(&path, &self.expr, FILTER_LIMIT)?;
            let filename = v.filename.clone();
            app.stack.push(crate::state::ViewState::new(id, self.expr.clone(), filtered, filename));
        }
        Ok(())
    }

[36m--- src/command/transform.rs:exec (lines 243-257) ---[0m
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        let v = app.req_mut()?;
        let dt = match self.dtype.as_str() {
            "String" => DataType::String,
            "Int64" => DataType::Int64,
            "Float64" => DataType::Float64,
            "Boolean" => DataType::Boolean,
            _ => return Err(anyhow!("Unknown type: {}", self.dtype)),
        };
        let c = v.dataframe.column(&self.col_name)?;
        let new_col = c.cast(&dt)?;
        v.dataframe.with_column(new_col)?;
        v.state.col_widths.clear();
        Ok(())
    }

  src/command/transform.rs:11-26 method exec <-> src/command/transform.rs:118-136 method exec
  Similarity: 94.89%
  Classes: Command <-> Command

[36m--- src/command/transform.rs:exec (lines 11-26) ---[0m
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        let n = self.col_names.len();
        {
            let v = app.req_mut()?;
            // Count how many deleted cols are before separator
            let sep_adjust = if let Some(sep) = v.col_separator {
                let all = df_cols(&v.dataframe);
                self.col_names.iter().filter(|c| all.iter().position(|n| n == *c).map(|i| i < sep).unwrap_or(false)).count()
            } else { 0 };
            for c in &self.col_names { v.dataframe = v.dataframe.drop(c)?; }
            if let Some(sep) = v.col_separator { v.col_separator = Some(sep.saturating_sub(sep_adjust)); }
            if v.state.cc >= v.cols() && v.cols() > 0 { v.state.cc = v.cols() - 1; }
        }
        app.msg(format!("{} columns deleted", n));
        Ok(())
    }

[36m--- src/command/transform.rs:exec (lines 118-136) ---[0m
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        let (agg_df, filename) = {
            let v = app.req()?;
            let grouped = v.dataframe.clone().lazy().group_by([col(&self.col)]);
            let result = match self.func.as_str() {
                "count" => grouped.agg([col("*").count().alias("count")]),
                "sum" => grouped.agg([col("*").sum()]),
                "mean" => grouped.agg([col("*").mean()]),
                "min" => grouped.agg([col("*").min()]),
                "max" => grouped.agg([col("*").max()]),
                "std" => grouped.agg([col("*").std(1)]),
                _ => return Err(anyhow::anyhow!("Unknown aggregation: {}", self.func)),
            };
            (result.collect()?, v.filename.clone())
        };
        let id = app.next_id();
        app.stack.push(crate::state::ViewState::new(id, format!("{}:{}", self.func, self.col), agg_df, filename));
        Ok(())
    }

  src/command/transform.rs:243-257 method exec <-> src/command/transform.rs:265-273 method exec
  Similarity: 93.83%
  Classes: Command <-> Command

[36m--- src/command/transform.rs:exec (lines 243-257) ---[0m
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        let v = app.req_mut()?;
        let dt = match self.dtype.as_str() {
            "String" => DataType::String,
            "Int64" => DataType::Int64,
            "Float64" => DataType::Float64,
            "Boolean" => DataType::Boolean,
            _ => return Err(anyhow!("Unknown type: {}", self.dtype)),
        };
        let c = v.dataframe.column(&self.col_name)?;
        let new_col = c.cast(&dt)?;
        v.dataframe.with_column(new_col)?;
        v.state.col_widths.clear();
        Ok(())
    }

[36m--- src/command/transform.rs:exec (lines 265-273) ---[0m
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        let v = app.req_mut()?;
        let c = v.dataframe.column(&self.col_name)?.clone();
        let new_name = format!("{}_copy", self.col_name);
        let new_col = c.as_materialized_series().clone().with_name(new_name.into());
        v.dataframe.with_column(new_col)?;
        v.state.col_widths.clear();
        Ok(())
    }


Duplicates in src/plugin/corr.rs:
------------------------------------------------------------
  src/plugin/corr.rs:19-27 method handle <-> src/plugin/corr.rs:29-36 method parse
  Similarity: 96.36%
  Classes: Plugin <-> Plugin

[36m--- src/plugin/corr.rs:handle (lines 19-27) ---[0m
    fn handle(&self, cmd: &str, app: &mut AppContext) -> Option<Box<dyn Command>> {
        if cmd != "enter" { return None; }
        // Get column name from current row (first column is row label)
        let col_name = app.view().and_then(|v| {
            v.dataframe.column("column").ok()?.get(v.state.cr).ok()
                .map(|v| unquote(&v.to_string()))
        })?;
        Some(Box::new(CorrEnter { col_name }))
    }

[36m--- src/plugin/corr.rs:parse (lines 29-36) ---[0m
    fn parse(&self, cmd: &str, arg: &str) -> Option<Box<dyn Command>> {
        if cmd != "corr" { return None; }
        // Parse selected columns (comma-separated indices or empty for all)
        let selected_cols = if arg.is_empty() { vec![] } else {
            arg.split(',').filter_map(|s| s.trim().parse().ok()).collect()
        };
        Some(Box::new(Correlation { selected_cols }))
    }


Duplicates in src/app.rs:
------------------------------------------------------------
  src/app.rs:126-147 method check_bg_meta <-> src/app.rs:150-167 method check_bg_freq
  Similarity: 96.18%
  Classes: AppContext <-> AppContext

[36m--- src/app.rs:check_bg_meta (lines 126-147) ---[0m
    pub fn check_bg_meta(&mut self) {
        use std::sync::mpsc::TryRecvError;
        let Some((parent_id, ref rx)) = self.bg_meta else { return };
        match rx.try_recv() {
            Ok(meta_df) => {
                // Update current meta view if it's the one we're computing for
                if let Some(view) = self.stack.cur_mut() {
                    if view.name == "metadata" && view.parent_id == Some(parent_id) {
                        view.dataframe = meta_df.clone();
                        view.state.col_widths.clear();
                    }
                }
                // Cache in parent
                if let Some(parent) = self.stack.find_mut(parent_id) {
                    parent.meta_cache = Some(meta_df);
                }
                self.bg_meta = None;
            }
            Err(TryRecvError::Empty) => {}
            Err(TryRecvError::Disconnected) => { self.bg_meta = None; }
        }
    }

[36m--- src/app.rs:check_bg_freq (lines 150-167) ---[0m
    pub fn check_bg_freq(&mut self) {
        use std::sync::mpsc::TryRecvError;
        let Some((freq_id, ref rx)) = self.bg_freq else { return };
        match rx.try_recv() {
            Ok(freq_df) => {
                // Update freq view if it's the one we're computing for
                if let Some(view) = self.stack.cur_mut() {
                    if view.id == freq_id {
                        view.dataframe = freq_df;
                        view.state.col_widths.clear();
                    }
                }
                self.bg_freq = None;
            }
            Err(TryRecvError::Empty) => {}
            Err(TryRecvError::Disconnected) => { self.bg_freq = None; }
        }
    }

  src/app.rs:113-123 method check_bg_saver <-> src/app.rs:150-167 method check_bg_freq
  Similarity: 86.49%
  Classes: AppContext <-> AppContext

[36m--- src/app.rs:check_bg_saver (lines 113-123) ---[0m
    pub fn check_bg_saver(&mut self) {
        use std::sync::mpsc::TryRecvError;
        let Some(rx) = &self.bg_saver else { return };
        loop {
            match rx.try_recv() {
                Ok(msg) => self.message = msg,
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => { self.bg_saver = None; break; }
            }
        }
    }

[36m--- src/app.rs:check_bg_freq (lines 150-167) ---[0m
    pub fn check_bg_freq(&mut self) {
        use std::sync::mpsc::TryRecvError;
        let Some((freq_id, ref rx)) = self.bg_freq else { return };
        match rx.try_recv() {
            Ok(freq_df) => {
                // Update freq view if it's the one we're computing for
                if let Some(view) = self.stack.cur_mut() {
                    if view.id == freq_id {
                        view.dataframe = freq_df;
                        view.state.col_widths.clear();
                    }
                }
                self.bg_freq = None;
            }
            Err(TryRecvError::Empty) => {}
            Err(TryRecvError::Disconnected) => { self.bg_freq = None; }
        }
    }

  src/app.rs:201-206 method nav_row <-> src/app.rs:209-215 method nav_col
  Similarity: 99.02%
  Classes: AppContext <-> AppContext

[36m--- src/app.rs:nav_row (lines 201-206) ---[0m
    pub fn nav_row(&mut self, d: isize) {
        if let Some(v) = self.view_mut() {
            let n = v.rows();
            if d < 0 { v.state.up((-d) as usize); } else { v.state.down(d as usize, n); }
        }
    }

[36m--- src/app.rs:nav_col (lines 209-215) ---[0m
    pub fn nav_col(&mut self, d: isize) {
        if let Some(v) = self.view_mut() {
            let n = v.cols();
            if d < 0 { v.state.left((-d) as usize); }
            else { v.state.right(d as usize, n); }
        }
    }

  src/app.rs:209-215 method nav_col <-> src/app.rs:218-220 method page
  Similarity: 91.74%
  Classes: AppContext <-> AppContext

[36m--- src/app.rs:nav_col (lines 209-215) ---[0m
    pub fn nav_col(&mut self, d: isize) {
        if let Some(v) = self.view_mut() {
            let n = v.cols();
            if d < 0 { v.state.left((-d) as usize); }
            else { v.state.right(d as usize, n); }
        }
    }

[36m--- src/app.rs:page (lines 218-220) ---[0m
    pub fn page(&self) -> isize {
        self.view().map(|v| (v.state.viewport.0 as isize).saturating_sub(2)).unwrap_or(10)
    }

  src/app.rs:201-206 method nav_row <-> src/app.rs:218-220 method page
  Similarity: 91.74%
  Classes: AppContext <-> AppContext

[36m--- src/app.rs:nav_row (lines 201-206) ---[0m
    pub fn nav_row(&mut self, d: isize) {
        if let Some(v) = self.view_mut() {
            let n = v.rows();
            if d < 0 { v.state.up((-d) as usize); } else { v.state.down(d as usize, n); }
        }
    }

[36m--- src/app.rs:page (lines 218-220) ---[0m
    pub fn page(&self) -> isize {
        self.view().map(|v| (v.state.viewport.0 as isize).saturating_sub(2)).unwrap_or(10)
    }

  src/app.rs:196-198 method viewport <-> src/app.rs:218-220 method page
  Similarity: 85.66%
  Classes: AppContext <-> AppContext

[36m--- src/app.rs:viewport (lines 196-198) ---[0m
    pub fn viewport(&mut self, rows: u16, cols: u16) {
        if let Some(v) = self.stack.cur_mut() { v.state.viewport = (rows, cols); }
    }

[36m--- src/app.rs:page (lines 218-220) ---[0m
    pub fn page(&self) -> isize {
        self.view().map(|v| (v.state.viewport.0 as isize).saturating_sub(2)).unwrap_or(10)
    }


Duplicates in src/plugin/pivot.rs:
------------------------------------------------------------
  src/plugin/pivot.rs:42-78 method exec <-> src/plugin/pivot.rs:97-124 method exec
  Similarity: 88.82%
  Classes: Command <-> Command

[36m--- src/plugin/pivot.rs:exec (lines 42-78) ---[0m
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        let (cols, keys, df, parent_id, parent_name) = {
            let v = app.req()?;
            let cols = df_cols(&v.dataframe);
            let keys: Vec<String> = v.col_separator.map(|sep| cols[..sep].to_vec()).unwrap_or_default();
            if keys.is_empty() { return Err(anyhow!("Set xkey columns first (!)")); }
            (cols, keys, v.dataframe.clone(), v.id, v.name.clone())
        };

        // Available columns for pivot (exclude key columns)
        let available: Vec<String> = cols.iter().filter(|c| !keys.contains(c)).cloned().collect();
        if available.len() < 2 { return Err(anyhow!("Need at least 2 non-key columns")); }

        // Pick pivot column (values become headers)
        let pivot_col = picker::fzf(available.clone(), "Pivot column: ")?.ok_or_else(|| anyhow!("No pivot column selected"))?;

        // Pick value column (to aggregate)
        let value_opts: Vec<String> = available.iter().filter(|c| *c != &pivot_col).cloned().collect();
        let value_col = picker::fzf(value_opts, "Value column: ")?.ok_or_else(|| anyhow!("No value column selected"))?;

        // Create placeholder and run in background
        let placeholder = placeholder_pivot(&keys, &pivot_col)?;
        let id = app.next_id();
        let name = format!("Pivot:{}", pivot_col);
        let v = ViewState::new_child(id, name, placeholder, parent_id, 0, parent_name);
        app.stack.push(v);

        // Background pivot computation
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            if let Ok(r) = do_pivot(&df, &keys, &pivot_col, &value_col, None) {
                let _ = tx.send(r);
            }
        });
        app.bg_meta = Some((parent_id, rx));
        Ok(())
    }

[36m--- src/plugin/pivot.rs:exec (lines 97-124) ---[0m
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        let (keys, df, parent_id, parent_name) = {
            let v = app.req()?;
            let cols = df_cols(&v.dataframe);
            let keys: Vec<String> = v.col_separator.map(|sep| cols[..sep].to_vec()).unwrap_or_default();
            if keys.is_empty() { return Err(anyhow!("Set xkey columns first (!)")); }
            (keys, v.dataframe.clone(), v.id, v.name.clone())
        };

        // Create placeholder and run in background
        let placeholder = placeholder_pivot(&keys, &self.pivot_col)?;
        let id = app.next_id();
        let name = format!("Pivot:{}", self.pivot_col);
        let v = ViewState::new_child(id, name, placeholder, parent_id, 0, parent_name);
        app.stack.push(v);

        let pivot_col = self.pivot_col.clone();
        let value_col = self.value_col.clone();
        let agg = self.agg.clone();
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            if let Ok(r) = do_pivot(&df, &keys, &pivot_col, &value_col, agg.as_deref()) {
                let _ = tx.send(r);
            }
        });
        app.bg_meta = Some((parent_id, rx));
        Ok(())
    }


Duplicates in src/backend/mod.rs:
------------------------------------------------------------
  src/backend/mod.rs:77-82 function df_save <-> src/backend/mod.rs:92-96 function metadata
  Similarity: 88.45%

[36m--- src/backend/mod.rs:df_save (lines 77-82) ---[0m
pub fn df_save(df: &DataFrame, path: &Path) -> Result<()> {
    let mut df = df.clone();
    ParquetWriter::new(std::fs::File::create(path)?)
        .finish(&mut df).map_err(|e| anyhow!("Parquet write: {}", e))?;
    Ok(())
}

[36m--- src/backend/mod.rs:metadata (lines 92-96) ---[0m
    fn metadata(&self, path: &str) -> Result<(usize, Vec<String>)> {
        let r = sql(self.lf(path)?, "SELECT COUNT(*) as cnt FROM df")?;
        let cnt = r.column("cnt")?.get(0)?.try_extract::<u64>().unwrap_or(0) as usize;
        Ok((cnt, self.cols(path)?))
    }

  src/backend/mod.rs:70-74 function sql_lazy <-> src/backend/mod.rs:77-82 function df_save
  Similarity: 88.39%

[36m--- src/backend/mod.rs:sql_lazy (lines 70-74) ---[0m
pub fn sql_lazy(lf: LazyFrame, query: &str) -> Result<LazyFrame> {
    let mut ctx = ::polars::sql::SQLContext::new();
    ctx.register("df", lf);
    ctx.execute(query).map_err(|e| anyhow!("{}", e))
}

[36m--- src/backend/mod.rs:df_save (lines 77-82) ---[0m
pub fn df_save(df: &DataFrame, path: &Path) -> Result<()> {
    let mut df = df.clone();
    ParquetWriter::new(std::fs::File::create(path)?)
        .finish(&mut df).map_err(|e| anyhow!("Parquet write: {}", e))?;
    Ok(())
}

  src/backend/mod.rs:77-82 function df_save <-> src/backend/mod.rs:133-136 function distinct
  Similarity: 90.73%

[36m--- src/backend/mod.rs:df_save (lines 77-82) ---[0m
pub fn df_save(df: &DataFrame, path: &Path) -> Result<()> {
    let mut df = df.clone();
    ParquetWriter::new(std::fs::File::create(path)?)
        .finish(&mut df).map_err(|e| anyhow!("Parquet write: {}", e))?;
    Ok(())
}

[36m--- src/backend/mod.rs:distinct (lines 133-136) ---[0m
    fn distinct(&self, path: &str, col: &str) -> Result<Vec<String>> {
        let df = sql(self.lf(path)?, &format!("SELECT DISTINCT \"{}\" FROM df ORDER BY \"{}\"", col, col))?;
        Ok(df.column(col).map(|c| (0..c.len()).filter_map(|i| c.get(i).ok().map(|v| v.to_string())).collect()).unwrap_or_default())
    }

  src/backend/mod.rs:77-82 function df_save <-> src/backend/mod.rs:121-124 function fetch_sel
  Similarity: 89.70%

[36m--- src/backend/mod.rs:df_save (lines 77-82) ---[0m
pub fn df_save(df: &DataFrame, path: &Path) -> Result<()> {
    let mut df = df.clone();
    ParquetWriter::new(std::fs::File::create(path)?)
        .finish(&mut df).map_err(|e| anyhow!("Parquet write: {}", e))?;
    Ok(())
}

[36m--- src/backend/mod.rs:fetch_sel (lines 121-124) ---[0m
    fn fetch_sel(&self, path: &str, cols: &[String], w: &str, offset: usize, limit: usize) -> Result<DataFrame> {
        let sel = if cols.is_empty() { "*".into() } else { cols.iter().map(|c| format!("\"{}\"", c)).collect::<Vec<_>>().join(",") };
        sql(self.lf(path)?, &format!("SELECT {} FROM df WHERE {} LIMIT {} OFFSET {}", sel, w, limit, offset))
    }

  src/backend/mod.rs:77-82 function df_save <-> src/backend/mod.rs:105-108 function schema
  Similarity: 87.48%

[36m--- src/backend/mod.rs:df_save (lines 77-82) ---[0m
pub fn df_save(df: &DataFrame, path: &Path) -> Result<()> {
    let mut df = df.clone();
    ParquetWriter::new(std::fs::File::create(path)?)
        .finish(&mut df).map_err(|e| anyhow!("Parquet write: {}", e))?;
    Ok(())
}

[36m--- src/backend/mod.rs:schema (lines 105-108) ---[0m
    fn schema(&self, path: &str) -> Result<Vec<(String, String)>> {
        let schema = self.lf(path)?.collect_schema().map_err(|e| anyhow!("{}", e))?;
        Ok(schema.iter().map(|(n, dt)| (n.to_string(), format!("{:?}", dt))).collect())
    }

  src/backend/mod.rs:70-74 function sql_lazy <-> src/backend/mod.rs:92-96 function metadata
  Similarity: 86.50%

[36m--- src/backend/mod.rs:sql_lazy (lines 70-74) ---[0m
pub fn sql_lazy(lf: LazyFrame, query: &str) -> Result<LazyFrame> {
    let mut ctx = ::polars::sql::SQLContext::new();
    ctx.register("df", lf);
    ctx.execute(query).map_err(|e| anyhow!("{}", e))
}

[36m--- src/backend/mod.rs:metadata (lines 92-96) ---[0m
    fn metadata(&self, path: &str) -> Result<(usize, Vec<String>)> {
        let r = sql(self.lf(path)?, "SELECT COUNT(*) as cnt FROM df")?;
        let cnt = r.column("cnt")?.get(0)?.try_extract::<u64>().unwrap_or(0) as usize;
        Ok((cnt, self.cols(path)?))
    }

  src/backend/mod.rs:52-54 function df_cols <-> src/backend/mod.rs:139-144 function save
  Similarity: 90.70%

[36m--- src/backend/mod.rs:df_cols (lines 52-54) ---[0m
pub fn df_cols(df: &DataFrame) -> Vec<String> {
    df.get_column_names().iter().map(|s| s.to_string()).collect()
}

[36m--- src/backend/mod.rs:save (lines 139-144) ---[0m
    fn save(&self, df: &DataFrame, path: &Path) -> Result<()> {
        match path.extension().and_then(|s| s.to_str()) {
            Some("csv") => polars::save_csv(df, path),
            _ => df_save(df, path),
        }
    }

  src/backend/mod.rs:23-28 function is_numeric <-> src/backend/mod.rs:52-54 function df_cols
  Similarity: 88.96%

[36m--- src/backend/mod.rs:is_numeric (lines 23-28) ---[0m
pub fn is_numeric(dt: &DataType) -> bool {
    matches!(dt,
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 |
        DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 |
        DataType::Float32 | DataType::Float64)
}

[36m--- src/backend/mod.rs:df_cols (lines 52-54) ---[0m
pub fn df_cols(df: &DataFrame) -> Vec<String> {
    df.get_column_names().iter().map(|s| s.to_string()).collect()
}

  src/backend/mod.rs:139-144 function save <-> src/backend/mod.rs:147-149 function freq_where
  Similarity: 86.62%

[36m--- src/backend/mod.rs:save (lines 139-144) ---[0m
    fn save(&self, df: &DataFrame, path: &Path) -> Result<()> {
        match path.extension().and_then(|s| s.to_str()) {
            Some("csv") => polars::save_csv(df, path),
            _ => df_save(df, path),
        }
    }

[36m--- src/backend/mod.rs:freq_where (lines 147-149) ---[0m
    fn freq_where(&self, path: &str, col: &str, w: &str) -> Result<DataFrame> {
        sql(self.lf(path)?, &format!("SELECT \"{}\", COUNT(*) as Cnt FROM df WHERE {} GROUP BY \"{}\" ORDER BY Cnt DESC", col, w, col))
    }

  src/backend/mod.rs:99-102 function cols <-> src/backend/mod.rs:105-108 function schema
  Similarity: 96.40%

[36m--- src/backend/mod.rs:cols (lines 99-102) ---[0m
    fn cols(&self, path: &str) -> Result<Vec<String>> {
        let schema = self.lf(path)?.collect_schema().map_err(|e| anyhow!("{}", e))?;
        Ok(schema.iter_names().map(|s| s.to_string()).collect())
    }

[36m--- src/backend/mod.rs:schema (lines 105-108) ---[0m
    fn schema(&self, path: &str) -> Result<Vec<(String, String)>> {
        let schema = self.lf(path)?.collect_schema().map_err(|e| anyhow!("{}", e))?;
        Ok(schema.iter().map(|(n, dt)| (n.to_string(), format!("{:?}", dt))).collect())
    }

  src/backend/mod.rs:99-102 function cols <-> src/backend/mod.rs:133-136 function distinct
  Similarity: 94.80%

[36m--- src/backend/mod.rs:cols (lines 99-102) ---[0m
    fn cols(&self, path: &str) -> Result<Vec<String>> {
        let schema = self.lf(path)?.collect_schema().map_err(|e| anyhow!("{}", e))?;
        Ok(schema.iter_names().map(|s| s.to_string()).collect())
    }

[36m--- src/backend/mod.rs:distinct (lines 133-136) ---[0m
    fn distinct(&self, path: &str, col: &str) -> Result<Vec<String>> {
        let df = sql(self.lf(path)?, &format!("SELECT DISTINCT \"{}\" FROM df ORDER BY \"{}\"", col, col))?;
        Ok(df.column(col).map(|c| (0..c.len()).filter_map(|i| c.get(i).ok().map(|v| v.to_string())).collect()).unwrap_or_default())
    }

  src/backend/mod.rs:105-108 function schema <-> src/backend/mod.rs:133-136 function distinct
  Similarity: 94.47%

[36m--- src/backend/mod.rs:schema (lines 105-108) ---[0m
    fn schema(&self, path: &str) -> Result<Vec<(String, String)>> {
        let schema = self.lf(path)?.collect_schema().map_err(|e| anyhow!("{}", e))?;
        Ok(schema.iter().map(|(n, dt)| (n.to_string(), format!("{:?}", dt))).collect())
    }

[36m--- src/backend/mod.rs:distinct (lines 133-136) ---[0m
    fn distinct(&self, path: &str, col: &str) -> Result<Vec<String>> {
        let df = sql(self.lf(path)?, &format!("SELECT DISTINCT \"{}\" FROM df ORDER BY \"{}\"", col, col))?;
        Ok(df.column(col).map(|c| (0..c.len()).filter_map(|i| c.get(i).ok().map(|v| v.to_string())).collect()).unwrap_or_default())
    }

  src/backend/mod.rs:127-130 function count_where <-> src/backend/mod.rs:133-136 function distinct
  Similarity: 94.47%

[36m--- src/backend/mod.rs:count_where (lines 127-130) ---[0m
    fn count_where(&self, path: &str, w: &str) -> Result<usize> {
        let r = sql(self.lf(path)?, &format!("SELECT COUNT(*) as cnt FROM df WHERE {}", w))?;
        Ok(r.column("cnt")?.get(0)?.try_extract::<u32>().unwrap_or(0) as usize)
    }

[36m--- src/backend/mod.rs:distinct (lines 133-136) ---[0m
    fn distinct(&self, path: &str, col: &str) -> Result<Vec<String>> {
        let df = sql(self.lf(path)?, &format!("SELECT DISTINCT \"{}\" FROM df ORDER BY \"{}\"", col, col))?;
        Ok(df.column(col).map(|c| (0..c.len()).filter_map(|i| c.get(i).ok().map(|v| v.to_string())).collect()).unwrap_or_default())
    }

  src/backend/mod.rs:121-124 function fetch_sel <-> src/backend/mod.rs:133-136 function distinct
  Similarity: 93.60%

[36m--- src/backend/mod.rs:fetch_sel (lines 121-124) ---[0m
    fn fetch_sel(&self, path: &str, cols: &[String], w: &str, offset: usize, limit: usize) -> Result<DataFrame> {
        let sel = if cols.is_empty() { "*".into() } else { cols.iter().map(|c| format!("\"{}\"", c)).collect::<Vec<_>>().join(",") };
        sql(self.lf(path)?, &format!("SELECT {} FROM df WHERE {} LIMIT {} OFFSET {}", sel, w, limit, offset))
    }

[36m--- src/backend/mod.rs:distinct (lines 133-136) ---[0m
    fn distinct(&self, path: &str, col: &str) -> Result<Vec<String>> {
        let df = sql(self.lf(path)?, &format!("SELECT DISTINCT \"{}\" FROM df ORDER BY \"{}\"", col, col))?;
        Ok(df.column(col).map(|c| (0..c.len()).filter_map(|i| c.get(i).ok().map(|v| v.to_string())).collect()).unwrap_or_default())
    }

  src/backend/mod.rs:99-102 function cols <-> src/backend/mod.rs:121-124 function fetch_sel
  Similarity: 92.89%

[36m--- src/backend/mod.rs:cols (lines 99-102) ---[0m
    fn cols(&self, path: &str) -> Result<Vec<String>> {
        let schema = self.lf(path)?.collect_schema().map_err(|e| anyhow!("{}", e))?;
        Ok(schema.iter_names().map(|s| s.to_string()).collect())
    }

[36m--- src/backend/mod.rs:fetch_sel (lines 121-124) ---[0m
    fn fetch_sel(&self, path: &str, cols: &[String], w: &str, offset: usize, limit: usize) -> Result<DataFrame> {
        let sel = if cols.is_empty() { "*".into() } else { cols.iter().map(|c| format!("\"{}\"", c)).collect::<Vec<_>>().join(",") };
        sql(self.lf(path)?, &format!("SELECT {} FROM df WHERE {} LIMIT {} OFFSET {}", sel, w, limit, offset))
    }

  src/backend/mod.rs:105-108 function schema <-> src/backend/mod.rs:121-124 function fetch_sel
  Similarity: 92.89%

[36m--- src/backend/mod.rs:schema (lines 105-108) ---[0m
    fn schema(&self, path: &str) -> Result<Vec<(String, String)>> {
        let schema = self.lf(path)?.collect_schema().map_err(|e| anyhow!("{}", e))?;
        Ok(schema.iter().map(|(n, dt)| (n.to_string(), format!("{:?}", dt))).collect())
    }

[36m--- src/backend/mod.rs:fetch_sel (lines 121-124) ---[0m
    fn fetch_sel(&self, path: &str, cols: &[String], w: &str, offset: usize, limit: usize) -> Result<DataFrame> {
        let sel = if cols.is_empty() { "*".into() } else { cols.iter().map(|c| format!("\"{}\"", c)).collect::<Vec<_>>().join(",") };
        sql(self.lf(path)?, &format!("SELECT {} FROM df WHERE {} LIMIT {} OFFSET {}", sel, w, limit, offset))
    }

  src/backend/mod.rs:121-124 function fetch_sel <-> src/backend/mod.rs:127-130 function count_where
  Similarity: 92.89%

[36m--- src/backend/mod.rs:fetch_sel (lines 121-124) ---[0m
    fn fetch_sel(&self, path: &str, cols: &[String], w: &str, offset: usize, limit: usize) -> Result<DataFrame> {
        let sel = if cols.is_empty() { "*".into() } else { cols.iter().map(|c| format!("\"{}\"", c)).collect::<Vec<_>>().join(",") };
        sql(self.lf(path)?, &format!("SELECT {} FROM df WHERE {} LIMIT {} OFFSET {}", sel, w, limit, offset))
    }

[36m--- src/backend/mod.rs:count_where (lines 127-130) ---[0m
    fn count_where(&self, path: &str, w: &str) -> Result<usize> {
        let r = sql(self.lf(path)?, &format!("SELECT COUNT(*) as cnt FROM df WHERE {}", w))?;
        Ok(r.column("cnt")?.get(0)?.try_extract::<u32>().unwrap_or(0) as usize)
    }

  src/backend/mod.rs:105-108 function schema <-> src/backend/mod.rs:127-130 function count_where
  Similarity: 92.52%

[36m--- src/backend/mod.rs:schema (lines 105-108) ---[0m
    fn schema(&self, path: &str) -> Result<Vec<(String, String)>> {
        let schema = self.lf(path)?.collect_schema().map_err(|e| anyhow!("{}", e))?;
        Ok(schema.iter().map(|(n, dt)| (n.to_string(), format!("{:?}", dt))).collect())
    }

[36m--- src/backend/mod.rs:count_where (lines 127-130) ---[0m
    fn count_where(&self, path: &str, w: &str) -> Result<usize> {
        let r = sql(self.lf(path)?, &format!("SELECT COUNT(*) as cnt FROM df WHERE {}", w))?;
        Ok(r.column("cnt")?.get(0)?.try_extract::<u32>().unwrap_or(0) as usize)
    }

  src/backend/mod.rs:105-108 function schema <-> src/backend/mod.rs:188-191 function sort_head
  Similarity: 91.35%

[36m--- src/backend/mod.rs:schema (lines 105-108) ---[0m
    fn schema(&self, path: &str) -> Result<Vec<(String, String)>> {
        let schema = self.lf(path)?.collect_schema().map_err(|e| anyhow!("{}", e))?;
        Ok(schema.iter().map(|(n, dt)| (n.to_string(), format!("{:?}", dt))).collect())
    }

[36m--- src/backend/mod.rs:sort_head (lines 188-191) ---[0m
    fn sort_head(&self, path: &str, col: &str, desc: bool, limit: usize) -> Result<DataFrame> {
        let ord = if desc { "DESC" } else { "ASC" };
        sql(self.lf(path)?, &format!("SELECT * FROM df ORDER BY \"{}\" {} LIMIT {}", col, ord, limit))
    }

  src/backend/mod.rs:99-102 function cols <-> src/backend/mod.rs:127-130 function count_where
  Similarity: 90.98%

[36m--- src/backend/mod.rs:cols (lines 99-102) ---[0m
    fn cols(&self, path: &str) -> Result<Vec<String>> {
        let schema = self.lf(path)?.collect_schema().map_err(|e| anyhow!("{}", e))?;
        Ok(schema.iter_names().map(|s| s.to_string()).collect())
    }

[36m--- src/backend/mod.rs:count_where (lines 127-130) ---[0m
    fn count_where(&self, path: &str, w: &str) -> Result<usize> {
        let r = sql(self.lf(path)?, &format!("SELECT COUNT(*) as cnt FROM df WHERE {}", w))?;
        Ok(r.column("cnt")?.get(0)?.try_extract::<u32>().unwrap_or(0) as usize)
    }

  src/backend/mod.rs:127-130 function count_where <-> src/backend/mod.rs:188-191 function sort_head
  Similarity: 89.57%

[36m--- src/backend/mod.rs:count_where (lines 127-130) ---[0m
    fn count_where(&self, path: &str, w: &str) -> Result<usize> {
        let r = sql(self.lf(path)?, &format!("SELECT COUNT(*) as cnt FROM df WHERE {}", w))?;
        Ok(r.column("cnt")?.get(0)?.try_extract::<u32>().unwrap_or(0) as usize)
    }

[36m--- src/backend/mod.rs:sort_head (lines 188-191) ---[0m
    fn sort_head(&self, path: &str, col: &str, desc: bool, limit: usize) -> Result<DataFrame> {
        let ord = if desc { "DESC" } else { "ASC" };
        sql(self.lf(path)?, &format!("SELECT * FROM df ORDER BY \"{}\" {} LIMIT {}", col, ord, limit))
    }

  src/backend/mod.rs:99-102 function cols <-> src/backend/mod.rs:188-191 function sort_head
  Similarity: 89.33%

[36m--- src/backend/mod.rs:cols (lines 99-102) ---[0m
    fn cols(&self, path: &str) -> Result<Vec<String>> {
        let schema = self.lf(path)?.collect_schema().map_err(|e| anyhow!("{}", e))?;
        Ok(schema.iter_names().map(|s| s.to_string()).collect())
    }

[36m--- src/backend/mod.rs:sort_head (lines 188-191) ---[0m
    fn sort_head(&self, path: &str, col: &str, desc: bool, limit: usize) -> Result<DataFrame> {
        let ord = if desc { "DESC" } else { "ASC" };
        sql(self.lf(path)?, &format!("SELECT * FROM df ORDER BY \"{}\" {} LIMIT {}", col, ord, limit))
    }

  src/backend/mod.rs:52-54 function df_cols <-> src/backend/mod.rs:147-149 function freq_where
  Similarity: 90.91%

[36m--- src/backend/mod.rs:df_cols (lines 52-54) ---[0m
pub fn df_cols(df: &DataFrame) -> Vec<String> {
    df.get_column_names().iter().map(|s| s.to_string()).collect()
}

[36m--- src/backend/mod.rs:freq_where (lines 147-149) ---[0m
    fn freq_where(&self, path: &str, col: &str, w: &str) -> Result<DataFrame> {
        sql(self.lf(path)?, &format!("SELECT \"{}\", COUNT(*) as Cnt FROM df WHERE {} GROUP BY \"{}\" ORDER BY Cnt DESC", col, w, col))
    }

  src/backend/mod.rs:65-67 function sql <-> src/backend/mod.rs:147-149 function freq_where
  Similarity: 86.96%

[36m--- src/backend/mod.rs:sql (lines 65-67) ---[0m
pub fn sql(lf: LazyFrame, query: &str) -> Result<DataFrame> {
    sql_lazy(lf, query)?.collect_with_engine(Engine::Streaming).map_err(|e| anyhow!("{}", e))
}

[36m--- src/backend/mod.rs:freq_where (lines 147-149) ---[0m
    fn freq_where(&self, path: &str, col: &str, w: &str) -> Result<DataFrame> {
        sql(self.lf(path)?, &format!("SELECT \"{}\", COUNT(*) as Cnt FROM df WHERE {} GROUP BY \"{}\" ORDER BY Cnt DESC", col, w, col))
    }


Duplicates in src/plugin/system.rs:
------------------------------------------------------------
  src/plugin/system.rs:445-534 function cargo <-> src/plugin/system.rs:551-620 function pacman
  Similarity: 89.98%

[36m--- src/plugin/system.rs:cargo (lines 445-534) ---[0m
fn cargo() -> Result<DataFrame> {
    use std::collections::{HashMap, HashSet};
    let text = run_cmd("cargo", &["metadata", "--format-version", "1"])?;
    let json: serde_json::Value = serde_json::from_str(&text)?;

    // Get linux-compiled packages via --filter-platform
    let linux_text = run_cmd("cargo", &["metadata", "--format-version", "1", "--filter-platform", "x86_64-unknown-linux-gnu"])?;
    let linux_json: serde_json::Value = serde_json::from_str(&linux_text)?;
    let linux_pkgs: HashSet<String> = linux_json["packages"].as_array()
        .map(|a| a.iter().filter_map(|p| p["name"].as_str().map(String::from)).collect())
        .unwrap_or_default();

    // Build package info map: id -> (name, ver, desc, deps)
    let mut pkg_info: HashMap<String, (String, String, String, Vec<String>)> = HashMap::new();
    let mut pkg_size: HashMap<String, u64> = HashMap::new();
    if let Some(pkgs) = json["packages"].as_array() {
        for p in pkgs {
            let id = p["id"].as_str().unwrap_or("").to_string();
            let name = p["name"].as_str().unwrap_or("").to_string();
            let ver = p["version"].as_str().unwrap_or("").to_string();
            let desc = p["description"].as_str().unwrap_or("").to_string();
            let deps: Vec<String> = p["dependencies"].as_array()
                .map(|a| a.iter().filter_map(|d| d["name"].as_str().map(String::from)).collect())
                .unwrap_or_default();
            // Estimate size from manifest_path's parent dir
            let size = p["manifest_path"].as_str()
                .and_then(|mp| std::path::Path::new(mp).parent())
                .map(|d| dir_size(d)).unwrap_or(0);
            pkg_size.insert(name.clone(), size);
            pkg_info.insert(id, (name, ver, desc, deps));
        }
    }

    // Build reverse deps from resolve graph
    let mut req_by: HashMap<String, Vec<String>> = HashMap::new();
    let mut resolved: Vec<String> = vec![];
    if let Some(resolve) = json["resolve"].as_object() {
        if let Some(nodes) = resolve["nodes"].as_array() {
            for n in nodes {
                let id = n["id"].as_str().unwrap_or("");
                let name = pkg_info.get(id).map(|i| i.0.clone()).unwrap_or_default();
                if name.is_empty() { continue; }
                resolved.push(id.to_string());
                if let Some(deps) = n["deps"].as_array() {
                    for d in deps {
                        let dep_name = d["name"].as_str().unwrap_or("");
                        req_by.entry(dep_name.to_string()).or_default().push(name.clone());
                    }
                }
            }
        }
    }

    // Build output vectors
    let (mut names, mut vers, mut latest, mut descs, mut plat) = (vec![], vec![], vec![], vec![], vec![]);
    let (mut sizes, mut rsizes, mut deps_cnt, mut req_cnt) = (vec![], vec![], vec![], vec![]);
    let ver_cache = load_ver_cache();
    let mut all_names: Vec<String> = vec![];

    for id in &resolved {
        if let Some((name, ver, desc, deps)) = pkg_info.get(id) {
            let size = pkg_size.get(name).copied().unwrap_or(0);
            let rsize = calc_rsize(name, size, deps, &pkg_size, &req_by);
            let reqs = req_by.get(name).map(|v| v.len()).unwrap_or(0) as u32;
            let lat = latest_ver(name, &ver_cache);
            all_names.push(name.clone());
            names.push(name.clone()); vers.push(ver.clone()); latest.push(lat); descs.push(desc.clone());
            sizes.push(size / 1024); rsizes.push(rsize / 1024);
            deps_cnt.push(deps.len() as u32); req_cnt.push(reqs);
            // Infer platform from package name or linux compilation
            let p = if linux_pkgs.contains(name) { "linux" }
                else if name.contains("windows") { "windows" }
                else if name.contains("macos") || name.contains("core-foundation") || name.contains("objc") { "macos" }
                else if name.contains("android") { "android" }
                else if name.contains("wasm") || name.contains("js-sys") || name.contains("web-sys") { "wasm" }
                else { "" };
            plat.push(p);
        }
    }

    // Update stale cache entries in background
    update_ver_cache_bg(all_names);

    Ok(DataFrame::new(vec![
        ser!("name", names), ser!("version", vers), ser!("latest", latest),
        ser!("size(k)", sizes), ser!("rsize(k)", rsizes),
        ser!("deps", deps_cnt), ser!("req_by", req_cnt),
        ser!("platform", plat), ser!("description", descs),
    ])?)
}

[36m--- src/plugin/system.rs:pacman (lines 551-620) ---[0m
fn pacman() -> Result<DataFrame> {
    use std::collections::{HashSet, HashMap};

    let orphan_text = run_cmd("pacman", &["-Qdt"])?;
    let orphans: HashSet<String> = orphan_text.lines().filter_map(|l| l.split_whitespace().next()).map(String::from).collect();

    let text = run_cmd("pacman", &["-Qi"])?;

    // First pass: collect sizes and who requires each package
    let (mut pkg_size, mut pkg_req_by): (HashMap<String, u64>, HashMap<String, Vec<String>>) = (HashMap::new(), HashMap::new());
    let (mut name, mut size, mut req_list) = (String::new(), 0u64, vec![]);

    for line in text.lines() {
        if line.is_empty() {
            if !name.is_empty() { pkg_size.insert(name.clone(), size); pkg_req_by.insert(std::mem::take(&mut name), std::mem::take(&mut req_list)); size = 0; }
            continue;
        }
        if let Some((k, v)) = line.split_once(':') {
            match k.trim() {
                "Name" => name = v.trim().into(),
                "Installed Size" => size = parse_size(v.trim()),
                "Required By" => req_list = parse_deps(v.trim()),
                _ => {}
            }
        }
    }
    if !name.is_empty() { pkg_size.insert(name.clone(), size); pkg_req_by.insert(name, req_list); }

    // Second pass: build output
    let (mut names, mut vers, mut descs, mut sizes, mut rsizes) = (vec![], vec![], vec![], vec![], vec![]);
    let (mut installed, mut reasons, mut deps_cnt, mut req_cnt, mut orphan_flags): (Vec<String>, Vec<String>, Vec<u32>, Vec<u32>, Vec<String>) = (vec![], vec![], vec![], vec![], vec![]);
    let (mut name, mut ver, mut desc, mut inst, mut reason) = (String::new(), String::new(), String::new(), String::new(), String::new());
    let (mut size, mut deps, mut reqs, mut deps_list) = (0u64, 0u32, 0u32, vec![]);

    // Helper to push current package to output vectors
    let mut push_pkg = |n: &mut String, v: &mut String, d: &mut String, i: &mut String, r: &mut String, sz: &mut u64, dc: &mut u32, rc: &mut u32, dl: &mut Vec<String>| {
        if n.is_empty() { return; }
        rsizes.push(calc_rsize(n, *sz, dl, &pkg_size, &pkg_req_by));
        orphan_flags.push(if orphans.contains(n.as_str()) { "x".into() } else { "".into() });
        names.push(std::mem::take(n)); vers.push(std::mem::take(v)); descs.push(std::mem::take(d));
        sizes.push(*sz); installed.push(std::mem::take(i)); reasons.push(std::mem::take(r));
        deps_cnt.push(*dc); req_cnt.push(*rc); dl.clear(); *sz = 0; *dc = 0; *rc = 0;
    };

    for line in text.lines() {
        if line.is_empty() { push_pkg(&mut name, &mut ver, &mut desc, &mut inst, &mut reason, &mut size, &mut deps, &mut reqs, &mut deps_list); continue; }
        if let Some((k, v)) = line.split_once(':') {
            let v = v.trim();
            match k.trim() {
                "Name" => name = v.into(), "Version" => ver = v.into(), "Description" => desc = v.into(),
                "Installed Size" => size = parse_size(v), "Install Date" => inst = parse_date(v),
                "Install Reason" => reason = if v.contains("dependency") { "dep".into() } else { "explicit".into() },
                "Depends On" => { deps_list = parse_deps(v); deps = deps_list.len() as u32; },
                "Required By" => reqs = if v == "None" { 0 } else { v.split_whitespace().count() as u32 },
                _ => {}
            }
        }
    }
    push_pkg(&mut name, &mut ver, &mut desc, &mut inst, &mut reason, &mut size, &mut deps, &mut reqs, &mut deps_list);

    let sizes: Vec<u64> = sizes.iter().map(|b| b / 1024).collect();
    let rsizes: Vec<u64> = rsizes.iter().map(|b| b / 1024).collect();
    Ok(DataFrame::new(vec![
        ser!("name", names), ser!("version", vers),
        ser!("size(k)", sizes), ser!("rsize(k)", rsizes),
        ser!("deps", deps_cnt), ser!("req_by", req_cnt),
        ser!("orphan", orphan_flags), ser!("reason", reasons),
        ser!("installed", installed), ser!("description", descs),
    ])?)
}

  src/plugin/system.rs:181-205 function ps <-> src/plugin/system.rs:320-337 function journalctl
  Similarity: 89.57%

[36m--- src/plugin/system.rs:ps (lines 181-205) ---[0m
fn ps() -> Result<DataFrame> {
    let text = run_cmd("ps", &["aux"])?;

    let (mut users, mut pids, mut cpus, mut mems) = (vec![], vec![], vec![], vec![]);
    let (mut vszs, mut rsss, mut ttys, mut stats) = (vec![], vec![], vec![], vec![]);
    let (mut starts, mut times, mut cmds) = (vec![], vec![], vec![]);

    for line in text.lines().skip(1) {
        let p: Vec<&str> = line.split_whitespace().collect();
        if p.len() >= 11 {
            users.push(p[0].to_string()); pids.push(p[1].parse::<i32>().unwrap_or(0));
            cpus.push(p[2].parse::<f64>().unwrap_or(0.0)); mems.push(p[3].parse::<f64>().unwrap_or(0.0));
            vszs.push(p[4].parse::<u64>().unwrap_or(0)); rsss.push(p[5].parse::<u64>().unwrap_or(0));
            ttys.push(p[6].to_string()); stats.push(p[7].to_string());
            starts.push(p[8].to_string()); times.push(p[9].to_string());
            cmds.push(p[10..].join(" "));
        }
    }

    Ok(DataFrame::new(vec![
        ser!("user", users), ser!("pid", pids), ser!("%cpu", cpus), ser!("%mem", mems),
        ser!("vsz", vszs), ser!("rss", rsss), ser!("tty", ttys), ser!("stat", stats),
        ser!("start", starts), ser!("time", times), ser!("command", cmds),
    ])?)
}

[36m--- src/plugin/system.rs:journalctl (lines 320-337) ---[0m
fn journalctl(n: usize) -> Result<DataFrame> {
    let ns = n.to_string();
    let text = run_cmd("journalctl", &["--no-pager", "-o", "short-iso", "-n", &ns])?;
    let (mut times, mut hosts, mut units, mut msgs) = (vec![], vec![], vec![], vec![]);
    for line in text.lines() {
        let p: Vec<&str> = line.splitn(4, ' ').collect();
        if p.len() >= 4 {
            times.push(p[0].to_string()); hosts.push(p[1].to_string());
            units.push(p[2].split('[').next().unwrap_or(p[2]).trim_end_matches(':').to_string());
            msgs.push(p[3].to_string());
        } else {
            times.push("".into()); hosts.push("".into()); units.push("".into()); msgs.push(line.into());
        }
    }
    Ok(DataFrame::new(vec![
        ser!("time", times), ser!("host", hosts), ser!("unit", units), ser!("message", msgs),
    ])?)
}

  src/plugin/system.rs:266-285 function lsof <-> src/plugin/system.rs:320-337 function journalctl
  Similarity: 89.11%

[36m--- src/plugin/system.rs:lsof (lines 266-285) ---[0m
fn lsof(pid: Option<i32>) -> Result<DataFrame> {
    let (mut pids, mut fds, mut paths) = (vec![], vec![], vec![]);
    let dirs: Vec<i32> = if let Some(p) = pid { vec![p] } else {
        fs::read_dir("/proc")?.filter_map(|e| e.ok())
            .filter_map(|e| e.file_name().to_string_lossy().parse().ok()).collect()
    };
    for p in dirs {
        if let Ok(entries) = fs::read_dir(format!("/proc/{}/fd", p)) {
            for e in entries.flatten() {
                if let Ok(fd) = e.file_name().to_string_lossy().parse::<i32>() {
                    let link = fs::read_link(e.path()).map(|p| p.to_string_lossy().to_string()).unwrap_or_default();
                    pids.push(p); fds.push(fd); paths.push(link);
                }
            }
        }
    }
    Ok(DataFrame::new(vec![
        ser!("pid", pids), ser!("fd", fds), ser!("path", paths),
    ])?)
}

[36m--- src/plugin/system.rs:journalctl (lines 320-337) ---[0m
fn journalctl(n: usize) -> Result<DataFrame> {
    let ns = n.to_string();
    let text = run_cmd("journalctl", &["--no-pager", "-o", "short-iso", "-n", &ns])?;
    let (mut times, mut hosts, mut units, mut msgs) = (vec![], vec![], vec![], vec![]);
    for line in text.lines() {
        let p: Vec<&str> = line.splitn(4, ' ').collect();
        if p.len() >= 4 {
            times.push(p[0].to_string()); hosts.push(p[1].to_string());
            units.push(p[2].split('[').next().unwrap_or(p[2]).trim_end_matches(':').to_string());
            msgs.push(p[3].to_string());
        } else {
            times.push("".into()); hosts.push("".into()); units.push("".into()); msgs.push(line.into());
        }
    }
    Ok(DataFrame::new(vec![
        ser!("time", times), ser!("host", hosts), ser!("unit", units), ser!("message", msgs),
    ])?)
}

  src/plugin/system.rs:49-63 method exec <-> src/plugin/system.rs:266-285 function lsof
  Similarity: 94.49%

[36m--- src/plugin/system.rs:exec (lines 49-63) ---[0m
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        let (name, df) = match self {
            SysCmd::Ps => ("ps", ps()?),
            SysCmd::Mounts => ("mounts", mounts()?),
            SysCmd::Tcp => ("tcp", tcp()?),
            SysCmd::Udp => ("udp", udp()?),
            SysCmd::Env => ("env", env()?),
            SysCmd::Systemctl => ("systemctl", systemctl()?),
            SysCmd::Pacman => ("pacman", pacman()?),
            SysCmd::Cargo => ("cargo", cargo()?),
        };
        let id = app.next_id();
        app.stack.push(ViewState::new(id, name.into(), df, None));
        Ok(())
    }

[36m--- src/plugin/system.rs:lsof (lines 266-285) ---[0m
fn lsof(pid: Option<i32>) -> Result<DataFrame> {
    let (mut pids, mut fds, mut paths) = (vec![], vec![], vec![]);
    let dirs: Vec<i32> = if let Some(p) = pid { vec![p] } else {
        fs::read_dir("/proc")?.filter_map(|e| e.ok())
            .filter_map(|e| e.file_name().to_string_lossy().parse().ok()).collect()
    };
    for p in dirs {
        if let Ok(entries) = fs::read_dir(format!("/proc/{}/fd", p)) {
            for e in entries.flatten() {
                if let Ok(fd) = e.file_name().to_string_lossy().parse::<i32>() {
                    let link = fs::read_link(e.path()).map(|p| p.to_string_lossy().to_string()).unwrap_or_default();
                    pids.push(p); fds.push(fd); paths.push(link);
                }
            }
        }
    }
    Ok(DataFrame::new(vec![
        ser!("pid", pids), ser!("fd", fds), ser!("path", paths),
    ])?)
}

  src/plugin/system.rs:266-285 function lsof <-> src/plugin/system.rs:302-317 function systemctl
  Similarity: 90.25%

[36m--- src/plugin/system.rs:lsof (lines 266-285) ---[0m
fn lsof(pid: Option<i32>) -> Result<DataFrame> {
    let (mut pids, mut fds, mut paths) = (vec![], vec![], vec![]);
    let dirs: Vec<i32> = if let Some(p) = pid { vec![p] } else {
        fs::read_dir("/proc")?.filter_map(|e| e.ok())
            .filter_map(|e| e.file_name().to_string_lossy().parse().ok()).collect()
    };
    for p in dirs {
        if let Ok(entries) = fs::read_dir(format!("/proc/{}/fd", p)) {
            for e in entries.flatten() {
                if let Ok(fd) = e.file_name().to_string_lossy().parse::<i32>() {
                    let link = fs::read_link(e.path()).map(|p| p.to_string_lossy().to_string()).unwrap_or_default();
                    pids.push(p); fds.push(fd); paths.push(link);
                }
            }
        }
    }
    Ok(DataFrame::new(vec![
        ser!("pid", pids), ser!("fd", fds), ser!("path", paths),
    ])?)
}

[36m--- src/plugin/system.rs:systemctl (lines 302-317) ---[0m
fn systemctl() -> Result<DataFrame> {
    let text = run_cmd("systemctl", &["list-units", "--type=service", "--all", "--no-pager", "--no-legend"])?;
    let (mut units, mut loads, mut actives, mut subs, mut descs) = (vec![], vec![], vec![], vec![], vec![]);
    for line in text.lines() {
        let p: Vec<&str> = line.split_whitespace().collect();
        if p.len() >= 5 {
            units.push(p[0].to_string()); loads.push(p[1].to_string());
            actives.push(p[2].to_string()); subs.push(p[3].to_string());
            descs.push(p[4..].join(" "));
        }
    }
    Ok(DataFrame::new(vec![
        ser!("unit", units), ser!("load", loads), ser!("active", actives),
        ser!("sub", subs), ser!("description", descs),
    ])?)
}

  src/plugin/system.rs:266-285 function lsof <-> src/plugin/system.rs:403-416 function fetch_latest
  Similarity: 89.12%

[36m--- src/plugin/system.rs:lsof (lines 266-285) ---[0m
fn lsof(pid: Option<i32>) -> Result<DataFrame> {
    let (mut pids, mut fds, mut paths) = (vec![], vec![], vec![]);
    let dirs: Vec<i32> = if let Some(p) = pid { vec![p] } else {
        fs::read_dir("/proc")?.filter_map(|e| e.ok())
            .filter_map(|e| e.file_name().to_string_lossy().parse().ok()).collect()
    };
    for p in dirs {
        if let Ok(entries) = fs::read_dir(format!("/proc/{}/fd", p)) {
            for e in entries.flatten() {
                if let Ok(fd) = e.file_name().to_string_lossy().parse::<i32>() {
                    let link = fs::read_link(e.path()).map(|p| p.to_string_lossy().to_string()).unwrap_or_default();
                    pids.push(p); fds.push(fd); paths.push(link);
                }
            }
        }
    }
    Ok(DataFrame::new(vec![
        ser!("pid", pids), ser!("fd", fds), ser!("path", paths),
    ])?)
}

[36m--- src/plugin/system.rs:fetch_latest (lines 403-416) ---[0m
fn fetch_latest(name: &str) -> Option<String> {
    use std::os::unix::process::CommandExt;
    let mut cmd = std::process::Command::new("cargo");
    cmd.args(["search", name, "--limit", "1", "--color=never"])
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null());
    // Detach from controlling terminal via new session
    unsafe { cmd.pre_exec(|| { nix::unistd::setsid().ok(); Ok(()) }); }
    cmd.output().ok()
        .map(|o| String::from_utf8_lossy(&o.stdout).to_string())
        .and_then(|s| s.lines().next().map(String::from))
        .and_then(|l| l.split('"').nth(1).map(String::from))
}

  src/plugin/system.rs:302-317 function systemctl <-> src/plugin/system.rs:320-337 function journalctl
  Similarity: 88.36%

[36m--- src/plugin/system.rs:systemctl (lines 302-317) ---[0m
fn systemctl() -> Result<DataFrame> {
    let text = run_cmd("systemctl", &["list-units", "--type=service", "--all", "--no-pager", "--no-legend"])?;
    let (mut units, mut loads, mut actives, mut subs, mut descs) = (vec![], vec![], vec![], vec![], vec![]);
    for line in text.lines() {
        let p: Vec<&str> = line.split_whitespace().collect();
        if p.len() >= 5 {
            units.push(p[0].to_string()); loads.push(p[1].to_string());
            actives.push(p[2].to_string()); subs.push(p[3].to_string());
            descs.push(p[4..].join(" "));
        }
    }
    Ok(DataFrame::new(vec![
        ser!("unit", units), ser!("load", loads), ser!("active", actives),
        ser!("sub", subs), ser!("description", descs),
    ])?)
}

[36m--- src/plugin/system.rs:journalctl (lines 320-337) ---[0m
fn journalctl(n: usize) -> Result<DataFrame> {
    let ns = n.to_string();
    let text = run_cmd("journalctl", &["--no-pager", "-o", "short-iso", "-n", &ns])?;
    let (mut times, mut hosts, mut units, mut msgs) = (vec![], vec![], vec![], vec![]);
    for line in text.lines() {
        let p: Vec<&str> = line.splitn(4, ' ').collect();
        if p.len() >= 4 {
            times.push(p[0].to_string()); hosts.push(p[1].to_string());
            units.push(p[2].split('[').next().unwrap_or(p[2]).trim_end_matches(':').to_string());
            msgs.push(p[3].to_string());
        } else {
            times.push("".into()); hosts.push("".into()); units.push("".into()); msgs.push(line.into());
        }
    }
    Ok(DataFrame::new(vec![
        ser!("time", times), ser!("host", hosts), ser!("unit", units), ser!("message", msgs),
    ])?)
}

  src/plugin/system.rs:419-437 function update_ver_cache_bg <-> src/plugin/system.rs:537-548 function dir_size
  Similarity: 96.34%

[36m--- src/plugin/system.rs:update_ver_cache_bg (lines 419-437) ---[0m
fn update_ver_cache_bg(names: Vec<String>) {
    std::thread::spawn(move || {
        let now = chrono::Utc::now().timestamp();
        let day = 86400;
        let mut cache = load_ver_cache();
        let mut cnt = 0;
        for name in names {
            let stale = cache.get(&name).map(|(_, ts)| now - ts > day).unwrap_or(true);
            if stale {
                if let Some(ver) = fetch_latest(&name) {
                    cache.insert(name, (ver, now));
                    cnt += 1;
                    if cnt % 10 == 0 { save_ver_cache(&cache); }  // save every 10 fetches
                }
            }
        }
        save_ver_cache(&cache);
    });
}

[36m--- src/plugin/system.rs:dir_size (lines 537-548) ---[0m
fn dir_size(path: &std::path::Path) -> u64 {
    fs::read_dir(path).ok().map(|entries| {
        entries.filter_map(|e| e.ok()).map(|e| {
            let m = e.metadata().ok();
            if m.as_ref().map(|m| m.is_dir()).unwrap_or(false) {
                dir_size(&e.path())
            } else {
                m.map(|m| m.len()).unwrap_or(0)
            }
        }).sum()
    }).unwrap_or(0)
}

  src/plugin/system.rs:49-63 method exec <-> src/plugin/system.rs:320-337 function journalctl
  Similarity: 89.51%

[36m--- src/plugin/system.rs:exec (lines 49-63) ---[0m
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        let (name, df) = match self {
            SysCmd::Ps => ("ps", ps()?),
            SysCmd::Mounts => ("mounts", mounts()?),
            SysCmd::Tcp => ("tcp", tcp()?),
            SysCmd::Udp => ("udp", udp()?),
            SysCmd::Env => ("env", env()?),
            SysCmd::Systemctl => ("systemctl", systemctl()?),
            SysCmd::Pacman => ("pacman", pacman()?),
            SysCmd::Cargo => ("cargo", cargo()?),
        };
        let id = app.next_id();
        app.stack.push(ViewState::new(id, name.into(), df, None));
        Ok(())
    }

[36m--- src/plugin/system.rs:journalctl (lines 320-337) ---[0m
fn journalctl(n: usize) -> Result<DataFrame> {
    let ns = n.to_string();
    let text = run_cmd("journalctl", &["--no-pager", "-o", "short-iso", "-n", &ns])?;
    let (mut times, mut hosts, mut units, mut msgs) = (vec![], vec![], vec![], vec![]);
    for line in text.lines() {
        let p: Vec<&str> = line.splitn(4, ' ').collect();
        if p.len() >= 4 {
            times.push(p[0].to_string()); hosts.push(p[1].to_string());
            units.push(p[2].split('[').next().unwrap_or(p[2]).trim_end_matches(':').to_string());
            msgs.push(p[3].to_string());
        } else {
            times.push("".into()); hosts.push("".into()); units.push("".into()); msgs.push(line.into());
        }
    }
    Ok(DataFrame::new(vec![
        ser!("time", times), ser!("host", hosts), ser!("unit", units), ser!("message", msgs),
    ])?)
}

  src/plugin/system.rs:320-337 function journalctl <-> src/plugin/system.rs:403-416 function fetch_latest
  Similarity: 89.14%

[36m--- src/plugin/system.rs:journalctl (lines 320-337) ---[0m
fn journalctl(n: usize) -> Result<DataFrame> {
    let ns = n.to_string();
    let text = run_cmd("journalctl", &["--no-pager", "-o", "short-iso", "-n", &ns])?;
    let (mut times, mut hosts, mut units, mut msgs) = (vec![], vec![], vec![], vec![]);
    for line in text.lines() {
        let p: Vec<&str> = line.splitn(4, ' ').collect();
        if p.len() >= 4 {
            times.push(p[0].to_string()); hosts.push(p[1].to_string());
            units.push(p[2].split('[').next().unwrap_or(p[2]).trim_end_matches(':').to_string());
            msgs.push(p[3].to_string());
        } else {
            times.push("".into()); hosts.push("".into()); units.push("".into()); msgs.push(line.into());
        }
    }
    Ok(DataFrame::new(vec![
        ser!("time", times), ser!("host", hosts), ser!("unit", units), ser!("message", msgs),
    ])?)
}

[36m--- src/plugin/system.rs:fetch_latest (lines 403-416) ---[0m
fn fetch_latest(name: &str) -> Option<String> {
    use std::os::unix::process::CommandExt;
    let mut cmd = std::process::Command::new("cargo");
    cmd.args(["search", name, "--limit", "1", "--color=never"])
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null());
    // Detach from controlling terminal via new session
    unsafe { cmd.pre_exec(|| { nix::unistd::setsid().ok(); Ok(()) }); }
    cmd.output().ok()
        .map(|o| String::from_utf8_lossy(&o.stdout).to_string())
        .and_then(|s| s.lines().next().map(String::from))
        .and_then(|l| l.split('"').nth(1).map(String::from))
}

  src/plugin/system.rs:49-63 method exec <-> src/plugin/system.rs:302-317 function systemctl
  Similarity: 90.70%

[36m--- src/plugin/system.rs:exec (lines 49-63) ---[0m
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        let (name, df) = match self {
            SysCmd::Ps => ("ps", ps()?),
            SysCmd::Mounts => ("mounts", mounts()?),
            SysCmd::Tcp => ("tcp", tcp()?),
            SysCmd::Udp => ("udp", udp()?),
            SysCmd::Env => ("env", env()?),
            SysCmd::Systemctl => ("systemctl", systemctl()?),
            SysCmd::Pacman => ("pacman", pacman()?),
            SysCmd::Cargo => ("cargo", cargo()?),
        };
        let id = app.next_id();
        app.stack.push(ViewState::new(id, name.into(), df, None));
        Ok(())
    }

[36m--- src/plugin/system.rs:systemctl (lines 302-317) ---[0m
fn systemctl() -> Result<DataFrame> {
    let text = run_cmd("systemctl", &["list-units", "--type=service", "--all", "--no-pager", "--no-legend"])?;
    let (mut units, mut loads, mut actives, mut subs, mut descs) = (vec![], vec![], vec![], vec![], vec![]);
    for line in text.lines() {
        let p: Vec<&str> = line.split_whitespace().collect();
        if p.len() >= 5 {
            units.push(p[0].to_string()); loads.push(p[1].to_string());
            actives.push(p[2].to_string()); subs.push(p[3].to_string());
            descs.push(p[4..].join(" "));
        }
    }
    Ok(DataFrame::new(vec![
        ser!("unit", units), ser!("load", loads), ser!("active", actives),
        ser!("sub", subs), ser!("description", descs),
    ])?)
}

  src/plugin/system.rs:49-63 method exec <-> src/plugin/system.rs:229-243 function parse_net
  Similarity: 92.63%

[36m--- src/plugin/system.rs:exec (lines 49-63) ---[0m
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        let (name, df) = match self {
            SysCmd::Ps => ("ps", ps()?),
            SysCmd::Mounts => ("mounts", mounts()?),
            SysCmd::Tcp => ("tcp", tcp()?),
            SysCmd::Udp => ("udp", udp()?),
            SysCmd::Env => ("env", env()?),
            SysCmd::Systemctl => ("systemctl", systemctl()?),
            SysCmd::Pacman => ("pacman", pacman()?),
            SysCmd::Cargo => ("cargo", cargo()?),
        };
        let id = app.next_id();
        app.stack.push(ViewState::new(id, name.into(), df, None));
        Ok(())
    }

[36m--- src/plugin/system.rs:parse_net (lines 229-243) ---[0m
fn parse_net(path: &str) -> Result<DataFrame> {
    let (mut la, mut lp, mut ra, mut rp, mut st, mut ino) = (vec![], vec![], vec![], vec![], vec![], vec![]);
    for line in fs::read_to_string(path)?.lines().skip(1) {
        let p: Vec<&str> = line.split_whitespace().collect();
        if p.len() >= 10 {
            let (a1, p1) = parse_addr(p[1]); let (a2, p2) = parse_addr(p[2]);
            la.push(a1); lp.push(p1); ra.push(a2); rp.push(p2);
            st.push(parse_tcp_state(p[3])); ino.push(p[9].parse::<u64>().unwrap_or(0));
        }
    }
    Ok(DataFrame::new(vec![
        ser!("local_addr", la), ser!("local_port", lp), ser!("remote_addr", ra),
        ser!("remote_port", rp), ser!("state", st), ser!("inode", ino),
    ])?)
}

  src/plugin/system.rs:27-41 method parse <-> src/plugin/system.rs:537-548 function dir_size
  Similarity: 97.72%

[36m--- src/plugin/system.rs:parse (lines 27-41) ---[0m
    fn parse(&self, cmd: &str, arg: &str) -> Option<Box<dyn Command>> {
        match cmd {
            "ps" => Some(Box::new(SysCmd::Ps)),
            "mounts" => Some(Box::new(SysCmd::Mounts)),
            "tcp" => Some(Box::new(SysCmd::Tcp)),
            "udp" => Some(Box::new(SysCmd::Udp)),
            "env" => Some(Box::new(SysCmd::Env)),
            "systemctl" => Some(Box::new(SysCmd::Systemctl)),
            "pacman" => Some(Box::new(SysCmd::Pacman)),
            "cargo" => Some(Box::new(SysCmd::Cargo)),
            "lsof" => Some(Box::new(Lsof { pid: arg.parse().ok() })),
            "journalctl" => Some(Box::new(Journalctl { n: arg.parse().unwrap_or(1000) })),
            _ => None,
        }
    }

[36m--- src/plugin/system.rs:dir_size (lines 537-548) ---[0m
fn dir_size(path: &std::path::Path) -> u64 {
    fs::read_dir(path).ok().map(|entries| {
        entries.filter_map(|e| e.ok()).map(|e| {
            let m = e.metadata().ok();
            if m.as_ref().map(|m| m.is_dir()).unwrap_or(false) {
                dir_size(&e.path())
            } else {
                m.map(|m| m.len()).unwrap_or(0)
            }
        }).sum()
    }).unwrap_or(0)
}

  src/plugin/system.rs:256-263 function parse_tcp_state <-> src/plugin/system.rs:419-437 function update_ver_cache_bg
  Similarity: 97.09%

[36m--- src/plugin/system.rs:parse_tcp_state (lines 256-263) ---[0m
fn parse_tcp_state(s: &str) -> String {
    match s {
        "01" => "ESTABLISHED", "02" => "SYN_SENT", "03" => "SYN_RECV",
        "04" => "FIN_WAIT1", "05" => "FIN_WAIT2", "06" => "TIME_WAIT",
        "07" => "CLOSE", "08" => "CLOSE_WAIT", "09" => "LAST_ACK",
        "0A" => "LISTEN", "0B" => "CLOSING", _ => "UNKNOWN",
    }.into()
}

[36m--- src/plugin/system.rs:update_ver_cache_bg (lines 419-437) ---[0m
fn update_ver_cache_bg(names: Vec<String>) {
    std::thread::spawn(move || {
        let now = chrono::Utc::now().timestamp();
        let day = 86400;
        let mut cache = load_ver_cache();
        let mut cnt = 0;
        for name in names {
            let stale = cache.get(&name).map(|(_, ts)| now - ts > day).unwrap_or(true);
            if stale {
                if let Some(ver) = fetch_latest(&name) {
                    cache.insert(name, (ver, now));
                    cnt += 1;
                    if cnt % 10 == 0 { save_ver_cache(&cache); }  // save every 10 fetches
                }
            }
        }
        save_ver_cache(&cache);
    });
}

  src/plugin/system.rs:49-63 method exec <-> src/plugin/system.rs:377-389 function load_ver_cache
  Similarity: 93.19%

[36m--- src/plugin/system.rs:exec (lines 49-63) ---[0m
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        let (name, df) = match self {
            SysCmd::Ps => ("ps", ps()?),
            SysCmd::Mounts => ("mounts", mounts()?),
            SysCmd::Tcp => ("tcp", tcp()?),
            SysCmd::Udp => ("udp", udp()?),
            SysCmd::Env => ("env", env()?),
            SysCmd::Systemctl => ("systemctl", systemctl()?),
            SysCmd::Pacman => ("pacman", pacman()?),
            SysCmd::Cargo => ("cargo", cargo()?),
        };
        let id = app.next_id();
        app.stack.push(ViewState::new(id, name.into(), df, None));
        Ok(())
    }

[36m--- src/plugin/system.rs:load_ver_cache (lines 377-389) ---[0m
fn load_ver_cache() -> std::collections::HashMap<String, (String, i64)> {
    let mut cache = std::collections::HashMap::new();
    let path = cargo_cache_path();
    if let Ok(content) = fs::read_to_string(&path) {
        for line in content.lines().skip(1) {  // skip header
            let p: Vec<&str> = line.split(',').collect();
            if p.len() >= 3 {
                cache.insert(p[0].to_string(), (p[1].to_string(), p[2].parse().unwrap_or(0)));
            }
        }
    }
    cache
}

  src/plugin/system.rs:49-63 method exec <-> src/plugin/system.rs:403-416 function fetch_latest
  Similarity: 88.17%

[36m--- src/plugin/system.rs:exec (lines 49-63) ---[0m
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        let (name, df) = match self {
            SysCmd::Ps => ("ps", ps()?),
            SysCmd::Mounts => ("mounts", mounts()?),
            SysCmd::Tcp => ("tcp", tcp()?),
            SysCmd::Udp => ("udp", udp()?),
            SysCmd::Env => ("env", env()?),
            SysCmd::Systemctl => ("systemctl", systemctl()?),
            SysCmd::Pacman => ("pacman", pacman()?),
            SysCmd::Cargo => ("cargo", cargo()?),
        };
        let id = app.next_id();
        app.stack.push(ViewState::new(id, name.into(), df, None));
        Ok(())
    }

[36m--- src/plugin/system.rs:fetch_latest (lines 403-416) ---[0m
fn fetch_latest(name: &str) -> Option<String> {
    use std::os::unix::process::CommandExt;
    let mut cmd = std::process::Command::new("cargo");
    cmd.args(["search", name, "--limit", "1", "--color=never"])
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null());
    // Detach from controlling terminal via new session
    unsafe { cmd.pre_exec(|| { nix::unistd::setsid().ok(); Ok(()) }); }
    cmd.output().ok()
        .map(|o| String::from_utf8_lossy(&o.stdout).to_string())
        .and_then(|s| s.lines().next().map(String::from))
        .and_then(|l| l.split('"').nth(1).map(String::from))
}

  src/plugin/system.rs:64-70 method to_str <-> src/plugin/system.rs:419-437 function update_ver_cache_bg
  Similarity: 97.09%

[36m--- src/plugin/system.rs:to_str (lines 64-70) ---[0m
    fn to_str(&self) -> String {
        match self {
            SysCmd::Ps => "ps", SysCmd::Mounts => "mounts",
            SysCmd::Tcp => "tcp", SysCmd::Udp => "udp", SysCmd::Env => "env",
            SysCmd::Systemctl => "systemctl", SysCmd::Pacman => "pacman", SysCmd::Cargo => "cargo",
        }.into()
    }

[36m--- src/plugin/system.rs:update_ver_cache_bg (lines 419-437) ---[0m
fn update_ver_cache_bg(names: Vec<String>) {
    std::thread::spawn(move || {
        let now = chrono::Utc::now().timestamp();
        let day = 86400;
        let mut cache = load_ver_cache();
        let mut cnt = 0;
        for name in names {
            let stale = cache.get(&name).map(|(_, ts)| now - ts > day).unwrap_or(true);
            if stale {
                if let Some(ver) = fetch_latest(&name) {
                    cache.insert(name, (ver, now));
                    cnt += 1;
                    if cnt % 10 == 0 { save_ver_cache(&cache); }  // save every 10 fetches
                }
            }
        }
        save_ver_cache(&cache);
    });
}

  src/plugin/system.rs:49-63 method exec <-> src/plugin/system.rs:208-220 function mounts
  Similarity: 89.96%

[36m--- src/plugin/system.rs:exec (lines 49-63) ---[0m
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        let (name, df) = match self {
            SysCmd::Ps => ("ps", ps()?),
            SysCmd::Mounts => ("mounts", mounts()?),
            SysCmd::Tcp => ("tcp", tcp()?),
            SysCmd::Udp => ("udp", udp()?),
            SysCmd::Env => ("env", env()?),
            SysCmd::Systemctl => ("systemctl", systemctl()?),
            SysCmd::Pacman => ("pacman", pacman()?),
            SysCmd::Cargo => ("cargo", cargo()?),
        };
        let id = app.next_id();
        app.stack.push(ViewState::new(id, name.into(), df, None));
        Ok(())
    }

[36m--- src/plugin/system.rs:mounts (lines 208-220) ---[0m
fn mounts() -> Result<DataFrame> {
    let (mut devs, mut mps, mut types, mut opts) = (vec![], vec![], vec![], vec![]);
    for line in fs::read_to_string("/proc/mounts")?.lines() {
        let p: Vec<&str> = line.split_whitespace().collect();
        if p.len() >= 4 {
            devs.push(p[0].to_string()); mps.push(p[1].to_string());
            types.push(p[2].to_string()); opts.push(p[3].to_string());
        }
    }
    Ok(DataFrame::new(vec![
        ser!("device", devs), ser!("mount", mps), ser!("type", types), ser!("options", opts),
    ])?)
}

  src/plugin/system.rs:302-317 function systemctl <-> src/plugin/system.rs:377-389 function load_ver_cache
  Similarity: 86.86%

[36m--- src/plugin/system.rs:systemctl (lines 302-317) ---[0m
fn systemctl() -> Result<DataFrame> {
    let text = run_cmd("systemctl", &["list-units", "--type=service", "--all", "--no-pager", "--no-legend"])?;
    let (mut units, mut loads, mut actives, mut subs, mut descs) = (vec![], vec![], vec![], vec![], vec![]);
    for line in text.lines() {
        let p: Vec<&str> = line.split_whitespace().collect();
        if p.len() >= 5 {
            units.push(p[0].to_string()); loads.push(p[1].to_string());
            actives.push(p[2].to_string()); subs.push(p[3].to_string());
            descs.push(p[4..].join(" "));
        }
    }
    Ok(DataFrame::new(vec![
        ser!("unit", units), ser!("load", loads), ser!("active", actives),
        ser!("sub", subs), ser!("description", descs),
    ])?)
}

[36m--- src/plugin/system.rs:load_ver_cache (lines 377-389) ---[0m
fn load_ver_cache() -> std::collections::HashMap<String, (String, i64)> {
    let mut cache = std::collections::HashMap::new();
    let path = cargo_cache_path();
    if let Ok(content) = fs::read_to_string(&path) {
        for line in content.lines().skip(1) {  // skip header
            let p: Vec<&str> = line.split(',').collect();
            if p.len() >= 3 {
                cache.insert(p[0].to_string(), (p[1].to_string(), p[2].parse().unwrap_or(0)));
            }
        }
    }
    cache
}

  src/plugin/system.rs:208-220 function mounts <-> src/plugin/system.rs:229-243 function parse_net
  Similarity: 89.54%

[36m--- src/plugin/system.rs:mounts (lines 208-220) ---[0m
fn mounts() -> Result<DataFrame> {
    let (mut devs, mut mps, mut types, mut opts) = (vec![], vec![], vec![], vec![]);
    for line in fs::read_to_string("/proc/mounts")?.lines() {
        let p: Vec<&str> = line.split_whitespace().collect();
        if p.len() >= 4 {
            devs.push(p[0].to_string()); mps.push(p[1].to_string());
            types.push(p[2].to_string()); opts.push(p[3].to_string());
        }
    }
    Ok(DataFrame::new(vec![
        ser!("device", devs), ser!("mount", mps), ser!("type", types), ser!("options", opts),
    ])?)
}

[36m--- src/plugin/system.rs:parse_net (lines 229-243) ---[0m
fn parse_net(path: &str) -> Result<DataFrame> {
    let (mut la, mut lp, mut ra, mut rp, mut st, mut ino) = (vec![], vec![], vec![], vec![], vec![], vec![]);
    for line in fs::read_to_string(path)?.lines().skip(1) {
        let p: Vec<&str> = line.split_whitespace().collect();
        if p.len() >= 10 {
            let (a1, p1) = parse_addr(p[1]); let (a2, p2) = parse_addr(p[2]);
            la.push(a1); lp.push(p1); ra.push(a2); rp.push(p2);
            st.push(parse_tcp_state(p[3])); ino.push(p[9].parse::<u64>().unwrap_or(0));
        }
    }
    Ok(DataFrame::new(vec![
        ser!("local_addr", la), ser!("local_port", lp), ser!("remote_addr", ra),
        ser!("remote_port", rp), ser!("state", st), ser!("inode", ino),
    ])?)
}

  src/plugin/system.rs:377-389 function load_ver_cache <-> src/plugin/system.rs:403-416 function fetch_latest
  Similarity: 89.43%

[36m--- src/plugin/system.rs:load_ver_cache (lines 377-389) ---[0m
fn load_ver_cache() -> std::collections::HashMap<String, (String, i64)> {
    let mut cache = std::collections::HashMap::new();
    let path = cargo_cache_path();
    if let Ok(content) = fs::read_to_string(&path) {
        for line in content.lines().skip(1) {  // skip header
            let p: Vec<&str> = line.split(',').collect();
            if p.len() >= 3 {
                cache.insert(p[0].to_string(), (p[1].to_string(), p[2].parse().unwrap_or(0)));
            }
        }
    }
    cache
}

[36m--- src/plugin/system.rs:fetch_latest (lines 403-416) ---[0m
fn fetch_latest(name: &str) -> Option<String> {
    use std::os::unix::process::CommandExt;
    let mut cmd = std::process::Command::new("cargo");
    cmd.args(["search", name, "--limit", "1", "--color=never"])
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null());
    // Detach from controlling terminal via new session
    unsafe { cmd.pre_exec(|| { nix::unistd::setsid().ok(); Ok(()) }); }
    cmd.output().ok()
        .map(|o| String::from_utf8_lossy(&o.stdout).to_string())
        .and_then(|s| s.lines().next().map(String::from))
        .and_then(|l| l.split('"').nth(1).map(String::from))
}

  src/plugin/system.rs:294-299 function mem_total <-> src/plugin/system.rs:419-437 function update_ver_cache_bg
  Similarity: 96.34%

[36m--- src/plugin/system.rs:mem_total (lines 294-299) ---[0m
pub fn mem_total() -> u64 {
    fs::read_to_string("/proc/meminfo").ok()
        .and_then(|s| s.lines().find(|l| l.starts_with("MemTotal:"))
            .and_then(|l| l.split_whitespace().nth(1)?.parse::<u64>().ok()).map(|kb| kb * 1024))
        .unwrap_or(8 * 1024 * 1024 * 1024)
}

[36m--- src/plugin/system.rs:update_ver_cache_bg (lines 419-437) ---[0m
fn update_ver_cache_bg(names: Vec<String>) {
    std::thread::spawn(move || {
        let now = chrono::Utc::now().timestamp();
        let day = 86400;
        let mut cache = load_ver_cache();
        let mut cnt = 0;
        for name in names {
            let stale = cache.get(&name).map(|(_, ts)| now - ts > day).unwrap_or(true);
            if stale {
                if let Some(ver) = fetch_latest(&name) {
                    cache.insert(name, (ver, now));
                    cnt += 1;
                    if cnt % 10 == 0 { save_ver_cache(&cache); }  // save every 10 fetches
                }
            }
        }
        save_ver_cache(&cache);
    });
}

  src/plugin/system.rs:208-220 function mounts <-> src/plugin/system.rs:377-389 function load_ver_cache
  Similarity: 92.11%

[36m--- src/plugin/system.rs:mounts (lines 208-220) ---[0m
fn mounts() -> Result<DataFrame> {
    let (mut devs, mut mps, mut types, mut opts) = (vec![], vec![], vec![], vec![]);
    for line in fs::read_to_string("/proc/mounts")?.lines() {
        let p: Vec<&str> = line.split_whitespace().collect();
        if p.len() >= 4 {
            devs.push(p[0].to_string()); mps.push(p[1].to_string());
            types.push(p[2].to_string()); opts.push(p[3].to_string());
        }
    }
    Ok(DataFrame::new(vec![
        ser!("device", devs), ser!("mount", mps), ser!("type", types), ser!("options", opts),
    ])?)
}

[36m--- src/plugin/system.rs:load_ver_cache (lines 377-389) ---[0m
fn load_ver_cache() -> std::collections::HashMap<String, (String, i64)> {
    let mut cache = std::collections::HashMap::new();
    let path = cargo_cache_path();
    if let Ok(content) = fs::read_to_string(&path) {
        for line in content.lines().skip(1) {  // skip header
            let p: Vec<&str> = line.split(',').collect();
            if p.len() >= 3 {
                cache.insert(p[0].to_string(), (p[1].to_string(), p[2].parse().unwrap_or(0)));
            }
        }
    }
    cache
}

  src/plugin/system.rs:365-369 function calc_rsize <-> src/plugin/system.rs:419-437 function update_ver_cache_bg
  Similarity: 96.67%

[36m--- src/plugin/system.rs:calc_rsize (lines 365-369) ---[0m
fn calc_rsize(name: &str, size: u64, deps: &[String], pkg_size: &std::collections::HashMap<String, u64>, pkg_req_by: &std::collections::HashMap<String, Vec<String>>) -> u64 {
    deps.iter().fold(size, |acc, dep| {
        acc + pkg_req_by.get(dep).filter(|r| r.len() == 1 && r[0] == name).map(|_| pkg_size.get(dep).copied().unwrap_or(0)).unwrap_or(0)
    })
}

[36m--- src/plugin/system.rs:update_ver_cache_bg (lines 419-437) ---[0m
fn update_ver_cache_bg(names: Vec<String>) {
    std::thread::spawn(move || {
        let now = chrono::Utc::now().timestamp();
        let day = 86400;
        let mut cache = load_ver_cache();
        let mut cnt = 0;
        for name in names {
            let stale = cache.get(&name).map(|(_, ts)| now - ts > day).unwrap_or(true);
            if stale {
                if let Some(ver) = fetch_latest(&name) {
                    cache.insert(name, (ver, now));
                    cnt += 1;
                    if cnt % 10 == 0 { save_ver_cache(&cache); }  // save every 10 fetches
                }
            }
        }
        save_ver_cache(&cache);
    });
}

  src/plugin/system.rs:27-41 method parse <-> src/plugin/system.rs:256-263 function parse_tcp_state
  Similarity: 98.05%

[36m--- src/plugin/system.rs:parse (lines 27-41) ---[0m
    fn parse(&self, cmd: &str, arg: &str) -> Option<Box<dyn Command>> {
        match cmd {
            "ps" => Some(Box::new(SysCmd::Ps)),
            "mounts" => Some(Box::new(SysCmd::Mounts)),
            "tcp" => Some(Box::new(SysCmd::Tcp)),
            "udp" => Some(Box::new(SysCmd::Udp)),
            "env" => Some(Box::new(SysCmd::Env)),
            "systemctl" => Some(Box::new(SysCmd::Systemctl)),
            "pacman" => Some(Box::new(SysCmd::Pacman)),
            "cargo" => Some(Box::new(SysCmd::Cargo)),
            "lsof" => Some(Box::new(Lsof { pid: arg.parse().ok() })),
            "journalctl" => Some(Box::new(Journalctl { n: arg.parse().unwrap_or(1000) })),
            _ => None,
        }
    }

[36m--- src/plugin/system.rs:parse_tcp_state (lines 256-263) ---[0m
fn parse_tcp_state(s: &str) -> String {
    match s {
        "01" => "ESTABLISHED", "02" => "SYN_SENT", "03" => "SYN_RECV",
        "04" => "FIN_WAIT1", "05" => "FIN_WAIT2", "06" => "TIME_WAIT",
        "07" => "CLOSE", "08" => "CLOSE_WAIT", "09" => "LAST_ACK",
        "0A" => "LISTEN", "0B" => "CLOSING", _ => "UNKNOWN",
    }.into()
}

  src/plugin/system.rs:49-63 method exec <-> src/plugin/system.rs:349-357 function parse_date
  Similarity: 89.34%

[36m--- src/plugin/system.rs:exec (lines 49-63) ---[0m
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        let (name, df) = match self {
            SysCmd::Ps => ("ps", ps()?),
            SysCmd::Mounts => ("mounts", mounts()?),
            SysCmd::Tcp => ("tcp", tcp()?),
            SysCmd::Udp => ("udp", udp()?),
            SysCmd::Env => ("env", env()?),
            SysCmd::Systemctl => ("systemctl", systemctl()?),
            SysCmd::Pacman => ("pacman", pacman()?),
            SysCmd::Cargo => ("cargo", cargo()?),
        };
        let id = app.next_id();
        app.stack.push(ViewState::new(id, name.into(), df, None));
        Ok(())
    }

[36m--- src/plugin/system.rs:parse_date (lines 349-357) ---[0m
fn parse_date(s: &str) -> String {
    let p: Vec<&str> = s.split_whitespace().collect();
    if p.len() < 5 { return s.into(); }
    let mon = match p[1] {
        "Jan" => "01", "Feb" => "02", "Mar" => "03", "Apr" => "04", "May" => "05", "Jun" => "06",
        "Jul" => "07", "Aug" => "08", "Sep" => "09", "Oct" => "10", "Nov" => "11", "Dec" => "12", _ => return s.into(),
    };
    format!("{}-{}-{:02}", p[4], mon, p[2].parse::<u32>().unwrap_or(0))
}

  src/plugin/system.rs:294-299 function mem_total <-> src/plugin/system.rs:537-548 function dir_size
  Similarity: 96.62%

[36m--- src/plugin/system.rs:mem_total (lines 294-299) ---[0m
pub fn mem_total() -> u64 {
    fs::read_to_string("/proc/meminfo").ok()
        .and_then(|s| s.lines().find(|l| l.starts_with("MemTotal:"))
            .and_then(|l| l.split_whitespace().nth(1)?.parse::<u64>().ok()).map(|kb| kb * 1024))
        .unwrap_or(8 * 1024 * 1024 * 1024)
}

[36m--- src/plugin/system.rs:dir_size (lines 537-548) ---[0m
fn dir_size(path: &std::path::Path) -> u64 {
    fs::read_dir(path).ok().map(|entries| {
        entries.filter_map(|e| e.ok()).map(|e| {
            let m = e.metadata().ok();
            if m.as_ref().map(|m| m.is_dir()).unwrap_or(false) {
                dir_size(&e.path())
            } else {
                m.map(|m| m.len()).unwrap_or(0)
            }
        }).sum()
    }).unwrap_or(0)
}

  src/plugin/system.rs:77-83 method exec <-> src/plugin/system.rs:377-389 function load_ver_cache
  Similarity: 85.19%

[36m--- src/plugin/system.rs:exec (lines 77-83) ---[0m
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        let df = lsof(self.pid)?;
        let name = self.pid.map(|p| format!("lsof:{}", p)).unwrap_or("lsof".into());
        let id = app.next_id();
        app.stack.push(ViewState::new(id, name, df, None));
        Ok(())
    }

[36m--- src/plugin/system.rs:load_ver_cache (lines 377-389) ---[0m
fn load_ver_cache() -> std::collections::HashMap<String, (String, i64)> {
    let mut cache = std::collections::HashMap::new();
    let path = cargo_cache_path();
    if let Ok(content) = fs::read_to_string(&path) {
        for line in content.lines().skip(1) {  // skip header
            let p: Vec<&str> = line.split(',').collect();
            if p.len() >= 3 {
                cache.insert(p[0].to_string(), (p[1].to_string(), p[2].parse().unwrap_or(0)));
            }
        }
    }
    cache
}

  src/plugin/system.rs:365-369 function calc_rsize <-> src/plugin/system.rs:537-548 function dir_size
  Similarity: 96.62%

[36m--- src/plugin/system.rs:calc_rsize (lines 365-369) ---[0m
fn calc_rsize(name: &str, size: u64, deps: &[String], pkg_size: &std::collections::HashMap<String, u64>, pkg_req_by: &std::collections::HashMap<String, Vec<String>>) -> u64 {
    deps.iter().fold(size, |acc, dep| {
        acc + pkg_req_by.get(dep).filter(|r| r.len() == 1 && r[0] == name).map(|_| pkg_size.get(dep).copied().unwrap_or(0)).unwrap_or(0)
    })
}

[36m--- src/plugin/system.rs:dir_size (lines 537-548) ---[0m
fn dir_size(path: &std::path::Path) -> u64 {
    fs::read_dir(path).ok().map(|entries| {
        entries.filter_map(|e| e.ok()).map(|e| {
            let m = e.metadata().ok();
            if m.as_ref().map(|m| m.is_dir()).unwrap_or(false) {
                dir_size(&e.path())
            } else {
                m.map(|m| m.len()).unwrap_or(0)
            }
        }).sum()
    }).unwrap_or(0)
}

  src/plugin/system.rs:340-346 function parse_size <-> src/plugin/system.rs:349-357 function parse_date
  Similarity: 91.62%

[36m--- src/plugin/system.rs:parse_size (lines 340-346) ---[0m
fn parse_size(s: &str) -> u64 {
    let p: Vec<&str> = s.split_whitespace().collect();
    if p.len() != 2 { return 0; }
    let n: f64 = p[0].parse().unwrap_or(0.0);
    let m: f64 = match p[1] { "B" => 1.0, "KiB" => 1024.0, "MiB" => 1048576.0, "GiB" => 1073741824.0, _ => 1.0 };
    (n * m) as u64
}

[36m--- src/plugin/system.rs:parse_date (lines 349-357) ---[0m
fn parse_date(s: &str) -> String {
    let p: Vec<&str> = s.split_whitespace().collect();
    if p.len() < 5 { return s.into(); }
    let mon = match p[1] {
        "Jan" => "01", "Feb" => "02", "Mar" => "03", "Apr" => "04", "May" => "05", "Jun" => "06",
        "Jul" => "07", "Aug" => "08", "Sep" => "09", "Oct" => "10", "Nov" => "11", "Dec" => "12", _ => return s.into(),
    };
    format!("{}-{}-{:02}", p[4], mon, p[2].parse::<u32>().unwrap_or(0))
}

  src/plugin/system.rs:256-263 function parse_tcp_state <-> src/plugin/system.rs:365-369 function calc_rsize
  Similarity: 94.45%

[36m--- src/plugin/system.rs:parse_tcp_state (lines 256-263) ---[0m
fn parse_tcp_state(s: &str) -> String {
    match s {
        "01" => "ESTABLISHED", "02" => "SYN_SENT", "03" => "SYN_RECV",
        "04" => "FIN_WAIT1", "05" => "FIN_WAIT2", "06" => "TIME_WAIT",
        "07" => "CLOSE", "08" => "CLOSE_WAIT", "09" => "LAST_ACK",
        "0A" => "LISTEN", "0B" => "CLOSING", _ => "UNKNOWN",
    }.into()
}

[36m--- src/plugin/system.rs:calc_rsize (lines 365-369) ---[0m
fn calc_rsize(name: &str, size: u64, deps: &[String], pkg_size: &std::collections::HashMap<String, u64>, pkg_req_by: &std::collections::HashMap<String, Vec<String>>) -> u64 {
    deps.iter().fold(size, |acc, dep| {
        acc + pkg_req_by.get(dep).filter(|r| r.len() == 1 && r[0] == name).map(|_| pkg_size.get(dep).copied().unwrap_or(0)).unwrap_or(0)
    })
}

  src/plugin/system.rs:64-70 method to_str <-> src/plugin/system.rs:365-369 function calc_rsize
  Similarity: 92.65%

[36m--- src/plugin/system.rs:to_str (lines 64-70) ---[0m
    fn to_str(&self) -> String {
        match self {
            SysCmd::Ps => "ps", SysCmd::Mounts => "mounts",
            SysCmd::Tcp => "tcp", SysCmd::Udp => "udp", SysCmd::Env => "env",
            SysCmd::Systemctl => "systemctl", SysCmd::Pacman => "pacman", SysCmd::Cargo => "cargo",
        }.into()
    }

[36m--- src/plugin/system.rs:calc_rsize (lines 365-369) ---[0m
fn calc_rsize(name: &str, size: u64, deps: &[String], pkg_size: &std::collections::HashMap<String, u64>, pkg_req_by: &std::collections::HashMap<String, Vec<String>>) -> u64 {
    deps.iter().fold(size, |acc, dep| {
        acc + pkg_req_by.get(dep).filter(|r| r.len() == 1 && r[0] == name).map(|_| pkg_size.get(dep).copied().unwrap_or(0)).unwrap_or(0)
    })
}

  src/plugin/system.rs:294-299 function mem_total <-> src/plugin/system.rs:365-369 function calc_rsize
  Similarity: 94.83%

[36m--- src/plugin/system.rs:mem_total (lines 294-299) ---[0m
pub fn mem_total() -> u64 {
    fs::read_to_string("/proc/meminfo").ok()
        .and_then(|s| s.lines().find(|l| l.starts_with("MemTotal:"))
            .and_then(|l| l.split_whitespace().nth(1)?.parse::<u64>().ok()).map(|kb| kb * 1024))
        .unwrap_or(8 * 1024 * 1024 * 1024)
}

[36m--- src/plugin/system.rs:calc_rsize (lines 365-369) ---[0m
fn calc_rsize(name: &str, size: u64, deps: &[String], pkg_size: &std::collections::HashMap<String, u64>, pkg_req_by: &std::collections::HashMap<String, Vec<String>>) -> u64 {
    deps.iter().fold(size, |acc, dep| {
        acc + pkg_req_by.get(dep).filter(|r| r.len() == 1 && r[0] == name).map(|_| pkg_size.get(dep).copied().unwrap_or(0)).unwrap_or(0)
    })
}

  src/plugin/system.rs:246-253 function parse_addr <-> src/plugin/system.rs:288-291 function env
  Similarity: 85.18%

[36m--- src/plugin/system.rs:parse_addr (lines 246-253) ---[0m
fn parse_addr(s: &str) -> (String, u32) {
    let p: Vec<&str> = s.split(':').collect();
    if p.len() == 2 {
        let a = u32::from_str_radix(p[0], 16).unwrap_or(0);
        let port = u32::from_str_radix(p[1], 16).unwrap_or(0);
        (format!("{}.{}.{}.{}", a & 0xff, (a >> 8) & 0xff, (a >> 16) & 0xff, (a >> 24) & 0xff), port)
    } else { (String::new(), 0) }
}

[36m--- src/plugin/system.rs:env (lines 288-291) ---[0m
fn env() -> Result<DataFrame> {
    let (names, vals): (Vec<String>, Vec<String>) = std::env::vars().unzip();
    Ok(DataFrame::new(vec![ser!("name", names), ser!("value", vals)])?)
}

  src/plugin/system.rs:256-263 function parse_tcp_state <-> src/plugin/system.rs:360-362 function parse_deps
  Similarity: 92.58%

[36m--- src/plugin/system.rs:parse_tcp_state (lines 256-263) ---[0m
fn parse_tcp_state(s: &str) -> String {
    match s {
        "01" => "ESTABLISHED", "02" => "SYN_SENT", "03" => "SYN_RECV",
        "04" => "FIN_WAIT1", "05" => "FIN_WAIT2", "06" => "TIME_WAIT",
        "07" => "CLOSE", "08" => "CLOSE_WAIT", "09" => "LAST_ACK",
        "0A" => "LISTEN", "0B" => "CLOSING", _ => "UNKNOWN",
    }.into()
}

[36m--- src/plugin/system.rs:parse_deps (lines 360-362) ---[0m
fn parse_deps(v: &str) -> Vec<String> {
    if v == "None" { vec![] } else { v.split_whitespace().map(|s| s.split(&['<','>','='][..]).next().unwrap_or(s).to_string()).collect() }
}

  src/plugin/system.rs:64-70 method to_str <-> src/plugin/system.rs:360-362 function parse_deps
  Similarity: 89.55%

[36m--- src/plugin/system.rs:to_str (lines 64-70) ---[0m
    fn to_str(&self) -> String {
        match self {
            SysCmd::Ps => "ps", SysCmd::Mounts => "mounts",
            SysCmd::Tcp => "tcp", SysCmd::Udp => "udp", SysCmd::Env => "env",
            SysCmd::Systemctl => "systemctl", SysCmd::Pacman => "pacman", SysCmd::Cargo => "cargo",
        }.into()
    }

[36m--- src/plugin/system.rs:parse_deps (lines 360-362) ---[0m
fn parse_deps(v: &str) -> Vec<String> {
    if v == "None" { vec![] } else { v.split_whitespace().map(|s| s.split(&['<','>','='][..]).next().unwrap_or(s).to_string()).collect() }
}

  src/plugin/system.rs:20-23 method matches <-> src/plugin/system.rs:365-369 function calc_rsize
  Similarity: 86.50%

[36m--- src/plugin/system.rs:matches (lines 20-23) ---[0m
    fn matches(&self, name: &str) -> bool {
        matches!(name, "ps" | "mounts" | "tcp" | "udp" | "env" | "systemctl" | "pacman" | "cargo")
            || name.starts_with("lsof") || name.starts_with("journalctl")
    }

[36m--- src/plugin/system.rs:calc_rsize (lines 365-369) ---[0m
fn calc_rsize(name: &str, size: u64, deps: &[String], pkg_size: &std::collections::HashMap<String, u64>, pkg_req_by: &std::collections::HashMap<String, Vec<String>>) -> u64 {
    deps.iter().fold(size, |acc, dep| {
        acc + pkg_req_by.get(dep).filter(|r| r.len() == 1 && r[0] == name).map(|_| pkg_size.get(dep).copied().unwrap_or(0)).unwrap_or(0)
    })
}

  src/plugin/system.rs:294-299 function mem_total <-> src/plugin/system.rs:360-362 function parse_deps
  Similarity: 85.85%

[36m--- src/plugin/system.rs:mem_total (lines 294-299) ---[0m
pub fn mem_total() -> u64 {
    fs::read_to_string("/proc/meminfo").ok()
        .and_then(|s| s.lines().find(|l| l.starts_with("MemTotal:"))
            .and_then(|l| l.split_whitespace().nth(1)?.parse::<u64>().ok()).map(|kb| kb * 1024))
        .unwrap_or(8 * 1024 * 1024 * 1024)
}

[36m--- src/plugin/system.rs:parse_deps (lines 360-362) ---[0m
fn parse_deps(v: &str) -> Vec<String> {
    if v == "None" { vec![] } else { v.split_whitespace().map(|s| s.split(&['<','>','='][..]).next().unwrap_or(s).to_string()).collect() }
}

  src/plugin/system.rs:105-108 function run_cmd <-> src/plugin/system.rs:288-291 function env
  Similarity: 88.89%

[36m--- src/plugin/system.rs:run_cmd (lines 105-108) ---[0m
fn run_cmd(cmd: &str, args: &[&str]) -> Result<String> {
    let out = std::process::Command::new(cmd).args(args).output()?;
    Ok(String::from_utf8_lossy(&out.stdout).into())
}

[36m--- src/plugin/system.rs:env (lines 288-291) ---[0m
fn env() -> Result<DataFrame> {
    let (names, vals): (Vec<String>, Vec<String>) = std::env::vars().unzip();
    Ok(DataFrame::new(vec![ser!("name", names), ser!("value", vals)])?)
}


Total duplicate pairs found: 225
