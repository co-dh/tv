# Code Review (AW style)

**Bloat identified:**

1. **9 ViewState constructors** - `new`, `new_parquet`, `new_gz`, `new_meta`, `new_freq`...
   - Fix: One `ViewState::new(kind, opts)` with `ViewOpts` struct or builder
   - 50+ lines → 15 lines

2. **22 Command impls** - each struct+impl is 10-20 lines boilerplate
   - Pattern: `exec` appends to PRQL, `to_str` formats
   - Fix: Macro or enum dispatch
   ```rust
   enum Cmd { Filter(String), Sort(String,bool), Take(usize), ... }
   impl Cmd { fn exec(&self, app) { match self { ... } } }
   ```
   - 400 lines → 100 lines

3. **renderer.rs 971 lines** - `render_headers_xs` and `render_row_xs` share 80% logic
   - Fix: `render_cells(frame, cells, xs, style)` unified
   - `render_info_box` and `render_commands_box` same structure
   - 971 → 400 lines

4. **keymap.rs 289 lines** - static data as code
   - Fix: Load from config or const array, not match arms
   - 289 → 50 lines

5. **Plugin trait** - `parse`, `handle`, `matches` repeated across 6 plugins
   - Most return None or simple patterns
   - Fix: Default impls, declarative registration

**Quick wins:**
```rust
// Before (transform.rs pattern repeated 11x)
pub struct Filter { pub expr: String }
impl Command for Filter {
    fn exec(&mut self, app: &mut AppContext) -> Result<()> { ... }
    fn to_str(&self) -> String { format!("filter {}", self.expr) }
}

// After - one enum, one match
fn exec(cmd: Cmd, app: &mut App) -> Result<()> {
    let v = app.req_mut()?;
    v.prql = match cmd {
        Cmd::Filter(e) => format!("{} | filter {}", v.prql, e),
        Cmd::Sort(c,d) => pure::append_sort(&v.prql, &c, d),
        Cmd::Take(n)   => format!("{} | take {}", v.prql, n),
        ...
    };
    Ok(())
}
```

**Ratio:** 5093 lines → ~2500 lines possible. 50% cut.
