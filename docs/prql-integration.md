# PRQL Integration Notes

## Key Learnings

### 1. `from` Statement
- PRQL `from "filepath"` causes internal compiler error (#4317)
- Use `from df` instead - the table name is passed separately to the SQL engine
- Path is provided to the plugin which loads it as `df`

### 2. Filter Syntax
- PRQL uses `==` for equality comparison, not SQL's `=`
- Must convert user input: `city = 'NYC'` → `city == 'NYC'`
- Preserve compound operators: `!=`, `>=`, `<=`, `~=`

### 3. Select Syntax
- PRQL select uses curly braces: `select {col1, col2}`
- Column names with special chars need backticks: `` select {`name`, `b`} ``

### 4. Derive (Computed Columns)
- PRQL: `derive {new_col = existing_col}`
- For rename: derive new name, then select without old name

### 5. Pagination with `take`
- PRQL has NO `skip` command - it doesn't exist
- Use `take start..end` with 1-based positive ranges
- For offset 0, limit 10: `take 1..11`
- For offset 5, limit 10: `take 6..16`
- Formula: `take (offset+1)..(offset+limit+1)`
- Error "Unknown name `skip`" means you used wrong syntax
- Error "expected a positive int range" means start must be >= 1

### 6. Compilation
```rust
let opts = prqlc::Options::default().no_format();
prqlc::compile(prql, &opts).ok()
```
- `no_format()` returns compact SQL on single line
- Returns `Result<String, Error>` - use `.ok()` to convert to Option

### 7. SQL Output
PRQL compiles to standard SQL:
```
from df | filter city == 'NYC' | select {name, value}
→ SELECT name, value FROM df WHERE city = 'NYC'
```

## Architecture

1. Views store PRQL expressions (`view.prql`)
2. Commands append to PRQL chain: `from df | filter ... | select ...`
3. On print/display, compile PRQL → SQL → execute via plugin
4. Plugin handles file loading and query execution

### 8. PRQL Functions
- Define with `let name = func args tbl <relation> -> (pipeline)`
- Functions can take column refs and table as args
- Example: `let freq = func c tbl <relation> -> (from tbl | group {c} (aggregate {Cnt = count this}) | sort {-Cnt})`
- Call: `from df | freq name` or `from df | freq {col1, col2}` for multi-column
- Functions are prepended to queries via `funcs.prql` file

### 9. Reserved Keyword Column Names
- Columns named `count`, `select`, `from` etc conflict with PRQL keywords
- Backticks alone don't help: `` `count` `` still conflicts
- Solution: use `this.column` syntax: `this.count`, `this.select`
- Works in function calls: `from df | meta this.count`

### 10. S-Strings for Raw SQL
- Use `s"SQL expression"` to embed raw SQL
- Column refs interpolate: `s"COUNT({c})"` → `COUNT(colname)`
- Useful when PRQL lacks a feature (e.g., COUNT(column) for non-null count)

## Gotchas

- Empty `select {}` compiles to `SELECT NULL` - initialize cols list first
- PRQL expects boolean for filter, not assignment
- Backticks required for column names with spaces or special chars
- `count col` gives `COUNT(*)` not `COUNT(col)` - use `s"COUNT({col})"` for non-null count
- Reserved words as columns: use `this.keyword` not `` `keyword` ``
