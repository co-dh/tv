# Nushell vs tv Feature Comparison

## Nushell has, tv doesn't:

| Feature                  | Nushell                              | tv                 |
|--------------------------|--------------------------------------|---------------------|
| **Shell integration**    | Full shell with pipes, scripts       | Standalone viewer   |
| **~350 built-in commands** | ls, ps, http, etc all return tables | Limited OS commands |
| **group-by**             | Group rows by column values          | ✗                   |
| **transpose**            | Flip rows/columns                    | ✗                   |
| **flatten/expand**       | Nested data handling                 | ✗                   |
| **merge/append**         | Join tables horizontally/vertically  | ✗                   |
| **reduce**               | Custom aggregation closures          | ✗                   |
| **each/map**             | Apply function to each row           | ✗                   |
| **insert/update**        | Add/modify columns with expressions  | ✗                   |
| **Built-in SQLite**      | `stor` commands for in-memory DB     | ✗                   |
| **JSON/YAML/TOML**       | Native structured data formats       | ✗                   |
| **Scripting**            | Full programming language            | ✗                   |
| **Type system**          | int, str, datetime, duration         | Basic types         |

## tv has, Nushell doesn't:

| Feature                | tv                               | Nushell          |
|------------------------|----------------------------------|------------------|
| **Lazy parquet**       | SQL on disk, GB files            | Loads into memory |
| **Freq tables**        | One-key frequency analysis       | Manual group-by   |
| **Correlation matrix** | ✓                                | Manual            |
| **Column stats**       | min/max/mean/null% inline        | Manual            |
| **Metadata view**      | Column type/null/distinct summary | ✗                |
| **Interactive TUI**    | Cursor navigation, vim keys      | Print to stdout   |

## Key gaps

group-by, transpose, merge/join, scripting/expressions, JSON/YAML formats.

## Sources

- https://www.nushell.sh/book/working_with_tables.html
- https://www.nushell.sh/commands/categories/filters.html
