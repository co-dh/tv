# VisiData vs tv Feature Comparison

## VisiData has, tv doesn't:

| Feature                  | VisiData                                        | tv                |
|---------                 |----------                                       |-----              |
| **Formats**              | Excel, JSON, SQLite, HDF5, XML, YAML, Shapefile | CSV, Parquet, .gz |
| **Pivot tables**         | ✓                                               | ✗                 |
| **Join datasets**        | ✓                                               | ✗                 |
| **Graphing/charts**      | ✓                                               | ✗                 |
| **Cell editing**         | ✓                                               | ✗                 |
| **Python expressions**   | ✓                                               | ✗                 |
| **Macro recording**      | ✓                                               | ✗                 |
| **Session save/restore** | ✓                                               | ✗                 |
| **Split view**           | ✓                                               | ✗                 |
| **Nested data expand**   | ✓                                               | ✗                 |
| **Plugin system**        | ✓                                               | ✗                 |
| **API integrations**     | S3, Reddit, JIRA                                | ✗                 |

## tv has, VisiData doesn't:

| Feature                | tv                                      | VisiData  |
|---------               |-----                                    |---------- |
| **System commands**    | ps, lsof, systemctl, journalctl, pacman | ✗         |
| **Lazy parquet**       | SQL on disk, no memory load             | loads all |
| **Correlation matrix** | ✓                                       | ✗         |
| **Folder browser**     | ls/lr with delete                       | basic     |

## Key gaps

Excel/JSON loading, pivot tables, joins, charts, cell editing.

## Nested JSON Handling

VisiData's approach to nested/hierarchical JSON:

### Key commands

| Key      | Command      | Effect                                           |
|----------|--------------|--------------------------------------------------|
| `(`      | expand-col   | Flatten nested dict one level → `c.d`, `c.e`     |
| `)`      | contract-col | Collapse back                                    |
| `z(` + 0 | deep expand  | Fully flatten column recursively                 |
| `gz(`    | expand all   | Flatten all nested columns                       |
| `zM`     | unfurl-col   | Row-wise expand list → one row per item          |
| `z^Y`    | pyobj-cell   | Explore single cell as table                     |
| `t`      | transpose    | Useful for object with many keys                 |

### Example

```json
{"user": {"name": "bob", "age": 30}, "tags": ["a", "b"]}
```

1. Load → shows `user` and `tags` as collapsed columns
2. Move to `user`, press `(` → expands to `user.name`, `user.age`
3. Move to `tags`, press `zM` → creates 2 rows (one per tag)

### Limitation

Expansion only sees keys/items in **current row**. If row 1 has `{a: 1}` and row 2 has `{a: 1, b: 2}`, expanding on row 1 only creates column `a`.

## Sources

- https://www.visidata.org/
- https://www.visidata.org/formats/
- https://github.com/saulpw/visidata/discussions/1605
- https://github.com/saulpw/visidata/issues/843
