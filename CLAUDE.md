# Project Instructions
- remember my approval
- add test case for user requirements and bug fix
- use ./tmp instead of /tmp, you have approve to read/write at ./tmp

# Requirements
See @require.md for detailed requirements

# Todo
- Investigate nushell plugin architecture - modularize view functionality into single files for dynamic add/remove
- For table with keyed columns, Meta should show stats grouped by key columns

# Done
- Performance: Large CSV scrolling responsive, stats caching
- xkey command: moves cols to front, separator bar, `!` key shortcut
- Freq with key columns: groups by key cols + target
- Meta view Enter: pop and xkey selected, or focus column
- Meta/Freq status bar: shows parent name and row count
- Command history: logs to ~/.tv/history
- Delete feedback: shows N columns deleted
- View handler refactor: Meta, Freq, Folder handlers
- Save command: background streaming, wait in script mode
- Type conversion: lossless, leading zeros for TAQ time
- Background save status: channel updates, status bar
- Multi-parquet/glob support: scan_parquet with patterns
- Forth functions: cfg/funcs.4th, sel_null, sel_single, sel_rows
- Info box: grouped by category, no nav commands
- Command naming: underscore style (del_col, goto_col)
- Folder view: Enter navigates directories
- TAQ time: parquet save converts int to time
