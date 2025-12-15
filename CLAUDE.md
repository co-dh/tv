# Project Instructions
- remember my approval
- add test case for user requirements and bug fix
- use ./tmp instead of /tmp, you have approval to read/write at ./tmp
- ask for approval if delete unit test that covers requirements.
- do not remove or change test case just to fit the code. ask for approval of changing tests.
- For bugs, implement a test to catch it first, then fix.
- use short module imports: `use crate::foo::bar;` then `bar::func()`, not `crate::foo::bar::func()`

# Architecture
## Source: csv, parquet on disk, odbc, .gz
## operation on source: head 100000, distinct value of column a, Frequence of column, meta, filter, search, row count, column stats
## small csv can load into memory and will support all above operations, same for lazy parquet, database connection.
## big files may implement empty operations due to memory or performance. why would you do frequence on a billion row csv?
## Do not repeat yourself. Not just on the syntax level, but also conceptually. Do not hold a in memory copy of parquet, but use a lazy frame.

# Todo
all the operation on lazy parquet need to against disk, not memory df. in fact, the stack top should be the lazyframe.
- add comments to each functions, to newbie rust programmer but know c++.

- Fix Frequency Enter for Polars backend
- make interactive test.
- Add backend × operation interactive test matrix (exclude gz)
- Add comments to all functions for newbie Rust programmers
- Fix sort on Folder view, add test
- Fix Frequency on Folder view, add test
- Sort on parquet should use sort_head to avoid OOM
- you don't want override keymap, as that will break macro your recorded with keys. it's just a mapping from user's key to our key.
- when filter, do not load filtered result into memory. you should limit to 10k rows. this is actually like a database view.
