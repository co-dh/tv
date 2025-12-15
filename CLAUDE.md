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
- search the code for /tmp in test code. save test file in tests/data/ and put in git instead.
- if I filter by Exchange = 'P', and it can display only 10k rows, but it should report total rows in status bar of the filtered view of disk file.
- if I filter again, it should create a new view with additional filter, not filter the in memory 10k row. same for frequence.
- add key test for them.
