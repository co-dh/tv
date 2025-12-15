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
- in filtered view of lazy polars, display the total row count of the filtered view, with something like select count(*) from t where filter. use the backend's sql interface.
- same in filtered view, when you run frequence on another column, it should just run against the disk version, not the memory. use sql interface.
- since the in memory dataframe also support sql interface, you should do the same to it. so one code path for both.
- add key play test for each of these. you can set the limit to small to test it.

