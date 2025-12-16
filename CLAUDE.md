# Project Instructions
- remember my approval
- add test case for user requirements and bug fix
- use ./tmp instead of /tmp, you have approval to read/write at ./tmp
- ask for approval if delete unit test that covers requirements.
- do not remove or change test case just to fit the code. ask for approval of changing tests.
- For bugs, implement a test to catch it first, then fix.
- use short module imports: `use crate::foo::bar;` then `bar::func()`, not `crate::foo::bar::func()`
- use sql if possible, instead of polars api. freq e.g. unify
- unify similar funciton like freq and freq_where, the former is just empyt where condition.

# Architecture
## Source: csv, parquet on disk, odbc, .gz
## operation on source: head 100000, distinct value of column a, Frequence of column, meta, filter, search, row count, column stats
## small csv can load into memory and will support all above operations, same for lazy parquet, database connection.
## big files may implement empty operations due to memory or performance. why would you do frequence on a billion row csv?
## Do not repeat yourself. Not just on the syntax level, but also conceptually. Do not hold a in memory copy of parquet, but use a lazy frame.

# Todo
- add comments to each functions, to newbie rust programmer but know c++.
- implement all busybox command that are has a table output. each command view should have their own special command, like ps view has kill, kill -9, start strace,
- analysis cargo package, remove unnecessary dependencies. find out big dependency introducer.
- use | syntax in test script.  modify tests/test_string_filter.sh to use simplified interface.
- what are 2 impl Viewstate in state.rs?
