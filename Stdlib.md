# Stdlib Author Review

## 1. Newtype Patterns for Type Safety

```rust
// Current: raw usize for IDs, rows, columns
pub id: usize,
pub cr: usize,  // cursor row
pub cc: usize,  // cursor col

// Better: distinct types prevent mixing
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ViewId(pub usize);

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct Row(pub usize);

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct Col(pub usize);

// Enables: impl Add<usize> for Row, saturating_sub, etc.
```

## 2. Use Cow<str> for Zero-Copy Where Possible

```rust
// Current: always allocates
pub fn col_name(&self, idx: usize) -> Option<String>

// Better: borrow when possible
pub fn col_name(&self, idx: usize) -> Option<Cow<'_, str>>

// Also for prql, name fields that are often just references
pub prql: Cow<'static, str>,
```

## 3. Custom Error Type with thiserror

```rust
// Current: anyhow everywhere
use anyhow::Result;

// Better: domain-specific errors
#[derive(Debug, thiserror::Error)]
pub enum TvError {
    #[error("no table loaded")]
    NoTable,
    #[error("column '{0}' not found")]
    ColumnNotFound(String),
    #[error("invalid filter: {0}")]
    InvalidFilter(String),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("polars error: {0}")]
    Polars(#[from] polars::error::PolarsError),
}
```

## 4. Implement Standard Traits

```rust
// StateStack should implement Index/IndexMut
impl std::ops::Index<usize> for StateStack {
    type Output = ViewState;
    fn index(&self, idx: usize) -> &Self::Output { &self.stack[idx] }
}

// ViewState could implement Deref to DataFrame for convenience
impl std::ops::Deref for ViewState {
    type Target = DataFrame;
    fn deref(&self) -> &Self::Target { &self.dataframe }
}

// StateStack should implement IntoIterator
impl<'a> IntoIterator for &'a StateStack {
    type Item = &'a ViewState;
    type IntoIter = std::slice::Iter<'a, ViewState>;
    fn into_iter(self) -> Self::IntoIter { self.stack.iter() }
}
```

## 5. Use Associated Types in Traits

```rust
// Current: returns Box<dyn Source>
pub fn source(&self) -> Box<dyn Source + '_>

// Better with GAT or enum dispatch
pub enum SourceKind<'a> {
    Polars(Polars),
    Memory(Memory<'a>),
    Gz(Gz<'a>),
}

// Avoids allocation, enables inlining
```

## 6. Builder Pattern with Typestate

```rust
// Current: many constructors with similar patterns
pub fn new_meta(...) -> Self
pub fn new_freq(...) -> Self

// Better: builder with compile-time guarantees
pub struct ViewBuilder<S = Initial> {
    id: usize,
    name: String,
    state: PhantomData<S>,
}

impl ViewBuilder<Initial> {
    pub fn table(self) -> ViewBuilder<Table> { ... }
    pub fn meta(self, parent: &ViewState) -> ViewBuilder<Meta> { ... }
}

impl ViewBuilder<Table> {
    pub fn build(self, df: DataFrame) -> ViewState { ... }
}
```

## 7. Const Generics for Fixed Sizes

```rust
// Current: magic numbers
let vis = (self.viewport.0 as usize).saturating_sub(3);  // header + footer + status

// Better: const generic
const RESERVED_ROWS: usize = 3;  // header + footer_header + status

pub fn visible_rows(&self) -> usize {
    (self.viewport.0 as usize).saturating_sub(RESERVED_ROWS)
}
```

## 8. Implement Display for Key Types

```rust
impl std::fmt::Display for ViewKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Table => write!(f, "table"),
            Self::Meta => write!(f, "meta"),
            // ...
        }
    }
}

impl std::fmt::Display for ViewState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}[{}x{}]", self.name, self.rows(), self.cols())
    }
}
```

## 9. Use NonZeroUsize Where Appropriate

```rust
// If ID 0 is never valid:
use std::num::NonZeroUsize;
pub id: NonZeroUsize,

// Enables Option<NonZeroUsize> to be same size as usize
```

## 10. Seal Traits That Shouldn't Be Extended

```rust
mod private { pub trait Sealed {} }

pub trait Source: private::Sealed + Send + Sync {
    // ...
}

impl private::Sealed for Polars {}
impl private::Sealed for Memory<'_> {}
impl private::Sealed for Gz<'_> {}
```

## 11. Use PhantomData for Lifetime Markers

```rust
// Current: Gz has df reference but lifetime is implicit
pub struct Gz<'a> {
    pub df: &'a DataFrame,
    pub partial: bool,
}

// Already correct, but ensure all Source variants are consistent
```

## 12. Consider Interning Strings

```rust
// Column names repeat often - intern them
use string_interner::{StringInterner, Symbol};

pub struct ColName(Symbol);  // 4 bytes instead of 24

// Or use Arc<str> for shared ownership
pub col_names: Vec<Arc<str>>,
```

## 13. Add #[inline] Hints for Hot Paths

```rust
#[inline]
pub fn rows(&self) -> usize {
    self.disk_rows.unwrap_or_else(|| self.dataframe.height())
}

#[inline]
pub fn cols(&self) -> usize {
    if self.col_names.is_empty() { self.dataframe.width() }
    else { self.col_names.len() }
}
```

## 14. Use #[non_exhaustive] for Enums

```rust
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum ViewKind {
    Table,
    Meta,
    Freq,
    Corr,
    Folder,
    Pivot,
}
```

## 15. Command Trait: Use Associated Type for Output

```rust
pub trait Command {
    type Output;
    fn exec(&mut self, app: &mut AppContext) -> Result<Self::Output>;
    fn to_str(&self) -> Cow<'static, str>;
    fn record(&self) -> bool { true }
}

// Most commands return (), but some could return data
```

## Priority Order

1. **#7 Const generics** - Quick win, improves readability
2. **#3 Custom errors** - Better error messages, pattern matching
3. **#8 Display impls** - Debugging, logging
4. **#1 Newtypes** - Prevents bugs from mixing IDs
5. **#4 Standard traits** - Better ergonomics
