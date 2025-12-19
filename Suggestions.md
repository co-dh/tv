# FP/Trait Suggestions

## 1. Use `std::mem::take` instead of clone
In places where we clone then immediately consume, use `take()`:
```rust
// Before: clones unnecessarily
let parent_prql = app.req()?.prql.clone();

// Better: if we're going to mutate anyway, use take or restructure
```

## 2. Registry could use inventory crate for auto-registration
Instead of hardcoding plugins:
```rust
// Current: Manual list
plugins: vec![Box::new(meta::MetaPlugin), Box::new(freq::FreqPlugin), ...]

// With inventory: Auto-collect at compile time
inventory::collect!(Box<dyn Plugin>);
```

## 3. ViewState builder pattern
Many optional fields - builder would clean up constructors:
```rust
ViewState::new(id, name).with_parent(pid).with_prql(prql).build()
```

## 4. More Option combinators
```rust
// Before
if let Some(v) = app.view() {
    if let Some(idx) = v.col_name(0) { ... }
}

// After
app.view().and_then(|v| v.col_name(0)).map(|idx| ...)
```

## 5. Use AsRef<str> for flexible string params
```rust
// Before
fn filter(&self, path: &str, w: &str, ...)

// After - accepts String, &str, Cow<str>
fn filter(&self, path: impl AsRef<str>, w: impl AsRef<str>, ...)
```

## 6. Extract repeated "pop+create" pattern
Many plugins do: get parent info -> pop -> create new view. Extract to helper.

## 7. Consider TryFrom for command parsing
```rust
impl TryFrom<&str> for Filter {
    type Error = ParseError;
    fn try_from(s: &str) -> Result<Self, Self::Error> { ... }
}
```

## 8. Registry could implement FromIterator
```rust
impl FromIterator<Box<dyn Plugin>> for Registry {
    fn from_iter<I: IntoIterator<Item = Box<dyn Plugin>>>(iter: I) -> Self { ... }
}
```

## 9. Use split_once() instead of splitn(2)
```rust
// Before
let parts: Vec<&str> = line.splitn(2, ' ').collect();

// After
if let Some((cmd, arg)) = line.split_once(' ') { ... }
```
