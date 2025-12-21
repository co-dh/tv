//! PRQL compilation with function library and query cache

use std::sync::{Mutex, OnceLock};
use std::num::NonZeroUsize;
use crate::LruCache;

/// PRQL function library (loaded once)
static FUNCS: OnceLock<String> = OnceLock::new();

fn funcs() -> &'static str {
    FUNCS.get_or_init(|| {
        // Try to load from file, fallback to empty
        std::fs::read_to_string("funcs.prql")
            .or_else(|_| std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/funcs.prql")))
            .unwrap_or_default()
    })
}

/// Compile PRQL to SQL (prepends funcs.prql)
pub fn compile(prql: &str) -> Option<String> {
    if prql.is_empty() { return None; }
    let full = format!("{}\n{}", funcs(), prql);
    let opts = prqlc::Options::default().no_format().with_signature_comment(false);
    prqlc::compile(&full, &opts).ok()
}

/// Query cache: (path, prql) -> Box<T>
pub struct QueryCache<T> {
    cache: Mutex<LruCache<(String, String), Box<T>>>,
}

impl<T> QueryCache<T> {
    pub fn new(cap: usize) -> Self {
        Self { cache: Mutex::new(LruCache::new(NonZeroUsize::new(cap).unwrap())) }
    }

    /// Get cached result or execute query
    pub fn get_or_exec<F>(&self, path: &str, prql: &str, exec: F) -> Option<*const T>
    where F: FnOnce(&str) -> Option<T>
    {
        let key = (path.to_string(), prql.to_string());
        let mut guard = self.cache.lock().ok()?;

        // Check cache
        if let Some(t) = guard.get(&key) {
            return Some(t.as_ref() as *const T);
        }

        // Compile PRQL to SQL
        let sql = compile(prql)?;

        // Execute and cache
        let result = exec(&sql)?;
        guard.put(key.clone(), Box::new(result));
        Some(guard.get(&key)?.as_ref() as *const T)
    }
}
