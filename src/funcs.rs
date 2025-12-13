// Forth-style user function definitions
// Syntax: : name body ... ;
use std::collections::HashMap;
use std::fs;
use std::path::Path;

/// User-defined functions
pub struct Funcs {
    defs: HashMap<String, String>,  // name -> body
}

impl Funcs {
    pub fn new() -> Self { Self { defs: HashMap::new() } }

    /// Load functions from file (Forth syntax: : name body ;)
    pub fn load(path: &Path) -> Self {
        let mut funcs = Self::new();
        if let Ok(content) = fs::read_to_string(path) {
            funcs.parse(&content);
        }
        funcs
    }

    /// Parse Forth-style definitions, skip ( comments )
    fn parse(&mut self, content: &str) {
        let mut in_def = false;
        let mut in_comment = false;
        let mut name = String::new();
        let mut body = Vec::new();

        for token in content.split_whitespace() {
            // Skip comments: ( ... )
            if token == "(" { in_comment = true; continue; }
            if token == ")" || token.ends_with(')') { in_comment = false; continue; }
            if in_comment { continue; }

            if token == ":" {
                in_def = true;
                name.clear();
                body.clear();
            } else if token == ";" {
                if in_def && !name.is_empty() {
                    self.defs.insert(name.clone(), body.join(" "));
                }
                in_def = false;
            } else if in_def {
                if name.is_empty() {
                    name = token.to_string();
                } else {
                    body.push(token.to_string());
                }
            }
        }
    }

    /// Get function body by name
    pub fn get(&self, name: &str) -> Option<&str> {
        self.defs.get(name).map(|s| s.as_str())
    }

    /// Expand function calls in command string (recursive)
    pub fn expand(&self, cmd: &str) -> String {
        let mut result = cmd.to_string();
        let mut changed = true;
        let mut depth = 0;
        while changed && depth < 10 {  // max 10 levels of expansion
            changed = false;
            depth += 1;
            for (name, body) in &self.defs {
                if result.contains(name) {
                    // Replace whole word only
                    let new = result.split_whitespace()
                        .map(|w| if w == name { body.as_str() } else { w })
                        .collect::<Vec<_>>()
                        .join(" ");
                    if new != result {
                        result = new;
                        changed = true;
                    }
                }
            }
        }
        result
    }

    /// List all defined functions
    pub fn list(&self) -> Vec<(&str, &str)> {
        self.defs.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect()
    }
}

impl Default for Funcs {
    fn default() -> Self { Self::new() }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple() {
        let mut f = Funcs::new();
        f.parse(": hello world ;");
        assert_eq!(f.get("hello"), Some("world"));
    }

    #[test]
    fn test_parse_multi_word() {
        let mut f = Funcs::new();
        f.parse(": sel_null sel_rows `null%` == '100.0' ;");
        assert_eq!(f.get("sel_null"), Some("sel_rows `null%` == '100.0'"));
    }

    #[test]
    fn test_expand() {
        let mut f = Funcs::new();
        f.parse(": greet hello world ;");
        assert_eq!(f.expand("greet"), "hello world");
    }

    #[test]
    fn test_expand_nested() {
        let mut f = Funcs::new();
        f.parse(": a b ; : b c ;");
        assert_eq!(f.expand("a"), "c");
    }
}
