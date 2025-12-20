//! Common test utilities shared across test modules.

/// Run tv with key replay mode and return rendered buffer as string
pub fn run_keys(keys: &str, file: &str) -> String {
    tv::test::replay_keys(keys, file)
}
