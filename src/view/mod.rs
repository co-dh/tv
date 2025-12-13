#![allow(dead_code)]
pub mod handler;
pub mod table;
pub mod meta;
pub mod freq;
pub mod folder;

/// View type enum
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum ViewKind {
    Table,   // regular data table
    Meta,    // metadata/profile view
    Freq,    // frequency table
    Folder,  // directory listing (ls, lr)
    Corr,    // correlation matrix
}

impl ViewKind {
    /// Get keymap tab name for this view kind
    pub fn tab(&self) -> &'static str {
        match self {
            ViewKind::Table => "table",
            ViewKind::Meta => "meta",
            ViewKind::Freq => "freq",
            ViewKind::Folder => "folder",
            ViewKind::Corr => "corr",
        }
    }

    /// Detect view kind from view name
    pub fn from_name(name: &str) -> Self {
        if name == "metadata" { ViewKind::Meta }
        else if name.starts_with("Freq:") { ViewKind::Freq }
        else if name == "correlation" { ViewKind::Corr }
        else if name == "ls" || name == "lr" { ViewKind::Folder }
        else { ViewKind::Table }
    }
}
