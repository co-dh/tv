//! View type definitions
//! Note: view handlers are now in the plugin module

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
        else if name == "ls" || name == "lr" || name.starts_with("ls:") || name.starts_with("lr:") { ViewKind::Folder }
        else { ViewKind::Table }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_view_kind_from_name() {
        assert_eq!(ViewKind::from_name("metadata"), ViewKind::Meta);
        assert_eq!(ViewKind::from_name("Freq:col1"), ViewKind::Freq);
        assert_eq!(ViewKind::from_name("Freq:another"), ViewKind::Freq);
        assert_eq!(ViewKind::from_name("correlation"), ViewKind::Corr);
        assert_eq!(ViewKind::from_name("ls"), ViewKind::Folder);
        assert_eq!(ViewKind::from_name("lr"), ViewKind::Folder);
        assert_eq!(ViewKind::from_name("ls:/tmp"), ViewKind::Folder);
        assert_eq!(ViewKind::from_name("data.csv"), ViewKind::Table);
        assert_eq!(ViewKind::from_name("my_table"), ViewKind::Table);
    }

    #[test]
    fn test_view_kind_tab() {
        assert_eq!(ViewKind::Table.tab(), "table");
        assert_eq!(ViewKind::Meta.tab(), "meta");
        assert_eq!(ViewKind::Freq.tab(), "freq");
        assert_eq!(ViewKind::Folder.tab(), "folder");
        assert_eq!(ViewKind::Corr.tab(), "corr");
    }
}
