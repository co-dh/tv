use crate::app::AppContext;
use crate::command::Command;
use crate::view::ViewKind;
use crate::view::{table, meta, freq, folder};

/// View-specific key handler trait
pub trait ViewHandler {
    /// Handle a command string, return Command to execute (if any)
    fn handle(&self, cmd: &str, app: &mut AppContext) -> Option<Box<dyn Command>>;
}

/// Dispatch to appropriate handler based on view kind
pub fn dispatch(kind: ViewKind, cmd: &str, app: &mut AppContext) -> Option<Box<dyn Command>> {
    match kind {
        ViewKind::Table => table::Handler.handle(cmd, app),
        ViewKind::Meta => meta::Handler.handle(cmd, app),
        ViewKind::Freq => freq::Handler.handle(cmd, app),
        ViewKind::Folder => folder::Handler.handle(cmd, app),
        ViewKind::Corr => table::Handler.handle(cmd, app),  // corr uses table handler
    }
}
