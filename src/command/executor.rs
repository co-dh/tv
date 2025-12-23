use crate::app::AppContext;
use crate::command::Command;
use anyhow::Result;

/// Command executor
pub struct CommandExecutor;

impl CommandExecutor {
    /// Run command, record to history file before exec
    pub fn exec(app: &mut AppContext, mut c: Box<dyn Command>) -> Result<()> {
        if c.record() { let _ = app.record(&c.to_str()); }
        c.exec(app)
    }
}
