use crate::app::AppContext;
use crate::command::Command;
use anyhow::Result;

/// Command executor
pub struct CommandExecutor;

impl CommandExecutor {
    /// Run command, record to history before exec (so failed commands are also logged)
    pub fn exec(app: &mut AppContext, mut c: Box<dyn Command>) -> Result<()> {
        let (s, rec) = (c.to_str(), c.record());
        // Record before exec so failed commands are in history too
        if rec { let _ = app.record(&s); }
        let res = c.exec(app);
        // Add to view history only on success (for undo)
        if res.is_ok() && rec { if let Some(v) = app.view_mut() { v.add_hist(s); } }
        res
    }
}
