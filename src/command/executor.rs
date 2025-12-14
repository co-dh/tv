use crate::app::AppContext;
use crate::command::Command;
use anyhow::Result;

/// Command executor
pub struct CommandExecutor;

impl CommandExecutor {
    /// Run command, record to history if needed
    pub fn exec(app: &mut AppContext, mut c: Box<dyn Command>) -> Result<()> {
        let (s, rec) = (c.to_str(), c.record());
        c.exec(app)?;
        if rec { app.record(&s)?; if let Some(v) = app.view_mut() { v.add_hist(s); } }
        Ok(())
    }
}
