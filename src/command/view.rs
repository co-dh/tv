use crate::app::AppContext;
use crate::command::Command;
use anyhow::{anyhow, Result};

/// Pop view from stack
pub struct Pop;

impl Command for Pop {
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        app.stack.pop();
        app.message.clear();
        Ok(())
    }
    fn to_str(&self) -> String { "pop".into() }
}

/// Swap top two views
pub struct Swap;

impl Command for Swap {
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        if app.stack.len() >= 2 {
            app.stack.swap();
            Ok(())
        } else {
            Err(anyhow!("Need at least 2 views to swap"))
        }
    }
    fn to_str(&self) -> String { "swap".into() }
    fn record(&self) -> bool { false }
}

/// Duplicate current view
pub struct Dup;

impl Command for Dup {
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        let view = app.req()?;
        let mut new_view = view.clone();
        new_view.name = format!("{} (copy)", view.name);
        new_view.id = app.next_id();
        app.stack.push(new_view);
        Ok(())
    }
    fn to_str(&self) -> String { "dup".into() }
    fn record(&self) -> bool { false }
}
