use crate::app::AppContext;
use crate::command::Command;
use anyhow::Result;

/// Command executor that runs commands and records history
pub struct CommandExecutor;

impl CommandExecutor {
    /// Execute a command and record it to history if applicable
    pub fn execute(app: &mut AppContext, mut cmd: Box<dyn Command>) -> Result<()> {
        // Get command string before execution
        let cmd_string = cmd.to_command_string();
        let should_record = cmd.should_record();

        // Execute the command
        cmd.execute(app)?;

        // Record to history file if needed
        if should_record {
            app.record_command(&cmd_string)?;

            // Also add to current view's history
            if let Some(view) = app.current_view_mut() {
                view.add_to_history(cmd_string);
            }
        }

        Ok(())
    }
}
