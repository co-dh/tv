pub mod executor;
pub mod io;
pub mod transform;
pub mod view;

use crate::app::AppContext;
use anyhow::Result;

/// Trait for all commands that can be executed
pub trait Command {
    /// Execute the command, modifying the app context
    fn execute(&mut self, app: &mut AppContext) -> Result<()>;

    /// Convert command to string representation for history
    fn to_command_string(&self) -> String;

    /// Whether this command should be recorded in history
    fn should_record(&self) -> bool {
        true
    }
}
