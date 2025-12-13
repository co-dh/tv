use crate::app::AppContext;
use crate::command::Command;
use crate::view::handler::ViewHandler;

pub struct Handler;

impl ViewHandler for Handler {
    fn handle(&self, cmd: &str, _app: &mut AppContext) -> Option<Box<dyn Command>> {
        match cmd {
            // Table-specific commands go here
            // Most table commands are handled in main.rs common code
            _ => None,
        }
    }
}
