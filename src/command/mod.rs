pub mod executor;
pub mod io;
pub mod nav;
pub mod transform;
pub mod view;

use crate::app::AppContext;
use anyhow::Result;

/// Command trait
pub trait Command {
    fn exec(&mut self, app: &mut AppContext) -> Result<()>;  // run command
    fn to_str(&self) -> String;                               // for history
    fn record(&self) -> bool { true }                         // save to history?
}
