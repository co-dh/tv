use crate::app::AppContext;
use crate::command::Command;
use crate::command::io::Load;
use crate::view::handler::ViewHandler;

pub struct Handler;

impl ViewHandler for Handler {
    fn handle(&self, cmd: &str, app: &mut AppContext) -> Option<Box<dyn Command>> {
        match cmd {
            "enter" => {
                // Open the file at current row
                let path = app.view().and_then(|v| {
                    // Folder view has "name" or "path" column
                    let col_idx = v.dataframe.get_column_names().iter()
                        .position(|c| c.as_str() == "path" || c.as_str() == "name")?;
                    v.dataframe.get_columns()[col_idx]
                        .get(v.state.cr)
                        .ok()
                        .map(|v| v.to_string().trim_matches('"').to_string())
                });

                path.map(|file_path| Box::new(Load { file_path }) as Box<dyn Command>)
            }
            _ => None,
        }
    }
}
