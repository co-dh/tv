use crate::app::AppContext;
use crate::command::Command;
use crate::command::io::From;
use crate::command::view::Ls;
use crate::view::handler::ViewHandler;

pub struct Handler;

impl ViewHandler for Handler {
    fn handle(&self, cmd: &str, app: &mut AppContext) -> Option<Box<dyn Command>> {
        match cmd {
            "enter" => {
                // Get path and check if it's a directory
                let (path, is_dir) = app.view().and_then(|v| {
                    let df = &v.dataframe;
                    let cols = df.get_column_names();

                    // Get path from "path" or "name" column
                    let path_col = cols.iter().position(|c| c.as_str() == "path" || c.as_str() == "name")?;
                    let path = df.get_columns()[path_col]
                        .get(v.state.cr).ok()
                        .map(|v| v.to_string().trim_matches('"').to_string())?;

                    // Check dir column for "x"
                    let is_dir = cols.iter().position(|c| c.as_str() == "dir")
                        .and_then(|i| df.get_columns()[i].get(v.state.cr).ok())
                        .map(|v| v.to_string().trim_matches('"') == "x")
                        .unwrap_or(false);

                    Some((path, is_dir))
                })?;

                if is_dir {
                    // Enter directory with ls
                    Some(Box::new(Ls { dir: std::path::PathBuf::from(path) }) as Box<dyn Command>)
                } else {
                    // Open file
                    Some(Box::new(From { file_path: path }) as Box<dyn Command>)
                }
            }
            _ => None,
        }
    }
}
