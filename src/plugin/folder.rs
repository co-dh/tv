//! Folder view plugin - directory listing (ls, lr)
//! Combines: view detection, command handling, Ls/Lr commands

use crate::app::AppContext;
use crate::command::Command;
use crate::command::io::From;
use crate::plugin::Plugin;
use crate::state::ViewState;
use anyhow::Result;
use std::path::PathBuf;

pub struct FolderPlugin;

impl Plugin for FolderPlugin {
    fn name(&self) -> &str { "folder" }
    fn tab(&self) -> &str { "folder" }

    fn matches(&self, name: &str) -> bool {
        name == "ls" || name == "lr" || name.starts_with("ls:") || name.starts_with("lr:")
    }

    fn handle(&self, cmd: &str, app: &mut AppContext) -> Option<Box<dyn Command>> {
        match cmd {
            "enter" => {
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
                    Some(Box::new(Ls { dir: PathBuf::from(path) }) as Box<dyn Command>)
                } else {
                    Some(Box::new(From { file_path: path }) as Box<dyn Command>)
                }
            }
            _ => None,
        }
    }

    fn parse(&self, cmd: &str, arg: &str) -> Option<Box<dyn Command>> {
        match cmd {
            "ls" => {
                let dir = if arg.is_empty() { PathBuf::from(".") } else { PathBuf::from(arg) };
                Some(Box::new(Ls { dir }))
            }
            "lr" => {
                let dir = if arg.is_empty() { PathBuf::from(".") } else { PathBuf::from(arg) };
                Some(Box::new(Lr { dir }))
            }
            _ => None,
        }
    }

    fn commands(&self) -> Vec<(&str, &str)> {
        vec![
            ("ls", "List directory"),
            ("lr", "List directory recursively"),
        ]
    }
}

// === Commands ===

/// List directory
pub struct Ls { pub dir: PathBuf }

impl Command for Ls {
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        let df = crate::os::ls(&self.dir)?;
        let id = app.next_id();
        app.stack.push(ViewState::new(id, format!("ls:{}", self.dir.display()), df, None));
        Ok(())
    }
    fn to_str(&self) -> String { format!("ls {}", self.dir.display()) }
}

/// List directory recursively
pub struct Lr { pub dir: PathBuf }

impl Command for Lr {
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        let df = crate::os::lr(&self.dir)?;
        let id = app.next_id();
        app.stack.push(ViewState::new(id, format!("lr:{}", self.dir.display()), df, None));
        Ok(())
    }
    fn to_str(&self) -> String { format!("lr {}", self.dir.display()) }
}
