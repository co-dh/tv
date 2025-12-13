//! Folder view plugin - directory listing (ls [-r])

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
    fn matches(&self, name: &str) -> bool { name.starts_with("ls") }

    fn handle(&self, cmd: &str, app: &mut AppContext) -> Option<Box<dyn Command>> {
        if cmd != "enter" { return None; }
        let (path, is_dir) = app.view().and_then(|v| {
            let df = &v.dataframe;
            let path = df.column("path").ok()?.get(v.state.cr).ok()
                .map(|v| v.to_string().trim_matches('"').to_string())?;
            let is_dir = df.column("dir").ok()
                .and_then(|c| c.get(v.state.cr).ok())
                .map(|v| v.to_string().trim_matches('"') == "x")
                .unwrap_or(false);
            Some((path, is_dir))
        })?;
        if is_dir { Some(Box::new(Ls { dir: PathBuf::from(path), recursive: false })) }
        else { Some(Box::new(From { file_path: path })) }
    }

    fn parse(&self, cmd: &str, arg: &str) -> Option<Box<dyn Command>> {
        if cmd != "ls" { return None; }
        // Parse: ls [-r] [dir]
        let (recursive, dir) = if arg.starts_with("-r") {
            (true, arg.trim_start_matches("-r").trim())
        } else { (false, arg) };
        let dir = if dir.is_empty() { PathBuf::from(".") } else { PathBuf::from(dir) };
        Some(Box::new(Ls { dir, recursive }))
    }
}

/// List directory (with optional -r for recursive)
pub struct Ls { pub dir: PathBuf, pub recursive: bool }

impl Command for Ls {
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        let df = if self.recursive { crate::os::lr(&self.dir)? } else { crate::os::ls(&self.dir)? };
        let id = app.next_id();
        let name = if self.recursive { format!("ls -r:{}", self.dir.display()) } else { format!("ls:{}", self.dir.display()) };
        app.stack.push(ViewState::new(id, name, df, None));
        Ok(())
    }
    fn to_str(&self) -> String {
        if self.recursive { format!("ls -r {}", self.dir.display()) } else { format!("ls {}", self.dir.display()) }
    }
}
