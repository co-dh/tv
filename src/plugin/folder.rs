//! Folder view plugin - directory listing (ls [-r])

use crate::app::AppContext;
use crate::backend::unquote;
use crate::command::Command;
use crate::command::io::From;
use crate::plugin::Plugin;
use crate::state::ViewState;
use anyhow::{anyhow, Result};
use std::path::{Path, PathBuf};

pub struct FolderPlugin;

/// Check if file is viewable with bat (text-like extension or no extension)
fn is_text_file(path: &Path) -> bool {
    match path.extension().and_then(|s| s.to_str()) {
        None => true,  // no extension = likely text
        Some(ext) => matches!(ext.to_lowercase().as_str(),
            "txt" | "md" | "rs" | "py" | "js" | "ts" | "json" | "yaml" | "yml" |
            "toml" | "sh" | "bash" | "zsh" | "fish" | "c" | "h" | "cpp" | "hpp" |
            "java" | "go" | "rb" | "pl" | "lua" | "sql" | "html" | "css" | "xml" |
            "log" | "conf" | "cfg" | "ini" | "env" | "gitignore" | "dockerfile" |
            "makefile" | "cmake" | "4th" | "forth" | "prql"
        ),
    }
}

impl Plugin for FolderPlugin {
    fn name(&self) -> &str { "folder" }
    fn tab(&self) -> &str { "folder" }
    fn matches(&self, name: &str) -> bool { name.starts_with("ls") }

    fn handle(&self, cmd: &str, app: &mut AppContext) -> Option<Box<dyn Command>> {
        let v = app.view()?;
        let df = &v.dataframe;
        // Extract parent dir from view name (ls:path or ls -r:path)
        let dir = v.name.split(':').nth(1).map(|s| PathBuf::from(s)).unwrap_or_else(|| PathBuf::from("."));

        // For delete: get all selected paths (or current row)
        if cmd == "delete" {
            let rows: Vec<usize> = if v.selected_rows.is_empty() { vec![v.state.cr] }
                else { v.selected_rows.iter().copied().collect() };
            let paths: Vec<String> = rows.iter().filter_map(|&r| {
                df.column("path").ok()?.get(r).ok().map(|v| unquote(&v.to_string()))
            }).collect();
            if !paths.is_empty() {
                return Some(Box::new(DelFiles { paths, dir }));
            }
            return None;
        }

        // For enter: get current row info
        let path = df.column("path").ok()?.get(v.state.cr).ok()
            .map(|v| unquote(&v.to_string()))?;
        let is_dir = df.column("dir").ok()
            .and_then(|c| c.get(v.state.cr).ok())
            .map(|v| unquote(&v.to_string()) == "x")
            .unwrap_or(false);

        match cmd {
            "enter" => {
                if is_dir {
                    Some(Box::new(Ls { dir: PathBuf::from(&path), recursive: false }))
                } else if is_text_file(Path::new(&path)) {
                    Some(Box::new(BatView { path }))
                } else {
                    Some(Box::new(From { file_path: path }))
                }
            }
            _ => None,
        }
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
        let df = if self.recursive { super::system::lr(&self.dir)? } else { super::system::ls(&self.dir)? };
        let id = app.next_id();
        let name = if self.recursive { format!("ls -r:{}", self.dir.display()) } else { format!("ls:{}", self.dir.display()) };
        app.stack.push(ViewState::new(id, name, df, None));
        Ok(())
    }
    fn to_str(&self) -> String {
        if self.recursive { format!("ls -r {}", self.dir.display()) } else { format!("ls {}", self.dir.display()) }
    }
}

/// Delete files with confirmation (supports multi-select)
pub struct DelFiles { pub paths: Vec<String>, pub dir: PathBuf }

impl Command for DelFiles {
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        use crate::picker;
        let n = self.paths.len();
        let prompt = if n == 1 {
            let name = Path::new(&self.paths[0]).file_name().and_then(|s| s.to_str()).unwrap_or(&self.paths[0]);
            format!("Delete '{}'? ", name)
        } else { format!("Delete {} files? ", n) };
        let result = picker::fzf(vec!["Yes".into(), "No".into()], &prompt)?;
        app.needs_redraw = true;
        match result {
            Some(s) if s == "Yes" => {
                let mut deleted = 0;
                for path in &self.paths {
                    if std::fs::remove_file(path).is_ok() { deleted += 1; }
                }
                app.msg(format!("Deleted {} file(s)", deleted));
                // Refresh by re-running ls on parent dir
                let df = super::system::ls(&self.dir)?;
                if let Some(view) = app.view_mut() {
                    view.dataframe = df;
                    view.selected_rows.clear();
                    if view.state.cr >= view.dataframe.height() {
                        view.state.cr = view.dataframe.height().saturating_sub(1);
                    }
                }
            }
            _ => app.msg("Cancelled".into()),
        }
        Ok(())
    }
    fn to_str(&self) -> String { format!("del {} files", self.paths.len()) }
}

/// View text file with bat (leaves alternate screen, restores on exit)
pub struct BatView { pub path: String }

impl Command for BatView {
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        use ratatui::crossterm::{execute, terminal::{LeaveAlternateScreen, EnterAlternateScreen, disable_raw_mode, enable_raw_mode}};
        use std::io::stdout;
        use std::process::Command as Cmd;

        // Leave alternate screen and disable raw mode so bat receives input
        execute!(stdout(), LeaveAlternateScreen)?;
        disable_raw_mode()?;

        // Run bat (or cat as fallback)
        let status = Cmd::new("bat")
            .args(["--paging=always", "--style=numbers", &self.path])
            .status()
            .or_else(|_| Cmd::new("less").arg(&self.path).status())
            .or_else(|_| Cmd::new("cat").arg(&self.path).status());

        // Re-enable raw mode and re-enter alternate screen
        enable_raw_mode()?;
        execute!(stdout(), EnterAlternateScreen)?;
        app.needs_redraw = true;  // force ratatui to redraw

        match status {
            Ok(s) if s.success() => { app.msg(format!("Viewed: {}", self.path)); Ok(()) }
            Ok(s) => Err(anyhow!("bat exited with: {}", s)),
            Err(e) => Err(anyhow!("Failed to view file: {}", e)),
        }
    }
    fn to_str(&self) -> String { format!("bat {}", self.path) }
}
