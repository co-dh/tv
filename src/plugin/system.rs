//! System view plugin - OS info commands (ps, df, tcp, etc.)

use crate::app::AppContext;
use crate::command::Command;
use crate::plugin::Plugin;
use crate::state::ViewState;
use anyhow::Result;

pub struct SystemPlugin;

impl Plugin for SystemPlugin {
    fn name(&self) -> &str { "system" }
    fn tab(&self) -> &str { "table" }

    fn matches(&self, name: &str) -> bool {
        matches!(name, "ps" | "df" | "mounts" | "tcp" | "udp" | "lsblk" | "who" | "env")
            || name.starts_with("lsof")
    }

    fn handle(&self, _cmd: &str, _app: &mut AppContext) -> Option<Box<dyn Command>> {
        None
    }

    fn parse(&self, cmd: &str, arg: &str) -> Option<Box<dyn Command>> {
        match cmd {
            "ps" => Some(Box::new(SysCmd::Ps)),
            "df" => Some(Box::new(SysCmd::Df)),
            "mounts" => Some(Box::new(SysCmd::Mounts)),
            "tcp" => Some(Box::new(SysCmd::Tcp)),
            "udp" => Some(Box::new(SysCmd::Udp)),
            "lsblk" => Some(Box::new(SysCmd::Lsblk)),
            "who" => Some(Box::new(SysCmd::Who)),
            "env" => Some(Box::new(SysCmd::Env)),
            "lsof" => Some(Box::new(Lsof { pid: arg.parse().ok() })),
            _ => None,
        }
    }

    fn commands(&self) -> Vec<(&str, &str)> {
        vec![
            ("ps", "Process list"), ("df", "Disk usage"), ("mounts", "Mount points"),
            ("tcp", "TCP connections"), ("udp", "UDP connections"), ("lsblk", "Block devices"),
            ("who", "Logged in users"), ("lsof", "Open files"), ("env", "Environment variables"),
        ]
    }
}

/// Unified system command enum - reduces boilerplate
#[derive(Clone, Copy)]
pub enum SysCmd { Ps, Df, Mounts, Tcp, Udp, Lsblk, Who, Env }

impl Command for SysCmd {
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        let (name, df) = match self {
            SysCmd::Ps => ("ps", crate::os::ps()?),
            SysCmd::Df => ("df", crate::os::df()?),
            SysCmd::Mounts => ("mounts", crate::os::mounts()?),
            SysCmd::Tcp => ("tcp", crate::os::tcp()?),
            SysCmd::Udp => ("udp", crate::os::udp()?),
            SysCmd::Lsblk => ("lsblk", crate::os::lsblk()?),
            SysCmd::Who => ("who", crate::os::who()?),
            SysCmd::Env => ("env", crate::os::env()?),
        };
        let id = app.next_id();
        app.stack.push(ViewState::new(id, name.into(), df, None));
        Ok(())
    }
    fn to_str(&self) -> String {
        match self {
            SysCmd::Ps => "ps", SysCmd::Df => "df", SysCmd::Mounts => "mounts",
            SysCmd::Tcp => "tcp", SysCmd::Udp => "udp", SysCmd::Lsblk => "lsblk",
            SysCmd::Who => "who", SysCmd::Env => "env",
        }.into()
    }
}

/// lsof needs special handling for optional pid
pub struct Lsof { pub pid: Option<i32> }

impl Command for Lsof {
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        let df = crate::os::lsof(self.pid)?;
        let id = app.next_id();
        let name = self.pid.map(|p| format!("lsof:{}", p)).unwrap_or("lsof".into());
        app.stack.push(ViewState::new(id, name, df, None));
        Ok(())
    }
    fn to_str(&self) -> String { self.pid.map(|p| format!("lsof {}", p)).unwrap_or("lsof".into()) }
}
