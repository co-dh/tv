use anyhow::Result;
use ratatui::crossterm::{
    cursor,
    execute,
    terminal::{self, EnterAlternateScreen, LeaveAlternateScreen},
};
use ratatui::backend::CrosstermBackend;
use std::io::{self, Stdout};

pub type Tui = ratatui::Terminal<CrosstermBackend<Stdout>>;

/// Initialize terminal with ratatui backend
pub fn init() -> Result<Tui> {
    terminal::enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen, cursor::Hide)?;
    let backend = CrosstermBackend::new(stdout);
    let terminal = ratatui::Terminal::new(backend)?;
    Ok(terminal)
}

/// Restore terminal to normal mode
pub fn restore() -> Result<()> {
    terminal::disable_raw_mode()?;
    execute!(io::stdout(), cursor::Show, LeaveAlternateScreen)?;
    Ok(())
}
