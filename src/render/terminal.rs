use anyhow::Result;
use crossterm::{
    cursor,
    event::{DisableMouseCapture, EnableMouseCapture},
    execute,
    terminal::{self, EnterAlternateScreen, LeaveAlternateScreen},
};
use std::io;

/// Terminal wrapper for managing raw mode and screen
pub struct Terminal {
    _private: (),
}

impl Terminal {
    /// Initialize terminal in raw mode with alternate screen
    pub fn init() -> Result<Self> {
        terminal::enable_raw_mode()?;
        let mut stdout = io::stdout();
        execute!(
            stdout,
            EnterAlternateScreen,
            EnableMouseCapture,
            cursor::Hide
        )?;
        Ok(Self { _private: () })
    }

    /// Restore terminal to normal mode
    pub fn restore() -> Result<()> {
        let mut stdout = io::stdout();
        execute!(
            stdout,
            cursor::Show,
            LeaveAlternateScreen,
            DisableMouseCapture
        )?;
        terminal::disable_raw_mode()?;
        Ok(())
    }

    /// Get terminal size (rows, cols)
    pub fn size() -> Result<(u16, u16)> {
        Ok(terminal::size()?)
    }

    /// Clear the screen
    pub fn clear() -> Result<()> {
        execute!(io::stdout(), terminal::Clear(terminal::ClearType::All))?;
        Ok(())
    }
}

impl Drop for Terminal {
    fn drop(&mut self) {
        let _ = Terminal::restore();
    }
}
