use anyhow::Result;
use crossterm::{cursor, execute, terminal};
use skim::prelude::*;
use std::io::{self, Cursor, Write};

/// Skim picker - returns selected item
pub fn pick(items: Vec<String>, prompt: &str) -> Result<Option<String>> {
    execute!(io::stdout(), terminal::LeaveAlternateScreen, cursor::Show)?;
    terminal::disable_raw_mode()?;

    let opts = SkimOptionsBuilder::default().prompt(Some(prompt)).height(Some("50%")).multi(false).build().unwrap();
    let items = SkimItemReader::default().of_bufread(Cursor::new(items.join("\n")));

    let r = Skim::run_with(&opts, Some(items))
        .map(|o| if o.is_abort { None } else { o.selected_items.first().map(|i| i.output().to_string()) })
        .unwrap_or(None);

    print!("\x1b[2J\x1b[H"); io::stdout().flush()?;  // clear screen
    terminal::enable_raw_mode()?;
    execute!(io::stdout(), terminal::EnterAlternateScreen, cursor::Hide)?;
    Ok(r)
}

/// Skim with hints - returns query or selected item
pub fn input(items: Vec<String>, prompt: &str) -> Result<Option<String>> {
    execute!(io::stdout(), terminal::LeaveAlternateScreen, cursor::Show)?;
    terminal::disable_raw_mode()?;

    let opts = SkimOptionsBuilder::default().prompt(Some(prompt)).height(Some("50%")).build().unwrap();
    let items = SkimItemReader::default().of_bufread(Cursor::new(items.join("\n")));

    let r = Skim::run_with(&opts, Some(items))
        .map(|o| {
            if o.is_abort { None }
            else {
                let q = o.query.trim().to_string();
                if q.is_empty() { o.selected_items.first().map(|i| i.output().to_string()) }
                else { Some(q) }
            }
        })
        .unwrap_or(None);

    print!("\x1b[2J\x1b[H"); io::stdout().flush()?;
    terminal::enable_raw_mode()?;
    execute!(io::stdout(), terminal::EnterAlternateScreen, cursor::Hide)?;
    Ok(r)
}
