use anyhow::Result;
use crossterm::{cursor, execute, terminal, ExecutableCommand};
use skim::prelude::*;
use std::io::{Cursor, Write};

fn setup() -> Result<()> {
    execute!(std::io::stdout(), cursor::Show)?;
    terminal::disable_raw_mode()?;
    Ok(())
}

fn restore() -> Result<()> {
    // Clear bottom half where skim was drawn
    let (_, h) = terminal::size()?;
    let start = h / 2;
    let mut out = std::io::stdout();
    for y in start..h {
        out.execute(cursor::MoveTo(0, y))?.execute(terminal::Clear(terminal::ClearType::CurrentLine))?;
    }
    out.flush()?;
    terminal::enable_raw_mode()?;
    execute!(std::io::stdout(), cursor::Hide)?;
    Ok(())
}

fn opts(prompt: &str, multi: bool) -> SkimOptions<'_> {
    SkimOptionsBuilder::default()
        .prompt(Some(prompt)).height(Some("50%")).layout("reverse-list")
        .no_clear(true).no_clear_start(true).multi(multi).build().unwrap()
}

/// Skim picker - returns selected item
pub fn pick(items: Vec<String>, prompt: &str) -> Result<Option<String>> {
    setup()?;
    let items = SkimItemReader::default().of_bufread(Cursor::new(items.join("\n")));
    let r = Skim::run_with(&opts(prompt, false), Some(items))
        .map(|o| if o.is_abort { None } else { o.selected_items.first().map(|i| i.output().to_string()) })
        .unwrap_or(None);
    restore()?;
    Ok(r)
}

/// Skim with hints - returns query or selected item
pub fn input(items: Vec<String>, prompt: &str) -> Result<Option<String>> {
    setup()?;
    let items = SkimItemReader::default().of_bufread(Cursor::new(items.join("\n")));
    let r = Skim::run_with(&opts(prompt, false), Some(items))
        .map(|o| {
            if o.is_abort { None }
            else if !o.query.is_empty() { Some(o.query) }
            else { o.selected_items.first().map(|i| i.output().to_string()) }
        })
        .unwrap_or(None);
    restore()?;
    Ok(r)
}
