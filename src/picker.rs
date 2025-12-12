use anyhow::Result;
use crossterm::{cursor, execute, terminal};
use skim::prelude::*;
use std::io::{self, Cursor};

/// Run skim fuzzy finder with the given items and prompt
/// Returns the selected item or None if cancelled
pub fn pick(items: Vec<String>, prompt: &str) -> Result<Option<String>> {
    // Leave alternate screen temporarily for skim
    execute!(io::stdout(), terminal::LeaveAlternateScreen, cursor::Show)?;
    terminal::disable_raw_mode()?;

    let options = SkimOptionsBuilder::default()
        .prompt(Some(prompt))
        .height(Some("50%"))
        .multi(false)
        .build()
        .unwrap();

    let input = items.join("\n");
    let item_reader = SkimItemReader::default();
    let items = item_reader.of_bufread(Cursor::new(input));

    let result = Skim::run_with(&options, Some(items))
        .map(|out| {
            if out.is_abort {
                None
            } else {
                out.selected_items
                    .first()
                    .map(|item| item.output().to_string())
            }
        })
        .unwrap_or(None);

    // Return to alternate screen
    terminal::enable_raw_mode()?;
    execute!(io::stdout(), terminal::EnterAlternateScreen, cursor::Hide)?;

    Ok(result)
}

/// Run skim with multi-select enabled
pub fn pick_multi(items: Vec<String>, prompt: &str) -> Result<Vec<String>> {
    execute!(io::stdout(), terminal::LeaveAlternateScreen, cursor::Show)?;
    terminal::disable_raw_mode()?;

    let options = SkimOptionsBuilder::default()
        .prompt(Some(prompt))
        .height(Some("50%"))
        .multi(true)
        .build()
        .unwrap();

    let input = items.join("\n");
    let item_reader = SkimItemReader::default();
    let items = item_reader.of_bufread(Cursor::new(input));

    let result = Skim::run_with(&options, Some(items))
        .map(|out| {
            if out.is_abort {
                Vec::new()
            } else {
                out.selected_items
                    .iter()
                    .map(|item| item.output().to_string())
                    .collect()
            }
        })
        .unwrap_or_default();

    terminal::enable_raw_mode()?;
    execute!(io::stdout(), terminal::EnterAlternateScreen, cursor::Hide)?;

    Ok(result)
}
