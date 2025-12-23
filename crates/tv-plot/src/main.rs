//! tv-plot CLI
//!
//! Usage:
//!   tv-plot [type] [options] < data.csv
//!
//! Types: scatter, line, hist, bar
//!
//! Options:
//!   --dark           dark theme
//!   --title TEXT     plot title
//!   -b, --bins N     histogram bins

use std::io::{self, BufRead};
use tv_plot::*;

fn main() {
    let args: Vec<String> = std::env::args().collect();

    // Parse args
    let mut cmd = "auto";
    let mut bins = 20;
    let mut dark = false;
    let mut ttl: Option<String> = None;

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "scatter" | "line" | "hist" | "bar" | "auto" => { cmd = args[i].as_str(); i += 1; }
            "-b" | "--bins" => { bins = args.get(i+1).and_then(|s| s.parse().ok()).unwrap_or(20); i += 2; }
            "--dark" => { dark = true; i += 1; }
            "--title" => { ttl = args.get(i+1).cloned(); i += 2; }
            _ => i += 1,
        }
    }

    // Read CSV
    let stdin = io::stdin();
    let mut lines = stdin.lock().lines();

    let hdr = match lines.next() {
        Some(Ok(h)) => h,
        _ => { eprintln!("Error: no input"); return; }
    };
    let names: Vec<&str> = hdr.split(',').collect();
    let n = names.len();

    let mut cols: Vec<Vec<f64>> = vec![Vec::new(); n];
    for line in lines.flatten() {
        for (i, v) in line.split(',').enumerate() {
            if i < n { if let Ok(x) = v.trim().parse() { cols[i].push(x); } }
        }
    }

    if cols.is_empty() || cols[0].is_empty() {
        eprintln!("Error: no data");
        return;
    }

    // Auto-detect
    let cmd = if cmd == "auto" {
        match n { 1 => "hist", _ => "scatter" }
    } else { cmd };

    // Build data frame
    let mut df = DataFrame::new();
    for (i, name) in names.iter().enumerate() {
        if i < cols.len() {
            df = df.col(name, std::mem::take(&mut cols[i]));
        }
    }

    // Build plot
    let mut p = ggplot(df);

    // Add aes + geom
    p = match cmd {
        "hist" => p + x(names[0]) + geom_histogram(bins),
        "scatter" => p + aes(names[0], names.get(1).unwrap_or(&names[0])) + geom_point(),
        "line" => p + aes(names[0], names.get(1).unwrap_or(&names[0])) + geom_line(),
        "bar" => p + aes(names[0], names.get(1).unwrap_or(&names[0])) + geom_bar(),
        _ => p,
    };

    // Theme
    if dark { p = p + theme_dark(); }

    // Title
    if let Some(t) = ttl { p = p + title(&t); }

    // Debug
    eprintln!("DEBUG: cols={} rows={} geoms={}",
        p.data.cols.len(),
        p.data.rows(),
        p.geoms.len());
    eprintln!("DEBUG: aes x={:?} y={:?}", p.aes.x, p.aes.y);

    p.show();
}
