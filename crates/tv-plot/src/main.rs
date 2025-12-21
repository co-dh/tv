//! tv-plot - terminal plotting via kitty graphics protocol
//!
//! Usage:
//!   tv-plot hist              # histogram of single column
//!   tv-plot line              # line chart (1 col = y, 2 cols = x,y)
//!   tv-plot candle            # candlestick (5 cols: time,open,high,low,close)
//!
//! Input: CSV from stdin (first row = header)
//! Options:
//!   -b, --bins N     histogram bins (default: 50)
//!   -w, --width N    image width (default: 800)
//!   -h, --height N   image height (default: 400)

use std::io::{self, BufRead};

fn main() {
    let args: Vec<String> = std::env::args().collect();

    // Parse command
    let cmd = args.get(1).map(|s| s.as_str()).unwrap_or("auto");

    // Parse options
    let mut bins = 20usize;  // fewer bins = wider bars
    let mut width = 800u32;   // full resolution for kitty protocol
    let mut height = 400u32;

    let mut i = 2;
    while i < args.len() {
        match args[i].as_str() {
            "-b" | "--bins" => { bins = args.get(i+1).and_then(|s| s.parse().ok()).unwrap_or(50); i += 2; }
            "-w" | "--width" => { width = args.get(i+1).and_then(|s| s.parse().ok()).unwrap_or(800); i += 2; }
            "-h" | "--height" => { height = args.get(i+1).and_then(|s| s.parse().ok()).unwrap_or(400); i += 2; }
            _ => { i += 1; }
        }
    }

    // Read CSV from stdin
    let stdin = io::stdin();
    let mut lines = stdin.lock().lines();

    // Skip header
    let header = match lines.next() {
        Some(Ok(h)) => h,
        _ => { eprintln!("Error: no input"); return; }
    };
    let ncols = header.split(',').count();

    // Read data
    let mut cols: Vec<Vec<f64>> = vec![Vec::new(); ncols];
    for line in lines {
        if let Ok(l) = line {
            for (i, val) in l.split(',').enumerate() {
                if i < ncols {
                    if let Ok(v) = val.trim().parse::<f64>() {
                        cols[i].push(v);
                    }
                }
            }
        }
    }

    if cols.is_empty() || cols[0].is_empty() {
        eprintln!("Error: no data");
        return;
    }

    // Auto-detect plot type if not specified
    let cmd = if cmd == "auto" {
        match ncols {
            1 => "hist",
            2 => "line",
            5 => "candle",
            _ => "line",
        }
    } else { cmd };

    // Generate plot
    let img = match cmd {
        "hist" | "histogram" => {
            tv_plot::histogram(&cols[0], bins, width, height)
        }
        "line" => {
            if ncols >= 2 {
                tv_plot::line(&cols[0], &cols[1], width, height)
            } else {
                // Use row index as x
                let xs: Vec<f64> = (0..cols[0].len()).map(|i| i as f64).collect();
                tv_plot::line(&xs, &cols[0], width, height)
            }
        }
        "candle" | "candlestick" => {
            if ncols >= 5 {
                tv_plot::candle(&cols[0], &cols[1], &cols[2], &cols[3], &cols[4], width, height)
            } else {
                eprintln!("Error: candle requires 5 columns (time,open,high,low,close)");
                return;
            }
        }
        _ => {
            eprintln!("Usage: tv-plot [hist|line|candle] [-b bins] [-w width] [-h height]");
            return;
        }
    };

    // Display
    match img {
        Some(img) => tv_plot::show(&img),
        None => eprintln!("Error: failed to generate plot"),
    }
}
