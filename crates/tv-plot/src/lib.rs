//! Plot library - renders charts to terminal via kitty graphics protocol

use plotters::prelude::*;
use image::{ImageBuffer, Rgb};

/// Render histogram from f64 values
pub fn histogram(vals: &[f64], bins: usize, w: u32, h: u32) -> Option<ImageBuffer<Rgb<u8>, Vec<u8>>> {
    if vals.is_empty() { return None; }

    // Compute histogram bins
    let (min, max) = vals.iter().fold((f64::MAX, f64::MIN), |(lo, hi), &v| (lo.min(v), hi.max(v)));
    let range = (max - min).max(1e-10);
    let bw = range / bins as f64;
    let mut counts = vec![0u32; bins];
    for &v in vals {
        let i = ((v - min) / bw).floor() as usize;
        counts[i.min(bins - 1)] += 1;
    }
    let max_cnt = *counts.iter().max().unwrap_or(&1) as f64;

    let mut buf = vec![0u8; (w * h * 3) as usize];
    {
        let root = BitMapBackend::with_buffer(&mut buf, (w, h)).into_drawing_area();
        root.fill(&WHITE).ok()?;

        let mut chart = ChartBuilder::on(&root)
            .margin(10)
            .x_label_area_size(30)
            .y_label_area_size(40)
            .build_cartesian_2d(min..max, 0f64..max_cnt)
            .ok()?;

        chart.configure_mesh().disable_mesh().disable_axes().draw().ok()?;

        // Draw histogram bars as rectangles
        for (i, &c) in counts.iter().enumerate() {
            let x0 = min + i as f64 * bw;
            let x1 = x0 + bw * 0.9;  // small gap between bars
            chart.draw_series(std::iter::once(
                Rectangle::new([(x0, 0.0), (x1, c as f64)], BLUE.filled())
            )).ok()?;
        }

        root.present().ok()?;
    }

    ImageBuffer::from_raw(w, h, buf)
}

/// Render line chart (time series)
pub fn line(xs: &[f64], ys: &[f64], w: u32, h: u32) -> Option<ImageBuffer<Rgb<u8>, Vec<u8>>> {
    if xs.is_empty() || ys.is_empty() || xs.len() != ys.len() { return None; }

    let (xmin, xmax) = xs.iter().fold((f64::MAX, f64::MIN), |(lo, hi), &v| (lo.min(v), hi.max(v)));
    let (ymin, ymax) = ys.iter().fold((f64::MAX, f64::MIN), |(lo, hi), &v| (lo.min(v), hi.max(v)));
    let xpad = (xmax - xmin).max(1.0) * 0.02;
    let ypad = (ymax - ymin).max(1.0) * 0.05;

    let mut buf = vec![0u8; (w * h * 3) as usize];
    {
        let root = BitMapBackend::with_buffer(&mut buf, (w, h)).into_drawing_area();
        root.fill(&WHITE).ok()?;

        let mut chart = ChartBuilder::on(&root)
            .margin(10)
            .x_label_area_size(30)
            .y_label_area_size(50)
            .build_cartesian_2d((xmin - xpad)..(xmax + xpad), (ymin - ypad)..(ymax + ypad))
            .ok()?;

        chart.configure_mesh().disable_mesh().disable_axes().draw().ok()?;

        chart.draw_series(LineSeries::new(
            xs.iter().zip(ys.iter()).map(|(&x, &y)| (x, y)),
            &BLUE,
        )).ok()?;

        root.present().ok()?;
    }

    ImageBuffer::from_raw(w, h, buf)
}

/// Render candlestick chart (OHLC)
pub fn candle(time: &[f64], open: &[f64], high: &[f64], low: &[f64], close: &[f64], w: u32, h: u32) -> Option<ImageBuffer<Rgb<u8>, Vec<u8>>> {
    let n = time.len();
    if n == 0 || open.len() != n || high.len() != n || low.len() != n || close.len() != n { return None; }

    let (tmin, tmax) = time.iter().fold((f64::MAX, f64::MIN), |(lo, hi), &v| (lo.min(v), hi.max(v)));
    let ymin = low.iter().fold(f64::MAX, |m, &v| m.min(v));
    let ymax = high.iter().fold(f64::MIN, |m, &v| m.max(v));
    let tpad = (tmax - tmin).max(1.0) * 0.02;
    let ypad = (ymax - ymin).max(1.0) * 0.05;

    let mut buf = vec![0u8; (w * h * 3) as usize];
    {
        let root = BitMapBackend::with_buffer(&mut buf, (w, h)).into_drawing_area();
        root.fill(&WHITE).ok()?;

        let mut chart = ChartBuilder::on(&root)
            .margin(10)
            .x_label_area_size(30)
            .y_label_area_size(50)
            .build_cartesian_2d((tmin - tpad)..(tmax + tpad), (ymin - ypad)..(ymax + ypad))
            .ok()?;

        chart.configure_mesh().disable_mesh().disable_axes().draw().ok()?;

        // Candle width based on data spacing
        let cw = if n > 1 { (time[1] - time[0]) * 0.8 } else { 1.0 };

        for i in 0..n {
            let (t, o, h, l, c) = (time[i], open[i], high[i], low[i], close[i]);
            let color = if c >= o { &GREEN } else { &RED };

            // Wick (high-low line)
            chart.draw_series(std::iter::once(
                PathElement::new(vec![(t, l), (t, h)], color)
            )).ok()?;

            // Body (open-close box)
            let (y1, y2) = if c >= o { (o, c) } else { (c, o) };
            chart.draw_series(std::iter::once(
                Rectangle::new([(t - cw/2.0, y1), (t + cw/2.0, y2)], color.filled())
            )).ok()?;
        }

        root.present().ok()?;
    }

    ImageBuffer::from_raw(w, h, buf)
}

/// Display image in terminal via kitty graphics protocol
pub fn show(img: &ImageBuffer<Rgb<u8>, Vec<u8>>) {
    use std::io::Write;
    use base64::Engine;

    // Encode PNG
    let mut png = Vec::new();
    let encoder = image::codecs::png::PngEncoder::new(&mut png);
    encoder.encode(img.as_raw(), img.width(), img.height(), image::ColorType::Rgb8).ok();

    // Base64 encode
    let b64 = base64::engine::general_purpose::STANDARD.encode(&png);

    // Send via kitty graphics protocol (chunked if needed)
    let mut stdout = std::io::stdout().lock();
    let chunks: Vec<&str> = b64.as_bytes().chunks(4096).map(|c| std::str::from_utf8(c).unwrap()).collect();
    for (i, chunk) in chunks.iter().enumerate() {
        let m = if i == chunks.len() - 1 { 0 } else { 1 };  // m=1 means more chunks
        if i == 0 {
            write!(stdout, "\x1b_Ga=T,f=100,m={};{}\x1b\\", m, chunk).ok();
        } else {
            write!(stdout, "\x1b_Gm={};{}\x1b\\", m, chunk).ok();
        }
    }
    writeln!(stdout).ok();
    stdout.flush().ok();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_histogram() {
        let vals: Vec<f64> = (0..100).map(|i| (i as f64 * 0.1).sin()).collect();
        let img = histogram(&vals, 20, 400, 300);
        assert!(img.is_some());
    }

    #[test]
    fn test_line() {
        let xs: Vec<f64> = (0..100).map(|i| i as f64).collect();
        let ys: Vec<f64> = xs.iter().map(|x| x.sin()).collect();
        let img = line(&xs, &ys, 400, 300);
        assert!(img.is_some());
    }
}
