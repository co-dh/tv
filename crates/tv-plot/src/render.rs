//! Rendering - plotters backend + kitty output

use crate::{Plot, Geom, Color};
use plotters::prelude::*;
use plotters::style::Color as PlotColor;
use image::{ImageBuffer, ImageEncoder, Rgb};

/// Render plot to image buffer
pub fn render(p: &Plot) -> Option<ImageBuffer<Rgb<u8>, Vec<u8>>> {
    let (w, h) = (p.width, p.height);
    let mut buf = vec![0u8; (w * h * 3) as usize];

    // Get data
    eprintln!("DEBUG render: looking for x={:?}", p.aes.x);
    for (name, vals) in &p.data.cols {
        eprintln!("DEBUG render: col '{}' has {} vals", name, vals.len());
    }
    let xs = p.aes.x.as_ref().and_then(|n| p.data.get(n));
    eprintln!("DEBUG render: xs={:?}", xs.map(|v| v.len()));
    let xs = xs?;
    let ys = p.aes.y.as_ref().and_then(|n| p.data.get(n)).unwrap_or(xs);

    let (xmin, xmax) = minmax(xs);
    let (ymin, ymax) = minmax(ys);
    let xpad = (xmax - xmin).max(1.0) * 0.05;
    let ypad = (ymax - ymin).max(1.0) * 0.05;

    {
        let root = BitMapBackend::with_buffer(&mut buf, (w, h)).into_drawing_area();
        root.fill(&to_rgb(p.theme.bg)).ok()?;

        let mut chart = ChartBuilder::on(&root)
            .margin(15)
            .x_label_area_size(35)
            .y_label_area_size(50)
            .caption(p.labs.title.as_deref().unwrap_or(""), ("sans-serif", 18).into_font().color(&to_rgb(p.theme.fg)))
            .build_cartesian_2d((xmin - xpad)..(xmax + xpad), (ymin - ypad)..(ymax + ypad))
            .ok()?;

        chart.configure_mesh()
            .x_desc(p.labs.x.as_deref().unwrap_or(""))
            .y_desc(p.labs.y.as_deref().unwrap_or(""))
            .axis_style(to_rgb(p.theme.fg))
            .label_style(("sans-serif", 12).into_font().color(&to_rgb(p.theme.fg)))
            .light_line_style(to_rgb(p.theme.grid))
            .draw().ok()?;

        // Draw each geom
        for (i, g) in p.geoms.iter().enumerate() {
            let c = geom_color(g).unwrap_or(p.theme.palette.get(i % p.theme.palette.len()).copied().unwrap_or(Color::BLUE));
            draw_geom(&mut chart, g, xs, ys, c)?;
        }

        root.present().ok()?;
    }

    ImageBuffer::from_raw(w, h, buf)
}

/// Draw single geom layer
fn draw_geom<DB: DrawingBackend>(
    chart: &mut ChartContext<DB, Cartesian2d<plotters::coord::types::RangedCoordf64, plotters::coord::types::RangedCoordf64>>,
    g: &Geom,
    xs: &[f64],
    ys: &[f64],
    color: Color,
) -> Option<()> {
    let c = to_rgb(color);
    match g {
        Geom::Point { size, .. } => {
            chart.draw_series(
                xs.iter().zip(ys).map(|(&x, &y)| Circle::new((x, y), *size, c.filled()))
            ).ok()?;
        }
        Geom::Line { width, .. } => {
            chart.draw_series(LineSeries::new(
                xs.iter().zip(ys).map(|(&x, &y)| (x, y)),
                c.stroke_width(*width),
            )).ok()?;
        }
        Geom::Bar { .. } | Geom::Histogram { .. } => {
            let bins = if let Geom::Histogram { bins, .. } = g { *bins } else { 20 };
            draw_hist(chart, ys, bins, c)?;
        }
        Geom::Area { alpha, .. } => {
            // Area as filled polygon
            let a = *alpha as f64;
            let fill = RGBAColor(c.0, c.1, c.2, a);
            let mut pts: Vec<(f64, f64)> = xs.iter().zip(ys).map(|(&x, &y)| (x, y)).collect();
            if let (Some(&first_x), Some(&last_x)) = (xs.first(), xs.last()) {
                pts.push((last_x, 0.0));
                pts.push((first_x, 0.0));
            }
            chart.draw_series(std::iter::once(Polygon::new(pts, fill))).ok()?;
        }
        Geom::Boxplot { .. } => {
            // TODO: boxplot
        }
    }
    Some(())
}

/// Draw histogram bars
fn draw_hist<DB: DrawingBackend>(
    chart: &mut ChartContext<DB, Cartesian2d<plotters::coord::types::RangedCoordf64, plotters::coord::types::RangedCoordf64>>,
    vals: &[f64],
    bins: usize,
    color: RGBColor,
) -> Option<()> {
    let (min, max) = minmax(vals);
    let bw = (max - min).max(1e-10) / bins as f64;
    let mut counts = vec![0u32; bins];
    for &v in vals {
        let i = ((v - min) / bw).floor() as usize;
        counts[i.min(bins - 1)] += 1;
    }

    for (i, &c) in counts.iter().enumerate() {
        let x0 = min + i as f64 * bw;
        let x1 = x0 + bw * 0.9;
        chart.draw_series(std::iter::once(
            Rectangle::new([(x0, 0.0), (x1, c as f64)], color.filled())
        )).ok()?;
    }
    Some(())
}

/// Get explicit color from geom
fn geom_color(g: &Geom) -> Option<Color> {
    match g {
        Geom::Point { color, .. } => *color,
        Geom::Line { color, .. } => *color,
        Geom::Bar { color } => *color,
        Geom::Histogram { color, .. } => *color,
        Geom::Boxplot { color } => *color,
        Geom::Area { color, .. } => *color,
    }
}

/// Convert to plotters RGB
fn to_rgb(c: Color) -> RGBColor { RGBColor(c.0, c.1, c.2) }

/// Min/max of slice
fn minmax(v: &[f64]) -> (f64, f64) {
    v.iter().fold((f64::MAX, f64::MIN), |(lo, hi), &x| (lo.min(x), hi.max(x)))
}

/// Encode image as PNG
pub fn to_png(img: &ImageBuffer<Rgb<u8>, Vec<u8>>) -> Vec<u8> {
    let mut png = Vec::new();
    image::codecs::png::PngEncoder::new(&mut png)
        .write_image(img.as_raw(), img.width(), img.height(), image::ColorType::Rgb8).ok();
    png
}

/// Display via kitty graphics protocol
pub fn kitty_show(img: &ImageBuffer<Rgb<u8>, Vec<u8>>) {
    use std::io::Write;
    use base64::Engine;

    let png = to_png(img);
    let b64 = base64::engine::general_purpose::STANDARD.encode(&png);

    let mut out = std::io::stdout().lock();
    for (i, chunk) in b64.as_bytes().chunks(4096).enumerate() {
        let m = if i == 0 { "a=T,f=100," } else { "" };
        let more = if chunk.len() == 4096 { "m=1" } else { "m=0" };
        write!(out, "\x1b_G{}{};{}\x1b\\", m, more, std::str::from_utf8(chunk).unwrap()).ok();
    }
    writeln!(out).ok();
    out.flush().ok();
}
