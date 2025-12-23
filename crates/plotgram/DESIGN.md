# plotgram - Grammar of Graphics for Rust

## API Design

```rust
use plotgram::*;

// Scatter plot
plot(data).aes(x("mpg"), y("hp")).geom_point().show();

// Line + points with color grouping
plot(data)
    .aes(x("date"), y("price"), color("symbol"))
    .geom_line()
    .geom_point()
    .show();

// Histogram
plot(data).aes(x("value")).geom_hist(20).show();

// Faceted plot
plot(data)
    .aes(x("x"), y("y"))
    .geom_point()
    .facet_wrap("category", 2)  // 2 columns
    .show();

// Full example
plot(data)
    .aes(x("date"), y("price"), color("symbol"), size("volume"))
    .geom_line()
    .scale_y_log()
    .labs("Stock Prices", "Date", "Price ($)")
    .theme_dark()
    .save("plot.png", 800, 600);
```

## Core Types

```rust
// Data source - columns of f64/String
trait Data {
    fn col(&self, name: &str) -> Option<Col>;
    fn cols(&self) -> Vec<&str>;
    fn len(&self) -> usize;
}

enum Col {
    Num(Vec<f64>),
    Str(Vec<String>),
}

// Aesthetics - maps columns to visual properties
struct Aes {
    x: Option<String>,
    y: Option<String>,
    color: Option<String>,
    fill: Option<String>,
    size: Option<String>,
    shape: Option<String>,
    group: Option<String>,
}

// Geom - geometric layer
enum Geom {
    Point { size: f64 },
    Line { width: f64 },
    Bar,
    Hist { bins: usize },
    Boxplot,
    Area,
    Text,
}

// Scale - axis/color transformations
enum Scale {
    Linear,
    Log,
    Sqrt,
    Discrete,
}

// Facet - small multiples
enum Facet {
    None,
    Wrap { col: String, ncol: usize },
    Grid { row: String, col: String },
}

// Theme
struct Theme {
    bg: Color,
    fg: Color,
    grid: Color,
    palette: Vec<Color>,
}

// Plot builder
struct Plot<D: Data> {
    data: D,
    aes: Aes,
    geoms: Vec<Geom>,
    scales: Scales,
    facet: Facet,
    labs: Labs,
    theme: Theme,
}
```

## Implementation Layers

```
┌─────────────────────────────────────┐
│  User API (plot, aes, geom_*)       │  <- Ergonomic builder
├─────────────────────────────────────┤
│  Plot struct                        │  <- Collects all config
├─────────────────────────────────────┤
│  Render engine                      │  <- Computes layout, draws
│  - compute scales from data+aes     │
│  - layout facets                    │
│  - draw each geom layer             │
├─────────────────────────────────────┤
│  plotters backend                   │  <- Actual drawing
└─────────────────────────────────────┘
```

## Aes Helper Functions

```rust
// These return AesPart for builder pattern
pub fn x(col: &str) -> AesPart { AesPart::X(col.into()) }
pub fn y(col: &str) -> AesPart { AesPart::Y(col.into()) }
pub fn color(col: &str) -> AesPart { AesPart::Color(col.into()) }
pub fn fill(col: &str) -> AesPart { AesPart::Fill(col.into()) }
pub fn size(col: &str) -> AesPart { AesPart::Size(col.into()) }
```

## Data Sources

```rust
// Vec of structs (via macro or manual impl)
let data = vec![
    ("2024-01", 100.0),
    ("2024-02", 105.0),
];

// HashMap columns
let data = cols![
    "date" => ["2024-01", "2024-02"],
    "price" => [100.0, 105.0],
];

// Future: polars DataFrame
```

## Geom Implementations

| Geom | Required Aes | Optional Aes |
|------|--------------|--------------|
| point | x, y | color, size, shape |
| line | x, y | color, group |
| bar | x, y | fill |
| hist | x | fill, bins |
| boxplot | x, y | fill |
| area | x, y | fill |
| text | x, y, label | color, size |

## Scales

```rust
.scale_x_log()      // log10 transform
.scale_y_sqrt()     // sqrt transform
.scale_color_brewer("Set1")  // color palette
.scale_x_reverse()  // flip axis
```

## Output

```rust
.show()                    // kitty graphics to terminal
.save("plot.png", w, h)    // PNG file
.to_svg()                  // SVG string
```

## Phase 1 (MVP)

- [x] Data trait + HashMap impl
- [ ] Aes with x, y, color
- [ ] Geom: point, line, hist
- [ ] Basic scales (linear, log)
- [ ] Labs (title, x, y labels)
- [ ] show() via kitty protocol
- [ ] save() to PNG

## Phase 2

- [ ] Facet wrap/grid
- [ ] More geoms: bar, area, boxplot
- [ ] Size/shape aesthetics
- [ ] Color palettes
- [ ] Themes (minimal, dark, classic)

## Phase 3

- [ ] Polars DataFrame support
- [ ] Statistical transforms (smooth, density)
- [ ] Annotations
- [ ] Legends
