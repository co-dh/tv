//! tv-plot - ggplot-style plotting for terminal
//!
//! ```ignore
//! use tv_plot::*;
//!
//! ggplot(data)
//!     + aes(x("price"), y("volume"))
//!     + geom_point()
//!     + geom_line()
//!     + labs(title("Stock"), x("Price"), y("Vol"))
//!     + theme_dark()
//! ```

mod render;

pub use render::kitty_show;

/// Data frame - columns of f64
pub struct DataFrame {
    pub cols: Vec<(String, Vec<f64>)>,
}

impl DataFrame {
    pub fn new() -> Self { Self { cols: Vec::new() } }

    pub fn col(mut self, name: &str, data: Vec<f64>) -> Self {
        self.cols.push((name.into(), data));
        self
    }

    pub fn get(&self, name: &str) -> Option<&[f64]> {
        self.cols.iter().find(|(n, _)| n == name).map(|(_, v)| v.as_slice())
    }

    pub fn rows(&self) -> usize {
        self.cols.first().map(|(_, v)| v.len()).unwrap_or(0)
    }
}

impl Default for DataFrame { fn default() -> Self { Self::new() } }

/// Aesthetic mapping
#[derive(Clone, Default)]
pub struct Aes {
    pub x: Option<String>,
    pub y: Option<String>,
    pub color: Option<String>,
    pub fill: Option<String>,
    pub size: Option<String>,
}

/// Create x aesthetic
pub fn x(col: &str) -> Aes { Aes { x: Some(col.into()), ..Default::default() } }

/// Create y aesthetic
pub fn y(col: &str) -> Aes { Aes { y: Some(col.into()), ..Default::default() } }

/// Combine aesthetics
impl std::ops::Add for Aes {
    type Output = Aes;
    fn add(mut self, rhs: Aes) -> Aes {
        if rhs.x.is_some() { self.x = rhs.x; }
        if rhs.y.is_some() { self.y = rhs.y; }
        if rhs.color.is_some() { self.color = rhs.color; }
        if rhs.fill.is_some() { self.fill = rhs.fill; }
        if rhs.size.is_some() { self.size = rhs.size; }
        self
    }
}

/// Shorthand for x + y
pub fn aes(x_col: &str, y_col: &str) -> Aes {
    Aes { x: Some(x_col.into()), y: Some(y_col.into()), ..Default::default() }
}

/// Geometry types
#[derive(Clone)]
pub enum Geom {
    Point { size: u32, color: Option<Color> },
    Line { width: u32, color: Option<Color> },
    Bar { color: Option<Color> },
    Histogram { bins: usize, color: Option<Color> },
    Boxplot { color: Option<Color> },
    Area { color: Option<Color>, alpha: f32 },
}

pub fn geom_point() -> Geom { Geom::Point { size: 3, color: None } }
pub fn geom_line() -> Geom { Geom::Line { width: 2, color: None } }
pub fn geom_bar() -> Geom { Geom::Bar { color: None } }
pub fn geom_histogram(bins: usize) -> Geom { Geom::Histogram { bins, color: None } }
pub fn geom_boxplot() -> Geom { Geom::Boxplot { color: None } }
pub fn geom_area() -> Geom { Geom::Area { color: None, alpha: 0.3 } }

/// RGB color
#[derive(Clone, Copy)]
pub struct Color(pub u8, pub u8, pub u8);

impl Color {
    pub const BLUE: Color = Color(65, 105, 225);
    pub const RED: Color = Color(220, 20, 60);
    pub const GREEN: Color = Color(34, 139, 34);
    pub const ORANGE: Color = Color(255, 140, 0);
    pub const PURPLE: Color = Color(148, 0, 211);
}

/// Labels
#[derive(Clone, Default)]
pub struct Labs {
    pub title: Option<String>,
    pub x: Option<String>,
    pub y: Option<String>,
    pub subtitle: Option<String>,
}

pub fn labs() -> Labs { Labs::default() }
pub fn title(s: &str) -> Labs { Labs { title: Some(s.into()), ..Default::default() } }
pub fn xlab(s: &str) -> Labs { Labs { x: Some(s.into()), ..Default::default() } }
pub fn ylab(s: &str) -> Labs { Labs { y: Some(s.into()), ..Default::default() } }

impl std::ops::Add for Labs {
    type Output = Labs;
    fn add(mut self, rhs: Labs) -> Labs {
        if rhs.title.is_some() { self.title = rhs.title; }
        if rhs.x.is_some() { self.x = rhs.x; }
        if rhs.y.is_some() { self.y = rhs.y; }
        if rhs.subtitle.is_some() { self.subtitle = rhs.subtitle; }
        self
    }
}

/// Theme
#[derive(Clone)]
pub struct Theme {
    pub bg: Color,
    pub fg: Color,
    pub grid: Color,
    pub palette: Vec<Color>,
}

impl Default for Theme {
    fn default() -> Self { theme_light() }
}

pub fn theme_light() -> Theme {
    Theme {
        bg: Color(255, 255, 255),
        fg: Color(30, 30, 30),
        grid: Color(220, 220, 220),
        palette: vec![Color::BLUE, Color::RED, Color::GREEN, Color::ORANGE, Color::PURPLE],
    }
}

pub fn theme_dark() -> Theme {
    Theme {
        bg: Color(30, 30, 30),
        fg: Color(220, 220, 220),
        grid: Color(60, 60, 60),
        palette: vec![Color(100, 149, 237), Color(255, 99, 71), Color(50, 205, 50), Color(255, 215, 0), Color(186, 85, 211)],
    }
}

pub fn theme_minimal() -> Theme {
    Theme {
        bg: Color(255, 255, 255),
        fg: Color(80, 80, 80),
        grid: Color(240, 240, 240),
        palette: vec![Color(50, 50, 50), Color(100, 100, 100), Color(150, 150, 150)],
    }
}

/// Plot layer - what can be added with +
pub enum Layer {
    Aes(Aes),
    Geom(Geom),
    Labs(Labs),
    Theme(Theme),
}

impl From<Aes> for Layer { fn from(a: Aes) -> Self { Layer::Aes(a) } }
impl From<Geom> for Layer { fn from(g: Geom) -> Self { Layer::Geom(g) } }
impl From<Labs> for Layer { fn from(l: Labs) -> Self { Layer::Labs(l) } }
impl From<Theme> for Layer { fn from(t: Theme) -> Self { Layer::Theme(t) } }

/// The plot
pub struct Plot {
    pub data: DataFrame,
    pub aes: Aes,
    pub geoms: Vec<Geom>,
    pub labs: Labs,
    pub theme: Theme,
    pub width: u32,
    pub height: u32,
}

/// Create plot from data
pub fn ggplot(data: DataFrame) -> Plot {
    Plot {
        data,
        aes: Aes::default(),
        geoms: Vec::new(),
        labs: Labs::default(),
        theme: Theme::default(),
        width: 800,
        height: 400,
    }
}

impl Plot {
    /// Set size
    pub fn size(mut self, w: u32, h: u32) -> Self {
        self.width = w;
        self.height = h;
        self
    }

    /// Render and show via kitty protocol
    pub fn show(&self) {
        match render::render(self) {
            Some(img) => {
                eprintln!("DEBUG: rendered {}x{}", img.width(), img.height());
                kitty_show(&img);
            }
            None => eprintln!("DEBUG: render returned None"),
        }
    }

    /// Render to PNG bytes
    pub fn png(&self) -> Option<Vec<u8>> {
        render::render(self).map(|img| render::to_png(&img))
    }
}

/// Add layer with + operator
impl<L: Into<Layer>> std::ops::Add<L> for Plot {
    type Output = Plot;
    fn add(mut self, layer: L) -> Plot {
        match layer.into() {
            Layer::Aes(a) => self.aes = self.aes + a,
            Layer::Geom(g) => self.geoms.push(g),
            Layer::Labs(l) => self.labs = self.labs + l,
            Layer::Theme(t) => self.theme = t,
        }
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_api() {
        let data = DataFrame::new()
            .col("x", vec![1.0, 2.0, 3.0])
            .col("y", vec![1.0, 4.0, 9.0]);

        let _plot = ggplot(data)
            + aes("x", "y")
            + geom_point()
            + geom_line()
            + title("Test")
            + theme_dark();
    }
}
