//! plotgram - Grammar of Graphics for Rust
//!
//! ```rust
//! use plotgram::*;
//!
//! // Basic scatter plot
//! plot(&data)
//!     .aes(x("mpg"), y("hp"))
//!     .geom_point()
//!     .save("scatter.png");
//!
//! // Line chart with color grouping
//! plot(&data)
//!     .aes(x("date"), y("price"), color("symbol"))
//!     .geom_line()
//!     .labs("Stock Prices", "Date", "Price")
//!     .save("stocks.png");
//!
//! // Histogram
//! plot(&data)
//!     .aes(x("value"))
//!     .geom_histogram(20)
//!     .save("hist.png");
//! ```

mod aes;
mod data;
mod geom;
mod plot;
mod render;
mod scale;
mod theme;

pub use aes::*;
pub use data::*;
pub use geom::*;
pub use plot::*;
pub use scale::*;
pub use theme::*;
