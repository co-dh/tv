//! Table abstraction - separates TUI from data backend
//! TUI code should only use this trait, never backend directly.

/// Cell value for display
#[derive(Clone, Debug)]
pub enum Cell {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Str(String),
    Date(String),    // formatted date
    Time(String),    // formatted time
    DateTime(String), // formatted datetime
}

impl Cell {
    /// Format cell for display with given decimal places
    pub fn format(&self, decimals: usize) -> String {
        match self {
            Cell::Null => "null".into(),
            Cell::Bool(b) => if *b { "true" } else { "false" }.into(),
            Cell::Int(n) => n.to_string(),
            Cell::Float(f) => format!("{:.prec$}", f, prec = decimals),
            Cell::Str(s) => s.clone(),
            Cell::Date(s) | Cell::Time(s) | Cell::DateTime(s) => s.clone(),
        }
    }

    /// Check if numeric
    pub fn is_numeric(&self) -> bool {
        matches!(self, Cell::Int(_) | Cell::Float(_))
    }
}

/// Column type for display/formatting decisions
#[derive(Clone, Debug, PartialEq)]
pub enum ColType {
    Bool,
    Int,
    Float,
    Str,
    Date,
    Time,
    DateTime,
    Other(String),
}

impl ColType {
    pub fn is_numeric(&self) -> bool {
        matches!(self, ColType::Int | ColType::Float)
    }
}

/// Table data abstraction - implemented by backend
pub trait Table: Send + Sync {
    /// Number of rows
    fn rows(&self) -> usize;
    /// Number of columns
    fn cols(&self) -> usize;
    /// Column name by index
    fn col_name(&self, idx: usize) -> Option<String>;
    /// All column names
    fn col_names(&self) -> Vec<String>;
    /// Column type by index
    fn col_type(&self, idx: usize) -> ColType;
    /// Get cell value
    fn cell(&self, row: usize, col: usize) -> Cell;
    /// Check if empty
    fn is_empty(&self) -> bool { self.rows() == 0 }
    /// Get column width hint (max chars in sample)
    fn col_width(&self, idx: usize, sample_rows: usize) -> usize;
}

/// Boxed table for storage
pub type BoxTable = Box<dyn Table>;

/// Empty table placeholder
#[allow(dead_code)]
pub struct EmptyTable;

impl Table for EmptyTable {
    fn rows(&self) -> usize { 0 }
    fn cols(&self) -> usize { 0 }
    fn col_name(&self, _: usize) -> Option<String> { None }
    fn col_names(&self) -> Vec<String> { vec![] }
    fn col_type(&self, _: usize) -> ColType { ColType::Str }
    fn cell(&self, _: usize, _: usize) -> Cell { Cell::Null }
    fn col_width(&self, _: usize, _: usize) -> usize { 0 }
}

/// Simple in-memory table (Vec of rows)
#[derive(Clone, Debug)]
pub struct SimpleTable {
    pub names: Vec<String>,
    pub types: Vec<ColType>,
    pub data: Vec<Vec<Cell>>,
}

impl SimpleTable {
    pub fn new(names: Vec<String>, types: Vec<ColType>, data: Vec<Vec<Cell>>) -> Self {
        Self { names, types, data }
    }

    pub fn empty() -> Self {
        Self { names: vec![], types: vec![], data: vec![] }
    }

    /// Append rows from another table (for streaming)
    pub fn append(&mut self, other: &dyn Table) {
        for r in 0..other.rows() {
            let row: Vec<Cell> = (0..other.cols()).map(|c| other.cell(r, c)).collect();
            self.data.push(row);
        }
    }

    /// Build table from columns (columnar layout)
    pub fn from_cols(cols: Vec<Col>) -> Self {
        if cols.is_empty() { return Self::empty(); }
        let n_rows = cols[0].len();
        let names: Vec<String> = cols.iter().map(|c| c.name.clone()).collect();
        let types: Vec<ColType> = cols.iter().map(|c| c.typ.clone()).collect();
        let data: Vec<Vec<Cell>> = (0..n_rows)
            .map(|r| cols.iter().map(|c| c.cells.get(r).cloned().unwrap_or(Cell::Null)).collect())
            .collect();
        Self { names, types, data }
    }
}

/// Column for building SimpleTable
pub struct Col {
    pub name: String,
    pub typ: ColType,
    pub cells: Vec<Cell>,
}

impl Col {
    /// String column
    pub fn str(name: &str, data: Vec<String>) -> Self {
        Self { name: name.into(), typ: ColType::Str, cells: data.into_iter().map(Cell::Str).collect() }
    }
    /// Int column
    pub fn int(name: &str, data: Vec<i64>) -> Self {
        Self { name: name.into(), typ: ColType::Int, cells: data.into_iter().map(Cell::Int).collect() }
    }
    /// Float column
    pub fn float(name: &str, data: Vec<f64>) -> Self {
        Self { name: name.into(), typ: ColType::Float, cells: data.into_iter().map(Cell::Float).collect() }
    }
    /// Bool column
    pub fn bool(name: &str, data: Vec<bool>) -> Self {
        Self { name: name.into(), typ: ColType::Bool, cells: data.into_iter().map(Cell::Bool).collect() }
    }
    /// Length of column
    pub fn len(&self) -> usize { self.cells.len() }
}

impl Table for SimpleTable {
    fn rows(&self) -> usize { self.data.len() }
    fn cols(&self) -> usize { self.names.len() }
    fn col_name(&self, idx: usize) -> Option<String> { self.names.get(idx).cloned() }
    fn col_names(&self) -> Vec<String> { self.names.clone() }
    fn col_type(&self, idx: usize) -> ColType { self.types.get(idx).cloned().unwrap_or(ColType::Str) }
    fn cell(&self, row: usize, col: usize) -> Cell {
        self.data.get(row).and_then(|r| r.get(col)).cloned().unwrap_or(Cell::Null)
    }
    fn col_width(&self, idx: usize, sample: usize) -> usize {
        let header = self.col_name(idx).map(|s| s.len()).unwrap_or(0);
        let max_data = self.data.iter().take(sample)
            .filter_map(|r| r.get(idx))
            .map(|c| c.format(3).len())
            .max().unwrap_or(0);
        header.max(max_data).max(3)
    }
}

/// Statistics for a column (used by meta view and status bar)
#[derive(Clone, Debug, Default)]
pub struct ColStats {
    pub name: String,
    pub dtype: String,
    pub null_pct: f64,
    pub distinct: usize,
    pub min: String,
    pub max: String,
    pub median: String,
    pub sigma: String,
}

impl ColStats {
    /// Format stats for status bar display
    #[must_use]
    pub fn format(&self) -> String {
        if !self.sigma.is_empty() {
            // Numeric: show [min,mean,max] σstd
            if self.null_pct > 0.0 {
                format!("null:{:.0}% [{},{},{}] σ{}", self.null_pct, self.min, self.median, self.max, self.sigma)
            } else {
                format!("[{},{},{}] σ{}", self.min, self.median, self.max, self.sigma)
            }
        } else if self.distinct > 0 {
            // String: show #unique 'mode'
            let mode = if self.max.len() > 10 { &self.max[..10] } else { &self.max };
            if self.null_pct > 0.0 {
                format!("null:{:.0}% #{}'{}'", self.null_pct, self.distinct, mode)
            } else {
                format!("#{}'{}'", self.distinct, mode)
            }
        } else { String::new() }
    }
}
