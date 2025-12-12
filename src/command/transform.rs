use crate::app::AppContext;
use crate::command::Command;
use anyhow::{anyhow, Result};
use polars::prelude::*;

/// Delete column
pub struct DelCol { pub col_name: String }

impl Command for DelCol {
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        let v = app.req_mut()?;
        v.dataframe = v.dataframe.drop(&self.col_name)?;
        if v.state.cc >= v.cols() && v.cols() > 0 { v.state.cc = v.cols() - 1; }
        Ok(())
    }
    fn to_str(&self) -> String { format!("delcol {}", self.col_name) }
}

/// Filter rows
pub struct Filter { pub expression: String }

impl Command for Filter {
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        let (filtered, filename) = {
            let v = app.req()?;
            let df = &v.dataframe;

            // Parse: col op value
            let (col_name, op, val) = self.parse_expr()?;
            let col = df.column(col_name)?.as_materialized_series();

            let mask = match col.dtype() {
                DataType::Int64 | DataType::Int32 | DataType::Int16 | DataType::Int8 |
                DataType::UInt64 | DataType::UInt32 | DataType::UInt16 | DataType::UInt8 |
                DataType::Float64 | DataType::Float32 => {
                    let n: f64 = val.parse().map_err(|_| anyhow!("Invalid number"))?;
                    self.num_mask(col.cast(&DataType::Float64)?.f64()?, op, n)
                }
                DataType::String => self.str_mask(col.str()?, op, val)?,
                _ => return Err(anyhow!("Unsupported type: {:?}", col.dtype())),
            };
            (df.filter(&mask)?, v.filename.clone())
        };
        let id = app.next_id();
        app.stack.push(crate::state::ViewState::new(id, self.expression.clone(), filtered, filename));
        Ok(())
    }
    fn to_str(&self) -> String { format!("filter {}", self.expression) }
}

impl Filter {
    fn parse_expr(&self) -> Result<(&str, &str, &str)> {
        for op in [">=", "<=", "==", ">", "<"] {
            if let Some(i) = self.expression.find(op) {
                return Ok((self.expression[..i].trim(), op, self.expression[i+op.len()..].trim()));
            }
        }
        Err(anyhow!("Invalid filter. Use: col>value, col==value, etc."))
    }

    fn num_mask(&self, c: &Float64Chunked, op: &str, v: f64) -> BooleanChunked {
        match op { ">=" => c.gt_eq(v), "<=" => c.lt_eq(v), "==" => c.equal(v), ">" => c.gt(v), _ => c.lt(v) }
    }

    fn str_mask(&self, c: &StringChunked, op: &str, v: &str) -> Result<BooleanChunked> {
        if op != "==" { return Err(anyhow!("Strings only support ==")); }
        // Glob: *x* (contains), *x (ends), x* (starts), x (exact)
        Ok(if v.starts_with('*') && v.ends_with('*') && v.len() > 2 {
            let p = &v[1..v.len()-1];
            BooleanChunked::from_iter_values("m".into(), c.into_iter().map(|s| s.map(|x| x.contains(p)).unwrap_or(false)))
        } else if v.starts_with('*') && v.len() > 1 {
            let p = &v[1..];
            BooleanChunked::from_iter_values("m".into(), c.into_iter().map(|s| s.map(|x| x.ends_with(p)).unwrap_or(false)))
        } else if v.ends_with('*') && v.len() > 1 {
            let p = &v[..v.len()-1];
            BooleanChunked::from_iter_values("m".into(), c.into_iter().map(|s| s.map(|x| x.starts_with(p)).unwrap_or(false)))
        } else {
            c.equal(v)
        })
    }
}

/// Select columns
pub struct Select { pub col_names: Vec<String> }

impl Command for Select {
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        let v = app.req_mut()?;
        v.dataframe = v.dataframe.select(&self.col_names)?;
        v.state.cc = 0;
        Ok(())
    }
    fn to_str(&self) -> String { format!("sel {}", self.col_names.join(",")) }
}

/// Sort by column
pub struct Sort { pub col_name: String, pub descending: bool }

impl Command for Sort {
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        let v = app.req_mut()?;
        v.dataframe = v.dataframe.sort([&self.col_name], SortMultipleOptions::default().with_order_descending(self.descending))?;
        Ok(())
    }
    fn to_str(&self) -> String { format!("{} {}", if self.descending { "sort_desc" } else { "sort_asc" }, self.col_name) }
}

/// Rename column
pub struct RenameCol { pub old_name: String, pub new_name: String }

impl Command for RenameCol {
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        app.req_mut()?.dataframe.rename(&self.old_name, self.new_name.as_str().into())?;
        Ok(())
    }
    fn to_str(&self) -> String { format!("rename {} {}", self.old_name, self.new_name) }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_df() -> DataFrame {
        df! { "name" => &["apple", "banana", "cherry", "pineapple", "grape", "blueberry"] }.unwrap()
    }

    #[test]
    fn test_str_exact() {
        let df = make_df();
        let f = Filter { expression: "name==apple".into() };
        let m = f.str_mask(df.column("name").unwrap().str().unwrap(), "==", "apple").unwrap();
        assert_eq!(df.filter(&m).unwrap().height(), 1);
    }

    #[test]
    fn test_str_contains() {
        let df = make_df();
        let f = Filter { expression: "name==*apple*".into() };
        let m = f.str_mask(df.column("name").unwrap().str().unwrap(), "==", "*apple*").unwrap();
        assert_eq!(df.filter(&m).unwrap().height(), 2);  // apple, pineapple
    }

    #[test]
    fn test_str_ends() {
        let df = make_df();
        let f = Filter { expression: "name==*rry".into() };
        let m = f.str_mask(df.column("name").unwrap().str().unwrap(), "==", "*rry").unwrap();
        assert_eq!(df.filter(&m).unwrap().height(), 2);  // cherry, blueberry
    }

    #[test]
    fn test_str_starts() {
        let df = make_df();
        let f = Filter { expression: "name==b*".into() };
        let m = f.str_mask(df.column("name").unwrap().str().unwrap(), "==", "b*").unwrap();
        assert_eq!(df.filter(&m).unwrap().height(), 2);  // banana, blueberry
    }
}
