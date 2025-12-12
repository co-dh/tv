use crate::app::AppContext;
use crate::command::Command;
use anyhow::Result;
use polars::prelude::*;

/// Delete columns
pub struct DelCol { pub col_names: Vec<String> }

impl Command for DelCol {
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        let v = app.req_mut()?;
        for c in &self.col_names { v.dataframe = v.dataframe.drop(c)?; }
        if v.state.cc >= v.cols() && v.cols() > 0 { v.state.cc = v.cols() - 1; }
        Ok(())
    }
    fn to_str(&self) -> String { format!("delcol {}", self.col_names.join(",")) }
}

/// Filter rows using SQL WHERE syntax
pub struct Filter { pub expr: String }

impl Command for Filter {
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        let (filtered, filename) = {
            let v = app.req()?;
            let mut ctx = polars::sql::SQLContext::new();
            ctx.register("df", v.dataframe.clone().lazy());
            let sql = format!("SELECT * FROM df WHERE {}", self.expr);
            (ctx.execute(&sql)?.collect()?, v.filename.clone())
        };
        let id = app.next_id();
        app.stack.push(crate::state::ViewState::new(id, self.expr.clone(), filtered, filename));
        Ok(())
    }
    fn to_str(&self) -> String { format!("filter {}", self.expr) }
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

    fn df() -> DataFrame {
        df! { "name" => &["apple", "banana", "cherry", "pineapple", "grape", "blueberry"] }.unwrap()
    }

    fn filt(e: &str) -> usize {
        let mut ctx = polars::sql::SQLContext::new();
        ctx.register("df", df().lazy());
        ctx.execute(&format!("SELECT * FROM df WHERE {}", e)).unwrap().collect().unwrap().height()
    }

    #[test]
    fn test_str_exact() { assert_eq!(filt("name = 'apple'"), 1); }

    #[test]
    fn test_str_contains() { assert_eq!(filt("name LIKE '%apple%'"), 2); }  // apple, pineapple

    #[test]
    fn test_str_ends() { assert_eq!(filt("name LIKE '%rry'"), 2); }  // cherry, blueberry

    #[test]
    fn test_str_starts() { assert_eq!(filt("name LIKE 'b%'"), 2); }  // banana, blueberry
}
