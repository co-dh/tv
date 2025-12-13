use crate::app::AppContext;
use crate::command::Command;
use crate::command::transform::Xkey;
use crate::command::view::Pop;
use crate::view::handler::ViewHandler;

fn unquote(s: &str) -> String {
    s.trim_matches('"').to_string()
}

pub struct Handler;

impl ViewHandler for Handler {
    fn handle(&self, cmd: &str, app: &mut AppContext) -> Option<Box<dyn Command>> {
        match cmd {
            "enter" => {
                // Get selected column names from meta view
                let col_names: Vec<String> = app.view().map(|v| {
                    let rows: Vec<usize> = if v.selected_rows.is_empty() {
                        vec![v.state.cr]
                    } else {
                        let mut r: Vec<_> = v.selected_rows.iter().copied().collect();
                        r.sort();
                        r
                    };
                    rows.iter()
                        .filter_map(|&r| v.dataframe.get_columns()[0].get(r).ok().map(|v| unquote(&v.to_string())))
                        .collect()
                }).unwrap_or_default();

                if col_names.is_empty() { return None; }

                // Pop meta view first, then handle column focus/xkey
                Some(Box::new(MetaEnter { col_names }))
            }
            "delete" => {
                // Delete selected columns from parent
                let col_names: Vec<String> = app.view().map(|v| {
                    let rows: Vec<usize> = if v.selected_rows.is_empty() {
                        vec![v.state.cr]
                    } else {
                        let mut r: Vec<_> = v.selected_rows.iter().copied().collect();
                        r.sort_by(|a, b| b.cmp(a));
                        r
                    };
                    rows.iter()
                        .filter_map(|&r| v.dataframe.get_columns()[0].get(r).ok().map(|v| unquote(&v.to_string())))
                        .collect()
                }).unwrap_or_default();

                if col_names.is_empty() { return None; }

                Some(Box::new(MetaDelete { col_names }))
            }
            _ => None,
        }
    }
}

/// Meta Enter: pop view and focus/xkey columns
pub struct MetaEnter { pub col_names: Vec<String> }

impl Command for MetaEnter {
    fn exec(&mut self, app: &mut AppContext) -> anyhow::Result<()> {
        use crate::command::executor::CommandExecutor;

        let _ = CommandExecutor::exec(app, Box::new(Pop));

        if self.col_names.len() == 1 {
            // Single column: just focus
            if let Some(v) = app.view_mut() {
                if let Some(idx) = v.dataframe.get_column_names().iter().position(|c| c.as_str() == self.col_names[0]) {
                    v.state.cc = idx;
                }
            }
        } else {
            // Multiple columns: use xkey
            let _ = CommandExecutor::exec(app, Box::new(Xkey { col_names: self.col_names.clone() }));
        }
        Ok(())
    }
    fn to_str(&self) -> String { "meta_enter".to_string() }
    fn record(&self) -> bool { false }
}

/// Meta Delete: delete columns from parent
pub struct MetaDelete { pub col_names: Vec<String> }

impl Command for MetaDelete {
    fn exec(&mut self, app: &mut AppContext) -> anyhow::Result<()> {
        use crate::command::executor::CommandExecutor;

        let n = self.col_names.len();
        let parent_id = app.view().and_then(|v| v.parent_id);

        if let Some(pid) = parent_id {
            if let Some(parent) = app.stack.find_mut(pid) {
                // Adjust col_separator for deleted cols
                if let Some(sep) = parent.col_separator {
                    let all: Vec<String> = parent.dataframe.get_column_names().iter().map(|s| s.to_string()).collect();
                    let adj = self.col_names.iter()
                        .filter(|c| all.iter().position(|n| n == *c).map(|i| i < sep).unwrap_or(false))
                        .count();
                    parent.col_separator = Some(sep.saturating_sub(adj));
                }
                for c in &self.col_names {
                    let _ = parent.dataframe.drop_in_place(c);
                }
            }
        }

        let _ = CommandExecutor::exec(app, Box::new(Pop));
        app.msg(format!("{} columns deleted", n));
        Ok(())
    }
    fn to_str(&self) -> String { format!("meta_delete {}", self.col_names.join(",")) }
    fn record(&self) -> bool { false }
}
