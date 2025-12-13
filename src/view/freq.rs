use crate::app::AppContext;
use crate::command::Command;
use crate::command::transform::FilterIn;
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
                // Filter parent by selected value(s)
                let info = app.view().and_then(|view| {
                    let freq_col = view.freq_col.clone()?;
                    let rows: Vec<usize> = if view.selected_rows.is_empty() {
                        vec![view.state.cr]
                    } else {
                        view.selected_rows.iter().copied().collect()
                    };
                    let values: Vec<String> = rows.iter()
                        .filter_map(|&r| view.dataframe.get_columns()[0].get(r).ok().map(|v| unquote(&v.to_string())))
                        .collect();
                    Some((freq_col, values, view.filename.clone()))
                });

                info.map(|(col, values, filename)| {
                    Box::new(FreqEnter { col, values, filename }) as Box<dyn Command>
                })
            }
            _ => None,
        }
    }
}

/// Freq Enter: pop view and filter parent by selected values
pub struct FreqEnter {
    pub col: String,
    pub values: Vec<String>,
    pub filename: Option<String>,
}

impl Command for FreqEnter {
    fn exec(&mut self, app: &mut AppContext) -> anyhow::Result<()> {
        use crate::command::executor::CommandExecutor;

        let _ = CommandExecutor::exec(app, Box::new(Pop));

        if !self.values.is_empty() {
            let _ = CommandExecutor::exec(app, Box::new(FilterIn {
                col: self.col.clone(),
                values: self.values.clone(),
                filename: self.filename.clone(),
            }));
            // Focus on the freq column in filtered view
            if let Some(v) = app.view_mut() {
                if let Some(idx) = v.dataframe.get_column_names().iter().position(|c| c.as_str() == self.col) {
                    v.state.cc = idx;
                }
            }
        }
        Ok(())
    }
    fn to_str(&self) -> String { "freq_enter".to_string() }
    fn record(&self) -> bool { false }
}
