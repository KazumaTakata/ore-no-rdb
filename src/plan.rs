use crate::record_page::{Layout, TableSchema};

struct TablePlan {
    // Fields for the plan
    table_name: String,
    layout: Layout,
}

impl TablePlan {
    pub fn new(table_name: String, layout: Layout) -> Self {
        TablePlan { table_name, layout }
    }

    pub fn get_table_name(&self) -> &String {
        &self.table_name
    }

    pub fn get_schema(&self) -> &TableSchema {
        &self.layout.schema
    }
}
