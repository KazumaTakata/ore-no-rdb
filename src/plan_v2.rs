use std::{cell::RefCell, rc::Rc};

use crate::{
    record_page::{Layout, TableSchema},
    scan_v2::ScanV2,
    stat_manager_v2::{StatInfoV2, StatManagerV2},
    table_manager_v2::TableManagerV2,
    table_scan_v2::TableScan,
    transaction_v2::TransactionV2,
};

pub trait Plan {
    fn open(&self) -> Box<dyn ScanV2>;
    fn get_schema(&self) -> &TableSchema;

    fn blocks_accessed(&self) -> u32;

    fn records_output(&self) -> u32;

    fn get_distinct_value(&self, field_name: String) -> u32;
}

struct TablePlanV2 {
    // Fields for the plan
    table_name: String,
    layout: Layout,
    stat_info: StatInfoV2,
    transaction: Rc<RefCell<TransactionV2>>,
}

impl TablePlanV2 {
    pub fn new(
        table_name: String,
        transaction: Rc<RefCell<TransactionV2>>,
        table_manager: &mut TableManagerV2,
        stat_manager: &mut StatManagerV2,
    ) -> Self {
        let layout = table_manager.get_layout(table_name.clone(), transaction.clone());
        let stat_info =
            stat_manager.get_table_stats(table_name.clone(), transaction.clone(), layout.clone());

        TablePlanV2 {
            table_name,
            layout,
            stat_info,
            transaction: transaction.clone(),
        }
    }
}

impl Plan for TablePlanV2 {
    fn open(&self) -> Box<dyn ScanV2> {
        return Box::new(TableScan::new(
            self.table_name.clone(),
            self.transaction.clone(),
            self.layout.clone(),
        ));
    }

    fn get_schema(&self) -> &TableSchema {
        &self.layout.schema
    }

    fn blocks_accessed(&self) -> u32 {
        self.stat_info.get_num_blocks()
    }

    fn records_output(&self) -> u32 {
        self.stat_info.get_num_records()
    }

    fn get_distinct_value(&self, field_name: String) -> u32 {
        self.stat_info.distinct_value(field_name)
    }
}
