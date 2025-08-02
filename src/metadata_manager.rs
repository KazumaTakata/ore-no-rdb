use std::{cell::RefCell, os::macos::raw::stat, rc::Rc};

use crate::{
    record_page::{Layout, TableSchema},
    stat_manager_v2::{StatInfoV2, StatManagerV2},
    table_manager_v2::TableManagerV2,
    transaction_v2,
};

pub struct MetadataManager {
    table_manager: Rc<RefCell<TableManagerV2>>,
    stat_manager: StatManagerV2,
}

impl MetadataManager {
    pub fn new() -> Self {
        let table_manager = Rc::new(RefCell::new(TableManagerV2::new()));
        let stat_manager = StatManagerV2::new(table_manager.clone());
        MetadataManager {
            table_manager: table_manager,
            stat_manager: stat_manager,
        }
    }

    pub fn create_table(
        &mut self,
        table_name: String,
        schema: &TableSchema,
        transaction: Rc<RefCell<transaction_v2::TransactionV2>>,
    ) {
        self.table_manager
            .borrow_mut()
            .create_table(table_name, schema, transaction);
    }

    pub fn get_layout(
        &self,
        table_name: String,
        transaction: Rc<RefCell<transaction_v2::TransactionV2>>,
    ) -> Layout {
        self.table_manager
            .borrow()
            .get_layout(table_name, transaction)
    }

    pub fn get_table_stats(
        &mut self,
        table_name: String,
        transaction: Rc<RefCell<transaction_v2::TransactionV2>>,
        layout: Layout,
    ) -> StatInfoV2 {
        self.stat_manager
            .get_table_stats(table_name, transaction, layout)
    }
}
