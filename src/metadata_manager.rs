use std::{cell::RefCell, collections::HashMap, os::macos::raw::stat, rc::Rc};

use crate::{
    index_manager::{self, IndexInfo, IndexManager},
    record_page::{Layout, TableSchema},
    stat_manager_v2::{StatInfoV2, StatManagerV2},
    table_manager_v2::TableManagerV2,
    transaction, transaction_v2,
};

pub struct MetadataManager {
    table_manager: Rc<RefCell<TableManagerV2>>,
    stat_manager: Rc<RefCell<StatManagerV2>>,
    index_manager: Rc<RefCell<IndexManager>>,
}

impl MetadataManager {
    pub fn new(is_new: bool, transaction: Rc<RefCell<transaction_v2::TransactionV2>>) -> Self {
        let table_manager = Rc::new(RefCell::new(TableManagerV2::new()));
        let stat_manager = Rc::new(RefCell::new(StatManagerV2::new(table_manager.clone())));
        let index_manager = Rc::new(RefCell::new(IndexManager::new(
            table_manager.clone(),
            stat_manager.clone(),
            is_new,
            transaction.clone(),
        )));

        let copied_table_manager = table_manager.clone();

        let borrowed_table_manager = copied_table_manager.borrow_mut();

        if is_new {
            borrowed_table_manager.create_table(
                "table_catalog".to_string(),
                &borrowed_table_manager.table_catalog_layout.schema.clone(),
                transaction.clone(),
            );
            borrowed_table_manager.create_table(
                "field_catalog".to_string(),
                &borrowed_table_manager.field_catalog_layout.schema.clone(),
                transaction.clone(),
            );
        }

        MetadataManager {
            table_manager: table_manager,
            stat_manager: stat_manager,
            index_manager: index_manager,
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
            .borrow_mut()
            .get_table_stats(table_name, transaction, layout)
    }

    pub fn create_index(
        &mut self,
        index_name: String,
        table_name: String,
        field_name: String,
        transaction: Rc<RefCell<transaction_v2::TransactionV2>>,
    ) {
        self.index_manager.borrow_mut().create_index(
            index_name,
            table_name,
            field_name,
            transaction,
        );
    }

    pub fn get_index_info(
        &self,
        table_name: String,
        transaction: Rc<RefCell<transaction_v2::TransactionV2>>,
    ) -> HashMap<String, IndexInfo> {
        self.index_manager
            .borrow()
            .get_index_info(table_name, transaction)
    }
}
