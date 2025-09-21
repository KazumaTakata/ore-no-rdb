use std::{cell::RefCell, collections::HashMap, os::macos::raw::stat, rc::Rc};

use crate::{
    error::{TableAlreadyExists, ValueNotFound},
    index_manager::{self, IndexInfo, IndexManager},
    parser::QueryData,
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
    pub fn new(
        transaction: Rc<RefCell<transaction_v2::TransactionV2>>,
    ) -> Result<Self, ValueNotFound> {
        let table_manager = Rc::new(RefCell::new(TableManagerV2::new()));
        let stat_manager = Rc::new(RefCell::new(StatManagerV2::new(table_manager.clone())));

        let _index_manager = index_manager::IndexManager::new(
            table_manager.clone(),
            stat_manager.clone(),
            transaction.clone(),
        )?;

        let index_manager = Rc::new(RefCell::new(_index_manager));

        let copied_table_manager = table_manager.clone();

        let borrowed_table_manager = copied_table_manager.borrow_mut();

        //  すでにテーブルが存在する場合はエラーを無視する
        let _table_catalog_result = borrowed_table_manager.create_table(
            "table_catalog".to_string(),
            &borrowed_table_manager.table_catalog_layout.schema.clone(),
            transaction.clone(),
        );

        //  すでにテーブルが存在する場合はエラーを無視する
        let _table_field_result = borrowed_table_manager.create_table(
            "field_catalog".to_string(),
            &borrowed_table_manager.field_catalog_layout.schema.clone(),
            transaction.clone(),
        );

        Ok(MetadataManager {
            table_manager: table_manager,
            stat_manager: stat_manager,
            index_manager: index_manager,
        })
    }

    pub fn create_table(
        &mut self,
        table_name: String,
        schema: &TableSchema,
        transaction: Rc<RefCell<transaction_v2::TransactionV2>>,
    ) -> Result<(), TableAlreadyExists> {
        self.table_manager
            .borrow_mut()
            .create_table(table_name, schema, transaction)
    }

    pub fn validate_select_sql(
        &self,
        query_data: &QueryData,
        transaction: Rc<RefCell<transaction_v2::TransactionV2>>,
    ) -> bool {
        self.table_manager
            .borrow()
            .check_if_table_exists(query_data.table_name_list[0].clone(), transaction.clone());

        match query_data.field_name_list[0].table_name {
            Some(ref table_name) => {
                return self.table_manager.borrow().check_if_field_exists(
                    table_name.clone(),
                    query_data.field_name_list[0].field_name.clone(),
                    transaction.clone(),
                )
            }
            None => {
                return self.table_manager.borrow().check_if_field_exists(
                    query_data.table_name_list[0].clone(),
                    query_data.field_name_list[0].field_name.clone(),
                    transaction.clone(),
                );
            }
        }
    }

    pub fn get_layout(
        &self,
        table_name: String,
        transaction: Rc<RefCell<transaction_v2::TransactionV2>>,
    ) -> Result<Layout, ValueNotFound> {
        self.table_manager
            .borrow()
            .get_layout(table_name, transaction)
    }

    pub fn get_table_stats(
        &mut self,
        table_name: String,
        transaction: Rc<RefCell<transaction_v2::TransactionV2>>,
        layout: Layout,
    ) -> Result<StatInfoV2, ValueNotFound> {
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
    ) -> Result<HashMap<String, IndexInfo>, ValueNotFound> {
        self.index_manager
            .borrow()
            .get_index_info(table_name, transaction)
    }
}
