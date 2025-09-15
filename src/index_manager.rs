use std::{cell::RefCell, collections::HashMap, ops::Index, rc::Rc};

use crate::{
    error::ValueNotFound,
    hash_index::HashIndex,
    record_page::{Layout, TableFieldType, TableSchema},
    scan_v2::ScanV2,
    stat_manager_v2::{StatInfoV2, StatManagerV2},
    table_manager_v2::TableManagerV2,
    table_scan_v2::TableScan,
    transaction,
    transaction_v2::TransactionV2,
};

pub struct IndexManager {
    layout: Layout,
    table_manager: Rc<RefCell<TableManagerV2>>,
    stat_manager: Rc<RefCell<StatManagerV2>>,
}

impl IndexManager {
    pub fn new(
        table_manager: Rc<RefCell<TableManagerV2>>,
        stat_manager: Rc<RefCell<StatManagerV2>>,
        is_new: bool,
        transaction: Rc<RefCell<TransactionV2>>,
    ) -> Result<Self, ValueNotFound> {
        if is_new {
            let field_length = 20; // Example field length
            let mut schema = TableSchema::new();
            schema.add_string_field("index_name".to_string(), field_length);
            schema.add_string_field("table_name".to_string(), field_length);
            schema.add_string_field("field_name".to_string(), field_length);
            table_manager.borrow_mut().create_table(
                "index_catalog".to_string(),
                &schema,
                transaction.clone(),
            );
        }

        let layout = table_manager
            .borrow()
            .get_layout("index_catalog".to_string(), transaction)?;

        return Ok(IndexManager {
            layout,
            table_manager,
            stat_manager,
        });
    }

    pub fn create_index(
        &mut self,
        index_name: String,
        table_name: String,
        field_name: String,
        transaction: Rc<RefCell<TransactionV2>>,
    ) {
        let mut table_scan = TableScan::new(
            "index_catalog".to_string(),
            transaction,
            self.layout.clone(),
        );
        table_scan.insert();
        table_scan.set_string("index_name".to_string(), index_name.clone());
        table_scan.set_string("table_name".to_string(), table_name.clone());
        table_scan.set_string("field_name".to_string(), field_name.clone());
        table_scan.close();
    }

    pub fn get_index_info(
        &self,
        table_name: String,
        transaction: Rc<RefCell<TransactionV2>>,
    ) -> Result<HashMap<String, IndexInfo>, ValueNotFound> {
        let mut table_scan = TableScan::new(
            "index_catalog".to_string(),
            transaction.clone(),
            self.layout.clone(),
        );

        let mut field_name_index_info_map = HashMap::new();

        while table_scan.next()? {
            if table_scan.get_string("table_name".to_string()) == Some(table_name.clone()) {
                let index_name = table_scan.get_string("index_name".to_string()).unwrap();
                let field_name = table_scan.get_string("field_name".to_string()).unwrap();
                let layout = self
                    .table_manager
                    .borrow()
                    .get_layout(table_name.clone(), transaction.clone())?;
                let stat_info = self.stat_manager.borrow_mut().get_table_stats(
                    table_name.clone(),
                    transaction.clone(),
                    layout.clone(),
                )?;
                let index_info = IndexInfo::new(
                    index_name,
                    field_name.clone(),
                    layout.schema.clone(),
                    stat_info,
                    transaction.clone(),
                );
                field_name_index_info_map.insert(field_name.clone(), index_info);
            }
        }

        table_scan.close();
        return Ok(field_name_index_info_map);
    }
}

pub struct IndexInfo {
    index_name: String,
    field_name: String,
    schema: TableSchema,
    stat_info: StatInfoV2,
    transaction: Rc<RefCell<TransactionV2>>,
    index_layout: Layout,
}

impl IndexInfo {
    pub fn new(
        index_name: String,
        field_name: String,
        tableSchema: TableSchema,
        stat_info: StatInfoV2,
        transaction: Rc<RefCell<TransactionV2>>,
    ) -> Self {
        let index_layout = IndexInfo::create_index_layout(&tableSchema, field_name.clone());

        IndexInfo {
            index_name,
            field_name,
            schema: tableSchema,
            index_layout,
            stat_info,
            transaction,
        }
    }

    pub fn open(&mut self) -> HashIndex {
        self.schema = TableSchema::new();
        HashIndex::new(
            self.transaction.clone(),
            self.index_name.clone(),
            self.index_layout.clone(),
        )
    }

    fn blocks_accessed(&self) -> i32 {
        let record_per_block =
            self.transaction.borrow().get_block_size() as i32 / self.index_layout.get_slot_size();
        let number_of_blocks = self.stat_info.get_num_records() / record_per_block as u32;
        return HashIndex::get_search_cost(number_of_blocks as i32);
    }

    fn records_output(&self) -> u32 {
        self.stat_info.get_num_records() / self.stat_info.distinct_value(self.field_name.clone())
    }

    fn distinct_values(&self, field_name: &str) -> u32 {
        if field_name != self.field_name {
            return self.stat_info.distinct_value(self.field_name.clone());
        }
        return 1;
    }

    pub fn create_index_layout(table_schema: &TableSchema, field_name: String) -> Layout {
        let mut schema = TableSchema::new();
        schema.add_integer_field("block".to_string());
        schema.add_integer_field("id".to_string());

        let field_type = table_schema.get_field_type(field_name.clone());

        match field_type {
            Some(ft) => match ft {
                TableFieldType::INTEGER => {
                    schema.add_integer_field("data_value".to_string());
                }
                TableFieldType::VARCHAR => {
                    schema.add_string_field("data_value".to_string(), 20);
                }
            },
            None => panic!("Field {} not found in table schema", field_name),
        }

        return Layout::new(schema);
    }
}
