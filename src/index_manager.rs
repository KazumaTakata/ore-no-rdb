use std::{cell::RefCell, ops::Index, rc::Rc};

use crate::{
    hash_index::HashIndex,
    record_page::{Layout, TableFieldType, TableSchema},
    scan_v2::ScanV2,
    stat_manager_v2::{StatInfoV2, StatManagerV2},
    table_manager_v2::TableManagerV2,
    table_scan_v2::TableScan,
    transaction,
    transaction_v2::TransactionV2,
};

struct IndexManager {
    layout: Layout,
    table_manager: Rc<RefCell<TableManagerV2>>,
    stat_manager: Rc<RefCell<StatManagerV2>>,
}

impl IndexManager {
    fn new(
        table_manager: Rc<RefCell<TableManagerV2>>,
        stat_manager: Rc<RefCell<StatManagerV2>>,
        is_new: bool,
        transaction: Rc<RefCell<TransactionV2>>,
    ) -> Self {
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
            .get_layout("index_catalog".to_string(), transaction);

        IndexManager {
            layout,
            table_manager,
            stat_manager,
        }
    }

    fn create_index(
        &mut self,
        index_name: String,
        table_name: String,
        field_name: String,
        transaction: Rc<RefCell<TransactionV2>>,
    ) {
        let mut table_scan = TableScan::new(table_name.clone(), transaction, self.layout.clone());
        table_scan.insert();
        table_scan.set_string("index_name".to_string(), index_name.clone());
        table_scan.set_string("table_name".to_string(), table_name.clone());
        table_scan.set_string("field_name".to_string(), field_name.clone());
        table_scan.close();
    }
}

struct IndexInfo {
    index_name: String,
    field_name: String,
    schema: TableSchema,
    layout: Layout,
    stat_info: StatInfoV2,
    transaction: Rc<RefCell<TransactionV2>>,
}

impl IndexInfo {
    fn new(
        index_name: String,
        field_name: String,
        schema: TableSchema,
        layout: Layout,
        stat_info: StatInfoV2,
        transaction: Rc<RefCell<TransactionV2>>,
    ) -> Self {
        IndexInfo {
            index_name,
            field_name,
            schema,
            layout,
            stat_info,
            transaction,
        }
    }

    fn open(&mut self) -> HashIndex {
        self.schema = TableSchema::new();
        HashIndex::new(
            self.transaction.clone(),
            self.index_name.clone(),
            self.layout.clone(),
        )
    }

    fn blocks_accessed(&self) -> i32 {
        let record_per_block =
            self.transaction.borrow().get_block_size() as i32 / self.layout.get_slot_size();
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

    fn create_index_layout(&self, table_schema: &TableSchema) -> Layout {
        let mut schema = TableSchema::new();
        schema.add_integer_field("block".to_string());
        schema.add_integer_field("id".to_string());

        let field_type = table_schema.get_field_type(self.field_name.clone());

        match field_type {
            Some(ft) => match ft {
                TableFieldType::INTEGER => {
                    schema.add_integer_field("data_value".to_string());
                }
                TableFieldType::VARCHAR => {
                    schema.add_string_field("data_value".to_string(), 20);
                }
            },
            None => panic!("Field {} not found in table schema", self.field_name),
        }

        return Layout::new(schema);
    }
}
