use std::{cell::RefCell, collections::HashMap, rc::Rc};

use crate::{
    error::ValueNotFound,
    hash_index::HashIndex,
    predicate::TableNameAndFieldName,
    record_page::{Layout, TableFieldType, TableSchema},
    scan_v2::ScanV2,
    stat_manager_v2::{StatInfoV2, StatManagerV2},
    table_manager_v2::TableManagerV2,
    table_scan_v2::TableScan,
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
        transaction: Rc<RefCell<TransactionV2>>,
    ) -> Result<Self, ValueNotFound> {
        let table_exists = table_manager
            .borrow()
            .check_if_table_exists("index_catalog".to_string(), transaction.clone());

        if !table_exists {
            let field_length = 20; // Example field length
            let mut schema = TableSchema::new();
            schema.add_string_field("index_name".to_string(), field_length);
            schema.add_string_field("table_name".to_string(), field_length);
            schema.add_string_field("field_name".to_string(), field_length);
            let _ = table_manager.borrow_mut().create_table(
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
            transaction.clone(),
            self.layout.clone(),
        );
        table_scan.insert();
        table_scan.set_string("index_name".to_string(), index_name.clone());
        table_scan.set_string("table_name".to_string(), table_name.clone());
        table_scan.set_string("field_name".to_string(), field_name.clone());
        table_scan.close();

        transaction.borrow_mut().commit();
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
            if table_scan.get_string(TableNameAndFieldName::new(None, "table_name".to_string()))
                == Some(table_name.clone())
            {
                let index_name = table_scan
                    .get_string(TableNameAndFieldName::new(None, "index_name".to_string()))
                    .unwrap();
                let field_name = table_scan
                    .get_string(TableNameAndFieldName::new(None, "field_name".to_string()))
                    .unwrap();

                println!(
                    "Found index: {} on field: {} for table: {}",
                    index_name, field_name, table_name
                );

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

#[derive(Clone)]
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

    pub fn blocks_accessed(&self) -> u32 {
        let record_per_block =
            self.transaction.borrow().get_block_size() as i32 / self.index_layout.get_slot_size();
        let number_of_blocks = self.stat_info.get_num_records() / record_per_block as u32;
        return HashIndex::get_search_cost(number_of_blocks as u32);
    }

    pub fn records_output(&self) -> u32 {
        self.stat_info.get_num_records() / self.stat_info.distinct_value(self.field_name.clone())
    }

    pub fn distinct_values(&self, field_name: &str) -> u32 {
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

#[cfg(test)]
mod tests {
    use std::{cell::RefCell, path::Path, rc::Rc};

    use crate::{
        buffer_manager_v2::BufferManagerV2,
        concurrency_manager::LockTable,
        file_manager::{self, FileManager},
        index_manager::IndexManager,
        log_manager_v2::LogManagerV2,
        record_page::TableSchema,
        scan_v2::ScanV2,
        stat_manager_v2::StatManagerV2,
        table_manager_v2::TableManagerV2,
        table_scan_v2::{self, TableScan},
        transaction_v2::TransactionV2,
        view_manager::ViewManager,
    };

    #[test]
    fn test_index_mgr() {
        let test_dir_name = format!("test_data_{}", uuid::Uuid::new_v4());
        let test_dir = Path::new(&test_dir_name);
        let block_size = 400;

        let log_file_name = format!("log_file_{}.txt", uuid::Uuid::new_v4());

        let file_manager = Rc::new(RefCell::new(FileManager::new(test_dir, block_size)));
        let log_manager = Rc::new(RefCell::new(LogManagerV2::new(
            file_manager.clone(),
            log_file_name.clone(),
        )));

        let buffer_manager = Rc::new(RefCell::new(BufferManagerV2::new(
            100,
            file_manager.clone(),
            log_manager.clone(),
        )));

        let lock_table = Rc::new(RefCell::new(LockTable::new()));

        let transaction = Rc::new(RefCell::new(TransactionV2::new(
            1,
            file_manager.clone(),
            buffer_manager.clone(),
            lock_table.clone(),
            log_manager.clone(),
        )));

        let table_manager = Rc::new(RefCell::new(TableManagerV2::new(transaction.clone(), true)));

        let mut student_shema = TableSchema::new();
        student_shema.add_integer_field("sid".to_string());
        student_shema.add_string_field("name".to_string(), 20);

        _ = table_manager.borrow_mut().create_table(
            "student".to_string(),
            &student_shema,
            transaction.clone(),
        );

        let student_layout = table_manager
            .borrow()
            .get_layout("student".to_string(), transaction.clone())
            .unwrap();

        let mut table_scan =
            TableScan::new("student".to_string(), transaction.clone(), student_layout);

        table_scan.insert();
        table_scan.set_string("sid".to_string(), "1".to_string());
        table_scan.set_string("name".to_string(), "Alice".to_string());

        table_scan.insert();
        table_scan.set_string("sid".to_string(), "2".to_string());
        table_scan.set_string("name".to_string(), "Alice2".to_string());

        table_scan.insert();
        table_scan.set_string("sid".to_string(), "3".to_string());
        table_scan.set_string("name".to_string(), "Alice4".to_string());

        table_scan.close();
        transaction.borrow_mut().commit();

        let stat_manager = Rc::new(RefCell::new(StatManagerV2::new(table_manager.clone())));
        _ = stat_manager
            .borrow_mut()
            .refresh_table_stats(transaction.clone());

        let mut index_manager = IndexManager::new(
            table_manager.clone(),
            stat_manager.clone(),
            transaction.clone(),
        )
        .unwrap();

        index_manager.create_index(
            "sidIdx".to_string(),
            "student".to_string(),
            "sid".to_string(),
            transaction.clone(),
        );

        index_manager.create_index(
            "nameIdx".to_string(),
            "student".to_string(),
            "name".to_string(),
            transaction.clone(),
        );

        let indexes = index_manager
            .get_index_info("student".to_string(), transaction.clone())
            .unwrap();

        for field_name in indexes.keys() {
            let index_info = indexes.get(field_name).unwrap();

            assert!(field_name == "sid" || field_name == "name");

            if field_name == "sid" {
                assert!(index_info.index_name == "sidIdx");
            } else if field_name == "name" {
                assert!(index_info.index_name == "nameIdx");
            }

            println!(
                "Index on field: {}, blocks accessed: {}, records output: {}, distinct values: {}",
                field_name,
                index_info.blocks_accessed(),
                index_info.records_output(),
                index_info.distinct_values(field_name)
            );
        }
    }
}
