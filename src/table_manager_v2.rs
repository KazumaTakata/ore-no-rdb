use std::{cell::RefCell, collections::HashMap, rc::Rc};

use crate::{
    buffer_manager,
    error::{TableAlreadyExists, ValueNotFound},
    file_manager,
    record_page::{self, TableFieldType, TableSchema},
    scan_v2::ScanV2,
    table_scan_v2::TableScan,
    transaction,
    transaction_v2::TransactionV2,
};

pub struct TableManagerV2 {
    pub table_catalog_layout: record_page::Layout,
    pub field_catalog_layout: record_page::Layout,
}

impl TableManagerV2 {
    pub fn new() -> TableManagerV2 {
        let mut table_catalog_schema = TableSchema::new();
        table_catalog_schema.add_string_field("table_name".to_string(), 20);
        table_catalog_schema.add_integer_field("slot_size".to_string());

        let table_catalog_layout = record_page::Layout::new(table_catalog_schema);

        let mut field_catalog_schema = TableSchema::new();
        field_catalog_schema.add_string_field("table_name".to_string(), 20);
        field_catalog_schema.add_string_field("field_name".to_string(), 20);
        field_catalog_schema.add_integer_field("field_type".to_string());
        field_catalog_schema.add_integer_field("field_length".to_string());
        field_catalog_schema.add_integer_field("field_offset".to_string());
        let table_field_schema = record_page::Layout::new(field_catalog_schema);

        TableManagerV2 {
            table_catalog_layout,
            field_catalog_layout: table_field_schema,
        }
    }

    fn check_if_table_exists(
        &self,
        table_name: String,
        transaction: Rc<RefCell<TransactionV2>>,
    ) -> bool {
        let mut table_scan = TableScan::new(
            "table_catalog".to_string(),
            transaction.clone(),
            self.table_catalog_layout.clone(),
        );

        while table_scan.next().unwrap() {
            let name = table_scan.get_string("table_name".to_string());

            match name {
                Some(name) => {
                    if name == table_name {
                        table_scan.close();
                        return true;
                    }
                }
                None => continue,
            }
        }
        table_scan.close();
        return false;
    }

    pub fn create_table(
        &self,
        table_name: String,
        schema: &TableSchema,
        transaction: Rc<RefCell<TransactionV2>>,
    ) -> Result<(), TableAlreadyExists> {
        let layout = record_page::Layout::new(schema.clone());

        let mut table_scan = TableScan::new(
            "table_catalog".to_string(),
            transaction.clone(),
            self.table_catalog_layout.clone(),
        );

        if self.check_if_table_exists(table_name.clone(), transaction.clone()) {
            table_scan.close();
            return Err(TableAlreadyExists::new(table_name));
        }

        table_scan.insert();
        table_scan.set_string("table_name".to_string(), table_name.clone());
        let slot_size = layout.get_slot_size() as i32;
        table_scan.set_integer("slot_size".to_string(), slot_size);
        table_scan.close();

        transaction.borrow_mut().commit();

        let mut field_scan = TableScan::new(
            "field_catalog".to_string(),
            transaction.clone(),
            self.field_catalog_layout.clone(),
        );

        // schemaのfieldsをループして、各フィールドの情報を挿入
        for (i, field_name) in schema.fields.iter().enumerate() {
            let field_type = schema.get_field_type(field_name.to_string()).unwrap();
            let field_length = schema.get_field_length(field_name.to_string()).unwrap();
            let field_offset = layout.get_offset(&field_name).unwrap();

            field_scan.insert();
            field_scan.set_string("table_name".to_string(), table_name.clone());
            field_scan.set_string("field_name".to_string(), field_name.clone());
            field_scan.set_integer("field_type".to_string(), field_type.into());
            field_scan.set_integer("field_length".to_string(), field_length);
            field_scan.set_integer("field_offset".to_string(), field_offset);
        }

        field_scan.close();

        transaction.borrow_mut().commit();

        return Ok(());
    }

    pub fn get_layout(
        &self,
        table_name: String,
        transaction: Rc<RefCell<TransactionV2>>,
    ) -> Result<record_page::Layout, ValueNotFound> {
        let mut table_scan = TableScan::new(
            "table_catalog".to_string(),
            transaction.clone(),
            self.table_catalog_layout.clone(),
        );

        let mut slot_size: Option<i32> = None;

        while table_scan.next()? {
            let name = table_scan.get_string("table_name".to_string());

            match name {
                Some(name) => {
                    if name == table_name {
                        let size = table_scan.get_integer("slot_size".to_string());
                        match size {
                            Some(size) => {
                                slot_size = Some(size);
                                break;
                            }
                            None => continue,
                        }
                    }
                }
                None => continue,
            }
        }
        table_scan.close();

        let mut table_schema = TableSchema::new();

        let mut field_scan = TableScan::new(
            "field_catalog".to_string(),
            transaction,
            self.field_catalog_layout.clone(),
        );

        let mut offsets = HashMap::new();

        while field_scan.next()? {
            let name = field_scan.get_string("table_name".to_string());

            match name {
                Some(name) => {
                    if name == table_name {
                        let field_name = field_scan.get_string("field_name".to_string());
                        let field_type = field_scan.get_integer("field_type".to_string());
                        let field_length = field_scan.get_integer("field_length".to_string());
                        let field_offset = field_scan.get_integer("field_offset".to_string());
                        offsets.insert(field_name.clone().unwrap(), field_offset.unwrap());
                        table_schema.add_field(
                            field_name.unwrap(),
                            TableFieldType::from(field_type.unwrap()),
                            field_length.unwrap() as i32,
                        );
                    }
                }
                None => continue,
            }
        }

        field_scan.close();

        return Ok(record_page::Layout::new_with_offset_and_size(
            table_schema,
            offsets,
            slot_size.unwrap(),
        ));
    }
}

#[cfg(test)]
mod tests {
    use std::{cell::RefCell, path::Path, rc::Rc};

    use rand::Rng;

    use crate::{
        buffer_manager_v2::BufferManagerV2,
        concurrency_manager::LockTable,
        file_manager::{self, FileManager},
        log_manager,
        log_manager_v2::LogManagerV2,
        record_page::TableSchema,
        transaction,
    };

    use super::*;

    #[test]
    fn test_table_mgr() {
        let mut file_manager = Rc::new(RefCell::new(FileManager::new(Path::new("data"), 400)));
        let log_manager = Rc::new(RefCell::new(LogManagerV2::new(
            file_manager.clone(),
            "log.txt".to_string(),
        )));

        let buffer_manager = Rc::new(RefCell::new(BufferManagerV2::new(
            3,
            file_manager.clone(),
            log_manager.clone(),
        )));

        let lock_table = Rc::new(RefCell::new(LockTable::new()));

        let mut transaction = Rc::new(RefCell::new(TransactionV2::new(
            1,
            file_manager.clone(),
            buffer_manager.clone(),
            lock_table.clone(),
        )));

        let table_manager = TableManagerV2::new();
        table_manager.create_table(
            "table_catalog".to_string(),
            &table_manager.table_catalog_layout.schema.clone(),
            transaction.clone(),
        );
        table_manager.create_table(
            "field_catalog".to_string(),
            &table_manager.field_catalog_layout.schema.clone(),
            transaction.clone(),
        );

        let layout = table_manager.get_layout("field_catalog".to_string(), transaction.clone());

        let mut schema = TableSchema::new();
        schema.add_integer_field("A".to_string());
        schema.add_string_field("B".to_string(), 9);

        table_manager.create_table("test_table".to_string(), &schema, transaction.clone());
        let layout = table_manager.get_layout("test_table".to_string(), transaction.clone());

        let layout = layout.unwrap();

        println!("Layout for test_table:");
        println!("Slot Size: {}", layout.get_slot_size());

        for field in layout.schema.fields.iter() {
            println!("Field: {}", field);
            let offset = layout.get_offset(field).unwrap();
            println!("Offset: {}", offset);
            let field_type = schema.get_field_type(field.to_string()).unwrap();
            println!("Field Type: {:?}", field_type);
            let field_length = schema.get_field_length(field.to_string()).unwrap();
        }
    }
}
