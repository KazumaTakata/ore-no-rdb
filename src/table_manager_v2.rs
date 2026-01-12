use std::{cell::RefCell, collections::HashMap, rc::Rc};

use crate::{
    error::{TableAlreadyExists, ValueNotFound},
    predicate::TableNameAndFieldName,
    record_page::{self, TableFieldType, TableSchema},
    scan_v2::ScanV2,
    table_scan_v2::TableScan,
    transaction_v2::TransactionV2,
};

pub struct TableManagerV2 {
    pub table_catalog_layout: record_page::Layout,
    pub field_catalog_layout: record_page::Layout,
}

impl TableManagerV2 {
    const TABLE_CATALOG_TABLE_NAME: &'static str = "table_catalog";
    const FIELD_CATALOG_TABLE_NAME: &'static str = "field_catalog";

    const TABLE_CATALOG_TABLE_NAME_FIELD: &'static str = "table_name";
    const TABLE_CATALOG_SLOT_SIZE_FIELD: &'static str = "slot_size";

    pub fn new(transaction: Rc<RefCell<TransactionV2>>, is_new: bool) -> TableManagerV2 {
        let mut table_catalog_schema = TableSchema::new();
        table_catalog_schema.add_string_field("table_name".to_string(), 20);
        table_catalog_schema.add_integer_field("slot_size".to_string());

        let table_catalog_layout = record_page::Layout::new(table_catalog_schema.clone());

        let mut field_catalog_schema = TableSchema::new();
        field_catalog_schema.add_string_field("table_name".to_string(), 20);
        field_catalog_schema.add_string_field("field_name".to_string(), 20);
        field_catalog_schema.add_integer_field("field_type".to_string());
        field_catalog_schema.add_integer_field("field_length".to_string());
        field_catalog_schema.add_integer_field("field_offset".to_string());
        let table_field_schema = record_page::Layout::new(field_catalog_schema.clone());

        let table_manager = TableManagerV2 {
            table_catalog_layout,
            field_catalog_layout: table_field_schema,
        };

        if is_new {
            table_manager.create_table(
                Self::TABLE_CATALOG_TABLE_NAME.to_string(),
                &table_catalog_schema,
                transaction.clone(),
            );

            table_manager.create_table(
                Self::FIELD_CATALOG_TABLE_NAME.to_string(),
                &field_catalog_schema,
                transaction.clone(),
            );
        }

        table_manager
    }

    pub fn check_if_field_exists(
        &self,
        table_name: String,
        field_name: String,
        transaction: Rc<RefCell<TransactionV2>>,
    ) -> bool {
        let mut field_scan = TableScan::new(
            Self::FIELD_CATALOG_TABLE_NAME.to_string(),
            transaction.clone(),
            self.field_catalog_layout.clone(),
        );

        while field_scan.next().unwrap() {
            let t_name =
                field_scan.get_string(TableNameAndFieldName::new(None, "table_name".to_string()));
            let f_name =
                field_scan.get_string(TableNameAndFieldName::new(None, "field_name".to_string()));

            match t_name {
                Some(t_name) => {
                    if t_name == table_name {
                        match f_name {
                            Some(f_name) => {
                                if f_name == field_name {
                                    field_scan.close();
                                    return true;
                                }
                            }
                            None => continue,
                        }
                    }
                }
                None => continue,
            }
        }
        field_scan.close();
        return false;
    }

    pub fn check_if_table_exists(
        &self,
        table_name: String,
        transaction: Rc<RefCell<TransactionV2>>,
    ) -> bool {
        let mut table_scan = TableScan::new(
            Self::TABLE_CATALOG_TABLE_NAME.to_string(),
            transaction.clone(),
            self.table_catalog_layout.clone(),
        );

        while table_scan.next().unwrap() {
            let name =
                table_scan.get_string(TableNameAndFieldName::new(None, "table_name".to_string()));

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
            Self::TABLE_CATALOG_TABLE_NAME.to_string(),
            transaction.clone(),
            self.table_catalog_layout.clone(),
        );

        if self.check_if_table_exists(table_name.clone(), transaction.clone()) {
            table_scan.close();
            return Err(TableAlreadyExists::new(table_name));
        }

        table_scan.insert();
        table_scan.set_string(
            Self::TABLE_CATALOG_TABLE_NAME_FIELD.to_string(),
            table_name.clone(),
        );
        let slot_size = layout.get_slot_size() as i32;
        table_scan.set_integer(Self::TABLE_CATALOG_SLOT_SIZE_FIELD.to_string(), slot_size);
        table_scan.close();

        transaction.borrow_mut().commit();

        let mut field_scan = TableScan::new(
            Self::FIELD_CATALOG_TABLE_NAME.to_string(),
            transaction.clone(),
            self.field_catalog_layout.clone(),
        );

        // schemaのfieldsをループして、各フィールドの情報を挿入
        for (_, field_name) in schema.fields.iter().enumerate() {
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
            Self::TABLE_CATALOG_TABLE_NAME.to_string(),
            transaction.clone(),
            self.table_catalog_layout.clone(),
        );

        let mut slot_size: Option<i32> = None;

        println!("Getting layout for table: {}", table_name);

        while table_scan.next()? {
            let name =
                table_scan.get_string(TableNameAndFieldName::new(None, "table_name".to_string()));

            println!("Found table in catalog: {:?}", name);

            match name {
                Some(name) => {
                    if name == table_name {
                        let size = table_scan.get_integer(TableNameAndFieldName::new(
                            None,
                            Self::TABLE_CATALOG_SLOT_SIZE_FIELD.to_string(),
                        ));
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
            let name =
                field_scan.get_string(TableNameAndFieldName::new(None, "table_name".to_string()));

            match name {
                Some(name) => {
                    if name == table_name {
                        let field_name = field_scan
                            .get_string(TableNameAndFieldName::new(None, "field_name".to_string()));
                        let field_type = field_scan.get_integer(TableNameAndFieldName::new(
                            None,
                            "field_type".to_string(),
                        ));
                        let field_length = field_scan.get_integer(TableNameAndFieldName::new(
                            None,
                            "field_length".to_string(),
                        ));
                        let field_offset = field_scan.get_integer(TableNameAndFieldName::new(
                            None,
                            "field_offset".to_string(),
                        ));
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
    use std::{cell::RefCell, fs::remove_file, path::Path, rc::Rc};

    use crate::{
        buffer_manager_v2::BufferManagerV2,
        concurrency_manager::LockTable,
        file_manager::{self, FileManager},
        log_manager_v2::LogManagerV2,
        record_page::TableSchema,
    };

    use super::*;

    #[test]
    fn test_table_mgr() {
        let test_dir = Path::new("test_data");
        let block_size = 400;

        let log_file_name = format!("log_file_{}.txt", uuid::Uuid::new_v4());

        let file_manager = Rc::new(RefCell::new(FileManager::new(test_dir, block_size)));
        let log_manager = Rc::new(RefCell::new(LogManagerV2::new(
            file_manager.clone(),
            log_file_name.clone(),
        )));

        let buffer_manager = Rc::new(RefCell::new(BufferManagerV2::new(
            3,
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

        let table_manager = TableManagerV2::new(transaction.clone(), true);

        let mut schema = TableSchema::new();
        schema.add_integer_field("A".to_string());
        schema.add_string_field("B".to_string(), 9);
        schema.add_integer_field("C".to_string());

        let test_table_name = format!("test_table_{}", uuid::Uuid::new_v4());

        table_manager.create_table(test_table_name.clone(), &schema, transaction.clone());
        let layout = table_manager.get_layout(test_table_name, transaction.clone());

        let layout = layout.unwrap();

        println!("Layout for test_table:");
        println!("Slot Size: {}", layout.get_slot_size());

        for (i, field) in layout.schema.fields.iter().enumerate() {
            println!("Field: {}", field);
            let offset = layout.get_offset(field).unwrap();
            println!("Offset: {}", offset);
            let field_type = schema.get_field_type(field.to_string()).unwrap();
            println!("Field Type: {:?}", field_type);
            let field_length = schema.get_field_length(field.to_string()).unwrap();
            println!("Field Length: {}", field_length);

            if i == 0 {
                assert_eq!(field, "A");
                assert_eq!(offset, 4);
                assert_eq!(field_type, TableFieldType::INTEGER);
                assert_eq!(field_length, 0);
            } else if i == 1 {
                assert_eq!(field, "B");
                assert_eq!(offset, 8);
                assert_eq!(field_type, TableFieldType::VARCHAR);
                assert_eq!(field_length, 9);
            } else if i == 2 {
                assert_eq!(field, "C");
                assert_eq!(offset, 48);
                assert_eq!(field_type, TableFieldType::INTEGER);
                assert_eq!(field_length, 0);
            }
        }

        remove_file(test_dir.join(log_file_name)).unwrap();
    }
}
