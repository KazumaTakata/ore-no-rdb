use std::{cell::RefCell, collections::HashMap, rc::Rc};

use crate::{
    buffer_manager, file_manager,
    record_page::{self, TableFieldType, TableSchema},
    scan::{Scan, ScanV2},
    table_scan_v2::TableScan,
    transaction,
    transaction_v2::TransactionV2,
};

pub struct TableManagerV2 {
    table_catalog_layout: record_page::Layout,
    field_catalog_layout: record_page::Layout,
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

    pub fn create_table(
        &self,
        table_name: String,
        schema: &TableSchema,
        transaction: Rc<RefCell<TransactionV2>>,
    ) {
        let layout = record_page::Layout::new(schema.clone());

        let mut table_scan = TableScan::new(
            "table_catalog".to_string(),
            transaction.clone(),
            layout.clone(),
        );
        table_scan.insert();
        table_scan.set_string("table_name".to_string(), table_name.clone());
        table_scan.set_integer("slot_size".to_string(), layout.get_slot_size() as i32);
        table_scan.close();

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
    }

    pub fn get_layout(
        &self,
        table_name: String,
        transaction: Rc<RefCell<TransactionV2>>,
    ) -> record_page::Layout {
        let mut table_scan = TableScan::new(
            "table_catalog".to_string(),
            transaction.clone(),
            self.table_catalog_layout.clone(),
        );

        let mut slot_size: Option<i32> = None;

        while table_scan.next() {
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

        while field_scan.next() {
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

        return record_page::Layout::new_with_offset_and_size(
            table_schema,
            offsets,
            slot_size.unwrap(),
        );
    }
}
