use crate::{
    buffer_manager, file_manager,
    record_page::{self, TableFieldType, TableSchema},
    scan::Scan,
    table_scan::TableScan,
    transaction,
};

pub struct TableManager {
    table_catalog_layout: record_page::Layout,
    field_catalog_layout: record_page::Layout,
}

impl TableManager {
    pub fn new() -> TableManager {
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

        TableManager {
            table_catalog_layout,
            field_catalog_layout: table_field_schema,
        }
    }

    pub fn create_table(
        &self,
        table_name: String,
        schema: &TableSchema,
        file_manager: &mut file_manager::FileManager,
        transaction: &mut transaction::Transaction,
        buffer_list: &mut crate::buffer_manager::BufferList,
        buffer_manager: &mut crate::buffer_manager::BufferManager,
    ) {
        let layout = record_page::Layout::new(schema.clone());

        let mut table_scan = TableScan::new(
            "table_catalog".to_string(),
            transaction,
            file_manager,
            layout.clone(),
            buffer_list,
        );

        table_scan.insert(transaction, buffer_list, file_manager, layout.clone());

        table_scan.set_string(
            transaction,
            buffer_list,
            "table_name".to_string(),
            table_name.clone(),
        );
        table_scan.set_integer(
            transaction,
            buffer_list,
            "slot_size".to_string(),
            layout.get_slot_size() as i32,
        );

        table_scan.close(transaction, buffer_list, buffer_manager);

        let mut field_scan = TableScan::new(
            "field_catalog".to_string(),
            transaction,
            file_manager,
            self.field_catalog_layout.clone(),
            buffer_list,
        );

        // schemaのfieldsをループして、各フィールドの情報を挿入
        for (i, field) in schema.fields.iter().enumerate() {
            let field_type = schema
                .get_field_type(field.clone())
                .expect("Field type not found");

            let field_length = schema
                .get_field_length(field.clone())
                .expect("Field length not found");

            let field_offset = layout.get_offset(&field).expect("Field offset not found");

            field_scan.insert(
                transaction,
                buffer_list,
                file_manager,
                self.field_catalog_layout.clone(),
            );

            field_scan.set_string(
                transaction,
                buffer_list,
                "table_name".to_string(),
                table_name.clone(),
            );
            field_scan.set_string(
                transaction,
                buffer_list,
                "field_name".to_string(),
                field.clone(),
            );
            field_scan.set_integer(
                transaction,
                buffer_list,
                "field_type".to_string(),
                field_type.into(),
            );
            field_scan.set_integer(
                transaction,
                buffer_list,
                "field_length".to_string(),
                field_length,
            );
            field_scan.set_integer(
                transaction,
                buffer_list,
                "field_offset".to_string(),
                field_offset,
            );
        }

        field_scan.close(transaction, buffer_list, buffer_manager);
    }

    pub fn get_layout(
        &self,
        table_name: String,
        transaction: &mut transaction::Transaction,
        file_manager: &mut file_manager::FileManager,
        buffer_list: &mut crate::buffer_manager::BufferList,
        buffer_manager: &mut crate::buffer_manager::BufferManager,
    ) -> record_page::Layout {
        let mut table_scan = TableScan::new(
            "table_catalog".to_string(),
            transaction,
            file_manager,
            self.table_catalog_layout.clone(),
            buffer_list,
        );

        while table_scan.next(file_manager, buffer_list, transaction) {
            let name = table_scan.get_string(transaction, buffer_list, "table_name".to_string());

            let mut slot_size: Option<usize> = None;

            match name {
                Some(name) => {
                    if name == table_name {
                        let size = table_scan.get_integer(
                            transaction,
                            buffer_list,
                            "slot_size".to_string(),
                        );

                        match size {
                            Some(size) => {
                                slot_size = Some(size as usize);
                                break;
                            }
                            None => continue,
                        }
                    }
                }
                None => continue,
            }
        }

        let mut table_schema = TableSchema::new();

        let mut field_scan = TableScan::new(
            "field_catalog".to_string(),
            transaction,
            file_manager,
            self.field_catalog_layout.clone(),
            buffer_list,
        );

        while field_scan.next(file_manager, buffer_list, transaction) {
            let name = field_scan.get_string(transaction, buffer_list, "table_name".to_string());

            match name {
                Some(name) => {
                    if name == table_name {
                        let field_name = field_scan.get_string(
                            transaction,
                            buffer_list,
                            "field_name".to_string(),
                        );
                        let field_type = field_scan.get_integer(
                            transaction,
                            buffer_list,
                            "field_type".to_string(),
                        );
                        let field_length = field_scan.get_integer(
                            transaction,
                            buffer_list,
                            "field_length".to_string(),
                        );
                        let field_offset = field_scan.get_integer(
                            transaction,
                            buffer_list,
                            "field_offset".to_string(),
                        );

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

        field_scan.close(transaction, buffer_list, buffer_manager);

        return record_page::Layout::new(table_schema);
    }
}
