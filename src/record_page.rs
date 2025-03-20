use std::collections::HashMap;

#[derive(Clone)]
struct TableFieldInfo {
    field_type: TableFieldType,
    field_length: i32,
}

#[derive(Clone)]
enum TableFieldType {
    INTEGER,
    VARCHAR,
}

pub struct TableSchema {
    fields: Vec<String>,
    field_infos: HashMap<String, TableFieldInfo>,
}

impl TableSchema {
    fn new() -> TableSchema {
        TableSchema {
            fields: Vec::new(),
            field_infos: HashMap::new(),
        }
    }

    fn add_field(&mut self, field_name: String, field_type: TableFieldType, field_length: i32) {
        self.fields.push(field_name.clone());
        self.field_infos.insert(
            field_name,
            TableFieldInfo {
                field_type,
                field_length,
            },
        );
    }

    fn add_integer_field(&mut self, field_name: String) {
        self.add_field(field_name, TableFieldType::INTEGER, -1);
    }

    fn add_string_field(&mut self, field_name: String, field_length: i32) {
        self.add_field(field_name, TableFieldType::VARCHAR, field_length);
    }

    fn get_field_type(&self, field_name: String) -> Option<TableFieldType> {
        let field_info = self.field_infos.get(&field_name);
        // field_infoが存在しない場合はNoneを返す

        if let Some(field_info) = field_info {
            return Some(field_info.field_type.clone());
        }

        return None;
    }

    fn get_field_length(&self, field_name: String) -> Option<i32> {
        let field_info = self.field_infos.get(&field_name);
        // field_infoが存在しない場合はNoneを返す

        if let Some(field_info) = field_info {
            return Some(field_info.field_length);
        }

        return None;
    }

    fn has_field(&self, field_name: String) -> bool {
        self.field_infos.contains_key(&field_name)
    }
}

struct Layout {
    schema: TableSchema,
    offsets: HashMap<String, i32>,
    slot_size: i32,
}

impl Layout {
    fn new(schema: TableSchema) -> Layout {
        let mut offsets = HashMap::new();
        // i32のサイズは4バイト
        let mut slot_size = 4;

        for field in &schema.fields {
            offsets.insert(field.clone(), slot_size);
            slot_size += Layout::get_length_in_bytes(&schema, field.clone());
        }

        Layout {
            schema,
            offsets,
            slot_size,
        }
    }

    fn get_length_in_bytes(schema: &TableSchema, field_name: String) -> i32 {
        let field_type = schema.get_field_type(field_name.clone()).unwrap();
        match field_type {
            TableFieldType::INTEGER => 4,
            TableFieldType::VARCHAR => schema.get_field_length(field_name).unwrap() * 4 + 4,
        }
    }

    fn get_slot_size(&self) -> i32 {
        self.slot_size
    }

    fn get_offset(&self, field_name: String) -> Option<i32> {
        let offset = self.offsets.get(&field_name);
        // offsetが存在しない場合はNoneを返す

        if let Some(offset) = offset {
            return Some(offset.clone());
        }

        return None;
    }

    fn get_field_type(&self, field_name: String) -> Option<TableFieldType> {
        self.schema.get_field_type(field_name)
    }

    fn has_field(&self, field_name: String) -> bool {
        self.schema.has_field(field_name)
    }
}
