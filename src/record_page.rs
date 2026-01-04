use std::collections::HashMap;

#[derive(Clone, Debug)]
pub struct TableFieldInfo {
    field_type: TableFieldType,
    field_length: i32,
}

impl TableFieldInfo {
    pub fn new(field_type: TableFieldType, field_length: i32) -> TableFieldInfo {
        TableFieldInfo {
            field_type,
            field_length,
        }
    }
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub enum TableFieldType {
    INTEGER,
    VARCHAR,
}

#[derive(Clone, Debug)]
pub struct TableSchema {
    pub fields: Vec<String>,
    pub field_infos: HashMap<String, TableFieldInfo>,
}

impl Into<i32> for TableFieldType {
    fn into(self) -> i32 {
        match self {
            TableFieldType::INTEGER => 0,
            TableFieldType::VARCHAR => 1,
        }
    }
}

impl From<i32> for TableFieldType {
    fn from(value: i32) -> Self {
        match value {
            0 => TableFieldType::INTEGER,
            1 => TableFieldType::VARCHAR,
            _ => panic!("Invalid field type"),
        }
    }
}

impl TableSchema {
    pub fn new() -> TableSchema {
        TableSchema {
            fields: Vec::new(),
            field_infos: HashMap::new(),
        }
    }

    pub fn add_field(&mut self, field_name: String, field_type: TableFieldType, field_length: i32) {
        self.fields.push(field_name.clone());
        self.field_infos.insert(
            field_name,
            TableFieldInfo {
                field_type,
                field_length,
            },
        );
    }

    pub fn add_integer_field(&mut self, field_name: String) {
        self.add_field(field_name, TableFieldType::INTEGER, 0);
    }

    pub fn add_string_field(&mut self, field_name: String, field_length: i32) {
        self.add_field(field_name, TableFieldType::VARCHAR, field_length);
    }

    pub fn add(&mut self, field_name: String, schema: TableSchema) {
        let field_type = schema.get_field_type(field_name.clone());
        let field_length = schema.get_field_length(field_name.clone());
        self.add_field(field_name, field_type.unwrap(), field_length.unwrap());
    }

    pub fn add_all(&mut self, schema: TableSchema) {
        for field in schema.clone().fields {
            self.add(field.clone(), schema.clone());
        }
    }

    pub fn fields(&self) -> &Vec<String> {
        &self.fields
    }

    fn add_from_schema(&mut self, field_name: String, schema: TableSchema) {
        let field_info = schema.field_infos.get(&field_name).unwrap();
        self.add_field(
            field_name,
            field_info.field_type.clone(),
            field_info.field_length,
        );
    }

    fn add_all_from_schema(&mut self, schema: TableSchema) {
        for field in schema.fields {
            let field_info = schema.field_infos.get(&field).unwrap();
            self.add_field(
                field,
                field_info.field_type.clone(),
                field_info.field_length,
            );
        }
    }

    pub fn get_field_type(&self, field_name: String) -> Option<TableFieldType> {
        let field_info = self.field_infos.get(&field_name);
        // field_infoが存在しない場合はNoneを返す

        if let Some(field_info) = field_info {
            return Some(field_info.field_type.clone());
        }

        return None;
    }

    pub fn get_field_length(&self, field_name: String) -> Option<i32> {
        let field_info = self.field_infos.get(&field_name);
        // field_infoが存在しない場合はNoneを返す

        if let Some(field_info) = field_info {
            return Some(field_info.field_length);
        }

        return None;
    }

    pub fn has_field(&self, field_name: String) -> bool {
        self.field_infos.contains_key(&field_name)
    }
}

#[derive(Clone, Debug)]
pub struct Layout {
    pub schema: TableSchema,
    offsets: HashMap<String, i32>,
    slot_size: i32,
}

impl Layout {
    pub fn new(schema: TableSchema) -> Layout {
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

    pub fn new_with_offset_and_size(
        schema: TableSchema,
        offsets: HashMap<String, i32>,
        slot_size: i32,
    ) -> Layout {
        Layout {
            schema,
            offsets,
            slot_size: slot_size,
        }
    }

    fn get_length_in_bytes(schema: &TableSchema, field_name: String) -> i32 {
        let field_type = schema.get_field_type(field_name.clone()).unwrap();
        match field_type {
            TableFieldType::INTEGER => 4,
            TableFieldType::VARCHAR => schema.get_field_length(field_name).unwrap() * 4 + 4,
        }
    }

    pub fn get_slot_size(&self) -> i32 {
        self.slot_size
    }

    pub fn get_offset(&self, field_name: &str) -> Option<i32> {
        let offset = self.offsets.get(field_name);
        // offsetが存在しない場合はNoneを返す

        if let Some(offset) = offset {
            return Some(offset.clone());
        }

        return None;
    }

    pub fn get_field_type(&self, field_name: String) -> Option<TableFieldType> {
        self.schema.get_field_type(field_name)
    }

    pub fn has_field(&self, field_name: String) -> bool {
        self.schema.has_field(field_name)
    }
}

#[derive(Copy, Clone)]
pub enum RecordType {
    EMPTY = 0,
    USED = 1,
}
