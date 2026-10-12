use std::collections::HashMap;

#[derive(Clone, PartialEq, Eq, Debug)]
pub enum TableFieldInfo {
    INTEGER,
    VARCHAR(usize),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TableSchema {
    pub fields_name_vec: Vec<String>,
    pub fields: HashMap<String, TableFieldInfo>,
}

impl From<&TableFieldInfo> for i32 {
    fn from(info: &TableFieldInfo) -> i32 {
        match info {
            TableFieldInfo::INTEGER => 0,
            TableFieldInfo::VARCHAR(_) => 1,
        }
    }
}

impl TableSchema {
    pub fn new() -> TableSchema {
        TableSchema {
            fields_name_vec: vec![],
            fields: HashMap::new(),
        }
    }

    pub fn add_field(&mut self, field_name: String, field_type: TableFieldInfo) {
        self.fields_name_vec.push(field_name.clone());
        self.fields.insert(field_name, field_type);
    }

    pub fn add_integer_field(&mut self, field_name: String) {
        self.add_field(field_name, TableFieldInfo::INTEGER);
    }

    pub fn add_string_field(&mut self, field_name: String, field_length: i32) {
        self.add_field(field_name, TableFieldInfo::VARCHAR(field_length as usize));
    }

    pub fn add(&mut self, field_name: String, schema: TableSchema) {
        let field_type = schema.get_field_type(field_name.clone());
        self.add_field(field_name, field_type.unwrap().clone());
    }

    pub fn add_all(&mut self, schema: TableSchema) {
        for field in schema.clone().fields_name_vec {
            self.add(field.clone(), schema.clone());
        }
    }

    pub fn fields(&self) -> &Vec<String> {
        return &self.fields_name_vec;
    }

    fn add_from_schema(&mut self, field_name: String, schema: TableSchema) {
        let field_info = schema.fields.get(&field_name).unwrap();
        self.add_field(field_name, field_info.clone());
    }

    fn add_all_from_schema(&mut self, schema: TableSchema) {
        for field in schema.fields_name_vec {
            let field_info = schema.fields.get(&field).unwrap();
            self.add_field(field, field_info.clone());
        }
    }

    pub fn get_field_type(&self, field_name: String) -> Option<&TableFieldInfo> {
        let field_info = self.fields.get(&field_name);
        return field_info;
    }

    pub fn get_field_length(&self, field_name: String) -> Option<i32> {
        let field_info = self.fields.get(&field_name);
        // field_infoが存在しない場合はNoneを返す

        if let Some(field_info) = field_info {
            return match field_info {
                TableFieldInfo::INTEGER => Some(0),
                TableFieldInfo::VARCHAR(size) => Some(size.clone() as i32),
            };
        }

        return None;
    }

    pub fn has_field(&self, field_name: String) -> bool {
        self.fields.contains_key(&field_name)
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

        // RecordTypeの分
        let mut slot_size = 4;

        for field in &schema.fields_name_vec {
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
            &TableFieldInfo::INTEGER => 4,
            &TableFieldInfo::VARCHAR(length) => length as i32 * 4 + 4,
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

    pub fn get_field_type(&self, field_name: String) -> Option<&TableFieldInfo> {
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
