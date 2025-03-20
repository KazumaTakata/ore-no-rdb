use std::collections::HashMap;

use crate::{BlockId, BufferList, FileManager, Transaction};

#[derive(Clone)]
struct TableFieldInfo {
    field_type: TableFieldType,
    field_length: i32,
}

#[derive(Clone, PartialEq, Eq)]
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

#[derive(Copy, Clone)]
enum RecordType {
    EMPTY = 0,
    USED = 1,
}

struct RecordPage {
    layout: Layout,
    transaction: Transaction,
    block_id: BlockId,
}

impl RecordPage {
    fn new(layout: Layout, transaction: Transaction, block_id: BlockId) -> RecordPage {
        RecordPage {
            layout,
            block_id,
            transaction,
        }
    }

    fn get_integer(
        &self,
        field_name: String,
        slot_id: i32,
        transaction: &mut Transaction,
        buffer_list: &mut BufferList,
    ) -> Option<i32> {
        if !self.layout.has_field(field_name.clone()) {
            return None;
        }

        let offset = self.layout.get_offset(field_name.clone()).unwrap();
        let record_offset = self.get_offset_of_record(slot_id);
        let field_type = self.layout.get_field_type(field_name.clone()).unwrap();

        if field_type != TableFieldType::INTEGER {
            return None;
        }

        let result = transaction.get_integer(
            buffer_list,
            self.block_id.clone(),
            (record_offset + offset) as usize,
        );

        Some(result)
    }

    fn set_integer(
        &self,
        field_name: String,
        slot_id: i32,
        transaction: &mut Transaction,
        buffer_list: &mut BufferList,
        value: i32,
    ) {
        if !self.layout.has_field(field_name.clone()) {
            return;
        }

        let offset = self.layout.get_offset(field_name.clone()).unwrap();
        let record_offset = self.get_offset_of_record(slot_id);
        let field_type = self.layout.get_field_type(field_name.clone()).unwrap();

        if field_type != TableFieldType::INTEGER {
            return;
        }

        transaction.set_integer(
            buffer_list,
            self.block_id.clone(),
            (record_offset + offset) as usize,
            value,
        );
    }

    fn delete(&self, slot_id: i32, transaction: &mut Transaction, buffer_list: &mut BufferList) {
        let record_offset = self.get_offset_of_record(slot_id);
        transaction.set_integer(
            buffer_list,
            self.block_id.clone(),
            record_offset as usize,
            RecordType::EMPTY as i32,
        );
    }

    fn set_string(
        &self,
        field_name: String,
        slot_id: i32,
        transaction: &mut Transaction,
        buffer_list: &mut BufferList,
        value: String,
    ) {
        if !self.layout.has_field(field_name.clone()) {
            return;
        }

        let offset = self.layout.get_offset(field_name.clone()).unwrap();
        let record_offset = self.get_offset_of_record(slot_id);
        let field_type = self.layout.get_field_type(field_name.clone()).unwrap();

        if field_type != TableFieldType::VARCHAR {
            return;
        }

        transaction.set_string(
            buffer_list,
            self.block_id.clone(),
            (record_offset + offset) as usize,
            value.as_str(),
        );
    }

    fn search_after(
        &self,
        slot_id: i32,
        file_manager: &FileManager,
        target_record_type: RecordType,
        buffer_list: &mut BufferList,
        transaction: &mut Transaction,
    ) -> Option<i32> {
        let mut next_slot_id = slot_id + 1;
        while self.is_valid_slot_id(slot_id, file_manager) {
            let record_offset = self.get_offset_of_record(next_slot_id);
            let record_type =
                transaction.get_integer(buffer_list, self.block_id.clone(), record_offset as usize);

            if record_type == target_record_type as i32 {
                return Some(next_slot_id);
            }

            next_slot_id += 1;
        }

        return None;
    }

    fn get_string(
        &self,
        field_name: String,
        slot_id: i32,
        transaction: &mut Transaction,
        buffer_list: &mut BufferList,
    ) -> Option<String> {
        if !self.layout.has_field(field_name.clone()) {
            return None;
        }

        let offset = self.layout.get_offset(field_name.clone()).unwrap();
        let record_offset = self.get_offset_of_record(slot_id);
        let field_type = self.layout.get_field_type(field_name.clone()).unwrap();

        if field_type != TableFieldType::VARCHAR {
            return None;
        }

        let result = transaction.get_string(
            buffer_list,
            self.block_id.clone(),
            (record_offset + offset) as usize,
        );

        Some(result)
    }

    fn set_flag(
        &self,
        slot_id: i32,
        transaction: &mut Transaction,
        buffer_list: &mut BufferList,
        record_type: RecordType,
    ) {
        let record_offset = self.get_offset_of_record(slot_id);
        transaction.set_integer(
            buffer_list,
            self.block_id.clone(),
            record_offset as usize,
            record_type as i32,
        );
    }

    fn is_valid_slot_id(&self, slot_id: i32, file_manager: &FileManager) -> bool {
        return self.get_offset_of_record(slot_id) < file_manager.block_size as i32;
    }

    fn get_offset_of_record(&self, slot_id: i32) -> i32 {
        slot_id * self.layout.get_slot_size()
    }

    fn find_next_after_slot_id(
        &self,
        slot_id: i32,
        file_manager: &FileManager,
        buffer_list: &mut BufferList,
        transaction: &mut Transaction,
    ) -> Option<i32> {
        return self.search_after(
            slot_id,
            file_manager,
            RecordType::USED,
            buffer_list,
            transaction,
        );
    }

    fn insert_after_slot_id(
        &self,
        slot_id: i32,
        file_manager: &FileManager,
        buffer_list: &mut BufferList,
        transaction: &mut Transaction,
    ) -> Option<i32> {
        let next_slot_id =
            self.find_next_after_slot_id(slot_id, file_manager, buffer_list, transaction);

        if let Some(next_slot_id) = next_slot_id {
            self.set_flag(next_slot_id, transaction, buffer_list, RecordType::USED);
            return Some(next_slot_id);
        }

        return None;
    }
}
