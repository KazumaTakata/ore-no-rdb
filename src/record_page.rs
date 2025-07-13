use std::collections::HashMap;

use crate::buffer_manager::BufferList;
use crate::transaction::Transaction;
use crate::{BlockId, FileManager};

#[derive(Clone)]
pub struct TableFieldInfo {
    field_type: TableFieldType,
    field_length: i32,
}

#[derive(Clone, PartialEq, Eq)]
pub enum TableFieldType {
    INTEGER,
    VARCHAR,
}

#[derive(Clone)]
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
        self.add_field(field_name, TableFieldType::INTEGER, -1);
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

#[derive(Clone)]
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

pub struct RecordPage {
    layout: Layout,
    block_id: BlockId,
}

impl RecordPage {
    pub fn new(layout: Layout, block_id: BlockId) -> RecordPage {
        RecordPage { layout, block_id }
    }

    pub fn get_integer(
        &self,
        field_name: String,
        slot_id: i32,
        transaction: &mut Transaction,
        buffer_list: &mut BufferList,
    ) -> Option<i32> {
        if !self.layout.has_field(field_name.clone()) {
            return None;
        }

        let offset = self.layout.get_offset(&field_name).unwrap();
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

    pub fn get_block_id(&self) -> BlockId {
        self.block_id.clone()
    }

    pub fn set_integer(
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

        let offset = self.layout.get_offset(&field_name).unwrap();
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

    pub fn set_string(
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

        let offset = self.layout.get_offset(&field_name).unwrap();
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
        while self.is_valid_slot_id(next_slot_id, file_manager) {
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

    pub fn get_string(
        &self,
        field_name: String,
        slot_id: i32,
        transaction: &mut Transaction,
        buffer_list: &mut BufferList,
    ) -> Option<String> {
        if !self.layout.has_field(field_name.clone()) {
            return None;
        }

        let offset = self.layout.get_offset(&field_name).unwrap();
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

    pub fn set_flag(
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

    pub fn is_valid_slot_id(&self, slot_id: i32, file_manager: &FileManager) -> bool {
        return self.get_offset_of_record(slot_id + 1) <= file_manager.block_size as i32;
    }

    pub fn get_offset_of_record(&self, slot_id: i32) -> i32 {
        slot_id * self.layout.get_slot_size()
    }

    pub fn find_next_after_slot_id(
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

    pub fn insert_after_slot_id(
        &self,
        slot_id: i32,
        file_manager: &FileManager,
        buffer_list: &mut BufferList,
        transaction: &mut Transaction,
    ) -> Option<i32> {
        let next_slot_id = self.search_after(
            slot_id,
            file_manager,
            RecordType::EMPTY,
            buffer_list,
            transaction,
        );

        if let Some(next_slot_id) = next_slot_id {
            self.set_flag(next_slot_id, transaction, buffer_list, RecordType::USED);
            return Some(next_slot_id);
        }

        return None;
    }

    pub fn format(
        &self,
        transaction: &mut Transaction,
        buffer_list: &mut BufferList,
        file_manager: &FileManager,
    ) {
        let mut slot_id = 0;
        while self.is_valid_slot_id(slot_id, file_manager) {
            print!(
                "slot_id: {}, slot_size: {}",
                slot_id,
                self.layout.get_slot_size()
            );
            self.set_flag(slot_id, transaction, buffer_list, RecordType::EMPTY);
            let schema = &self.layout.schema;

            for field in schema.fields() {
                let field_type = schema.get_field_type(field.clone()).unwrap();
                let offset = self.layout.get_offset(&field).unwrap();

                match field_type {
                    TableFieldType::INTEGER => {
                        self.set_integer(field.clone(), slot_id, transaction, buffer_list, 0);
                    }
                    TableFieldType::VARCHAR => {
                        self.set_string(
                            field.clone(),
                            slot_id,
                            transaction,
                            buffer_list,
                            "".to_string(),
                        );
                    }
                }
            }

            slot_id += 1;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{cell::RefCell, path::Path, rc::Rc};

    use crate::{buffer_manager::BufferManager, page, transaction};

    use rand::Rng;

    use super::*;

    #[test]
    fn test_buffer() {
        let mut file_manager = FileManager::new(Path::new("data"), 1000);
        let buffer_manager = Rc::new(RefCell::new(BufferManager::new(3)));

        let mut transaction = transaction::Transaction::new(1);

        let mut schema = TableSchema::new();

        schema.add_integer_field("id".to_string());
        schema.add_string_field("name".to_string(), 10);

        let layout = Layout::new(schema);

        // layerのschemaをiterateしてoffsetをprintoutする
        for field in layout.schema.fields() {
            let offset = layout.get_offset(&field).unwrap();
            println!("{}: {}", field, offset);
        }

        let block_id = transaction.append(&mut file_manager, "test.txt");
        let mut buffer_list = BufferList::new();

        let buffer_manager_ref1 = Rc::clone(&buffer_manager);
        let mut buffer_manager_mut = buffer_manager_ref1.borrow_mut();

        transaction.pin(
            &mut file_manager,
            &mut buffer_list,
            &mut buffer_manager_mut,
            block_id.clone(),
        );

        let record_page = RecordPage::new(layout, block_id.clone());

        record_page.format(&mut transaction, &mut buffer_list, &file_manager);

        let mut next_slot =
            record_page.insert_after_slot_id(-1, &file_manager, &mut buffer_list, &mut transaction);

        while let Some(slot) = next_slot {
            println!("slot.....: {}", slot);

            // 1..100までのランダムなintegerを生成
            let random_integer = rand::thread_rng().gen_range(0..100);

            record_page.set_integer(
                "id".to_string(),
                slot,
                &mut transaction,
                &mut buffer_list,
                random_integer,
            );

            record_page.set_string(
                "name".to_string(),
                slot,
                &mut transaction,
                &mut buffer_list,
                format!("name{}", random_integer),
            );

            next_slot = record_page.insert_after_slot_id(
                slot,
                &file_manager,
                &mut buffer_list,
                &mut transaction,
            );

            // println!("next_slot: {}", next_slot.unwrap());

            // _slot = next_slot;
        }

        let mut next_slot = record_page.find_next_after_slot_id(
            -1,
            &file_manager,
            &mut buffer_list,
            &mut transaction,
        );

        // next_slotが存在する間は繰り返す
        while let Some(slot) = next_slot {
            println!("slot: {}", slot);

            let id =
                record_page.get_integer("id".to_string(), slot, &mut transaction, &mut buffer_list);
            let name = record_page.get_string(
                "name".to_string(),
                slot,
                &mut transaction,
                &mut buffer_list,
            );

            println!("id: {}, name: {}", id.unwrap(), name.unwrap());

            next_slot = record_page.find_next_after_slot_id(
                slot,
                &file_manager,
                &mut buffer_list,
                &mut transaction,
            );
        }

        transaction.commit(&mut buffer_list, &mut buffer_manager_mut, &mut file_manager);

        // let mut buffer_mut_1 = {
        //     let mut buffer_manager_mut_ref_1 = buffer_manager_ref1.borrow_mut();
        //     let buffer_1 = buffer_manager_mut_ref_1.pin(&mut file_manager, block_id.clone());
        //     let mut buffer_mut_1 = buffer_1.unwrap().borrow_mut();
        //     let page_1 = buffer_mut_1.content();
        //     let test_integer = page_1.get_integer(80);
        //     page_1.set_integer(80, test_integer + 1);

        //     buffer_mut_1.set_modified(1, 0);

        //     Rc::clone(buffer_1.unwrap())
        // };
    }
}
