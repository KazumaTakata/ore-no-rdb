use std::{cell::RefCell, rc::Rc};

use crate::{
    block::BlockId,
    record_page::{Layout, RecordType, TableFieldType},
    transaction_v2::TransactionV2,
};

pub struct RecordPage {
    layout: Layout,
    block_id: BlockId,
    transaction: Rc<RefCell<TransactionV2>>,
}

impl RecordPage {
    pub fn new(
        transaction: Rc<RefCell<TransactionV2>>,
        layout: Layout,
        block_id: BlockId,
    ) -> RecordPage {
        transaction.borrow_mut().pin(block_id.clone());
        RecordPage {
            transaction,
            layout,
            block_id,
        }
    }

    pub fn get_integer(&mut self, field_name: String, slot_id: i32) -> Option<i32> {
        if !self.layout.has_field(field_name.clone()) {
            return None;
        }

        let offset = self.layout.get_offset(&field_name).unwrap();
        let record_offset = self.get_offset_of_record(slot_id);
        let field_type = self.layout.get_field_type(field_name.clone()).unwrap();

        if field_type != TableFieldType::INTEGER {
            return None;
        }

        let result = self
            .transaction
            .borrow_mut()
            .get_integer(self.block_id.clone(), (record_offset + offset) as usize);

        Some(result)
    }

    pub fn get_block_id(&self) -> BlockId {
        self.block_id.clone()
    }

    pub fn set_integer(&mut self, field_name: String, slot_id: i32, value: i32) {
        if !self.layout.has_field(field_name.clone()) {
            return;
        }

        let offset = self.layout.get_offset(&field_name).unwrap();
        let record_offset = self.get_offset_of_record(slot_id);
        let field_type = self.layout.get_field_type(field_name.clone()).unwrap();

        if field_type != TableFieldType::INTEGER {
            return;
        }

        println!(
            "set_integer: field_name: {}, slot_id: {}, value: {}",
            field_name, slot_id, value
        );

        self.transaction.borrow_mut().set_integer(
            self.block_id.clone(),
            (record_offset + offset) as usize,
            value,
        );
    }

    fn delete(&mut self, slot_id: i32) {
        let record_offset = self.get_offset_of_record(slot_id);
        self.transaction.borrow_mut().set_integer(
            self.block_id.clone(),
            record_offset as usize,
            RecordType::EMPTY as i32,
        );
    }

    pub fn set_string(&mut self, field_name: String, slot_id: i32, value: String) {
        if !self.layout.has_field(field_name.clone()) {
            return;
        }

        let offset = self.layout.get_offset(&field_name).unwrap();
        let record_offset = self.get_offset_of_record(slot_id);
        let field_type = self.layout.get_field_type(field_name.clone()).unwrap();

        if field_type != TableFieldType::VARCHAR {
            return;
        }

        self.transaction.borrow_mut().set_string(
            self.block_id.clone(),
            (record_offset + offset) as usize,
            value.as_str(),
        );
    }

    fn search_after(&mut self, slot_id: i32, target_record_type: RecordType) -> Option<i32> {
        let mut next_slot_id = slot_id + 1;
        while self.is_valid_slot_id(next_slot_id) {
            let record_offset = self.get_offset_of_record(next_slot_id);
            let record_type = self
                .transaction
                .borrow_mut()
                .get_integer(self.block_id.clone(), record_offset as usize);

            if record_type == target_record_type as i32 {
                return Some(next_slot_id);
            }

            next_slot_id += 1;
        }

        return None;
    }

    pub fn get_string(&mut self, field_name: String, slot_id: i32) -> Option<String> {
        if !self.layout.has_field(field_name.clone()) {
            return None;
        }

        let offset = self.layout.get_offset(&field_name).unwrap();
        let record_offset = self.get_offset_of_record(slot_id);
        let field_type = self.layout.get_field_type(field_name.clone()).unwrap();

        if field_type != TableFieldType::VARCHAR {
            return None;
        }

        let result = self
            .transaction
            .borrow_mut()
            .get_string(self.block_id.clone(), (record_offset + offset) as usize);

        Some(result)
    }

    pub fn set_flag(&mut self, slot_id: i32, record_type: RecordType) {
        let record_offset = self.get_offset_of_record(slot_id);
        self.transaction.borrow_mut().set_integer(
            self.block_id.clone(),
            record_offset as usize,
            record_type as i32,
        );
    }

    pub fn is_valid_slot_id(&self, slot_id: i32) -> bool {
        return self.get_offset_of_record(slot_id + 1)
            <= self.transaction.borrow_mut().get_block_size() as i32;
    }

    pub fn get_offset_of_record(&self, slot_id: i32) -> i32 {
        slot_id * self.layout.get_slot_size()
    }

    pub fn find_next_after_slot_id(&mut self, slot_id: i32) -> Option<i32> {
        return self.search_after(slot_id, RecordType::USED);
    }

    pub fn insert_after_slot_id(&mut self, slot_id: i32) -> Option<i32> {
        let next_slot_id = self.search_after(slot_id, RecordType::EMPTY);

        if let Some(next_slot_id) = next_slot_id {
            self.set_flag(next_slot_id, RecordType::USED);
            return Some(next_slot_id);
        }

        return None;
    }

    pub fn format(&mut self) {
        let mut slot_id = 0;
        while self.is_valid_slot_id(slot_id) {
            println!(
                "slot_id: {}, slot_size: {}",
                slot_id,
                self.layout.get_slot_size()
            );
            self.set_flag(slot_id, RecordType::EMPTY);
            let schema = &self.layout.schema;

            for field in schema.fields() {
                let field_type = schema.get_field_type(field.clone()).unwrap();
                let offset = self.layout.get_offset(&field).unwrap();

                match field_type {
                    TableFieldType::INTEGER => {
                        self.transaction.borrow_mut().set_integer(
                            self.block_id.clone(),
                            offset as usize,
                            0,
                        );
                    }
                    TableFieldType::VARCHAR => {
                        self.transaction.borrow_mut().set_string(
                            self.block_id.clone(),
                            offset as usize,
                            "",
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
    fn test_record_page_v2() {
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

        let mut schema = TableSchema::new();
        schema.add_integer_field("Field1".to_string());
        schema.add_string_field("Field2".to_string(), 9);
        let layout = Layout::new(schema);

        layout.schema.fields().iter().for_each(|field| {
            let offset = layout.get_offset(field);
            println!("Field: {}, Offset: {}", field, offset.unwrap());
        });

        let block = transaction.borrow_mut().append("test_block.txt");

        transaction.borrow_mut().pin(block.clone());

        let mut record_page = RecordPage::new(transaction.clone(), layout, block.clone());
        record_page.format();

        let mut maybe_slot = record_page.insert_after_slot_id(-1);

        while let Some(slot) = maybe_slot {
            let random_value = rand::rng().random_range(0..100);

            record_page.set_integer("Field1".to_string(), slot, random_value);
            record_page.set_string(
                "Field2".to_string(),
                slot,
                format!("Hello {}", random_value),
            );

            maybe_slot = record_page.insert_after_slot_id(slot);
        }

        let mut slot_id = Some(0);

        while let Some(slot) = slot_id {
            let field1_value = record_page.get_integer("Field1".to_string(), slot);
            let field2_value = record_page.get_string("Field2".to_string(), slot);

            if let Some(value) = field1_value {
                println!("Slot: {}, Field1: {}", slot, value);
            } else {
                println!("Slot: {}, Field1: None", slot);
            }

            if let Some(value) = field2_value {
                println!("Slot: {}, Field2: {}", slot, value);
            } else {
                println!("Slot: {}, Field2: None", slot);
            }

            slot_id = record_page.find_next_after_slot_id(slot);
        }

        transaction.borrow_mut().unpin(block.clone());
        transaction.borrow_mut().commit();
    }
}
