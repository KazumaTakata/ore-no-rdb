use std::sync::{Arc, Mutex};

use crate::{
    block::BlockId,
    constant::INTEGER_BYTE_SIZE,
    predicate::{Constant, ConstantValue},
    record_page::Layout,
    transaction_v2::TransactionV2,
};

struct BTreePage {
    transaction: Arc<Mutex<TransactionV2>>,
    current_block: Option<BlockId>,
    layout: Layout,
}

impl BTreePage {
    fn new(transaction: Arc<Mutex<TransactionV2>>, current_block: BlockId, layout: Layout) -> Self {
        transaction.lock().unwrap().pin(current_block.clone());
        BTreePage {
            transaction,
            current_block,
            layout,
        }
    }

    fn find_slot_before(&self, key: Constant) -> i32 {
        let mut slot = 0;

        while slot < self.get_number_of_records()
            && self.get_data_value(slot).compare_to(key.value.clone()) == std::cmp::Ordering::Less
        {
            slot += 1;
        }

        return slot - 1;
    }

    fn close(&mut self) {
        if let Some(current_block) = self.current_block.clone() {
            self.transaction
                .lock()
                .unwrap()
                .unpin(current_block.clone());
        }

        self.current_block = None;
    }

    fn is_full(&self) -> bool {
        let number_of_records = self.get_number_of_records();
        let slot_position = self.get_slot_position(number_of_records as usize + 1);
        return slot_position >= self.transaction.lock().unwrap().get_block_size();
    }

    fn append_new(&self, flag: i32) {
        let block_id = self
            .transaction
            .lock()
            .unwrap()
            .append(self.current_block.clone().unwrap().get_file_name());

        self.transaction.lock().unwrap().pin(block_id);
    }

    fn format(&self, flag: i32) {
        self.transaction.lock().unwrap().set_integer(
            self.current_block.clone().unwrap(),
            0,
            flag,
            false,
        );
        self.transaction.lock().unwrap().set_integer(
            self.current_block.clone().unwrap(),
            INTEGER_BYTE_SIZE,
            0,
            false,
        );
    }

    fn make_default_record(&self, block_id: BlockId, position: i32) {
        for field_name in self.layout.schema.fields() {
            let offset = self.layout.get_offset(field_name).unwrap() as usize;
            let field_type = self
                .layout
                .schema
                .get_field_type(field_name.to_string())
                .unwrap();
            match field_type {
                crate::record_page::TableFieldType::INTEGER => {
                    self.transaction.lock().unwrap().set_integer(
                        block_id.clone(),
                        position as usize + offset,
                        0,
                        false,
                    );
                }
                crate::record_page::TableFieldType::VARCHAR => {
                    self.transaction.lock().unwrap().set_string(
                        block_id.clone(),
                        position as usize + offset,
                        "",
                        false,
                    );
                }
            }
        }
    }

    fn get_number_of_records(&self) -> i32 {
        self.transaction
            .lock()
            .unwrap()
            .get_integer(self.current_block.clone(), INTEGER_BYTE_SIZE)
    }

    fn get_data_value(&self, slot: i32) -> Constant {
        return self.get_value(slot, "dataval");
    }

    fn get_value(&self, slot: i32, field_name: &str) -> Constant {
        let value_type = self.layout.schema.get_field_type(field_name.to_string());
        match value_type {
            Some(field_type) => match field_type {
                crate::record_page::TableFieldType::INTEGER => {
                    let value = self.get_integer(slot, field_name);
                    Constant::new(ConstantValue::Number(value))
                }
                crate::record_page::TableFieldType::VARCHAR => {
                    let value = self.get_string(slot, field_name);
                    Constant::new(ConstantValue::String(value))
                }
            },
            None => panic!("Field not found in schema"),
        }
    }

    fn get_integer(&self, slot: i32, field_name: &str) -> i32 {
        let position = self.field_position(slot as usize, field_name);
        self.transaction
            .lock()
            .unwrap()
            .get_integer(self.current_block.clone(), position)
    }

    fn get_string(&self, slot: i32, field_name: &str) -> String {
        let position = self.field_position(slot as usize, field_name);
        self.transaction
            .lock()
            .unwrap()
            .get_string(self.current_block.clone(), position)
    }

    fn field_position(&self, slot: usize, field_name: &str) -> usize {
        let offset = self.layout.get_offset(field_name);
        return self.get_slot_position(slot) + offset.unwrap() as usize;
    }

    fn get_slot_position(&self, slot: usize) -> usize {
        let slot_size = self.layout.get_slot_size();
        return INTEGER_BYTE_SIZE * 2 + slot * slot_size as usize;
    }
}
