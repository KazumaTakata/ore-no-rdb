use std::{cell::RefCell, rc::Rc};

use crate::{
    block::BlockId,
    record_page::{Layout, TableFieldType},
    record_page_v2::RecordPage,
    scan::ScanV2,
    table_scan::RecordID,
    transaction_v2::TransactionV2,
};

pub struct TableScan {
    file_name: String,
    record_page: RecordPage,
    transaction: Rc<RefCell<TransactionV2>>,
    current_slot: i32,
    layout: Layout,
}

impl TableScan {
    pub fn new(
        table_name: String,
        transaction: Rc<RefCell<TransactionV2>>,
        layout: Layout,
    ) -> Self {
        let file_name = format!("{}.tbl", table_name);
        let block_size = transaction.borrow_mut().get_size(file_name.clone());

        if block_size == 0 {
            let block_id = transaction.borrow_mut().append(&file_name);
            let mut record_page = RecordPage::new(transaction.clone(), layout.clone(), block_id);
            record_page.format();
            return TableScan {
                file_name,
                record_page,
                transaction: transaction.clone(),
                layout: layout.clone(),
                current_slot: -1,
            };
        } else {
            let block_id = BlockId::new(file_name.clone(), 0);
            let record_page = RecordPage::new(transaction.clone(), layout.clone(), block_id);
            return TableScan {
                file_name,
                record_page,
                transaction: transaction.clone(),
                layout,
                current_slot: -1,
            };
        }
    }
    pub fn move_to_block(&mut self, block_number: u64) {
        let block_id = BlockId::new(self.file_name.clone(), block_number);
        self.record_page = RecordPage::new(self.transaction.clone(), self.layout.clone(), block_id);
        self.current_slot = -1;
    }

    pub fn move_to_new_block(&mut self) {
        let block_id = self.transaction.borrow_mut().append(&self.file_name);
        let mut record_page =
            RecordPage::new(self.transaction.clone(), self.layout.clone(), block_id);
        record_page.format();
        self.current_slot = -1;
    }

    fn at_last_block(&self) -> bool {
        let file_size = self.transaction.borrow().get_size(self.file_name.clone());
        let current_block = self.record_page.get_block_id().get_block_number();
        current_block as usize == file_size - 1
    }

    fn close(&mut self) {
        self.transaction
            .borrow_mut()
            .unpin(self.record_page.get_block_id());
    }
}

impl ScanV2 for TableScan {
    fn set_integer(&mut self, field_name: String, value: i32) {
        self.record_page
            .set_integer(field_name, self.current_slot, value)
    }

    fn set_string(&mut self, field_name: String, value: String) {
        self.record_page
            .set_string(field_name, self.current_slot, value)
    }

    fn set_value(&mut self, field_name: String, value: crate::predicate::ConstantValue) {
        match value {
            crate::predicate::ConstantValue::Number(num) => {
                self.set_integer(field_name, num);
            }
            crate::predicate::ConstantValue::String(string) => {
                self.set_string(field_name, string);
            }
            crate::predicate::ConstantValue::Null => {
                panic!("Null value cannot be set");
            }
        }
    }

    fn insert(&mut self) {
        if self.current_slot == -1 {
            self.current_slot = 0;
        }

        let mut current_slot = self.record_page.insert_after_slot_id(self.current_slot);

        // current_slotが optionalだったら、次のblockに移動する
        while current_slot.is_none() {
            if self.at_last_block() {
                self.move_to_new_block();
            } else {
                let block_id = BlockId::new(
                    self.file_name.clone(),
                    self.record_page.get_block_id().get_block_number() + 1,
                );
                self.record_page =
                    RecordPage::new(self.transaction.clone(), self.layout.clone(), block_id);
            }
            current_slot = self.record_page.insert_after_slot_id(self.current_slot)
        }
    }

    fn get_record_id(&self) -> RecordID {
        return RecordID::new(
            self.record_page.get_block_id().get_block_number(),
            self.current_slot,
        );
    }

    fn delete(&mut self) {}

    fn move_to_record_id(&mut self, record_id: RecordID) {
        let block_id = BlockId::new(self.file_name.clone(), record_id.get_block_number());
        self.record_page = RecordPage::new(self.transaction.clone(), self.layout.clone(), block_id);
        self.current_slot = record_id.get_slot_number();
    }

    fn move_to_before_first(&mut self) {
        self.move_to_block(0)
    }

    fn get_integer(&mut self, field_name: String) -> Option<i32> {
        self.record_page.get_integer(field_name, self.current_slot)
    }

    fn get_string(&mut self, field_name: String) -> Option<String> {
        self.record_page.get_string(field_name, self.current_slot)
    }

    fn get_value(&mut self, field_name: String) -> crate::predicate::ConstantValue {
        let field_type = self.layout.schema.get_field_type(field_name.clone());

        if let field_type = TableFieldType::INTEGER {
            let integer_value = self.get_integer(field_name);
            if let Some(value) = integer_value {
                return crate::predicate::ConstantValue::Number(value);
            } else {
                return crate::predicate::ConstantValue::Null;
            }
        } else if let _field_type = TableFieldType::VARCHAR {
            let string_value = self.get_string(field_name);
            if let Some(value) = string_value {
                return crate::predicate::ConstantValue::String(value);
            } else {
                return crate::predicate::ConstantValue::Null;
            }
        } else {
            panic!("Unknown field type");
        }
    }

    fn next(&mut self) -> bool {
        let mut current_slot = self.record_page.find_next_after_slot_id(self.current_slot);

        while current_slot.is_none() {
            if self.at_last_block() {
                return false;
            }
            self.move_to_block(self.record_page.get_block_id().get_block_number() + 1);

            current_slot = self.record_page.find_next_after_slot_id(-1);
        }

        return true;
    }

    fn has_field(&self, field_name: String) -> bool {
        self.layout.schema.has_field(field_name)
    }

    fn close(&mut self) {
        self.transaction
            .borrow_mut()
            .unpin(self.record_page.get_block_id());
    }
}
