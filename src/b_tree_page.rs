use std::cell::RefCell;
use std::rc::Rc;

use crate::{
    b_tree_leaf::DirectoryEntry,
    block::BlockId,
    constant::INTEGER_BYTE_SIZE,
    predicate::{Constant, ConstantValue},
    record_page::{Layout, TableFieldType},
    table_scan_v2::RecordID,
    transaction_v2::TransactionV2,
};

pub struct BTreePage {
    transaction: Rc<RefCell<TransactionV2>>,
    current_block: BlockId,
    layout: Layout,
}

impl BTreePage {
    pub fn new(
        transaction: Rc<RefCell<TransactionV2>>,
        current_block: BlockId,
        layout: Layout,
    ) -> Self {
        transaction.borrow_mut().pin(current_block.clone());
        BTreePage {
            transaction,
            current_block,
            layout,
        }
    }

    pub fn find_slot_before(&self, key: Constant) -> i32 {
        let mut slot = 0;

        while slot < self.get_number_of_records()
            && self
                .get_data_value(slot as usize)
                .compare_to(key.value.clone())
                == std::cmp::Ordering::Less
        {
            slot += 1;
        }

        return slot - 1;
    }

    pub fn split(&mut self, split_pos: usize, flag: i32) -> BlockId {
        let new_block_id = self.append_new(flag);
        let mut new_page = BTreePage::new(
            self.transaction.clone(),
            new_block_id.clone(),
            self.layout.clone(),
        );
        self.transfer_records(split_pos, &mut new_page);
        new_page.set_flag(flag);
        new_page.close();
        return new_block_id;
    }

    pub fn get_data_record_id(&self, slot: usize) -> RecordID {
        let block_id = self.get_integer(slot, "block");
        let slot_number = self.get_integer(slot, "id");
        return RecordID::new(block_id as u64, slot_number);
    }

    pub fn set_flag(&mut self, flag: i32) {
        self.transaction
            .borrow_mut()
            .set_integer(self.current_block.clone(), 0, flag, true);
    }

    pub fn get_flag(&self) -> i32 {
        self.transaction
            .borrow_mut()
            .get_integer(self.current_block.clone(), 0)
    }

    fn transfer_records(&mut self, slot: usize, dest: &mut BTreePage) {
        let mut dest_slot = 0;
        while slot < self.get_number_of_records() as usize {
            dest.insert(dest_slot);
            let schema = self.layout.schema.clone();
            for field_name in schema.fields() {
                let value = self.get_value(slot, field_name);
                dest.set_value(field_name, dest_slot as usize, value);
            }
            self.delete(slot);
            dest_slot += 1;
        }
    }

    fn insert(&mut self, slot: usize) {
        let number_of_records = self.get_number_of_records();
        for i in (slot + 1..=number_of_records as usize).rev() {
            self.copy_record(i - 1, i);
        }
        self.set_number_of_records(number_of_records + 1);
    }

    pub fn delete(&mut self, slot: usize) {
        let number_of_records = self.get_number_of_records();
        for i in slot..number_of_records as usize - 1 {
            self.copy_record(i + 1, i);
        }
        self.set_number_of_records(number_of_records - 1);
    }

    fn set_number_of_records(&mut self, number_of_records: i32) {
        self.transaction.borrow_mut().set_integer(
            self.current_block.clone(),
            INTEGER_BYTE_SIZE,
            number_of_records,
            true,
        );
    }

    fn copy_record(&mut self, from_slot: usize, to_slot: usize) {
        let schema = self.layout.schema.clone();
        for field_name in schema.fields() {
            let value = self.get_value(from_slot, field_name);
            self.set_value(field_name, to_slot, value);
        }
    }

    fn set_value(&mut self, field_name: &str, slot: usize, value: Constant) {
        let value_type = self
            .layout
            .schema
            .get_field_type(field_name.to_string())
            .unwrap();

        match value_type {
            TableFieldType::INTEGER => {
                let integer_value = match value.value {
                    ConstantValue::Number(num) => num,
                    _ => panic!("Expected a number for INTEGER field"),
                };
                self.set_integer(slot, field_name, integer_value);
            }
            TableFieldType::VARCHAR => {
                let string_value = match value.value {
                    ConstantValue::String(s) => s,
                    _ => panic!("Expected a string for VARCHAR field"),
                };
                self.set_string(slot, field_name, string_value);
            }
        }
    }

    fn set_integer(&mut self, slot: usize, field_name: &str, integer_value: i32) {
        let position = self.field_position(slot, field_name);
        self.transaction.borrow_mut().set_integer(
            self.current_block.clone(),
            position,
            integer_value,
            true,
        );
    }

    fn set_string(&mut self, slot: usize, field_name: &str, string_value: String) {
        let position = self.field_position(slot, field_name);
        self.transaction.borrow_mut().set_string(
            self.current_block.clone(),
            position,
            &string_value,
            true,
        );
    }

    pub fn close(&mut self) {
        self.transaction
            .borrow_mut()
            .unpin(self.current_block.clone());
    }

    pub fn is_full(&self) -> bool {
        let number_of_records = self.get_number_of_records();
        let slot_position = self.get_slot_position(number_of_records as usize + 1);
        return slot_position >= self.transaction.borrow_mut().get_block_size();
    }

    fn append_new(&self, flag: i32) -> BlockId {
        let block_id = self
            .transaction
            .borrow_mut()
            .append(self.current_block.clone().get_file_name());

        self.transaction.borrow_mut().pin(block_id.clone());
        self.format(block_id.clone(), flag);
        return block_id;
    }

    /// BTreeページを初期化する。
    ///
    /// ページのバイトレイアウト (整数はすべて i32 / 4byte / ビッグエンディアン):
    ///
    /// ```text
    /// byte:  0          4          8                          block_size
    ///        ┌──────────┬──────────┬───────────────────────────┐
    ///        │ flag     │ #records │ records...                │
    ///        │ (i32)    │ (i32)    │ slot0 | slot1 | slot2 | … │
    ///        │ 4byte    │ 4byte    │ 各 slot は slot_size byte │
    ///        └──────────┴──────────┴───────────────────────────┘
    ///         0          4          8 = INTEGER_BYTE_SIZE * 2
    /// ```
    ///
    /// - `0..4`   : flag（葉=-1 / 内部ノードはレベルなど）
    /// - `4..8`   : このページが持つレコード数（初期値 0）
    /// - `8..`    : レコード本体。slot ごとに `layout` の各 field が
    ///              `get_offset(field)` の位置に格納される。
    pub fn format(&self, block_id: BlockId, flag: i32) {
        self.transaction
            .borrow_mut()
            .set_integer(self.current_block.clone(), 0, flag, false);
        self.transaction.borrow_mut().set_integer(
            self.current_block.clone(),
            INTEGER_BYTE_SIZE,
            0,
            false,
        );

        let record_size = self.layout.get_slot_size() as usize;

        for position in (INTEGER_BYTE_SIZE * 2..=self.transaction.borrow_mut().get_block_size())
            .step_by(record_size)
        {
            self.make_default_record(block_id.clone(), position as i32);
        }
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
                    self.transaction.borrow_mut().set_integer(
                        block_id.clone(),
                        position as usize + offset,
                        0,
                        false,
                    );
                }
                crate::record_page::TableFieldType::VARCHAR => {
                    self.transaction.borrow_mut().set_string(
                        block_id.clone(),
                        position as usize + offset,
                        "",
                        false,
                    );
                }
            }
        }
    }

    pub fn insert_leaf(&mut self, slot: usize, value: Constant, record_id: RecordID) {
        self.insert(slot);
        self.set_value("dataval", slot, value);
        self.set_integer(slot, "block", record_id.get_block_number() as i32);
        self.set_integer(slot, "id", record_id.get_slot_number() as i32);
    }

    pub fn insert_directory(&mut self, slot: usize, directory_entry: DirectoryEntry) {
        self.insert(slot);
        self.set_value("dataval", slot, directory_entry.data_value.clone());
        self.set_integer(slot, "block", directory_entry.block_number as i32);
    }

    pub fn get_child_number(&self, slot: usize) -> i32 {
        return self.get_integer(slot, "block");
    }

    pub fn get_number_of_records(&self) -> i32 {
        self.transaction
            .borrow_mut()
            .get_integer(self.current_block.clone(), INTEGER_BYTE_SIZE)
    }

    pub fn get_data_value(&self, slot: usize) -> Constant {
        return self.get_value(slot, "dataval");
    }

    fn get_value(&self, slot: usize, field_name: &str) -> Constant {
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

    fn get_integer(&self, slot: usize, field_name: &str) -> i32 {
        let position = self.field_position(slot, field_name);
        self.transaction
            .borrow_mut()
            .get_integer(self.current_block.clone(), position)
    }

    fn get_string(&self, slot: usize, field_name: &str) -> String {
        let position = self.field_position(slot, field_name);
        self.transaction
            .borrow_mut()
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
