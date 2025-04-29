use core::panic;

use crate::{
    block,
    buffer_manager::{self, BufferList},
    file_manager::{self, FileManager},
    record_page,
    scan::Scan,
    transaction::{self, Transaction},
};

pub struct RecordID {
    block_number: u64,
    slot_number: i32,
}

impl RecordID {
    pub fn new(block_number: u64, slot_number: i32) -> Self {
        RecordID {
            block_number,
            slot_number,
        }
    }

    pub fn get_block_number(&self) -> u64 {
        self.block_number
    }

    pub fn get_slot_number(&self) -> i32 {
        self.slot_number
    }

    pub fn equals(&self, other: &RecordID) -> bool {
        self.block_number == other.block_number && self.slot_number == other.slot_number
    }

    pub fn to_string(&self) -> String {
        format!("RecordID({}, {})", self.block_number, self.slot_number)
    }
}

pub struct TableScan {
    file_name: String,
    record_page: record_page::RecordPage,
    current_slot: i32,
    layout: record_page::Layout,
}

impl TableScan {
    pub fn new(
        table_name: String,
        transaction: &mut Transaction,
        file_manager: &mut FileManager,
        layout: record_page::Layout,
        buffer_list: &mut BufferList,
    ) -> Self {
        let file_name = format!("{}.tbl", table_name);
        let block_size = transaction.get_size(file_manager, file_name.clone());

        if block_size == 0 {
            let block_id = transaction.append(file_manager, &file_name);
            let record_page = record_page::RecordPage::new(layout.clone(), block_id);
            record_page.format(transaction, buffer_list, &file_manager);
            return TableScan {
                file_name,
                record_page,
                layout,
                current_slot: -1,
            };
        } else {
            let block_id = block::BlockId::new(file_name.clone(), 0);
            let record_page = record_page::RecordPage::new(layout.clone(), block_id);
            return TableScan {
                file_name,
                record_page,
                layout,
                current_slot: -1,
            };
        }
    }
    pub fn move_to_block(&mut self, block_number: u64) {
        let block_id = block::BlockId::new(self.file_name.clone(), block_number);
        self.record_page = record_page::RecordPage::new(self.layout.clone(), block_id);
    }

    pub fn move_to_new_block(
        &mut self,
        transaction: &mut Transaction,
        file_manager: &mut FileManager,
        layout: record_page::Layout,
        buffer_list: &mut BufferList,
    ) -> record_page::RecordPage {
        let block_id = transaction.append(file_manager, &self.file_name);
        let record_page = record_page::RecordPage::new(layout, block_id);
        record_page.format(transaction, buffer_list, &file_manager);
        self.current_slot = -1;
        return record_page;
    }

    fn at_last_block(&self, transaction: &Transaction, file_manager: &mut FileManager) -> bool {
        let file_size = transaction.get_size(file_manager, self.file_name.clone());
        let current_block = self.record_page.get_block_id().get_block_number();
        current_block as usize == file_size - 1
    }
}

impl Scan for TableScan {
    fn set_integer(
        &mut self,
        transaction: &mut Transaction,
        buffer_list: &mut BufferList,
        field_name: String,
        value: i32,
    ) {
        self.record_page.set_integer(
            field_name,
            self.current_slot,
            transaction,
            buffer_list,
            value,
        )
    }

    fn set_string(
        &mut self,
        transaction: &mut Transaction,
        buffer_list: &mut BufferList,
        field_name: String,
        value: String,
    ) {
        self.record_page.set_string(
            field_name,
            self.current_slot,
            transaction,
            buffer_list,
            value,
        )
    }

    fn insert(
        &mut self,
        transaction: &mut Transaction,
        buffer_list: &mut BufferList,
        file_manager: &mut FileManager,
        layout: record_page::Layout,
    ) {
        if self.current_slot == -1 {
            self.current_slot = 0;
        }

        let mut current_slot = self.record_page.insert_after_slot_id(
            self.current_slot,
            file_manager,
            buffer_list,
            transaction,
        );

        // current_slotが optionalだったら、次のblockに移動する
        while current_slot.is_none() {
            if self.at_last_block(transaction, file_manager) {
                self.record_page =
                    self.move_to_new_block(transaction, file_manager, layout.clone(), buffer_list);
            } else {
                let block_id = block::BlockId::new(
                    self.file_name.clone(),
                    self.record_page.get_block_id().get_block_number() + 1,
                );
                self.record_page = record_page::RecordPage::new(layout.clone(), block_id);
            }
            current_slot =
                self.record_page
                    .insert_after_slot_id(-1, file_manager, buffer_list, transaction);
        }
    }

    fn get_record_id(&self) -> RecordID {
        return RecordID::new(
            self.record_page.get_block_id().get_block_number(),
            self.current_slot,
        );
    }

    fn delete(&mut self) {}

    fn move_to_record_id(&mut self, layout: record_page::Layout, record_id: RecordID) {
        let block_id = block::BlockId::new(self.file_name.clone(), record_id.get_block_number());
        self.record_page = record_page::RecordPage::new(layout, block_id);
        self.current_slot = record_id.get_slot_number();
    }

    fn move_to_before_first(
        &mut self,
        file_manager: &mut FileManager,
        buffer_list: &mut BufferList,
        transaction: &mut Transaction,
    ) {
        self.move_to_block(0)
    }

    fn get_integer(
        &mut self,
        transaction: &mut Transaction,
        buffer_list: &mut BufferList,
        field_name: String,
    ) -> Option<i32> {
        self.record_page
            .get_integer(field_name, self.current_slot, transaction, buffer_list)
    }

    fn get_string(
        &mut self,
        transaction: &mut Transaction,
        buffer_list: &mut BufferList,
        field_name: String,
    ) -> Option<String> {
        self.record_page
            .get_string(field_name, self.current_slot, transaction, buffer_list)
    }

    fn get_value(
        &mut self,
        transaction: &mut Transaction,
        buffer_list: &mut BufferList,
        field_name: String,
    ) -> crate::predicate::ConstantValue {
        let field_type = self.layout.schema.get_field_type(field_name.clone());

        if let field_type = record_page::TableFieldType::INTEGER {
            let integer_value = self.get_integer(transaction, buffer_list, field_name);
            if let Some(value) = integer_value {
                return crate::predicate::ConstantValue::Number(value);
            } else {
                return crate::predicate::ConstantValue::Null;
            }
        } else if let _field_type = record_page::TableFieldType::VARCHAR {
            let string_value = self.get_string(transaction, buffer_list, field_name);
            if let Some(value) = string_value {
                return crate::predicate::ConstantValue::String(value);
            } else {
                return crate::predicate::ConstantValue::Null;
            }
        } else {
            panic!("Unknown field type");
        }
    }

    fn next(
        &mut self,
        file_manager: &mut FileManager,
        buffer_list: &mut BufferList,
        transaction: &mut Transaction,
    ) -> bool {
        let mut current_slot = self.record_page.find_next_after_slot_id(
            self.current_slot,
            file_manager,
            buffer_list,
            transaction,
        );

        while current_slot.is_none() {
            if self.at_last_block(transaction, file_manager) {
                return false;
            }
            self.move_to_block(self.record_page.get_block_id().get_block_number() + 1);

            current_slot = self.record_page.find_next_after_slot_id(
                -1,
                file_manager,
                buffer_list,
                transaction,
            );
        }

        return true;
    }

    fn has_field(&self, field_name: String) -> bool {
        self.layout.schema.has_field(field_name)
    }

    fn close(
        &mut self,
        transaction: &mut Transaction,
        buffer_list: &mut BufferList,
        buffer_manager: &mut buffer_manager::BufferManager,
    ) {
        transaction.unpin(buffer_list, buffer_manager, self.record_page.get_block_id());
    }
}

pub struct ProjectScan {
    scan: Box<dyn Scan>,
    field_names: Vec<String>,
}

impl ProjectScan {
    pub fn new(scan: Box<dyn Scan>, field_names: Vec<String>) -> Self {
        ProjectScan { scan, field_names }
    }
}

impl Scan for ProjectScan {
    fn has_field(&self, field_name: String) -> bool {
        self.field_names.contains(&field_name)
    }

    fn move_to_before_first(
        &mut self,
        file_manager: &mut FileManager,
        buffer_list: &mut BufferList,
        transaction: &mut Transaction,
    ) {
        self.scan
            .move_to_before_first(file_manager, buffer_list, transaction);
    }

    fn get_integer(
        &mut self,
        transaction: &mut Transaction,
        buffer_list: &mut BufferList,
        field_name: String,
    ) -> Option<i32> {
        if self.has_field(field_name.clone()) {
            return self.scan.get_integer(transaction, buffer_list, field_name);
        } else {
            panic!("Field {} not found in ProjectScan", field_name);
        }
    }

    fn get_string(
        &mut self,
        transaction: &mut Transaction,
        buffer_list: &mut BufferList,
        field_name: String,
    ) -> Option<String> {
        if self.has_field(field_name.clone()) {
            return self.scan.get_string(transaction, buffer_list, field_name);
        } else {
            panic!("Field {} not found in ProjectScan", field_name);
        }
    }

    fn get_value(
        &mut self,
        transaction: &mut Transaction,
        buffer_list: &mut BufferList,
        field_name: String,
    ) -> crate::predicate::ConstantValue {
        if self.has_field(field_name.clone()) {
            return self.scan.get_value(transaction, buffer_list, field_name);
        } else {
            panic!("Field {} not found in ProjectScan", field_name);
        }
    }

    fn close(
        &mut self,
        transaction: &mut Transaction,
        buffer_list: &mut BufferList,
        buffer_manager: &mut buffer_manager::BufferManager,
    ) {
        self.scan.close(transaction, buffer_list, buffer_manager);
    }

    fn next(
        &mut self,
        file_manager: &mut FileManager,
        buffer_list: &mut BufferList,
        transaction: &mut Transaction,
    ) -> bool {
        self.scan.next(file_manager, buffer_list, transaction)
    }

    fn delete(&mut self) {
        panic!("Delete not supported in ProjectScan");
    }

    fn get_record_id(&self) -> RecordID {
        panic!("get_record_id not supported in ProjectScan");
    }

    fn insert(
        &mut self,
        transaction: &mut Transaction,
        buffer_list: &mut BufferList,
        file_manager: &mut FileManager,
        layout: record_page::Layout,
    ) {
        panic!("Insert not supported in ProjectScan");
    }

    fn move_to_record_id(&mut self, layout: record_page::Layout, record_id: RecordID) {
        panic!("move_to_record_id not supported in ProjectScan");
    }

    fn set_integer(
        &mut self,
        transaction: &mut Transaction,
        buffer_list: &mut BufferList,
        field_name: String,
        value: i32,
    ) {
        panic!("set_integer not supported in ProjectScan");
    }

    fn set_string(
        &mut self,
        transaction: &mut Transaction,
        buffer_list: &mut BufferList,
        field_name: String,
        value: String,
    ) {
        panic!("set_string not supported in ProjectScan");
    }
}
