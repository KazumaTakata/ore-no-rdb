use std::{cell::RefCell, rc::Rc};

use crate::{
    block::BlockId,
    error::ValueNotFound,
    predicate::TableNameAndFieldName,
    record_page::{Layout, TableFieldType},
    record_page_v2::RecordPage,
    scan_v2::ScanV2,
    transaction_v2::TransactionV2,
};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
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
    table_name: String,
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
                table_name,
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
                table_name,
                record_page,
                transaction: transaction.clone(),
                layout,
                current_slot: -1,
            };
        }
    }
    pub fn move_to_block(&mut self, block_number: u64) {
        self.close();
        let block_id = BlockId::new(self.file_name.clone(), block_number);
        self.record_page = RecordPage::new(self.transaction.clone(), self.layout.clone(), block_id);
        self.current_slot = -1;
    }

    pub fn move_to_new_block(&mut self) {
        self.close();
        let block_id = self.transaction.borrow_mut().append(&self.file_name);
        self.record_page = RecordPage::new(self.transaction.clone(), self.layout.clone(), block_id);
        self.record_page.format();
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
        self.current_slot = self
            .record_page
            .insert_after_slot_id(self.current_slot)
            .unwrap_or(-1);

        // current_slotが optionalだったら、次のblockに移動する
        while self.current_slot == -1 {
            if self.at_last_block() {
                self.move_to_new_block();
            } else {
                self.move_to_block(self.record_page.get_block_id().get_block_number() + 1);
            }
            self.current_slot = self
                .record_page
                .insert_after_slot_id(self.current_slot)
                .unwrap_or(-1)
        }
    }

    fn get_record_id(&self) -> RecordID {
        return RecordID::new(
            self.record_page.get_block_id().get_block_number(),
            self.current_slot,
        );
    }

    fn delete(&mut self) {
        self.record_page.delete(self.current_slot);
    }

    fn move_to_record_id(&mut self, record_id: RecordID) {
        self.close();
        let block_id = BlockId::new(self.file_name.clone(), record_id.get_block_number());
        self.record_page = RecordPage::new(self.transaction.clone(), self.layout.clone(), block_id);
        self.current_slot = record_id.get_slot_number();
    }

    fn move_to_before_first(&mut self) -> Result<(), ValueNotFound> {
        self.move_to_block(0);
        Ok(())
    }

    fn get_integer(&mut self, field_name: TableNameAndFieldName) -> Option<i32> {
        match field_name.table_name.clone() {
            Some(name) => {
                if name != self.table_name {
                    return None;
                }
            }
            None => {}
        }

        self.record_page
            .get_integer(field_name.field_name, self.current_slot)
    }

    fn get_string(&mut self, field_name: TableNameAndFieldName) -> Option<String> {
        match field_name.table_name.clone() {
            Some(name) => {
                if name != self.table_name {
                    return None;
                }
            }
            None => {}
        }

        self.record_page
            .get_string(field_name.field_name, self.current_slot)
    }

    fn get_value(
        &mut self,
        field_name: TableNameAndFieldName,
    ) -> Option<crate::predicate::ConstantValue> {
        match field_name.table_name.clone() {
            Some(name) => {
                if name != self.table_name {
                    return None;
                }
            }
            None => {}
        }

        let field_type = self
            .layout
            .schema
            .get_field_type(field_name.field_name.clone());

        match field_type {
            None => return None,
            Some(table_field_type) => match table_field_type {
                TableFieldType::INTEGER => {
                    let integer_value = self.get_integer(field_name);
                    if let Some(value) = integer_value {
                        return Some(crate::predicate::ConstantValue::Number(value));
                    } else {
                        return Some(crate::predicate::ConstantValue::Null);
                    }
                }
                TableFieldType::VARCHAR => {
                    let string_value = self.get_string(field_name);
                    if let Some(value) = string_value {
                        return Some(crate::predicate::ConstantValue::String(value));
                    } else {
                        return Some(crate::predicate::ConstantValue::Null);
                    }
                }
            },
        };
    }

    fn next(&mut self) -> Result<bool, ValueNotFound> {
        self.current_slot = self
            .record_page
            .find_next_after_slot_id(self.current_slot)
            .unwrap_or(-1);

        while self.current_slot == -1 {
            if self.at_last_block() {
                return Ok(false);
            }
            self.move_to_block(self.record_page.get_block_id().get_block_number() + 1);

            self.current_slot = self
                .record_page
                .find_next_after_slot_id(self.current_slot)
                .unwrap_or(-1)
        }

        return Ok(true);
    }

    fn has_field(&self, field_name: TableNameAndFieldName) -> bool {
        self.layout.schema.has_field(field_name.field_name)
            && (field_name.table_name.is_none()
                || field_name.table_name.as_ref().unwrap() == &self.table_name)
    }

    fn close(&mut self) {
        self.transaction
            .borrow_mut()
            .unpin(self.record_page.get_block_id());
    }
}

#[cfg(test)]
mod tests {
    use std::{cell::RefCell, fs::remove_file, path::Path, rc::Rc};

    use rand::Rng;

    use crate::{
        buffer_manager_v2::BufferManagerV2,
        concurrency_manager::LockTable,
        file_manager::{self, FileManager},
        log_manager_v2::LogManagerV2,
        record_page::TableSchema,
    };

    use super::*;

    #[test]
    fn test_table_scan_v2() -> Result<(), Box<dyn std::error::Error>> {
        let test_dir = Path::new("test_data");
        let block_size = 400;
        let log_file_name = format!("log_file_{}.txt", uuid::Uuid::new_v4());

        let file_manager = Rc::new(RefCell::new(FileManager::new(test_dir, block_size)));
        let log_manager = Rc::new(RefCell::new(LogManagerV2::new(
            file_manager.clone(),
            log_file_name.clone(),
        )));

        let buffer_manager = Rc::new(RefCell::new(BufferManagerV2::new(
            3,
            file_manager.clone(),
            log_manager.clone(),
        )));

        let lock_table = Rc::new(RefCell::new(LockTable::new()));

        let transaction = Rc::new(RefCell::new(TransactionV2::new(
            1,
            file_manager.clone(),
            buffer_manager.clone(),
            lock_table.clone(),
            log_manager.clone(),
        )));

        let mut schema = TableSchema::new();

        let integer_field_name = "Field1".to_string();
        let string_field_name = "Field2".to_string();

        schema.add_integer_field(integer_field_name.clone());
        schema.add_string_field(string_field_name.clone(), 9);
        let layout = Layout::new(schema);

        layout.schema.fields().iter().for_each(|field| {
            let offset = layout.get_offset(field);
            println!("Field: {}, Offset: {}", field, offset.unwrap());
        });

        let test_file_name = format!("test_table_{}", uuid::Uuid::new_v4());

        let mut table_scan =
            TableScan::new(test_file_name.clone(), transaction.clone(), layout.clone());

        _ = table_scan.move_to_before_first();

        for _ in 0..1000 {
            table_scan.insert();
            let random_value = rand::rng().random_range(0..100);
            table_scan.set_integer(integer_field_name.clone(), random_value);
            table_scan.set_string(string_field_name.clone(), format!("Hello {}", random_value));
        }

        _ = table_scan.move_to_before_first();

        let mut count = 0;

        while table_scan.next()? {
            let field1_value = table_scan
                .get_integer(TableNameAndFieldName::new(None, integer_field_name.clone()));
            let field2_value =
                table_scan.get_string(TableNameAndFieldName::new(None, string_field_name.clone()));

            assert!(field1_value.unwrap() >= 0 && field1_value.unwrap() < 100);
            assert!(field2_value
                .unwrap()
                .starts_with(&format!("Hello {}", field1_value.unwrap())));
            count += 1;
        }

        assert_eq!(count, 1000);

        table_scan.close();
        transaction.borrow_mut().commit();

        _ = remove_file(test_dir.join(log_file_name));
        _ = remove_file(test_dir.join(format!("{}.tbl", test_file_name)));

        return Ok(());
    }
}
