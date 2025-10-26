use std::{cell::RefCell, rc::Rc, vec};

use crate::{
    block::BlockId,
    error::ValueNotFound,
    predicate::ConstantValue,
    record_page::{Layout, TableFieldType},
    record_page_v2::RecordPage,
    scan_v2::ScanV2,
    transaction_v2::TransactionV2,
};

fn get_buffer_size_for_sorting(available_buffer_size: usize, table_size: usize) -> usize {
    let available_buffer_size = available_buffer_size - 2;

    if available_buffer_size <= 1 {
        return 1;
    }

    let mut best_buffer_size = usize::MAX;

    let mut index = 1;

    while best_buffer_size > available_buffer_size {
        index += 1;
        best_buffer_size = (table_size as f64).powf(1.0 / index as f64).ceil() as usize;
    }

    return best_buffer_size;
}

fn get_buffer_size_for_product(available_buffer_size: usize, table_size: usize) -> usize {
    let available_buffer_size = available_buffer_size - 2;

    if available_buffer_size <= 1 {
        return 1;
    }

    let mut best_buffer_size = usize::MAX;

    let mut index = 1;

    while best_buffer_size > available_buffer_size {
        index += 1;
        best_buffer_size = (table_size as f64).powf(1.0 / index as f64).ceil() as usize;
    }

    return best_buffer_size;
}

struct ChunkScan {
    transaction: Rc<RefCell<TransactionV2>>,
    file_name: String,
    layout: Layout,
    start_buffer_index: usize,
    end_buffer_index: usize,
    current_buffer_index: usize,
    current_slot_id: i32,
    record_page_list: Vec<RecordPage>,
}

impl ChunkScan {
    pub fn new(
        transaction: Rc<RefCell<TransactionV2>>,
        file_name: String,
        layout: Layout,
        start_buffer_index: usize,
        end_buffer_index: usize,
    ) -> ChunkScan {
        let mut record_page_list = vec![];
        for i in start_buffer_index..end_buffer_index {
            let block = BlockId::new(file_name.clone(), i as u64);
            let record_page = RecordPage::new(transaction.clone(), layout.clone(), block);
            record_page_list.push(record_page);
        }

        ChunkScan {
            transaction: transaction.clone(),
            file_name,
            layout,
            start_buffer_index,
            end_buffer_index,
            current_buffer_index: start_buffer_index,
            record_page_list,
            current_slot_id: -1,
        }
    }

    fn move_to_block(&mut self, block_index: usize) {
        self.current_buffer_index = block_index;
        self.current_slot_id = -1;
    }

    fn get_current_record_page(&mut self) -> &mut RecordPage {
        self.record_page_list
            .get_mut(self.current_buffer_index - self.start_buffer_index)
            .unwrap()
    }
}

impl ScanV2 for ChunkScan {
    fn next(&mut self) -> Result<bool, ValueNotFound> {
        self.current_slot_id = {
            let current_slot_id = self.current_slot_id;
            let record_page = self.get_current_record_page();
            record_page
                .find_next_after_slot_id(current_slot_id)
                .unwrap()
        };

        while self.current_slot_id < 0 {
            if self.current_buffer_index == self.end_buffer_index {
                return Ok(false);
            }
            let mut record_page = self.get_current_record_page();
            let block_number = record_page.get_block_id().get_block_number();

            self.move_to_block(block_number as usize + 1);

            let current_slot_id = self.current_slot_id;
            record_page = self.get_current_record_page();
            self.current_slot_id = record_page
                .find_next_after_slot_id(current_slot_id)
                .unwrap();
        }

        return Ok(true);
    }

    fn get_integer(&mut self, field_name: crate::predicate::TableNameAndFieldName) -> Option<i32> {
        let current_slot_id = self.current_slot_id;
        let record_page = self.get_current_record_page();
        record_page.get_integer(field_name.field_name, current_slot_id)
    }

    fn get_string(
        &mut self,
        field_name: crate::predicate::TableNameAndFieldName,
    ) -> Option<String> {
        let current_slot_id = self.current_slot_id;
        let record_page = self.get_current_record_page();
        record_page.get_string(field_name.field_name, current_slot_id)
    }

    fn move_to_before_first(&mut self) -> Result<(), ValueNotFound> {
        self.move_to_block(self.start_buffer_index);
        Ok(())
    }

    fn close(&mut self) {
        for i in 0..self.record_page_list.len() {
            let block = BlockId::new(self.file_name.clone(), (self.start_buffer_index + i) as u64);
            self.transaction.borrow_mut().unpin(block);
        }
    }

    fn get_value(
        &mut self,
        field_name: crate::predicate::TableNameAndFieldName,
    ) -> Option<ConstantValue> {
        if self
            .layout
            .schema
            .get_field_type(field_name.field_name.clone())
            == Some(TableFieldType::INTEGER)
        {
            let int_value = self.get_integer(field_name)?;
            return Some(ConstantValue::Number(int_value));
        } else {
            let string_value = self.get_string(field_name)?;
            return Some(ConstantValue::String(string_value));
        }
    }

    fn has_field(&self, field_name: crate::predicate::TableNameAndFieldName) -> bool {
        self.layout.schema.has_field(field_name.field_name.clone())
    }

    fn delete(&mut self) {
        panic!("not implemented");
    }

    fn insert(&mut self) {
        panic!("not implemented");
    }

    fn get_record_id(&self) -> crate::table_scan_v2::RecordID {
        panic!("not implemented");
    }

    fn move_to_record_id(&mut self, record_id: crate::table_scan_v2::RecordID) {
        panic!("not implemented");
    }

    fn set_integer(&mut self, field_name: String, value: i32) {
        panic!("not implemented");
    }

    fn set_string(&mut self, field_name: String, value: String) {
        panic!("not implemented");
    }

    fn set_value(&mut self, field_name: String, value: ConstantValue) {
        panic!("not implemented");
    }
}
