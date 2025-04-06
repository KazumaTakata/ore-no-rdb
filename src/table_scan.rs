use crate::{
    block,
    buffer_manager::BufferList,
    file_manager::{self, FileManager},
    record_page,
    transaction::{self, Transaction},
};

struct RecordID {
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

struct TableScan {
    file_name: String,
    record_page: record_page::RecordPage,
    current_slot: i32,
}

impl TableScan {
    pub fn new(
        file_name: String,
        transaction: &mut Transaction,
        file_manager: &mut FileManager,
        layout: record_page::Layout,
        buffer_list: &mut BufferList,
    ) -> Self {
        let block_size = transaction.get_size(file_manager, file_name.clone());

        if block_size == 0 {
            let block_id = transaction.append(file_manager, &file_name);
            let record_page = record_page::RecordPage::new(layout, block_id);
            record_page.format(transaction, buffer_list, &file_manager);
            return TableScan {
                file_name,
                record_page,
                current_slot: -1,
            };
        } else {
            let block_id = block::BlockId::new(file_name.clone(), 0);
            let record_page = record_page::RecordPage::new(layout, block_id);
            return TableScan {
                file_name,
                record_page,
                current_slot: -1,
            };
        }
    }

    pub fn move_to_block(
        &mut self,
        block_number: u64,
        layout: record_page::Layout,
    ) -> record_page::RecordPage {
        let block_id = block::BlockId::new(self.file_name.clone(), block_number);
        let record_page = record_page::RecordPage::new(layout, block_id);
        self.current_slot = -1;

        return record_page;
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
