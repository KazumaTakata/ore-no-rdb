use crate::{
    block,
    buffer_manager::{self, BufferList},
    file_manager::{self, FileManager},
    record_page,
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

    pub fn move_to_block(&mut self, block_number: u64, layout: record_page::Layout) {
        let block_id = block::BlockId::new(self.file_name.clone(), block_number);
        self.record_page = record_page::RecordPage::new(layout, block_id);
    }

    pub fn move_to_before_first(&mut self, layout: record_page::Layout) {
        self.move_to_block(0, layout)
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

    pub fn get_integer(
        &mut self,
        transaction: &mut Transaction,
        buffer_list: &mut BufferList,
    ) -> Option<i32> {
        self.record_page.get_integer(
            self.file_name.clone(),
            self.current_slot,
            transaction,
            buffer_list,
        )
    }

    pub fn get_string(
        &mut self,
        transaction: &mut Transaction,
        buffer_list: &mut BufferList,
    ) -> Option<String> {
        self.record_page.get_string(
            self.file_name.clone(),
            self.current_slot,
            transaction,
            buffer_list,
        )
    }

    pub fn set_integer(
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

    pub fn set_string(
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

    pub fn insert(
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

    pub fn get_record_id(&self) -> RecordID {
        return RecordID::new(
            self.record_page.get_block_id().get_block_number(),
            self.current_slot,
        );
    }

    pub fn move_to_record_id(&mut self, layout: record_page::Layout, record_id: RecordID) {
        let block_id = block::BlockId::new(self.file_name.clone(), record_id.get_block_number());
        self.record_page = record_page::RecordPage::new(layout, block_id);
        self.current_slot = record_id.get_slot_number();
    }

    fn at_last_block(&self, transaction: &Transaction, file_manager: &mut FileManager) -> bool {
        let file_size = transaction.get_size(file_manager, self.file_name.clone());
        let current_block = self.record_page.get_block_id().get_block_number();
        current_block as usize == file_size - 1
    }

    pub fn close(
        &mut self,
        transaction: &mut Transaction,
        buffer_list: &mut BufferList,
        buffer_manager: &mut buffer_manager::BufferManager,
    ) {
        transaction.unpin(buffer_list, buffer_manager, self.record_page.get_block_id());
    }
}
