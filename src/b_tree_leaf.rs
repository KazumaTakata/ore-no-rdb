use std::sync::{Arc, Mutex};

use crate::b_tree_page::BTreePage;
use crate::block::BlockId;
use crate::predicate::Constant;
use crate::record_page::Layout;
use crate::table_scan_v2::RecordID;
use crate::transaction_v2::TransactionV2;

pub struct DirectoryEntry {
    pub block_number: u64,
    pub data_value: Constant,
}

struct BTreeLeaf {
    transaction: Arc<Mutex<TransactionV2>>,
    layout: Layout,
    search_key: Constant,
    contents: BTreePage,
    current_slot: usize,
}

impl BTreeLeaf {
    fn new(
        transaction: Arc<Mutex<TransactionV2>>,
        layout: Layout,
        search_key: Constant,
        block_id: BlockId,
    ) -> BTreeLeaf {
        let contents = BTreePage::new(transaction.clone(), block_id, layout.clone());

        let current_slot = contents.find_slot_before(search_key.clone());

        BTreeLeaf {
            transaction,
            layout,
            search_key,
            contents,
            current_slot,
        }
    }

    fn close(&mut self) {
        self.contents.close();
    }

    fn next(&mut self) -> bool {
        self.current_slot += 1;

        if self.current_slot >= self.contents.get_number_of_records() {
            return self.try_overflow();
        } else if self
            .contents
            .get_data_value(self.current_slot)
            .equals(self.search_key.value.clone())
        {
            return true;
        } else {
            return self.try_overflow();
        }
    }

    pub fn get_data_record_id(&self) -> RecordID {
        return self.contents.get_data_record_id(self.current_slot);
    }

    pub fn delete(&mut self, record_id: RecordID) {
        while self.next() {
            let data_record_id = self.get_data_record_id();
            if data_record_id.equals(&record_id) {
                self.contents.delete(self.current_slot);
                return;
            }
        }
    }

    fn try_overflow(&mut self) -> bool {
        return true;
    }

    fn insert(&mut self, record_id: RecordID) -> Option<DirectoryEntry> {
        if self.contents.get_flag() >= 0
            && self
                .contents
                .get_data_value(0)
                .compare_to(self.search_key.value.clone())
                == std::cmp::Ordering::Greater
        {
            let first_value = self.contents.get_data_value(0);
            let current_flag = self.contents.get_flag();
            let new_block_id = self.contents.split(0, current_flag);
            self.current_slot = 0;
            self.contents.set_flag(-1);
            self.contents
                .insert_leaf(self.current_slot, self.search_key.clone(), record_id);
            let directory_entry = DirectoryEntry {
                block_number: new_block_id.get_block_number(),
                data_value: first_value,
            };
            return Some(directory_entry);
        }

        self.current_slot += 1;
        self.contents
            .insert_leaf(self.current_slot, self.search_key.clone(), record_id);

        if !self.contents.is_full() {
            return None;
        }

        // TODO: Handle overflow by splitting the leaf and creating a new directory entry
        return None;
    }
}
