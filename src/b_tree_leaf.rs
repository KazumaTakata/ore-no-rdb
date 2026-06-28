use std::cell::RefCell;
use std::rc::Rc;

use crate::b_tree_page::BTreePage;
use crate::storage::block::BlockId;
use crate::predicate::Constant;
use crate::record_page::Layout;
use crate::table_scan_v2::RecordID;
use crate::transaction_v2::TransactionV2;

pub struct DirectoryEntry {
    pub block_number: u64,
    pub data_value: Constant,
}

pub struct BTreeLeaf {
    transaction: Rc<RefCell<TransactionV2>>,
    layout: Layout,
    search_key: Constant,
    contents: BTreePage,
    current_slot: i32,
}

impl BTreeLeaf {
    pub fn new(
        transaction: Rc<RefCell<TransactionV2>>,
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

    pub fn close(&mut self) {
        self.contents.close();
    }

    pub fn next(&mut self) -> bool {
        self.current_slot += 1;

        let number_of_records = self.contents.get_number_of_records();

        if self.current_slot >= number_of_records {
            self.try_overflow()
        } else if self
            .contents
            .get_data_value(self.current_slot as usize)
            .equals(self.search_key.value.clone())
        {
            true
        } else {
            self.try_overflow()
        }
    }

    pub fn get_data_record_id(&self) -> RecordID {
        self.contents.get_data_record_id(self.current_slot as usize)
    }

    pub fn delete(&mut self, record_id: RecordID) {
        while self.next() {
            let data_record_id = self.get_data_record_id();
            if data_record_id.equals(&record_id) {
                self.contents.delete(self.current_slot as usize);
                return;
            }
        }
    }

    fn try_overflow(&mut self) -> bool {
        // TODO: Handle overflow by checking if there is a directory entry for the next block and
        // moving to it if necessary
        true
    }

    pub fn insert(&mut self, record_id: RecordID) -> Option<DirectoryEntry> {
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
            self.contents.insert_leaf(
                self.current_slot as usize,
                self.search_key.clone(),
                record_id,
            );
            let directory_entry = DirectoryEntry {
                block_number: new_block_id.get_block_number(),
                data_value: first_value,
            };
            return Some(directory_entry);
        }

        self.current_slot += 1;
        self.contents.insert_leaf(
            self.current_slot as usize,
            self.search_key.clone(),
            record_id,
        );

        if !self.contents.is_full() {
            return None;
        }

        // TODO 最後のレコードと最初のレコードが同じ値の場合
        // let first_value = self.contents.get_data_value(0);
        // ...

        let mut split_position = self.contents.get_number_of_records() / 2;
        let mut split_key = self.contents.get_data_value(split_position as usize);

        if split_key.equals(self.search_key.value.clone()) {
            while self
                .contents
                .get_data_value(split_position as usize)
                .equals(split_key.value.clone())
            {
                split_position += 1;
            }

            split_key = self.contents.get_data_value(split_position as usize);
        } else {
            while self
                .contents
                .get_data_value((split_position - 1) as usize)
                .equals(split_key.value.clone())
            {
                split_position -= 1;
            }
        }

        let new_block_id = self.contents.split(split_position as usize, -1);
        return Some(DirectoryEntry {
            block_number: new_block_id.get_block_number(),
            data_value: split_key,
        });
    }
}
