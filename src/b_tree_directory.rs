use std::cell::RefCell;
use std::rc::Rc;

use crate::b_tree_leaf::DirectoryEntry;
use crate::b_tree_page::BTreePage;
use crate::storage::block::BlockId;
use crate::predicate::Constant;
use crate::record_page::Layout;
use crate::transaction_v2::TransactionV2;

pub struct BTreeDirectory {
    transaction: Rc<RefCell<TransactionV2>>,
    layout: Layout,
    contents: BTreePage,
    file_name: String,
}

impl BTreeDirectory {
    pub fn new(
        transaction: Rc<RefCell<TransactionV2>>,
        layout: Layout,
        block_id: BlockId,
    ) -> BTreeDirectory {
        let contents = BTreePage::new(transaction.clone(), block_id.clone(), layout.clone());
        let file_name = block_id.get_file_name();

        BTreeDirectory {
            transaction,
            layout,
            contents,
            file_name: file_name.clone(),
        }
    }

    pub fn close(&mut self) {
        self.contents.close();
    }

    fn find_child_block(&mut self, search_key: Constant) -> BlockId {
        let mut slot = self.contents.find_slot_before(search_key.clone());

        if self
            .contents
            .get_data_value(slot as usize)
            .equals(search_key.value.clone())
        {
            slot += 1;
        }

        let block_number = self.contents.get_child_number(slot as usize);
        return BlockId::new(self.file_name.clone(), block_number as u64);
    }

    pub fn search(&mut self, search_key: Constant) -> u64 {
        let mut child_block = self.find_child_block(search_key.clone());
        while self.contents.get_flag() > 0 {
            self.contents.close();
            self.contents = BTreePage::new(
                self.transaction.clone(),
                child_block.clone(),
                self.layout.clone(),
            );
            child_block = self.find_child_block(search_key.clone());
        }
        return child_block.get_block_number();
    }

    pub fn make_new_root(&mut self, directory_entry: DirectoryEntry) {
        let first_value = self.contents.get_data_value(0);
        let level = self.contents.get_flag();
        // transfer all the data from the old root to the new root
        let new_block_id = self.contents.split(0, level);
        let old_root_block_id = DirectoryEntry {
            block_number: new_block_id.get_block_number(),
            data_value: first_value,
        };

        self.insert_entry(old_root_block_id);
        self.insert_entry(directory_entry);
        self.contents.set_flag(level + 1)
    }

    pub fn insert(&mut self, directory_entry: DirectoryEntry) -> Option<DirectoryEntry> {
        if self.contents.get_flag() == 0 {
            return self.insert_entry(directory_entry);
        }

        let child_block = self.find_child_block(directory_entry.data_value.clone());
        let mut child_directory = BTreeDirectory::new(
            self.transaction.clone(),
            self.layout.clone(),
            child_block.clone(),
        );
        let new_entry = child_directory.insert(directory_entry);
        child_directory.close();
        if let Some(entry) = new_entry {
            return self.insert_entry(entry);
        }
        return None;
    }

    pub fn insert_entry(&mut self, directory_entry: DirectoryEntry) -> Option<DirectoryEntry> {
        let new_slot = 1 + self
            .contents
            .find_slot_before(directory_entry.data_value.clone());

        self.contents
            .insert_directory(new_slot as usize, directory_entry);
        if !self.contents.is_full() {
            return None;
        }

        let level = self.contents.get_flag();
        let split_position = self.contents.get_number_of_records() / 2;
        let split_value = self.contents.get_data_value(split_position as usize);
        let new_block_id = self.contents.split(split_position as usize, level);
        return Some(DirectoryEntry {
            block_number: new_block_id.get_block_number(),
            data_value: split_value,
        });
    }
}
