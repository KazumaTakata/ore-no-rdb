use std::sync::{Arc, Mutex};

use crate::b_tree_directory::BTreeDirectory;
use crate::b_tree_leaf::{BTreeLeaf, DirectoryEntry};
use crate::b_tree_page::BTreePage;
use crate::block::BlockId;
use crate::predicate::{Constant, ConstantValue};
use crate::record_page::{Layout, TableFieldType, TableSchema};
use crate::table_scan_v2::RecordID;
use crate::transaction_v2::TransactionV2;

struct BTreeIndex {
    leaf_table_name: String,
    transaction: Arc<Mutex<TransactionV2>>,
    directory_layout: Layout,
    leaf_layout: Layout,
    leaf: Option<BTreeLeaf>,
    root_block_id: BlockId,
}

impl BTreeIndex {
    fn new(
        transaction: Arc<Mutex<TransactionV2>>,
        index_name: String,
        leaf_layout: Layout,
    ) -> BTreeIndex {
        // leafを初期化する
        let leaf_table_name = format!("{}_leaf", index_name);

        let leaf_table_size = transaction
            .lock()
            .unwrap()
            .get_size(leaf_table_name.clone());

        if leaf_table_size == 0 {
            let block_id = transaction.lock().unwrap().append(&leaf_table_name);
            let node = BTreePage::new(transaction.clone(), block_id.clone(), leaf_layout.clone());
            node.format(block_id.clone(), -1);
        }

        // directoryを初期化する
        let mut directory_schema = TableSchema::new();
        directory_schema.add("block".to_string(), leaf_layout.schema.clone());
        directory_schema.add("dataval".to_string(), leaf_layout.schema.clone());

        let directory_table_name = format!("{}_directory", index_name);
        let directory_layout = Layout::new(directory_schema.clone());
        let root_block_id = BlockId::new(directory_table_name.clone(), 0);

        if transaction
            .lock()
            .unwrap()
            .get_size(directory_table_name.clone())
            == 0
        {
            transaction.lock().unwrap().append(&directory_table_name);
            let mut node = BTreePage::new(
                transaction.clone(),
                root_block_id.clone(),
                directory_layout.clone(),
            );
            node.format(root_block_id.clone(), 0);
            let field_type = directory_schema
                .get_field_type("dataval".to_string())
                .unwrap();

            let min_value = match field_type {
                TableFieldType::INTEGER => Constant::new(ConstantValue::Number(i32::MIN)),
                TableFieldType::VARCHAR => Constant::new(ConstantValue::String(String::new())),
            };

            let directory_entry = DirectoryEntry {
                block_number: 0,
                data_value: min_value,
            };
            node.insert_directory(0, directory_entry);
            node.close();
        }

        BTreeIndex {
            leaf_table_name,
            transaction,
            directory_layout,
            leaf_layout,
            leaf: None,
            root_block_id,
        }
    }

    fn before_first(&mut self, search_key: Constant) {
        self.close();
        let mut btree_root = BTreeDirectory::new(
            self.transaction.clone(),
            self.directory_layout.clone(),
            self.root_block_id.clone(),
        );

        let block_number = btree_root.search(search_key.clone());
        btree_root.close();

        let leaf_block_id = BlockId::new(self.leaf_table_name.clone(), block_number);

        self.leaf = Some(BTreeLeaf::new(
            self.transaction.clone(),
            self.leaf_layout.clone(),
            search_key,
            leaf_block_id,
        ));
    }

    pub fn next(&mut self) -> bool {
        if let Some(leaf) = &mut self.leaf {
            return leaf.next();
        }
        false
    }

    fn close(&mut self) {
        if let Some(leaf) = &mut self.leaf {
            leaf.close();
        }
        self.leaf = None;
    }

    pub fn get_data_record_id(&mut self) -> Option<RecordID> {
        if let Some(leaf) = &mut self.leaf {
            return Some(leaf.get_data_record_id());
        }
        None
    }

    pub fn insert(&mut self, data_value: Constant, data_record_id: RecordID) {
        self.before_first(data_value.clone());
        let optional_directory_entry = self.leaf.as_mut().unwrap().insert(data_record_id);
        self.leaf.as_mut().unwrap().close();
        let Some(directory_entry) = optional_directory_entry else {
            return;
        };

        let mut btree_root = BTreeDirectory::new(
            self.transaction.clone(),
            self.directory_layout.clone(),
            self.root_block_id.clone(),
        );

        let directory_entry_2 = btree_root.insert(directory_entry);

        if let Some(entry) = directory_entry_2 {
            btree_root.make_new_root(entry);
        }
        btree_root.close();
    }

    pub fn delete(&mut self, data_value: Constant, data_record_id: RecordID) {
        self.before_first(data_value.clone());
        if let Some(leaf) = &mut self.leaf {
            leaf.delete(data_record_id);
            leaf.close();
        }
    }

    pub fn search_cost(number_of_blocks: i32, record_per_block: i32) -> i32 {
        1 + (f64::log(number_of_blocks as f64, record_per_block as f64)) as i32
    }
}
