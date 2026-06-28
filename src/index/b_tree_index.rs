use std::cell::RefCell;
use std::rc::Rc;

use crate::index::b_tree_directory::BTreeDirectory;
use crate::index::b_tree_leaf::{BTreeLeaf, DirectoryEntry};
use crate::index::b_tree_page::BTreePage;
use crate::storage::block::BlockId;
use crate::query::predicate::{Constant, ConstantValue};
use crate::record::record_page::{Layout, TableFieldType, TableSchema};
use crate::record::table_scan_v2::RecordID;
use crate::tx::transaction_v2::TransactionV2;

pub struct BTreeIndex {
    leaf_table_name: String,
    transaction: Rc<RefCell<TransactionV2>>,
    directory_layout: Layout,
    leaf_layout: Layout,
    leaf: Option<BTreeLeaf>,
    root_block_id: BlockId,
}

impl BTreeIndex {
    pub fn new(
        transaction: Rc<RefCell<TransactionV2>>,
        index_name: String,
        leaf_layout: Layout,
    ) -> BTreeIndex {
        // leafを初期化する
        let leaf_table_name = format!("{}_leaf", index_name);

        let leaf_table_size = transaction.borrow_mut().get_size(leaf_table_name.clone());

        if leaf_table_size == 0 {
            let block_id = transaction.borrow_mut().append(&leaf_table_name);
            let node = BTreePage::new(transaction.clone(), block_id.clone(), leaf_layout.clone());
            node.format(block_id.clone(), -1);
        }

        // directoryを初期化する
        let mut directory_schema = TableSchema::new();
        // directory nodeのレイアウトは以下のようにする
        // - block: 子供のブロック番号を格納する整数フィールド
        // - data_value: インデックスが作成されているフィールドの値
        directory_schema.add("block".to_string(), leaf_layout.schema.clone());
        directory_schema.add("data_value".to_string(), leaf_layout.schema.clone());

        let directory_table_name = format!("{}_directory", index_name);
        let directory_layout = Layout::new(directory_schema.clone());
        let root_block_id = BlockId::new(directory_table_name.clone(), 0);

        if transaction
            .borrow_mut()
            .get_size(directory_table_name.clone())
            == 0
        {
            transaction.borrow_mut().append(&directory_table_name);
            let mut node = BTreePage::new(
                transaction.clone(),
                root_block_id.clone(),
                directory_layout.clone(),
            );
            node.format(root_block_id.clone(), 0);
            let field_type = directory_schema
                .get_field_type("data_value".to_string())
                .expect("directory schema must contain a 'data_value' field");

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

    pub fn before_first(&mut self, search_key: Constant) {
        self.position_at(search_key);
    }

    /// `search_key` を含む leaf までたどり、`self.leaf` を確実に設定して
    /// その可変参照を返す。戻り値があることで、呼び出し側は `self.leaf` を
    /// `unwrap` せずに leaf を操作できる。
    fn position_at(&mut self, search_key: Constant) -> &mut BTreeLeaf {
        self.close();
        let mut btree_root = BTreeDirectory::new(
            self.transaction.clone(),
            self.directory_layout.clone(),
            self.root_block_id.clone(),
        );

        let block_number = btree_root.search(search_key.clone());
        btree_root.close();

        let leaf_block_id = BlockId::new(self.leaf_table_name.clone(), block_number);

        self.leaf.insert(BTreeLeaf::new(
            self.transaction.clone(),
            self.leaf_layout.clone(),
            search_key,
            leaf_block_id,
        ))
    }

    pub fn next(&mut self) -> bool {
        if let Some(leaf) = &mut self.leaf {
            return leaf.next();
        }
        false
    }

    pub fn close(&mut self) {
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
        let leaf = self.position_at(data_value.clone());
        let optional_directory_entry = leaf.insert(data_record_id);
        leaf.close();
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
        let leaf = self.position_at(data_value.clone());
        leaf.delete(data_record_id);
        leaf.close();
    }

    pub fn search_cost(number_of_blocks: i32, record_per_block: i32) -> i32 {
        1 + (f64::log(number_of_blocks as f64, record_per_block as f64)) as i32
    }

    /// このインデックスが構築した B-tree を stdout にツリーとして可視化する。
    ///
    /// directory(内部/最下層) ページをルートから再帰的にたどり、罫線で
    /// 親子関係を示す。各ノードはそのページが持つキーの一覧を表示する。
    pub fn print_tree(&self) {
        self.print_directory(self.root_block_id.clone(), "", "");
    }

    /// directory ページを 1 ノードとして出力し、子を再帰的にたどる。
    /// `prefix` はこの行の行頭、`child_prefix` は子の行頭に付ける罫線。
    fn print_directory(&self, block_id: BlockId, prefix: &str, child_prefix: &str) {
        let page = BTreePage::new(
            self.transaction.clone(),
            block_id.clone(),
            self.directory_layout.clone(),
        );
        let level = page.get_flag();
        let number_of_records = page.get_number_of_records() as usize;

        let keys: Vec<String> = (0..number_of_records)
            .map(|slot| format_constant(&page.get_data_value(slot)))
            .collect();
        println!("{}[{}]", prefix, keys.join(", "));

        for slot in 0..number_of_records {
            let last = slot + 1 == number_of_records;
            let branch = if last { "└─ " } else { "├─ " };
            let next = if last { "   " } else { "│  " };
            let child_block_number = page.get_child_number(slot) as u64;

            if level == 0 {
                let leaf_block_id = BlockId::new(self.leaf_table_name.clone(), child_block_number);
                self.print_leaf(leaf_block_id, &format!("{}{}", child_prefix, branch));
            } else {
                let child_block_id =
                    BlockId::new(block_id.get_file_name().clone(), child_block_number);
                self.print_directory(
                    child_block_id,
                    &format!("{}{}", child_prefix, branch),
                    &format!("{}{}", child_prefix, next),
                );
            }
        }
    }

    fn print_leaf(&self, block_id: BlockId, prefix: &str) {
        let page = BTreePage::new(
            self.transaction.clone(),
            block_id.clone(),
            self.leaf_layout.clone(),
        );
        let number_of_records = page.get_number_of_records() as usize;

        let keys: Vec<String> = (0..number_of_records)
            .map(|slot| format_constant(&page.get_data_value(slot)))
            .collect();
        println!("{}{}", prefix, keys.join(", "));
    }
}

fn format_constant(constant: &Constant) -> String {
    match &constant.value {
        ConstantValue::Number(n) => n.to_string(),
        ConstantValue::String(s) => format!("\"{}\"", s),
        ConstantValue::Null => "NULL".to_string(),
    }
}
