use std::sync::{Arc, Mutex};

use crate::b_tree_page::BTreePage;
use crate::predicate::Constant;
use crate::record_page::Layout;
use crate::transaction_v2::TransactionV2;

struct BTreeLeaf {
    transaction: Arc<Mutex<TransactionV2>>,
    layout: Layout,
    search_key: Constant,
    contents: BTreePage
    current_slot: i32,
    file_name: String,
}
