use std::{
    cell::RefCell,
    hash::{DefaultHasher, Hash, Hasher},
    rc::Rc,
};

use crate::{
    b_tree_index::BTreeIndex,
    error::ValueNotFound,
    index_manager::IndexInfo,
    plan_v2::{PlanTreeNodeForDebug, PlanV2},
    predicate::{Constant, ConstantValue, TableNameAndFieldName},
    record_page::{Layout, TableSchema},
    scan_v2::ScanV2,
    table_scan_v2::{RecordID, TableScan},
    tx::transaction_v2::TransactionV2,
};

pub struct IndexSelectPlan {
    plan: Box<dyn PlanV2>,
    index_info: IndexInfo,
    key: Constant,
}

impl IndexSelectPlan {
    pub fn new(plan: Box<dyn PlanV2>, index_info: IndexInfo, key: Constant) -> Self {
        IndexSelectPlan {
            plan,
            index_info,
            key,
        }
    }
}

impl PlanV2 for IndexSelectPlan {
    fn open(&mut self) -> Result<Box<dyn ScanV2>, ValueNotFound> {
        let table_scan = self.plan.open().unwrap();
        let index = Rc::new(RefCell::new(self.index_info.open()));
        return Ok(Box::new(IndexSelectScan::new(
            table_scan,
            index,
            self.key.clone(),
        )));
    }

    fn blocks_accessed(&self) -> u32 {
        return self.index_info.blocks_accessed();
    }

    fn records_output(&self) -> u32 {
        self.index_info.records_output()
    }

    fn get_distinct_value(&self, field_name: String) -> u32 {
        self.index_info.distinct_values(&field_name)
    }

    fn get_schema(&self) -> &TableSchema {
        self.plan.get_schema()
    }

    fn get_child_plans(&self) -> PlanTreeNodeForDebug {
        PlanTreeNodeForDebug {
            current_node_type: "IndexSelectPlan".to_string(),
            child_nodes: vec![self.plan.get_child_plans()],
        }
    }
}

pub struct IndexSelectScan {
    table_scan: Box<dyn ScanV2>,
    index: Rc<RefCell<BTreeIndex>>,
    key: Constant,
}

impl IndexSelectScan {
    pub fn new(table_scan: Box<dyn ScanV2>, index: Rc<RefCell<BTreeIndex>>, key: Constant) -> Self {
        index.borrow_mut().before_first(key.clone());
        let index_select_scan = IndexSelectScan {
            table_scan,
            index: index.clone(),
            key,
        };
        index_select_scan
    }
}

impl ScanV2 for IndexSelectScan {
    fn move_to_before_first(&mut self) -> Result<(), ValueNotFound> {
        self.index.borrow_mut().before_first(self.key.clone());
        return Ok(());
    }

    fn next(&mut self) -> Result<bool, ValueNotFound> {
        let has_next = self.index.borrow_mut().next();

        if has_next {
            let record_id = self.index.borrow_mut().get_data_record_id();

            if let Some(rid) = record_id {
                self.table_scan.move_to_record_id(rid);
                return Ok(true);
            } else {
                return Ok(false);
            }
        }
        return Ok(has_next);
    }

    fn get_integer(&mut self, field_name: TableNameAndFieldName) -> Option<i32> {
        self.table_scan.get_integer(field_name)
    }

    fn get_string(&mut self, field_name: TableNameAndFieldName) -> Option<String> {
        self.table_scan.get_string(field_name)
    }

    fn get_value(&mut self, field_name: TableNameAndFieldName) -> Option<ConstantValue> {
        self.table_scan.get_value(field_name)
    }

    fn has_field(&self, field_name: TableNameAndFieldName) -> bool {
        self.table_scan.has_field(field_name)
    }
    fn close(&mut self) {
        self.index.borrow_mut().close();
        self.table_scan.close();
    }

    fn delete(&mut self) {
        panic!("IndexSelectScan does not support delete operation");
    }

    fn get_record_id(&self) -> RecordID {
        panic!("IndexSelectScan does not support get_record_id operation");
    }

    fn insert(&mut self) {
        panic!("IndexSelectScan does not support insert operation");
    }

    fn move_to_record_id(&mut self, record_id: RecordID) {
        panic!("IndexSelectScan does not support move_to_record_id operation");
    }

    fn set_integer(&mut self, field_name: String, value: i32) {
        panic!("IndexSelectScan does not support set_integer operation");
    }

    fn set_string(&mut self, field_name: String, value: String) {
        panic!("IndexSelectScan does not support set_string operation");
    }

    fn set_value(&mut self, field_name: String, value: ConstantValue) {
        panic!("IndexSelectScan does not support set_value operation");
    }
}
