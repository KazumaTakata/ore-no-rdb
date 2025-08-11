use core::hash;
use std::{
    cell::RefCell,
    hash::{DefaultHasher, Hash, Hasher},
    rc::Rc,
};

use rand::seq::index;

use crate::{
    plan_v2::PlanV2,
    predicate::{Constant, ConstantValue},
    record_page::Layout,
    scan_v2::ScanV2,
    table_scan::RecordID,
    table_scan_v2::TableScan,
    transaction_v2::TransactionV2,
};

pub struct HashIndex {
    transaction: Rc<RefCell<TransactionV2>>,
    index_name: String,
    layout: Layout,
    search_key: Option<Constant>,
    number_of_buckets: usize,
    table_scan: Option<Box<dyn ScanV2>>,
}

fn calculate_hash<T: Hash>(t: &T) -> u64 {
    let mut s = DefaultHasher::new();
    t.hash(&mut s);
    s.finish()
}

impl HashIndex {
    pub fn new(
        transaction: Rc<RefCell<TransactionV2>>,
        index_name: String,
        layout: Layout,
    ) -> Self {
        HashIndex {
            transaction,
            index_name,
            layout,
            search_key: None,
            number_of_buckets: 10,
            table_scan: None,
        }
    }

    fn before_first(&mut self, search_key: Constant) {
        self.close();
        self.search_key = Some(search_key);

        let hash_value = match &self.search_key {
            Some(key) => match key.value {
                ConstantValue::String(ref str) => calculate_hash(str),
                ConstantValue::Number(n) => calculate_hash(&n),
                ConstantValue::Null => panic!("Null value cannot be hashed"),
            },
            None => panic!("Search key must be set before calling before_first"),
        };

        let bucket_index = (hash_value as usize) % self.number_of_buckets;

        let table_name = format!("{}{}", self.index_name.clone(), bucket_index);

        let table_scan = TableScan::new(table_name, self.transaction.clone(), self.layout.clone());
        self.table_scan = Some(Box::new(table_scan));
    }

    fn next(&mut self) -> bool {
        if let Some(scan) = &mut self.table_scan {
            while scan.next() {
                let data_value = scan.get_value("data_value".to_string());
                match self.search_key {
                    Some(ref key) if key.equals(data_value) => {
                        return true;
                    }
                    _ => continue,
                }
            }
        } else {
            panic!("Table scan is not initialized. Call before_first first.");
        }
        false
    }

    fn get_data_record_id(&mut self) -> Option<RecordID> {
        if let Some(scan) = &mut self.table_scan {
            if scan.next() {
                let value = scan.get_integer("block".to_string());
                let id = scan.get_integer("id".to_string());

                if let (Some(block), Some(id)) = (value, id) {
                    return Some(RecordID::new(block as u64, id));
                }
            }
        }
        None
    }

    fn insert(&mut self, value: Constant, record_id: RecordID) {
        self.before_first(value.clone());

        if let Some(scan) = &mut self.table_scan {
            scan.insert();
            scan.set_value("data_value".to_string(), value.value.clone());
            scan.set_integer("block".to_string(), record_id.get_block_number() as i32);
            scan.set_integer("id".to_string(), record_id.get_slot_number());
        } else {
            panic!("Table scan is not initialized. Call before_first first.");
        }
    }

    fn delete(&mut self, value: Constant, record_id: RecordID) {
        self.before_first(value.clone());
        if let Some(scan) = &mut self.table_scan {
            while scan.next() {
                let data_record_id = scan.get_record_id();
                if data_record_id.equals(&record_id) {
                    scan.delete();
                    return;
                }
            }
        } else {
            panic!("Table scan is not initialized. Call before_first first.");
        }
    }

    fn close(&mut self) {
        if let Some(scan) = &mut self.table_scan {
            scan.close();
        }
        // Logic to close the index
    }

    pub fn get_search_cost(number_of_blocks: i32) -> i32 {
        return number_of_blocks / 10;
    }
}

struct IndexSelectPlan {
    plan: Box<dyn PlanV2>,
}
