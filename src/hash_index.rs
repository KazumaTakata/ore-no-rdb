use core::hash;
use std::{
    cell::RefCell,
    hash::{DefaultHasher, Hash, Hasher},
    iter::Scan,
    rc::Rc,
};

use rand::seq::index;

use crate::{
    error::ValueNotFound,
    index_manager::IndexInfo,
    plan_v2::PlanV2,
    predicate::{Constant, ConstantValue, TableNameAndFieldName},
    record_page::{Layout, TableSchema},
    scan_v2::ScanV2,
    table_scan_v2::{RecordID, TableScan},
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

    fn next(&mut self) -> Result<bool, ValueNotFound> {
        if let Some(scan) = &mut self.table_scan {
            while scan.next()? {
                let data_value =
                    scan.get_value(TableNameAndFieldName::new(None, "data_value".to_string()));

                if let Some(inner_value) = data_value {
                    match self.search_key {
                        Some(ref key) if key.equals(inner_value) => {
                            return Ok(true);
                        }
                        _ => continue,
                    }
                } else {
                    return Err(ValueNotFound::new("data_value".to_string(), None));
                }
            }

            return Ok(false);
        } else {
            panic!("Table scan is not initialized. Call before_first first.");
        }
    }

    fn get_data_record_id(&mut self) -> Result<Option<RecordID>, ValueNotFound> {
        if let Some(scan) = &mut self.table_scan {
            if scan.next()? {
                let value = scan.get_integer(TableNameAndFieldName::new(None, "block".to_string()));
                let id = scan.get_integer(TableNameAndFieldName::new(None, "id".to_string()));

                if let (Some(block), Some(id)) = (value, id) {
                    return Ok(Some(RecordID::new(block as u64, id)));
                }
            }
        }
        Ok(None)
    }

    pub fn insert(&mut self, value: Constant, record_id: RecordID) {
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

    pub fn delete(&mut self, value: Constant, record_id: RecordID) -> Result<bool, ValueNotFound> {
        self.before_first(value.clone());
        if let Some(scan) = &mut self.table_scan {
            while scan.next()? {
                let data_record_id = scan.get_record_id();
                if data_record_id.equals(&record_id) {
                    scan.delete();
                    return Ok(true);
                }
            }

            return Ok(false);
        } else {
            panic!("Table scan is not initialized. Call before_first first.");
        }
    }

    pub fn close(&mut self) {
        if let Some(scan) = &mut self.table_scan {
            scan.close();
        }
        // Logic to close the index
    }

    pub fn get_search_cost(number_of_blocks: u32) -> u32 {
        return number_of_blocks / 10;
    }
}

struct IndexSelectPlan {
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
}

struct IndexSelectScan {
    table_scan: Box<dyn ScanV2>,
    index: Rc<RefCell<HashIndex>>,
    key: Constant,
}

impl IndexSelectScan {
    pub fn new(table_scan: Box<dyn ScanV2>, index: Rc<RefCell<HashIndex>>, key: Constant) -> Self {
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
        let has_next = self.index.borrow_mut().next()?;

        if has_next {
            let record_id = self.index.borrow_mut().get_data_record_id()?;

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
