use std::{cell::RefCell, rc::Rc, sync::Mutex};

use crate::{
    plan_v2::PlanV2,
    record_page::{Layout, TableSchema},
    scan_v2::ScanV2,
    table_scan_v2::TableScan,
    transaction_v2::TransactionV2,
};

static INDEX_COUNTER: Mutex<i32> = Mutex::new(0);

struct TempTable {
    transaction: Rc<RefCell<TransactionV2>>,
    table_name: String,
    layout: Layout,
}

impl TempTable {
    pub fn new(transaction: Rc<RefCell<TransactionV2>>, schema: TableSchema) -> Self {
        let table_name = TempTable::next_table_name();
        let layout = Layout::new(schema.clone());
        TempTable {
            transaction,
            table_name,
            layout,
        }
    }

    pub fn get_layout(&self) -> &Layout {
        &self.layout
    }

    pub fn get_table_name(&self) -> &String {
        &self.table_name
    }

    pub fn open(&mut self) -> Box<dyn ScanV2> {
        Box::new(TableScan::new(
            self.table_name.clone(),
            self.transaction.clone(),
            self.layout.clone(),
        ))
    }

    fn next_table_name() -> String {
        let mut index_counter = INDEX_COUNTER.lock().unwrap();
        *index_counter += 1;
        format!("{}_{}", "temp", index_counter)
    }
}

struct MaterializePlan {
    transaction: Rc<RefCell<TransactionV2>>,
    src_plan: Box<dyn PlanV2>,
}

impl MaterializePlan {
    pub fn new(transaction: Rc<RefCell<TransactionV2>>, src_plan: Box<dyn PlanV2>) -> Self {
        MaterializePlan {
            transaction,
            src_plan,
        }
    }

    pub fn open(&mut self) -> Box<dyn ScanV2> {
        let schema = self.src_plan.get_schema();
        let mut temp_table = TempTable::new(self.transaction.clone(), schema.clone());

        let mut src = self.src_plan.open();

        let mut dest = temp_table.open();

        while src.next() {
            dest.insert();
            for field in schema.fields.iter() {
                let value = src.get_value(field.clone());
                dest.set_value(field.clone(), value);
            }
        }

        src.close();
        dest.move_to_before_first();
        dest
    }

    pub fn blocks_accessed(&self) -> i32 {
        let layout = Layout::new(self.src_plan.get_schema().clone());
        let rpb = self.transaction.borrow().get_block_size() as i32 / layout.get_slot_size();
        return self.src_plan.blocks_accessed() as i32 / rpb;
    }

    pub fn records_output(&self) -> u32 {
        self.src_plan.records_output()
    }

    pub fn get_distinct_value(&self, field_name: String) -> u32 {
        self.src_plan.get_distinct_value(field_name)
    }

    pub fn get_schema(&self) -> &TableSchema {
        self.src_plan.get_schema()
    }
}
