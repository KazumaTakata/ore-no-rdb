use std::{cell::RefCell, rc::Rc, sync::Mutex};

use crate::{
    error::ValueNotFound,
    plan_v2::PlanV2,
    predicate::TableNameAndFieldName,
    record_page::{Layout, TableSchema},
    scan_v2::ScanV2,
    table_scan_v2::TableScan,
    transaction_v2::TransactionV2,
};

static INDEX_COUNTER: Mutex<i32> = Mutex::new(0);

pub struct TempTable {
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

pub struct MaterializePlan {
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
}

impl PlanV2 for MaterializePlan {
    fn open(&mut self) -> Result<Box<dyn ScanV2>, ValueNotFound> {
        let schema = self.src_plan.get_schema().clone();

        let mut temp_table = TempTable::new(self.transaction.clone(), schema.clone());

        let mut src = self.src_plan.open()?;

        let mut dest = temp_table.open();

        while src.next()? {
            dest.insert();
            for field in schema.fields.iter() {
                let value = src.get_value(TableNameAndFieldName {
                    table_name: None,
                    field_name: field.clone(),
                });
                if let Some(value) = value {
                    dest.set_value(field.clone(), value);
                } else {
                    return Err(ValueNotFound::new(field.clone(), None));
                }
            }
        }

        src.close();
        dest.move_to_before_first();
        Ok(dest)
    }

    fn blocks_accessed(&self) -> u32 {
        let layout = Layout::new(self.src_plan.get_schema().clone());
        let rpb = self.transaction.borrow().get_block_size() as i32 / layout.get_slot_size();
        return self.src_plan.blocks_accessed() as u32 / rpb as u32;
    }

    fn records_output(&self) -> u32 {
        self.src_plan.records_output()
    }

    fn get_distinct_value(&self, field_name: String) -> u32 {
        self.src_plan.get_distinct_value(field_name)
    }

    fn get_schema(&self) -> &TableSchema {
        self.src_plan.get_schema()
    }
}
