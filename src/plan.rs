use crate::{
    buffer_manager::BufferList,
    file_manager::{self, FileManager},
    predicate::Predicate,
    record_page::{Layout, TableSchema},
    scan::{Scan, SelectScan},
    table_scan::TableScan,
    transaction::{self, Transaction},
};

trait Plan {
    fn open(
        &self,
        transaction: &mut Transaction,
        file_manager: &mut FileManager,
        buffer_list: &mut BufferList,
    ) -> Box<dyn Scan>;
    fn get_schema(&self) -> &TableSchema;
}

struct TablePlan {
    // Fields for the plan
    table_name: String,
    layout: Layout,
}

impl TablePlan {
    pub fn new(table_name: String, layout: Layout) -> Self {
        TablePlan { table_name, layout }
    }
}

impl Plan for TablePlan {
    fn open(
        &self,
        transaction: &mut Transaction,
        file_manager: &mut FileManager,
        buffer_list: &mut BufferList,
    ) -> Box<dyn Scan> {
        return Box::new(TableScan::new(
            self.table_name.clone(),
            transaction,
            file_manager,
            self.layout.clone(),
            buffer_list,
        ));
    }

    fn get_schema(&self) -> &TableSchema {
        &self.layout.schema
    }
}

struct SelectPlan {
    // Fields for the plan
    table_plan: TablePlan,
    predicate: Predicate,
}

impl SelectPlan {
    pub fn new(table_plan: TablePlan, predicate: Predicate) -> Self {
        SelectPlan {
            table_plan,
            predicate,
        }
    }
}

impl Plan for SelectPlan {
    fn open(
        &self,
        transaction: &mut Transaction,
        file_manager: &mut FileManager,
        buffer_list: &mut BufferList,
    ) -> Box<dyn Scan> {
        let scan = self.table_plan.open(transaction, file_manager, buffer_list);
        return Box::new(SelectScan::new(scan, self.predicate.clone()));
    }

    fn get_schema(&self) -> &TableSchema {
        self.table_plan.get_schema()
    }
}
