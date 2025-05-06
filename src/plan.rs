use crate::{
    buffer_manager::{self, BufferList},
    file_manager::{self, FileManager},
    predicate::Predicate,
    record_page::{self, Layout, TableSchema},
    scan::{Scan, SelectScan},
    stat_manager::{StatInfo, StatManager},
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

    fn blocks_accessed(&self) -> u32;

    fn records_output(&self) -> u32;

    fn get_distinct_value(&self) -> u32;
}

struct TablePlan {
    // Fields for the plan
    table_name: String,
    layout: Layout,
    stat_info: StatInfo,
}

impl TablePlan {
    pub fn new(
        table_name: String,
        transaction: &mut Transaction,
        file_manager: &mut FileManager,
        layout: record_page::Layout,
        buffer_list: &mut BufferList,
        buffer_manager: &mut buffer_manager::BufferManager,
        stat_manager: &mut StatManager,
    ) -> Self {
        let stat_info = stat_manager.get_table_stats(
            table_name.clone(),
            transaction,
            file_manager,
            layout.clone(),
            buffer_list,
            buffer_manager,
        );
        TablePlan {
            table_name,
            layout,
            stat_info,
        }
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

    fn blocks_accessed(&self) -> u32 {
        self.stat_info.get_num_blocks()
    }

    fn records_output(&self) -> u32 {
        self.stat_info.get_num_records()
    }

    fn get_distinct_value(&self) -> u32 {
        self.stat_info.distinct_value()
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

    fn blocks_accessed(&self) -> u32 {
        self.table_plan.blocks_accessed()
    }

    fn get_distinct_value(&self) -> u32 {
        self.table_plan.get_distinct_value()
    }

    fn records_output(&self) -> u32 {
        self.table_plan.records_output()
    }
}
