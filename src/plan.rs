use std::cmp::min;

use crate::{
    buffer_manager::{self, BufferList},
    file_manager::{self, FileManager},
    predicate::Predicate,
    record_page::{self, Layout, TableSchema},
    scan::{Scan, SelectScan},
    stat_manager::{StatInfo, StatManager},
    table_scan::{ProjectScan, TableScan},
    transaction::{self, Transaction},
};

pub trait Plan {
    fn open(
        &self,
        transaction: &mut Transaction,
        file_manager: &mut FileManager,
        buffer_list: &mut BufferList,
    ) -> Box<dyn Scan>;
    fn get_schema(&self) -> &TableSchema;

    fn blocks_accessed(&self) -> u32;

    fn records_output(&self, transaction: &mut Transaction, buffer_list: &mut BufferList) -> u32;

    fn get_distinct_value(
        &self,
        field_name: String,
        transaction: &mut Transaction,
        buffer_list: &mut BufferList,
    ) -> u32;
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

    fn records_output(&self, transaction: &mut Transaction, buffer_list: &mut BufferList) -> u32 {
        self.stat_info.get_num_records()
    }

    fn get_distinct_value(
        &self,
        field_name: String,
        transaction: &mut Transaction,
        buffer_list: &mut BufferList,
    ) -> u32 {
        self.stat_info.distinct_value(field_name)
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

    fn get_distinct_value(
        &self,
        field_name: String,
        transaction: &mut Transaction,
        buffer_list: &mut BufferList,
    ) -> u32 {
        if self
            .predicate
            .equates_with_constant(transaction, buffer_list, field_name.clone())
            .is_some()
        {
            return 1;
        } else {
            let field_name2 =
                self.predicate
                    .equate_with_field(transaction, buffer_list, field_name.clone());

            if (field_name2.is_some()) {
                return min(
                    self.table_plan.get_distinct_value(
                        field_name2.clone().unwrap(),
                        transaction,
                        buffer_list,
                    ),
                    self.table_plan.get_distinct_value(
                        field_name.clone(),
                        transaction,
                        buffer_list,
                    ),
                );
            } else {
                return self
                    .table_plan
                    .get_distinct_value(field_name, transaction, buffer_list);
            }
        }
    }

    fn records_output(&self, transaction: &mut Transaction, buffer_list: &mut BufferList) -> u32 {
        self.table_plan.records_output(transaction, buffer_list)
            / self
                .predicate
                .reduction_factor(&self.table_plan, transaction, buffer_list)
    }
}

struct ProjectPlan {
    // Fields for the plan
    table_plan: Box<dyn Plan>,
    schema: TableSchema,
}

impl ProjectPlan {
    pub fn new(table_plan: Box<dyn Plan>, field_list: Vec<String>) -> Self {
        let mut schema = TableSchema::new();

        for field in field_list.iter() {
            schema.add(field.clone(), table_plan.get_schema().clone());
        }

        ProjectPlan { table_plan, schema }
    }
}

impl Plan for ProjectPlan {
    fn open(
        &self,
        transaction: &mut Transaction,
        file_manager: &mut FileManager,
        buffer_list: &mut BufferList,
    ) -> Box<dyn Scan> {
        let scan = self.table_plan.open(transaction, file_manager, buffer_list);
        let field_names = self.schema.fields().clone(); // Assuming `fields()` returns `Vec<String>`
        return Box::new(ProjectScan::new(scan, field_names));
    }

    fn get_schema(&self) -> &TableSchema {
        &self.schema
    }

    fn blocks_accessed(&self) -> u32 {
        self.table_plan.blocks_accessed()
    }

    fn get_distinct_value(
        &self,
        field_name: String,
        transaction: &mut Transaction,
        buffer_list: &mut BufferList,
    ) -> u32 {
        self.table_plan
            .get_distinct_value(field_name, transaction, buffer_list)
    }

    fn records_output(&self, transaction: &mut Transaction, buffer_list: &mut BufferList) -> u32 {
        self.table_plan.records_output(transaction, buffer_list)
    }
}
