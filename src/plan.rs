use std::cmp::min;

use crate::{
    buffer_manager::{self, BufferList},
    file_manager::{self, FileManager},
    parser::{InsertData, QueryData},
    predicate::Predicate,
    record_page::{self, Layout, TableSchema},
    scan::{ProductScan, Scan, SelectScan},
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

    fn blocks_accessed(&self, transaction: &mut Transaction, buffer_list: &mut BufferList) -> u32;

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

    fn blocks_accessed(&self, transaction: &mut Transaction, buffer_list: &mut BufferList) -> u32 {
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
    table_plan: Box<dyn Plan>,
    predicate: Predicate,
}

impl SelectPlan {
    pub fn new(table_plan: Box<dyn Plan>, predicate: Predicate) -> Self {
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

    fn blocks_accessed(&self, transaction: &mut Transaction, buffer_list: &mut BufferList) -> u32 {
        self.table_plan.blocks_accessed(transaction, buffer_list)
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
                .reduction_factor(self.table_plan.as_ref(), transaction, buffer_list)
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

    fn blocks_accessed(&self, transaction: &mut Transaction, buffer_list: &mut BufferList) -> u32 {
        self.table_plan.blocks_accessed(transaction, buffer_list)
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

struct ProductPlan {
    // Fields for the plan
    left_plan: Box<dyn Plan>,
    right_plan: Box<dyn Plan>,
    schema: TableSchema,
}

impl ProductPlan {
    pub fn new(left_plan: Box<dyn Plan>, right_plan: Box<dyn Plan>) -> Self {
        let mut schema = TableSchema::new();
        schema.add_all(right_plan.get_schema().clone());
        schema.add_all(left_plan.get_schema().clone());

        ProductPlan {
            left_plan,
            right_plan,
            schema,
        }
    }
}

impl Plan for ProductPlan {
    fn open(
        &self,
        transaction: &mut Transaction,
        file_manager: &mut FileManager,
        buffer_list: &mut BufferList,
    ) -> Box<dyn Scan> {
        let scan1 = self.left_plan.open(transaction, file_manager, buffer_list);
        let scan2 = self.right_plan.open(transaction, file_manager, buffer_list);
        return Box::new(ProductScan::new(scan1, scan2));
    }

    fn get_schema(&self) -> &TableSchema {
        &self.schema
    }

    fn blocks_accessed(&self, transaction: &mut Transaction, buffer_list: &mut BufferList) -> u32 {
        self.left_plan.blocks_accessed(transaction, buffer_list)
            + self.left_plan.records_output(transaction, buffer_list)
                * self.right_plan.blocks_accessed(transaction, buffer_list)
    }

    fn records_output(&self, transaction: &mut Transaction, buffer_list: &mut BufferList) -> u32 {
        self.left_plan.records_output(transaction, buffer_list)
            * self.right_plan.records_output(transaction, buffer_list)
    }

    fn get_distinct_value(
        &self,
        field_name: String,
        transaction: &mut Transaction,
        buffer_list: &mut BufferList,
    ) -> u32 {
        if self.left_plan.get_schema().has_field(field_name.clone()) {
            return self
                .left_plan
                .get_distinct_value(field_name, transaction, buffer_list);
        } else {
            return self
                .right_plan
                .get_distinct_value(field_name, transaction, buffer_list);
        }
    }
}

fn create_query_plan(
    query_data: QueryData,
    transaction: &mut Transaction,
    file_manager: &mut FileManager,
    buffer_list: &mut BufferList,
    buffer_manager: &mut buffer_manager::BufferManager,
    stat_manager: &mut StatManager,
) -> Box<dyn Plan> {
    let mut plans: Vec<Box<dyn Plan>> = Vec::new();

    for table_name in query_data.table_name_list.iter() {
        let layout = record_page::Layout::new(TableSchema::new());
        let table_plan = TablePlan::new(
            table_name.clone(),
            transaction,
            file_manager,
            layout,
            buffer_list,
            buffer_manager,
            stat_manager,
        );
        let mut plan: Box<dyn Plan> = Box::new(table_plan);
        plans.push(plan);
    }

    let mut plan: Box<dyn Plan> = plans.pop().unwrap();

    for next_plan in plans.into_iter() {
        let product_plan = ProductPlan::new(plan, next_plan);
        plan = Box::new(product_plan);
    }

    let select_plan = SelectPlan::new(plan, query_data.predicate.clone());

    let project_plan = ProjectPlan::new(Box::new(select_plan), query_data.field_name_list.clone());

    return Box::new(project_plan);
}

pub fn execute_insert(
    transaction: &mut Transaction,
    file_manager: &mut FileManager,
    buffer_list: &mut BufferList,
    buffer_manager: &mut buffer_manager::BufferManager,
    stat_manager: &mut StatManager,
    insert_data: InsertData,
    layout: record_page::Layout,
) {
    let plan = TablePlan::new(
        insert_data.table_name.clone(),
        transaction,
        file_manager,
        layout.clone(),
        buffer_list,
        buffer_manager,
        stat_manager,
    );

    let mut scan = plan.open(transaction, file_manager, buffer_list);

    scan.insert(transaction, buffer_list, file_manager, layout.clone());

    let mut val_inter = insert_data.value_list.iter();

    for field in insert_data.field_name_list.iter() {
        let value = val_inter.next().unwrap();
        scan.set_value(transaction, buffer_list, field.clone(), value.value.clone());
    }

    scan.close(transaction, buffer_list, buffer_manager);
}
