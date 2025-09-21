use std::{cell::RefCell, cmp::min, fs::Metadata, path::Path, rc::Rc};

use crate::{
    buffer_manager_v2::BufferManagerV2,
    concurrency_manager::LockTable,
    error::{TableAlreadyExists, ValueNotFound},
    file_manager::FileManager,
    log_manager_v2::LogManagerV2,
    metadata_manager::{self, MetadataManager},
    parser::{CreateTableData, DeleteData, InsertData, QueryData, UpdateData},
    predicate::TableNameAndFieldName,
    predicate_v3::PredicateV2,
    record_page::{Layout, TableSchema},
    scan_v2::{ProductScanV2, ProjectScanV2, ScanV2, SelectScanV2},
    stat_manager_v2::{StatInfoV2, StatManagerV2},
    table_manager_v2::TableManagerV2,
    table_scan_v2::TableScan,
    transaction_v2::TransactionV2,
};

pub trait PlanV2 {
    fn open(&mut self) -> Result<Box<dyn ScanV2>, ValueNotFound>;
    fn get_schema(&self) -> &TableSchema;

    fn blocks_accessed(&self) -> u32;

    fn records_output(&self) -> u32;

    fn get_distinct_value(&self, field_name: String) -> u32;
}

pub struct TablePlanV2 {
    // Fields for the plan
    table_name: String,
    layout: Layout,
    stat_info: StatInfoV2,
    transaction: Rc<RefCell<TransactionV2>>,
}

impl TablePlanV2 {
    pub fn new(
        table_name: String,
        transaction: Rc<RefCell<TransactionV2>>,
        metadata_manager: &mut MetadataManager,
    ) -> Result<Self, ValueNotFound> {
        let layout = metadata_manager.get_layout(table_name.clone(), transaction.clone())?;
        let stat_info = metadata_manager.get_table_stats(
            table_name.clone(),
            transaction.clone(),
            layout.clone(),
        )?;

        Ok(TablePlanV2 {
            table_name,
            layout,
            stat_info,
            transaction: transaction.clone(),
        })
    }
}

impl PlanV2 for TablePlanV2 {
    fn open(&mut self) -> Result<Box<dyn ScanV2>, ValueNotFound> {
        return Ok(Box::new(TableScan::new(
            self.table_name.clone(),
            self.transaction.clone(),
            self.layout.clone(),
        )));
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

    fn get_distinct_value(&self, field_name: String) -> u32 {
        self.stat_info.distinct_value(field_name)
    }
}

pub struct SelectPlanV2 {
    // Fields for the plan
    table_plan: Box<dyn PlanV2>,
    predicate: PredicateV2,
}

impl SelectPlanV2 {
    pub fn new(table_plan: Box<dyn PlanV2>, predicate: PredicateV2) -> Self {
        SelectPlanV2 {
            table_plan,
            predicate,
        }
    }
}

impl PlanV2 for SelectPlanV2 {
    fn open(&mut self) -> Result<Box<dyn ScanV2>, ValueNotFound> {
        let scan = self.table_plan.open()?;
        println!("Opening SelectPlanV2 with predicate: {:?}", self.predicate);
        return Ok(Box::new(SelectScanV2::new(scan, self.predicate.clone())));
    }

    fn get_schema(&self) -> &TableSchema {
        self.table_plan.get_schema()
    }

    fn blocks_accessed(&self) -> u32 {
        self.table_plan.blocks_accessed()
    }

    fn get_distinct_value(&self, field_name: String) -> u32 {
        if self
            .predicate
            .equates_with_constant(field_name.clone())
            .is_some()
        {
            return 1;
        } else {
            let field_name2 = self.predicate.equate_with_field(field_name.clone());

            if (field_name2.is_some()) {
                return min(
                    self.table_plan
                        .get_distinct_value(field_name2.clone().unwrap()),
                    self.table_plan.get_distinct_value(field_name.clone()),
                );
            } else {
                return self.table_plan.get_distinct_value(field_name);
            }
        }
    }

    fn records_output(&self) -> u32 {
        self.table_plan.records_output() / self.predicate.reduction_factor(self.table_plan.as_ref())
    }
}

struct ProjectPlanV2 {
    // Fields for the plan
    plan: Box<dyn PlanV2>,
    schema: TableSchema,
    fields: Vec<TableNameAndFieldName>,
}

impl ProjectPlanV2 {
    pub fn new(plan: Box<dyn PlanV2>, field_list: Vec<TableNameAndFieldName>) -> Self {
        let mut schema = TableSchema::new();

        for field in field_list.iter() {
            schema.add(field.field_name.clone(), plan.get_schema().clone());
        }

        ProjectPlanV2 {
            plan,
            schema,
            fields: field_list,
        }
    }
}

impl PlanV2 for ProjectPlanV2 {
    fn open(&mut self) -> Result<Box<dyn ScanV2>, ValueNotFound> {
        let scan = self.plan.open()?;
        return Ok(Box::new(ProjectScanV2::new(scan, self.fields.clone())));
    }

    fn get_schema(&self) -> &TableSchema {
        &self.schema
    }

    fn blocks_accessed(&self) -> u32 {
        self.plan.blocks_accessed()
    }

    fn get_distinct_value(&self, field_name: String) -> u32 {
        self.plan.get_distinct_value(field_name)
    }

    fn records_output(&self) -> u32 {
        self.plan.records_output()
    }
}

struct ProductPlanV2 {
    // Fields for the plan
    left_plan: Box<dyn PlanV2>,
    right_plan: Box<dyn PlanV2>,
    schema: TableSchema,
}

impl ProductPlanV2 {
    pub fn new(left_plan: Box<dyn PlanV2>, right_plan: Box<dyn PlanV2>) -> Self {
        let mut schema = TableSchema::new();
        schema.add_all(right_plan.get_schema().clone());
        schema.add_all(left_plan.get_schema().clone());

        ProductPlanV2 {
            left_plan,
            right_plan,
            schema,
        }
    }
}

impl PlanV2 for ProductPlanV2 {
    fn open(&mut self) -> Result<Box<dyn ScanV2>, ValueNotFound> {
        let scan1 = self.left_plan.open()?;
        let scan2 = self.right_plan.open()?;
        return Ok(Box::new(ProductScanV2::new(scan1, scan2)));
    }

    fn get_schema(&self) -> &TableSchema {
        &self.schema
    }

    fn blocks_accessed(&self) -> u32 {
        self.left_plan.blocks_accessed()
            + self.left_plan.records_output() * self.right_plan.blocks_accessed()
    }

    fn records_output(&self) -> u32 {
        self.left_plan.records_output() * self.right_plan.records_output()
    }

    fn get_distinct_value(&self, field_name: String) -> u32 {
        if self.left_plan.get_schema().has_field(field_name.clone()) {
            return self.left_plan.get_distinct_value(field_name);
        } else {
            return self.right_plan.get_distinct_value(field_name);
        }
    }
}

pub fn create_query_plan(
    query_data: &QueryData,
    transaction: Rc<RefCell<TransactionV2>>,
    metadata_manager: &mut MetadataManager,
) -> Result<Box<dyn PlanV2>, ValueNotFound> {
    let mut plans: Vec<Box<dyn PlanV2>> = Vec::new();

    for table_name in query_data.table_name_list.iter() {
        let table_plan =
            TablePlanV2::new(table_name.clone(), transaction.clone(), metadata_manager)?;
        let plan: Box<dyn PlanV2> = Box::new(table_plan);
        plans.push(plan);
    }

    let mut plan: Box<dyn PlanV2> = plans.pop().unwrap();

    for next_plan in plans.into_iter() {
        let product_plan = ProductPlanV2::new(plan, next_plan);
        plan = Box::new(product_plan);
    }

    let select_plan = SelectPlanV2::new(plan, query_data.predicate.clone());

    let project_plan =
        ProjectPlanV2::new(Box::new(select_plan), query_data.field_name_list.clone());

    return Ok(Box::new(project_plan));
}

pub fn execute_insert(
    transaction: Rc<RefCell<TransactionV2>>,
    metadata_manager: &mut MetadataManager,
    insert_data: InsertData,
) -> Result<(), ValueNotFound> {
    let mut plan = TablePlanV2::new(
        insert_data.table_name.clone(),
        transaction,
        metadata_manager,
    )?;

    let mut scan = plan.open()?;

    scan.insert();

    let mut val_inter = insert_data.value_list.iter();

    for field in insert_data.field_name_list.iter() {
        let value = val_inter.next().unwrap();
        scan.set_value(field.clone(), value.value.clone());
    }

    scan.close();

    return Ok(());
}

pub fn execute_delete(
    transaction: Rc<RefCell<TransactionV2>>,
    metadata_manager: &mut MetadataManager,
    delete_data: DeleteData,
) -> Result<u32, ValueNotFound> {
    let mut plan = TablePlanV2::new(
        delete_data.table_name.clone(),
        transaction.clone(),
        metadata_manager,
    )?;
    let mut select_plan = SelectPlanV2::new(Box::new(plan), delete_data.predicate.clone());
    let mut scan = select_plan.open()?;

    let mut count = 0;

    while scan.next()? {
        scan.delete();
        count += 1;
    }

    scan.close();
    return Ok(count);
}

pub fn execute_update(
    transaction: Rc<RefCell<TransactionV2>>,
    metadata_manager: &mut MetadataManager,
    update_data: UpdateData,
) -> Result<u32, ValueNotFound> {
    let mut plan = TablePlanV2::new(
        update_data.table_name.clone(),
        transaction.clone(),
        metadata_manager,
    )?;
    let mut select_plan = SelectPlanV2::new(Box::new(plan), update_data.predicate.clone());
    let mut scan = select_plan.open()?;

    let mut count = 0;

    while scan.next()? {
        let field = update_data.field_name.clone();
        let value = update_data.new_value.clone();
        scan.set_value(field.clone(), value.value);
    }

    scan.close();
    return Ok(count);
}

pub fn execute_create_table(
    transaction: Rc<RefCell<TransactionV2>>,
    metadata_manager: &mut MetadataManager,
    create_table_data: CreateTableData,
) -> Result<(), TableAlreadyExists> {
    metadata_manager.create_table(
        create_table_data.table_name.clone(),
        &create_table_data.schema,
        transaction,
    )
}

#[cfg(test)]
mod tests {
    use std::{cell::RefCell, f32::consts::E, path::Path, rc::Rc};

    use rand::Rng;

    use crate::{
        buffer_manager_v2::BufferManagerV2,
        concurrency_manager::LockTable,
        database::Database,
        file_manager::{self, FileManager},
        log_manager,
        log_manager_v2::LogManagerV2,
        metadata_manager::{self, MetadataManager},
        parser::parse_sql,
        predicate::{Constant, ConstantValue, ExpressionValue},
        predicate_v3::{ExpressionV2, TermV2},
        record_page::TableSchema,
        transaction,
    };

    use super::*;

    #[test]
    fn test_insert_plan() -> Result<(), ValueNotFound> {
        let database = Database::new();
        let transaction = database.new_transaction(1);
        let mut metadata_manager = MetadataManager::new(transaction.clone())?;

        let parsed_sql =
            parse_sql("insert into posts_2 (title, content) values ('title1', 'body')".to_string())
                .unwrap();

        let insert_data = match parsed_sql {
            crate::parser::ParsedSQL::Insert(q) => q,
            _ => panic!("Expected a Insert variant from parse_sql"),
        };

        execute_insert(transaction.clone(), &mut metadata_manager, insert_data);

        transaction.borrow_mut().commit();

        return Ok(());
    }

    #[test]
    fn insert_data() -> Result<(), ValueNotFound> {
        let database = Database::new();
        let transaction = database.new_transaction(1);
        let mut metadata_manager = MetadataManager::new(transaction.clone())?;

        let create_table_sql =
            "create table test_table_11 (A_1 integer, B_1 varchar(10))".to_string();

        let create_table_data = match parse_sql(create_table_sql).unwrap() {
            crate::parser::ParsedSQL::CreateTable(q) => q,
            _ => panic!("Expected a CreateTable variant from parse_sql"),
        };

        let result = execute_create_table(
            transaction.clone(),
            &mut metadata_manager,
            create_table_data,
        );

        if result.is_err() {
            println!("Table already exists");
        }

        // mutable_table_manager.create_table(
        //     "table_catalog".to_string(),
        //     &mutable_table_manager.table_catalog_layout.schema.clone(),
        //     transaction.clone(),
        // );

        let insert_sql =
            "insert into test_table_11 (A_1, B_1) values (42, 'Hello World!')".to_string();

        let insert_data = match parse_sql(insert_sql).unwrap() {
            crate::parser::ParsedSQL::Insert(q) => q,
            _ => panic!("Expected a Insert variant from parse_sql"),
        };

        execute_insert(
            transaction.clone(),
            &mut metadata_manager,
            insert_data.clone(),
        );

        let insert_sql_2 =
            "insert into test_table_11 (A_1, B_1) values (42, 'Hello World!')".to_string();

        let insert_data_2 = match parse_sql(insert_sql_2).unwrap() {
            crate::parser::ParsedSQL::Insert(q) => q,
            _ => panic!("Expected a Insert variant from parse_sql"),
        };

        execute_insert(
            transaction.clone(),
            &mut metadata_manager,
            insert_data_2.clone(),
        );

        return Ok(());
    }

    #[test]
    fn test_join_query() -> Result<(), ValueNotFound> {
        let database = Database::new();
        let transaction = database.new_transaction(1);
        let mut metadata_manager = MetadataManager::new(transaction.clone())?;

        let parsed_sql = parse_sql(
            "select A, B, A_1, B_1 from test_table_10, test_table_11 where A = B_1".to_string(),
        )
        .unwrap();

        let query_data = match parsed_sql {
            crate::parser::ParsedSQL::Query(q) => q,
            _ => panic!("Expected a Query variant from parse_sql"),
        };

        let mut plan = create_query_plan(&query_data, transaction.clone(), &mut metadata_manager)?;

        let mut scan = plan.open()?;
        scan.move_to_before_first();
        while scan.next()? {
            let field1_value = scan.get_value(TableNameAndFieldName::new(None, "A".to_string()));
            let field2_value = scan.get_value(TableNameAndFieldName::new(None, "B_1".to_string()));

            if let Some(value) = field1_value {
                println!("Field A: {:?}", value);
            } else {
                println!("Field A: None");
            }

            if let Some(value) = field2_value {
                println!("Field B_1: {:?}", value);
            } else {
                println!("Field B_1: None");
            }
        }

        return Ok(());
    }

    fn test_plan() -> Result<(), ValueNotFound> {
        let database = Database::new();
        let transaction = database.new_transaction(1);
        let mut metadata_manager = MetadataManager::new(transaction.clone())?;

        // mutable_table_manager.create_table(
        //     "table_catalog".to_string(),
        //     &mutable_table_manager.table_catalog_layout.schema.clone(),
        //     transaction.clone(),
        // );

        // mutable_table_manager.create_table(
        //     "field_catalog".to_string(),
        //     &mutable_table_manager.field_catalog_layout.schema.clone(),
        //     transaction.clone(),
        // );

        // mutable_table_manager.create_table("test_table".to_string(), &schema, transaction.clone());

        let parsed_sql = parse_sql(
            "insert into test_table (A, B) values (44, 'Hello World yay!111')".to_string(),
        )
        .unwrap();

        let insert_data = match parsed_sql {
            crate::parser::ParsedSQL::Insert(q) => q,
            _ => panic!("Expected a Insert variant from parse_sql"),
        };

        // execute_insert(transaction.clone(), &mut metadata_manager, insert_data);

        // transaction.borrow_mut().commit();

        let parsed_sql = parse_sql("select A, B from test_table where A = 42".to_string()).unwrap();

        let query_data = match parsed_sql {
            crate::parser::ParsedSQL::Query(q) => q,
            _ => panic!("Expected a Query variant from parse_sql"),
        };

        let mut plan = create_query_plan(&query_data, transaction.clone(), &mut metadata_manager)?;

        let mut scan = plan.open()?;
        scan.move_to_before_first();
        while scan.next()? {
            let field1_value = scan.get_integer(TableNameAndFieldName::new(None, "A".to_string()));
            let field2_value = scan.get_string(TableNameAndFieldName::new(None, "B".to_string()));

            if let Some(value) = field1_value {
                println!("Field A: {}", value);
            } else {
                println!("Field A: None");
            }

            if let Some(value) = field2_value {
                println!("Field B: {}", value);
            } else {
                println!("Field B: None");
            }
        }

        return Ok(());
    }
}
