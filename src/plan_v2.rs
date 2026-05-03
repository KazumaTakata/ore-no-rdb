use std::{cell::RefCell, cmp::min, collections::HashMap, fs::Metadata, path::Path, rc::Rc};

use crate::{
    block,
    buffer_manager_v2::BufferManagerV2,
    concurrency_manager::LockTable,
    error::{TableAlreadyExists, ValueNotFound},
    file_manager::FileManager,
    group_by::{AggregateFunction, GroupByPlan, MaxFunction},
    hash_index::IndexSelectPlan,
    index_manager::IndexInfo,
    log_manager_v2::LogManagerV2,
    metadata_manager::{self, MetadataManager},
    parser::{parse_sql, CreateTableData, DeleteData, InsertData, QueryData, UpdateData},
    predicate::{Constant, ConstantValue, TableNameAndFieldName},
    predicate_v3::PredicateV2,
    record_page::{Layout, TableSchema},
    scan_v2::{ProductScanV2, ProjectScanV2, ScanV2, SelectScanV2},
    sort_plan::SortPlan,
    stat_manager_v2::StatInfoV2,
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

    fn block_accessed(left_plan: &Box<dyn PlanV2>, right_plan: &Box<dyn PlanV2>) -> u32 {
        println!(
            "Calculating block accessed for ProductPlanV2: left_plan blocks_accessed = {}, left_plan records_output = {}, right_plan blocks_accessed = {}",
            left_plan.blocks_accessed(),
            left_plan.records_output(),
            right_plan.blocks_accessed()
        );

        left_plan.blocks_accessed() + left_plan.records_output() * right_plan.blocks_accessed()
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

fn create_index_select(
    index_info_hash: HashMap<String, IndexInfo>,
    predicate: PredicateV2,
    table_plan: Box<dyn PlanV2>,
) -> Box<dyn PlanV2> {
    for (field_name, index_info) in index_info_hash.iter() {
        if let Some(constant) = predicate.equates_with_constant(field_name.clone()) {
            let index_select_plan = IndexSelectPlan::new(table_plan, index_info.clone(), constant);
            return Box::new(index_select_plan);
        }
    }
    return table_plan;
}

pub fn get_optimized_product_plan(plans: &mut Vec<Box<dyn PlanV2>>) -> Box<dyn PlanV2> {
    let mut plan: Box<dyn PlanV2> = plans.pop().unwrap();

    //TODO: productの順番を最適化する
    for next_plan in plans.drain(..) {
        let block_access_1 = ProductPlanV2::block_accessed(&plan, &next_plan);
        let block_access_2 = ProductPlanV2::block_accessed(&next_plan, &plan);

        if block_access_1 < block_access_2 {
            let product_plan = ProductPlanV2::new(plan, next_plan);
            plan = Box::new(product_plan);
        } else {
            let product_plan_2 = ProductPlanV2::new(next_plan, plan);
            plan = Box::new(product_plan_2);
        }
    }

    return plan;
}

pub fn create_query_plan(
    query_data: &QueryData,
    transaction: Rc<RefCell<TransactionV2>>,
    metadata_manager: &mut MetadataManager,
) -> Result<Box<dyn PlanV2>, ValueNotFound> {
    let mut plans: Vec<Box<dyn PlanV2>> = Vec::new();

    for table_name in query_data.table_name_list.iter() {
        let view_definition =
            metadata_manager.get_view_definition(table_name.clone(), transaction.clone());

        if let Some(view_def) = view_definition {
            let parsed_sql_list = parse_sql(view_def.clone());
            let parsed_sql = &parsed_sql_list[0];

            match parsed_sql {
                crate::parser::ParsedSQL::Query(q) => {
                    let view_plan = create_query_plan(q, transaction.clone(), metadata_manager)?;
                    plans.push(view_plan);
                    continue;
                }
                _ => panic!("Expected a Query variant from parse_sql for view definition"),
            }
        }

        let table_plan =
            TablePlanV2::new(table_name.clone(), transaction.clone(), metadata_manager)?;
        let plan: Box<dyn PlanV2> = Box::new(table_plan);

        let index_info_list =
            metadata_manager.get_index_info(table_name.clone(), transaction.clone());

        match index_info_list {
            Err(_) => {
                plans.push(plan);
                continue;
            }
            Ok(info) => {
                let index_select_plan =
                    create_index_select(info.clone(), query_data.predicate.clone(), plan);

                plans.push(index_select_plan);
            }
        };
    }

    // //TODO: productの順番を最適化する
    // for next_plan in plans.into_iter() {
    //     let block_access_1 = ProductPlanV2::block_accessed(&plan, &next_plan);
    //     let block_access_2 = ProductPlanV2::block_accessed(&next_plan, &plan);

    //     if block_access_1 < block_access_2 {
    //         let product_plan = ProductPlanV2::new(plan, next_plan);
    //         plan = Box::new(product_plan);
    //     } else {
    //         let product_plan_2 = ProductPlanV2::new(next_plan, plan);
    //         plan = Box::new(product_plan_2);
    //     }
    // }

    let optimized_plan = get_optimized_product_plan(&mut plans);

    let select_plan = SelectPlanV2::new(optimized_plan, query_data.predicate.clone());

    // let project_plan =
    //     ProjectPlanV2::new(Box::new(select_plan), query_data.field_name_list.clone());

    if query_data.order_by_list.len() > 0 {
        let sort_plan = SortPlan::new(
            transaction.clone(),
            Box::new(select_plan),
            query_data.order_by_list.clone(),
        );
        return Ok(Box::new(sort_plan));
    }

    if query_data.group_by_list.len() > 0 {
        let max_aggregate_functions = query_data
            .aggregate_functions
            .iter()
            .map(|f| Box::new(MaxFunction::new(f.field.clone())) as Box<dyn AggregateFunction>)
            .collect::<Vec<Box<dyn AggregateFunction>>>();

        let group_by_plan = GroupByPlan::new(
            transaction.clone(),
            query_data.group_by_list.clone(),
            Rc::new(RefCell::new(max_aggregate_functions)),
            Box::new(select_plan),
        );
        return Ok(Box::new(group_by_plan));
    }

    return Ok(Box::new(select_plan));
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
    let plan = TablePlanV2::new(
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

    use super::*;
    use crate::{database::Database, metadata_manager::MetadataManager, parser::parse_sql};

    #[test]

    fn test_optimized_product_plan() -> Result<(), ValueNotFound> {
        let directory_path_name = format!("test_data_{}", uuid::Uuid::new_v4());
        let directory_path = Path::new(&directory_path_name);
        let database = Database::new(directory_path);
        let transaction = database.new_transaction(1);
        let mut metadata_manager = MetadataManager::new(transaction.clone())?;

        let create_table_sql =
            "create table test_table_1 (A_1 integer, B_1 varchar(10))".to_string();

        let parsed_sql_list = parse_sql(create_table_sql.clone());

        let create_table_data = match &parsed_sql_list[0] {
            crate::parser::ParsedSQL::CreateTable(q) => q,
            _ => panic!("Expected a CreateTable variant from parse_sql"),
        };

        let result = execute_create_table(
            transaction.clone(),
            &mut metadata_manager,
            create_table_data.clone(),
        );

        if result.is_err() {
            println!("Table already exists");
        }

        let create_table_sql =
            "create table test_table_2 (A_2 integer, B_2 varchar(10))".to_string();

        let parsed_sql_list = parse_sql(create_table_sql.clone());

        let create_table_data = match &parsed_sql_list[0] {
            crate::parser::ParsedSQL::CreateTable(q) => q,
            _ => panic!("Expected a CreateTable variant from parse_sql"),
        };

        let result = execute_create_table(
            transaction.clone(),
            &mut metadata_manager,
            create_table_data.clone(),
        );

        if result.is_err() {
            println!("Table already exists");
        }

        let insert_sql_list = [
            "insert into test_table_1 (A_1, B_1) values (1, 'Hello World!')".to_string(),
            "insert into test_table_1 (A_1, B_1) values (2, 'Hello World2!')".to_string(),
            "insert into test_table_1 (A_1, B_1) values (3, 'Hello World3!')".to_string(),
            "insert into test_table_1 (A_1, B_1) values (4, 'Hello World4!')".to_string(),
            "insert into test_table_1 (A_1, B_1) values (1, 'Hello World!')".to_string(),
            "insert into test_table_1 (A_1, B_1) values (2, 'Hello World2!')".to_string(),
            "insert into test_table_1 (A_1, B_1) values (3, 'Hello World3!')".to_string(),
            "insert into test_table_1 (A_1, B_1) values (4, 'Hello World4!')".to_string(),
            "insert into test_table_1 (A_1, B_1) values (1, 'Hello World!')".to_string(),
            "insert into test_table_1 (A_1, B_1) values (2, 'Hello World2!')".to_string(),
            "insert into test_table_1 (A_1, B_1) values (3, 'Hello World3!')".to_string(),
            "insert into test_table_1 (A_1, B_1) values (4, 'Hello World4!')".to_string(),
            "insert into test_table_1 (A_1, B_1) values (1, 'Hello World!')".to_string(),
            "insert into test_table_1 (A_1, B_1) values (2, 'Hello World2!')".to_string(),
            "insert into test_table_1 (A_1, B_1) values (3, 'Hello World3!')".to_string(),
            "insert into test_table_1 (A_1, B_1) values (4, 'Hello World4!')".to_string(),
            "insert into test_table_1 (A_1, B_1) values (1, 'Hello World!')".to_string(),
            "insert into test_table_1 (A_1, B_1) values (2, 'Hello World2!')".to_string(),
            "insert into test_table_1 (A_1, B_1) values (3, 'Hello World3!')".to_string(),
            "insert into test_table_1 (A_1, B_1) values (4, 'Hello World4!')".to_string(),
            "insert into test_table_1 (A_1, B_1) values (1, 'Hello World!')".to_string(),
            "insert into test_table_1 (A_1, B_1) values (2, 'Hello World2!')".to_string(),
            "insert into test_table_1 (A_1, B_1) values (3, 'Hello World3!')".to_string(),
            "insert into test_table_1 (A_1, B_1) values (4, 'Hello World4!')".to_string(),
            "insert into test_table_1 (A_1, B_1) values (1, 'Hello World!')".to_string(),
            "insert into test_table_1 (A_1, B_1) values (2, 'Hello World2!')".to_string(),
            "insert into test_table_1 (A_1, B_1) values (3, 'Hello World3!')".to_string(),
            "insert into test_table_1 (A_1, B_1) values (4, 'Hello World4!')".to_string(),
            "insert into test_table_1 (A_1, B_1) values (1, 'Hello World!')".to_string(),
            "insert into test_table_1 (A_1, B_1) values (2, 'Hello World2!')".to_string(),
            "insert into test_table_1 (A_1, B_1) values (3, 'Hello World3!')".to_string(),
            "insert into test_table_1 (A_1, B_1) values (4, 'Hello World4!')".to_string(),
            "insert into test_table_2 (A_2, B_2) values (1, 'Hello World4!')".to_string(),
        ];

        for insert_sql in insert_sql_list.iter() {
            let parsed_sql_list = parse_sql(insert_sql.clone());

            let insert_data = match &parsed_sql_list[0] {
                crate::parser::ParsedSQL::Insert(q) => q,
                _ => panic!("Expected a Insert variant from parse_sql"),
            };

            execute_insert(
                transaction.clone(),
                &mut metadata_manager,
                insert_data.clone(),
            );
        }

        transaction.borrow_mut().commit();

        let database = Database::new(directory_path);
        let transaction = database.new_transaction(1);
        let mut metadata_manager = MetadataManager::new(transaction.clone())?;

        let table_plan_1 = TablePlanV2::new(
            "test_table_1".to_string(),
            transaction.clone(),
            &mut metadata_manager,
        )?;

        let table_plan_2 = TablePlanV2::new(
            "test_table_2".to_string(),
            transaction.clone(),
            &mut metadata_manager,
        )?;

        let mut plans = vec![
            Box::new(table_plan_1) as Box<dyn PlanV2>,
            Box::new(table_plan_2) as Box<dyn PlanV2>,
        ];

        let mut optimized_plan = get_optimized_product_plan(&mut plans);

        let mut scan = optimized_plan.open()?;

        assert_eq!(optimized_plan.blocks_accessed(), 6);

        // scan.move_to_before_first();

        // while scan.next()? {
        //     let field1_value = scan
        //         .get_value(TableNameAndFieldName::new(None, "A_1".to_string()))
        //         .unwrap();
        //     let field2_value = scan
        //         .get_value(TableNameAndFieldName::new(None, "B_1".to_string()))
        //         .unwrap();

        //     let field3_value = scan
        //         .get_value(TableNameAndFieldName::new(None, "A_2".to_string()))
        //         .unwrap();
        //     let field4_value = scan
        //         .get_value(TableNameAndFieldName::new(None, "B_2".to_string()))
        //         .unwrap();

        //     println!(
        //         "field1_value: {:?}, field2_value: {:?}, field3_value: {:?}, field4_value: {:?}",
        //         field1_value, field2_value, field3_value, field4_value
        //     );
        // }

        return Ok(());
    }

    fn test_insert_data() -> Result<(), ValueNotFound> {
        let directory_path_name = format!("test_data_{}", uuid::Uuid::new_v4());
        let directory_path = Path::new(&directory_path_name);
        let database = Database::new(directory_path);
        let transaction = database.new_transaction(1);
        let mut metadata_manager = MetadataManager::new(transaction.clone())?;

        let create_table_sql =
            "create table test_table_11 (A_1 integer, B_1 varchar(10))".to_string();

        let parsed_sql_list = parse_sql(create_table_sql.clone());

        let create_table_data = match &parsed_sql_list[0] {
            crate::parser::ParsedSQL::CreateTable(q) => q,
            _ => panic!("Expected a CreateTable variant from parse_sql"),
        };

        let result = execute_create_table(
            transaction.clone(),
            &mut metadata_manager,
            create_table_data.clone(),
        );

        if result.is_err() {
            println!("Table already exists");
        }

        let insert_sql =
            "insert into test_table_11 (A_1, B_1) values (42, 'Hello World!')".to_string();

        let parsed_sql_list = parse_sql(insert_sql.clone());

        let insert_data = match &parsed_sql_list[0] {
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

        let parsed_sql_list = parse_sql(insert_sql_2.clone());

        let insert_data_2 = match &parsed_sql_list[0] {
            crate::parser::ParsedSQL::Insert(q) => q,
            _ => panic!("Expected a Insert variant from parse_sql"),
        };

        execute_insert(
            transaction.clone(),
            &mut metadata_manager,
            insert_data_2.clone(),
        );
        transaction.borrow_mut().commit();

        // ここでテーブルにデータが挿入されたことを確認するために、selectクエリを実行してみる

        let database = Database::new(directory_path);
        let transaction = database.new_transaction(1);
        let mut metadata_manager = MetadataManager::new(transaction.clone())?;

        let create_table_sql = "select A_1, B_1 from test_table_11".to_string();

        let parsed_sql_list = parse_sql(create_table_sql.clone());

        let select_query = match &parsed_sql_list[0] {
            crate::parser::ParsedSQL::Query(q) => q,
            _ => panic!("Expected a Query  variant from parse_sql"),
        };
        let mut plan =
            create_query_plan(&select_query, transaction.clone(), &mut metadata_manager)?;

        let mut scan = plan.open()?;
        scan.move_to_before_first();

        let mut count = 0;

        while scan.next()? {
            count += 1;
            let field1_value = scan
                .get_value(TableNameAndFieldName::new(None, "A_1".to_string()))
                .unwrap();
            let field2_value = scan
                .get_value(TableNameAndFieldName::new(None, "B_1".to_string()))
                .unwrap();

            assert_eq!(field1_value, ConstantValue::Number(42));
            assert_eq!(
                field2_value,
                ConstantValue::String("Hello World!".to_string())
            );
        }

        assert_eq!(count, 2);

        return Ok(());
    }

    fn prepare_test_data_1(directory_path_name: &Path) -> Result<(), ValueNotFound> {
        let directory_path = Path::new(&directory_path_name);
        let database = Database::new(directory_path);
        let transaction = database.new_transaction(1);
        let mut metadata_manager = MetadataManager::new(transaction.clone())?;

        let create_table_sql =
            "create table test_table_1 (A_1 integer, B_1 varchar(10))".to_string();

        let parsed_sql_list = parse_sql(create_table_sql.clone());

        let create_table_data = match &parsed_sql_list[0] {
            crate::parser::ParsedSQL::CreateTable(q) => q,
            _ => panic!("Expected a CreateTable variant from parse_sql"),
        };

        let result = execute_create_table(
            transaction.clone(),
            &mut metadata_manager,
            create_table_data.clone(),
        );

        if result.is_err() {
            println!("Table already exists");
        }

        let insert_sql =
            "insert into test_table_1 (A_1, B_1) values (1, 'Hello World1')".to_string();

        insert_data_for_test(
            insert_sql.clone(),
            transaction.clone(),
            &mut metadata_manager,
        );

        let insert_sql =
            "insert into test_table_1 (A_1, B_1) values (2, 'Hello World2')".to_string();

        insert_data_for_test(
            insert_sql.clone(),
            transaction.clone(),
            &mut metadata_manager,
        );

        transaction.borrow_mut().commit();
        Ok(())
    }

    fn insert_data_for_test(
        insert_sql: String,
        transaction: Rc<RefCell<TransactionV2>>,
        metadata_manager: &mut MetadataManager,
    ) {
        let parsed_sql_list = parse_sql(insert_sql.clone());

        let insert_data = match &parsed_sql_list[0] {
            crate::parser::ParsedSQL::Insert(q) => q,
            _ => panic!("Expected a Insert variant from parse_sql"),
        };

        execute_insert(transaction.clone(), metadata_manager, insert_data.clone());

        transaction.borrow_mut().commit();
    }

    fn prepare_test_data_2(directory_path_name: &Path) -> Result<(), ValueNotFound> {
        let directory_path = Path::new(&directory_path_name);
        let database = Database::new(directory_path);
        let transaction = database.new_transaction(1);
        let mut metadata_manager = MetadataManager::new(transaction.clone())?;

        let create_table_sql =
            "create table test_table_2 (A_2 integer, B_2 varchar(10))".to_string();

        let parsed_sql_list = parse_sql(create_table_sql.clone());

        let create_table_data = match &parsed_sql_list[0] {
            crate::parser::ParsedSQL::CreateTable(q) => q,
            _ => panic!("Expected a CreateTable variant from parse_sql"),
        };

        let result = execute_create_table(
            transaction.clone(),
            &mut metadata_manager,
            create_table_data.clone(),
        );

        if result.is_err() {
            println!("Table already exists");
        }

        let insert_sql =
            "insert into test_table_2 (A_2, B_2) values (3, 'Hello World3')".to_string();
        insert_data_for_test(
            insert_sql.clone(),
            transaction.clone(),
            &mut metadata_manager,
        );

        let insert_sql =
            "insert into test_table_2 (A_2, B_2) values (4, 'Hello World4')".to_string();
        insert_data_for_test(
            insert_sql.clone(),
            transaction.clone(),
            &mut metadata_manager,
        );

        Ok(())
    }

    #[test]
    fn test_join_query() -> Result<(), ValueNotFound> {
        let directory_path_name = format!("test_data_{}", uuid::Uuid::new_v4());
        let directory_path = Path::new(&directory_path_name);

        prepare_test_data_1(directory_path);
        prepare_test_data_2(directory_path);

        let database = Database::new(directory_path);

        let transaction = database.new_transaction(1);
        let mut metadata_manager = MetadataManager::new(transaction.clone())?;

        let parsed_sql =
            &parse_sql("select A_1, B_1, A_2, B_2 from test_table_1, test_table_2".to_string())[0];

        let query_data = match parsed_sql {
            crate::parser::ParsedSQL::Query(q) => q,
            _ => panic!("Expected a Query variant from parse_sql"),
        };
        struct TestValue {
            a1: ConstantValue,
            b1: ConstantValue,
            a2: ConstantValue,
            b2: ConstantValue,
        }

        let test_value_1 = TestValue {
            a1: ConstantValue::Number(1),
            b1: ConstantValue::String("Hello World1".to_string()),
            a2: ConstantValue::Number(3),
            b2: ConstantValue::String("Hello World3".to_string()),
        };
        let test_value_3 = TestValue {
            a1: ConstantValue::Number(2),
            b1: ConstantValue::String("Hello World2".to_string()),
            a2: ConstantValue::Number(3),
            b2: ConstantValue::String("Hello World3".to_string()),
        };
        let test_value_2 = TestValue {
            a1: ConstantValue::Number(1),
            b1: ConstantValue::String("Hello World1".to_string()),
            a2: ConstantValue::Number(4),
            b2: ConstantValue::String("Hello World4".to_string()),
        };
        let test_value_4 = TestValue {
            a1: ConstantValue::Number(2),
            b1: ConstantValue::String("Hello World2".to_string()),
            a2: ConstantValue::Number(4),
            b2: ConstantValue::String("Hello World4".to_string()),
        };

        let mut test_value_list: Vec<TestValue> =
            vec![test_value_1, test_value_2, test_value_3, test_value_4];

        let mut plan = create_query_plan(&query_data, transaction.clone(), &mut metadata_manager)?;

        let mut scan = plan.open()?;
        scan.move_to_before_first();

        let mut count = 0;

        while scan.next()? {
            let field1_value = scan
                .get_value(TableNameAndFieldName::new(None, "A_1".to_string()))
                .unwrap();
            let field2_value = scan
                .get_value(TableNameAndFieldName::new(None, "B_1".to_string()))
                .unwrap();
            let field3_value = scan
                .get_value(TableNameAndFieldName::new(None, "A_2".to_string()))
                .unwrap();
            let field4_value = scan
                .get_value(TableNameAndFieldName::new(None, "B_2".to_string()))
                .unwrap();

            println!(
                "{}",
                format!(
                "field1_value: {:?}, field2_value: {:?}, field3_value: {:?}, field4_value: {:?}",
                field1_value, field2_value, field3_value, field4_value
            )
            );

            let test_value = &test_value_list[count];
            count += 1;

            assert_eq!(field1_value, test_value.a1);
            assert_eq!(field2_value, test_value.b1);
            assert_eq!(field3_value, test_value.a2);
            assert_eq!(field4_value, test_value.b2);
        }

        return Ok(());
    }

    fn prepare_test_data_3(directory_path_name: &Path) -> Result<(), ValueNotFound> {
        let directory_path = Path::new(&directory_path_name);
        let database = Database::new(directory_path);
        let transaction = database.new_transaction(1);
        let mut metadata_manager = MetadataManager::new(transaction.clone())?;

        let create_table_sql =
            "create table test_table_1 (A_1 integer, B_1 varchar(10))".to_string();

        let parsed_sql_list = parse_sql(create_table_sql.clone());

        let create_table_data = match &parsed_sql_list[0] {
            crate::parser::ParsedSQL::CreateTable(q) => q,
            _ => panic!("Expected a CreateTable variant from parse_sql"),
        };

        let result = execute_create_table(
            transaction.clone(),
            &mut metadata_manager,
            create_table_data.clone(),
        );

        if result.is_err() {
            println!("Table already exists");
        }

        let insert_sql =
            "insert into test_table_1 (A_1, B_1) values (1, 'Hello World1')".to_string();

        insert_data_for_test(
            insert_sql.clone(),
            transaction.clone(),
            &mut metadata_manager,
        );

        let insert_sql =
            "insert into test_table_1 (A_1, B_1) values (2, 'Hello World2')".to_string();

        insert_data_for_test(
            insert_sql.clone(),
            transaction.clone(),
            &mut metadata_manager,
        );

        let insert_sql =
            "insert into test_table_1 (A_1, B_1) values (3, 'Hello World3')".to_string();

        insert_data_for_test(
            insert_sql.clone(),
            transaction.clone(),
            &mut metadata_manager,
        );

        let insert_sql =
            "insert into test_table_1 (A_1, B_1) values (4, 'Hello World4')".to_string();

        insert_data_for_test(
            insert_sql.clone(),
            transaction.clone(),
            &mut metadata_manager,
        );

        let insert_sql =
            "insert into test_table_1 (A_1, B_1) values (1, 'Hello World1111')".to_string();

        insert_data_for_test(
            insert_sql.clone(),
            transaction.clone(),
            &mut metadata_manager,
        );

        transaction.borrow_mut().commit();

        metadata_manager.create_view(
            "test_view".to_string(),
            "select A_1, B_1 from test_table_1".to_string(),
            transaction.clone(),
        );

        Ok(())
    }

    #[test]
    fn test_view_query() -> Result<(), ValueNotFound> {
        let directory_path_name = format!("test_data_{}", uuid::Uuid::new_v4());
        let directory_path = Path::new(&directory_path_name);

        prepare_test_data_3(directory_path)?;

        let database = Database::new(directory_path);

        let transaction = database.new_transaction(1);
        let mut metadata_manager = MetadataManager::new(transaction.clone())?;

        let parsed_sql = &parse_sql("select A_1, B_1 from test_view where A_1 = 1".to_string())[0];

        let query_data = match parsed_sql {
            crate::parser::ParsedSQL::Query(q) => q,
            _ => panic!("Expected a Query variant from parse_sql"),
        };

        let mut plan = create_query_plan(&query_data, transaction.clone(), &mut metadata_manager)?;
        let mut scan = plan.open()?;
        scan.move_to_before_first();
        let mut count = 0;

        scan.next()?;
        let field1_value = scan
            .get_value(TableNameAndFieldName::new(None, "A_1".to_string()))
            .unwrap();
        let field2_value = scan
            .get_value(TableNameAndFieldName::new(None, "B_1".to_string()))
            .unwrap();

        assert_eq!(field1_value, ConstantValue::Number(1));
        assert_eq!(
            field2_value,
            ConstantValue::String("Hello World1".to_string())
        );

        scan.next()?;
        let field1_value = scan
            .get_value(TableNameAndFieldName::new(None, "A_1".to_string()))
            .unwrap();
        let field2_value = scan
            .get_value(TableNameAndFieldName::new(None, "B_1".to_string()))
            .unwrap();

        assert_eq!(field1_value, ConstantValue::Number(1));
        assert_eq!(
            field2_value,
            ConstantValue::String("Hello World1111".to_string())
        );

        assert_eq!(scan.next().unwrap(), false);

        Ok(())
    }
}
