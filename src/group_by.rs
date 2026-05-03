use std::{cell::RefCell, collections::HashMap, rc::Rc};

use crate::{
    error::ValueNotFound,
    plan_v2::PlanV2,
    predicate::{Constant, ConstantValue, TableNameAndFieldName},
    record_page::{TableFieldType, TableSchema},
    scan_v2::ScanV2,
    sort_plan::SortPlan,
    transaction_v2::TransactionV2,
};

pub struct GroupByPlan {
    transaction: Rc<RefCell<TransactionV2>>,
    group_fields: Vec<TableNameAndFieldName>,
    aggregate_functions: Rc<RefCell<Vec<Box<dyn AggregateFunction>>>>,
    plan: Box<dyn PlanV2>,
}

impl GroupByPlan {
    pub fn new(
        transaction: Rc<RefCell<TransactionV2>>,
        group_fields: Vec<TableNameAndFieldName>,
        aggregate_functions: Rc<RefCell<Vec<Box<dyn AggregateFunction>>>>,
        plan: Box<dyn PlanV2>,
    ) -> Self {
        let sort_plan = SortPlan::new(transaction.clone(), plan, group_fields.clone());

        let mut table_schema = TableSchema::new();

        for func in aggregate_functions.borrow_mut().iter() {
            table_schema.add_field(func.get_field(), TableFieldType::INTEGER, 0);
        }

        for field in group_fields.iter() {
            table_schema.add(field.field_name.clone(), sort_plan.get_schema().clone());
        }

        GroupByPlan {
            transaction,
            group_fields,
            aggregate_functions,
            plan: Box::new(sort_plan),
        }
    }
}

impl PlanV2 for GroupByPlan {
    fn open(&mut self) -> Result<Box<dyn ScanV2>, ValueNotFound> {
        let source_scan = self.plan.open()?;
        return Ok(Box::new(GroupByScan::new(
            source_scan,
            self.group_fields.clone(),
            self.aggregate_functions.clone(),
        )));
    }

    fn blocks_accessed(&self) -> u32 {
        self.plan.blocks_accessed()
    }

    fn records_output(&self) -> u32 {
        let mut number_of_groups = 1;
        for field in self.group_fields.iter() {
            number_of_groups *= self.plan.get_distinct_value(field.field_name.clone());
        }

        return number_of_groups;
    }

    fn get_distinct_value(&self, field_name: String) -> u32 {
        if (self.get_schema().has_field(field_name.clone())) {
            return self.plan.get_distinct_value(field_name);
        } else {
            return self.records_output();
        }
    }

    fn get_schema(&self) -> &TableSchema {
        self.plan.get_schema()
    }
}

pub trait AggregateFunction {
    fn process_first(&mut self, scan: &mut dyn ScanV2);
    fn process_next(&mut self, scan: &mut dyn ScanV2);
    fn get_field(&self) -> String;
    fn get_value(&self) -> Constant;
}

struct GroupValue {
    values: HashMap<String, Constant>,
}

impl GroupValue {
    pub fn new(scan: &mut dyn ScanV2, fields: Vec<TableNameAndFieldName>) -> Self {
        let mut values = HashMap::new();
        for field in fields.iter() {
            let value = scan.get_value(field.clone()).unwrap();
            values.insert(field.field_name.clone(), Constant::new(value));
        }
        GroupValue { values }
    }

    pub fn get_value(&self, field_name: &String) -> Option<&Constant> {
        self.values.get(field_name)
    }

    pub fn equals(&self, other: &GroupValue) -> bool {
        for (key, value) in self.values.iter() {
            if let Some(other_value) = other.get_value(key) {
                if value.compare_to(other_value.value.clone()) != std::cmp::Ordering::Equal {
                    return false;
                }
            } else {
                return false;
            }
        }
        true
    }
}

struct GroupByScan {
    source_scan: Box<dyn ScanV2>,
    group_fields: Vec<TableNameAndFieldName>,
    aggregate_functions: Rc<RefCell<Vec<Box<dyn AggregateFunction>>>>,
    group_value: Option<GroupValue>,
    more_groups: bool,
}

impl GroupByScan {
    pub fn new(
        source_scan: Box<dyn ScanV2>,
        group_fields: Vec<TableNameAndFieldName>,
        aggregate_functions: Rc<RefCell<Vec<Box<dyn AggregateFunction>>>>,
    ) -> Self {
        GroupByScan {
            source_scan,
            group_fields,
            aggregate_functions,
            group_value: None,
            more_groups: true,
        }
    }
}

impl ScanV2 for GroupByScan {
    fn next(&mut self) -> Result<bool, ValueNotFound> {
        if self.more_groups == false {
            return Ok(false);
        }

        for func in self.aggregate_functions.borrow_mut().iter_mut() {
            func.process_first(self.source_scan.as_mut());
        }

        self.group_value = Some(GroupValue::new(
            self.source_scan.as_mut(),
            self.group_fields.clone(),
        ));

        self.more_groups = self.source_scan.next()?;

        while self.more_groups {
            let current_group_value =
                GroupValue::new(self.source_scan.as_mut(), self.group_fields.clone());
            if !self
                .group_value
                .as_ref()
                .unwrap()
                .equals(&current_group_value)
            {
                break;
            }

            for func in self.aggregate_functions.borrow_mut().iter_mut() {
                func.process_next(self.source_scan.as_mut());
            }

            self.more_groups = self.source_scan.next()?;
        }

        Ok(true)
    }

    fn get_value(&mut self, field_name: TableNameAndFieldName) -> Option<ConstantValue> {
        let value = self.group_fields.iter().find(|f| {
            f.field_name == field_name.field_name && f.table_name == field_name.table_name
        });

        if let Some(_) = value {
            return self
                .group_value
                .as_ref()
                .unwrap()
                .get_value(&field_name.field_name)
                .map(|c| c.value.clone());
        }

        for func in self.aggregate_functions.borrow_mut().iter() {
            if func.get_field() == field_name.field_name
                && func.get_field() == field_name.field_name
            {
                return Some(func.get_value().value.clone());
            }
        }

        panic!("Field not found: {}", field_name.field_name);
    }

    fn get_string(&mut self, field_name: TableNameAndFieldName) -> Option<String> {
        if let Some(ConstantValue::String(s)) = self.get_value(field_name) {
            return Some(s);
        }
        return None;
    }

    fn get_integer(&mut self, field_name: TableNameAndFieldName) -> Option<i32> {
        if let Some(ConstantValue::Number(i)) = self.get_value(field_name) {
            return Some(i);
        }
        return None;
    }

    fn has_field(&self, field_name: TableNameAndFieldName) -> bool {
        for func in self.aggregate_functions.borrow().iter() {
            if func.get_field() == field_name.field_name {
                return true;
            }
        }

        for field in self.group_fields.iter() {
            if field.field_name == field_name.field_name {
                return true;
            }
        }

        return false;
    }

    fn close(&mut self) {
        self.source_scan.close()
    }

    fn move_to_before_first(&mut self) -> Result<(), ValueNotFound> {
        self.source_scan.move_to_before_first();
        self.more_groups = self.source_scan.next().unwrap();
        return Ok(());
    }

    fn delete(&mut self) {
        panic!("Cannot delete from GroupByScan")
    }

    fn insert(&mut self) {
        panic!("Cannot insert into GroupByScan")
    }

    fn set_integer(&mut self, field_name: String, value: i32) {
        panic!("Cannot set value in GroupByScan")
    }

    fn set_value(&mut self, field_name: String, value: ConstantValue) {
        panic!("Cannot set value in GroupByScan")
    }

    fn set_string(&mut self, field_name: String, value: String) {
        panic!("Cannot set value in GroupByScan")
    }

    fn get_record_id(&self) -> crate::table_scan_v2::RecordID {
        panic!("Cannot get RecordID from GroupByScan")
    }

    fn move_to_record_id(&mut self, record_id: crate::table_scan_v2::RecordID) {
        panic!("Cannot move to RecordID in GroupByScan")
    }
}

pub struct MaxFunction {
    field_name: TableNameAndFieldName,
    max_value: Option<Constant>,
}

impl MaxFunction {
    pub fn new(field_name: TableNameAndFieldName) -> Self {
        MaxFunction {
            field_name,
            max_value: None,
        }
    }
}

impl AggregateFunction for MaxFunction {
    fn process_first(&mut self, scan: &mut dyn ScanV2) {
        let value = scan.get_value(self.field_name.clone()).unwrap();
        self.max_value = Some(Constant::new(value));
    }

    fn process_next(&mut self, scan: &mut dyn ScanV2) {
        let new_value = scan.get_value(self.field_name.clone()).unwrap();
        if (Constant::new(new_value.clone())).compare_to(self.max_value.clone().unwrap().value)
            == std::cmp::Ordering::Greater
        {
            self.max_value = Some(Constant::new(new_value));
        }
    }

    fn get_field(&self) -> String {
        let field_name = format!("max_{}", self.field_name.clone().field_name);
        return field_name;
    }

    fn get_value(&self) -> Constant {
        self.max_value.clone().unwrap()
    }
}

#[cfg(test)]
mod tests {

    use std::path::Path;

    use crate::{
        database::Database,
        metadata_manager::MetadataManager,
        parser::parse_sql,
        plan_v2::{execute_create_table, execute_insert, TablePlanV2},
        predicate::ConstantValue,
    };

    use super::*;

    fn prepare_test_data(directory_path_name: String) -> Result<(), ValueNotFound> {
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
            "insert into test_table_1 (A_1, B_1) values (2, 'Hello World!')".to_string(),
            "insert into test_table_1 (A_1, B_1) values (3, 'Hello World!')".to_string(),
            "insert into test_table_1 (A_1, B_1) values (4, 'Hello World!')".to_string(),
            "insert into test_table_1 (A_1, B_1) values (1, 'Hello World!')".to_string(),
            "insert into test_table_1 (A_1, B_1) values (2, 'Hello World2!')".to_string(),
            "insert into test_table_1 (A_1, B_1) values (3, 'Hello World2!')".to_string(),
            "insert into test_table_1 (A_1, B_1) values (4, 'Hello World4!')".to_string(),
            "insert into test_table_1 (A_1, B_1) values (1, 'Hello World!')".to_string(),
            "insert into test_table_1 (A_1, B_1) values (2, 'Hello World2!')".to_string(),
            "insert into test_table_1 (A_1, B_1) values (3, 'Hello World3!')".to_string(),
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

        return Ok(());
    }

    #[test]
    fn test_group_by() -> Result<(), ValueNotFound> {
        let directory_path_name = format!("test_data_{}", uuid::Uuid::new_v4());
        prepare_test_data(directory_path_name.clone());
        let directory_path = Path::new(&directory_path_name);
        let database = Database::new(directory_path);
        let transaction = database.new_transaction(1);
        let mut metadata_manager = MetadataManager::new(transaction.clone())?;

        let table_plan = TablePlanV2::new(
            "test_table_1".to_string(),
            transaction.clone(),
            &mut metadata_manager,
        )
        .unwrap();

        let mut group_by_plan = GroupByPlan::new(
            transaction.clone(),
            vec![TableNameAndFieldName::new(None, "B_1".to_string())],
            Rc::new(RefCell::new(vec![Box::new(MaxFunction::new(
                TableNameAndFieldName::new(None, "A_1".to_string()),
            ))])),
            Box::new(table_plan),
        );

        let mut group_by_scan = group_by_plan.open()?;

        struct TestValue {
            a1: ConstantValue,
            b1: ConstantValue,
        }

        let test_value_list = vec![
            TestValue {
                a1: ConstantValue::Number(4),
                b1: ConstantValue::String("Hello World!".to_string()),
            },
            TestValue {
                a1: ConstantValue::Number(3),
                b1: ConstantValue::String("Hello World2!".to_string()),
            },
            TestValue {
                a1: ConstantValue::Number(3),
                b1: ConstantValue::String("Hello World3!".to_string()),
            },
            TestValue {
                a1: ConstantValue::Number(4),
                b1: ConstantValue::String("Hello World4!".to_string()),
            },
        ];

        group_by_scan.move_to_before_first()?;

        let mut count = 0;

        while group_by_scan.next()? {
            let field1_value = group_by_scan
                .get_value(TableNameAndFieldName::new(None, "B_1".to_string()))
                .unwrap();
            let field2_value = group_by_scan
                .get_value(TableNameAndFieldName::new(None, "max_A_1".to_string()))
                .unwrap();

            println!(
                "field1_value: {:?}, field2_value: {:?}",
                field1_value, field2_value
            );

            assert_eq!(field1_value, test_value_list[count].b1);
            assert_eq!(field2_value, test_value_list[count].a1);

            count += 1;
        }

        return Ok(());
    }
}
