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

#[cfg(test)]
mod tests {

    use std::{fs::remove_file, path::Path};

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
    fn test_materialize() -> Result<(), ValueNotFound> {
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

        let mut materialize_plan = MaterializePlan::new(transaction.clone(), Box::new(table_plan));

        let mut temp_table = materialize_plan.open()?;

        struct TestValue {
            a1: ConstantValue,
            b1: ConstantValue,
        }

        let test_value_list = vec![
            TestValue {
                a1: ConstantValue::Number(1),
                b1: ConstantValue::String("Hello World!".to_string()),
            },
            TestValue {
                a1: ConstantValue::Number(2),
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
            TestValue {
                a1: ConstantValue::Number(1),
                b1: ConstantValue::String("Hello World!".to_string()),
            },
            TestValue {
                a1: ConstantValue::Number(2),
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
            TestValue {
                a1: ConstantValue::Number(1),
                b1: ConstantValue::String("Hello World!".to_string()),
            },
            TestValue {
                a1: ConstantValue::Number(2),
                b1: ConstantValue::String("Hello World2!".to_string()),
            },
            TestValue {
                a1: ConstantValue::Number(3),
                b1: ConstantValue::String("Hello World3!".to_string()),
            },
        ];

        let mut count = 0;

        while temp_table.next()? {
            let a1_value = temp_table
                .get_value(TableNameAndFieldName::new(None, "A_1".to_string()))
                .unwrap();
            let b1_value = temp_table
                .get_value(TableNameAndFieldName::new(None, "B_1".to_string()))
                .unwrap();
            println!("A_1: {:?}, B_1: {:?}", a1_value, b1_value);

            assert_eq!(a1_value, test_value_list[count].a1);
            assert_eq!(b1_value, test_value_list[count].b1);

            count += 1;
        }

        return Ok(());
    }
}
