use std::{cell::RefCell, rc::Rc};

use crate::{
    hash_index::HashIndex,
    index_update_planner::IndexUpdatePlanner,
    metadata_manager::MetadataManager,
    parser::InsertData,
    predicate::{Constant, ConstantValue, TableNameAndFieldName},
    scan_v2::ScanV2,
    table_scan_v2::TableScan,
    transaction_v2::TransactionV2,
};

#[cfg(test)]
use crate::{
    buffer_manager_v2::BufferManagerV2, concurrency_manager::LockTable, file_manager::FileManager,
    log_manager_v2::LogManagerV2, record_page::TableSchema, table_manager_v2::TableManagerV2,
};
#[cfg(test)]
use std::{
    path::Path,
    sync::{Arc, Mutex},
};

pub struct IndexJoinScan {
    pub left_scan: Box<dyn ScanV2>,
    pub index: HashIndex,
    pub join_field: TableNameAndFieldName,
    pub right_scan: TableScan,
}

impl IndexJoinScan {
    pub fn new(
        left_scan: Box<dyn ScanV2>,
        index: HashIndex,
        join_field: TableNameAndFieldName,
        right_scan: TableScan,
    ) -> Self {
        IndexJoinScan {
            left_scan,
            index,
            join_field,
            right_scan,
        }
    }

    fn reset_index(&mut self) {
        let search_key = self.left_scan.get_value(self.join_field.clone());
        let search_key_constant = Constant::new(search_key.unwrap());
        self.index.before_first(search_key_constant);
    }
}

impl ScanV2 for IndexJoinScan {
    fn move_to_before_first(&mut self) -> Result<(), crate::error::ValueNotFound> {
        self.left_scan.move_to_before_first()?;
        self.left_scan.next()?;
        self.reset_index();
        return Ok(());
    }

    fn next(&mut self) -> Result<bool, crate::error::ValueNotFound> {
        loop {
            if self.index.next().unwrap() {
                let new_record_id = self.index.get_data_record_id().unwrap().unwrap();
                self.right_scan.move_to_record_id(new_record_id);
                return Ok(true);
            }

            if !self.left_scan.next().unwrap() {
                return Ok(false);
            }

            self.reset_index();
        }
    }

    fn get_integer(&mut self, field_name: TableNameAndFieldName) -> Option<i32> {
        if self.right_scan.has_field(field_name.clone()) {
            return self.right_scan.get_integer(field_name);
        }
        return self.left_scan.get_integer(field_name);
    }

    fn get_string(&mut self, field_name: TableNameAndFieldName) -> Option<String> {
        if self.right_scan.has_field(field_name.clone()) {
            return self.right_scan.get_string(field_name);
        }
        return self.left_scan.get_string(field_name);
    }

    fn get_value(
        &mut self,
        field_name: TableNameAndFieldName,
    ) -> Option<crate::predicate::ConstantValue> {
        if self.right_scan.has_field(field_name.clone()) {
            return self.right_scan.get_value(field_name);
        }
        return self.left_scan.get_value(field_name);
    }

    fn has_field(&self, field_name: TableNameAndFieldName) -> bool {
        return self.right_scan.has_field(field_name.clone())
            || self.left_scan.has_field(field_name.clone());
    }

    fn close(&mut self) {
        self.left_scan.close();
        self.index.close();
        self.right_scan.close();
    }

    fn delete(&mut self) {
        panic!("IndexJoinScan does not support delete operation");
    }

    fn get_record_id(&self) -> crate::table_scan_v2::RecordID {
        panic!("IndexJoinScan does not support get_record_id operation");
    }

    fn insert(&mut self) {
        panic!("IndexJoinScan does not support insert operation");
    }

    fn move_to_record_id(&mut self, record_id: crate::table_scan_v2::RecordID) {
        panic!("IndexJoinScan does not support move_to_record_id operation");
    }

    fn set_integer(&mut self, field_name: String, value: i32) {
        panic!("IndexJoinScan does not support set_integer operation");
    }

    fn set_string(&mut self, field_name: String, value: String) {
        panic!("IndexJoinScan does not support set_string operation");
    }

    fn set_value(&mut self, field_name: String, value: crate::predicate::ConstantValue) {
        panic!("IndexJoinScan does not support set_value operation");
    }
}

#[test]
fn test_view_mgr() {
    let test_dir_name = format!("test_data_{}", uuid::Uuid::new_v4());
    let test_dir = Path::new(&test_dir_name);
    let block_size = 400;

    let log_file_name = format!("log_file_{}.txt", uuid::Uuid::new_v4());

    let file_manager = Arc::new(Mutex::new(FileManager::new(test_dir, block_size)));
    let log_manager = Arc::new(Mutex::new(LogManagerV2::new(
        file_manager.clone(),
        log_file_name.clone(),
    )));

    let buffer_manager = Arc::new(Mutex::new(BufferManagerV2::new(
        100,
        file_manager.clone(),
        log_manager.clone(),
    )));

    let lock_table = Arc::new(Mutex::new(LockTable::new()));

    let transaction = Rc::new(RefCell::new(TransactionV2::new(
        1,
        file_manager.clone(),
        buffer_manager.clone(),
        lock_table.clone(),
        log_manager.clone(),
    )));

    let table_manager = TableManagerV2::new(transaction.clone(), true);

    let mut student_table_schema = TableSchema::new();
    student_table_schema.add_integer_field("student_id".to_string());
    student_table_schema.add_string_field("name".to_string(), 10);
    student_table_schema.add_integer_field("age".to_string());

    let table_name = "student".to_string();

    table_manager.create_table(
        table_name.clone(),
        &student_table_schema,
        transaction.clone(),
    );

    let mut book_table_schema = TableSchema::new();
    book_table_schema.add_integer_field("student_id".to_string());
    book_table_schema.add_string_field("name".to_string(), 10);
    book_table_schema.add_integer_field("book_id".to_string());

    let book_table_name = "book".to_string();

    table_manager.create_table(
        book_table_name.clone(),
        &book_table_schema,
        transaction.clone(),
    );

    let mut index_update_planner = IndexUpdatePlanner::new();

    let metadata_manager = Rc::new(RefCell::new(
        MetadataManager::new(transaction.clone()).unwrap(),
    ));

    index_update_planner.execute_create_index(
        "student_id_index".to_string(),
        table_name.clone(),
        "student_id".to_string(),
        transaction.clone(),
        &mut metadata_manager.borrow_mut(),
    );

    create_student_test_data(
        table_name.clone(),
        transaction.clone(),
        metadata_manager.clone(),
        &mut index_update_planner,
    );

    create_book_test_data(
        book_table_name.clone(),
        transaction.clone(),
        metadata_manager.clone(),
        &mut index_update_planner,
    );

    let layout = metadata_manager
        .borrow()
        .get_layout(table_name.clone(), transaction.clone())
        .unwrap();

    let mut table_scan = TableScan::new(table_name.clone(), transaction.clone(), layout);

    let layout = metadata_manager
        .borrow()
        .get_layout(book_table_name.clone(), transaction.clone())
        .unwrap();

    let mut book_table_scan = TableScan::new(book_table_name.clone(), transaction.clone(), layout);

    let mut index = metadata_manager
        .borrow_mut()
        .get_index_info(table_name.clone(), transaction.clone())
        .unwrap();

    let index_info = index.get_mut(&"student_id".to_string()).unwrap();

    let hash_index = index_info.open();

    let mut index_join_scan = IndexJoinScan::new(
        Box::new(book_table_scan),
        hash_index,
        TableNameAndFieldName::new(None, "student_id".to_string()),
        table_scan,
    );

    index_join_scan.move_to_before_first();

    struct ScanResult {
        student_id: i32,
        name: String,
        age: i32,
        book_name: String,
    }

    let mut scan_results: Vec<ScanResult> = Vec::new();

    while index_join_scan.next().unwrap() {
        let student_id = index_join_scan
            .get_integer(TableNameAndFieldName::new(None, "student_id".to_string()))
            .unwrap();
        let name = index_join_scan
            .get_string(TableNameAndFieldName::new(
                Some("student".to_string()),
                "name".to_string(),
            ))
            .unwrap();
        let age = index_join_scan
            .get_integer(TableNameAndFieldName::new(None, "age".to_string()))
            .unwrap();

        let book_name = index_join_scan
            .get_string(TableNameAndFieldName::new(
                Some("book".to_string()),
                "name".to_string(),
            ))
            .unwrap();

        println!(
            "student_id: {}, name: {}, age: {}, book_name: {}",
            student_id, name, age, book_name
        );

        let scan_result = ScanResult {
            student_id,
            name,
            age,
            book_name,
        };

        scan_results.push(scan_result);
    }

    let test_scan_results = vec![
        ScanResult {
            student_id: 1,
            name: "Alice".to_string(),
            age: 20,
            book_name: "book_1".to_string(),
        },
        ScanResult {
            student_id: 1,
            name: "Alice".to_string(),
            age: 20,
            book_name: "book_2".to_string(),
        },
        ScanResult {
            student_id: 2,
            name: "Bob".to_string(),
            age: 22,
            book_name: "book_3".to_string(),
        },
    ];

    assert_eq!(scan_results.len(), test_scan_results.len());
    for (result, test_result) in scan_results.iter().zip(test_scan_results.iter()) {
        assert_eq!(result.student_id, test_result.student_id);
        assert_eq!(result.name, test_result.name);
        assert_eq!(result.age, test_result.age);
        assert_eq!(result.book_name, test_result.book_name);
    }
}

fn create_book_test_data(
    table_name: String,
    transaction: Rc<RefCell<TransactionV2>>,
    metadata_manager: Rc<RefCell<MetadataManager>>,
    index_update_planner: &mut IndexUpdatePlanner,
) {
    let field_name_list = vec![
        "student_id".to_string(),
        "name".to_string(),
        "book_id".to_string(),
    ];

    let value_list = vec![
        Constant::new(ConstantValue::Number(1)),
        Constant::new(ConstantValue::String("book_1".to_string())),
        Constant::new(ConstantValue::Number(1)),
    ];

    let insert_data = InsertData::new(table_name.clone(), field_name_list.clone(), value_list);

    index_update_planner.execute_insert(
        insert_data,
        transaction.clone(),
        &mut metadata_manager.borrow_mut(),
    );

    let value_list = vec![
        Constant::new(ConstantValue::Number(1)),
        Constant::new(ConstantValue::String("book_2".to_string())),
        Constant::new(ConstantValue::Number(2)),
    ];

    let insert_data = InsertData::new(table_name.clone(), field_name_list.clone(), value_list);

    index_update_planner.execute_insert(
        insert_data,
        transaction.clone(),
        &mut metadata_manager.borrow_mut(),
    );

    let value_list = vec![
        Constant::new(ConstantValue::Number(2)),
        Constant::new(ConstantValue::String("book_3".to_string())),
        Constant::new(ConstantValue::Number(3)),
    ];

    let insert_data = InsertData::new(table_name.clone(), field_name_list.clone(), value_list);

    index_update_planner.execute_insert(
        insert_data,
        transaction.clone(),
        &mut metadata_manager.borrow_mut(),
    );
}

fn create_student_test_data(
    table_name: String,
    transaction: Rc<RefCell<TransactionV2>>,
    metadata_manager: Rc<RefCell<MetadataManager>>,
    index_update_planner: &mut IndexUpdatePlanner,
) {
    let field_name_list = vec![
        "student_id".to_string(),
        "name".to_string(),
        "age".to_string(),
    ];
    let value_list = vec![
        Constant::new(ConstantValue::Number(4)),
        Constant::new(ConstantValue::String("ellie".to_string())),
        Constant::new(ConstantValue::Number(31)),
    ];

    let insert_data = InsertData::new(table_name.clone(), field_name_list.clone(), value_list);

    index_update_planner.execute_insert(
        insert_data,
        transaction.clone(),
        &mut metadata_manager.borrow_mut(),
    );

    let value_list = vec![
        Constant::new(ConstantValue::Number(5)),
        Constant::new(ConstantValue::String("risa".to_string())),
        Constant::new(ConstantValue::Number(41)),
    ];

    let insert_data = InsertData::new(table_name.clone(), field_name_list.clone(), value_list);

    index_update_planner.execute_insert(
        insert_data,
        transaction.clone(),
        &mut metadata_manager.borrow_mut(),
    );

    let value_list = vec![
        Constant::new(ConstantValue::Number(1)),
        Constant::new(ConstantValue::String("Alice".to_string())),
        Constant::new(ConstantValue::Number(20)),
    ];

    let insert_data = InsertData::new(table_name.clone(), field_name_list.clone(), value_list);

    index_update_planner.execute_insert(
        insert_data,
        transaction.clone(),
        &mut metadata_manager.borrow_mut(),
    );

    let value_list = vec![
        Constant::new(ConstantValue::Number(2)),
        Constant::new(ConstantValue::String("Bob".to_string())),
        Constant::new(ConstantValue::Number(22)),
    ];

    let insert_data = InsertData::new(table_name.clone(), field_name_list.clone(), value_list);

    index_update_planner.execute_insert(
        insert_data,
        transaction.clone(),
        &mut metadata_manager.borrow_mut(),
    );

    let value_list = vec![
        Constant::new(ConstantValue::Number(3)),
        Constant::new(ConstantValue::String("Robert".to_string())),
        Constant::new(ConstantValue::Number(30)),
    ];

    let insert_data = InsertData::new(table_name.clone(), field_name_list.clone(), value_list);

    index_update_planner.execute_insert(
        insert_data,
        transaction.clone(),
        &mut metadata_manager.borrow_mut(),
    );
}
