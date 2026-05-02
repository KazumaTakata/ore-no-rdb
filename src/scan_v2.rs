use pest::pratt_parser::Op;

use crate::{
    error::ValueNotFound,
    predicate::{ConstantValue, TableNameAndFieldName},
    predicate_v3::PredicateV2,
    table_scan_v2::RecordID,
};

pub trait ScanV2 {
    fn move_to_before_first(&mut self) -> Result<(), ValueNotFound>;
    fn next(&mut self) -> Result<bool, ValueNotFound>;
    fn get_integer(&mut self, field_name: TableNameAndFieldName) -> Option<i32>;
    fn get_string(&mut self, field_name: TableNameAndFieldName) -> Option<String>;
    fn get_value(&mut self, field_name: TableNameAndFieldName) -> Option<ConstantValue>;
    fn close(&mut self);
    fn has_field(&self, field_name: TableNameAndFieldName) -> bool;
    fn set_integer(&mut self, field_name: String, value: i32);
    fn set_string(&mut self, field_name: String, value: String);
    fn set_value(&mut self, field_name: String, value: ConstantValue);

    fn insert(&mut self);
    fn delete(&mut self);

    fn get_record_id(&self) -> RecordID;
    fn move_to_record_id(&mut self, record_id: RecordID);
}

pub struct SelectScanV2 {
    scan: Box<dyn ScanV2>,
    predicate: PredicateV2,
}

impl SelectScanV2 {
    pub fn new(scan: Box<dyn ScanV2>, predicate: PredicateV2) -> Self {
        SelectScanV2 { scan, predicate }
    }
}

impl ScanV2 for SelectScanV2 {
    fn move_to_before_first(&mut self) -> Result<(), ValueNotFound> {
        self.scan.move_to_before_first();
        return Ok(());
    }

    fn next(&mut self) -> Result<bool, ValueNotFound> {
        while self.scan.next()? {
            let is_satisfied = self.predicate.is_satisfied(&mut *self.scan);
            match is_satisfied {
                Some(true) => return Ok(true),
                Some(false) => continue,
                // TODO: 適切なエラー処理
                None => return Err(ValueNotFound::new("predicate evaluation".to_string(), None)),
            }
        }
        return Ok(false);
    }

    fn get_integer(&mut self, field_name: TableNameAndFieldName) -> Option<i32> {
        self.scan.get_integer(field_name)
    }

    fn get_string(&mut self, field_name: TableNameAndFieldName) -> Option<String> {
        self.scan.get_string(field_name)
    }
    fn get_value(&mut self, field_name: TableNameAndFieldName) -> Option<ConstantValue> {
        self.scan.get_value(field_name)
    }

    fn close(&mut self) {
        self.scan.close();
    }

    fn has_field(&self, field_name: TableNameAndFieldName) -> bool {
        self.scan.has_field(field_name)
    }

    fn set_integer(&mut self, field_name: String, value: i32) {
        self.scan.set_integer(field_name, value);
    }

    fn set_string(&mut self, field_name: String, value: String) {
        self.scan.set_string(field_name, value);
    }

    fn set_value(&mut self, field_name: String, value: ConstantValue) {
        self.scan.set_value(field_name, value);
    }

    fn delete(&mut self) {
        self.scan.delete();
    }

    fn insert(&mut self) {
        self.scan.insert();
    }

    fn get_record_id(&self) -> RecordID {
        self.scan.get_record_id()
    }

    fn move_to_record_id(&mut self, record_id: RecordID) {
        self.scan.move_to_record_id(record_id);
    }
}

pub struct ProjectScanV2 {
    scan: Box<dyn ScanV2>,
    fields: Vec<TableNameAndFieldName>,
}

impl ProjectScanV2 {
    pub fn new(scan: Box<dyn ScanV2>, fields: Vec<TableNameAndFieldName>) -> Self {
        ProjectScanV2 { scan, fields }
    }
}

impl ScanV2 for ProjectScanV2 {
    fn move_to_before_first(&mut self) -> Result<(), ValueNotFound> {
        self.scan.move_to_before_first()
    }

    fn next(&mut self) -> Result<bool, ValueNotFound> {
        self.scan.next()
    }

    fn get_integer(&mut self, field_name: TableNameAndFieldName) -> Option<i32> {
        if self.has_field(field_name.clone()) {
            return self.scan.get_integer(field_name);
        }
        None
    }

    fn get_string(&mut self, field_name: TableNameAndFieldName) -> Option<String> {
        if self.has_field(field_name.clone()) {
            return self.scan.get_string(field_name);
        }
        None
    }

    fn get_value(&mut self, field_name: TableNameAndFieldName) -> Option<ConstantValue> {
        if self.has_field(field_name.clone()) {
            return self.scan.get_value(field_name);
        }

        return None;
    }

    fn close(&mut self) {
        self.scan.close();
    }

    fn has_field(&self, table_name_and_field_name: TableNameAndFieldName) -> bool {
        self.fields.iter().any(|table_and_field| {
            table_and_field.field_name == table_name_and_field_name.field_name
                && (table_and_field.table_name.is_none()
                    || table_and_field.table_name == table_name_and_field_name.table_name)
        })
    }

    fn set_integer(&mut self, field_name: String, value: i32) {
        panic!("set_integer not implemented for ProjectScan");
    }

    fn set_string(&mut self, field_name: String, value: String) {
        panic!("set_string not implemented for ProjectScan");
    }

    fn set_value(&mut self, field_name: String, value: ConstantValue) {
        panic!("set_value not implemented for ProjectScan");
    }

    fn delete(&mut self) {
        panic!("Delete not supported in ProjectScan");
    }

    fn insert(&mut self) {
        panic!("Insert not supported in ProjectScan");
    }

    fn get_record_id(&self) -> RecordID {
        panic!("get_record_id not implemented for ProjectScan");
    }

    fn move_to_record_id(&mut self, record_id: RecordID) {
        panic!("move_to_record_id not implemented for ProjectScan");
    }
}

pub struct ProductScanV2 {
    left_scan: Box<dyn ScanV2>,
    right_scan: Box<dyn ScanV2>,
}

impl ProductScanV2 {
    pub fn new(left_scan: Box<dyn ScanV2>, right_scan: Box<dyn ScanV2>) -> Self {
        ProductScanV2 {
            left_scan,
            right_scan,
        }
    }

    pub fn new_with_product_scan(product_scan: ProductScanV2, right_scan: Box<dyn ScanV2>) -> Self {
        ProductScanV2 {
            left_scan: product_scan.left_scan,
            right_scan,
        }
    }
}

impl ScanV2 for ProductScanV2 {
    fn move_to_before_first(&mut self) -> Result<(), ValueNotFound> {
        self.left_scan.move_to_before_first()?;
        self.left_scan.next()?;
        self.right_scan.move_to_before_first()?;
        Ok(())
    }

    fn next(&mut self) -> Result<bool, ValueNotFound> {
        let right_scan_next = self.right_scan.next()?;

        if right_scan_next {
            return Ok(true);
        }

        self.right_scan.move_to_before_first();

        let left_scan_next = self.left_scan.next()?;
        let right_scan_next = self.right_scan.next()?;

        return Ok(right_scan_next && left_scan_next);
    }

    fn get_integer(&mut self, field_name: TableNameAndFieldName) -> Option<i32> {
        if self.left_scan.has_field(field_name.clone()) {
            return self.left_scan.get_integer(field_name);
        } else {
            return self.right_scan.get_integer(field_name);
        }
    }

    fn get_string(&mut self, field_name: TableNameAndFieldName) -> Option<String> {
        if self.left_scan.has_field(field_name.clone()) {
            return self.left_scan.get_string(field_name);
        } else {
            return self.right_scan.get_string(field_name);
        }
    }

    fn get_value(&mut self, field_name: TableNameAndFieldName) -> Option<ConstantValue> {
        if self.left_scan.has_field(field_name.clone()) {
            return self.left_scan.get_value(field_name);
        } else {
            return self.right_scan.get_value(field_name);
        }
    }

    fn has_field(&self, field_name: TableNameAndFieldName) -> bool {
        self.left_scan.has_field(field_name.clone()) || self.right_scan.has_field(field_name)
    }

    fn close(&mut self) {
        self.left_scan.close();
        self.right_scan.close();
    }

    fn delete(&mut self) {}

    fn get_record_id(&self) -> RecordID {
        panic!("get_record_id not implemented for ProductScan");
    }

    fn insert(&mut self) {
        panic!("insert not implemented for ProductScan");
    }

    fn move_to_record_id(&mut self, record_id: RecordID) {
        panic!("move_to_record_id not implemented for ProductScan");
    }

    fn set_integer(&mut self, field_name: String, value: i32) {
        panic!("set_integer not implemented for ProductScan");
    }

    fn set_string(&mut self, field_name: String, value: String) {
        panic!("set_string not implemented for ProductScan");
    }

    fn set_value(&mut self, field_name: String, value: ConstantValue) {
        panic!("set_value not implemented for ProductScan");
    }
}

#[cfg(test)]
mod tests {
    use std::{
        cell::RefCell,
        path::Path,
        rc::Rc,
        sync::{Arc, Mutex},
    };

    use crate::{
        buffer_manager_v2::BufferManagerV2,
        concurrency_manager::LockTable,
        file_manager::{self, FileManager},
        log_manager_v2::LogManagerV2,
        predicate::{Constant, ConstantValue, ExpressionValue, TableNameAndFieldName},
        predicate_v3::{ExpressionV2, PredicateV2, TermV2},
        record_page::{Layout, TableSchema},
        scan_v2::{ScanV2, SelectScanV2},
        table_manager_v2::TableManagerV2,
        table_scan_v2::TableScan,
        transaction_v2::TransactionV2,
        view_manager::ViewManager,
    };

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

        let table_manager = Rc::new(RefCell::new(TableManagerV2::new(transaction.clone(), true)));

        let mut view_manager = ViewManager::new(true, table_manager.clone(), transaction.clone());

        let mut student_table_schema = TableSchema::new();
        student_table_schema.add_string_field("name".to_string(), 10);
        student_table_schema.add_integer_field("age".to_string());

        let student_table_layout = Layout::new(student_table_schema.clone());

        table_manager.borrow_mut().create_table(
            "student".to_string(),
            &student_table_schema,
            transaction.clone(),
        );

        let mut student_table_scan = TableScan::new(
            "student".to_string(),
            transaction.clone(),
            student_table_layout,
        );

        student_table_scan.insert();
        student_table_scan.set_string("name".to_string(), "Alice".to_string());
        student_table_scan.set_integer("age".to_string(), 20);

        student_table_scan.insert();
        student_table_scan.set_string("name".to_string(), "Bob".to_string());
        student_table_scan.set_integer("age".to_string(), 22);

        student_table_scan.insert();
        student_table_scan.set_string("name".to_string(), "John".to_string());
        student_table_scan.set_integer("age".to_string(), 20);

        student_table_scan.move_to_before_first();

        let lhs_expression = ExpressionV2::new(ExpressionValue::TableNameAndFieldName(
            TableNameAndFieldName::new(None, "age".to_string()),
        ));
        let rhs_expression = ExpressionV2::new(ExpressionValue::Constant(Constant::new(
            ConstantValue::Number(20),
        )));
        let terms = vec![TermV2::new(lhs_expression, rhs_expression)];
        let predicate = PredicateV2::new(terms);

        let mut student_table_select_scan =
            SelectScanV2::new(Box::new(student_table_scan), predicate);

        assert_eq!(student_table_select_scan.next().unwrap(), true);
        let name = student_table_select_scan
            .get_string(TableNameAndFieldName::new(None, "name".to_string()))
            .unwrap();
        let age = student_table_select_scan
            .get_integer(TableNameAndFieldName::new(None, "age".to_string()))
            .unwrap();

        assert_eq!(name, "Alice".to_string());
        assert_eq!(age, 20);

        assert_eq!(student_table_select_scan.next().unwrap(), true);
        let name = student_table_select_scan
            .get_string(TableNameAndFieldName::new(None, "name".to_string()))
            .unwrap();
        let age = student_table_select_scan
            .get_integer(TableNameAndFieldName::new(None, "age".to_string()))
            .unwrap();

        assert_eq!(name, "John".to_string());
        assert_eq!(age, 20);

        println!("name: {}, age: {}", name, age);
    }
}
