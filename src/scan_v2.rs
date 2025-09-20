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
        println!(
            "SelectScanV2: Moving to next record with predicate: {:?}",
            self.predicate
        );
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
        println!("Getting value for field: {:?}", field_name);
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
