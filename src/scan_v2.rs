use crate::{predicate::ConstantValue, predicate_v3::PredicateV2, table_scan::RecordID};

pub trait ScanV2 {
    fn move_to_before_first(&mut self);
    fn next(&mut self) -> bool;
    fn get_integer(&mut self, field_name: String) -> Option<i32>;
    fn get_string(&mut self, field_name: String) -> Option<String>;
    fn get_value(&mut self, field_name: String) -> ConstantValue;
    fn close(&mut self);
    fn has_field(&self, field_name: String) -> bool;
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
    fn move_to_before_first(&mut self) {
        self.scan.move_to_before_first();
    }

    fn next(&mut self) -> bool {
        while self.scan.next() {
            if self.predicate.is_satisfied(&mut *self.scan) {
                return true;
            }
        }
        return false;
    }

    fn get_integer(&mut self, field_name: String) -> Option<i32> {
        self.scan.get_integer(field_name)
    }

    fn get_string(&mut self, field_name: String) -> Option<String> {
        self.scan.get_string(field_name)
    }
    fn get_value(&mut self, field_name: String) -> ConstantValue {
        self.scan.get_value(field_name)
    }

    fn close(&mut self) {
        self.scan.close();
    }

    fn has_field(&self, field_name: String) -> bool {
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
    fields: Vec<String>,
}

impl ProjectScanV2 {
    pub fn new(scan: Box<dyn ScanV2>, fields: Vec<String>) -> Self {
        ProjectScanV2 { scan, fields }
    }
}

impl ScanV2 for ProjectScanV2 {
    fn move_to_before_first(&mut self) {
        self.scan.move_to_before_first();
    }

    fn next(&mut self) -> bool {
        self.scan.next()
    }

    fn get_integer(&mut self, field_name: String) -> Option<i32> {
        if self.fields.contains(&field_name) {
            return self.scan.get_integer(field_name);
        }
        None
    }

    fn get_string(&mut self, field_name: String) -> Option<String> {
        if self.fields.contains(&field_name) {
            return self.scan.get_string(field_name);
        }
        None
    }

    fn get_value(&mut self, field_name: String) -> ConstantValue {
        if self.fields.contains(&field_name) {
            return self.scan.get_value(field_name);
        }
        ConstantValue::Null
    }

    fn close(&mut self) {
        self.scan.close();
    }

    fn has_field(&self, field_name: String) -> bool {
        self.fields.contains(&field_name)
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
    fn move_to_before_first(&mut self) {
        self.left_scan.move_to_before_first();
        self.left_scan.next();
        self.right_scan.move_to_before_first();
    }

    fn next(&mut self) -> bool {
        if self.right_scan.next() {
            return true;
        }
        self.right_scan.move_to_before_first();
        return self.right_scan.next() && self.left_scan.next();
    }

    fn get_integer(&mut self, field_name: String) -> Option<i32> {
        if self.left_scan.has_field(field_name.clone()) {
            return self.left_scan.get_integer(field_name);
        } else {
            return self.right_scan.get_integer(field_name);
        }
    }

    fn get_string(&mut self, field_name: String) -> Option<String> {
        if self.left_scan.has_field(field_name.clone()) {
            return self.left_scan.get_string(field_name);
        } else {
            return self.right_scan.get_string(field_name);
        }
    }

    fn get_value(&mut self, field_name: String) -> ConstantValue {
        if self.left_scan.has_field(field_name.clone()) {
            return self.left_scan.get_value(field_name);
        } else {
            return self.right_scan.get_value(field_name);
        }
    }

    fn has_field(&self, field_name: String) -> bool {
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
