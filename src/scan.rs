use crate::{
    predicate::{ConstantValue, Predicate},
    record_page::TableSchema,
    table_scan::TableScan,
};

pub trait Scan {
    fn move_to_before_first(&mut self);
    fn next(&mut self) -> bool;
    fn get_integer(&mut self) -> Option<i32>;
    fn get_string(&mut self) -> Option<String>;
    fn get_value(&mut self, field_name: String) -> ConstantValue;
    fn set_integer(&mut self, value: i32);
    fn set_string(&mut self, value: String);
}

pub struct SelectScan {
    scan: Box<dyn Scan>,
    predicate: Predicate,
}

impl Scan for SelectScan {
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

    fn get_integer(&mut self) -> Option<i32> {
        self.scan.get_integer()
    }

    fn get_string(&mut self) -> Option<String> {
        self.scan.get_string()
    }
    fn get_value(&mut self, field_name: String) -> ConstantValue {
        self.scan.get_value(field_name)
    }

    fn set_integer(&mut self, value: i32) {
        self.scan.set_integer(value);
    }

    fn set_string(&mut self, value: String) {
        self.scan.set_string(value);
    }
}
