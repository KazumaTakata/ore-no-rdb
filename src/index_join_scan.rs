use crate::{
    hash_index::HashIndex,
    predicate::{Constant, TableNameAndFieldName},
    scan_v2::ScanV2,
    table_scan_v2::TableScan,
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
