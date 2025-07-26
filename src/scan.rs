use core::slice;

use crate::{
    buffer_manager::{BufferList, BufferManager},
    file_manager::FileManager,
    predicate::{ConstantValue, Predicate},
    record_page::{self, TableSchema},
    table_scan::{RecordID, TableScan},
    transaction::Transaction,
};

pub trait Scan {
    fn move_to_before_first(
        &mut self,
        file_manager: &mut FileManager,
        buffer_list: &mut BufferList,
        transaction: &mut Transaction,
    );
    fn next(
        &mut self,
        file_manager: &mut FileManager,
        buffer_list: &mut BufferList,
        transaction: &mut Transaction,
    ) -> bool;
    fn get_integer(
        &mut self,
        transaction: &mut Transaction,
        buffer_list: &mut BufferList,
        field_name: String,
    ) -> Option<i32>;
    fn get_string(
        &mut self,
        transaction: &mut Transaction,
        buffer_list: &mut BufferList,
        field_name: String,
    ) -> Option<String>;
    fn get_value(
        &mut self,
        transaction: &mut Transaction,
        buffer_list: &mut BufferList,
        field_name: String,
    ) -> ConstantValue;
    fn close(
        &mut self,
        transaction: &mut Transaction,
        buffer_list: &mut BufferList,
        buffer_manager: &mut BufferManager,
    );
    fn has_field(&self, field_name: String) -> bool;
    fn set_integer(
        &mut self,
        transaction: &mut Transaction,
        buffer_list: &mut BufferList,
        field_name: String,
        value: i32,
    );
    fn set_string(
        &mut self,
        transaction: &mut Transaction,
        buffer_list: &mut BufferList,
        field_name: String,
        value: String,
    );
    fn set_value(
        &mut self,
        transaction: &mut Transaction,
        buffer_list: &mut BufferList,
        field_name: String,
        value: ConstantValue,
    );

    fn insert(
        &mut self,
        transaction: &mut Transaction,
        buffer_list: &mut BufferList,
        file_manager: &mut FileManager,
        layout: record_page::Layout,
    );
    fn delete(&mut self);

    fn get_record_id(&self) -> RecordID;
    fn move_to_record_id(&mut self, layout: record_page::Layout, record_id: RecordID);
}

// pub trait UpdateScan: Scan {
//     fn set_integer(
//         &mut self,
//         transaction: &mut Transaction,
//         buffer_list: &mut BufferList,
//         field_name: String,
//         value: i32,
//     );
//     fn set_string(
//         &mut self,
//         transaction: &mut Transaction,
//         buffer_list: &mut BufferList,
//         field_name: String,
//         value: String,
//     );
//     fn insert(
//         &mut self,
//         transaction: &mut Transaction,
//         buffer_list: &mut BufferList,
//         file_manager: &mut FileManager,
//         layout: record_page::Layout,
//     );
//     fn delete(&mut self);

//     fn get_record_id(&self) -> RecordID;
//     fn move_to_record_id(&mut self, layout: record_page::Layout, record_id: RecordID);
// }

pub struct SelectScan {
    scan: Box<dyn Scan>,
    predicate: Predicate,
}

impl SelectScan {
    pub fn new(scan: Box<dyn Scan>, predicate: Predicate) -> Self {
        SelectScan { scan, predicate }
    }
}

impl Scan for SelectScan {
    fn move_to_before_first(
        &mut self,
        file_manager: &mut FileManager,
        buffer_list: &mut BufferList,
        transaction: &mut Transaction,
    ) {
        self.scan
            .move_to_before_first(file_manager, buffer_list, transaction);
    }

    fn next(
        &mut self,
        file_manager: &mut FileManager,

        buffer_list: &mut BufferList,
        transaction: &mut Transaction,
    ) -> bool {
        while self.scan.next(file_manager, buffer_list, transaction) {
            if self
                .predicate
                .is_satisfied(&mut *self.scan, transaction, buffer_list)
            {
                return true;
            }
        }
        return false;
    }

    fn get_integer(
        &mut self,
        transaction: &mut Transaction,
        buffer_list: &mut BufferList,
        field_name: String,
    ) -> Option<i32> {
        self.scan.get_integer(transaction, buffer_list, field_name)
    }

    fn get_string(
        &mut self,
        transaction: &mut Transaction,
        buffer_list: &mut BufferList,
        field_name: String,
    ) -> Option<String> {
        self.scan.get_string(transaction, buffer_list, field_name)
    }
    fn get_value(
        &mut self,
        transaction: &mut Transaction,
        buffer_list: &mut BufferList,
        field_name: String,
    ) -> ConstantValue {
        self.scan.get_value(transaction, buffer_list, field_name)
    }

    fn close(
        &mut self,
        transaction: &mut Transaction,
        buffer_list: &mut BufferList,
        buffer_manager: &mut BufferManager,
    ) {
        self.scan.close(transaction, buffer_list, buffer_manager);
    }

    fn has_field(&self, field_name: String) -> bool {
        self.scan.has_field(field_name)
    }

    fn set_integer(
        &mut self,
        transaction: &mut Transaction,
        buffer_list: &mut BufferList,
        field_name: String,
        value: i32,
    ) {
        self.scan
            .set_integer(transaction, buffer_list, field_name, value);
    }

    fn set_string(
        &mut self,
        transaction: &mut Transaction,
        buffer_list: &mut BufferList,
        field_name: String,
        value: String,
    ) {
        self.scan
            .set_string(transaction, buffer_list, field_name, value);
    }

    fn set_value(
        &mut self,
        transaction: &mut Transaction,
        buffer_list: &mut BufferList,
        field_name: String,
        value: ConstantValue,
    ) {
        self.scan
            .set_value(transaction, buffer_list, field_name, value);
    }

    fn delete(&mut self) {
        self.scan.delete();
    }

    fn insert(
        &mut self,
        transaction: &mut Transaction,
        buffer_list: &mut BufferList,
        file_manager: &mut FileManager,
        layout: record_page::Layout,
    ) {
        self.scan
            .insert(transaction, buffer_list, file_manager, layout);
    }

    fn get_record_id(&self) -> RecordID {
        self.scan.get_record_id()
    }

    fn move_to_record_id(&mut self, layout: record_page::Layout, record_id: RecordID) {
        self.scan.move_to_record_id(layout, record_id);
    }
}

pub struct ProductScan {
    left_scan: Box<dyn Scan>,
    right_scan: Box<dyn Scan>,
}

impl ProductScan {
    pub fn new(left_scan: Box<dyn Scan>, right_scan: Box<dyn Scan>) -> Self {
        ProductScan {
            left_scan,
            right_scan,
        }
    }
}

impl Scan for ProductScan {
    fn move_to_before_first(
        &mut self,
        file_manager: &mut FileManager,
        buffer_list: &mut BufferList,
        transaction: &mut Transaction,
    ) {
        self.left_scan
            .move_to_before_first(file_manager, buffer_list, transaction);
        self.left_scan.next(file_manager, buffer_list, transaction);
        self.right_scan
            .move_to_before_first(file_manager, buffer_list, transaction);
    }

    fn next(
        &mut self,
        file_manager: &mut FileManager,
        buffer_list: &mut BufferList,
        transaction: &mut Transaction,
    ) -> bool {
        if self.right_scan.next(file_manager, buffer_list, transaction) {
            return true;
        }
        self.right_scan
            .move_to_before_first(file_manager, buffer_list, transaction);
        return self.right_scan.next(file_manager, buffer_list, transaction)
            && self.left_scan.next(file_manager, buffer_list, transaction);
    }

    fn get_integer(
        &mut self,
        transaction: &mut Transaction,
        buffer_list: &mut BufferList,
        field_name: String,
    ) -> Option<i32> {
        if self.left_scan.has_field(field_name.clone()) {
            return self
                .left_scan
                .get_integer(transaction, buffer_list, field_name);
        } else {
            return self
                .right_scan
                .get_integer(transaction, buffer_list, field_name);
        }
    }

    fn get_string(
        &mut self,
        transaction: &mut Transaction,
        buffer_list: &mut BufferList,
        field_name: String,
    ) -> Option<String> {
        if self.left_scan.has_field(field_name.clone()) {
            return self
                .left_scan
                .get_string(transaction, buffer_list, field_name);
        } else {
            return self
                .right_scan
                .get_string(transaction, buffer_list, field_name);
        }
    }

    fn get_value(
        &mut self,
        transaction: &mut Transaction,
        buffer_list: &mut BufferList,
        field_name: String,
    ) -> ConstantValue {
        if self.left_scan.has_field(field_name.clone()) {
            return self
                .left_scan
                .get_value(transaction, buffer_list, field_name);
        } else {
            return self
                .right_scan
                .get_value(transaction, buffer_list, field_name);
        }
    }

    fn has_field(&self, field_name: String) -> bool {
        self.left_scan.has_field(field_name.clone()) || self.right_scan.has_field(field_name)
    }

    fn close(
        &mut self,
        transaction: &mut Transaction,
        buffer_list: &mut BufferList,
        buffer_manager: &mut BufferManager,
    ) {
        self.left_scan
            .close(transaction, buffer_list, buffer_manager);
        self.right_scan
            .close(transaction, buffer_list, buffer_manager);
    }

    fn delete(&mut self) {}

    fn get_record_id(&self) -> RecordID {
        panic!("get_record_id not implemented for ProductScan");
    }

    fn insert(
        &mut self,
        transaction: &mut Transaction,
        buffer_list: &mut BufferList,
        file_manager: &mut FileManager,
        layout: record_page::Layout,
    ) {
        panic!("insert not implemented for ProductScan");
    }

    fn move_to_record_id(&mut self, layout: record_page::Layout, record_id: RecordID) {
        panic!("move_to_record_id not implemented for ProductScan");
    }

    fn set_integer(
        &mut self,
        transaction: &mut Transaction,
        buffer_list: &mut BufferList,
        field_name: String,
        value: i32,
    ) {
        panic!("set_integer not implemented for ProductScan");
    }

    fn set_string(
        &mut self,
        transaction: &mut Transaction,
        buffer_list: &mut BufferList,
        field_name: String,
        value: String,
    ) {
        panic!("set_string not implemented for ProductScan");
    }

    fn set_value(
        &mut self,
        transaction: &mut Transaction,
        buffer_list: &mut BufferList,
        field_name: String,
        value: ConstantValue,
    ) {
        panic!("set_value not implemented for ProductScan");
    }
}
