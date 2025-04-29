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
    fn move_to_before_first(&mut self);
    fn next(
        &mut self,
        slot_id: i32,
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
    fn move_to_before_first(&mut self) {
        self.scan.move_to_before_first();
    }

    fn next(
        &mut self,
        slot_id: i32,
        file_manager: &mut FileManager,
        buffer_list: &mut BufferList,
        transaction: &mut Transaction,
    ) -> bool {
        while self
            .scan
            .next(slot_id, file_manager, buffer_list, transaction)
        {
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
