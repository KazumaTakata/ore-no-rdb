use std::{cell::RefCell, rc::Rc};

use crate::{
    block::{self, BlockId},
    buffer_manager::{self, Buffer, BufferList, BufferManager},
    buffer_manager_v2::BufferManagerV2,
    file_manager::{self, FileManager},
    log_manager_v2::{self, LogManagerV2},
    page::Page,
    transaction::{self, Transaction},
    transaction_v2::TransactionV2,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LogRecordType {
    CHECKPOINT = 0,
    START = 1,
    COMMIT = 2,
    ROLLBACK = 3,
    SETINT = 4,
    SETSTRING = 5,
}

impl LogRecordType {
    fn from(value: i32) -> LogRecordType {
        match value {
            0 => LogRecordType::CHECKPOINT,
            1 => LogRecordType::START,
            2 => LogRecordType::COMMIT,
            3 => LogRecordType::ROLLBACK,
            4 => LogRecordType::SETINT,
            5 => LogRecordType::SETSTRING,
            _ => panic!("Invalid log record type"),
        }
    }
}

pub trait LogRecord {
    fn operator_code(&self) -> LogRecordType;
    fn transaction_id(&self) -> i32;
    fn undo(&self, transaction: &mut TransactionV2);
}

fn create_log_record(bytes: Vec<u8>) -> Box<dyn LogRecord> {
    let page = Page::from(bytes);

    let record_type = LogRecordType::from(page.get_integer(0));

    match record_type {
        LogRecordType::CHECKPOINT => {
            return Box::new(CheckpointRecord::new());
        }
        LogRecordType::START => {
            return Box::new(StartRecord::new(page));
        }
        LogRecordType::COMMIT => {
            return Box::new(CommitRecord::new(page));
        }
        LogRecordType::ROLLBACK => {
            return Box::new(RollbackRecord::new(page));
        }
        LogRecordType::SETINT => {
            return Box::new(SetIntegerRecord::new(page));
        }
        LogRecordType::SETSTRING => {
            return Box::new(SetStringRecord::new(page));
        }
    }
}

struct SetStringRecord {
    transaction_id: i32,
    block_id: BlockId,
    offset: usize,
    value: String,
}

impl SetStringRecord {
    pub fn new(page: Page) -> Self {
        let transaction_id_offset = Page::get_integer_byte_size();
        let transaction_id = page.get_integer(transaction_id_offset);
        let file_name_offset = transaction_id_offset + Page::get_integer_byte_size();
        let filename = page.get_string(file_name_offset);
        let block_number_offset = file_name_offset + Page::get_max_length(filename.len() as u32);

        let block_number = page.get_integer(block_number_offset);

        let block_id = BlockId::new(filename, block_number as u64);

        let offset_offset = block_number_offset + Page::get_integer_byte_size();
        let offset_value = page.get_integer(offset_offset) as usize;

        let value_offset = offset_offset + Page::get_integer_byte_size();
        let value = page.get_string(value_offset);

        SetStringRecord {
            transaction_id,
            block_id,
            offset: offset_value,
            value,
        }
    }

    fn write_to_log(
        log_manager: &mut LogManagerV2,
        transaction_id: i32,
        block_id: &BlockId,
        offset: usize,
        value: &str,
    ) -> i32 {
        let transaction_id_offset = Page::get_integer_byte_size();
        let filename_offset = transaction_id_offset + Page::get_integer_byte_size();
        let block_number_offset =
            filename_offset + Page::get_max_length(block_id.get_file_name().len() as u32);
        let offset_value_offset = block_number_offset + Page::get_integer_byte_size();
        let value_offset = offset_value_offset + Page::get_integer_byte_size();
        let record_length = value_offset + Page::get_max_length(value.len() as u32);

        let mut page = Page::new(record_length);
        page.set_integer(0, LogRecordType::SETSTRING as i32);
        page.set_integer(transaction_id_offset, transaction_id);
        page.set_string(filename_offset, block_id.get_file_name());
        page.set_integer(block_number_offset, block_id.get_block_number() as i32);
        page.set_integer(offset_value_offset, offset as i32);
        page.set_string(value_offset, value);
        let lsn = log_manager.append_record(page.get_data());

        return lsn;
    }
}

impl LogRecord for SetStringRecord {
    fn operator_code(&self) -> LogRecordType {
        LogRecordType::SETSTRING
    }

    fn transaction_id(&self) -> i32 {
        self.transaction_id
    }

    fn undo(&self, transaction: &mut TransactionV2) {
        transaction.pin(self.block_id.clone());
        transaction.set_string(self.block_id.clone(), self.offset, &self.value);
        transaction.unpin(self.block_id.clone());
    }
}

struct SetIntegerRecord {
    transaction_id: i32,
    block_id: BlockId,
    offset: usize,
    value: i32,
}

impl SetIntegerRecord {
    pub fn new(page: Page) -> Self {
        let transaction_id_offset = Page::get_integer_byte_size();
        let transaction_id = page.get_integer(transaction_id_offset);
        let file_name_offset = transaction_id_offset + Page::get_integer_byte_size();
        let filename = page.get_string(file_name_offset);
        let block_number_offset = file_name_offset + Page::get_max_length(filename.len() as u32);

        let block_number = page.get_integer(block_number_offset);

        let block_id = BlockId::new(filename, block_number as u64);

        let offset_offset = block_number_offset + Page::get_integer_byte_size();
        let offset_value = page.get_integer(offset_offset) as usize;

        let value_offset = offset_offset + Page::get_integer_byte_size();
        let value = page.get_integer(value_offset);

        SetIntegerRecord {
            transaction_id,
            block_id,
            offset: offset_value,
            value,
        }
    }

    fn write_to_log(
        log_manager: &mut LogManagerV2,
        transaction_id: i32,
        block_id: &BlockId,
        offset: usize,
        value: i32,
    ) -> i32 {
        let transaction_id_offset = Page::get_integer_byte_size();
        let filename_offset = transaction_id_offset + Page::get_integer_byte_size();
        let block_number_offset =
            filename_offset + Page::get_max_length(block_id.get_file_name().len() as u32);
        let offset_value_offset = block_number_offset + Page::get_integer_byte_size();
        let value_offset = offset_value_offset + Page::get_integer_byte_size();
        let record_length = value_offset + Page::get_integer_byte_size();

        let mut page = Page::new(record_length);
        page.set_integer(0, LogRecordType::SETSTRING as i32);
        page.set_integer(transaction_id_offset, transaction_id);
        page.set_string(filename_offset, block_id.get_file_name());
        page.set_integer(block_number_offset, block_id.get_block_number() as i32);
        page.set_integer(offset_value_offset, offset as i32);
        page.set_integer(value_offset, value);
        let lsn = log_manager.append_record(page.get_data());

        return lsn;
    }
}

impl LogRecord for SetIntegerRecord {
    fn operator_code(&self) -> LogRecordType {
        LogRecordType::SETINT
    }

    fn transaction_id(&self) -> i32 {
        self.transaction_id
    }

    fn undo(&self, transaction: &mut TransactionV2) {
        transaction.pin(self.block_id.clone());
        transaction.set_integer(self.block_id.clone(), self.offset, self.value);
        transaction.unpin(self.block_id.clone());
    }
}

struct CheckpointRecord {}

impl CheckpointRecord {
    pub fn new() -> Self {
        CheckpointRecord {}
    }

    fn write_to_log(log_manager: &mut LogManagerV2) -> i32 {
        let mut page = Page::new(Page::get_integer_byte_size());
        page.set_integer(0, LogRecordType::CHECKPOINT as i32);
        let lsn = log_manager.append_record(page.get_data());
        return lsn;
    }
}

impl LogRecord for CheckpointRecord {
    fn operator_code(&self) -> LogRecordType {
        LogRecordType::CHECKPOINT
    }

    fn transaction_id(&self) -> i32 {
        return -1;
    }

    fn undo(&self, _transaction: &mut TransactionV2) {}
}

struct StartRecord {
    transaction_id: i32,
}

impl StartRecord {
    pub fn new(page: Page) -> Self {
        let offset = Page::get_integer_byte_size();
        let transaction_id = page.get_integer(offset);
        StartRecord { transaction_id }
    }

    fn write_to_log(log_manager: &mut LogManagerV2, transaction_id: i32) -> i32 {
        let mut page = Page::new(Page::get_integer_byte_size() * 2);
        page.set_integer(0, LogRecordType::START as i32);
        page.set_integer(Page::get_integer_byte_size(), transaction_id);
        let lsn = log_manager.append_record(page.get_data());
        return lsn;
    }
}

impl LogRecord for StartRecord {
    fn operator_code(&self) -> LogRecordType {
        LogRecordType::START
    }

    fn transaction_id(&self) -> i32 {
        self.transaction_id
    }

    fn undo(&self, _transaction: &mut TransactionV2) {
        // No action needed for START record
    }
}

struct CommitRecord {
    transaction_id: i32,
}

impl CommitRecord {
    pub fn new(page: Page) -> Self {
        let offset = Page::get_integer_byte_size();
        let transaction_id = page.get_integer(offset);
        CommitRecord { transaction_id }
    }

    fn write_to_log(log_manager: &mut LogManagerV2, transaction_id: i32) -> i32 {
        let mut page = Page::new(Page::get_integer_byte_size() * 2);
        page.set_integer(0, LogRecordType::COMMIT as i32);
        page.set_integer(Page::get_integer_byte_size(), transaction_id);
        let lsn = log_manager.append_record(page.get_data());
        return lsn;
    }
}

impl LogRecord for CommitRecord {
    fn operator_code(&self) -> LogRecordType {
        LogRecordType::COMMIT
    }

    fn transaction_id(&self) -> i32 {
        self.transaction_id
    }

    fn undo(&self, _transaction: &mut TransactionV2) {
        // No action needed for COMMIT record
    }
}
struct RollbackRecord {
    transaction_id: i32,
}
impl RollbackRecord {
    pub fn new(page: Page) -> Self {
        let offset = Page::get_integer_byte_size();
        let transaction_id = page.get_integer(offset);
        RollbackRecord { transaction_id }
    }

    fn write_to_log(log_manager: &mut LogManagerV2, transaction_id: i32) -> i32 {
        let mut page = Page::new(Page::get_integer_byte_size() * 2);
        page.set_integer(0, LogRecordType::ROLLBACK as i32);
        page.set_integer(Page::get_integer_byte_size(), transaction_id);
        let lsn = log_manager.append_record(page.get_data());

        return lsn;
    }
}
impl LogRecord for RollbackRecord {
    fn operator_code(&self) -> LogRecordType {
        LogRecordType::ROLLBACK
    }

    fn transaction_id(&self) -> i32 {
        self.transaction_id
    }

    fn undo(&self, transaction: &mut TransactionV2) {
        // No action needed for ROLLBACK record
    }
}

struct RecoveryManager {
    transaction: Rc<RefCell<TransactionV2>>,
    transaction_number: i32,
    buffer_manager: Rc<RefCell<BufferManagerV2>>,
    log_manager: Rc<RefCell<LogManagerV2>>,
}

impl RecoveryManager {
    fn new(
        transaction: Rc<RefCell<TransactionV2>>,
        transaction_number: i32,
        buffer_manager: Rc<RefCell<BufferManagerV2>>,
        log_manager: Rc<RefCell<LogManagerV2>>,
    ) -> Self {
        let _ = StartRecord::write_to_log(&mut log_manager.borrow_mut(), transaction_number);

        RecoveryManager {
            transaction,
            transaction_number,
            buffer_manager,
            log_manager,
        }
    }

    fn commit(&self) {
        self.buffer_manager
            .borrow_mut()
            .flush_all(self.transaction_number);
        let lsn =
            CommitRecord::write_to_log(&mut self.log_manager.borrow_mut(), self.transaction_number);
        self.log_manager.borrow_mut().flush_with_lsn(lsn);
    }

    fn rollback(&self) {
        self.do_rollback();
        self.buffer_manager
            .borrow_mut()
            .flush_all(self.transaction_number);
        let lsn = RollbackRecord::write_to_log(
            &mut self.log_manager.borrow_mut(),
            self.transaction_number,
        );
        self.log_manager.borrow_mut().flush_with_lsn(lsn);
    }

    fn do_rollback(&self) {
        let mut iterator = self.log_manager.borrow_mut().iterator();
        while iterator.has_next() {
            let bytes = iterator.next();
            let log_record = create_log_record(bytes);
            if log_record.transaction_id() == self.transaction_number {
                if log_record.operator_code() == LogRecordType::START {
                    return;
                }
                log_record.undo(&mut self.transaction.borrow_mut());
            }
        }
    }

    fn do_recover(&self) {
        let mut finished_transactions = vec![];

        let mut iterator = self.log_manager.borrow_mut().iterator();
        while iterator.has_next() {
            let bytes = iterator.next();
            let log_record = create_log_record(bytes);
            if log_record.operator_code() == LogRecordType::CHECKPOINT {
                return;
            }

            if log_record.operator_code() == LogRecordType::COMMIT
                || log_record.operator_code() == LogRecordType::ROLLBACK
            {
                finished_transactions.push(log_record.transaction_id());
            } else if !finished_transactions.contains(&log_record.transaction_id()) {
                log_record.undo(&mut self.transaction.borrow_mut());
            }
        }
    }

    fn recover(&self) {
        self.do_recover();
        self.buffer_manager
            .borrow_mut()
            .flush_all(self.transaction_number);
        let lsn = CheckpointRecord::write_to_log(&mut self.log_manager.borrow_mut());
        self.log_manager.borrow_mut().flush_with_lsn(lsn);
    }

    fn set_integer(&self, offset: usize, buffer: &mut Buffer) -> i32 {
        let old_value = buffer.content().get_integer(offset);
        let block = buffer.block_id().as_ref().unwrap().clone();
        let lsn = SetIntegerRecord::write_to_log(
            &mut self.log_manager.borrow_mut(),
            self.transaction_number,
            &block,
            offset,
            old_value,
        );
        return lsn;
    }

    fn set_string(&self, offset: usize, buffer: &mut Buffer) -> i32 {
        let old_value = buffer.content().get_string(offset);
        let block = buffer.block_id().as_ref().unwrap().clone();

        let lsn = SetStringRecord::write_to_log(
            &mut self.log_manager.borrow_mut(),
            self.transaction_number,
            &block,
            offset,
            &old_value,
        );

        return lsn;
    }
}
