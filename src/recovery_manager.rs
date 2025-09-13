use crate::{
    block::{self, BlockId},
    buffer_manager::{self, Buffer, BufferList, BufferManager},
    file_manager::{self, FileManager},
    log_manager,
    page::Page,
    transaction::{self, Transaction},
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
    fn undo(
        &self,
        transaction: &mut Transaction,
        file_manager: &mut FileManager,
        buffer_list: &mut BufferList,
        buffer_manager: &mut BufferManager,
    );
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
        let mut offset = Page::get_integer_byte_size();
        let transaction_id = page.get_integer(offset);
        offset += Page::get_integer_byte_size();
        let filename = page.get_string(offset);
        offset += String::len(&filename) + Page::get_integer_byte_size();
        let block_number = page.get_integer(offset);
        let block_id = BlockId::new(filename, block_number as u64);

        offset += Page::get_integer_byte_size();
        let offset_value = page.get_integer(offset) as usize;
        offset += Page::get_integer_byte_size();
        let value = page.get_string(offset);

        SetStringRecord {
            transaction_id,
            block_id,
            offset: offset_value,
            value,
        }
    }

    fn write_to_log(
        log_manager: &mut log_manager::LogManager,
        file_manager: &mut file_manager::FileManager,
        transaction_id: i32,
        block_id: &BlockId,
        offset: usize,
        value: &str,
    ) -> i32 {
        let transaction_id_offset = Page::get_integer_byte_size();
        let filename_offset = transaction_id_offset + Page::get_integer_byte_size();
        let block_number_offset =
            filename_offset + block_id.get_file_name().len() + Page::get_integer_byte_size();
        let offset_value_offset = block_number_offset + Page::get_integer_byte_size();
        let value_offset = offset_value_offset + Page::get_integer_byte_size();
        let record_length = value_offset + value.len() + Page::get_integer_byte_size();

        let mut page = Page::new(record_length);
        page.set_integer(0, LogRecordType::SETSTRING as i32);
        page.set_integer(transaction_id_offset, transaction_id);
        page.set_string(filename_offset, block_id.get_file_name());
        page.set_integer(block_number_offset, block_id.get_block_number() as i32);
        page.set_integer(offset_value_offset, offset as i32);
        page.set_string(value_offset, value);
        let lsn = log_manager.append_record(page.get_data(), file_manager);

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

    fn undo(
        &self,
        transaction: &mut Transaction,
        file_manager: &mut FileManager,
        buffer_list: &mut BufferList,
        buffer_manager: &mut BufferManager,
    ) {
        transaction.pin(
            file_manager,
            buffer_list,
            buffer_manager,
            self.block_id.clone(),
        );
        transaction.set_string(buffer_list, self.block_id.clone(), self.offset, &self.value);

        transaction.unpin(buffer_list, buffer_manager, self.block_id.clone());
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
        let mut offset = Page::get_integer_byte_size();
        let transaction_id = page.get_integer(offset);
        offset += Page::get_integer_byte_size();
        let filename = page.get_string(offset);
        offset += String::len(&filename) + Page::get_integer_byte_size();
        let block_number = page.get_integer(offset);
        let block_id = BlockId::new(filename, block_number as u64);

        offset += Page::get_integer_byte_size();
        let offset_value = page.get_integer(offset) as usize;
        offset += Page::get_integer_byte_size();
        let value = page.get_integer(offset);

        SetIntegerRecord {
            transaction_id,
            block_id,
            offset: offset_value,
            value,
        }
    }

    fn write_to_log(
        log_manager: &mut log_manager::LogManager,
        file_manager: &mut file_manager::FileManager,
        transaction_id: i32,
        block_id: &BlockId,
        offset: usize,
        value: i32,
    ) -> i32 {
        let transaction_id_offset = Page::get_integer_byte_size();
        let filename_offset = transaction_id_offset + Page::get_integer_byte_size();
        let block_number_offset =
            filename_offset + block_id.get_file_name().len() + Page::get_integer_byte_size();
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
        let lsn = log_manager.append_record(page.get_data(), file_manager);

        return lsn;
    }
}

impl LogRecord for SetIntegerRecord {
    fn operator_code(&self) -> LogRecordType {
        LogRecordType::SETSTRING
    }

    fn transaction_id(&self) -> i32 {
        self.transaction_id
    }

    fn undo(
        &self,
        transaction: &mut Transaction,
        file_manager: &mut FileManager,
        buffer_list: &mut BufferList,
        buffer_manager: &mut BufferManager,
    ) {
        transaction.pin(
            file_manager,
            buffer_list,
            buffer_manager,
            self.block_id.clone(),
        );
        transaction.set_integer(buffer_list, self.block_id.clone(), self.offset, self.value);

        transaction.unpin(buffer_list, buffer_manager, self.block_id.clone());
    }
}

struct CheckpointRecord {}

impl CheckpointRecord {
    pub fn new() -> Self {
        CheckpointRecord {}
    }

    fn write_to_log(
        log_manager: &mut log_manager::LogManager,
        file_manager: &mut file_manager::FileManager,
    ) -> i32 {
        let mut page = Page::new(Page::get_integer_byte_size());
        page.set_integer(0, LogRecordType::SETSTRING as i32);
        let lsn = log_manager.append_record(page.get_data(), file_manager);

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

    fn undo(
        &self,
        transaction: &mut Transaction,
        file_manager: &mut FileManager,
        buffer_list: &mut BufferList,
        buffer_manager: &mut BufferManager,
    ) {
    }
}

struct StartRecord {
    transaction_id: i32,
}

impl StartRecord {
    pub fn new(page: Page) -> Self {
        let mut offset = Page::get_integer_byte_size();
        let transaction_id = page.get_integer(offset);
        StartRecord { transaction_id }
    }

    fn write_to_log(
        log_manager: &mut log_manager::LogManager,
        file_manager: &mut file_manager::FileManager,
        transaction_id: i32,
    ) -> i32 {
        let mut page = Page::new(Page::get_integer_byte_size());
        page.set_integer(0, LogRecordType::START as i32);
        page.set_integer(Page::get_integer_byte_size(), transaction_id);
        let lsn = log_manager.append_record(page.get_data(), file_manager);

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

    fn undo(
        &self,
        transaction: &mut Transaction,
        file_manager: &mut FileManager,
        buffer_list: &mut BufferList,
        buffer_manager: &mut BufferManager,
    ) {
        // No action needed for START record
    }
}

struct CommitRecord {
    transaction_id: i32,
}

impl CommitRecord {
    pub fn new(page: Page) -> Self {
        let mut offset = Page::get_integer_byte_size();
        let transaction_id = page.get_integer(offset);
        CommitRecord { transaction_id }
    }

    fn write_to_log(
        log_manager: &mut log_manager::LogManager,
        file_manager: &mut file_manager::FileManager,
        transaction_id: i32,
    ) -> i32 {
        let mut page = Page::new(Page::get_integer_byte_size());
        page.set_integer(0, LogRecordType::COMMIT as i32);
        page.set_integer(Page::get_integer_byte_size(), transaction_id);
        let lsn = log_manager.append_record(page.get_data(), file_manager);

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

    fn undo(
        &self,
        transaction: &mut Transaction,
        file_manager: &mut FileManager,
        buffer_list: &mut BufferList,
        buffer_manager: &mut BufferManager,
    ) {
        // No action needed for COMMIT record
    }
}
struct RollbackRecord {
    transaction_id: i32,
}
impl RollbackRecord {
    pub fn new(page: Page) -> Self {
        let mut offset = Page::get_integer_byte_size();
        let transaction_id = page.get_integer(offset);
        RollbackRecord { transaction_id }
    }

    fn write_to_log(
        log_manager: &mut log_manager::LogManager,
        file_manager: &mut file_manager::FileManager,
        transaction_id: i32,
    ) -> i32 {
        let mut page = Page::new(Page::get_integer_byte_size());
        page.set_integer(0, LogRecordType::ROLLBACK as i32);
        page.set_integer(Page::get_integer_byte_size(), transaction_id);
        let lsn = log_manager.append_record(page.get_data(), file_manager);

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

    fn undo(
        &self,
        transaction: &mut Transaction,
        file_manager: &mut FileManager,
        buffer_list: &mut BufferList,
        buffer_manager: &mut BufferManager,
    ) {
        // No action needed for ROLLBACK record
    }
}

struct RecoveryManager {
    transaction_number: i32,
}

impl RecoveryManager {
    fn new(transaction_number: i32) -> Self {
        RecoveryManager { transaction_number }
    }

    fn commit(
        &self,
        buffer_manager: &mut BufferManager,
        file_manager: &mut FileManager,
        log_manager: &mut log_manager::LogManager,
    ) {
        buffer_manager.flush_all(file_manager, self.transaction_number);

        let lsn = CommitRecord::write_to_log(
            &mut log_manager::LogManager::new(file_manager, "log_file".to_string()),
            file_manager,
            self.transaction_number,
        );

        log_manager.flush_with_lsn(file_manager, lsn);
    }

    fn do_rollback(
        &self,
        transaction: &mut Transaction,
        buffer_manager: &mut BufferManager,
        buffer_list: &mut BufferList,
        file_manager: &mut FileManager,
        log_manager: &mut log_manager::LogManager,
    ) {
        let mut iterator = log_manager.iterator(file_manager);

        while iterator.has_next(&file_manager) {
            let bytes = iterator.next(file_manager);
            let log_record = create_log_record(bytes);
            if log_record.transaction_id() == self.transaction_number {
                if log_record.operator_code() == LogRecordType::START {
                    return;
                }
                log_record.undo(transaction, file_manager, buffer_list, buffer_manager);
            }
        }
    }

    fn rollback(
        &self,
        transaction: &mut Transaction,
        buffer_manager: &mut BufferManager,
        buffer_list: &mut BufferList,
        file_manager: &mut FileManager,
        log_manager: &mut log_manager::LogManager,
    ) {
        self.do_rollback(
            transaction,
            buffer_manager,
            buffer_list,
            file_manager,
            log_manager,
        );

        buffer_manager.flush_all(file_manager, self.transaction_number);

        let lsn = RollbackRecord::write_to_log(log_manager, file_manager, self.transaction_number);

        log_manager.flush_with_lsn(file_manager, lsn);
    }

    fn do_recover(
        &self,
        transaction: &mut Transaction,
        buffer_manager: &mut BufferManager,
        buffer_list: &mut BufferList,
        file_manager: &mut FileManager,
        log_manager: &mut log_manager::LogManager,
    ) {
        let mut finished_transactions = vec![];

        let mut iterator = log_manager.iterator(file_manager);

        while iterator.has_next(file_manager) {
            let bytes = iterator.next(file_manager);
            let log_record = create_log_record(bytes);

            if log_record.operator_code() == LogRecordType::CHECKPOINT {
                return;
            }

            if log_record.operator_code() == LogRecordType::COMMIT
                || log_record.operator_code() == LogRecordType::ROLLBACK
            {
                finished_transactions.push(log_record.transaction_id());
            } else if !finished_transactions.contains(&log_record.transaction_id()) {
                log_record.undo(transaction, file_manager, buffer_list, buffer_manager);
            }
        }
    }

    fn recover(
        &self,
        transaction: &mut Transaction,
        buffer_manager: &mut BufferManager,
        buffer_list: &mut BufferList,
        file_manager: &mut FileManager,
        log_manager: &mut log_manager::LogManager,
    ) {
        self.do_recover(
            transaction,
            buffer_manager,
            buffer_list,
            file_manager,
            log_manager,
        );

        buffer_manager.flush_all(file_manager, self.transaction_number);

        let lsn = CheckpointRecord::write_to_log(log_manager, file_manager);

        log_manager.flush_with_lsn(file_manager, lsn);
    }

    fn set_integer(
        &self,
        offset: usize,
        file_manager: &mut FileManager,
        log_manager: &mut log_manager::LogManager,
        buffer: &mut Buffer,
    ) -> i32 {
        let old_value = buffer.content().get_integer(offset);

        let block = buffer.block_id().as_ref().unwrap().clone();

        let lsn = SetIntegerRecord::write_to_log(
            log_manager,
            file_manager,
            self.transaction_number,
            &block,
            offset,
            old_value,
        );

        return lsn;
    }

    fn set_string(
        &self,
        offset: usize,
        file_manager: &mut FileManager,
        log_manager: &mut log_manager::LogManager,
        buffer: &mut Buffer,
    ) -> i32 {
        let old_value = buffer.content().get_string(offset);

        let block = buffer.block_id().as_ref().unwrap().clone();

        let lsn = SetStringRecord::write_to_log(
            log_manager,
            file_manager,
            self.transaction_number,
            &block,
            offset,
            &old_value,
        );

        return lsn;
    }
}
