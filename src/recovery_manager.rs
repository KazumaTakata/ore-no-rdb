use crate::{
    block::{self, BlockId},
    buffer_manager::{BufferList, BufferManager},
    file_manager::{self, FileManager},
    log_manager,
    page::Page,
    transaction::{self, Transaction},
};

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
    fn operator_code(&self) -> i32;
    fn transaction_id(&self) -> i32;
    fn undo(
        &self,
        transaction: &mut Transaction,
        file_manager: &mut FileManager,
        buffer_list: &mut BufferList,
        buffer_manager: &mut BufferManager,
    );

    fn create_log_record(&self, bytes: Vec<u8>) {
        let page = Page::from(bytes);

        let record_type = LogRecordType::from(page.get_integer(0));

        match record_type {
            LogRecordType::CHECKPOINT => {
                // Handle checkpoint record
            }
            LogRecordType::START => {
                // Handle start record
            }
            LogRecordType::COMMIT => {
                // Handle commit record
            }
            LogRecordType::ROLLBACK => {
                // Handle rollback record
            }
            LogRecordType::SETINT => {
                // Handle set int record
            }
            LogRecordType::SETSTRING => {
                // Handle set string record
            }
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
        let mut offset = Page::get_integer_size();
        let transaction_id = page.get_integer(offset);
        offset += Page::get_integer_size();
        let filename = page.get_string(offset);
        offset += String::len(&filename) + Page::get_integer_size();
        let block_number = page.get_integer(offset);
        let block_id = BlockId::new(filename, block_number as u64);

        offset += Page::get_integer_size();
        let offset_value = page.get_integer(offset) as usize;
        offset += Page::get_integer_size();
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
        let transaction_id_offset = Page::get_integer_size();
        let filename_offset = transaction_id_offset + Page::get_integer_size();
        let block_number_offset = filename_offset + value.len() + Page::get_integer_size();
        let offset_value_offset = block_number_offset + Page::get_integer_size();
        let value_offset = offset_value_offset + Page::get_integer_size();
        let record_length = value_offset + value.len() + Page::get_integer_size();

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
    fn operator_code(&self) -> i32 {
        LogRecordType::SETSTRING as i32
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
