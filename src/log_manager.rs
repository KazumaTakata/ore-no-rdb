use std::{
    cell::RefCell,
    collections::HashMap,
    fs::{self, File},
    io::Write,
    iter::Map,
    os::unix::fs::FileExt,
    path::Path,
    rc::Rc,
};

use crate::block::BlockId;
use crate::file_manager::FileManager;
use crate::page::Page;
pub struct LogManager {
    current_block_id: BlockId,
    log_file_name: String,
    log_page: Page,
    latest_lsn: i32,
    latest_saved_lsn: i32,
}

impl LogManager {
    pub fn new(file_manager: &mut FileManager, log_file_name: String) -> LogManager {
        let log_size = file_manager.length(&log_file_name);

        let mut log_page = Page::new(400);

        let block_id;

        if log_size == 0 {
            block_id = LogManager::_append_new_block(file_manager, &log_file_name, &mut log_page);
        } else {
            block_id = BlockId::new(log_file_name.to_string(), log_size as u64 - 1);
            file_manager.read(&block_id, &mut log_page);
        }

        LogManager {
            current_block_id: block_id,
            log_file_name,
            log_page,
            latest_lsn: 0,
            latest_saved_lsn: 0,
        }
    }

    fn _append_new_block(
        file_manager: &mut FileManager,
        log_file: &String,
        log_page: &mut Page,
    ) -> BlockId {
        let block_id = file_manager.append(&log_file);
        log_page.set_integer(0, file_manager.block_size as i32);
        file_manager.write(&block_id, log_page);
        block_id
    }

    pub fn flush(&mut self, file_manager: &mut FileManager) {
        file_manager.write(&self.current_block_id, &mut self.log_page);
        self.latest_saved_lsn = self.latest_lsn;
    }

    pub fn iterator(&mut self, file_manager: &mut FileManager) -> LogIterator {
        self.flush(file_manager);
        LogIterator::new(file_manager, self.current_block_id.clone())
    }

    pub fn append_new_block(&mut self, file_manager: &mut FileManager) -> BlockId {
        let block_id = file_manager.append(&self.log_file_name);
        self.log_page = Page::new(file_manager.block_size);
        self.log_page.set_integer(0, file_manager.block_size as i32);
        file_manager.write(&block_id, &mut self.log_page);
        block_id
    }

    pub fn append_record(&mut self, record: &[u8], file_manager: &mut FileManager) -> i32 {
        let record_length = record.len();
        let mut boundary = self.log_page.get_integer(0);

        let bytes_needed = 4 + record_length;

        if (boundary as usize) < bytes_needed + 4 {
            self.flush(file_manager);
            self.current_block_id = self.append_new_block(file_manager);
            boundary = self.log_page.get_integer(0);
        }

        let offset = (boundary as usize) - (bytes_needed);
        self.log_page.set_integer(0, offset as i32);
        self.log_page.set_bytes(offset, record);
        self.latest_lsn += 1;
        self.latest_lsn
    }
}

struct LogIterator {
    current_block_id: BlockId,
    current_offset: usize,
    log_page: Page,
}

impl LogIterator {
    fn new(file_manager: &mut FileManager, block_id: BlockId) -> LogIterator {
        let mut log_page = Page::new(file_manager.block_size);
        file_manager.read(&block_id, &mut log_page);
        let current_offset = log_page.get_integer(0) as usize;

        LogIterator {
            current_block_id: block_id,
            current_offset,
            log_page,
        }
    }

    fn has_next(&self, file_manager: &FileManager) -> bool {
        self.current_offset < file_manager.block_size
            || self.current_block_id.get_block_number() > 0
    }

    fn next(&mut self, file_manager: &mut FileManager) -> Vec<u8> {
        let block_size = file_manager.block_size;
        if block_size == self.current_offset {
            self.current_block_id = BlockId::new(
                self.current_block_id.get_file_name().to_string(),
                self.current_block_id.get_block_number() - 1,
            );
            file_manager.read(&self.current_block_id, &mut self.log_page);
            self.current_offset = self.log_page.get_integer(0) as usize;
        }

        let record = self.log_page.get_bytes(self.current_offset);
        // i32(4byte) + Vec<u8>の長さ
        self.current_offset += 4 + record.len() as usize;
        record
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{fmt::format, path::Path};

    // LogManagerのテスト
    #[test]
    fn test_log_manager_append_record() {
        let test_dir = Path::new("data");
        let block_size = 400;
        let mut file_manager = FileManager::new(test_dir, block_size);
        let log_file = "test_log_manager_append_record.txt".to_string();
        let mut log_manager = LogManager::new(&mut file_manager, log_file);

        create_record(&mut log_manager, &mut file_manager);
        print_log_record(&mut log_manager, &mut file_manager);
    }

    fn print_log_record(log_manager: &mut LogManager, file_manager: &mut FileManager) {
        let mut log_iterator = log_manager.iterator(file_manager);
        while log_iterator.has_next(file_manager) {
            let record = log_iterator.next(file_manager);
            let tmp_page = Page::from(record);
            let test_string = tmp_page.get_string(0);
            let test_integer = tmp_page.get_integer(Page::get_max_length(test_string.len() as u32));
            print!("record: [{:?}, {:?}]\n", test_string, test_integer);
        }
    }

    fn create_record(log_manager: &mut LogManager, file_manager: &mut FileManager) {
        for i in 1..35 {
            let test_string = format!("test_sting_{}", i);
            let record = create_log_record(test_string, i);
            let lsn = log_manager.append_record(&record, file_manager);
            print!("lsn: {}\n", lsn);
        }
    }

    fn create_log_record(test_string: String, test_integer: i32) -> Vec<u8> {
        let offset = Page::get_max_length(test_string.len() as u32);
        // i32(4byte) + 文字列の長さ * utf-8の最大バイト数(4byte)
        let test_vector = vec![0; offset + 4];

        let mut page = Page::from(test_vector);
        page.set_string(0, &test_string);
        page.set_integer(offset, test_integer);

        return page.get_data().to_vec();
    }
}
