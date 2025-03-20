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
    log_file: String,
    log_page: Page,
    latest_lsn: i32,
    latest_saved_lsn: i32,
}

impl LogManager {
    pub fn new(file_manager: &mut FileManager, log_file: String) -> LogManager {
        // ログファイルが存在していなければ作成
        if !Path::new(&log_file).exists() {
            File::create(&log_file).unwrap();
        }

        let log_size = file_manager.length(&log_file);

        let mut log_page = Page::new(400);

        let block_id;

        if log_size == 0 {
            block_id = file_manager.append(&log_file);
            log_page.set_integer(0, file_manager.block_size as i32);
            file_manager.write(&block_id, &mut log_page);
        } else {
            block_id = BlockId::new(log_file.to_string(), log_size as u64 - 1);
            file_manager.read(&block_id, &mut log_page);
        }

        let latest_lsn = 0;
        let latest_saved_lsn = 0;

        LogManager {
            current_block_id: block_id,
            log_file,
            log_page,
            latest_lsn,
            latest_saved_lsn,
        }
    }

    pub fn flush(&mut self, file_manager: &mut FileManager) {
        file_manager.write(&self.current_block_id, &mut self.log_page);
        self.latest_saved_lsn = self.latest_lsn;
    }

    pub fn append_new_block(&mut self, file_manager: &mut FileManager) -> BlockId {
        let block_id = file_manager.append(&self.log_file);
        self.log_page = Page::new(400);
        self.log_page.set_integer(0, file_manager.block_size as i32);
        file_manager.write(&block_id, &mut self.log_page);
        block_id
    }

    fn append_record(&mut self, record: &[u8], file_manager: &mut FileManager) -> i32 {
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
        let mut log_page = Page::new(400);
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
        self.current_offset += 4 + record.len() as usize;
        record
    }
}
