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

use crate::file_manager::{self, FileManager};
use crate::page::Page;
use crate::{block::BlockId, constant::INTEGER_BYTE_SIZE};
pub struct LogManagerV2 {
    current_block_id: BlockId,
    log_file_name: String,
    log_page: Page,
    latest_lsn: i32,
    latest_saved_lsn: i32,
    file_manager: Rc<RefCell<FileManager>>,
}

impl LogManagerV2 {
    pub fn new(file_manager: Rc<RefCell<FileManager>>, log_file_name: String) -> LogManagerV2 {
        let mut file_manager_mut_ref = file_manager.borrow_mut();
        let log_size = file_manager_mut_ref.length(&log_file_name);

        let mut log_page = Page::new(400);

        let block_id;

        if log_size == 0 {
            block_id = file_manager_mut_ref.append(&log_file_name);
            log_page.set_integer(0, file_manager_mut_ref.block_size as i32);
            file_manager_mut_ref.write(&block_id, &mut log_page);
        } else {
            block_id = BlockId::new(log_file_name.to_string(), log_size as u64 - 1);
            file_manager_mut_ref.read(&block_id, &mut log_page);
        }

        LogManagerV2 {
            current_block_id: block_id,
            log_file_name,
            log_page,
            latest_lsn: 0,
            latest_saved_lsn: 0,
            file_manager: file_manager.clone(),
        }
    }

    pub fn append_new_block(&mut self) -> BlockId {
        let block_id = self.file_manager.borrow_mut().append(&self.log_file_name);
        self.log_page = Page::new(self.file_manager.borrow().block_size);
        self.log_page
            .set_integer(0, self.file_manager.borrow().block_size as i32);
        self.file_manager
            .borrow_mut()
            .write(&block_id, &mut self.log_page);
        block_id
    }

    pub fn flush(&mut self) {
        self.file_manager
            .borrow_mut()
            .write(&self.current_block_id, &mut self.log_page);

        self.latest_saved_lsn = self.latest_lsn;
    }

    pub fn flush_with_lsn(&mut self, lsn: i32) {
        if lsn > self.latest_saved_lsn {
            self.flush();
        }
    }

    pub fn append_record(&mut self, record: &[u8]) -> i32 {
        let record_length = record.len();
        let mut boundary = self.log_page.get_integer(0);

        let bytes_needed = 4 + record_length;

        if (boundary as usize) < bytes_needed + 4 {
            self.flush();
            self.current_block_id = self.append_new_block();
            boundary = self.log_page.get_integer(0);
        }

        let offset = (boundary as usize) - (bytes_needed);
        self.log_page.set_integer(0, offset as i32);
        self.log_page.set_bytes(offset, record);
        self.latest_lsn += 1;
        self.latest_lsn
    }

    pub fn iterator(&mut self) -> LogIteratorV2 {
        self.flush();
        LogIteratorV2::new(self.file_manager.clone(), self.current_block_id.clone())
    }
}

pub struct LogIteratorV2 {
    current_block_id: BlockId,
    current_offset: usize,
    log_page: Page,
    file_manager: Rc<RefCell<FileManager>>,
}

impl LogIteratorV2 {
    fn new(file_manager: Rc<RefCell<FileManager>>, block_id: BlockId) -> LogIteratorV2 {
        let mut log_page = Page::new(file_manager.borrow().block_size);
        file_manager.borrow_mut().read(&block_id, &mut log_page);
        let current_offset = log_page.get_integer(0) as usize;

        LogIteratorV2 {
            current_block_id: block_id,
            current_offset,
            log_page,
            file_manager,
        }
    }

    pub fn has_next(&self) -> bool {
        self.current_offset < self.file_manager.borrow().block_size
            || self.current_block_id.get_block_number() > 0
    }

    pub fn next(&mut self) -> Vec<u8> {
        let block_size = self.file_manager.borrow().block_size;
        if block_size == self.current_offset {
            self.current_block_id = BlockId::new(
                self.current_block_id.get_file_name().to_string(),
                self.current_block_id.get_block_number() - 1,
            );
            self.file_manager
                .borrow_mut()
                .read(&self.current_block_id, &mut self.log_page);
            self.current_offset = self.log_page.get_integer(0) as usize;
        }

        let record = self.log_page.get_bytes(self.current_offset);
        // i32(4byte) + Vec<u8>の長さ
        self.current_offset += INTEGER_BYTE_SIZE + record.len() as usize;
        record
    }
}
