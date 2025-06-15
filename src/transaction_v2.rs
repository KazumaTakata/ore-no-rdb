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

use crate::file_manager::FileManager;
use crate::log_manager::LogManager;
use crate::{block::BlockId, file_manager};
use crate::{buffer_manager::BufferList, recovery_manager};
use crate::{buffer_manager::BufferManager, concurrency_manager};
use crate::{
    buffer_manager_v2::{BufferListV2, BufferManagerV2},
    concurrency_manager::LockTable,
};
use crate::{
    concurrency_manager::ConcurrencyManager, concurrency_manager_v2::ConcurrencyManagerV2,
};

pub struct TransactionV2 {
    tx_num: i32,
    buffer_manager: Rc<RefCell<BufferManagerV2>>,
    lock_table: Rc<RefCell<LockTable>>,
    concurrency_manager: ConcurrencyManagerV2,
    buffer_list: BufferListV2,
    file_manager: Rc<RefCell<FileManager>>,
}

impl TransactionV2 {
    pub fn new(
        tx_num: i32,
        file_manager: Rc<RefCell<FileManager>>,
        buffer_manager: Rc<RefCell<BufferManagerV2>>,
        lock_table: Rc<RefCell<LockTable>>,
    ) -> TransactionV2 {
        let concurrency_manager = ConcurrencyManagerV2::new(lock_table.clone());
        let buffer_list = BufferListV2::new(buffer_manager.clone());

        TransactionV2 {
            tx_num,
            buffer_manager,
            file_manager,
            lock_table,
            concurrency_manager,
            buffer_list,
        }
    }

    pub fn pin(&mut self, block_id: BlockId) {
        self.buffer_list.pin(block_id);
    }

    pub fn unpin(&mut self, block_id: BlockId) {
        self.buffer_list.unpin(block_id);
    }

    pub fn commit(&mut self) {
        self.buffer_list.unpin_all();
        self.buffer_manager.borrow_mut().flush_all(self.tx_num);
    }

    fn rollback(&mut self, lock_table: &mut LockTable) {}

    pub fn set_integer(&mut self, block_id: BlockId, offset: usize, value: i32) {
        let buffer = self.buffer_list.get_buffer(block_id).unwrap();
        let mut buffer = buffer.borrow_mut();
        let page = buffer.content();
        page.set_integer(offset, value);
        buffer.set_modified(self.tx_num, -1);
    }

    pub fn set_string(&mut self, block_id: BlockId, offset: usize, value: &str) {
        let buffer = self.buffer_list.get_buffer(block_id).unwrap();
        let mut buffer = buffer.borrow_mut();
        let page = buffer.content();
        page.set_string(offset, value);
        buffer.set_modified(self.tx_num, -1);
    }

    pub fn get_integer(&mut self, block_id: BlockId, offset: usize) -> i32 {
        let buffer = self.buffer_list.get_buffer(block_id).unwrap();
        let mut buffer = buffer.borrow_mut();
        let page = buffer.content();
        page.get_integer(offset)
    }

    pub fn get_size(&self, file_name: String) -> usize {
        return self.file_manager.borrow_mut().length(&file_name);
    }

    pub fn get_string(&mut self, block_id: BlockId, offset: usize) -> String {
        let buffer = self.buffer_list.get_buffer(block_id).unwrap();
        let mut buffer = buffer.borrow_mut();
        let page = buffer.content();
        page.get_string(offset)
    }

    pub fn append(&mut self, file_name: &str) -> BlockId {
        self.file_manager.borrow_mut().append(file_name)
    }
}
