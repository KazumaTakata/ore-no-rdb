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
use crate::buffer_manager::BufferList;
use crate::buffer_manager::BufferManager;
use crate::concurrency_manager::ConcurrencyManager;
use crate::concurrency_manager::LockTable;
use crate::file_manager::FileManager;
use crate::log_manager::LogManager;

pub struct Transaction {
    tx_num: i32,
    concurrency_manager: ConcurrencyManager,
}

impl Transaction {
    pub fn new(tx_num: i32, concurrency_manager: ConcurrencyManager) -> Transaction {
        Transaction {
            tx_num,
            concurrency_manager,
        }
    }

    pub fn pin(
        &mut self,
        file_manager: &mut FileManager,
        buffer_list: &mut BufferList,
        buffer_manager: &mut BufferManager,
        block_id: BlockId,
    ) {
        buffer_list.pin(block_id, file_manager, buffer_manager);
    }

    pub fn commit(
        &mut self,
        log_manager: &mut LogManager,
        file_manager: &mut FileManager,
        lock_table: &mut LockTable,
        buffer_manager: &mut BufferManager,
    ) {
        log_manager.flush(file_manager);
        self.concurrency_manager.release(lock_table);
        buffer_manager.flush_all(file_manager, self.tx_num);
    }

    fn rollback(&mut self, lock_table: &mut LockTable) {
        self.concurrency_manager.release(lock_table);
    }

    pub fn set_integer(
        &mut self,
        buffer_list: &mut BufferList,
        block_id: BlockId,
        offset: usize,
        value: i32,
    ) {
        let buffer = buffer_list.get_buffer(block_id).unwrap();
        let mut buffer = buffer.borrow_mut();
        let page = buffer.content();
        page.set_integer(offset, value);
        buffer.set_modified(self.tx_num, -1);
    }

    pub fn set_string(
        &mut self,
        buffer_list: &mut BufferList,
        block_id: BlockId,
        offset: usize,
        value: &str,
    ) {
        let buffer = buffer_list.get_buffer(block_id).unwrap();
        let mut buffer = buffer.borrow_mut();
        let page = buffer.content();
        page.set_string(offset, value);
        buffer.set_modified(self.tx_num, -1);
    }

    pub fn get_integer(
        &mut self,
        buffer_list: &mut BufferList,
        block_id: BlockId,
        offset: usize,
    ) -> i32 {
        let buffer = buffer_list.get_buffer(block_id).unwrap();
        let mut buffer = buffer.borrow_mut();
        let page = buffer.content();
        page.get_integer(offset)
    }

    pub fn get_string(
        &mut self,
        buffer_list: &mut BufferList,
        block_id: BlockId,
        offset: usize,
    ) -> String {
        let buffer = buffer_list.get_buffer(block_id).unwrap();
        let mut buffer = buffer.borrow_mut();
        let page = buffer.content();
        page.get_string(offset)
    }
}
