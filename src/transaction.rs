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

use crate::buffer_manager::BufferList;
use crate::buffer_manager::BufferManager;
use crate::concurrency_manager::ConcurrencyManager;
use crate::concurrency_manager::LockTable;
use crate::file_manager::FileManager;
use crate::log_manager::LogManager;
use crate::{block::BlockId, file_manager};

pub struct Transaction {
    tx_num: i32,
}

impl Transaction {
    pub fn new(tx_num: i32) -> Transaction {
        Transaction { tx_num }
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
        buffer_list: &mut BufferList,
        buffer_manager: &mut BufferManager,
        file_manager: &mut FileManager,
    ) {
        buffer_list.unpin_all(buffer_manager);
        buffer_manager.flush_all(file_manager, self.tx_num);
    }

    fn rollback(&mut self, lock_table: &mut LockTable) {}

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

    pub fn append(&mut self, file_manager: &mut FileManager, file_name: &str) -> BlockId {
        file_manager.append(file_name)
    }
}
