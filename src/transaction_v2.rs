use std::{cell::RefCell, rc::Rc};

use crate::block::BlockId;
use crate::{
    buffer_manager_v2::{BufferListV2, BufferManagerV2},
    concurrency_manager::LockTable,
};
use crate::{concurrency_manager::ConcurrencyManagerV2, file_manager::FileManager};

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

    pub fn get_block_size(&self) -> usize {
        self.file_manager.borrow().get_block_size()
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

    pub fn get_available_buffer_size(&self) -> i32 {
        self.buffer_manager.borrow().get_available_buffer_size()
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use crate::{log_manager, log_manager_v2::LogManagerV2};

    use super::*;

    // FileManagerのテスト
    #[test]
    fn test_transaction_v2() {
        let test_dir = Path::new("data");
        let block_size = 400;
        let file_manager = Rc::new(RefCell::new(FileManager::new(test_dir, block_size)));
        let log_manager = Rc::new(RefCell::new(LogManagerV2::new(
            file_manager.clone(),
            "log.txt".to_string(),
        )));
        let buffer_manager = Rc::new(RefCell::new(BufferManagerV2::new(
            10,
            file_manager.clone(),
            log_manager.clone(),
        )));
        let lock_table = Rc::new(RefCell::new(LockTable::new()));

        let mut transaction = TransactionV2::new(
            1,
            file_manager.clone(),
            buffer_manager.clone(),
            lock_table.clone(),
        );

        let block_id_1 = BlockId::new("test_file_1.txt".to_string(), 1);
        let block_id_2 = BlockId::new("test_file_1.txt".to_string(), 2);

        transaction.pin(block_id_1.clone());
        transaction.pin(block_id_2.clone());

        transaction.set_string(block_id_2.clone(), 0, "hello world");

        let value = transaction.get_string(block_id_2.clone(), 0);
        print!("Value at block_id_1: {}\n", value);
        transaction.commit();
    }
}
