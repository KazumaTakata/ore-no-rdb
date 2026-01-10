use std::{cell::RefCell, path::Path, rc::Rc};

use crate::{
    buffer_manager_v2::BufferManagerV2, concurrency_manager::LockTable, file_manager::FileManager,
    log_manager_v2::LogManagerV2, transaction_v2::TransactionV2,
};

pub struct Database {
    lock_table: Rc<RefCell<LockTable>>,
    log_manager: Rc<RefCell<LogManagerV2>>,
    buffer_manager: Rc<RefCell<BufferManagerV2>>,
    file_manager: Rc<RefCell<FileManager>>,
}

impl Database {
    pub fn new() -> Self {
        let file_manager = Rc::new(RefCell::new(FileManager::new(Path::new("data"), 400)));
        let log_manager = Rc::new(RefCell::new(LogManagerV2::new(
            file_manager.clone(),
            "log.txt".to_string(),
        )));

        let buffer_manager = Rc::new(RefCell::new(BufferManagerV2::new(
            1000,
            file_manager.clone(),
            log_manager.clone(),
        )));

        let lock_table = Rc::new(RefCell::new(LockTable::new()));

        Database {
            lock_table,
            log_manager,
            buffer_manager,
            file_manager,
        }
    }

    pub fn new_transaction(&self, transaction_id: i32) -> Rc<RefCell<TransactionV2>> {
        Rc::new(RefCell::new(TransactionV2::new(
            transaction_id,
            self.file_manager.clone(),
            self.buffer_manager.clone(),
            self.lock_table.clone(),
            self.log_manager.clone(),
        )))
    }
}
