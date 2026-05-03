use std::path::Path;
use std::sync::{Arc, Mutex};
use std::{cell::RefCell, rc::Rc};

use crate::{
    buffer_manager_v2::BufferManagerV2, concurrency_manager::LockTable, file_manager::FileManager,
    log_manager_v2::LogManagerV2, transaction_v2::TransactionV2,
};

pub struct Database {
    lock_table: Arc<Mutex<LockTable>>,
    log_manager: Arc<Mutex<LogManagerV2>>,
    buffer_manager: Arc<Mutex<BufferManagerV2>>,
    file_manager: Arc<Mutex<FileManager>>,
}

impl Database {
    pub fn new(directory_path: &Path) -> Self {
        let file_manager = Arc::new(Mutex::new(FileManager::new(directory_path, 400)));
        let log_manager = Arc::new(Mutex::new(LogManagerV2::new(
            file_manager.clone(),
            "log.txt".to_string(),
        )));

        let buffer_manager = Arc::new(Mutex::new(BufferManagerV2::new(
            1000,
            file_manager.clone(),
            log_manager.clone(),
        )));

        let lock_table = Arc::new(Mutex::new(LockTable::new()));

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
