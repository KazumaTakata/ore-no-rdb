use std::path::Path;
use std::sync::{Arc, Condvar, Mutex};
use std::{cell::RefCell, rc::Rc};

use crate::storage::file_manager::FileManager;
use crate::{
    buffer::buffer_manager_v2::BufferManagerV2, storage::log_manager_v2::LogManagerV2,
    tx::concurrency_manager::LockTable, tx::transaction_v2::TransactionV2,
};

pub struct Database {
    lock_table: Arc<Mutex<LockTable>>,
    log_manager: Arc<Mutex<LogManagerV2>>,
    buffer_manager: Arc<Mutex<BufferManagerV2>>,
    file_manager: Arc<Mutex<FileManager>>,
    condvar: Arc<Condvar>,
}

impl Database {
    pub fn new(directory_path: &Path) -> Self {
        let file_manager = Arc::new(Mutex::new(FileManager::new(directory_path, 400)));
        let log_manager = Arc::new(Mutex::new(
            LogManagerV2::new(file_manager.clone(), "log.txt".to_string())
                .expect("failed to initialize log manager"),
        ));

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
            condvar: Arc::new(Condvar::new()),
        }
    }

    pub fn new_transaction(&self, transaction_id: i32) -> Rc<RefCell<TransactionV2>> {
        Rc::new(RefCell::new(TransactionV2::new(
            transaction_id,
            self.file_manager.clone(),
            self.buffer_manager.clone(),
            self.lock_table.clone(),
            self.log_manager.clone(),
            self.condvar.clone(),
        )))
    }
}
