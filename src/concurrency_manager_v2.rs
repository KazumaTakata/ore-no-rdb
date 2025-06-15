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

use crate::{block::BlockId, concurrency_manager::LockTable};

pub struct ConcurrencyManagerV2 {
    locks: HashMap<BlockId, String>,
    lock_table: Rc<RefCell<LockTable>>,
}

impl ConcurrencyManagerV2 {
    pub fn new(lock_table: Rc<RefCell<LockTable>>) -> ConcurrencyManagerV2 {
        let locks: HashMap<BlockId, String> = HashMap::new();
        ConcurrencyManagerV2 { locks, lock_table }
    }

    fn s_lock(&mut self, block_id: BlockId) {
        let lock_value = self.locks.get(&block_id);
        if lock_value.is_none() {
            self.lock_table.borrow_mut().s_lock(block_id.clone());
            self.locks.insert(block_id, "S".to_string());
        }
    }

    fn x_lock(&mut self, block_id: BlockId) {
        if !self.has_xlock(&block_id) {
            self.s_lock(block_id.clone());
            self.lock_table.borrow_mut().x_lock(block_id.clone());
            self.locks.insert(block_id, "X".to_string());
        }
    }

    fn has_xlock(&self, block_id: &BlockId) -> bool {
        let lock_value = self.locks.get(block_id);

        if let Some(lock_value) = lock_value {
            if lock_value == "X" {
                return true;
            }
        }

        return false;
    }

    pub fn release(&mut self) {
        for (key, value) in self.locks.iter() {
            self.lock_table.borrow_mut().unlock(key);
        }
        self.locks.clear();
    }
}
