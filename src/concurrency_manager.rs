use std::collections::HashMap;
use std::{cell::RefCell, rc::Rc};

use crate::block::BlockId;

pub struct LockTable {
    locks: HashMap<BlockId, i32>,
}

impl LockTable {
    pub fn new() -> LockTable {
        let locks: HashMap<BlockId, i32> = HashMap::new();
        LockTable { locks }
    }

    pub fn s_lock(&mut self, block_id: BlockId) {
        if self.has_xlock(&block_id) {
            panic!("lock conflict");
        }

        let lock = self.get_lock_value(&block_id);
        self.locks.insert(block_id, lock + 1);
    }

    pub fn x_lock(&mut self, block_id: BlockId) {
        if self.has_other_slock(&block_id) || self.has_xlock(&block_id) {
            panic!("lock conflict");
        }
        self.locks.insert(block_id, -1);
    }

    fn has_xlock(&self, block_id: &BlockId) -> bool {
        self.get_lock_value(block_id) < 0
    }

    fn has_other_slock(&self, block_id: &BlockId) -> bool {
        return self.get_lock_value(block_id) > 1;
    }

    pub fn unlock(&mut self, block_id: &BlockId) {
        let val = self.get_lock_value(block_id);

        if val > 1 {
            self.locks.insert(block_id.clone(), val - 1);
        } else {
            self.locks.remove(block_id);
        }
    }

    fn get_lock_value(&self, block_id: &BlockId) -> i32 {
        let lock_value = self.locks.get(block_id);

        if let Some(lock_value) = lock_value {
            return *lock_value;
        }

        return 0;
    }
}

pub struct ConcurrencyManagerV2 {
    locks: HashMap<BlockId, String>,
    lock_table: Rc<RefCell<LockTable>>,
}

impl ConcurrencyManagerV2 {
    pub fn new(lock_table: Rc<RefCell<LockTable>>) -> ConcurrencyManagerV2 {
        let locks: HashMap<BlockId, String> = HashMap::new();
        ConcurrencyManagerV2 { locks, lock_table }
    }

    pub fn s_lock(&mut self, block_id: BlockId) {
        let lock_value = self.locks.get(&block_id);
        if lock_value.is_none() {
            self.lock_table.borrow_mut().s_lock(block_id.clone());
            self.locks.insert(block_id, "S".to_string());
        }
    }

    pub fn x_lock(&mut self, block_id: BlockId) {
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
