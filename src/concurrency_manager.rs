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

pub struct ConcurrencyManager {
    locks: HashMap<BlockId, String>,
}
impl ConcurrencyManager {
    pub fn new() -> ConcurrencyManager {
        let locks: HashMap<BlockId, String> = HashMap::new();
        ConcurrencyManager { locks }
    }

    fn s_lock(&mut self, block_id: BlockId, lock_table: &mut LockTable) {
        let lock_value = self.locks.get(&block_id);
        if lock_value.is_none() {
            lock_table.s_lock(block_id.clone());
            self.locks.insert(block_id, "S".to_string());
        }
    }

    fn x_lock(&mut self, block_id: BlockId, lock_table: &mut LockTable) {
        if !self.has_xlock(&block_id) {
            self.s_lock(block_id.clone(), lock_table);
            lock_table.x_lock(block_id.clone());
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

    pub fn release(&mut self, lock_table: &mut LockTable) {
        for (key, value) in self.locks.iter() {
            lock_table.unlock(key);
        }
        self.locks.clear();
    }
}
