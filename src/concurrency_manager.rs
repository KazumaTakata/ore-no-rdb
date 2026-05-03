use std::collections::HashMap;
use std::sync::{Arc, Mutex};
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

    fn wait_too_long(&self, start_time: std::time::Instant) -> bool {
        let elapsed = start_time.elapsed();
        elapsed.as_secs() > 5
    }
}

pub struct ConcurrencyManagerV2 {
    locks: HashMap<BlockId, String>,
    lock_table: Arc<Mutex<LockTable>>,
}

impl ConcurrencyManagerV2 {
    pub fn new(lock_table: Arc<Mutex<LockTable>>) -> ConcurrencyManagerV2 {
        let locks: HashMap<BlockId, String> = HashMap::new();
        ConcurrencyManagerV2 { locks, lock_table }
    }

    pub fn s_lock(&mut self, block_id: BlockId) {
        let lock_value = self.locks.get(&block_id);
        let current_time = std::time::Instant::now();
        if lock_value.is_none() {
            loop {
                {
                    let mut lock_table = self.lock_table.lock().unwrap();
                    if !lock_table.has_xlock(&block_id) {
                        lock_table.s_lock(block_id.clone());
                        self.locks.insert(block_id, "S".to_string());
                        return;
                    }
                }
                std::thread::sleep(std::time::Duration::from_millis(100));
                if self.lock_table.lock().unwrap().wait_too_long(current_time) {
                    panic!("lock wait timeout");
                }
            }
        }
    }

    pub fn x_lock(&mut self, block_id: BlockId) {
        if !self.has_xlock(&block_id) {
            self.s_lock(block_id.clone());
            let current_time = std::time::Instant::now();

            loop {
                {
                    let mut lock_table = self.lock_table.lock().unwrap();
                    if !lock_table.has_other_slock(&block_id) {
                        lock_table.x_lock(block_id.clone());
                        self.locks.insert(block_id, "X".to_string());
                        return;
                    }
                }
                std::thread::sleep(std::time::Duration::from_millis(100));
                if self.lock_table.lock().unwrap().wait_too_long(current_time) {
                    panic!("lock wait timeout");
                }
            }
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
            self.lock_table.lock().unwrap().unlock(key);
        }
        self.locks.clear();
    }
}
