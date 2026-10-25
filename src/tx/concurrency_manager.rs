use std::collections::HashMap;
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

use crate::storage::block::BlockId;

#[derive(Debug)]
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
        println!("lock value: {}", lock);
        self.locks.insert(block_id, lock + 1);
    }

    pub fn x_lock(&mut self, block_id: BlockId) {
        self.locks.insert(block_id, -1);
    }

    fn has_xlock(&self, block_id: &BlockId) -> bool {
        self.get_lock_value(block_id) < 0
    }

    fn has_other_slock(&self, block_id: &BlockId) -> bool {
        return self.get_lock_value(block_id) > 1;
    }

    pub fn unlock(&mut self, block_id: &BlockId, condvar: &Condvar) {
        let val = self.get_lock_value(block_id);

        if val > 1 {
            self.locks.insert(block_id.clone(), val - 1);
        } else {
            self.locks.remove(block_id);
            condvar.notify_all();
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

#[derive(Debug)]
pub struct ConcurrencyManagerV2 {
    locks: HashMap<BlockId, String>,
    lock_table: Arc<Mutex<LockTable>>,
    condvar: Arc<Condvar>,
}

impl ConcurrencyManagerV2 {
    pub fn new(lock_table: Arc<Mutex<LockTable>>, condvar: Arc<Condvar>) -> ConcurrencyManagerV2 {
        let locks: HashMap<BlockId, String> = HashMap::new();
        ConcurrencyManagerV2 {
            locks,
            lock_table,
            condvar,
        }
    }

    pub fn s_lock(&mut self, block_id: BlockId) {
        let lock_value = self.locks.get(&block_id);
        if lock_value.is_none() {
            let current_time = std::time::Instant::now();
            let mut lock_table = self.lock_table.lock().unwrap();
            while lock_table.has_xlock(&block_id) && !lock_table.wait_too_long(current_time) {
                (lock_table, _) = self
                    .condvar
                    .wait_timeout(lock_table, Duration::from_millis(1000))
                    .unwrap();
            }
            if lock_table.has_xlock(&block_id) {
                panic!("failed to slock")
            }
            lock_table.s_lock(block_id.clone());
            self.locks.insert(block_id, "S".to_string());
            return;
        }
    }

    pub fn x_lock(&mut self, block_id: BlockId) {
        if !self.has_xlock(&block_id) {
            self.s_lock(block_id.clone());
            let current_time = std::time::Instant::now();

            let mut lock_table = self.lock_table.lock().unwrap();

            while lock_table.has_other_slock(&block_id) && !lock_table.wait_too_long(current_time) {
                (lock_table, _) = self
                    .condvar
                    .wait_timeout(lock_table, Duration::from_millis(1000))
                    .unwrap();
            }

            if lock_table.has_other_slock(&block_id) {
                panic!("failed to xlock")
            }

            lock_table.x_lock(block_id.clone());
            self.locks.insert(block_id, "X".to_string());

            return;
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
        for (key, _value) in self.locks.iter() {
            self.lock_table.lock().unwrap().unlock(key, &self.condvar);
        }
        self.locks.clear();
    }
}
