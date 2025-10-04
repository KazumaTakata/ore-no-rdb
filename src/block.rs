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

#[derive(Eq, PartialEq, Clone, Debug)]
pub struct BlockId {
    file_name: String,
    block_number: u64,
}

use std::hash::{Hash, Hasher};

impl Hash for BlockId {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.file_name.hash(state);
        self.block_number.hash(state);
    }
}

impl BlockId {
    pub fn new(file_name: String, block_number: u64) -> BlockId {
        BlockId {
            file_name,
            block_number,
        }
    }

    pub fn get_file_name(&self) -> &String {
        &self.file_name
    }

    pub fn get_block_number(&self) -> u64 {
        self.block_number
    }

    pub fn equals(&self, other: &BlockId) -> bool {
        self.file_name == other.file_name && self.block_number == other.block_number
    }

    pub fn to_string(&self) -> String {
        format!("{}-{}", self.file_name, self.block_number)
    }

    fn file_name_hash_code(&self) -> i32 {
        let mut h: i32 = 0;
        for char in self.file_name.chars() {
            h = 31_i32.wrapping_mul(h).wrapping_add(char as i32);
        }
        h
    }

    pub fn hash_code(&self) -> i32 {
        let mut result = 17;
        result = 31 * result + self.file_name_hash_code();
        result = 31 * result + self.block_number as i32;
        result
    }
}
