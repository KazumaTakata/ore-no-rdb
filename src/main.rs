use std::{
    collections::HashMap,
    fs::{self, File},
    path::Path,
    rc::Rc,
};

fn main() {
    println!("Hello, world!");
}

struct BlockId {
    file_name: String,
    block_number: u64,
}

impl BlockId {
    fn new(file_name: String, block_number: u64) -> BlockId {
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

struct Page {
    data: Vec<u8>,
}

impl Page {
    fn new(block_size: usize) -> Page {
        Page {
            data: vec![0; block_size],
        }
    }

    fn set_integer(&mut self, offset: usize, value: i32) {
        let integer_bytes = value.to_be_bytes();
        self.data[offset..offset + 4].copy_from_slice(&integer_bytes);
    }

    fn get_integer(&self, offset: usize) -> i32 {
        let mut bytes = [0; 4];
        bytes.copy_from_slice(&self.data[offset..offset + 4]);
        i32::from_be_bytes(bytes)
    }

    fn set_bytes(&mut self, offset: usize, value: &[u8]) {
        self.set_integer(offset, value.len() as i32);
        let offset = offset + 4;
        self.data[offset..offset + value.len()].copy_from_slice(value);
    }

    fn get_bytes(&self, offset: usize) -> Vec<u8> {
        let length = self.get_integer(offset) as usize;
        let offset = offset + 4;
        self.data[offset..offset + length].to_vec()
    }

    fn set_string(&mut self, offset: usize, value: &str) {
        self.set_bytes(offset, value.as_bytes());
    }

    fn get_string(&self, offset: usize) -> String {
        String::from_utf8(self.get_bytes(offset)).unwrap()
    }

    fn get_max_length(&self, string_length: u32) -> usize {
        return 4 + string_length as usize * 4;
    }

    fn get_data(&self) -> Vec<u8> {
        self.data.clone()
    }
}

struct FileManager {
    block_size: usize,
    open_files: HashMap<String, File>,
}

impl FileManager {
    fn new(directory_path: &Path, block_size: usize) -> FileManager {
        fs::create_dir_all(directory_path).unwrap();
        let open_files: HashMap<String, File> = HashMap::new();

        FileManager {
            block_size,
            open_files,
        }
    }

    fn get_file(&mut self, file_name: &str) -> &File {
        let result = self
            .open_files
            .entry(file_name.to_string())
            .or_insert_with(|| {
                File::options()
                    .read(true)
                    .write(true)
                    .open(file_name)
                    .unwrap()
            });
        result
    }
}
