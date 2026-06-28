use crate::constant::{INTEGER_BYTE_SIZE, MAX_BYTE_SIZE_PER_CHAR};

pub struct Page {
    data: Vec<u8>,
}

impl Page {
    pub fn new(block_size: usize) -> Page {
        Page {
            data: vec![0; block_size],
        }
    }

    // log_managerのtestで使用
    pub fn from(data: Vec<u8>) -> Page {
        Page { data }
    }

    pub fn set_integer(&mut self, offset: usize, value: i32) {
        let integer_bytes = value.to_be_bytes();
        self.data[offset..offset + INTEGER_BYTE_SIZE].copy_from_slice(&integer_bytes);
    }

    pub fn get_integer(&self, offset: usize) -> i32 {
        let mut bytes = [0; INTEGER_BYTE_SIZE];
        bytes.copy_from_slice(&self.data[offset..offset + INTEGER_BYTE_SIZE]);
        i32::from_be_bytes(bytes)
    }

    pub fn set_bytes(&mut self, offset: usize, value: &[u8]) {
        self.set_integer(offset, value.len() as i32);
        let offset = offset + INTEGER_BYTE_SIZE;
        self.data[offset..offset + value.len()].copy_from_slice(value);
    }

    pub fn get_bytes(&self, offset: usize) -> Vec<u8> {
        let length = self.get_integer(offset) as usize;
        let offset = offset + INTEGER_BYTE_SIZE;
        self.data[offset..offset + length].to_vec()
    }

    pub fn set_string(&mut self, offset: usize, value: &str) {
        self.set_bytes(offset, value.as_bytes());
    }

    pub fn get_string(&self, offset: usize) -> String {
        String::from_utf8(self.get_bytes(offset)).unwrap()
    }

    pub fn get_max_length(string_length: u32) -> usize {
        // string型のlengthをi32 = 4byteで表現 + 文字列の長さ * utf-8の最大バイト数(4byte)
        return INTEGER_BYTE_SIZE + string_length as usize * MAX_BYTE_SIZE_PER_CHAR;
    }

    pub fn get_string_data_length(string: &str) -> usize {
        // stringのbyte数
        string.len() + INTEGER_BYTE_SIZE
    }

    pub fn get_data(&mut self) -> &mut Vec<u8> {
        &mut self.data
    }

    pub fn get_integer_byte_size() -> usize {
        INTEGER_BYTE_SIZE
    }
}
