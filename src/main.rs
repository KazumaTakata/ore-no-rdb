use std::{
    cell::RefCell,
    collections::HashMap,
    fs::{self, File},
    io::Write,
    os::unix::fs::FileExt,
    path::Path,
    rc::Rc,
};

fn main() {
    println!("Hello, world!");

    // let block = BlockId::new("./data/test.txt".to_string(), 0);

    // let mut page = Page::new(400);

    // page.set_string(88, "Hello, world! from page");

    let mut file_manager = FileManager::new(Path::new("data"), 400);

    // file_manager.write(&block, &mut page);

    // let mut page2 = Page::new(400);

    // file_manager.read(&block, &mut page2);

    // println!("{}", page2.get_string(88));

    // let mut file = file_manager.get_file("./data/test.txt");

    // let mut log_manager = LogManager::new(&mut file_manager, "data/log".to_string());

    // let block_id = BlockId::new("data/test.txt".to_string(), 0);

    // let buffer = buffer_manager.pin(block_id);

    // if let Some(buffer) = buffer {
    //     let mut buffer_ref = buffer.borrow_mut();
    //     let page = buffer_ref.content();
    //     let integer_1 = page.get_integer(80);
    //     println!("{}", integer_1);
    //     page.set_integer(80, integer_1 + 1);
    //     buffer_ref.set_modified(1, 0);

    //     drop(buffer_ref);

    //     buffer_manager.flush_all(1);
    // }

    // for i in 0..10 {
    //     let message = format!("Hello, world! from log {}", i);
    //     let lsn = log_manager.append_record(message.as_bytes());
    // }

    // log_manager.flush();
}

use std::hash::{Hash, Hasher};

#[derive(Eq, PartialEq, Clone, Debug)]
struct BlockId {
    file_name: String,
    block_number: u64,
}

impl Hash for BlockId {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.file_name.hash(state);
        self.block_number.hash(state);
    }
}

impl BlockId {
    fn new(file_name: String, block_number: u64) -> BlockId {
        // ファイルが存在していなければ作成
        if !Path::new(&file_name).exists() {
            File::create(&file_name).unwrap();
        }

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

    fn get_data(&mut self) -> &mut Vec<u8> {
        &mut self.data
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

    fn length(&mut self, file_name: &str) -> usize {
        let file = self.get_file(file_name);
        let file_length = file.metadata().unwrap().len() as usize;
        file_length / self.block_size
    }

    fn read(&mut self, block_id: &BlockId, page: &mut Page) {
        let block_size = self.block_size;
        let file = self.get_file(block_id.get_file_name());
        let offset = block_id.get_block_number() as usize * block_size;
        file.read_at(page.get_data().as_mut_slice(), offset as u64)
            .unwrap();
    }

    fn write(&mut self, block_id: &BlockId, page: &mut Page) {
        let block_size = self.block_size;
        let file = self.get_file(block_id.get_file_name());
        let offset = block_id.get_block_number() as usize * block_size;
        file.write_at(page.get_data().as_slice(), offset as u64)
            .unwrap();
    }
    fn append(&mut self, file_name: &str) -> BlockId {
        let block_size = self.block_size;
        let file = self.get_file(file_name);
        let offset = file.metadata().unwrap().len() as usize;
        let block_number = offset / block_size;
        let byte_array = vec![0; block_size];
        file.write_at(&byte_array, offset as u64).unwrap();

        return BlockId::new(file_name.to_string(), block_number as u64);
    }
}

struct LogManager {
    current_block_id: BlockId,
    log_file: String,
    log_page: Page,
    latest_lsn: i32,
    latest_saved_lsn: i32,
}

impl LogManager {
    fn new(file_manager: &mut FileManager, log_file: String) -> LogManager {
        // ログファイルが存在していなければ作成
        if !Path::new(&log_file).exists() {
            File::create(&log_file).unwrap();
        }

        let log_size = file_manager.length(&log_file);

        let mut log_page = Page::new(400);

        let block_id;

        if log_size == 0 {
            block_id = file_manager.append(&log_file);
            log_page.set_integer(0, file_manager.block_size as i32);
            file_manager.write(&block_id, &mut log_page);
        } else {
            block_id = BlockId::new(log_file.to_string(), log_size as u64 - 1);
            file_manager.read(&block_id, &mut log_page);
        }

        let latest_lsn = 0;
        let latest_saved_lsn = 0;

        LogManager {
            current_block_id: block_id,
            log_file,
            log_page,
            latest_lsn,
            latest_saved_lsn,
        }
    }

    fn flush(&mut self, file_manager: &mut FileManager) {
        file_manager.write(&self.current_block_id, &mut self.log_page);
        self.latest_saved_lsn = self.latest_lsn;
    }

    fn append_new_block(&mut self, file_manager: &mut FileManager) -> BlockId {
        let block_id = file_manager.append(&self.log_file);
        self.log_page = Page::new(400);
        self.log_page.set_integer(0, file_manager.block_size as i32);
        file_manager.write(&block_id, &mut self.log_page);
        block_id
    }

    fn append_record(&mut self, record: &[u8], file_manager: &mut FileManager) -> i32 {
        let record_length = record.len();
        let mut boundary = self.log_page.get_integer(0);

        let bytes_needed = 4 + record_length;

        if (boundary as usize) < bytes_needed + 4 {
            self.flush(file_manager);
            self.current_block_id = self.append_new_block(file_manager);
            boundary = self.log_page.get_integer(0);
        }

        let offset = (boundary as usize) - (bytes_needed);
        self.log_page.set_integer(0, offset as i32);
        self.log_page.set_bytes(offset, record);
        self.latest_lsn += 1;
        self.latest_lsn
    }
}

struct LogIterator {
    current_block_id: BlockId,
    current_offset: usize,
    log_page: Page,
}

impl LogIterator {
    fn new(file_manager: &mut FileManager, block_id: BlockId) -> LogIterator {
        let mut log_page = Page::new(400);
        file_manager.read(&block_id, &mut log_page);
        let current_offset = log_page.get_integer(0) as usize;

        LogIterator {
            current_block_id: block_id,
            current_offset,
            log_page,
        }
    }

    fn has_next(&self, file_manager: &FileManager) -> bool {
        self.current_offset < file_manager.block_size
            || self.current_block_id.get_block_number() > 0
    }

    fn next(&mut self, file_manager: &mut FileManager) -> Vec<u8> {
        let block_size = file_manager.block_size;
        if block_size == self.current_offset {
            self.current_block_id = BlockId::new(
                self.current_block_id.get_file_name().to_string(),
                self.current_block_id.get_block_number() - 1,
            );
            file_manager.read(&self.current_block_id, &mut self.log_page);
            self.current_offset = self.log_page.get_integer(0) as usize;
        }

        let record = self.log_page.get_bytes(self.current_offset);
        self.current_offset += 4 + record.len() as usize;
        record
    }
}

struct Buffer {
    page: Page,
    block_id: Option<BlockId>,
    tx_num: Option<i32>,
    lsn: Option<i32>,
    pin_count: i32,
}

impl Buffer {
    fn new() -> Buffer {
        let page = Page::new(400);
        let pin_count = 0;

        Buffer {
            page,
            block_id: None,
            tx_num: None,
            pin_count,
            lsn: None,
        }
    }

    fn content(&mut self) -> &mut Page {
        &mut self.page
    }

    fn set_modified(&mut self, tx_num: i32, lsn: i32) {
        self.tx_num = Some(tx_num);
        if lsn >= 0 {
            self.lsn = Some(lsn);
        }
    }

    fn block_id(&self) -> &Option<BlockId> {
        &self.block_id
    }

    fn is_pinned(&self) -> bool {
        self.pin_count > 0
    }

    fn get_tx_num(&self) -> Option<i32> {
        self.tx_num
    }

    fn assign_to_block(&mut self, file_manager: &mut FileManager, block_id: BlockId) {
        file_manager.read(&block_id, &mut self.page);
        self.block_id = Some(block_id);
        self.pin_count = 0;
    }

    fn flush(&mut self, file_manager: &mut FileManager) {
        if self.tx_num.is_some() && self.block_id.is_some() {
            let block_id = self.block_id.as_ref().unwrap();
            file_manager.write(&block_id, &mut self.page);
            self.tx_num = None;
        }
    }

    fn pin(&mut self) {
        self.pin_count += 1;
    }

    fn unpin(&mut self) {
        self.pin_count -= 1;
    }
}

struct BufferManager {
    buffer_pool: Vec<Rc<RefCell<Buffer>>>,
    number_of_buffer: i32,
}

impl BufferManager {
    fn new(number_of_buffer: i32) -> BufferManager {
        let mut buffer_pool = Vec::new();
        for _ in 0..number_of_buffer {
            buffer_pool.push(Rc::new(RefCell::new(Buffer::new())));
        }

        BufferManager {
            buffer_pool,
            number_of_buffer,
        }
    }

    fn flush_all(&mut self, file_manager: &mut FileManager, tx_num: i32) {
        for buffer in self.buffer_pool.iter() {
            let mut buffer = buffer.borrow_mut();
            if buffer.tx_num.is_some() && buffer.tx_num.unwrap() == tx_num {
                buffer.flush(file_manager);
            }
        }
    }

    fn try_to_pin(
        &mut self,
        file_manager: &mut FileManager,
        block_id: BlockId,
    ) -> Option<&Rc<RefCell<Buffer>>> {
        let mut buffer = self.buffer_pool.iter().find(|buffer| {
            let buffer = buffer.borrow();
            return buffer.block_id().is_some()
                && buffer.block_id().as_ref().unwrap().equals(&block_id);
        });

        if buffer.is_none() {
            buffer = self.buffer_pool.iter().find(|buffer| {
                let buffer = buffer.borrow();
                !buffer.is_pinned()
            });

            if buffer.is_none() {
                return None;
            }

            if buffer.is_some() {
                let mut buffer = buffer.unwrap().borrow_mut();
                self.number_of_buffer = self.number_of_buffer - 1;
                buffer.assign_to_block(file_manager, block_id);
            }
        }

        if let Some(buffer) = buffer {
            let mut buffer_mut = buffer.borrow_mut();
            if !buffer_mut.is_pinned() {
                self.number_of_buffer = self.number_of_buffer - 1;
            }
            buffer_mut.pin();

            return Some(buffer);
        }

        return None;
    }

    fn find_existing_buffer(&mut self, block_id: &BlockId) -> Option<&mut Rc<RefCell<Buffer>>> {
        self.buffer_pool.iter_mut().find(|buffer| {
            buffer.borrow_mut().block_id().is_some()
                && buffer
                    .borrow_mut()
                    .block_id()
                    .as_ref()
                    .unwrap()
                    .equals(&block_id)
        })
    }

    fn pin(
        &mut self,
        file_manager: &mut FileManager,
        block_id: BlockId,
    ) -> Option<&Rc<RefCell<Buffer>>> {
        return self.try_to_pin(file_manager, block_id);
    }
}

struct LockTable {
    locks: HashMap<BlockId, i32>,
}

impl LockTable {
    fn new() -> LockTable {
        let locks: HashMap<BlockId, i32> = HashMap::new();
        LockTable { locks }
    }

    fn s_lock(&mut self, block_id: BlockId) {
        if self.has_xlock(&block_id) {
            panic!("lock conflict");
        }

        let lock = self.get_lock_value(&block_id);
        self.locks.insert(block_id, lock + 1);
    }

    fn x_lock(&mut self, block_id: BlockId) {
        if self.has_other_slock(&block_id) || self.has_xlock(&block_id) {
            panic!("lock conflict");
        }
        self.locks.insert(block_id, -1);
    }

    fn has_xlock(&self, block_id: &BlockId) -> bool {
        let lock_value = self.locks.get(block_id);

        if let Some(lock_value) = lock_value {
            return *lock_value < 0;
        }

        return false;
    }

    fn has_other_slock(&self, block_id: &BlockId) -> bool {
        return self.get_lock_value(block_id) > 1;
    }

    fn unlock(&mut self, block_id: &BlockId) {
        let val = self.get_lock_value(block_id);

        if (val > 1) {
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

struct ConcurrencyManager {
    locks: HashMap<BlockId, String>,
}
impl ConcurrencyManager {
    fn new() -> ConcurrencyManager {
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

    fn release(&mut self, lock_table: &mut LockTable) {
        for (key, value) in self.locks.iter() {
            lock_table.unlock(key);
        }
        self.locks.clear();
    }
}

struct BufferList {
    buffers: HashMap<BlockId, Rc<RefCell<Buffer>>>,
    pins: Vec<BlockId>,
}

impl BufferList {
    fn new() -> BufferList {
        BufferList {
            buffers: HashMap::new(),
            pins: Vec::new(),
        }
    }

    fn pin(
        &mut self,
        block_id: BlockId,
        file_manager: &mut FileManager,
        buffer_manager: &mut BufferManager,
    ) {
        if let Some(buffer) = buffer_manager.pin(file_manager, block_id.clone()) {
            self.buffers.insert(block_id.clone(), Rc::clone(&buffer));
            self.pins.push(block_id);
        }
    }

    fn get_buffer(&mut self, block_id: BlockId) -> Option<&Rc<RefCell<Buffer>>> {
        let buffer = self.buffers.get(&block_id);
        return buffer;
    }
}

struct Transaction {
    tx_num: i32,
    concurrency_manager: ConcurrencyManager,
}

impl Transaction {
    fn new(tx_num: i32, concurrency_manager: ConcurrencyManager) -> Transaction {
        Transaction {
            tx_num,
            concurrency_manager,
        }
    }

    fn pin(
        &mut self,
        file_manager: &mut FileManager,
        buffer_list: &mut BufferList,
        buffer_manager: &mut BufferManager,
        block_id: BlockId,
    ) {
        buffer_list.pin(block_id, file_manager, buffer_manager);
    }

    fn commit(
        &mut self,
        log_manager: &mut LogManager,
        file_manager: &mut FileManager,
        lock_table: &mut LockTable,
        buffer_manager: &mut BufferManager,
    ) {
        log_manager.flush(file_manager);
        self.concurrency_manager.release(lock_table);
        buffer_manager.flush_all(file_manager, self.tx_num);
    }

    fn rollback(&mut self, lock_table: &mut LockTable) {
        self.concurrency_manager.release(lock_table);
    }

    fn set_integer(
        &mut self,
        buffer_list: &mut BufferList,
        block_id: BlockId,
        offset: usize,
        value: i32,
    ) {
        let buffer = buffer_list.get_buffer(block_id).unwrap();
        let mut buffer = buffer.borrow_mut();
        let page = buffer.content();
        page.set_integer(offset, value);
        buffer.set_modified(self.tx_num, -1);
    }

    fn set_string(
        &mut self,
        buffer_list: &mut BufferList,
        block_id: BlockId,
        offset: usize,
        value: &str,
    ) {
        let buffer = buffer_list.get_buffer(block_id).unwrap();
        let mut buffer = buffer.borrow_mut();
        let page = buffer.content();
        page.set_string(offset, value);
        buffer.set_modified(self.tx_num, -1);
    }

    fn get_integer(
        &mut self,
        buffer_list: &mut BufferList,
        block_id: BlockId,
        offset: usize,
    ) -> i32 {
        let buffer = buffer_list.get_buffer(block_id).unwrap();
        let mut buffer = buffer.borrow_mut();
        let page = buffer.content();
        page.get_integer(offset)
    }

    fn get_string(
        &mut self,
        buffer_list: &mut BufferList,
        block_id: BlockId,
        offset: usize,
    ) -> String {
        let buffer = buffer_list.get_buffer(block_id).unwrap();
        let mut buffer = buffer.borrow_mut();
        let page = buffer.content();
        page.get_string(offset)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    // FileManagerのテスト
    #[test]
    fn test_file_manager_read_write() {
        let test_dir = Path::new("data");
        let block_size = 400;
        let mut file_manager = FileManager::new(test_dir, block_size);

        // テスト用のBlockIdとPageを作成
        let block_id = BlockId::new("data/test_file.txt".to_string(), 0);
        let mut page = Page::new(block_size);

        // データを書き込む
        page.set_integer(0, 42);
        page.set_string(4, "Hello, Test World!");

        // ファイルに書き込む
        file_manager.write(&block_id, &mut page);

        // 別のページを作成して読み込む
        let mut page2 = Page::new(block_size);
        file_manager.read(&block_id, &mut page2);

        // 読み込んだデータを検証
        assert_eq!(page2.get_integer(0), 42);
        assert_eq!(page2.get_string(4), "Hello, Test World!");

        // // テスト後にディレクトリを削除
        // std::fs::remove_dir_all(test_dir).unwrap_or_default();
    }

    // transactionのテスト
    #[test]
    fn test_transaction() {
        let test_dir = Path::new("data");
        let block_size = 400;
        let mut file_manager = FileManager::new(test_dir, block_size);
        let mut buffer_manager = BufferManager::new(10);
        let mut lock_table = LockTable::new();
        let mut log_manager = LogManager::new(&mut file_manager, "data/log".to_string());
        let mut buffer_list = BufferList::new();
        let mut transaction = Transaction::new(1, ConcurrencyManager::new());

        let block_id = BlockId::new("data/test_file.txt".to_string(), 0);
        let offset = 0;

        transaction.pin(
            &mut file_manager,
            &mut buffer_list,
            &mut buffer_manager,
            block_id.clone(),
        );
        transaction.set_integer(&mut buffer_list, block_id.clone(), offset, 42);
        transaction.commit(
            &mut log_manager,
            &mut file_manager,
            &mut lock_table,
            &mut buffer_manager,
        );

        let mut transaction2 = Transaction::new(2, ConcurrencyManager::new());

        let value = transaction2.get_integer(&mut buffer_list, block_id.clone(), offset);
        assert_eq!(value, 42);

        // std::fs::remove_dir_all(test_dir).unwrap_or_default();
    }
}
