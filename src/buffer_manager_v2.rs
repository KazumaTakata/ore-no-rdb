use std::{cell::RefCell, collections::HashMap, rc::Rc};

use crate::file_manager::FileManager;
use crate::page::Page;
use crate::{block::BlockId, log_manager_v2::LogManagerV2};

pub struct BufferV2 {
    page: Page,
    block_id: Option<BlockId>,
    pub tx_num: Option<i32>,
    lsn: Option<i32>,
    pin_count: i32,
    log_manager: Rc<RefCell<LogManagerV2>>,
    file_manager: Rc<RefCell<FileManager>>,
}

impl BufferV2 {
    pub fn new(
        file_manager: Rc<RefCell<FileManager>>,
        log_manager: Rc<RefCell<LogManagerV2>>,
    ) -> BufferV2 {
        let page = Page::new(file_manager.borrow().block_size);
        let pin_count = 0;

        BufferV2 {
            page,
            block_id: None,
            tx_num: None,
            pin_count,
            lsn: None,
            file_manager,
            log_manager,
        }
    }

    pub fn content(&mut self) -> &mut Page {
        &mut self.page
    }

    pub fn set_modified(&mut self, tx_num: i32, lsn: i32) {
        self.tx_num = Some(tx_num);
        if lsn >= 0 {
            self.lsn = Some(lsn);
        }
    }

    pub fn block_id(&self) -> &Option<BlockId> {
        &self.block_id
    }

    pub fn is_pinned(&self) -> bool {
        self.pin_count > 0
    }

    fn get_tx_num(&self) -> Option<i32> {
        self.tx_num
    }

    pub fn assign_to_block(&mut self, block_id: BlockId) {
        self.flush();
        self.file_manager
            .borrow_mut()
            .read(&block_id, &mut self.page);
        self.block_id = Some(block_id);
        self.pin_count = 0;
    }

    pub fn flush(&mut self) {
        if self.tx_num.is_some() && self.block_id.is_some() {
            let block_id = self.block_id.as_ref().unwrap();
            self.file_manager
                .borrow_mut()
                .write(&block_id, &mut self.page);
            self.tx_num = None;
        }
    }

    pub fn pin(&mut self) {
        self.pin_count += 1;
    }

    pub fn unpin(&mut self) {
        self.pin_count -= 1;
    }
}

pub struct BufferManagerV2 {
    buffer_pool: Vec<Rc<RefCell<BufferV2>>>,
    number_of_available: i32,
    file_manager: Rc<RefCell<FileManager>>,
}

impl BufferManagerV2 {
    pub fn new(
        number_of_buffer: i32,
        file_manager: Rc<RefCell<FileManager>>,
        log_manager: Rc<RefCell<LogManagerV2>>,
    ) -> BufferManagerV2 {
        let mut buffer_pool = Vec::new();
        for _ in 0..number_of_buffer {
            buffer_pool.push(Rc::new(RefCell::new(BufferV2::new(
                file_manager.clone(),
                log_manager.clone(),
            ))));
        }

        BufferManagerV2 {
            buffer_pool,
            number_of_available: number_of_buffer,
            file_manager: file_manager.clone(),
        }
    }

    pub fn unpin(&mut self, buffer: &mut BufferV2) {
        buffer.unpin();
        if !buffer.is_pinned() {
            self.number_of_available = self.number_of_available + 1;
            return;
        }
    }

    pub fn flush_all(&mut self, tx_num: i32) {
        for buffer in self.buffer_pool.iter() {
            let mut buffer = buffer.borrow_mut();
            if buffer.tx_num.is_some() && buffer.tx_num.unwrap() == tx_num {
                buffer.flush();
            }
        }
    }

    pub fn try_to_pin(&mut self, block_id: BlockId) -> Option<Rc<RefCell<BufferV2>>> {
        let buffer = self.find_existing_buffer(&block_id);

        let buffer = match buffer {
            Some(buffer) => Some(buffer),
            None => {
                let buffer = self.choose_unpinned_buffer();
                match buffer {
                    Some(buffer) => {
                        buffer.borrow_mut().assign_to_block(block_id);
                        Some(buffer)
                    }
                    None => panic!("All buffers are pinned"),
                }
            }
        };

        if let Some(buffer) = buffer {
            let mut buffer_mut = buffer.borrow_mut();
            if !buffer_mut.is_pinned() {
                self.number_of_available = self.number_of_available - 1;
            }
            buffer_mut.pin();
            return Some(buffer.clone());
        } else {
            return None;
        }
    }

    fn find_existing_buffer(&mut self, block_id: &BlockId) -> Option<Rc<RefCell<BufferV2>>> {
        let buffer = self.buffer_pool.iter().find(|buffer| {
            let buffer_ref = buffer.borrow();
            buffer_ref.block_id().is_some()
                && buffer_ref.block_id().as_ref().unwrap().equals(&block_id)
        });

        if let Some(buffer) = buffer {
            return Some(buffer.clone());
        } else {
            return None;
        }
    }

    fn choose_unpinned_buffer(&mut self) -> Option<Rc<RefCell<BufferV2>>> {
        let buffer = self.buffer_pool.iter().find(|buffer| {
            let buffer = buffer.borrow();
            !buffer.is_pinned()
        });

        if let Some(buffer) = buffer {
            return Some(buffer.clone());
        } else {
            return None;
        }
    }

    pub fn pin(&mut self, block_id: BlockId) -> Option<Rc<RefCell<BufferV2>>> {
        return self.try_to_pin(block_id);
    }

    pub fn get_available_buffer_size(&self) -> i32 {
        self.number_of_available
    }
}

pub struct BufferListV2 {
    buffers: HashMap<BlockId, Rc<RefCell<BufferV2>>>,
    pins: Vec<BlockId>,
    buffer_manager: Rc<RefCell<BufferManagerV2>>,
}

impl BufferListV2 {
    pub fn new(buffer_manager: Rc<RefCell<BufferManagerV2>>) -> BufferListV2 {
        BufferListV2 {
            buffers: HashMap::new(),
            pins: Vec::new(),
            buffer_manager,
        }
    }

    pub fn pin(&mut self, block_id: BlockId) {
        if let Some(buffer) = self.buffer_manager.borrow_mut().pin(block_id.clone()) {
            self.buffers.insert(block_id.clone(), Rc::clone(&buffer));
            self.pins.push(block_id);
        }
    }

    pub fn unpin(&mut self, block_id: BlockId) {
        let mut should_remove_from_buffers = false;

        if let Some(buffer) = self.buffers.get(&block_id) {
            let mut buffer = buffer.borrow_mut();
            self.buffer_manager.borrow_mut().unpin(&mut buffer);
            // self.pinsから始めに見つかったblock_idを削除
            if let Some(index) = self.pins.iter().position(|x| *x == block_id) {
                self.pins.remove(index);
            }

            if (self.pins.iter().find(|x| **x == block_id)).is_none() {
                should_remove_from_buffers = true;
            }
        }

        if (should_remove_from_buffers) {
            self.buffers.remove(&block_id);
        }
    }

    pub fn unpin_all(&mut self) {
        for block_id in self.pins.iter() {
            if let Some(buffer) = self.buffers.get(block_id) {
                let mut buffer = buffer.borrow_mut();
                self.buffer_manager.borrow_mut().unpin(&mut buffer);
            }
        }
        self.buffers.clear();
        self.pins.clear();
    }

    pub fn get_buffer(&mut self, block_id: BlockId) -> Option<&Rc<RefCell<BufferV2>>> {
        let buffer = self.buffers.get(&block_id);
        return buffer;
    }
}

#[cfg(test)]
mod tests {

    use std::fs::remove_file;

    use super::*;

    #[test]
    fn test_buffer_manager() {
        let test_dir = std::path::Path::new("test_data");

        let block_size = 400;
        let file_manager = Rc::new(RefCell::new(FileManager::new(test_dir, block_size)));

        let log_manager = Rc::new(RefCell::new(LogManagerV2::new(
            file_manager.clone(),
            "log.txt".to_string(),
        )));

        let number_of_buffers = 3;

        let buffer_manager = Rc::new(RefCell::new(BufferManagerV2::new(
            number_of_buffers,
            file_manager.clone(),
            log_manager.clone(),
        )));

        let data_file_name = "test_buffer_manager.txt".to_string();

        let block_1_id = BlockId::new(data_file_name.clone(), 0);

        let buffer = buffer_manager.borrow_mut().pin(block_1_id).unwrap();

        {
            let mut borrowed_buffer = buffer.borrow_mut();

            let page_1 = borrowed_buffer.content();

            page_1.set_integer(80, 123);

            page_1.set_string(140, "hello buffer manager");

            borrowed_buffer.set_modified(1, 0);

            buffer_manager.borrow_mut().unpin(&mut borrowed_buffer);
        }

        let block_2_id = BlockId::new("test_buffer_manager.txt".to_string(), 1);
        let buffer_2 = buffer_manager.borrow_mut().pin(block_2_id).unwrap();

        let block_3_id = BlockId::new("test_buffer_manager.txt".to_string(), 2);
        let _buffer_3 = buffer_manager.borrow_mut().pin(block_3_id).unwrap();

        let block_4_id = BlockId::new("test_buffer_manager.txt".to_string(), 3);
        let _buffer_4 = buffer_manager.borrow_mut().pin(block_4_id).unwrap();

        buffer_manager
            .borrow_mut()
            .unpin(&mut buffer_2.borrow_mut());

        let block_5_id = BlockId::new("test_buffer_manager.txt".to_string(), 0);
        let buffer_5 = buffer_manager.borrow_mut().pin(block_5_id).unwrap();

        let mut borrowed_buffer_5 = buffer_5.borrow_mut();

        let content = borrowed_buffer_5.content();
        let value = content.get_integer(80);

        let string_value = content.get_string(140);

        assert!(value == 123);
        assert!(string_value == "hello buffer manager");

        // テストファイルに書き込んだデータが正しいか確認
        // テスト用にfile_managerを使って読み込み直して確認
        let mut file_manager_for_test = FileManager::new(test_dir, block_size);

        // テスト用のBlockIdとPageを作成
        let block_id = BlockId::new(data_file_name.clone(), 0);
        let mut page = Page::new(file_manager_for_test.get_block_size());

        file_manager_for_test.read(&block_id, &mut page);

        let integer_value = page.get_integer(80);
        assert!(integer_value == 123);

        let string_value = page.get_string(140);
        assert!(string_value == "hello buffer manager");

        remove_file(test_dir.join("test_buffer_manager.txt")).unwrap();
    }
}
