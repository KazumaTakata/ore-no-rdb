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
use crate::file_manager::FileManager;
use crate::page::Page;

pub struct Buffer {
    page: Page,
    block_id: Option<BlockId>,
    tx_num: Option<i32>,
    lsn: Option<i32>,
    pin_count: i32,
}

impl Buffer {
    fn new() -> Buffer {
        let page = Page::new(1000);
        let pin_count = 0;

        Buffer {
            page,
            block_id: None,
            tx_num: None,
            pin_count,
            lsn: None,
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
        self.flush(file_manager);
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

pub struct BufferManager {
    buffer_pool: Vec<Rc<RefCell<Buffer>>>,
    number_of_available: i32,
}

impl BufferManager {
    pub fn new(number_of_buffer: i32) -> BufferManager {
        let mut buffer_pool = Vec::new();
        for _ in 0..number_of_buffer {
            buffer_pool.push(Rc::new(RefCell::new(Buffer::new())));
        }

        BufferManager {
            buffer_pool,
            number_of_available: number_of_buffer,
        }
    }

    pub fn unpin(&mut self, buffer: &mut Buffer) {
        buffer.unpin();
        if !buffer.is_pinned() {
            self.number_of_available = self.number_of_available + 1;
            return;
        }
    }

    pub fn flush_all(&mut self, file_manager: &mut FileManager, tx_num: i32) {
        for buffer in self.buffer_pool.iter() {
            let mut buffer = buffer.borrow_mut();
            if buffer.tx_num.is_some() && buffer.tx_num.unwrap() == tx_num {
                buffer.flush(file_manager);
            }
        }
    }

    pub fn try_to_pin(
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
                buffer.assign_to_block(file_manager, block_id);
            }
        }

        if let Some(buffer) = buffer {
            let mut buffer_mut = buffer.borrow_mut();
            if !buffer_mut.is_pinned() {
                self.number_of_available = self.number_of_available - 1;
            }
            buffer_mut.pin();

            return Some(buffer);
        }

        return None;
    }

    pub fn find_existing_buffer(&mut self, block_id: &BlockId) -> Option<&mut Rc<RefCell<Buffer>>> {
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

    pub fn pin(
        &mut self,
        file_manager: &mut FileManager,
        block_id: BlockId,
    ) -> Option<&Rc<RefCell<Buffer>>> {
        return self.try_to_pin(file_manager, block_id);
    }
}

pub struct BufferList {
    buffers: HashMap<BlockId, Rc<RefCell<Buffer>>>,
    pins: Vec<BlockId>,
}

impl BufferList {
    pub fn new() -> BufferList {
        BufferList {
            buffers: HashMap::new(),
            pins: Vec::new(),
        }
    }

    pub fn pin(
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

    pub fn unpin(&mut self, block_id: BlockId, buffer_manager: &mut BufferManager) {
        if let Some(buffer) = self.buffers.get(&block_id) {
            let mut buffer = buffer.borrow_mut();
            buffer.unpin();
            // self.pinsから始めに見つかったblock_idを削除
            if let Some(index) = self.pins.iter().position(|x| *x == block_id) {
                self.pins.remove(index);
            }

            if !buffer.is_pinned() {
                buffer_manager.unpin(&mut buffer);
            }
        }
    }

    pub fn unpin_all(&mut self, buffer_manager: &mut BufferManager) {
        for block_id in self.pins.iter() {
            if let Some(buffer) = self.buffers.get(block_id) {
                let mut buffer = buffer.borrow_mut();
                buffer_manager.unpin(&mut buffer);
            }
        }
        self.buffers.clear();
        self.pins.clear();
    }

    pub fn get_buffer(&mut self, block_id: BlockId) -> Option<&Rc<RefCell<Buffer>>> {
        let buffer = self.buffers.get(&block_id);
        return buffer;
    }
}

#[cfg(test)]
mod tests {
    use crate::page;

    use super::*;

    #[test]
    fn test_buffer() {
        let mut file_manager = FileManager::new(Path::new("data"), 400);
        let buffer_manager = Rc::new(RefCell::new(BufferManager::new(3)));

        let buffer_manager_ref1 = Rc::clone(&buffer_manager);

        let block_id = BlockId::new("test.txt".to_string(), 1);

        let mut buffer_mut_1 = {
            let mut buffer_manager_mut_ref_1 = buffer_manager_ref1.borrow_mut();
            let buffer_1 = buffer_manager_mut_ref_1.pin(&mut file_manager, block_id.clone());
            let mut buffer_mut_1 = buffer_1.unwrap().borrow_mut();
            let page_1 = buffer_mut_1.content();
            let test_integer = page_1.get_integer(80);
            page_1.set_integer(80, test_integer + 1);

            buffer_mut_1.set_modified(1, 0);

            Rc::clone(buffer_1.unwrap())
        };

        {
            assert!(buffer_manager_ref1.borrow().number_of_available == 2);
        }

        {
            let mut buffer_manager_mut_ref_2 = buffer_manager_ref1.borrow_mut();
            buffer_manager_mut_ref_2.unpin(&mut buffer_mut_1.borrow_mut());
        }

        {
            assert!(buffer_manager_ref1.borrow().number_of_available == 3);
        }

        {
            let block_id = BlockId::new("test.txt".to_string(), 2);
            let mut buffer_manager_mut_ref_3 = buffer_manager_ref1.borrow_mut();
            let buffer_2 = buffer_manager_mut_ref_3.pin(&mut file_manager, block_id.clone());
        }

        {
            let block_id = BlockId::new("test.txt".to_string(), 3);
            let mut buffer_manager_mut_ref_3 = buffer_manager_ref1.borrow_mut();
            let buffer_2 = buffer_manager_mut_ref_3.pin(&mut file_manager, block_id.clone());
        }

        {
            assert!(buffer_manager_ref1.borrow().number_of_available == 1);
        }

        // {
        //     let block_id = BlockId::new("test.txt".to_string(), 4);
        //     let mut buffer_manager_mut_ref_3 = buffer_manager_ref1.borrow_mut();
        //     let buffer_2 = buffer_manager_mut_ref_3.pin(&mut file_manager, block_id.clone());
        // }

        // let block_id = BlockId::new("test.txt".to_string(), 3);

        // let mut buffer_manager_mut_ref_3 = buffer_manager.borrow_mut();
        // let buffer_3 = buffer_manager_mut_ref_3.pin(&mut file_manager, block_id.clone());

        // let block_id = BlockId::new("test.txt".to_string(), 4);

        // let mut buffer_manager_mut_ref_3 = buffer_manager.borrow_mut();
        // let buffer_3 = buffer_manager_mut_ref_3.pin(&mut file_manager, block_id.clone());
    }
}
