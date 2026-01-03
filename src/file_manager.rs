use std::{
    collections::HashMap,
    fs::{self, File},
    os::unix::fs::FileExt,
    path::{Path, PathBuf},
};

use crate::BlockId;
use crate::Page;

pub struct FileManager {
    pub block_size: usize,
    open_files: HashMap<String, File>,
    directory_path: PathBuf,
}

impl FileManager {
    pub fn new(directory_path: &Path, block_size: usize) -> FileManager {
        fs::create_dir_all(directory_path).unwrap();
        let open_files: HashMap<String, File> = HashMap::new();

        FileManager {
            directory_path: directory_path.to_path_buf(),
            block_size,
            open_files,
        }
    }

    pub fn get_block_size(&self) -> usize {
        self.block_size
    }

    pub fn get_file(&mut self, file_name: &str) -> &File {
        let file_path = self.directory_path.join(file_name);

        // ファイルが存在しない場合は作成
        if !Path::new(&file_path).exists() {
            File::create(&file_path).unwrap();
        }

        let result = self
            .open_files
            .entry(file_name.to_string())
            .or_insert_with(|| {
                File::options()
                    .read(true)
                    .write(true)
                    .open(file_path)
                    .unwrap()
            });
        result
    }

    pub fn length(&mut self, file_name: &str) -> usize {
        let file = self.get_file(file_name);
        let file_length = file.metadata().unwrap().len() as usize;
        file_length / self.block_size
    }

    pub fn read(&mut self, block_id: &BlockId, page: &mut Page) {
        let block_size = self.block_size;
        let file = self.get_file(block_id.get_file_name());
        let offset = block_id.get_block_number() as usize * block_size;
        file.read_at(page.get_data().as_mut_slice(), offset as u64)
            .unwrap();
    }

    pub fn write(&mut self, block_id: &BlockId, page: &mut Page) {
        // もし、ファイルが存在しない場合は作成
        let file_path = self.directory_path.join(&block_id.get_file_name());
        if !Path::new(&file_path).exists() {
            File::create(&file_path).unwrap();
        }

        let block_size = self.block_size;
        let file = self.get_file(block_id.get_file_name());
        let offset = block_id.get_block_number() as usize * block_size;
        file.write_at(page.get_data().as_slice(), offset as u64)
            .unwrap();
    }
    pub fn append(&mut self, file_name: &str) -> BlockId {
        let block_size = self.block_size;
        let file = self.get_file(file_name);
        let offset = file.metadata().unwrap().len() as usize;
        let block_number = offset / block_size;
        let byte_array = vec![0; block_size];
        file.write_at(&byte_array, offset as u64).unwrap();

        return BlockId::new(file_name.to_string(), block_number as u64);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{fs::remove_file, path::Path};

    // FileManagerのテスト
    #[test]
    fn test_file_manager_read_write() {
        let test_dir = Path::new("test_data");
        let block_size = 400;
        let mut file_manager = FileManager::new(test_dir, block_size);

        let test_file_name = format!("test_file_{}.txt", uuid::Uuid::new_v4());

        // テスト用のBlockIdとPageを作成
        let block_id = BlockId::new(test_file_name.to_string(), 0);
        let mut page = Page::new(file_manager.get_block_size());

        // データを書き込む
        page.set_integer(0, 42);

        let offset_2 = 4;

        page.set_string(offset_2, "Hello, Test World!");

        let offset_3 = Page::get_max_length("Hello, Test World!".len() as u32) + offset_2;

        page.set_integer(offset_3, 23333);

        // ファイルに書き込む
        file_manager.write(&block_id, &mut page);

        // 別のページを作成して読み込む
        let mut page2 = Page::new(file_manager.get_block_size());
        file_manager.read(&block_id, &mut page2);

        // 読み込んだデータを検証
        assert_eq!(page2.get_integer(0), 42);
        assert_eq!(page2.get_string(offset_2), "Hello, Test World!");
        assert_eq!(page2.get_integer(offset_3), 23333);

        // // テスト後にディレクトリを削除
        remove_file(test_dir.join(&test_file_name)).unwrap_or_default();
    }
}
