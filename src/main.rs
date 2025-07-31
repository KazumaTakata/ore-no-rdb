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

mod block;
mod buffer_manager;
mod buffer_manager_v2;
mod concurrency_manager;
mod concurrency_manager_v2;
mod constant;
mod file_manager;
mod log_manager;
mod log_manager_v2;
mod metadata_manager;
mod page;
mod parser;
mod plan;
mod plan_v2;
mod predicate;
mod predicate_v3;
mod record_page;
mod record_page_v2;
mod recovery_manager;
mod scan;
mod scan_v2;
mod stat_manager;
mod stat_manager_v2;
mod table_manager;
mod table_manager_v2;
mod table_scan;
mod table_scan_v2;
mod transaction;
mod transaction_v2;

use block::BlockId;
use file_manager::FileManager;
use page::Page;
use parser::{parse_sql, Rule, SQLParser};
use pest::Parser;

fn main() {
    println!("Hello, world!");

    // let block = BlockId::new("./data/test.txt".to_string(), 0);

    // let mut page = Page::new(400);

    // page.set_string(88, "Hello, world! from page");

    // let mut file_manager = FileManager::new(Path::new("data"), 400);

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

#[cfg(test)]
mod tests {
    use crate::{
        buffer_manager::{BufferList, BufferManager},
        concurrency_manager::{ConcurrencyManager, LockTable},
        log_manager::LogManager,
        transaction::Transaction,
    };

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

    // // transactionのテスト
    // #[test]
    // fn test_transaction() {
    //     let test_dir = Path::new("data");
    //     let block_size = 400;
    //     let mut file_manager = FileManager::new(test_dir, block_size);
    //     let mut buffer_manager = BufferManager::new(10);
    //     let mut lock_table = LockTable::new();
    //     let mut log_manager = LogManager::new(&mut file_manager, "data/log".to_string());
    //     let mut buffer_list = BufferList::new();
    //     let mut transaction = Transaction::new(1);

    //     let block_id = BlockId::new("data/test_file.txt".to_string(), 0);
    //     let offset = 0;

    //     transaction.pin(
    //         &mut file_manager,
    //         &mut buffer_list,
    //         &mut buffer_manager,
    //         block_id.clone(),
    //     );
    //     transaction.set_integer(&mut buffer_list, block_id.clone(), offset, 42);
    //     transaction.commit(&mut log_manager, &mut file_manager, &mut buffer_manager);

    //     let mut transaction2 = Transaction::new(2, ConcurrencyManager::new());

    //     let value = transaction2.get_integer(&mut buffer_list, block_id.clone(), offset);
    //     assert_eq!(value, 42);

    //     // std::fs::remove_dir_all(test_dir).unwrap_or_default();
    // }
}
