use rustyline::error::ReadlineError;
use rustyline::{DefaultEditor, Result};
use std::vec;
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
mod database;
mod error;
mod file_manager;
mod hash_index;
mod index_manager;
mod index_update_planner;
mod log_manager;
mod log_manager_v2;
mod materialize;
mod metadata_manager;
mod page;
mod parser;
mod plan_v2;
mod predicate;
mod predicate_v3;
mod record_page;
mod record_page_v2;
mod recovery_manager;
mod scan_v2;
mod sort_plan;
mod stat_manager_v2;
mod table_manager_v2;
mod table_scan_v2;
mod transaction;
mod transaction_v2;

use block::BlockId;
use file_manager::FileManager;
use page::Page;
use parser::{parse_sql, Rule, SQLParser};
use pest::Parser;

use crate::database::Database;
use crate::metadata_manager::MetadataManager;
use crate::parser::{ParsedSQL, QueryData};
use crate::plan_v2::{create_query_plan, execute_create_table, execute_delete, execute_insert};
use crate::predicate::ConstantValue;
use crate::predicate_v3::PredicateV2;

fn main() -> Result<()> {
    let database = Database::new();
    let transaction = database.new_transaction(1);
    let mut metadata_manager = MetadataManager::new(transaction.clone()).unwrap();

    // `()` can be used when no completer is required
    let mut rl = DefaultEditor::new()?;
    #[cfg(feature = "with-file-history")]
    if rl.load_history("history.txt").is_err() {
        println!("No previous history.");
    }

    loop {
        let readline = rl.readline(">> ");
        match readline {
            Ok(line) => {
                rl.add_history_entry(line.as_str())?;
                println!("Line: {}", line);

                let parsed_sql = parse_sql(line.to_string()).unwrap();
                match parsed_sql {
                    ParsedSQL::Query(select_query) => {
                        let table_exist = metadata_manager
                            .validate_select_sql(&select_query, transaction.clone());

                        if !table_exist {
                            println!("Table or field does not exist");
                            continue;
                        }

                        let mut plan = create_query_plan(
                            &select_query,
                            transaction.clone(),
                            &mut metadata_manager,
                        )
                        .unwrap();
                        let mut scan = plan.open().unwrap();
                        scan.move_to_before_first();
                        while scan.next().unwrap() {
                            let results = select_query
                                .field_name_list
                                .iter()
                                .map(|field_name| {
                                    let value = scan.get_value(field_name.clone());
                                    return value;
                                })
                                .collect::<Vec<_>>();

                            println!("Results: {:?}", results);
                        }
                    }
                    ParsedSQL::Insert(insert_data) => {
                        execute_insert(transaction.clone(), &mut metadata_manager, insert_data);
                        transaction.borrow_mut().commit();
                    }
                    ParsedSQL::Delete(delete_data) => {
                        execute_delete(transaction.clone(), &mut metadata_manager, delete_data);
                        transaction.borrow_mut().commit();
                    }
                    ParsedSQL::CreateTable(create_table_data) => {
                        execute_create_table(
                            transaction.clone(),
                            &mut metadata_manager,
                            create_table_data,
                        );
                    }

                    ParsedSQL::ShowTables => {
                        let select_query = QueryData::new(
                            vec!["table_catalog".to_string()],
                            vec!["table_name".to_string()],
                            PredicateV2::new(vec![]),
                        );

                        let mut plan = create_query_plan(
                            &select_query,
                            transaction.clone(),
                            &mut metadata_manager,
                        )
                        .unwrap();
                        let mut scan = plan.open().unwrap();
                        scan.move_to_before_first();
                        while scan.next().unwrap() {
                            let results = select_query
                                .field_name_list
                                .iter()
                                .map(|field_name| {
                                    let value = scan.get_value(field_name.clone());
                                    return value;
                                })
                                .filter(|v| {
                                    if let Some(constant_value) = v {
                                        return match constant_value.clone() {
                                            ConstantValue::String(s) => {
                                                s != "table_catalog"
                                                    && s != "field_catalog"
                                                    && s != "index_catalog"
                                            }
                                            _ => true,
                                        };
                                    }
                                    return false;
                                })
                                // 空になったvectorは除外
                                .collect::<Vec<_>>();

                            if results.len() == 0 {
                                continue;
                            }
                            println!("Results: {:?}", results);
                        }
                    }
                    _ => panic!("Expected a Query variant from parse_sql"),
                };
            }
            Err(ReadlineError::Interrupted) => {
                println!("CTRL-C");
                break;
            }
            Err(ReadlineError::Eof) => {
                println!("CTRL-D");
                break;
            }
            Err(err) => {
                println!("Error: {:?}", err);
                break;
            }
        }
    }
    #[cfg(feature = "with-file-history")]
    rl.save_history("history.txt");
    Ok(())

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
