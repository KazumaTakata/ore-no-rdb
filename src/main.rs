use rustyline::error::ReadlineError;
use rustyline::{DefaultEditor, Result};
use std::cell::RefCell;
use std::rc::Rc;
use std::vec;

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
mod query_handler;
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
use crate::predicate::{ConstantValue, TableNameAndFieldName};
use crate::predicate_v3::PredicateV2;
use crate::query_handler::handle_select_query;
use crate::transaction_v2::TransactionV2;

fn handle_parsed_sql(
    parsed_sql: &ParsedSQL,
    metadata_manager: &mut MetadataManager,
    transaction: Rc<RefCell<TransactionV2>>,
) -> () {
    match parsed_sql {
        ParsedSQL::Query(select_query) => {
            handle_select_query(select_query.clone(), metadata_manager, transaction.clone());
        }
        ParsedSQL::Insert(insert_data) => {
            execute_insert(transaction.clone(), metadata_manager, insert_data.clone());
            transaction.borrow_mut().commit();
        }
        ParsedSQL::Delete(delete_data) => {
            execute_delete(transaction.clone(), metadata_manager, delete_data.clone());
            transaction.borrow_mut().commit();
        }
        ParsedSQL::CreateTable(create_table_data) => {
            execute_create_table(
                transaction.clone(),
                metadata_manager,
                create_table_data.clone(),
            );
        }

        ParsedSQL::DescribeTable { table_name } => {
            let layout = metadata_manager
                .get_layout(table_name.clone(), transaction.clone())
                .unwrap();

            println!("schema for table '{:?}'", layout.schema);
        }

        ParsedSQL::ShowTables => {
            let select_query = QueryData::new(
                vec!["table_catalog".to_string()],
                vec![TableNameAndFieldName::new(None, "table_name".to_string())],
                PredicateV2::new(vec![]),
            );

            let mut plan =
                create_query_plan(&select_query, transaction.clone(), metadata_manager).unwrap();
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
    // ここにParsedSQLを処理するコードを追加
}

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

                let parsed_sql = parse_sql(line.to_string());
                handle_parsed_sql(&parsed_sql[0], &mut metadata_manager, transaction.clone());
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
}
