use nu_ansi_term::{Color, Style};
use reedline::{
    default_vi_insert_keybindings, default_vi_normal_keybindings, ColumnarMenu, DefaultCompleter,
    DefaultHinter, DefaultPrompt, ExampleHighlighter, FileBackedHistory, KeyCode, KeyModifiers,
    MenuBuilder, Reedline, ReedlineEvent, ReedlineMenu, Signal, Vi,
};
use std::cell::RefCell;
use std::path::Path;
use std::rc::Rc;
use std::vec;

mod block;
mod buffer_manager_v2;
mod concurrency_manager;
mod constant;
mod database;
mod error;
mod file_manager;
mod group_by;
mod hash_index;
mod index_join_scan;
mod index_manager;
mod index_update_planner;
mod log_manager_v2;
mod materialize;
mod metadata_manager;
mod multi_buffer;
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
mod transaction_v2;
mod view_manager;

use block::BlockId;
use clap::Parser;
use page::Page;
use parser::parse_sql;

use crate::database::Database;
use crate::index_update_planner::IndexUpdatePlanner;
use crate::metadata_manager::MetadataManager;
use crate::parser::{ParsedSQL, QueryData};
use crate::plan_v2::{create_query_plan, execute_create_table};
use crate::predicate::{ConstantValue, TableNameAndFieldName};
use crate::predicate_v3::PredicateV2;
use crate::query_handler::handle_select_query;
use crate::transaction_v2::TransactionV2;

#[derive(Parser)]
struct Args {
    /// 名前を指定
    #[arg(short, long)]
    file: Option<String>,
}

fn handle_parsed_sql(
    parsed_sql: &ParsedSQL,
    metadata_manager: &mut MetadataManager,
    transaction: Rc<RefCell<TransactionV2>>,
    index_update_planner: &mut IndexUpdatePlanner,
) -> () {
    match parsed_sql {
        ParsedSQL::Query(select_query) => {
            handle_select_query(select_query.clone(), metadata_manager, transaction.clone());
        }
        ParsedSQL::Explain(query_data) => {
            let table_exist =
                metadata_manager.validate_select_sql(&query_data, transaction.clone());

            if !table_exist {
                println!("Table or field does not exist");
                return;
            }

            let mut plan =
                create_query_plan(&query_data, transaction.clone(), metadata_manager).unwrap();

            let plan_tree = plan.get_child_plans();
            println!("Query Plan:");
            plan_tree.print_tree();
        }
        ParsedSQL::Insert(insert_data) => {
            // execute_insert(transaction.clone(), metadata_manager, insert_data.clone());
            let result = index_update_planner.execute_insert(
                insert_data.clone(),
                transaction.clone(),
                metadata_manager,
            );

            let Ok(()) = result else {
                eprintln!("Error executing insert: {:?}", result.err());
                return;
            };

            transaction.borrow_mut().commit();
        }
        ParsedSQL::Delete(delete_data) => {
            let result = index_update_planner.execute_delete(
                delete_data.clone(),
                transaction.clone(),
                metadata_manager,
            );

            let Ok(()) = result else {
                eprintln!("Error executing delete: {:?}", result.err());
                return;
            };

            transaction.borrow_mut().commit();
        }
        ParsedSQL::CreateTable(create_table_data) => {
            let result = execute_create_table(
                transaction.clone(),
                metadata_manager,
                create_table_data.clone(),
            );
            let Ok(()) = result else {
                eprintln!("Error executing create table: {:?}", result.err());
                return;
            };
        }
        ParsedSQL::Update(update_data) => {
            // handle_update_query(update_data.clone(), metadata_manager, transaction.clone());
            let result = index_update_planner.execute_modify(
                update_data.clone(),
                transaction.clone(),
                metadata_manager,
            );
            let Ok(()) = result else {
                eprintln!("Error executing update: {:?}", result.err());
                return;
            };
        }
        ParsedSQL::DescribeTable { table_name } => {
            let layout = metadata_manager
                .get_layout(table_name.clone(), transaction.clone())
                .unwrap();

            println!("schema for table '{:?}'", layout.schema);
        }
        ParsedSQL::CreateIndex(create_index_data) => {
            metadata_manager.create_index(
                create_index_data.index_name.clone(),
                create_index_data.table_name.clone(),
                create_index_data.field_name.clone(),
                transaction.clone(),
            );
        }

        ParsedSQL::ShowTables => {
            let select_query = QueryData::new(
                vec!["table_catalog".to_string()],
                vec![TableNameAndFieldName::new(None, "table_name".to_string())],
                PredicateV2::new(vec![]),
                vec![],
                vec![],
                vec![],
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
            }
        }
        _ => panic!("Expected a Query variant from parse_sql"),
    };
    // ここにParsedSQLを処理するコードを追加
}

fn main() -> std::io::Result<()> {
    let directory_path_name = format!("test_data_{}", uuid::Uuid::new_v4());
    let directory_path = Path::new(&directory_path_name);
    let database = Database::new(directory_path);

    let transaction = database.new_transaction(1);
    let mut metadata_manager = Rc::new(RefCell::new(
        MetadataManager::new(transaction.clone()).unwrap(),
    ));

    let mut index_update_planner = index_update_planner::IndexUpdatePlanner::new();
    let args = Args::parse();

    if let Some(file_path) = args.file {
        let sql = std::fs::read_to_string(file_path).expect("Failed to read SQL file");
        let parsed_sql_list = parse_sql(sql);
        for parsed_sql in &parsed_sql_list {
            handle_parsed_sql(
                parsed_sql,
                &mut metadata_manager.borrow_mut(),
                transaction.clone(),
                &mut index_update_planner,
            );
        }
        return Ok(());
    }

    let commands = vec![
        "select".into(),
        "insert".into(),
        "update".into(),
        "delete".into(),
        "create".into(),
        "table".into(),
        "index".into(),
        "view".into(),
        "from".into(),
        "into".into(),
        "where".into(),
        "values".into(),
        "set".into(),
        "and".into(),
        "or".into(),
        "on".into(),
        "as".into(),
        "order".into(),
        "by".into(),
        "group".into(),
        "having".into(),
        "integer".into(),
        "varchar".into(),
        "show".into(),
        "tables".into(),
        "describe".into(),
        "max".into(),
        "min".into(),
        "count".into(),
        "sum".into(),
        "avg".into(),
    ];

    // 2. 補完器(単純なprefix一致)
    let completer = Box::new(DefaultCompleter::new_with_wordlen(commands.clone(), 2));

    // 3. 補完メニュー(Tabで開くカラム表示)
    let completion_menu = Box::new(
        ColumnarMenu::default()
            .with_name("completion_menu")
            .with_columns(2)
            .with_column_padding(2),
    );

    // 4. シンタックスハイライト
    let highlighter = Box::new(ExampleHighlighter::new(commands));

    // 5. 履歴ベースのオートサジェスト(fish風の灰色予測)
    let hinter =
        Box::new(DefaultHinter::default().with_style(Style::new().italic().fg(Color::DarkGray)));

    // 6. ファイルに保存される履歴
    let history = Box::new(
        FileBackedHistory::with_file(1000, "history.txt".into())
            .expect("Error configuring history"),
    );

    // 7. Vi モードのキーバインド (insert / normal の2モード)
    let mut insert_keybindings = default_vi_insert_keybindings();
    let normal_keybindings = default_vi_normal_keybindings();

    // Tab で補完メニューを開く / 開いている時は次候補へ (insert モードのみ)
    insert_keybindings.add_binding(
        KeyModifiers::NONE,
        KeyCode::Tab,
        ReedlineEvent::UntilFound(vec![
            ReedlineEvent::Menu("completion_menu".to_string()),
            ReedlineEvent::MenuNext,
        ]),
    );

    let edit_mode = Box::new(Vi::new(insert_keybindings, normal_keybindings));

    // 8. すべてを組み立てる
    let mut line_editor = Reedline::create()
        .with_completer(completer)
        .with_menu(ReedlineMenu::EngineCompleter(completion_menu))
        .with_highlighter(highlighter)
        .with_hinter(hinter)
        .with_history(history)
        .with_edit_mode(edit_mode)
        .with_quick_completions(true) // 一意な候補を即補完
        .with_partial_completions(true) // 共通部分まで補完
        .with_ansi_colors(true);

    let prompt = DefaultPrompt::default();

    loop {
        match line_editor.read_line(&prompt) {
            Ok(Signal::Success(buffer)) => {
                let buffer = buffer.trim();
                if buffer == "quit" {
                    break;
                }

                if buffer.is_empty() {
                    continue;
                }

                let parsed_sql = parse_sql(buffer.to_string());
                handle_parsed_sql(
                    &parsed_sql[0],
                    &mut metadata_manager.borrow_mut(),
                    transaction.clone(),
                    &mut index_update_planner,
                );
            }
            Ok(Signal::CtrlC | Signal::CtrlD) => {
                println!("終了します");
                break;
            }
            Ok(_) => {
                break;
            }
            Err(e) => {
                eprintln!("エラー: {e}");
                break;
            }
        }
    }
    Ok(())
    // log_manager.flush();
}
