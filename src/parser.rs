use std::fs;

use pest::Parser;
use pest_derive::Parser;

use crate::predicate::Predicate;

// #[derive(Parser)]
// #[grammar = "pest/csv.pest"]
// pub struct CSVParser;

#[derive(Parser)]
#[grammar = "pest/sql.pest"]
pub struct SQLParser;

struct QueryData {
    table_name_list: Vec<String>,
    field_name_list: Vec<String>,
    predicate: Predicate,
}

impl QueryData {
    pub fn new(
        table_name_list: Vec<String>,
        field_name_list: Vec<String>,
        predicate: Predicate,
    ) -> Self {
        QueryData {
            table_name_list,
            field_name_list,
            predicate,
        }
    }

    pub fn to_string(&self) -> String {
        let mut result = String::new();
        result.push_str("Tables: ");
        for table in &self.table_name_list {
            result.push_str(&format!("{} ", table));
        }
        result.push_str("\nFields: ");
        for field in &self.field_name_list {
            result.push_str(&format!("{} ", field));
        }
        result.push_str("\nPredicate: ");
        result.push_str(&self.predicate.to_string());
        result
    }
}

pub fn parse_sql() {
    let unparsed_file = fs::read_to_string("sample.sql").expect("cannot read file");

    let file = SQLParser::parse(Rule::sql, &unparsed_file)
        .expect("unsuccessful parse") // unwrap the parse result
        .next()
        .unwrap(); // get and unwrap the `file` rule; never fails

    let mut field_sum: f64 = 0.0;
    let mut record_count: u64 = 0;

    let mut table_name = String::new();
    let mut field_name_vec: Vec<String> = Vec::new();

    for record in file.into_inner() {
        match record.as_rule() {
            Rule::select_sql => {
                let mut table_name_list: Vec<String> = Vec::new();
                let mut field_name_list: Vec<String> = Vec::new();

                // Handle SELECT SQL
                println!("Found SELECT SQL: {:?}", record);
                record
                    .into_inner()
                    .for_each(|inner_value| match inner_value.as_rule() {
                        Rule::table_list => {
                            inner_value.into_inner().for_each(|inner_value| {
                                match inner_value.as_rule() {
                                    Rule::id_token => {
                                        println!("Table name: {}", inner_value.as_str());
                                        table_name = inner_value.as_str().to_string();
                                        table_name_list.push(table_name.clone());
                                    }
                                    _ => {}
                                }
                            });
                        }
                        Rule::select_list => inner_value.into_inner().for_each(|inner_value| {
                            match inner_value.as_rule() {
                                Rule::field => {
                                    field_name_vec.push(inner_value.as_str().to_string());
                                    field_name_list.push(inner_value.as_str().to_string());
                                }
                                _ => {}
                            }
                        }),
                        Rule::predicate => {
                            inner_value.into_inner().for_each(|inner_value| {
                                match inner_value.as_rule() {
                                    Rule::term => {
                                        inner_value.into_inner().for_each(|inner_value| {
                                            match inner_value.as_rule() {
                                                Rule::expression => {
                                                    inner_value.into_inner().for_each(
                                                        |inner_value| match inner_value.as_rule() {
                                                            Rule::field => {
                                                                println!(
                                                                    "Rule::id_token {}",
                                                                    inner_value.as_str()
                                                                );
                                                            }
                                                            Rule::constant => {
                                                                let value = inner_value
                                                                    .as_str()
                                                                    .parse::<f64>()
                                                                    .unwrap();
                                                                field_sum += value;
                                                                record_count += 1;
                                                                println!(
                                                                    "Rule::int_token {}",
                                                                    inner_value.as_str()
                                                                );
                                                            }
                                                            _ => {}
                                                        },
                                                    );
                                                }
                                                _ => {}
                                            }
                                        });
                                    }
                                    _ => {}
                                }
                            });
                        }
                        _ => {}
                    });
            }
            Rule::insert_sql => {
                // Handle INSERT SQL
                println!("Found INSERT SQL: {:?}", record);
                record
                    .into_inner()
                    .for_each(|inner_value| match inner_value.as_rule() {
                        Rule::table_name => {
                            table_name = inner_value.as_str().to_string();
                        }
                        Rule::column_name => field_name_vec.push(inner_value.as_str().to_string()),
                        _ => {}
                    });
            }
            _ => {
                println!("Unexpected rule: {:?}", record.as_rule());
            }
        }
    }

    println!("table_name: {}", table_name);
    println!("field_name: {}", field_name_vec.join(", "));

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
