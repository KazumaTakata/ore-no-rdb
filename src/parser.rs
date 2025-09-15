use std::{collections::HashMap, fs, slice::RChunks, vec};

use pest::{iterators::Pair, Parser};
use pest_derive::Parser;

use crate::{
    predicate::{Constant, ConstantValue, ExpressionValue},
    predicate_v3::{ExpressionV2, PredicateV2, TermV2},
    record_page::{TableFieldInfo, TableFieldType, TableSchema},
};

// #[derive(Parser)]
// #[grammar = "pest/csv.pest"]
// pub struct CSVParser;

#[derive(Parser)]
#[grammar = "pest/sql.pest"]
pub struct SQLParser;

#[derive(Debug, Clone)]
pub struct InsertData {
    pub table_name: String,
    pub field_name_list: Vec<String>,
    pub value_list: Vec<Constant>,
}

pub struct DeleteData {
    pub table_name: String,
    pub predicate: PredicateV2,
}

impl DeleteData {
    pub fn new(table_name: String, predicate: PredicateV2) -> Self {
        DeleteData {
            table_name,
            predicate,
        }
    }
}

pub struct UpdateData {
    pub table_name: String,
    pub field_name: String,
    pub new_value: Constant,
    pub predicate: PredicateV2,
}

impl UpdateData {
    pub fn new(
        table_name: String,
        field_name: String,
        new_value: Constant,
        predicate: PredicateV2,
    ) -> Self {
        UpdateData {
            table_name,
            field_name,
            new_value,
            predicate,
        }
    }
}

impl InsertData {
    pub fn new(
        table_name: String,
        field_name_list: Vec<String>,
        value_list: Vec<Constant>,
    ) -> Self {
        InsertData {
            table_name,
            field_name_list,
            value_list,
        }
    }

    pub fn to_string(&self) -> String {
        let mut result = String::new();
        result.push_str("Table: ");
        result.push_str(&self.table_name);
        result.push_str("\nFields: ");
        for field in &self.field_name_list {
            result.push_str(&format!("{} ", field));
        }
        result.push_str("\nValues: ");
        for value in &self.value_list {
            result.push_str(&format!("{:?} ", value));
        }
        result
    }
}

pub enum ParsedSQL {
    Query(QueryData),
    Insert(InsertData),
    CreateTable(CreateTableData),
    Delete(DeleteData),
    Update(UpdateData),
}

impl ParsedSQL {
    pub fn debug_print(&self) {
        match self {
            ParsedSQL::Query(query_data) => {
                println!("Parsed Query Data: \n{}", query_data.to_string());
            }
            ParsedSQL::Insert(insert_data) => {
                println!("Parsed Insert Data: \n{}", insert_data.to_string());
            }
            ParsedSQL::CreateTable(create_table_data) => {
                println!(
                    "Parsed Create Table Data: \n{}",
                    create_table_data.to_string()
                );
            }
            ParsedSQL::Delete(delete_data) => {
                println!(
                    "Parsed Delete Data: \nTable: {}\nPredicate: {}",
                    delete_data.table_name,
                    delete_data.predicate.to_string()
                );
            }
            ParsedSQL::Update(update_data) => {
                println!(
                    "Parsed Update Data: \nTable: {}\nField: {}\nNew Value: {:?}\nPredicate: {}",
                    update_data.table_name,
                    update_data.field_name,
                    update_data.new_value,
                    update_data.predicate.to_string()
                );
            }
        }
    }
}

pub struct CreateTableData {
    pub table_name: String,
    pub schema: TableSchema,
}

use std::fmt;

impl fmt::Display for CreateTableData {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Table: {}\nSchema: {:?}", self.table_name, self.schema)
    }
}

pub struct QueryData {
    pub table_name_list: Vec<String>,
    pub field_name_list: Vec<String>,
    pub predicate: PredicateV2,
}

impl QueryData {
    pub fn new(
        table_name_list: Vec<String>,
        field_name_list: Vec<String>,
        predicate: PredicateV2,
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

pub fn parse_predicate(inner_value: Pair<'_, Rule>) -> Option<PredicateV2> {
    let mut terms: Vec<TermV2> = Vec::new();
    inner_value
        .into_inner()
        .for_each(|inner_value| match inner_value.as_rule() {
            Rule::term => {
                let mut lhs: Option<ExpressionV2> = None;
                let mut rhs: Option<ExpressionV2> = None;

                inner_value
                    .into_inner()
                    .for_each(|inner_value| match inner_value.as_rule() {
                        Rule::expression => {
                            inner_value.into_inner().for_each(|inner_value| {
                                match inner_value.as_rule() {
                                    Rule::field => {
                                        println!("Rule::id_token {}", inner_value.as_str());

                                        let expression = inner_value.as_str();

                                        let expression =
                                            ExpressionV2::new(ExpressionValue::FieldName(
                                                inner_value.as_str().to_string(),
                                            ));

                                        if lhs.is_none() {
                                            lhs = Some(expression);
                                        } else {
                                            rhs = Some(expression);
                                        }
                                    }
                                    Rule::constant => {
                                        let value = inner_value.as_str().parse::<i32>().unwrap();
                                        let int_constant_value = ConstantValue::Number(value);

                                        let constant = Constant::new(int_constant_value);

                                        let expression = ExpressionV2::new(
                                            ExpressionValue::Constant(constant.clone()),
                                        );
                                        println!("parsed constant value: {:?}", constant);

                                        rhs = Some(expression);
                                    }
                                    _ => {}
                                }
                            });
                        }
                        _ => {}
                    });

                let term = TermV2::new(lhs.unwrap(), rhs.unwrap());
                terms.push(term);
            }
            _ => {}
        });

    return Some(PredicateV2::new(terms));
}

pub fn parse_sql(sql: String) -> Option<ParsedSQL> {
    let file = SQLParser::parse(Rule::sql, &sql)
        .expect("unsuccessful parse") // unwrap the parse result
        .next()
        .unwrap(); // get and unwrap the `file` rule; never fails

    let mut table_name = String::new();
    let mut field_name_vec: Vec<String> = Vec::new();

    for record in file.into_inner() {
        match record.as_rule() {
            Rule::select_sql => {
                let mut table_name_list: Vec<String> = Vec::new();
                let mut field_name_list: Vec<String> = Vec::new();

                let mut predicate: Option<PredicateV2> = None;

                record
                    .into_inner()
                    .for_each(|inner_value| match inner_value.as_rule() {
                        Rule::table_list => {
                            inner_value.into_inner().for_each(|inner_value| {
                                match inner_value.as_rule() {
                                    Rule::id_token => {
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
                            predicate = parse_predicate(inner_value);
                        }
                        _ => {}
                    });
                let query_data = QueryData::new(
                    table_name_list,
                    field_name_list,
                    predicate.unwrap_or(PredicateV2::new(vec![])),
                );

                return Some(ParsedSQL::Query(query_data));
            }
            Rule::insert_sql => {
                // Handle INSERT SQL
                let mut table_name: Option<String> = None;
                let mut field_name_vec: Vec<String> = Vec::new();
                let mut constant_list: Vec<Constant> = Vec::new();
                record
                    .into_inner()
                    .for_each(|inner_value| match inner_value.as_rule() {
                        Rule::id_token => {
                            table_name = Some(inner_value.as_str().to_string());
                        }
                        Rule::field_list => {
                            inner_value.into_inner().for_each(|inner_value| {
                                match inner_value.as_rule() {
                                    Rule::field => {
                                        field_name_vec.push(inner_value.as_str().to_string());
                                    }
                                    _ => {}
                                }
                            });
                        }
                        Rule::constant_list => {
                            inner_value.into_inner().for_each(|inner_value| {
                                match inner_value.as_rule() {
                                    Rule::constant => match inner_value.into_inner().next() {
                                        Some(inner_value) => match inner_value.as_rule() {
                                            Rule::int_token => {
                                                let value =
                                                    inner_value.as_str().parse::<i32>().unwrap();
                                                let int_constant_value =
                                                    ConstantValue::Number(value);
                                                let constant = Constant::new(int_constant_value);
                                                constant_list.push(constant);
                                            }
                                            Rule::string_token => {
                                                let value = inner_value.as_str().to_string();
                                                let string_constant_value =
                                                    ConstantValue::String(value.clone());
                                                let constant = Constant::new(string_constant_value);
                                                constant_list.push(constant);
                                            }
                                            _ => {}
                                        },
                                        None => {}
                                    },
                                    _ => {}
                                }
                            });
                        }
                        _ => {}
                    });

                let insert_data =
                    InsertData::new(table_name.unwrap(), field_name_vec, constant_list);
                return Some(ParsedSQL::Insert(insert_data));
            }

            Rule::delete_sql => {
                let mut table_name: Option<String> = None;
                let mut predicate: Option<PredicateV2> = None;

                record
                    .into_inner()
                    .for_each(|inner_value| match inner_value.as_rule() {
                        Rule::id_token => {
                            table_name = Some(inner_value.as_str().to_string());
                        }
                        Rule::predicate => {
                            predicate = parse_predicate(inner_value);
                        }
                        _ => {}
                    });

                let delete_data = DeleteData::new(table_name.unwrap(), predicate.unwrap());
                return Some(ParsedSQL::Delete(delete_data));
            }

            Rule::update_sql => {
                let mut table_name: Option<String> = None;
                let mut field_name: Option<String> = None;
                let mut new_value: Option<Constant> = None;
                let mut predicate: Option<PredicateV2> = None;

                record
                    .into_inner()
                    .for_each(|inner_value| match inner_value.as_rule() {
                        Rule::id_token => {
                            table_name = Some(inner_value.as_str().to_string());
                        }
                        Rule::field => {
                            if field_name.is_none() {
                                field_name = Some(inner_value.as_str().to_string());
                            }
                        }
                        Rule::constant => {
                            if new_value.is_none() {
                                inner_value
                                    .into_inner()
                                    .for_each(|inner_value| match inner_value.as_rule() {
                                        Rule::int_token => {
                                            let value =
                                                inner_value.as_str().parse::<i32>().unwrap();
                                            let int_constant_value = ConstantValue::Number(value);
                                            let constant = Constant::new(int_constant_value);
                                            new_value = Some(constant);
                                        }
                                        Rule::string_token => {
                                            let value = inner_value.as_str().to_string();
                                            let string_constant_value =
                                                ConstantValue::String(value.clone());
                                            let constant = Constant::new(string_constant_value);
                                            new_value = Some(constant);
                                        }
                                        _ => {}
                                    });
                            }
                        }
                        Rule::predicate => {
                            predicate = parse_predicate(inner_value);
                        }
                        _ => {}
                    });

                let update_data = UpdateData::new(
                    table_name.unwrap(),
                    field_name.unwrap(),
                    new_value.unwrap(),
                    predicate.unwrap(),
                );
                return Some(ParsedSQL::Update(update_data));
            }

            Rule::create_table_sql => {
                let mut table_name: Option<String> = None;
                let mut schema = TableSchema {
                    fields: Vec::new(),
                    field_infos: HashMap::new(),
                };

                record
                    .into_inner()
                    .for_each(|inner_value| match inner_value.as_rule() {
                        Rule::id_token => {
                            table_name = Some(inner_value.as_str().to_string());
                        }
                        Rule::field_definitions => {
                            let mut table_field_infos: Vec<TableFieldInfo> = vec![];
                            inner_value.into_inner().for_each(|inner_value| {
                                match inner_value.as_rule() {
                                    Rule::field_definition => {
                                        let mut field_name = String::new();
                                        let mut field_type = TableFieldType::INTEGER;
                                        let mut field_length: Option<i32> = None;

                                        inner_value.into_inner().for_each(|inner_value| {
                                            match inner_value.as_rule() {
                                                Rule::id_token => {
                                                    field_name = inner_value.as_str().to_string();
                                                }
                                                Rule::text => {
                                                    field_type = TableFieldType::VARCHAR;
                                                }
                                                Rule::integer => {
                                                    field_type = TableFieldType::INTEGER;
                                                }
                                                Rule::int_token => {
                                                    field_length = Some(
                                                        inner_value
                                                            .as_str()
                                                            .parse::<i32>()
                                                            .unwrap(),
                                                    );
                                                }

                                                _ => {}
                                            }
                                        });

                                        schema.add_field(
                                            field_name,
                                            field_type,
                                            field_length.unwrap_or(0),
                                        );
                                    }
                                    _ => {}
                                }
                            });
                        }
                        _ => {}
                    });

                let create_table_data = CreateTableData {
                    table_name: table_name.unwrap(),
                    schema,
                };
                return Some(ParsedSQL::CreateTable(create_table_data));
            }

            _ => {
                return None;
            }
        }
    }

    return None;
}

#[cfg(test)]
mod tests {
    use std::{cell::RefCell, f32::consts::E, path::Path, rc::Rc};

    use rand::Rng;

    use crate::{
        buffer_manager_v2::BufferManagerV2,
        concurrency_manager::LockTable,
        file_manager::{self, FileManager},
        log_manager,
        log_manager_v2::LogManagerV2,
        predicate::{Constant, ConstantValue, ExpressionValue},
        predicate_v3::{ExpressionV2, TermV2},
        record_page::TableSchema,
        transaction,
    };

    use super::*;

    #[test]
    fn test_plan() {
        let unparsed_file =
            fs::read_to_string("./sql/sample_create_table.sql").expect("cannot read file");
        let parsed_sql = parse_sql(unparsed_file);
        parsed_sql.unwrap().debug_print();
    }

    #[test]
    fn test_delete_sql() {
        let sql = "delete from test_table where A = 44".to_string();
        let parsed_sql = parse_sql(sql);
        parsed_sql.unwrap().debug_print();
    }

    #[test]
    fn test_select_join_query() {
        let sql = "select A, B from test_table, test_table2 where C = D".to_string();
        let parsed_sql = parse_sql(sql);
        parsed_sql.unwrap().debug_print();
    }

    #[test]
    fn test_create_table() {
        let sql = "create table posts (title varchar(10), content varchar(10))".to_string();
        let parsed_sql = parse_sql(sql);
        parsed_sql.unwrap().debug_print();
    }

    #[test]
    fn test_insert_sql() {
        let sql = "insert into test_table (A, B) values (44, 'Hello World')".to_string();
        let parsed_sql = parse_sql(sql);
        parsed_sql.unwrap().debug_print();
    }

    #[test]
    fn test_update_sql() {
        let sql = "update test_table set B = 'Updated Value' where A = 44".to_string();
        let parsed_sql = parse_sql(sql);
        parsed_sql.unwrap().debug_print();
    }
}
