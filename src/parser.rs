use std::{collections::HashMap, fs, slice::RChunks, vec};

use pest::Parser;
use pest_derive::Parser;

use crate::{
    predicate::{Constant, ConstantValue, Expression, ExpressionValue, Predicate, Term},
    predicate_v3::{ExpressionV2, PredicateV2, TermV2},
    record_page::{TableFieldInfo, TableFieldType, TableSchema},
};

// #[derive(Parser)]
// #[grammar = "pest/csv.pest"]
// pub struct CSVParser;

#[derive(Parser)]
#[grammar = "pest/sql.pest"]
pub struct SQLParser;

pub struct InsertData {
    pub table_name: String,
    pub field_name_list: Vec<String>,
    pub value_list: Vec<Constant>,
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
                            inner_value.into_inner().for_each(|inner_value| {
                                let mut terms: Vec<TermV2> = Vec::new();

                                match inner_value.as_rule() {
                                    Rule::term => {
                                        let mut lhs: Option<ExpressionV2> = None;
                                        let mut rhs: Option<ExpressionV2> = None;

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

                                                                let expression =
                                                                    inner_value.as_str();

                                                                let expression = ExpressionV2::new(
                                                                    ExpressionValue::FieldName(
                                                                        inner_value
                                                                            .as_str()
                                                                            .to_string(),
                                                                    ),
                                                                );

                                                                if lhs.is_none() {
                                                                    lhs = Some(expression);
                                                                } else {
                                                                    rhs = Some(expression);
                                                                }
                                                            }
                                                            Rule::constant => {
                                                                let value = inner_value
                                                                    .as_str()
                                                                    .parse::<i32>()
                                                                    .unwrap();
                                                                let int_constant_value =
                                                                    ConstantValue::Number(value);

                                                                let constant = Constant::new(
                                                                    int_constant_value,
                                                                );

                                                                let expression = ExpressionV2::new(
                                                                    ExpressionValue::Constant(
                                                                        constant.clone(),
                                                                    ),
                                                                );
                                                                println!(
                                                                    "parsed constant value: {:?}",
                                                                    constant
                                                                );

                                                                rhs = Some(expression);
                                                            }
                                                            _ => {}
                                                        },
                                                    );
                                                }
                                                _ => {}
                                            }
                                        });

                                        let term = TermV2::new(lhs.unwrap(), rhs.unwrap());
                                        terms.push(term);
                                    }
                                    _ => {}
                                }

                                predicate = Some(PredicateV2::new(terms));
                            });
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
        stat_manager, transaction,
    };

    use super::*;

    #[test]
    fn test_plan() {
        let unparsed_file =
            fs::read_to_string("sample_create_table.sql").expect("cannot read file");
        let parsed_sql = parse_sql(unparsed_file);
        match parsed_sql.unwrap() {
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
        }
    }
}
