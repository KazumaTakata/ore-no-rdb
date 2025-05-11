use std::{fs, slice::RChunks};

use pest::Parser;
use pest_derive::Parser;

use crate::predicate::{Constant, ConstantValue, Expression, ExpressionValue, Predicate, Term};

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

pub struct QueryData {
    pub table_name_list: Vec<String>,
    pub field_name_list: Vec<String>,
    pub predicate: Predicate,
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

pub fn parse_sql() -> Option<QueryData> {
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

                let mut predicate: Option<Predicate> = None;

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
                                let mut terms: Vec<Term> = Vec::new();

                                match inner_value.as_rule() {
                                    Rule::term => {
                                        let mut lhs = None;
                                        let mut rhs = None;

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

                                                                let expression = Expression::new(
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

                                                                let expression = Expression::new(
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

                                        let term = Term::new(lhs.unwrap(), rhs.unwrap());
                                        terms.push(term);
                                    }
                                    _ => {}
                                }

                                predicate = Some(Predicate::new(terms));
                            });
                        }
                        _ => {}
                    });
                let query_data =
                    QueryData::new(table_name_list, field_name_list, predicate.unwrap());

                println!("Query Data: \n{}", query_data.to_string());

                return Some(query_data);
            }
            Rule::insert_sql => {
                // Handle INSERT SQL
                return None;
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
                return None;
                println!("Unexpected rule: {:?}", record.as_rule());
            }
        }
    }

    return None;
    println!("table_name: {}", table_name);
    println!("field_name: {}", field_name_vec.join(", "));
}
