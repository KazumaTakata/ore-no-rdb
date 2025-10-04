use std::{collections::HashMap, fs, slice::RChunks, vec};

use pest::{iterators::Pair, Parser};
use pest_derive::Parser;

use crate::{
    predicate::{Constant, ConstantValue, ExpressionValue, TableNameAndFieldName},
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

#[derive(Debug, Clone)]
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

#[derive(Debug, Clone)]
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
    CreateIndex(CreateIndexData),
    Delete(DeleteData),
    Update(UpdateData),
    ShowTables,
    DescribeTable { table_name: String },
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
            ParsedSQL::ShowTables => {
                println!("Parsed Show Tables Command");
            }
            ParsedSQL::DescribeTable { table_name } => {
                println!("Parsed Describe Table Command for table: {}", table_name);
            }

            ParsedSQL::CreateIndex(create_index_data) => {
                println!(
                    "Parsed Create Index Data: \nIndex Name: {}\nTable Name: {}\nField Name: {}",
                    create_index_data.index_name,
                    create_index_data.table_name,
                    create_index_data.field_name
                );
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct CreateTableData {
    pub table_name: String,
    pub schema: TableSchema,
}

#[derive(Debug, Clone)]
pub struct CreateIndexData {
    pub index_name: String,
    pub table_name: String,
    pub field_name: String,
}

use std::fmt;

impl fmt::Display for CreateTableData {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Table: {}\nSchema: {:?}", self.table_name, self.schema)
    }
}

#[derive(Debug, Clone)]
pub struct QueryData {
    pub table_name_list: Vec<String>,
    pub field_name_list: Vec<TableNameAndFieldName>,
    pub predicate: PredicateV2,
    pub order_by_list: Vec<TableNameAndFieldName>,
}

impl QueryData {
    pub fn new(
        table_name_list: Vec<String>,
        field_name_list: Vec<TableNameAndFieldName>,
        predicate: PredicateV2,
        order_by_list: Vec<TableNameAndFieldName>,
    ) -> Self {
        QueryData {
            table_name_list,
            field_name_list,
            predicate,
            order_by_list,
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
            result.push_str(&format!(
                "{} {} ",
                field.table_name.clone().unwrap_or("".to_string()),
                field.field_name
            ));
        }
        result.push_str("\nPredicate: ");
        result.push_str(&self.predicate.to_string());
        result.push_str("\nOrder By: ");
        for order_by in &self.order_by_list {
            result.push_str(&format!(
                "{} {} ",
                order_by.table_name.clone().unwrap_or("".to_string()),
                order_by.field_name
            ));
        }
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
                                    Rule::field => match inner_value.into_inner().next() {
                                        Some(inner_value) => match inner_value.as_rule() {
                                            Rule::qualified_field => {
                                                let mut inner_iter = inner_value.into_inner();
                                                let table_name =
                                                    inner_iter.next().unwrap().as_str();
                                                let field_name =
                                                    inner_iter.next().unwrap().as_str();

                                                let expression = ExpressionV2::new(
                                                    ExpressionValue::TableNameAndFieldName(
                                                        TableNameAndFieldName::new(
                                                            Some(table_name.to_string()),
                                                            field_name.to_string(),
                                                        ),
                                                    ),
                                                );

                                                if lhs.is_none() {
                                                    lhs = Some(expression);
                                                } else {
                                                    rhs = Some(expression);
                                                }
                                            }
                                            Rule::id_token => {
                                                let expression = ExpressionV2::new(
                                                    ExpressionValue::TableNameAndFieldName(
                                                        TableNameAndFieldName::new(
                                                            None,
                                                            inner_value.as_str().to_string(),
                                                        ),
                                                    ),
                                                );

                                                if lhs.is_none() {
                                                    lhs = Some(expression);
                                                } else {
                                                    rhs = Some(expression);
                                                }
                                            }
                                            _ => {}
                                        },
                                        None => {}
                                    },
                                    Rule::constant => match inner_value.into_inner().next() {
                                        Some(inner_value) => match inner_value.as_rule() {
                                            Rule::int_token => {
                                                let value =
                                                    inner_value.as_str().parse::<i32>().unwrap();
                                                let int_constant_value =
                                                    ConstantValue::Number(value);

                                                let constant = Constant::new(int_constant_value);

                                                let expression = ExpressionV2::new(
                                                    ExpressionValue::Constant(constant.clone()),
                                                );

                                                if lhs.is_none() {
                                                    lhs = Some(expression);
                                                } else {
                                                    rhs = Some(expression);
                                                }
                                            }
                                            Rule::string_token => {
                                                let value = inner_value
                                                    .into_inner()
                                                    .find(|p| p.as_rule() == Rule::string_content)
                                                    .map(|p| p.as_str().to_string())
                                                    .unwrap_or_default();
                                                let string_constant_value =
                                                    ConstantValue::String(value.clone());
                                                let constant = Constant::new(string_constant_value);
                                                let expression = ExpressionV2::new(
                                                    ExpressionValue::Constant(constant.clone()),
                                                );

                                                if lhs.is_none() {
                                                    lhs = Some(expression);
                                                } else {
                                                    rhs = Some(expression);
                                                }
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

                let term = TermV2::new(lhs.unwrap(), rhs.unwrap());
                terms.push(term);
            }
            _ => {}
        });

    return Some(PredicateV2::new(terms));
}

fn parse_select_sql(record: Pair<Rule>) -> QueryData {
    let mut table_name_list: Vec<String> = Vec::new();
    let mut field_name_list: Vec<TableNameAndFieldName> = Vec::new();
    let mut order_by_list: Vec<TableNameAndFieldName> = Vec::new();

    let mut predicate: Option<PredicateV2> = None;

    record
        .into_inner()
        .for_each(|inner_value| match inner_value.as_rule() {
            Rule::table_list => {
                inner_value
                    .into_inner()
                    .for_each(|inner_value| match inner_value.as_rule() {
                        Rule::id_token => {
                            let table_name = inner_value.as_str().to_string();
                            table_name_list.push(table_name.clone());
                        }
                        _ => {}
                    });
            }
            Rule::select_list => inner_value
                .into_inner()
                .for_each(|inner_value| match inner_value.as_rule() {
                    Rule::field => {
                        inner_value.into_inner().for_each(|inner_value| {
                            match inner_value.as_rule() {
                                Rule::id_token => {
                                    let field_name = inner_value.as_str().to_string();
                                    field_name_list.push(TableNameAndFieldName::new(
                                        None,
                                        field_name.to_string(),
                                    ));
                                }
                                Rule::qualified_field => {
                                    let mut inner_iter = inner_value.into_inner();
                                    let table_name = inner_iter.next().unwrap().as_str();
                                    let field_name = inner_iter.next().unwrap().as_str();

                                    field_name_list.push(TableNameAndFieldName::new(
                                        Some(table_name.to_string()),
                                        field_name.to_string(),
                                    ));
                                }
                                _ => {}
                            }
                        });
                    }
                    _ => {}
                }),
            Rule::predicate => {
                predicate = parse_predicate(inner_value);
            }
            Rule::order_by_list => {
                inner_value
                    .into_inner()
                    .for_each(|inner_value| match inner_value.as_rule() {
                        Rule::field => {
                            inner_value.into_inner().for_each(|inner_value| {
                                match inner_value.as_rule() {
                                    Rule::id_token => {
                                        let field_name = inner_value.as_str().to_string();
                                        order_by_list.push(TableNameAndFieldName::new(
                                            None,
                                            field_name.to_string(),
                                        ));
                                    }
                                    Rule::qualified_field => {
                                        let mut inner_iter = inner_value.into_inner();
                                        let table_name = inner_iter.next().unwrap().as_str();
                                        let field_name = inner_iter.next().unwrap().as_str();

                                        order_by_list.push(TableNameAndFieldName::new(
                                            Some(table_name.to_string()),
                                            field_name.to_string(),
                                        ));
                                    }
                                    _ => {}
                                }
                            });
                        }
                        _ => {}
                    })
            }
            _ => {}
        });
    let query_data = QueryData::new(
        table_name_list,
        field_name_list,
        predicate.unwrap_or(PredicateV2::new(vec![])),
        order_by_list,
    );

    return query_data;
}

fn parse_insert_sql(record: Pair<Rule>) -> InsertData {
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
                inner_value
                    .into_inner()
                    .for_each(|inner_value| match inner_value.as_rule() {
                        Rule::field => {
                            field_name_vec.push(inner_value.as_str().to_string());
                        }
                        _ => {}
                    });
            }
            Rule::constant_list => {
                inner_value
                    .into_inner()
                    .for_each(|inner_value| match inner_value.as_rule() {
                        Rule::constant => match inner_value.into_inner().next() {
                            Some(inner_value) => match inner_value.as_rule() {
                                Rule::int_token => {
                                    let value = inner_value.as_str().parse::<i32>().unwrap();
                                    let int_constant_value = ConstantValue::Number(value);
                                    let constant = Constant::new(int_constant_value);
                                    constant_list.push(constant);
                                }
                                Rule::string_token => {
                                    let value = inner_value
                                        .into_inner()
                                        .find(|p| p.as_rule() == Rule::string_content)
                                        .map(|p| p.as_str().to_string())
                                        .unwrap_or_default();
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
                    });
            }
            _ => {}
        });

    let insert_data = InsertData::new(table_name.unwrap(), field_name_vec, constant_list);

    return insert_data;
}

fn parse_delete_sql(record: Pair<Rule>) -> DeleteData {
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

    return delete_data;
}

fn parse_update_sql(record: Pair<Rule>) -> UpdateData {
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
                                let value = inner_value.as_str().parse::<i32>().unwrap();
                                let int_constant_value = ConstantValue::Number(value);
                                let constant = Constant::new(int_constant_value);
                                new_value = Some(constant);
                            }
                            Rule::string_token => {
                                let value = inner_value
                                    .into_inner()
                                    .find(|p| p.as_rule() == Rule::string_content)
                                    .map(|p| p.as_str().to_string())
                                    .unwrap_or_default();
                                let string_constant_value = ConstantValue::String(value.clone());
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

    return update_data;
}

fn parse_create_index_sql(record: Pair<Rule>) -> CreateIndexData {
    // Currently not implemented
    let mut index_name: Option<String> = None;
    let mut table_name: Option<String> = None;
    let mut field_name: Option<String> = None;

    record
        .into_inner()
        .for_each(|inner_value| match inner_value.as_rule() {
            Rule::id_token => {
                if index_name.is_none() {
                    index_name = Some(inner_value.as_str().to_string());
                } else if table_name.is_none() {
                    table_name = Some(inner_value.as_str().to_string());
                }
            }
            Rule::field => {
                if field_name.is_none() {
                    field_name = Some(inner_value.as_str().to_string());
                }
            }
            Rule::constant => {}
            _ => {}
        });

    let create_index_data = CreateIndexData {
        index_name: index_name.unwrap(),
        table_name: table_name.unwrap(),
        field_name: field_name.unwrap(),
    };

    return create_index_data;
}

fn parse_create_table_sql(record: Pair<Rule>) -> CreateTableData {
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
                inner_value
                    .into_inner()
                    .for_each(|inner_value| match inner_value.as_rule() {
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
                                        field_length =
                                            Some(inner_value.as_str().parse::<i32>().unwrap());
                                    }

                                    _ => {}
                                }
                            });

                            schema.add_field(field_name, field_type, field_length.unwrap_or(0));
                        }
                        _ => {}
                    });
            }
            _ => {}
        });

    let create_table_data = CreateTableData {
        table_name: table_name.unwrap(),
        schema,
    };

    return create_table_data;
}

pub fn parse_sql(sql: String) -> Vec<ParsedSQL> {
    let file = SQLParser::parse(Rule::sql, &sql)
        .expect("unsuccessful parse") // unwrap the parse result
        .next()
        .unwrap(); // get and unwrap the `file` rule; never fails

    for record in file.into_inner() {
        let mut result: Vec<ParsedSQL> = Vec::new();
        match record.as_rule() {
            Rule::sql_list => {
                record
                    .into_inner()
                    .for_each(|inner_value| match inner_value.as_rule() {
                        Rule::sql_statement => {
                            inner_value.into_inner().for_each(|inner_value| {
                                match inner_value.as_rule() {
                                    Rule::select_sql => {
                                        let select_query = parse_select_sql(inner_value);
                                        result.push(ParsedSQL::Query(select_query));
                                    }

                                    Rule::insert_sql => {
                                        let insert_data = parse_insert_sql(inner_value);
                                        result.push(ParsedSQL::Insert(insert_data));
                                    }

                                    Rule::delete_sql => {
                                        let delete_data = parse_delete_sql(inner_value);
                                        result.push(ParsedSQL::Delete(delete_data));
                                    }

                                    Rule::update_sql => {
                                        let update_data = parse_update_sql(inner_value);
                                        result.push(ParsedSQL::Update(update_data));
                                    }

                                    Rule::create_table_sql => {
                                        let create_table_data = parse_create_table_sql(inner_value);
                                        result.push(ParsedSQL::CreateTable(create_table_data));
                                    }

                                    Rule::create_index_sql => {
                                        let create_index_data = parse_create_index_sql(inner_value);
                                        result.push(ParsedSQL::CreateIndex(create_index_data));
                                    }

                                    Rule::show_tables_sql => {
                                        result.push(ParsedSQL::ShowTables);
                                    }

                                    Rule::describe_table_sql => {
                                        let mut table_name: Option<String> = None;

                                        inner_value.into_inner().for_each(|inner_value| {
                                            match inner_value.as_rule() {
                                                Rule::id_token => {
                                                    table_name =
                                                        Some(inner_value.as_str().to_string());
                                                }
                                                _ => {}
                                            }
                                        });

                                        result.push(ParsedSQL::DescribeTable {
                                            table_name: table_name.unwrap(),
                                        });
                                    }

                                    _ => {}
                                }
                            });
                        }
                        _ => {}
                    });
            }

            _ => {
                return vec![];
            }
        }

        return result;
    }

    return vec![];
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
        parsed_sql[0].debug_print();
    }

    #[test]
    fn test_delete_sql() {
        let sql = "delete from test_table where A = 44".to_string();
        let parsed_sql = parse_sql(sql);
        parsed_sql[0].debug_print();
    }

    #[test]
    fn test_select_join_query() {
        let sql = "select A, B from test_table, test_table2 where C = 'content'".to_string();
        let parsed_sql = parse_sql(sql);
        parsed_sql[0].debug_print();
    }

    #[test]
    fn test_select_table_name() {
        let sql =
            "select test_table.A, B from test_table, test_table2 where C = 'content'".to_string();
        let parsed_sql = parse_sql(sql);
        parsed_sql[0].debug_print();
    }

    #[test]
    fn test_create_table() {
        let sql = "create table posts (title varchar(10), content varchar(10))".to_string();
        let parsed_sql = parse_sql(sql);
        parsed_sql[0].debug_print();
    }

    #[test]
    fn test_insert_sql() {
        let sql = "insert into test_table (A, B) values (44, 'Hello World')".to_string();
        let parsed_sql = parse_sql(sql);
        parsed_sql[0].debug_print();
    }

    #[test]
    fn test_update_sql() {
        let sql = "update test_table set B = 'Updated Value' where A = 44".to_string();
        let parsed_sql = parse_sql(sql);
        parsed_sql[0].debug_print();
    }

    #[test]
    fn test_create_index_sql() {
        let sql = "create index idx_test on test_table (A)".to_string();
        let parsed_sql = parse_sql(sql);
        parsed_sql[0].debug_print();
    }

    #[test]
    fn test_select_order_by() {
        let sql = "select A, B from test_table order by A".to_string();
        let parsed_sql = parse_sql(sql);
        parsed_sql[0].debug_print();
    }
}
