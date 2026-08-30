use std::{collections::HashMap, str::FromStr, vec};

use pest::{iterators::Pair, Parser};
use pest_derive::Parser;

use crate::{
    query::group_by::AggregateFunctionType,
    query::predicate::{Constant, ConstantValue, ExpressionValue, TableNameAndFieldName},
    query::predicate_v3::{ExpressionV2, PredicateV2, TermV2},
    record::record_page::{TableFieldInfo, TableFieldType, TableSchema},
};

// #[derive(Parser)]
// #[grammar = "pest/csv.pest"]
// pub struct CSVParser;

#[derive(Parser)]
#[grammar = "pest/sql.pest"]
pub struct SQLParser;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InsertData {
    pub table_name: String,
    pub field_name_list: Vec<String>,
    pub value_list: Vec<Constant>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteData {
    pub table_name: String,
    pub predicate: Option<PredicateV2>,
}

impl DeleteData {
    pub fn new(table_name: String, predicate: Option<PredicateV2>) -> Self {
        DeleteData {
            table_name,
            predicate,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateData {
    pub table_name: String,
    pub field_name: String,
    pub new_value: ExpressionV2,
    pub predicate: Option<PredicateV2>,
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParsedSQL {
    Query(QueryData),
    Insert(InsertData),
    CreateTable(CreateTableData),
    CreateIndex(CreateIndexData),
    CreateView(ViewData),
    Delete(DeleteData),
    Update(UpdateData),
    ShowTables,
    DescribeTable { table_name: String },
    Explain(QueryData),
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
            ParsedSQL::CreateView(create_view_data) => {
                println!(
                    "Parsed Create View Data: \nView Name: {}\nView Definition:\n{}",
                    create_view_data.view_name,
                    create_view_data.view_definition.to_string()
                );
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
            ParsedSQL::Explain(query_data) => {
                println!(
                    "Parsed Explain Command for query: \n{}",
                    query_data.to_string()
                );
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateTableData {
    pub table_name: String,
    pub schema: TableSchema,
}

#[derive(Debug, Clone, PartialEq, Eq)]
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AggregateFunctionInfo {
    pub function_type: AggregateFunctionType,
    pub field: TableNameAndFieldName,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ViewData {
    pub view_name: String,
    pub view_definition: QueryData,
}

impl ViewData {
    pub fn new(view_name: String, view_definition: QueryData) -> Self {
        ViewData {
            view_name,
            view_definition,
        }
    }

    pub fn to_string(&self) -> String {
        let mut result = String::new();
        result.push_str(&format!("View Name: {}\n", self.view_name));
        result.push_str("View Definition:\n");
        result.push_str(&self.view_definition.to_string());
        result
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryData {
    pub table_name_list: Vec<String>,
    pub field_name_list: Vec<TableNameAndFieldName>,
    pub predicate: Option<PredicateV2>,
    pub order_by_list: Vec<TableNameAndFieldName>,
    pub group_by_list: Vec<TableNameAndFieldName>,
    pub aggregate_functions: Vec<AggregateFunctionInfo>,
}

impl QueryData {
    pub fn new(
        table_name_list: Vec<String>,
        field_name_list: Vec<TableNameAndFieldName>,
        predicate: Option<PredicateV2>,
        order_by_list: Vec<TableNameAndFieldName>,
        group_by_list: Vec<TableNameAndFieldName>,
        aggregate_functions: Vec<AggregateFunctionInfo>,
    ) -> Self {
        QueryData {
            table_name_list,
            field_name_list,
            predicate,
            order_by_list,
            group_by_list,
            aggregate_functions,
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
        result.push_str("\nGroup By: ");
        for group_by in &self.group_by_list {
            result.push_str(&format!(
                "{} {} ",
                group_by.table_name.clone().unwrap_or("".to_string()),
                group_by.field_name
            ));
        }
        result.push_str("\nAggregate Functions: ");
        for agg in &self.aggregate_functions {
            result.push_str(&format!(
                "{}({} {}) ",
                agg.function_type,
                agg.field.table_name.clone().unwrap_or("".to_string()),
                agg.field.field_name
            ));
        }
        result
    }
}
