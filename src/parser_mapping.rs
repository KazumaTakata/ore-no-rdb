use crate::{
    parser::{
        self,
        parser::{Expression, FieldType, Predicate, SQLNode, SelectNode, Term},
    },
    query::{
        parser::{
            CreateIndexData, CreateTableData, DeleteData, InsertData, ParsedSQL, QueryData,
            UpdateData, ViewData,
        },
        predicate::{Constant, ConstantValue, ExpressionValue, TableNameAndFieldName},
        predicate_v3::{ExpressionV2, PredicateV2, TermV2},
    },
    record::record_page::{TableFieldInfo, TableSchema},
};

fn sql_node_predicate_to_parsed_sql_predicate(predicate: Predicate) -> PredicateV2 {
    let term = sql_node_predicate_term_to_parsed_sql_predicate_term(predicate.term);

    let Some(predicate) = predicate.next_predicate else {
        return PredicateV2::new(vec![term]);
    };

    let mut predicate = sql_node_predicate_to_parsed_sql_predicate(*predicate);

    predicate.terms.push(term);

    PredicateV2 {
        terms: predicate.terms,
    }
}

fn sql_node_expression_to_parsed_expression(expression: Expression) -> ExpressionV2 {
    match expression {
        Expression::Constant(constant) => match constant {
            parser::parser::Constant::String(value) => ExpressionV2::new(
                ExpressionValue::Constant(Constant::new(ConstantValue::String(value))),
            ),
            parser::parser::Constant::Integer(value) => ExpressionV2::new(
                ExpressionValue::Constant(Constant::new(ConstantValue::Number(value))),
            ),
        },
        Expression::Field(field) => ExpressionV2::new(ExpressionValue::TableNameAndFieldName(
            TableNameAndFieldName {
                table_name: None,
                field_name: field,
            },
        )),
    }
}

fn sql_node_constant_to_parsed_constant(constant: parser::parser::Constant) -> Constant {
    match constant {
        parser::parser::Constant::String(value) => Constant::new(ConstantValue::String(value)),
        parser::parser::Constant::Integer(value) => Constant::new(ConstantValue::Number(value)),
    }
}

fn sql_node_predicate_term_to_parsed_sql_predicate_term(term: Term) -> TermV2 {
    let left_expression = match term.left_expression {
        Expression::Constant(constant) => match constant {
            parser::parser::Constant::String(value) => ExpressionV2::new(
                ExpressionValue::Constant(Constant::new(ConstantValue::String(value))),
            ),
            parser::parser::Constant::Integer(value) => ExpressionV2::new(
                ExpressionValue::Constant(Constant::new(ConstantValue::Number(value))),
            ),
        },
        Expression::Field(field) => ExpressionV2::new(ExpressionValue::TableNameAndFieldName(
            TableNameAndFieldName {
                table_name: None,
                field_name: field,
            },
        )),
    };

    let right_expression = match term.right_expression {
        Expression::Constant(constant) => match constant {
            parser::parser::Constant::String(value) => ExpressionV2::new(
                ExpressionValue::Constant(Constant::new(ConstantValue::String(value))),
            ),
            parser::parser::Constant::Integer(value) => ExpressionV2::new(
                ExpressionValue::Constant(Constant::new(ConstantValue::Number(value))),
            ),
        },
        Expression::Field(field) => ExpressionV2::new(ExpressionValue::TableNameAndFieldName(
            TableNameAndFieldName {
                table_name: None,
                field_name: field,
            },
        )),
    };

    return TermV2::new(left_expression, right_expression);
}

fn sql_select_node_to_parsed_sql(select_node: SelectNode) -> QueryData {
    QueryData {
        table_name_list: select_node.tables,
        field_name_list: select_node
            .fields
            .iter()
            .map(|field| TableNameAndFieldName {
                table_name: None,
                field_name: field.clone(),
            })
            .collect(),
        predicate: select_node
            .predicate
            .map(sql_node_predicate_to_parsed_sql_predicate),
        order_by_list: vec![],
        group_by_list: vec![],
        aggregate_functions: vec![],
    }
}

pub fn sql_node_to_parsed_sql(sql_node: SQLNode) -> ParsedSQL {
    match sql_node {
        SQLNode::SELECT(select_node) => ParsedSQL::Query(QueryData {
            table_name_list: select_node.tables,
            field_name_list: select_node
                .fields
                .iter()
                .map(|field| TableNameAndFieldName {
                    table_name: None,
                    field_name: field.clone(),
                })
                .collect(),
            predicate: select_node
                .predicate
                .map(sql_node_predicate_to_parsed_sql_predicate),
            order_by_list: vec![],
            group_by_list: vec![],
            aggregate_functions: vec![],
        }),
        SQLNode::INSERT(insert_node) => ParsedSQL::Insert(InsertData {
            table_name: insert_node.table,
            field_name_list: insert_node.fields,
            value_list: insert_node
                .constants
                .into_iter()
                .map(sql_node_constant_to_parsed_constant)
                .collect(),
        }),
        SQLNode::DELETE(delete_node) => ParsedSQL::Delete(DeleteData {
            table_name: delete_node.table,
            predicate: delete_node
                .predicate
                .map(sql_node_predicate_to_parsed_sql_predicate),
        }),
        SQLNode::UPDATE(update_node) => ParsedSQL::Update(UpdateData {
            table_name: update_node.table,
            field_name: update_node.field,
            new_value: sql_node_expression_to_parsed_expression(update_node.value),
            predicate: update_node
                .predicate
                .map(sql_node_predicate_to_parsed_sql_predicate),
        }),
        SQLNode::CreateTable(create_table) => ParsedSQL::CreateTable(CreateTableData {
            table_name: create_table.table_name,
            schema: TableSchema {
                fields: create_table
                    .field_defs
                    .iter()
                    .into_iter()
                    .map(|field_def| {
                        (
                            field_def.field_name.clone(),
                            match field_def.field_type {
                                FieldType::INTEGER => TableFieldInfo::INTEGER,
                                FieldType::VARCHAR(value) => TableFieldInfo::VARCHAR(value),
                            },
                        )
                    })
                    .collect(),
            },
        }),
        SQLNode::CreateIndexNode(create_index) => ParsedSQL::CreateIndex(CreateIndexData {
            index_name: create_index.index_name,
            table_name: create_index.table_name,
            field_name: create_index.field_name,
        }),
        SQLNode::CreateViewNode(create_view) => ParsedSQL::CreateView(ViewData {
            view_name: create_view.view_name,
            view_definition: sql_select_node_to_parsed_sql(create_view.query),
        }),
    }
}
