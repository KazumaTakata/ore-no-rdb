use std::{cell::RefCell, rc::Rc};

use crate::{
    metadata_manager::MetadataManager, parser::QueryData, plan_v2::create_query_plan,
    predicate::TableNameAndFieldName, transaction_v2::TransactionV2,
};

pub fn handle_select_query(
    select_query: QueryData,
    metadata_manager: &mut MetadataManager,
    transaction: Rc<RefCell<TransactionV2>>,
) {
    println!("Parsed Query: {:?}", select_query);

    let table_exist = metadata_manager.validate_select_sql(&select_query, transaction.clone());

    if !table_exist {
        println!("Table or field does not exist");
        return;
    }

    let mut plan = create_query_plan(&select_query, transaction.clone(), metadata_manager).unwrap();
    let mut scan = plan.open().unwrap();
    scan.move_to_before_first();

    loop {
        match scan.next() {
            Ok(has_next) => {
                if !has_next {
                    break;
                }
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
            Err(e) => {
                println!("Error during scan: {:?}", e);
                break;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        database::Database,
        page,
        predicate::{Constant, ConstantValue, ExpressionValue},
        predicate_v3::{ExpressionV2, PredicateV2, TermV2},
    };

    use super::*;

    #[test]
    fn test_handle_select_query() {
        let database = Database::new();
        let transaction = database.new_transaction(1);
        let mut metadata_manager = MetadataManager::new(transaction.clone()).unwrap();

        let table_name_and_field_name =
            TableNameAndFieldName::new(Some("table_catalog".to_string()), "table_name".to_string());

        let term = TermV2::new(
            ExpressionV2::new(ExpressionValue::TableNameAndFieldName(
                table_name_and_field_name.clone(),
            )),
            ExpressionV2::new(ExpressionValue::Constant(Constant::new(
                ConstantValue::String("posts".to_string()),
            ))),
        );

        let select_query = QueryData {
            field_name_list: vec![TableNameAndFieldName::new(None, "table_name".to_string())],
            table_name_list: vec!["table_catalog".to_string()],
            predicate: PredicateV2::new(vec![term]),
        };

        handle_select_query(select_query, &mut metadata_manager, transaction);
    }
    #[test]
    fn test_handle_select_query_2() {
        let database = Database::new();
        let transaction = database.new_transaction(1);
        let mut metadata_manager = MetadataManager::new(transaction.clone()).unwrap();

        let table_name_and_field_name =
            TableNameAndFieldName::new(Some("table_catalog".to_string()), "table_name".to_string());

        let term = TermV2::new(
            ExpressionV2::new(ExpressionValue::TableNameAndFieldName(
                table_name_and_field_name.clone(),
            )),
            ExpressionV2::new(ExpressionValue::Constant(Constant::new(
                ConstantValue::String("posts".to_string()),
            ))),
        );

        let select_query = QueryData {
            field_name_list: vec![TableNameAndFieldName::new(None, "table_name".to_string())],
            table_name_list: vec!["field_catalog".to_string(), "table_catalog".to_string()],
            predicate: PredicateV2::new(vec![term]),
        };

        handle_select_query(select_query, &mut metadata_manager, transaction);
    }

    #[test]
    fn test_handle_select_query_3() {
        let database = Database::new();
        let transaction = database.new_transaction(1);
        let mut metadata_manager = MetadataManager::new(transaction.clone()).unwrap();

        let table_name_and_field_name =
            TableNameAndFieldName::new(Some("table_catalog".to_string()), "table_name".to_string());

        let term = TermV2::new(
            ExpressionV2::new(ExpressionValue::TableNameAndFieldName(
                table_name_and_field_name.clone(),
            )),
            ExpressionV2::new(ExpressionValue::Constant(Constant::new(
                ConstantValue::String("posts".to_string()),
            ))),
        );

        let select_query = QueryData {
            field_name_list: vec![TableNameAndFieldName::new(
                Some("table_catalog".to_string()),
                "table_name".to_string(),
            )],
            table_name_list: vec!["field_catalog".to_string(), "table_catalog".to_string()],
            predicate: PredicateV2::new(vec![]),
        };

        handle_select_query(select_query, &mut metadata_manager, transaction);
    }
}
