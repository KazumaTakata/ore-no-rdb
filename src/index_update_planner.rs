use std::{cell::RefCell, ops::Index, rc::Rc};

use crate::{
    error::ValueNotFound,
    metadata_manager::MetadataManager,
    parser::InsertData,
    plan_v2::{PlanV2, TablePlanV2},
    transaction_v2::TransactionV2,
};

struct IndexUpdatePlanner {
    metadata_manager: Rc<RefCell<MetadataManager>>,
}

impl IndexUpdatePlanner {
    pub fn new(metadata_manager: Rc<RefCell<MetadataManager>>) -> Self {
        IndexUpdatePlanner { metadata_manager }
    }

    pub fn execute_insert(
        &self,
        insert_data: InsertData,
        transaction: Rc<RefCell<TransactionV2>>,
    ) -> Result<(), ValueNotFound> {
        let table_name = insert_data.table_name.clone();
        let mut plan = TablePlanV2::new(
            table_name.clone(),
            transaction.clone(),
            &mut self.metadata_manager.borrow_mut(),
        )?;

        let mut update_scan = plan.open()?;
        update_scan.insert();
        let record_id = update_scan.get_record_id();

        let mut indexes = self
            .metadata_manager
            .borrow()
            .get_index_info(table_name, transaction.clone())?;

        insert_data
            .field_name_list
            .iter()
            .enumerate()
            .for_each(|(i, field_name)| {
                let insert_value = insert_data.value_list[i].clone();
                update_scan.set_value(field_name.clone(), insert_value.value.clone());

                let index_info = indexes.get_mut(field_name);

                if let Some(info) = index_info {
                    let mut index = info.open();
                    index.insert(insert_value, record_id.clone());
                    index.close();
                }
            });
        update_scan.close();

        return Ok(());
    }
}

// test

#[cfg(test)]
mod tests {
    use rand::{seq::index, Rng};
    use std::{cell::RefCell, path::Path, rc::Rc};

    use crate::{
        buffer_manager_v2::BufferManagerV2,
        concurrency_manager::LockTable,
        database::Database,
        file_manager::{self, FileManager},
        log_manager,
        log_manager_v2::LogManagerV2,
        parser::parse_sql,
        record_page::TableSchema,
        transaction_v2::TransactionV2,
    };

    use super::*;

    #[test]
    fn test_index_update_planner() -> Result<(), ValueNotFound> {
        let database = Database::new();
        let transaction = database.new_transaction(1);
        let mut metadata_manager = MetadataManager::new(true, transaction.clone())?;

        let mut schema = TableSchema::new();
        schema.add_integer_field("A".to_string());
        schema.add_string_field("B".to_string(), 9);

        let table_name = "test_table".to_string();

        metadata_manager.create_table(table_name.clone(), &schema, transaction.clone());

        let parsed_sql = parse_sql(
            "insert into test_table (A, B) values (44, 'Hello World yay!111')".to_string(),
        )
        .unwrap();

        let insert_data = match parsed_sql {
            crate::parser::ParsedSQL::Insert(q) => q,
            _ => panic!("Expected a Insert variant from parse_sql"),
        };

        metadata_manager.create_index(
            "my_index".to_string(),
            table_name.clone(),
            "A".to_string(),
            transaction.clone(),
        );

        let index_update_planner = IndexUpdatePlanner::new(Rc::new(RefCell::new(metadata_manager)));

        index_update_planner.execute_insert(insert_data, transaction.clone());

        transaction.borrow_mut().commit();

        return Ok(());
    }
}
