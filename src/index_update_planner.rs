use std::{cell::RefCell, ops::Index, process::id, rc::Rc};

use rand::seq::index;

use crate::{
    error::ValueNotFound,
    metadata_manager::MetadataManager,
    parser::{DeleteData, InsertData, UpdateData},
    plan_v2::{PlanV2, SelectPlanV2, TablePlanV2},
    predicate::{Constant, TableNameAndFieldName},
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

    pub fn execute_delete(
        &mut self,
        delete_data: DeleteData,
        transaction: Rc<RefCell<TransactionV2>>,
    ) -> Result<(), ValueNotFound> {
        let table_name = delete_data.table_name.clone();
        let mut plan = TablePlanV2::new(
            table_name.clone(),
            transaction.clone(),
            &mut self.metadata_manager.borrow_mut(),
        )?;

        let mut select_plan = SelectPlanV2::new(Box::new(plan), delete_data.predicate);

        let mut update_scan = select_plan.open()?;

        let mut indexes = self
            .metadata_manager
            .borrow()
            .get_index_info(table_name, transaction.clone())?;

        while update_scan.next()? {
            let record_id = update_scan.get_record_id();

            for (field_name, index_info) in indexes.iter_mut() {
                let value =
                    update_scan.get_value(TableNameAndFieldName::new(None, field_name.clone()));
                let constant = Constant::new(value.unwrap());
                let mut index = index_info.open();
                index.delete(constant, record_id.clone());
                index.close();
            }

            update_scan.delete();
        }
        update_scan.close();
        return Ok(());
    }

    pub fn execute_modify(
        &mut self,
        update_data: UpdateData,
        transaction: Rc<RefCell<TransactionV2>>,
    ) -> Result<(), ValueNotFound> {
        let table_name = update_data.table_name.clone();
        let field_name = update_data.field_name.clone();

        let table_plan = TablePlanV2::new(
            table_name.clone(),
            transaction.clone(),
            &mut self.metadata_manager.borrow_mut(),
        )
        .unwrap();

        let mut select_plan = SelectPlanV2::new(Box::new(table_plan), update_data.predicate);

        let mut index_info_hash = self
            .metadata_manager
            .borrow()
            .get_index_info(table_name.clone(), transaction.clone())
            .unwrap();

        let index_info = index_info_hash.get_mut(&field_name);

        let mut index = match index_info {
            Some(info) => Some(info.open()),
            None => None,
        };

        let mut update_scan = select_plan.open()?;

        while update_scan.next()? {
            let new_value = update_data.new_value.clone();
            let old_value =
                update_scan.get_value(TableNameAndFieldName::new(None, field_name.clone()));
            update_scan.set_value(update_data.field_name.clone(), new_value.value.clone());

            if let Some(idx) = index.as_mut() {
                let record_id = update_scan.get_record_id();
                idx.delete(Constant::new(old_value.unwrap()), record_id.clone());
                idx.insert(new_value, record_id.clone());
            }
        }

        if let Some(idx) = index.as_mut() {
            idx.close();
        }

        update_scan.close();
        return Ok(());
    }

    pub fn execute_create_table(
        &mut self,
        table_name: String,
        schema: &crate::record_page::TableSchema,
        transaction: Rc<RefCell<TransactionV2>>,
    ) -> Result<(), ValueNotFound> {
        self.metadata_manager
            .borrow_mut()
            .create_table(table_name, schema, transaction);
        return Ok(());
    }

    pub fn execute_create_index(
        &mut self,
        index_name: String,
        table_name: String,
        field_name: String,
        transaction: Rc<RefCell<TransactionV2>>,
    ) -> Result<(), ValueNotFound> {
        self.metadata_manager.borrow_mut().create_index(
            index_name,
            table_name,
            field_name,
            transaction,
        );
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
        let mut metadata_manager = MetadataManager::new(transaction.clone())?;

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
