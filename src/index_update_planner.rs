use std::{cell::RefCell, ops::Index, rc::Rc};

use crate::{
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

    pub fn execute_insert(&self, insert_data: InsertData, transaction: Rc<RefCell<TransactionV2>>) {
        let table_name = insert_data.table_name.clone();
        let plan = TablePlanV2::new(
            table_name.clone(),
            transaction.clone(),
            &mut self.metadata_manager.borrow_mut(),
        );

        let mut update_scan = plan.open();
        update_scan.insert();
        let record_id = update_scan.get_record_id();

        let mut indexes = self
            .metadata_manager
            .borrow()
            .get_index_info(table_name, transaction.clone());

        insert_data
            .field_name_list
            .iter()
            .enumerate()
            .for_each(|(i, field_name)| {
                let insert_value = insert_data.value_list[i].clone();
                update_scan.set_value(field_name.clone(), insert_value.value.clone());

                let mut index_info = indexes.get_mut(field_name);

                if let Some(info) = index_info {
                    let mut index = info.open();
                    index.insert(insert_value, record_id.clone());
                    index.close();
                }
            });
        update_scan.close();
    }
}
