use std::{cell::RefCell, rc::Rc};

use crate::{
    predicate::TableNameAndFieldName, record_page::TableSchema, scan_v2::ScanV2,
    table_manager_v2::TableManagerV2, table_scan_v2::TableScan, transaction_v2::TransactionV2,
};

pub struct ViewManager {
    table_manager: Rc<RefCell<TableManagerV2>>,
}

impl ViewManager {
    pub fn new(
        is_new: bool,
        table_manager: Rc<RefCell<TableManagerV2>>,
        transaction: Rc<RefCell<TransactionV2>>,
    ) -> Self {
        if is_new {
            let mut view_manager_schema = TableSchema::new();
            view_manager_schema.add_string_field("view_name".to_string(), 10);
            view_manager_schema.add_string_field("view_definition".to_string(), 30);
            let _ = table_manager.borrow_mut().create_table(
                "view_catelog".to_string(),
                &view_manager_schema,
                transaction,
            );
            println!("Created view catalog");
        }
        ViewManager { table_manager }
    }

    pub fn create_view(
        &mut self,
        view_name: String,
        view_definition: String,
        transaction: Rc<RefCell<TransactionV2>>,
    ) {
        let layout = self
            .table_manager
            .borrow()
            .get_layout("view_catelog".to_string(), transaction.clone())
            .unwrap();

        let mut table_scan =
            TableScan::new("view_catelog".to_string(), transaction.clone(), layout);
        table_scan.insert();
        table_scan.set_string("view_name".to_string(), view_name);
        table_scan.set_string("view_definition".to_string(), view_definition);
        table_scan.close();
        transaction.borrow_mut().commit();
    }

    pub fn get_view_definition(
        &self,
        view_name: String,
        transaction: Rc<RefCell<TransactionV2>>,
    ) -> Option<String> {
        let layout = self
            .table_manager
            .borrow()
            .get_layout("view_catelog".to_string(), transaction.clone())
            .unwrap();

        let mut table_scan =
            TableScan::new("view_catelog".to_string(), transaction.clone(), layout);
        while table_scan.next().unwrap() {
            if table_scan.get_string(TableNameAndFieldName::new(None, "view_name".to_string()))
                == Some(view_name.clone())
            {
                let value = table_scan.get_string(TableNameAndFieldName::new(
                    None,
                    "view_definition".to_string(),
                ));

                table_scan.close();
                return value;
            }
        }
        table_scan.close();
        None
    }
}

#[cfg(test)]
mod tests {
    use std::{cell::RefCell, path::Path, rc::Rc};

    use crate::{
        buffer_manager_v2::BufferManagerV2,
        concurrency_manager::LockTable,
        file_manager::{self, FileManager},
        log_manager_v2::LogManagerV2,
        table_manager_v2::TableManagerV2,
        transaction_v2::TransactionV2,
        view_manager::ViewManager,
    };

    #[test]
    fn test_view_mgr() {
        let test_dir = Path::new("test_data");
        let block_size = 400;

        let log_file_name = format!("log_file_{}.txt", uuid::Uuid::new_v4());

        let file_manager = Rc::new(RefCell::new(FileManager::new(test_dir, block_size)));
        let log_manager = Rc::new(RefCell::new(LogManagerV2::new(
            file_manager.clone(),
            log_file_name.clone(),
        )));

        let buffer_manager = Rc::new(RefCell::new(BufferManagerV2::new(
            100,
            file_manager.clone(),
            log_manager.clone(),
        )));

        let lock_table = Rc::new(RefCell::new(LockTable::new()));

        let transaction = Rc::new(RefCell::new(TransactionV2::new(
            1,
            file_manager.clone(),
            buffer_manager.clone(),
            lock_table.clone(),
            log_manager.clone(),
        )));

        let table_manager = Rc::new(RefCell::new(TableManagerV2::new(transaction.clone(), true)));

        let mut view_manager = ViewManager::new(true, table_manager.clone(), transaction.clone());

        view_manager.create_view(
            "my_view".to_string(),
            "SELECT * FROM my_table".to_string(),
            transaction.clone(),
        );

        let definition =
            view_manager.get_view_definition("my_view".to_string(), transaction.clone());

        println!("View definition: {:?}", definition);

        assert_eq!(definition, Some("SELECT * FROM my_table".to_string()));
    }
}
