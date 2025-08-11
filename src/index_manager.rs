use std::{cell::RefCell, rc::Rc};

use crate::{
    record_page::{Layout, TableSchema},
    scan_v2::ScanV2,
    stat_manager_v2::StatManagerV2,
    table_manager_v2::TableManagerV2,
    table_scan_v2::TableScan,
    transaction,
    transaction_v2::TransactionV2,
};

struct IndexManager {
    layout: Layout,
    table_manager: Rc<RefCell<TableManagerV2>>,
    stat_manager: Rc<RefCell<StatManagerV2>>,
}

impl IndexManager {
    fn new(
        table_manager: Rc<RefCell<TableManagerV2>>,
        stat_manager: Rc<RefCell<StatManagerV2>>,
        is_new: bool,
        transaction: Rc<RefCell<TransactionV2>>,
    ) -> Self {
        if is_new {
            let field_length = 20; // Example field length
            let mut schema = TableSchema::new();
            schema.add_string_field("index_name".to_string(), field_length);
            schema.add_string_field("table_name".to_string(), field_length);
            schema.add_string_field("field_name".to_string(), field_length);
            table_manager.borrow_mut().create_table(
                "index_catalog".to_string(),
                &schema,
                transaction.clone(),
            );
        }

        let layout = table_manager
            .borrow()
            .get_layout("index_catalog".to_string(), transaction);

        IndexManager {
            layout,
            table_manager,
            stat_manager,
        }
    }

    fn create_index(
        &mut self,
        index_name: String,
        table_name: String,
        field_name: String,
        transaction: Rc<RefCell<TransactionV2>>,
    ) {
        let mut table_scan = TableScan::new(table_name.clone(), transaction, self.layout.clone());
        table_scan.insert();
        table_scan.set_string("index_name".to_string(), index_name.clone());
        table_scan.set_string("table_name".to_string(), table_name.clone());
        table_scan.set_string("field_name".to_string(), field_name.clone());
        table_scan.close();
    }
}
