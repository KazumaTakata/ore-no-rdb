use std::{cell::RefCell, collections::HashMap, rc::Rc};

use crate::{
    error::ValueNotFound, predicate::TableNameAndFieldName, record_page, scan_v2::ScanV2,
    table_manager_v2::TableManagerV2, table_scan_v2::TableScan, transaction_v2::TransactionV2,
};

pub struct StatManagerV2 {
    table_manager: Rc<RefCell<TableManagerV2>>,
    table_stats: HashMap<String, StatInfoV2>,
    num_calls: u32,
}

#[derive(Debug, Clone)]
pub struct StatInfoV2 {
    num_records: u32,
    num_blocks: u32,
}

impl StatInfoV2 {
    pub fn new(num_records: u32, num_blocks: u32) -> Self {
        StatInfoV2 {
            num_records,
            num_blocks,
        }
    }

    pub fn get_num_records(&self) -> u32 {
        self.num_records
    }
    pub fn get_num_blocks(&self) -> u32 {
        self.num_blocks
    }

    pub fn distinct_value(&self, field_name: String) -> u32 {
        // this is wildly inaccurate
        return 1 + self.num_records / 3;
    }
}

impl StatManagerV2 {
    pub fn new(table_manager: Rc<RefCell<TableManagerV2>>) -> Self {
        StatManagerV2 {
            table_manager,
            table_stats: HashMap::new(),
            num_calls: 0,
        }
    }

    pub fn get_table_stats(
        &mut self,
        table_name: String,
        transaction: Rc<RefCell<TransactionV2>>,
        layout: record_page::Layout,
    ) -> Result<StatInfoV2, ValueNotFound> {
        self.num_calls += 1;

        if self.num_calls > 100 {
            self.refresh_table_stats(transaction.clone())?;
        }

        if let Some(stats) = self.table_stats.get(&table_name) {
            return Ok(stats.clone());
        }

        let stat_info =
            self.calc_table_stats(table_name.clone(), transaction.clone(), layout.clone())?;

        self.table_stats.insert(table_name, stat_info.clone());

        return Ok(stat_info);
    }

    pub fn refresh_table_stats(
        &mut self,
        transaction: Rc<RefCell<TransactionV2>>,
    ) -> Result<(), ValueNotFound> {
        self.num_calls = 0;
        let table_layout = self
            .table_manager
            .borrow()
            .get_layout("table_catalog".to_string(), transaction.clone())?;

        let mut table_scan = TableScan::new(
            "table_catalog".to_string(),
            transaction.clone(),
            table_layout.clone(),
        );

        while table_scan.next()? {
            let table_name =
                table_scan.get_string(TableNameAndFieldName::new(None, "table_name".to_string()));

            match table_name {
                Some(name) => {
                    let table_name = name.clone();
                    let layout = self
                        .table_manager
                        .borrow()
                        .get_layout(table_name.clone(), transaction.clone())?;
                    let table_stat = self.calc_table_stats(
                        table_name.clone(),
                        transaction.clone(),
                        layout.clone(),
                    )?;
                    self.table_stats.insert(table_name.clone(), table_stat);
                }
                None => continue,
            }
        }

        table_scan.close();

        return Ok(());
    }

    pub fn calc_table_stats(
        &mut self,
        table_name: String,
        transaction: Rc<RefCell<TransactionV2>>,
        layout: record_page::Layout,
    ) -> Result<StatInfoV2, ValueNotFound> {
        let mut num_records = 0;
        let mut num_blocks = 0;

        let mut table_scan =
            TableScan::new(table_name.clone(), transaction.clone(), layout.clone());

        while table_scan.next()? {
            num_records += 1;
            num_blocks = (table_scan.get_record_id().get_block_number() + 1) as u32;
        }
        table_scan.close();

        return Ok(StatInfoV2 {
            num_records,
            num_blocks,
        });
    }
}

#[cfg(test)]
mod tests {
    use std::{
        cell::RefCell,
        path::Path,
        rc::Rc,
        sync::{Arc, Mutex},
    };

    use crate::{
        buffer_manager_v2::BufferManagerV2,
        concurrency_manager::LockTable,
        file_manager::FileManager,
        log_manager_v2::LogManagerV2,
    };

    use super::*;

    #[test]
    fn test_stat_v2() {
        let test_dir_name = format!("test_data_{}", uuid::Uuid::new_v4());
        let test_dir = Path::new(&test_dir_name);
        let block_size = 400;

        let log_file_name = format!("log_file_{}.txt", uuid::Uuid::new_v4());

        let file_manager = Arc::new(Mutex::new(FileManager::new(test_dir, block_size)));
        let log_manager = Arc::new(Mutex::new(LogManagerV2::new(
            file_manager.clone(),
            log_file_name.clone(),
        )));

        let buffer_manager = Arc::new(Mutex::new(BufferManagerV2::new(
            100,
            file_manager.clone(),
            log_manager.clone(),
        )));

        let lock_table = Arc::new(Mutex::new(LockTable::new()));

        let transaction = Rc::new(RefCell::new(TransactionV2::new(
            1,
            file_manager.clone(),
            buffer_manager.clone(),
            lock_table.clone(),
            log_manager.clone(),
        )));

        let table_manager = Rc::new(RefCell::new(TableManagerV2::new(transaction.clone(), true)));

        let mut stat_manager = StatManagerV2::new(table_manager.clone());

        let result = stat_manager.refresh_table_stats(transaction.clone());

        let layout = table_manager
            .borrow()
            .get_layout("field_catalog".to_string(), transaction.clone());

        let stat_info = stat_manager
            .get_table_stats(
                "field_catalog".to_string(),
                transaction.clone(),
                layout.unwrap(),
            )
            .unwrap();

        println!("Stat info: {:?}", stat_info);
        assert!(stat_info.get_num_blocks() == 4);
        assert!(stat_info.get_num_records() == 7);
    }
}
