use std::{cell::RefCell, collections::HashMap, rc::Rc};

use crate::{
    buffer_manager::{self, BufferList},
    file_manager::FileManager,
    record_page,
    scan::{Scan, ScanV2},
    table_manager::TableManager,
    table_manager_v2::TableManagerV2,
    table_scan_v2::TableScan,
    transaction::Transaction,
    transaction_v2::TransactionV2,
};

pub struct StatManagerV2 {
    table_manager: TableManagerV2,
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
    pub fn new(table_manager: TableManagerV2) -> Self {
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
    ) -> StatInfoV2 {
        self.num_calls += 1;
        if let Some(stats) = self.table_stats.get(&table_name) {
            return stats.clone();
        }

        let stat_info =
            self.calc_table_stats(table_name.clone(), transaction.clone(), layout.clone());

        self.table_stats.insert(table_name, stat_info.clone());

        return stat_info;
    }

    fn refresh_table_stats(&mut self, transaction: Rc<RefCell<TransactionV2>>) {
        self.num_calls = 0;
        let table_layout = self
            .table_manager
            .get_layout("table_catalog".to_string(), transaction.clone());

        let mut table_scan = TableScan::new(
            "table_catalog".to_string(),
            transaction.clone(),
            table_layout.clone(),
        );

        while table_scan.next() {
            let table_name = table_scan.get_string("table_name".to_string());

            match table_name {
                Some(name) => {
                    let table_name = name.clone();
                    let layout = self
                        .table_manager
                        .get_layout(table_name.clone(), transaction.clone());
                    let table_stat = self.calc_table_stats(
                        table_name.clone(),
                        transaction.clone(),
                        layout.clone(),
                    );
                    self.table_stats
                        .insert(table_name.clone(), table_stat.clone());
                }
                None => continue,
            }
        }

        table_scan.close();
    }

    pub fn calc_table_stats(
        &mut self,
        table_name: String,
        transaction: Rc<RefCell<TransactionV2>>,
        layout: record_page::Layout,
    ) -> StatInfoV2 {
        let mut num_records = 0;
        let mut num_blocks = 0;

        let mut table_scan =
            TableScan::new(table_name.clone(), transaction.clone(), layout.clone());

        while table_scan.next() {
            num_records += 1;
            num_blocks += (table_scan.get_record_id().get_block_number() + 1) as u32;
        }
        table_scan.close();

        return StatInfoV2 {
            num_records,
            num_blocks,
        };
    }
}
