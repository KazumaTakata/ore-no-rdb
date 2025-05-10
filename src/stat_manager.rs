use std::collections::HashMap;

use crate::{
    buffer_manager::{self, BufferList},
    file_manager::FileManager,
    record_page,
    scan::Scan,
    table_manager::TableManager,
    table_scan::TableScan,
    transaction::Transaction,
};

pub struct StatManager {
    table_manager: TableManager,
    table_stats: HashMap<String, StatInfo>,
    num_calls: u32,
}

#[derive(Debug, Clone)]
pub struct StatInfo {
    num_records: u32,
    num_blocks: u32,
}

impl StatInfo {
    pub fn new(num_records: u32, num_blocks: u32) -> Self {
        StatInfo {
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

impl StatManager {
    pub fn new(table_manager: TableManager) -> Self {
        StatManager {
            table_manager,
            table_stats: HashMap::new(),
            num_calls: 0,
        }
    }

    pub fn get_table_stats(
        &mut self,
        table_name: String,
        transaction: &mut Transaction,
        file_manager: &mut FileManager,
        layout: record_page::Layout,
        buffer_list: &mut BufferList,
        buffer_manager: &mut buffer_manager::BufferManager,
    ) -> StatInfo {
        self.num_calls += 1;
        if let Some(stats) = self.table_stats.get(&table_name) {
            return stats.clone();
        }

        let stat_info = self.calc_table_stats(
            table_name.clone(),
            transaction,
            file_manager,
            layout.clone(),
            buffer_list,
            buffer_manager,
        );

        self.table_stats.insert(table_name, stat_info.clone());

        return stat_info;
    }

    fn refresh_table_stats(
        &mut self,
        table_name: String,
        transaction: &mut Transaction,
        file_manager: &mut FileManager,
        layout: record_page::Layout,
        buffer_list: &mut BufferList,
        buffer_manager: &mut buffer_manager::BufferManager,
    ) {
        self.num_calls = 0;
        let table_layout = self.table_manager.get_layout(
            "table_catalog".to_string(),
            transaction,
            file_manager,
            buffer_list,
            buffer_manager,
        );

        let mut table_scan = TableScan::new(
            "table_catalog".to_string(),
            transaction,
            file_manager,
            table_layout.clone(),
            buffer_list,
        );

        while table_scan.next(file_manager, buffer_list, transaction) {
            let table_name =
                table_scan.get_string(transaction, buffer_list, "table_name".to_string());

            match table_name {
                Some(name) => {
                    let table_name = name.clone();
                    let layout = self.table_manager.get_layout(
                        table_name.clone(),
                        transaction,
                        file_manager,
                        buffer_list,
                        buffer_manager,
                    );
                    let table_stat = self.calc_table_stats(
                        table_name.clone(),
                        transaction,
                        file_manager,
                        layout.clone(),
                        buffer_list,
                        buffer_manager,
                    );

                    self.table_stats
                        .insert(table_name.clone(), table_stat.clone());
                }
                None => continue,
            }
        }

        table_scan.close(transaction, buffer_list, buffer_manager);
    }

    pub fn calc_table_stats(
        &mut self,
        table_name: String,
        transaction: &mut Transaction,
        file_manager: &mut FileManager,
        layout: record_page::Layout,
        buffer_list: &mut BufferList,
        buffer_manager: &mut buffer_manager::BufferManager,
    ) -> StatInfo {
        let mut num_records = 0;
        let mut num_blocks = 0;

        let mut table_scan = TableScan::new(
            table_name.clone(),
            transaction,
            file_manager,
            layout.clone(),
            buffer_list,
        );

        while table_scan.next(file_manager, buffer_list, transaction) {
            num_records += 1;
            num_blocks += (table_scan.get_record_id().get_block_number() + 1) as u32;
        }
        table_scan.close(transaction, buffer_list, buffer_manager);

        return StatInfo {
            num_records,
            num_blocks,
        };
    }
}
