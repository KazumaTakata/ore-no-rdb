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

struct StatManager {
    table_manager: TableManager,
    table_stats: HashMap<String, StatInfo>,
    num_calls: u32,
}

#[derive(Debug, Clone)]
struct StatInfo {
    num_records: u32,
    num_blocks: u32,
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

        let stat_info = self.calc_table_stats(
            table_name.clone(),
            transaction,
            file_manager,
            layout.clone(),
            buffer_list,
            buffer_manager,
        );

        self.table_stats.insert(table_name, stat_info);
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
