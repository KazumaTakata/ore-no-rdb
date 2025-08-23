use std::{cell::RefCell, rc::Rc};

use crate::{
    materialize::TempTable, plan_v2::PlanV2, predicate::Constant, record_page::TableSchema,
    scan_v2::ScanV2, transaction_v2::TransactionV2,
};

struct SortPlan {
    transaction: Rc<RefCell<TransactionV2>>,
    plan: Box<dyn PlanV2>,
    comparator: RecordComparator,
    table_schema: TableSchema,
}

impl SortPlan {
    pub fn new(
        transaction: Rc<RefCell<TransactionV2>>,
        plan: Box<dyn PlanV2>,
        sort_fields: Vec<String>,
    ) -> Self {
        let table_schema = plan.get_schema().clone();
        let comparator = RecordComparator::new(sort_fields);

        SortPlan {
            transaction,
            plan,
            comparator,
            table_schema,
        }
    }

    fn copy(&self, src_scan: &mut dyn ScanV2, dest: &mut dyn ScanV2) -> bool {
        dest.insert();

        for field in self.table_schema.fields.iter() {
            let value = src_scan.get_value(field.clone());
            dest.set_value(field.clone(), value);
        }

        return src_scan.next();
    }

    fn split_into_runs(&mut self, src_scan: &mut dyn ScanV2) -> Vec<TempTable> {
        let mut temp_tables: Vec<TempTable> = Vec::new();
        src_scan.move_to_before_first();

        if !src_scan.next() {
            return temp_tables; // No records to sort
        }

        // Start a new run
        let mut current_temp_table =
            TempTable::new(self.transaction.clone(), self.table_schema.clone());

        let mut current_scan = current_temp_table.open();

        while self.copy(src_scan, &mut *current_scan) {
            if self.comparator.compare(src_scan, &mut *current_scan) == std::cmp::Ordering::Less {
                current_scan.close();
                temp_tables.push(current_temp_table);
                current_temp_table =
                    TempTable::new(self.transaction.clone(), self.table_schema.clone());
                current_scan = current_temp_table.open();
            }
        }

        current_scan.close();
        return temp_tables;
    }

    fn merge_two_runs(
        &mut self,
        temp_table_1: &mut TempTable,
        temp_table_2: &mut TempTable,
    ) -> TempTable {
        // Implementation for merging two sorted runs
        // This is a placeholder; actual implementation would depend on the specific requirements

        let mut merged_table = TempTable::new(self.transaction.clone(), self.table_schema.clone());
        let mut scan1 = temp_table_1.open();
        let mut scan2 = temp_table_2.open();

        let mut destination_result = merged_table.open();

        let mut has_more_data_1 = scan1.next();
        let mut has_more_data_2 = scan2.next();

        while has_more_data_1 && has_more_data_2 {
            if self.comparator.compare(&mut *scan1, &mut *scan2) != std::cmp::Ordering::Less {
                has_more_data_1 = self.copy(&mut *scan1, &mut *destination_result);
            } else {
                has_more_data_2 = self.copy(&mut *scan2, &mut *destination_result);
            }
        }

        if has_more_data_1 {
            while has_more_data_1 {
                has_more_data_1 = self.copy(&mut *scan1, &mut *destination_result);
            }
        } else if has_more_data_2 {
            while has_more_data_2 {
                has_more_data_2 = self.copy(&mut *scan2, &mut *destination_result);
            }
        }

        scan1.close();
        scan2.close();
        destination_result.close();

        return merged_table;
    }
}

impl SortPlan {
    fn open(&mut self) {
        let mut src_scan = self.plan.open();
        let runs = self.split_into_runs(&mut *src_scan);
        src_scan.close();

        while runs.len() > 2 {}
    }

    fn records_output(&self) -> u32 {
        self.plan.records_output()
    }

    fn get_distinct_value(&self, field_name: String) -> u32 {
        self.plan.get_distinct_value(field_name)
    }

    fn get_schema(&self) -> &TableSchema {
        &self.table_schema
    }
}

struct RecordComparator {
    field_name_list: Vec<String>,
}

impl RecordComparator {
    pub fn new(field_name_list: Vec<String>) -> Self {
        RecordComparator { field_name_list }
    }

    pub fn compare(
        &mut self,
        scan1: &mut dyn ScanV2,
        scan2: &mut dyn ScanV2,
    ) -> std::cmp::Ordering {
        for field_name in &self.field_name_list {
            let value1 = scan1.get_value(field_name.clone());
            let value2 = scan2.get_value(field_name.clone());

            let val1 = Constant::new(value1);
            let val2 = Constant::new(value2);

            match val1.compare_to(val2.value) {
                std::cmp::Ordering::Equal => continue,
                std::cmp::Ordering::Less => return std::cmp::Ordering::Less,
                std::cmp::Ordering::Greater => return std::cmp::Ordering::Greater,
            }
        }
        std::cmp::Ordering::Equal
    }
}
