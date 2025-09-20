use std::{cell::RefCell, rc::Rc};

use crate::{
    error::ValueNotFound,
    materialize::{self, MaterializePlan, TempTable},
    plan_v2::PlanV2,
    predicate::{Constant, TableNameAndFieldName},
    record_page::TableSchema,
    scan_v2::ScanV2,
    table_scan_v2::RecordID,
    transaction_v2::TransactionV2,
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
        sort_fields: Vec<TableNameAndFieldName>,
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

    fn copy(
        &self,
        src_scan: &mut dyn ScanV2,
        dest: &mut dyn ScanV2,
    ) -> Result<bool, ValueNotFound> {
        dest.insert();

        for field in self.table_schema.fields.iter() {
            let value = src_scan.get_value(TableNameAndFieldName::new(None, field.clone()));
            if let Some(inner_value) = value {
                dest.set_value(field.clone(), inner_value);
            } else {
                return Err(ValueNotFound::new(field.clone(), None));
            }
        }

        return src_scan.next();
    }

    fn do_merge_iteration(
        &mut self,
        runs: &mut Vec<TempTable>,
    ) -> Result<Vec<TempTable>, ValueNotFound> {
        let mut new_runs: Vec<TempTable> = Vec::new();

        while runs.len() > 1 {
            let mut run1 = runs.remove(0);
            let mut run2 = runs.remove(0);
            let merged_run = self.merge_two_runs(&mut run1, &mut run2)?;
            new_runs.push(merged_run);
        }

        if runs.len() == 1 {
            new_runs.push(runs.remove(0));
        }

        return Ok(new_runs);
    }

    fn split_into_runs(
        &mut self,
        src_scan: &mut dyn ScanV2,
    ) -> Result<Vec<TempTable>, ValueNotFound> {
        let mut temp_tables: Vec<TempTable> = Vec::new();
        src_scan.move_to_before_first();

        if !src_scan.next()? {
            return Ok(temp_tables); // No records to sort
        }

        // Start a new run
        let mut current_temp_table =
            TempTable::new(self.transaction.clone(), self.table_schema.clone());

        let mut current_scan = current_temp_table.open();

        while self.copy(src_scan, &mut *current_scan)? {
            if self.comparator.compare(src_scan, &mut *current_scan)? == std::cmp::Ordering::Less {
                current_scan.close();
                temp_tables.push(current_temp_table);
                current_temp_table =
                    TempTable::new(self.transaction.clone(), self.table_schema.clone());
                current_scan = current_temp_table.open();
            }
        }

        current_scan.close();
        return Ok(temp_tables);
    }

    fn merge_two_runs(
        &mut self,
        temp_table_1: &mut TempTable,
        temp_table_2: &mut TempTable,
    ) -> Result<TempTable, ValueNotFound> {
        // Implementation for merging two sorted runs
        // This is a placeholder; actual implementation would depend on the specific requirements

        let mut merged_table = TempTable::new(self.transaction.clone(), self.table_schema.clone());
        let mut scan1 = temp_table_1.open();
        let mut scan2 = temp_table_2.open();

        let mut destination_result = merged_table.open();

        let mut has_more_data_1 = scan1.next();
        let mut has_more_data_2 = scan2.next();

        while has_more_data_1.clone()? && has_more_data_2.clone()? {
            if self.comparator.compare(&mut *scan1, &mut *scan2)? != std::cmp::Ordering::Less {
                has_more_data_1 = self.copy(&mut *scan1, &mut *destination_result);
            } else {
                has_more_data_2 = self.copy(&mut *scan2, &mut *destination_result);
            }
        }

        if has_more_data_1.clone()? {
            while has_more_data_1.clone()? {
                has_more_data_1 = self.copy(&mut *scan1, &mut *destination_result);
            }
        } else if has_more_data_2.clone()? {
            while has_more_data_2.clone()? {
                has_more_data_2 = self.copy(&mut *scan2, &mut *destination_result);
            }
        }

        scan1.close();
        scan2.close();
        destination_result.close();

        return Ok(merged_table);
    }
}

impl PlanV2 for SortPlan {
    fn open(&mut self) -> Result<Box<dyn ScanV2>, ValueNotFound> {
        let mut src_scan = self.plan.open()?;
        let runs = self.split_into_runs(&mut *src_scan);
        src_scan.close();

        let mut inner_runs = runs?;

        while inner_runs.len() > 2 {
            inner_runs = self.do_merge_iteration(&mut inner_runs)?;
        }

        let sort_scan = SortScan::new(&mut inner_runs, self.comparator.clone())?;

        return Ok(Box::new(sort_scan));
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

    fn blocks_accessed(&self) -> u32 {
        // TODO: 所有権の問題で実装できていない
        return 10;
    }
}

#[derive(Clone)]
struct RecordComparator {
    field_name_list: Vec<TableNameAndFieldName>,
}

impl RecordComparator {
    pub fn new(field_name_list: Vec<TableNameAndFieldName>) -> Self {
        RecordComparator { field_name_list }
    }

    pub fn compare(
        &self,
        scan1: &mut dyn ScanV2,
        scan2: &mut dyn ScanV2,
    ) -> Result<std::cmp::Ordering, ValueNotFound> {
        for field_name in &self.field_name_list {
            let value1 = scan1.get_value(field_name.clone());
            let value2 = scan2.get_value(field_name.clone());

            if let (Some(inner_value1), Some(inner_value2)) = (value1, value2) {
                let val1 = Constant::new(inner_value1);
                let val2 = Constant::new(inner_value2);

                match val1.compare_to(val2.value) {
                    std::cmp::Ordering::Equal => continue,
                    std::cmp::Ordering::Less => return Ok(std::cmp::Ordering::Less),
                    std::cmp::Ordering::Greater => return Ok(std::cmp::Ordering::Greater),
                }
            } else {
                return Err(ValueNotFound::new(
                    field_name.field_name.clone(),
                    field_name.table_name.clone(),
                ));
            }
        }
        Ok(std::cmp::Ordering::Equal)
    }
}

#[derive(PartialEq)]
enum CurrentScan {
    Scan1,
    Scan2,
    None,
}

struct SortScan {
    scan1: Box<dyn ScanV2>,
    scan2: Option<Box<dyn ScanV2>>,
    current_scan: CurrentScan,
    has_more_data_1: bool,
    has_more_data_2: bool,
    comparator: RecordComparator,
}

impl SortScan {
    pub fn new(
        runs: &mut Vec<TempTable>,
        comparator: RecordComparator,
    ) -> Result<Self, ValueNotFound> {
        let mut scan1 = runs.get_mut(0).unwrap().open();
        let has_more_data_1 = scan1.next()?;

        let (scan2, has_more_data_2) = if runs.len() > 1 {
            let mut scan2 = runs.get_mut(1).unwrap().open();
            let has_more_data_2 = scan2.next()?;

            (Some(scan2), has_more_data_2)
        } else {
            (None, false)
        };

        Ok(SortScan {
            scan1,
            scan2,
            has_more_data_1,
            has_more_data_2,
            comparator,
            current_scan: CurrentScan::None,
        })
    }
}

impl ScanV2 for SortScan {
    fn move_to_before_first(&mut self) -> Result<(), ValueNotFound> {
        self.scan1.move_to_before_first();
        let has_more_data_1 = self.scan1.next();

        match has_more_data_1 {
            Err(e) => return Err(e),
            Ok(inner_has_more_data_1) => {
                self.has_more_data_1 = inner_has_more_data_1;
            }
        }

        if let Some(scan2) = &mut self.scan2 {
            scan2.move_to_before_first();
            let has_more_data_2 = scan2.next();
            match has_more_data_2 {
                Err(e) => return Err(e),
                Ok(inner_has_more_data_2) => {
                    self.has_more_data_2 = inner_has_more_data_2;
                    return Ok(());
                }
            }
        } else {
            return Ok(());
        }
    }

    fn next(&mut self) -> Result<bool, ValueNotFound> {
        if self.current_scan == CurrentScan::Scan1 {
            self.has_more_data_1 = self.scan1.next()?;
        } else if self.current_scan == CurrentScan::Scan2 {
            if let Some(scan2) = &mut self.scan2 {
                self.has_more_data_2 = scan2.next()?;
            }
        }

        if (!self.has_more_data_1) && (!self.has_more_data_2 || self.scan2.is_none()) {
            self.current_scan = CurrentScan::None;
            return Ok(false);
        } else if self.has_more_data_1 && self.has_more_data_2 {
            if let Some(scan2) = &mut self.scan2 {
                let compare_value = self.comparator.compare(&mut *self.scan1, &mut **scan2)?;
                if compare_value != std::cmp::Ordering::Less {
                    self.current_scan = CurrentScan::Scan1;
                } else {
                    self.current_scan = CurrentScan::Scan2;
                }
            }
        } else if self.has_more_data_1 {
            self.current_scan = CurrentScan::Scan1;
        } else if self.has_more_data_2 {
            self.current_scan = CurrentScan::Scan2;
        }

        return Ok(true);
    }

    fn get_integer(&mut self, field_name: TableNameAndFieldName) -> Option<i32> {
        if self.current_scan == CurrentScan::Scan1 {
            self.scan1.get_integer(field_name)
        } else if self.current_scan == CurrentScan::Scan2 {
            if let Some(scan2) = &mut self.scan2 {
                scan2.get_integer(field_name)
            } else {
                None
            }
        } else {
            None
        }
    }

    fn get_string(&mut self, field_name: TableNameAndFieldName) -> Option<String> {
        if self.current_scan == CurrentScan::Scan1 {
            self.scan1.get_string(field_name)
        } else if self.current_scan == CurrentScan::Scan2 {
            if let Some(scan2) = &mut self.scan2 {
                scan2.get_string(field_name)
            } else {
                None
            }
        } else {
            None
        }
    }

    fn has_field(&self, field_name: TableNameAndFieldName) -> bool {
        if self.current_scan == CurrentScan::Scan1 {
            self.scan1.has_field(field_name)
        } else if self.current_scan == CurrentScan::Scan2 {
            if let Some(scan2) = &self.scan2 {
                scan2.has_field(field_name)
            } else {
                false
            }
        } else {
            false
        }
    }

    fn get_value(
        &mut self,
        field_name: TableNameAndFieldName,
    ) -> Option<crate::predicate::ConstantValue> {
        if self.current_scan == CurrentScan::Scan1 {
            self.scan1.get_value(field_name)
        } else if self.current_scan == CurrentScan::Scan2 {
            if let Some(scan2) = &mut self.scan2 {
                scan2.get_value(field_name)
            } else {
                None
            }
        } else {
            None
        }
    }

    fn close(&mut self) {
        self.scan1.close();
        if let Some(scan2) = &mut self.scan2 {
            scan2.close();
        }
    }

    fn delete(&mut self) {
        panic!("SortScan does not support delete operation.");
    }

    fn get_record_id(&self) -> RecordID {
        panic!("SortScan does not support get_record_id operation.");
    }
    fn insert(&mut self) {
        panic!("SortScan does not support insert operation.");
    }

    fn move_to_record_id(&mut self, record_id: RecordID) {
        panic!("SortScan does not support move_to_record_id operation.");
    }

    fn set_integer(&mut self, field_name: String, value: i32) {
        panic!("SortScan does not support set_integer operation.");
    }

    fn set_string(&mut self, field_name: String, value: String) {
        panic!("SortScan does not support set_string operation.");
    }

    fn set_value(&mut self, field_name: String, value: crate::predicate::ConstantValue) {
        panic!("SortScan does not support set_value operation.");
    }
}
