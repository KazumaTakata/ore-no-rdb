use std::{cell::RefCell, collections::HashMap, rc::Rc};

use crate::{
    error::ValueNotFound,
    plan_v2::PlanV2,
    predicate::{Constant, ConstantValue, TableNameAndFieldName},
    record_page::{TableFieldType, TableSchema},
    scan_v2::ScanV2,
    sort_plan::SortPlan,
    transaction_v2::TransactionV2,
};

pub struct GroupByPlan {
    transaction: Rc<RefCell<TransactionV2>>,
    group_fields: Vec<TableNameAndFieldName>,
    aggregate_functions: Rc<RefCell<Vec<Box<dyn AggregateFunction>>>>,
    plan: Box<dyn PlanV2>,
}

impl GroupByPlan {
    pub fn new(
        transaction: Rc<RefCell<TransactionV2>>,
        group_fields: Vec<TableNameAndFieldName>,
        aggregate_functions: Rc<RefCell<Vec<Box<dyn AggregateFunction>>>>,
        plan: Box<dyn PlanV2>,
    ) -> Self {
        let sort_plan = SortPlan::new(transaction.clone(), plan, group_fields.clone());

        let mut table_schema = TableSchema::new();

        for func in aggregate_functions.borrow_mut().iter() {
            table_schema.add_field(func.get_field().field_name, TableFieldType::INTEGER, 0);
        }

        for field in group_fields.iter() {
            table_schema.add(field.field_name.clone(), sort_plan.get_schema().clone());
        }

        GroupByPlan {
            transaction,
            group_fields,
            aggregate_functions,
            plan: Box::new(sort_plan),
        }
    }
}

impl PlanV2 for GroupByPlan {
    fn open(&mut self) -> Result<Box<dyn ScanV2>, ValueNotFound> {
        let source_scan = self.plan.open()?;
        return Ok(Box::new(GroupByScan::new(
            source_scan,
            self.group_fields.clone(),
            self.aggregate_functions.clone(),
        )));
    }

    fn blocks_accessed(&self) -> u32 {
        self.plan.blocks_accessed()
    }

    fn records_output(&self) -> u32 {
        let mut number_of_groups = 1;
        for field in self.group_fields.iter() {
            number_of_groups *= self.plan.get_distinct_value(field.field_name.clone());
        }

        return number_of_groups;
    }

    fn get_distinct_value(&self, field_name: String) -> u32 {
        if (self.get_schema().has_field(field_name.clone())) {
            return self.plan.get_distinct_value(field_name);
        } else {
            return self.records_output();
        }
    }

    fn get_schema(&self) -> &TableSchema {
        self.plan.get_schema()
    }
}

pub trait AggregateFunction {
    fn process_first(&mut self, scan: &mut dyn ScanV2);
    fn process_next(&mut self, scan: &mut dyn ScanV2);
    fn get_field(&self) -> TableNameAndFieldName;
    fn get_value(&self) -> Constant;
}

struct GroupValue {
    values: HashMap<String, Constant>,
}

impl GroupValue {
    pub fn new(scan: &mut dyn ScanV2, fields: Vec<TableNameAndFieldName>) -> Self {
        let mut values = HashMap::new();
        for field in fields.iter() {
            let value = scan.get_value(field.clone()).unwrap();
            values.insert(field.field_name.clone(), Constant::new(value));
        }
        GroupValue { values }
    }

    pub fn get_value(&self, field_name: &String) -> Option<&Constant> {
        self.values.get(field_name)
    }

    pub fn equals(&self, other: &GroupValue) -> bool {
        for (key, value) in self.values.iter() {
            if let Some(other_value) = other.get_value(key) {
                if value.compare_to(other_value.value.clone()) != std::cmp::Ordering::Equal {
                    return false;
                }
            } else {
                return false;
            }
        }
        true
    }
}

struct GroupByScan {
    source_scan: Box<dyn ScanV2>,
    group_fields: Vec<TableNameAndFieldName>,
    aggregate_functions: Rc<RefCell<Vec<Box<dyn AggregateFunction>>>>,
    group_value: Option<GroupValue>,
    more_groups: bool,
}

impl GroupByScan {
    pub fn new(
        source_scan: Box<dyn ScanV2>,
        group_fields: Vec<TableNameAndFieldName>,
        aggregate_functions: Rc<RefCell<Vec<Box<dyn AggregateFunction>>>>,
    ) -> Self {
        GroupByScan {
            source_scan,
            group_fields,
            aggregate_functions,
            group_value: None,
            more_groups: true,
        }
    }
}

impl ScanV2 for GroupByScan {
    fn next(&mut self) -> Result<bool, ValueNotFound> {
        if self.more_groups == false {
            return Ok(false);
        }

        for func in self.aggregate_functions.borrow_mut().iter_mut() {
            func.process_first(self.source_scan.as_mut());
        }

        self.group_value = Some(GroupValue::new(
            self.source_scan.as_mut(),
            self.group_fields.clone(),
        ));

        self.more_groups = self.source_scan.next()?;

        while self.more_groups {
            let current_group_value =
                GroupValue::new(self.source_scan.as_mut(), self.group_fields.clone());
            if !self
                .group_value
                .as_ref()
                .unwrap()
                .equals(&current_group_value)
            {
                break;
            }

            for func in self.aggregate_functions.borrow_mut().iter_mut() {
                func.process_next(self.source_scan.as_mut());
            }

            self.more_groups = self.source_scan.next()?;
        }

        Ok(true)
    }

    fn get_value(&mut self, field_name: TableNameAndFieldName) -> Option<ConstantValue> {
        let value = self.group_fields.iter().find(|f| {
            f.field_name == field_name.field_name && f.table_name == field_name.table_name
        });

        if let Some(_) = value {
            return self
                .group_value
                .as_ref()
                .unwrap()
                .get_value(&field_name.field_name)
                .map(|c| c.value.clone());
        }

        for func in self.aggregate_functions.borrow_mut().iter() {
            if func.get_field().field_name == field_name.field_name
                && func.get_field().field_name == field_name.field_name
            {
                return Some(func.get_value().value.clone());
            }
        }

        panic!("Field not found: {}", field_name.field_name);
    }

    fn get_string(&mut self, field_name: TableNameAndFieldName) -> Option<String> {
        if let Some(ConstantValue::String(s)) = self.get_value(field_name) {
            return Some(s);
        }
        return None;
    }

    fn get_integer(&mut self, field_name: TableNameAndFieldName) -> Option<i32> {
        if let Some(ConstantValue::Number(i)) = self.get_value(field_name) {
            return Some(i);
        }
        return None;
    }

    fn has_field(&self, field_name: TableNameAndFieldName) -> bool {
        for func in self.aggregate_functions.borrow().iter() {
            if func.get_field().field_name == field_name.field_name
                && func.get_field().table_name == field_name.table_name
            {
                return true;
            }
        }

        for field in self.group_fields.iter() {
            if field.field_name == field_name.field_name {
                return true;
            }
        }

        return false;
    }

    fn close(&mut self) {
        self.source_scan.close()
    }

    fn move_to_before_first(&mut self) -> Result<(), ValueNotFound> {
        self.source_scan.move_to_before_first();
        self.more_groups = self.source_scan.next().unwrap();
        return Ok(());
    }

    fn delete(&mut self) {
        panic!("Cannot delete from GroupByScan")
    }

    fn insert(&mut self) {
        panic!("Cannot insert into GroupByScan")
    }

    fn set_integer(&mut self, field_name: String, value: i32) {
        panic!("Cannot set value in GroupByScan")
    }

    fn set_value(&mut self, field_name: String, value: ConstantValue) {
        panic!("Cannot set value in GroupByScan")
    }

    fn set_string(&mut self, field_name: String, value: String) {
        panic!("Cannot set value in GroupByScan")
    }

    fn get_record_id(&self) -> crate::table_scan_v2::RecordID {
        panic!("Cannot get RecordID from GroupByScan")
    }

    fn move_to_record_id(&mut self, record_id: crate::table_scan_v2::RecordID) {
        panic!("Cannot move to RecordID in GroupByScan")
    }
}

pub struct MaxFunction {
    field_name: TableNameAndFieldName,
    max_value: Option<Constant>,
}

impl MaxFunction {
    pub fn new(field_name: TableNameAndFieldName) -> Self {
        MaxFunction {
            field_name,
            max_value: None,
        }
    }
}

impl AggregateFunction for MaxFunction {
    fn process_first(&mut self, scan: &mut dyn ScanV2) {
        let value = scan.get_value(self.field_name.clone()).unwrap();
        self.max_value = Some(Constant::new(value));
    }

    fn process_next(&mut self, scan: &mut dyn ScanV2) {
        let new_value = scan.get_value(self.field_name.clone()).unwrap();
        if (Constant::new(new_value.clone())).compare_to(self.max_value.clone().unwrap().value)
            == std::cmp::Ordering::Greater
        {
            self.max_value = Some(Constant::new(new_value));
        }
    }

    fn get_field(&self) -> TableNameAndFieldName {
        self.field_name.clone()
    }

    fn get_value(&self) -> Constant {
        self.max_value.clone().unwrap()
    }
}
