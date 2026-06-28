use std::{cell::RefCell, rc::Rc, vec};

use crate::{
    storage::block::BlockId,
    error::ValueNotFound,
    materialize::{MaterializePlan, TempTable},
    plan_v2::{PlanTreeNodeForDebug, PlanV2},
    predicate::{ConstantValue, TableNameAndFieldName},
    record::record_page::{Layout, TableFieldType, TableSchema},
    record::record_page_v2::RecordPage,
    record::scan_v2::{ProductScanV2, ScanV2},
    tx::transaction_v2::TransactionV2,
};

fn get_buffer_size_for_sorting(available_buffer_size: usize, table_size: usize) -> usize {
    let available_buffer_size = available_buffer_size - 2;

    if available_buffer_size <= 1 {
        return 1;
    }

    let mut best_buffer_size = usize::MAX;

    let mut index = 1;

    while best_buffer_size > available_buffer_size {
        index += 1;
        best_buffer_size = (table_size as f64).powf(1.0 / index as f64).ceil() as usize;
    }

    return best_buffer_size;
}

fn get_buffer_size_for_product(available_buffer_size: usize, table_size: usize) -> usize {
    let available_buffer_size = available_buffer_size - 2;

    if available_buffer_size <= 1 {
        return 1;
    }

    let mut best_buffer_size = usize::MAX;

    let mut index = 1;

    while best_buffer_size > available_buffer_size {
        index += 1;
        best_buffer_size = (table_size as f64).powf(1.0 / index as f64).ceil() as usize;
    }

    return best_buffer_size;
}

struct ChunkScan {
    transaction: Rc<RefCell<TransactionV2>>,
    file_name: String,
    layout: Layout,
    start_buffer_index: usize,
    end_buffer_index: usize,
    current_buffer_index: usize,
    current_slot_id: i32,
    record_page_list: Vec<RecordPage>,
}

impl ChunkScan {
    pub fn new(
        transaction: Rc<RefCell<TransactionV2>>,
        file_name: String,
        layout: Layout,
        start_buffer_index: usize,
        end_buffer_index: usize,
    ) -> ChunkScan {
        let mut record_page_list = vec![];
        for i in start_buffer_index..end_buffer_index + 1 {
            let file_name_with_extension = format!("{}.tbl", file_name);
            let block = BlockId::new(file_name_with_extension, i as u64);
            let record_page = RecordPage::new(transaction.clone(), layout.clone(), block);
            record_page_list.push(record_page);
        }

        ChunkScan {
            transaction: transaction.clone(),
            file_name,
            layout,
            start_buffer_index,
            end_buffer_index,
            current_buffer_index: start_buffer_index,
            record_page_list,
            current_slot_id: -1,
        }
    }

    fn move_to_block(&mut self, block_index: usize) {
        self.current_buffer_index = block_index;
        self.current_slot_id = -1;
    }

    fn get_current_record_page(&mut self) -> &mut RecordPage {
        self.record_page_list
            .get_mut(self.current_buffer_index - self.start_buffer_index)
            .unwrap()
    }
}

impl ScanV2 for ChunkScan {
    fn next(&mut self) -> Result<bool, ValueNotFound> {
        self.current_slot_id = {
            let current_slot_id = self.current_slot_id;
            let record_page = self.get_current_record_page();
            record_page
                .find_next_after_slot_id(current_slot_id)
                .unwrap_or(-1)
        };

        while self.current_slot_id < 0 {
            if self.current_buffer_index == self.end_buffer_index {
                return Ok(false);
            }
            let mut record_page = self.get_current_record_page();
            let block_number = record_page.get_block_id().get_block_number();

            self.move_to_block(block_number as usize + 1);

            let current_slot_id = self.current_slot_id;

            record_page = self.get_current_record_page();

            self.current_slot_id = record_page
                .find_next_after_slot_id(current_slot_id)
                .unwrap();
        }

        return Ok(true);
    }

    fn get_integer(&mut self, field_name: crate::predicate::TableNameAndFieldName) -> Option<i32> {
        let current_slot_id = self.current_slot_id;
        let record_page = self.get_current_record_page();
        record_page.get_integer(field_name.field_name, current_slot_id)
    }

    fn get_string(
        &mut self,
        field_name: crate::predicate::TableNameAndFieldName,
    ) -> Option<String> {
        let current_slot_id = self.current_slot_id;
        let record_page = self.get_current_record_page();
        record_page.get_string(field_name.field_name, current_slot_id)
    }

    fn move_to_before_first(&mut self) -> Result<(), ValueNotFound> {
        self.move_to_block(self.start_buffer_index);
        Ok(())
    }

    fn close(&mut self) {
        for i in 0..self.record_page_list.len() {
            let block = BlockId::new(self.file_name.clone(), (self.start_buffer_index + i) as u64);
            self.transaction.borrow_mut().unpin(block);
        }
    }

    fn get_value(
        &mut self,
        field_name: crate::predicate::TableNameAndFieldName,
    ) -> Option<ConstantValue> {
        if self
            .layout
            .schema
            .get_field_type(field_name.field_name.clone())
            == Some(TableFieldType::INTEGER)
        {
            let int_value = self.get_integer(field_name)?;
            return Some(ConstantValue::Number(int_value));
        } else {
            let string_value = self.get_string(field_name)?;
            return Some(ConstantValue::String(string_value));
        }
    }

    fn has_field(&self, field_name: crate::predicate::TableNameAndFieldName) -> bool {
        self.layout.schema.has_field(field_name.field_name.clone())
    }

    fn delete(&mut self) {
        panic!("not implemented");
    }

    fn insert(&mut self) {
        panic!("not implemented");
    }

    fn get_record_id(&self) -> crate::record::table_scan_v2::RecordID {
        panic!("not implemented");
    }

    fn move_to_record_id(&mut self, record_id: crate::record::table_scan_v2::RecordID) {
        panic!("not implemented");
    }

    fn set_integer(&mut self, field_name: String, value: i32) {
        panic!("not implemented");
    }

    fn set_string(&mut self, field_name: String, value: String) {
        panic!("not implemented");
    }

    fn set_value(&mut self, field_name: String, value: ConstantValue) {
        panic!("not implemented");
    }
}

struct MultiBufferProductPlan {
    transaction: Rc<RefCell<TransactionV2>>,
    left_plan: Box<dyn PlanV2>,
    right_plan: Box<dyn PlanV2>,
    schema: TableSchema,
}

impl MultiBufferProductPlan {
    pub fn new(
        transaction: Rc<RefCell<TransactionV2>>,
        left_plan: Box<dyn PlanV2>,
        right_plan: Box<dyn PlanV2>,
    ) -> MultiBufferProductPlan {
        let mut schema = TableSchema::new();

        schema.add_all(left_plan.get_schema().clone());
        schema.add_all(right_plan.get_schema().clone());

        MultiBufferProductPlan {
            transaction,
            left_plan,
            right_plan,
            schema,
        }
    }

    fn copy_records(transaction: Rc<RefCell<TransactionV2>>, plan: &mut dyn PlanV2) -> TempTable {
        let mut source = plan.open().unwrap();
        let schema = plan.get_schema().clone();

        let mut temp_table = TempTable::new(transaction.clone(), schema.clone());

        let mut destination_scan = temp_table.open();

        while source.as_mut().next().unwrap() {
            destination_scan.as_mut().insert();
            for field_name in schema.fields() {
                destination_scan.as_mut().set_value(
                    field_name.clone(),
                    source
                        .as_mut()
                        .get_value(TableNameAndFieldName::new(None, field_name.clone()))
                        .unwrap(),
                );
            }
        }
        transaction.borrow_mut().commit();

        return temp_table;
    }
}

impl PlanV2 for MultiBufferProductPlan {
    fn open(&mut self) -> Result<Box<dyn ScanV2>, ValueNotFound> {
        let left_scan = self.left_plan.open()?;
        let right_plan = self.right_plan.as_mut();
        let temp_table = Self::copy_records(self.transaction.clone(), right_plan);

        return Ok(Box::new(MultiBufferProductScan::new(
            self.transaction.clone(),
            left_scan,
            temp_table,
        )));
    }

    fn blocks_accessed(&self) -> u32 {
        let available_buffer_size = self.transaction.borrow().get_available_buffer_size() as usize;

        let right_plan_block_accessed = {
            let layout = Layout::new(self.right_plan.get_schema().clone());
            let rpb = self.transaction.borrow().get_block_size() as i32 / layout.get_slot_size();
            self.right_plan.records_output() as u32 / rpb as u32
        };

        let number_of_chunks =
            (right_plan_block_accessed as f64 / available_buffer_size as f64).ceil() as u32;

        return self.left_plan.blocks_accessed() * number_of_chunks
            + self.right_plan.blocks_accessed() as u32;
    }

    fn records_output(&self) -> u32 {
        return self.left_plan.records_output() * self.right_plan.records_output();
    }

    fn get_distinct_value(&self, field_name: String) -> u32 {
        if self.left_plan.get_schema().has_field(field_name.clone()) {
            return self.left_plan.get_distinct_value(field_name);
        } else {
            return self.right_plan.get_distinct_value(field_name);
        }
    }

    fn get_schema(&self) -> &TableSchema {
        return &self.schema;
    }

    fn get_child_plans(&self) -> PlanTreeNodeForDebug {
        PlanTreeNodeForDebug {
            current_node_type: "MultiBufferProductPlan".to_string(),
            child_nodes: vec![
                self.left_plan.get_child_plans(),
                self.right_plan.get_child_plans(),
            ],
        }
    }
}

struct MultiBufferProductScan {
    transaction: Rc<RefCell<TransactionV2>>,
    left_scan: Option<Box<dyn ScanV2>>,
    right_scan: Option<Box<dyn ScanV2>>,
    product_scan: Option<ProductScanV2>,
    file_name: String,
    layout: Layout,
    chunk_size: usize,
    next_block_index: usize,
    file_size: usize,
}

impl MultiBufferProductScan {
    pub fn new(
        transaction: Rc<RefCell<TransactionV2>>,
        left_scan: Box<dyn ScanV2>,
        temp_table: TempTable,
    ) -> MultiBufferProductScan {
        let layout = temp_table.get_layout().clone();
        let file_name = temp_table.get_table_name().clone();
        let file_name_with_extension = format!("{}.tbl", file_name);
        let file_size = transaction
            .borrow()
            .get_size(file_name_with_extension.clone());

        let available_buffer_size = transaction.borrow().get_available_buffer_size() as usize;

        let chunk_size = get_buffer_size_for_product(available_buffer_size, file_size);

        MultiBufferProductScan {
            transaction,
            left_scan: Some(left_scan),
            product_scan: None,
            right_scan: None,
            layout,
            file_name,
            chunk_size,
            next_block_index: 0,
            file_size,
        }
    }

    fn use_next_chunk(&mut self) -> bool {
        if let Some(right_scan) = self.right_scan.as_mut() {
            right_scan.close();
        }

        if self.next_block_index >= self.file_size {
            return false;
        }

        let mut end_block_index = self.next_block_index + self.chunk_size - 1;

        if end_block_index >= self.file_size {
            end_block_index = self.file_size - 1;
        }

        let right_scan = ChunkScan::new(
            self.transaction.clone(),
            self.file_name.clone(),
            self.layout.clone(),
            self.next_block_index,
            end_block_index,
        );

        let current_product_scan = self.product_scan.take();

        self.left_scan = if let Some(prod_scan) = current_product_scan {
            Some(prod_scan.left_scan)
        } else {
            self.left_scan.take()
        };

        self.left_scan
            .as_mut()
            .unwrap()
            .move_to_before_first()
            .unwrap();

        self.product_scan = Some(ProductScanV2::new(
            self.left_scan.take().unwrap(),
            Box::new(right_scan),
        ));

        self.product_scan
            .as_mut()
            .unwrap()
            .move_to_before_first()
            .unwrap();

        self.next_block_index = end_block_index + 1;

        return true;
    }
}

impl ScanV2 for MultiBufferProductScan {
    fn next(&mut self) -> Result<bool, ValueNotFound> {
        while !self.product_scan.as_mut().unwrap().next()? {
            if !self.use_next_chunk() {
                return Ok(false);
            }
        }

        return Ok(true);
    }

    fn get_integer(&mut self, field_name: crate::predicate::TableNameAndFieldName) -> Option<i32> {
        let product_scan = self.product_scan.as_mut().unwrap();
        product_scan.get_integer(field_name)
    }

    fn get_string(
        &mut self,
        field_name: crate::predicate::TableNameAndFieldName,
    ) -> Option<String> {
        let product_scan = self.product_scan.as_mut().unwrap();
        product_scan.get_string(field_name)
    }

    fn move_to_before_first(&mut self) -> Result<(), ValueNotFound> {
        self.next_block_index = 0;
        self.use_next_chunk();
        Ok(())
    }

    fn close(&mut self) {
        if let Some(product_scan) = self.product_scan.as_mut() {
            product_scan.close();
        }
    }

    fn get_value(
        &mut self,
        field_name: crate::predicate::TableNameAndFieldName,
    ) -> Option<ConstantValue> {
        let product_scan = self.product_scan.as_mut().unwrap();
        product_scan.get_value(field_name)
    }

    fn has_field(&self, field_name: crate::predicate::TableNameAndFieldName) -> bool {
        if let Some(product_scan) = self.product_scan.as_ref() {
            return product_scan.has_field(field_name);
        } else if let Some(left_scan) = self.left_scan.as_ref() {
            return left_scan.has_field(field_name);
        } else {
            panic!("invalid state");
        }
    }

    fn delete(&mut self) {
        panic!("not implemented");
    }

    fn insert(&mut self) {
        panic!("not implemented");
    }

    fn get_record_id(&self) -> crate::record::table_scan_v2::RecordID {
        panic!("not implemented");
    }

    fn move_to_record_id(&mut self, record_id: crate::record::table_scan_v2::RecordID) {
        panic!("not implemented");
    }
    fn set_integer(&mut self, field_name: String, value: i32) {
        panic!("not implemented");
    }
    fn set_string(&mut self, field_name: String, value: String) {
        panic!("not implemented");
    }
    fn set_value(&mut self, field_name: String, value: ConstantValue) {
        panic!("not implemented");
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::{
        database::Database,
        metadata::metadata_manager::MetadataManager,
        parser::parse_sql,
        plan_v2::{execute_create_table, execute_insert, get_optimized_product_plan, TablePlanV2},
        predicate::ConstantValue,
    };
    use std::path::Path;

    #[test]

    fn test_optimized_product_plan() -> Result<(), ValueNotFound> {
        let directory_path_name = format!("test_data_{}", uuid::Uuid::new_v4());
        let directory_path = Path::new(&directory_path_name);
        let database = Database::new(directory_path);
        let transaction = database.new_transaction(1);
        let mut metadata_manager = MetadataManager::new(transaction.clone())?;

        let create_table_sql =
            "create table test_table_1 (A_1 integer, B_1 varchar(10))".to_string();

        let parsed_sql_list = parse_sql(create_table_sql.clone());

        let create_table_data = match &parsed_sql_list[0] {
            crate::parser::ParsedSQL::CreateTable(q) => q,
            _ => panic!("Expected a CreateTable variant from parse_sql"),
        };

        let result = execute_create_table(
            transaction.clone(),
            &mut metadata_manager,
            create_table_data.clone(),
        );

        if result.is_err() {
            println!("Table already exists");
        }

        let create_table_sql =
            "create table test_table_2 (A_2 integer, B_2 varchar(10))".to_string();

        let parsed_sql_list = parse_sql(create_table_sql.clone());

        let create_table_data = match &parsed_sql_list[0] {
            crate::parser::ParsedSQL::CreateTable(q) => q,
            _ => panic!("Expected a CreateTable variant from parse_sql"),
        };

        let result = execute_create_table(
            transaction.clone(),
            &mut metadata_manager,
            create_table_data.clone(),
        );

        if result.is_err() {
            println!("Table already exists");
        }

        let insert_sql_list_for_table_1 = (0..2)
            .map(|i| {
                format!(
                    "insert into test_table_1 (A_1, B_1) values ({}, 'Hello World{}!')",
                    i, i
                )
            })
            .collect::<Vec<String>>();

        let insert_sql_list_for_table_2 = (0..30)
            .map(|i| {
                format!(
                    "insert into test_table_2 (A_2, B_2) values ({}, 'Hello World!{}!')",
                    i + 1000,
                    i + 1000
                )
            })
            .collect::<Vec<String>>();

        for insert_sql in insert_sql_list_for_table_1.iter() {
            let parsed_sql_list = parse_sql(insert_sql.clone());

            let insert_data = match &parsed_sql_list[0] {
                crate::parser::ParsedSQL::Insert(q) => q,
                _ => panic!("Expected a Insert variant from parse_sql"),
            };

            execute_insert(
                transaction.clone(),
                &mut metadata_manager,
                insert_data.clone(),
            );
        }

        for insert_sql in insert_sql_list_for_table_2.iter() {
            let parsed_sql_list = parse_sql(insert_sql.clone());

            let insert_data = match &parsed_sql_list[0] {
                crate::parser::ParsedSQL::Insert(q) => q,
                _ => panic!("Expected a Insert variant from parse_sql"),
            };

            execute_insert(
                transaction.clone(),
                &mut metadata_manager,
                insert_data.clone(),
            );
        }

        transaction.borrow_mut().commit();

        let database = Database::new(directory_path);
        let transaction = database.new_transaction(1);
        let mut metadata_manager = MetadataManager::new(transaction.clone())?;

        let table_plan_1 = TablePlanV2::new(
            "test_table_1".to_string(),
            transaction.clone(),
            &mut metadata_manager,
        )?;

        let table_plan_2 = TablePlanV2::new(
            "test_table_2".to_string(),
            transaction.clone(),
            &mut metadata_manager,
        )?;

        let left_plan = Box::new(table_plan_1) as Box<dyn PlanV2>;
        let right_plan = Box::new(table_plan_2) as Box<dyn PlanV2>;

        let mut prod_plan = MultiBufferProductPlan::new(transaction.clone(), left_plan, right_plan);

        let mut scan = prod_plan.open()?;

        scan.move_to_before_first()?;

        let mut result = vec![];

        while scan.next()? {
            let a_1_value = scan.get_integer(TableNameAndFieldName::new(
                Some("test_table_1".to_string()),
                "A_1".to_string(),
            ));
            let b_1_value = scan.get_string(TableNameAndFieldName::new(
                Some("test_table_1".to_string()),
                "B_1".to_string(),
            ));
            let a_2_value = scan.get_integer(TableNameAndFieldName::new(None, "A_2".to_string()));
            let b_2_value = scan.get_string(TableNameAndFieldName::new(None, "B_2".to_string()));

            result.push((a_1_value, b_1_value, a_2_value, b_2_value));
        }

        let test_assert_value = (0..21)
            .map(|i| {
                (
                    Some(0),
                    Some(format!("Hello World{}!", 0)),
                    Some(i + 1000),
                    Some(format!("Hello World!{}!", i + 1000)),
                )
            })
            .collect::<Vec<(Option<i32>, Option<String>, Option<i32>, Option<String>)>>();

        let test_assert_value_2 = (0..21)
            .map(|i| {
                (
                    Some(1),
                    Some(format!("Hello World{}!", 1)),
                    Some(i + 1000),
                    Some(format!("Hello World!{}!", i + 1000)),
                )
            })
            .collect::<Vec<(Option<i32>, Option<String>, Option<i32>, Option<String>)>>();

        let test_assert_value_3 = (21..30)
            .map(|i| {
                (
                    Some(0),
                    Some(format!("Hello World{}!", 0)),
                    Some(i + 1000),
                    Some(format!("Hello World!{}!", i + 1000)),
                )
            })
            .collect::<Vec<(Option<i32>, Option<String>, Option<i32>, Option<String>)>>();

        let test_assert_value_4 = (21..30)
            .map(|i| {
                (
                    Some(1),
                    Some(format!("Hello World{}!", 1)),
                    Some(i + 1000),
                    Some(format!("Hello World!{}!", i + 1000)),
                )
            })
            .collect::<Vec<(Option<i32>, Option<String>, Option<i32>, Option<String>)>>();

        let test_assert_values = [
            test_assert_value,
            test_assert_value_2,
            test_assert_value_3,
            test_assert_value_4,
        ]
        .concat();

        assert_eq!(result.len(), test_assert_values.len());
        for (result, test_assert_value) in result.iter().zip(test_assert_values.iter()) {
            assert_eq!(result, test_assert_value);
        }

        return Ok(());
    }
}
