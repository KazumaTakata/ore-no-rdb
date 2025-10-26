use std::{cell::RefCell, rc::Rc, vec};

use crate::{
    block::BlockId,
    error::ValueNotFound,
    materialize::TempTable,
    plan_v2::PlanV2,
    predicate::{ConstantValue, TableNameAndFieldName},
    record_page::{Layout, TableFieldType, TableSchema},
    record_page_v2::RecordPage,
    scan_v2::{ProductScanV2, ScanV2},
    transaction_v2::TransactionV2,
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
        for i in start_buffer_index..end_buffer_index {
            let block = BlockId::new(file_name.clone(), i as u64);
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
                .unwrap()
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

    fn get_record_id(&self) -> crate::table_scan_v2::RecordID {
        panic!("not implemented");
    }

    fn move_to_record_id(&mut self, record_id: crate::table_scan_v2::RecordID) {
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

    fn copy_records(&mut self, plan: &mut dyn PlanV2) -> TempTable {
        let mut source = plan.open().unwrap();
        let schema = plan.get_schema().clone();

        let mut temp_table = TempTable::new(self.transaction.clone(), schema.clone());

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
        return temp_table;
    }
}

impl PlanV2 for MultiBufferProductPlan {
    fn open(&mut self) -> Result<Box<dyn ScanV2>, ValueNotFound> {
        let left_scan = self.left_plan.open()?;
        let temp_table = self.copy_records(self.right_plan.as_mut());

        return Ok(Box::new(MultiBufferProductScan::new(
            self.transaction.clone(),
            left_scan,
            temp_table,
        )));
    }
}

struct MultiBufferProductScan {
    transaction: Rc<RefCell<TransactionV2>>,
    left_scan: Box<dyn ScanV2>,
    right_scan: Option<Box<dyn ScanV2>>,
    product_scan: Option<ProductScanV2>,
    layout: Layout,
    file_name: String,
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
        let file_size = transaction.borrow().get_size(file_name.clone());

        let file_size = transaction.borrow().get_size(file_name.clone());

        let available_buffer_size = transaction.borrow().get_available_buffer_size() as usize;

        let chunk_size = get_buffer_size_for_product(available_buffer_size, file_size);

        MultiBufferProductScan {
            transaction,
            left_scan,
            right_scan: temp_table.open(),
            product_scan: None,
            layout,
            file_name,
            chunk_size,
            next_block_index: 0,
            file_size,
        }
    }

    fn use_next_chunk(&mut self, left_scan: Option<Box<dyn ScanV2>>) -> bool {
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

        self.left_scan.move_to_before_first().unwrap();

        let current_product_scan = self.product_scan.take();

        self.product_scan = if let Some(left_scan) = left_scan {
            Some(ProductScanV2::new(left_scan, Box::new(right_scan)))
        } else {
            Some(ProductScanV2::new_with_product_scan(
                current_product_scan.unwrap(),
                Box::new(right_scan),
            ))
        };

        self.next_block_index = end_block_index + 1;

        return true;
    }
}
