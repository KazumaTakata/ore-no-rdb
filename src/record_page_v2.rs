use crate::{
    block::BlockId,
    record_page::{Layout, RecordType, TableFieldType},
    transaction_v2::TransactionV2,
};

pub struct RecordPage {
    layout: Layout,
    block_id: BlockId,
    transaction: TransactionV2,
}

impl RecordPage {
    pub fn new(transaction: TransactionV2, layout: Layout, block_id: BlockId) -> RecordPage {
        RecordPage {
            transaction,
            layout,
            block_id,
        }
    }

    pub fn get_integer(&mut self, field_name: String, slot_id: i32) -> Option<i32> {
        if !self.layout.has_field(field_name.clone()) {
            return None;
        }

        let offset = self.layout.get_offset(field_name.clone()).unwrap();
        let record_offset = self.get_offset_of_record(slot_id);
        let field_type = self.layout.get_field_type(field_name.clone()).unwrap();

        if field_type != TableFieldType::INTEGER {
            return None;
        }

        let result = self
            .transaction
            .get_integer(self.block_id.clone(), (record_offset + offset) as usize);

        Some(result)
    }

    pub fn get_block_id(&self) -> BlockId {
        self.block_id.clone()
    }

    pub fn set_integer(&mut self, field_name: String, slot_id: i32, value: i32) {
        if !self.layout.has_field(field_name.clone()) {
            return;
        }

        let offset = self.layout.get_offset(field_name.clone()).unwrap();
        let record_offset = self.get_offset_of_record(slot_id);
        let field_type = self.layout.get_field_type(field_name.clone()).unwrap();

        if field_type != TableFieldType::INTEGER {
            return;
        }

        self.transaction.set_integer(
            self.block_id.clone(),
            (record_offset + offset) as usize,
            value,
        );
    }

    fn delete(&mut self, slot_id: i32) {
        let record_offset = self.get_offset_of_record(slot_id);
        self.transaction.set_integer(
            self.block_id.clone(),
            record_offset as usize,
            RecordType::EMPTY as i32,
        );
    }

    pub fn set_string(&mut self, field_name: String, slot_id: i32, value: String) {
        if !self.layout.has_field(field_name.clone()) {
            return;
        }

        let offset = self.layout.get_offset(field_name.clone()).unwrap();
        let record_offset = self.get_offset_of_record(slot_id);
        let field_type = self.layout.get_field_type(field_name.clone()).unwrap();

        if field_type != TableFieldType::VARCHAR {
            return;
        }

        self.transaction.set_string(
            self.block_id.clone(),
            (record_offset + offset) as usize,
            value.as_str(),
        );
    }

    fn search_after(&mut self, slot_id: i32, target_record_type: RecordType) -> Option<i32> {
        let mut next_slot_id = slot_id + 1;
        while self.is_valid_slot_id(next_slot_id) {
            let record_offset = self.get_offset_of_record(next_slot_id);
            let record_type = self
                .transaction
                .get_integer(self.block_id.clone(), record_offset as usize);

            if record_type == target_record_type as i32 {
                return Some(next_slot_id);
            }

            next_slot_id += 1;
        }

        return None;
    }

    pub fn get_string(&mut self, field_name: String, slot_id: i32) -> Option<String> {
        if !self.layout.has_field(field_name.clone()) {
            return None;
        }

        let offset = self.layout.get_offset(field_name.clone()).unwrap();
        let record_offset = self.get_offset_of_record(slot_id);
        let field_type = self.layout.get_field_type(field_name.clone()).unwrap();

        if field_type != TableFieldType::VARCHAR {
            return None;
        }

        let result = self
            .transaction
            .get_string(self.block_id.clone(), (record_offset + offset) as usize);

        Some(result)
    }

    pub fn set_flag(&mut self, slot_id: i32, record_type: RecordType) {
        let record_offset = self.get_offset_of_record(slot_id);
        self.transaction.set_integer(
            self.block_id.clone(),
            record_offset as usize,
            record_type as i32,
        );
    }

    pub fn is_valid_slot_id(&self, slot_id: i32) -> bool {
        return self.get_offset_of_record(slot_id + 1) <= self.transaction.get_block_size() as i32;
    }

    pub fn get_offset_of_record(&self, slot_id: i32) -> i32 {
        slot_id * self.layout.get_slot_size()
    }

    pub fn find_next_after_slot_id(&mut self, slot_id: i32) -> Option<i32> {
        return self.search_after(slot_id, RecordType::USED);
    }

    pub fn insert_after_slot_id(&mut self, slot_id: i32) -> Option<i32> {
        let next_slot_id = self.search_after(slot_id, RecordType::EMPTY);

        if let Some(next_slot_id) = next_slot_id {
            self.set_flag(next_slot_id, RecordType::USED);
            return Some(next_slot_id);
        }

        return None;
    }

    pub fn format(&mut self) {
        let mut slot_id = 0;
        while self.is_valid_slot_id(slot_id) {
            print!(
                "slot_id: {}, slot_size: {}",
                slot_id,
                self.layout.get_slot_size()
            );
            self.set_flag(slot_id, RecordType::EMPTY);
            let schema = &self.layout.schema;

            for field in schema.fields() {
                let field_type = schema.get_field_type(field.clone()).unwrap();
                let offset = self.layout.get_offset(field.clone()).unwrap();

                match field_type {
                    TableFieldType::INTEGER => {
                        self.transaction
                            .set_integer(self.block_id.clone(), offset as usize, 0);
                    }
                    TableFieldType::VARCHAR => {
                        self.transaction
                            .set_string(self.block_id.clone(), offset as usize, "");
                    }
                }
            }

            slot_id += 1;
        }
    }
}
