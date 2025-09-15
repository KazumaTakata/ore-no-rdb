use std::cmp::max;

use pest::pratt_parser::Op;

use crate::{
    buffer_manager::BufferList,
    record_page::TableSchema,
    transaction::{self, Transaction},
};

#[derive(Debug, Clone)]
pub enum ConstantValue {
    String(String),
    Number(i32),
    Null,
}

#[derive(Debug, Clone)]
pub struct Constant {
    pub value: ConstantValue,
}

impl Constant {
    pub fn new(value: ConstantValue) -> Constant {
        Constant { value }
    }

    pub fn equals(&self, value: ConstantValue) -> bool {
        match value {
            ConstantValue::String(s) => match self.value {
                ConstantValue::String(ref str) => {
                    if str == &s {
                        return true;
                    } else {
                        return false;
                    }
                }
                ConstantValue::Number(_n) => return false,
                ConstantValue::Null => return false,
            },
            ConstantValue::Number(n) => match self.value {
                ConstantValue::String(ref _str) => return false,
                ConstantValue::Number(m) => {
                    if n == m {
                        return true;
                    } else {
                        return false;
                    }
                }
                ConstantValue::Null => return false,
            },
            ConstantValue::Null => return false,
        }
    }

    pub fn compare_to(&self, value: ConstantValue) -> std::cmp::Ordering {
        match value {
            ConstantValue::String(s) => match self.value {
                ConstantValue::String(ref str) => {
                    if str == &s {
                        return std::cmp::Ordering::Equal;
                    } else if str < &s {
                        return std::cmp::Ordering::Less;
                    } else {
                        return std::cmp::Ordering::Greater;
                    }
                }
                _ => {
                    panic!("Cannot compare String with non-String value")
                }
            },
            ConstantValue::Number(n) => match self.value {
                ConstantValue::Number(m) => {
                    if n == m {
                        return std::cmp::Ordering::Equal;
                    } else if m < n {
                        return std::cmp::Ordering::Less;
                    } else {
                        return std::cmp::Ordering::Greater;
                    }
                }
                _ => {
                    panic!("Cannot compare Number with non-Number value")
                }
            },
            _ => {
                panic!("Cannot compare Null value")
            }
        }
    }
}

#[derive(Debug, Clone)]
pub enum ExpressionValue {
    FieldName(String),
    Constant(Constant),
}
