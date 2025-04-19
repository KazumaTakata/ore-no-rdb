use crate::{record_page::TableSchema, table_scan::TableScan};

#[derive(Debug, Clone)]
enum ConstantValue {
    String(String),
    Number(i32),
}

#[derive(Debug, Clone)]
struct Constant {
    value: ConstantValue,
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
            },
        }
    }
}

enum ExpressionValue {
    FieldName(String),
    Constant(Constant),
}

struct Expression {
    value: ExpressionValue,
}

impl Expression {
    pub fn new(value: ExpressionValue) -> Expression {
        Expression { value }
    }

    pub fn evaluate(&self, scan: &mut dyn Scan) -> Constant {
        match self.value {
            ExpressionValue::FieldName(ref field_name) => {
                let value = scan.get_value(field_name.clone());
                return Constant { value };
            }
            ExpressionValue::Constant(ref constant) => constant.clone(),
        }
    }

    pub fn can_apply_to(&self, schema: TableSchema) -> bool {
        match self.value {
            ExpressionValue::FieldName(ref field) => schema.has_field(field.clone()),
            ExpressionValue::Constant(_) => return true,
        }
    }
}

struct Term {
    lhs: Expression,
    rhs: Expression,
}

impl Term {
    pub fn new(lhs: Expression, rhs: Expression) -> Term {
        Term { lhs, rhs }
    }

    pub fn is_satisfied(&self, value: ConstantValue, scan: &mut dyn Scan) -> bool {
        let lhs = self.lhs.evaluate(scan);
        let rhs = self.rhs.evaluate(scan);
        return lhs.equals(rhs.value.clone());
    }

    pub fn can_apply_to(&self, schema: TableSchema) -> bool {
        self.lhs.can_apply_to(schema.clone()) && self.rhs.can_apply_to(schema)
    }
}

pub trait Scan {
    fn move_to_before_first(&mut self);
    fn next(&mut self) -> bool;
    fn get_integer(&mut self) -> Option<i32>;
    fn get_string(&mut self) -> Option<String>;
    fn get_value(&mut self, field_name: String) -> ConstantValue;
    fn set_integer(&mut self, value: i32);
    fn set_string(&mut self, value: String);
}

pub struct SelectScan {
    scan: Box<dyn Scan>,
}

impl Scan for SelectScan {
    fn move_to_before_first(&mut self) {
        self.scan.move_to_before_first();
    }
    fn next(&mut self) -> bool {
        self.scan.next()
    }

    fn get_integer(&mut self) -> Option<i32> {
        self.scan.get_integer()
    }

    fn get_string(&mut self) -> Option<String> {
        self.scan.get_string()
    }
    fn get_value(&mut self, field_name: String) -> ConstantValue {
        self.scan.get_value(field_name)
    }

    fn set_integer(&mut self, value: i32) {
        self.scan.set_integer(value);
    }

    fn set_string(&mut self, value: String) {
        self.scan.set_string(value);
    }
}
