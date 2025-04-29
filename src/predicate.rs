use crate::{
    buffer_manager::BufferList,
    record_page::TableSchema,
    scan::Scan,
    transaction::{self, Transaction},
};

#[derive(Debug, Clone)]
pub enum ConstantValue {
    String(String),
    Number(i32),
    Null,
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
}

#[derive(Debug, Clone)]
enum ExpressionValue {
    FieldName(String),
    Constant(Constant),
}

#[derive(Debug, Clone)]
struct Expression {
    value: ExpressionValue,
}

impl Expression {
    pub fn new(value: ExpressionValue) -> Expression {
        Expression { value }
    }

    pub fn evaluate(
        &self,
        scan: &mut dyn Scan,
        transaction: &mut Transaction,
        buffer_list: &mut BufferList,
    ) -> Constant {
        match self.value {
            ExpressionValue::FieldName(ref field_name) => {
                let value = scan.get_value(transaction, buffer_list, field_name.clone());
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

#[derive(Debug, Clone)]
struct Term {
    lhs: Expression,
    rhs: Expression,
}

impl Term {
    pub fn new(lhs: Expression, rhs: Expression) -> Term {
        Term { lhs, rhs }
    }

    pub fn is_satisfied(
        &self,
        scan: &mut dyn Scan,
        transaction: &mut Transaction,
        buffer_list: &mut BufferList,
    ) -> bool {
        let lhs = self.lhs.evaluate(scan, transaction, buffer_list);
        let rhs = self.rhs.evaluate(scan, transaction, buffer_list);
        return lhs.equals(rhs.value.clone());
    }

    pub fn can_apply_to(&self, schema: TableSchema) -> bool {
        self.lhs.can_apply_to(schema.clone()) && self.rhs.can_apply_to(schema)
    }
}

#[derive(Debug, Clone)]
pub struct Predicate {
    terms: Vec<Term>,
}

impl Predicate {
    pub fn new(terms: Vec<Term>) -> Predicate {
        Predicate { terms }
    }

    pub fn is_satisfied(
        &self,
        scan: &mut dyn Scan,
        transaction: &mut Transaction,
        buffer_list: &mut BufferList,
    ) -> bool {
        for term in &self.terms {
            if !term.is_satisfied(scan, transaction, buffer_list) {
                return false;
            }
        }
        return true;
    }

    pub fn conjoin_with(&mut self, predicate: Predicate) {
        self.terms.extend(predicate.terms);
    }
}
