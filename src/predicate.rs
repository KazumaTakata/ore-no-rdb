use std::cmp::max;

use pest::pratt_parser::Op;

use crate::{
    buffer_manager::BufferList,
    plan::Plan,
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
}

#[derive(Debug, Clone)]
pub enum ExpressionValue {
    FieldName(String),
    Constant(Constant),
}

#[derive(Debug, Clone)]
pub struct Expression {
    pub value: ExpressionValue,
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

    pub fn to_string(&self) -> String {
        match self.value {
            ExpressionValue::FieldName(ref field_name) => field_name.clone(),
            ExpressionValue::Constant(ref constant) => match constant.value {
                ConstantValue::String(ref str) => str.clone(),
                ConstantValue::Number(n) => n.to_string(),
                ConstantValue::Null => "NULL".to_string(),
            },
        }
    }
}

#[derive(Debug, Clone)]
pub struct Term {
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

    pub fn reduction_factor(
        &self,
        plan: &dyn Plan,
        transaction: &mut Transaction,
        buffer_list: &mut BufferList,
    ) -> u32 {
        match self.lhs.value {
            ExpressionValue::FieldName(ref field_name) => match self.rhs.value {
                ExpressionValue::FieldName(ref field_name2) => {
                    return max(
                        plan.get_distinct_value(field_name.clone(), transaction, buffer_list),
                        plan.get_distinct_value(field_name2.clone(), transaction, buffer_list),
                    );
                }
                ExpressionValue::Constant(ref constant) => {
                    return plan.get_distinct_value(field_name.clone(), transaction, buffer_list);
                }
            },
            ExpressionValue::Constant(_) => match self.rhs.value {
                ExpressionValue::FieldName(ref field_name) => {
                    return plan.get_distinct_value(field_name.clone(), transaction, buffer_list);
                }
                ExpressionValue::Constant(_) => return 1,
            },
        }
    }

    pub fn equate_with_constant(
        &self,
        transaction: &mut Transaction,
        buffer_list: &mut BufferList,
        field_name: String,
    ) -> Option<Constant> {
        match self.lhs.value.clone() {
            ExpressionValue::FieldName(_field_name) => match self.rhs.value {
                ExpressionValue::FieldName(_) => return None,
                ExpressionValue::Constant(ref constant2) => {
                    if _field_name == field_name {
                        return Some(constant2.clone());
                    } else {
                        return None;
                    }
                }
            },
            ExpressionValue::Constant(ref constant) => match self.rhs.value.clone() {
                ExpressionValue::FieldName(_field_name) => {
                    if _field_name == field_name {
                        return Some(constant.clone());
                    } else {
                        return None;
                    }
                }
                ExpressionValue::Constant(_) => return None,
            },
        }
    }

    pub fn equate_with_field(
        &self,
        transaction: &mut Transaction,
        buffer_list: &mut BufferList,
        field_name: String,
    ) -> Option<String> {
        match self.lhs.value.clone() {
            ExpressionValue::FieldName(_field_name) => match self.rhs.value.clone() {
                ExpressionValue::FieldName(_field_name2) => {
                    if _field_name == field_name {
                        return Some(_field_name2.clone());
                    } else if _field_name2 == field_name {
                        return Some(_field_name.clone());
                    } else {
                        return None;
                    }
                }
                ExpressionValue::Constant(ref constant2) => return None,
            },
            ExpressionValue::Constant(ref constant) => return None,
        }
    }

    pub fn to_string(&self) -> String {
        let lhs = self.lhs.to_string();
        let rhs = self.rhs.to_string();
        return format!("{} = {}", lhs, rhs);
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

    pub fn reduction_factor(
        &self,
        plan: &dyn Plan,
        transaction: &mut Transaction,
        buffer_list: &mut BufferList,
    ) -> u32 {
        let mut reduction_factor = 1;
        for term in &self.terms {
            reduction_factor *= term.reduction_factor(plan, transaction, buffer_list);
        }
        return reduction_factor;
    }

    pub fn select_sub_predicate(&self, schema: TableSchema) -> Option<Predicate> {
        let mut predicate = Predicate::new(vec![]);

        for term in &self.terms {
            if term.can_apply_to(schema.clone()) {
                predicate.terms.push(term.clone());
            }
        }

        if predicate.terms.len() == 0 {
            return None;
        } else {
            return Some(predicate);
        }
    }

    pub fn join_sub_predicate(
        &self,
        schema: TableSchema,
        schema2: TableSchema,
    ) -> Option<Predicate> {
        let mut predicate = Predicate::new(vec![]);

        let mut new_schema = TableSchema::new();
        new_schema.add_all(schema.clone());
        new_schema.add_all(schema2.clone());

        for term in &self.terms {
            if !term.can_apply_to(schema.clone())
                && !term.can_apply_to(schema2.clone())
                && term.can_apply_to(new_schema.clone())
            {
                predicate.terms.push(term.clone());
            }
        }

        if predicate.terms.len() == 0 {
            return None;
        } else {
            return Some(predicate);
        }
    }

    pub fn equates_with_constant(
        &self,
        transaction: &mut Transaction,
        buffer_list: &mut BufferList,
        field_name: String,
    ) -> Option<Constant> {
        for term in &self.terms {
            let constant = term.equate_with_constant(transaction, buffer_list, field_name.clone());
            if constant.is_some() {
                return constant;
            }
        }
        return None;
    }

    pub fn equate_with_field(
        &self,
        transaction: &mut Transaction,
        buffer_list: &mut BufferList,
        field_name: String,
    ) -> Option<String> {
        for term in &self.terms {
            let field = term.equate_with_field(transaction, buffer_list, field_name.clone());
            if field.is_some() {
                return field;
            }
        }
        return None;
    }

    pub fn to_string(&self) -> String {
        let mut result = String::new();
        for term in &self.terms {
            result.push_str(&term.to_string());
            result.push_str(" AND ");
        }
        if result.len() > 5 {
            result.truncate(result.len() - 5);
        }
        return result;
    }
}
