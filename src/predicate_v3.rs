use crate::{
    plan_v2::PlanV2,
    predicate::{Constant, ConstantValue, ExpressionValue},
    record_page::TableSchema,
    scan_v2::ScanV2,
};

#[derive(Debug, Clone)]
pub struct ExpressionV2 {
    pub value: ExpressionValue,
}

impl ExpressionV2 {
    pub fn new(value: ExpressionValue) -> ExpressionV2 {
        ExpressionV2 { value }
    }

    pub fn evaluate(&self, scan: &mut dyn ScanV2) -> Constant {
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
pub struct TermV2 {
    lhs: ExpressionV2,
    rhs: ExpressionV2,
}

impl TermV2 {
    pub fn new(lhs: ExpressionV2, rhs: ExpressionV2) -> TermV2 {
        TermV2 { lhs, rhs }
    }

    pub fn is_satisfied(&self, scan: &mut dyn ScanV2) -> bool {
        let lhs = self.lhs.evaluate(scan);
        let rhs = self.rhs.evaluate(scan);
        return lhs.equals(rhs.value.clone());
    }

    pub fn can_apply_to(&self, schema: TableSchema) -> bool {
        self.lhs.can_apply_to(schema.clone()) && self.rhs.can_apply_to(schema)
    }

    pub fn to_string(&self) -> String {
        let lhs = self.lhs.to_string();
        let rhs = self.rhs.to_string();
        return format!("{} = {}", lhs, rhs);
    }

    pub fn equate_with_constant(&self, field_name: String) -> Option<Constant> {
        match &self.lhs.value {
            ExpressionValue::FieldName(_field_name) => match self.rhs.value {
                ExpressionValue::FieldName(_) => return None,
                ExpressionValue::Constant(ref constant2) => {
                    if *_field_name == field_name {
                        return Some(constant2.clone());
                    } else {
                        return None;
                    }
                }
            },
            ExpressionValue::Constant(ref constant) => match &self.rhs.value {
                ExpressionValue::FieldName(_field_name) => {
                    if *_field_name == field_name {
                        return Some(constant.clone());
                    } else {
                        return None;
                    }
                }
                ExpressionValue::Constant(_) => return None,
            },
        }
    }

    pub fn equate_with_field(&self, field_name: String) -> Option<String> {
        match &self.lhs.value {
            ExpressionValue::FieldName(_field_name) => match &self.rhs.value {
                ExpressionValue::FieldName(_field_name2) => {
                    if *_field_name == field_name {
                        return Some(_field_name2.clone());
                    } else if *_field_name2 == field_name {
                        return Some(_field_name.clone());
                    } else {
                        return None;
                    }
                }
                ExpressionValue::Constant(_constant2) => return None,
            },
            ExpressionValue::Constant(_constant) => return None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct PredicateV2 {
    terms: Vec<TermV2>,
}

impl PredicateV2 {
    pub fn new(terms: Vec<TermV2>) -> PredicateV2 {
        PredicateV2 { terms }
    }

    pub fn is_satisfied(&self, scan: &mut dyn ScanV2) -> bool {
        for term in &self.terms {
            if !term.is_satisfied(scan) {
                return false;
            }
        }
        return true;
    }

    pub fn conjunction_with(&mut self, predicate: PredicateV2) {
        self.terms.extend(predicate.terms);
    }

    pub fn equates_with_constant(&self, field_name: String) -> Option<Constant> {
        for term in &self.terms {
            let constant = term.equate_with_constant(field_name.clone());
            if constant.is_some() {
                return constant;
            }
        }
        return None;
    }

    pub fn equate_with_field(&self, field_name: String) -> Option<String> {
        for term in &self.terms {
            let field = term.equate_with_field(field_name.clone());
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

    pub fn reduction_factor(&self, plan: &dyn PlanV2) -> u32 {
        return 1;
    }
}
