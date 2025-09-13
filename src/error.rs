use std::fmt;

#[derive(Debug, Clone)]
pub struct ValueNotFound {
    field: String,
    table: Option<String>,
}

impl ValueNotFound {
    pub fn new(field: String, table: Option<String>) -> Self {
        ValueNotFound { field, table }
    }
}

impl fmt::Display for ValueNotFound {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ValueNotFound { field, table } => {
                if let Some(table_name) = table {
                    write!(f, "Value not found in table '{}': {}", table_name, field)
                } else {
                    write!(f, "Value not found: {}", field)
                }
            }
        }
    }
}

impl std::error::Error for ValueNotFound {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            _ => None,
        }
    }
}
