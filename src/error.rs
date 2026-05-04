use std::fmt;

#[derive(Debug, Clone)]
pub enum DatabaseError {
    UniqueConstraintViolation(UniqueConstraintError),
    ValueNotFound(ValueNotFound),
    TableAlreadyExists(TableAlreadyExists),
}

impl From<ValueNotFound> for DatabaseError {
    fn from(e: ValueNotFound) -> Self {
        DatabaseError::ValueNotFound(e)
    }
}

impl From<TableAlreadyExists> for DatabaseError {
    fn from(e: TableAlreadyExists) -> Self {
        DatabaseError::TableAlreadyExists(e)
    }
}

impl From<UniqueConstraintError> for DatabaseError {
    fn from(e: UniqueConstraintError) -> Self {
        DatabaseError::UniqueConstraintViolation(e)
    }
}

#[derive(Debug, Clone)]
pub struct UniqueConstraintError {
    field: String,
    table: String,
}

impl UniqueConstraintError {
    pub fn new(field: String, table: String) -> Self {
        UniqueConstraintError { field, table }
    }
}

impl fmt::Display for UniqueConstraintError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "Duplicate value for unique index on table '{}', field '{}'",
            self.table, self.field
        )
    }
}

impl std::error::Error for UniqueConstraintError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            _ => None,
        }
    }
}

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

#[derive(Debug, Clone)]
pub struct TableAlreadyExists {
    table_name: String,
}

impl TableAlreadyExists {
    pub fn new(table_name: String) -> Self {
        TableAlreadyExists { table_name }
    }
}

impl fmt::Display for TableAlreadyExists {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Table already exists: {}", self.table_name)
    }
}

impl std::error::Error for TableAlreadyExists {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            _ => None,
        }
    }
}
