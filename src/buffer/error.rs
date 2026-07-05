use core::fmt;

#[derive(Debug)]
pub enum BufferError {
    NoAvailableBuffer,
}

impl fmt::Display for BufferError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            BufferError::NoAvailableBuffer => write!(f, "No available buffer"),
        }
    }
}

impl std::error::Error for BufferError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            BufferError::NoAvailableBuffer => None,
        }
    }
}
