use core::fmt;

#[derive(Debug)]
pub enum LogError {
    IoError(std::io::Error),
}

impl fmt::Display for LogError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            LogError::IoError(e) => write!(f, "IO Error: {}", e),
        }
    }
}

impl std::error::Error for LogError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            LogError::IoError(e) => Some(e),
        }
    }
}

impl From<std::io::Error> for LogError {
    fn from(error: std::io::Error) -> Self {
        LogError::IoError(error)
    }
}
