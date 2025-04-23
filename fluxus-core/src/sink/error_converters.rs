use crate::models::StreamError;
use csv;
use serde_json;

/// Error converter for CSV errors
impl From<csv::Error> for StreamError {
    fn from(err: csv::Error) -> Self {
        StreamError::Serialization(err.to_string())
    }
}

/// Error converter for UTF-8 errors
impl From<std::string::FromUtf8Error> for StreamError {
    fn from(err: std::string::FromUtf8Error) -> Self {
        StreamError::Serialization(err.to_string())
    }
}

/// Error converter for serde_json errors
impl From<serde_json::Error> for StreamError {
    fn from(err: serde_json::Error) -> Self {
        StreamError::Serialization(err.to_string())
    }
}

/// Error converter for CSV writer's IntoInnerError
impl<T> From<csv::IntoInnerError<T>> for StreamError {
    fn from(err: csv::IntoInnerError<T>) -> Self {
        StreamError::Serialization(err.to_string())
    }
}
