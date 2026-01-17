/*!
 * Request types for async bridge
 */

use pyo3::prelude::*;
use serde::{Deserialize, Serialize};

/// Unique identifier for a request
pub type RequestId = String;

/// Request from Python to Rust
///
/// Contains:
/// - operation: Type of operation to perform (e.g., "poll_activation")
/// - data: Serialized request data (JSON or bytes)
/// - callback: Python callable to invoke with result
#[derive(Debug)]
pub struct Request {
    /// Unique request identifier
    pub request_id: RequestId,

    /// Operation name (e.g., "poll_activation", "complete_activation")
    pub operation: String,

    /// Request data (serialized as JSON or raw bytes)
    pub data: Vec<u8>,

    /// Python callback to invoke when result is ready
    /// Must be called via Python::with_gil and trio.from_thread
    pub callback: PyObject,
}

impl Request {
    /// Create a new request
    pub fn new(
        request_id: RequestId,
        operation: String,
        data: Vec<u8>,
        callback: PyObject,
    ) -> Self {
        Self {
            request_id,
            operation,
            data,
            callback,
        }
    }
}

/// Result sent back to Python as a PyO3 class
///
/// This is exposed as a Python class, allowing direct attribute access
/// without JSON serialization overhead.
#[pyclass]
#[derive(Debug, Clone)]
pub struct RequestResult {
    /// Request ID this result is for
    #[pyo3(get)]
    pub request_id: RequestId,

    /// Success flag
    #[pyo3(get)]
    pub success: bool,

    /// Result data (if successful)
    /// Not directly exposed - use get_data() method instead
    data: Option<Vec<u8>>,

    /// Error message (if failed)
    #[pyo3(get)]
    pub error: Option<String>,
}

#[pymethods]
impl RequestResult {
    /// Get result data as Python bytes
    ///
    /// Returns None if no data is present (error case).
    /// Returns bytes object if data is present.
    #[pyo3(name = "get_data")]
    fn get_data_py(&self, py: Python<'_>) -> PyObject {
        match &self.data {
            Some(d) => pyo3::types::PyBytes::new(py, d).into(),
            None => py.None(),
        }
    }

    /// String representation for debugging
    fn __repr__(&self) -> String {
        if self.success {
            format!(
                "RequestResult(request_id={}, success=True, data_len={})",
                self.request_id,
                self.data.as_ref().map_or(0, |d| d.len())
            )
        } else {
            format!(
                "RequestResult(request_id={}, success=False, error={})",
                self.request_id,
                self.error.as_ref().map_or("None", |e| e.as_str())
            )
        }
    }
}

impl RequestResult {
    /// Create a success result
    pub fn success(request_id: RequestId, data: Vec<u8>) -> Self {
        Self {
            request_id,
            success: true,
            data: Some(data),
            error: None,
        }
    }

    /// Create an error result
    pub fn error(request_id: RequestId, error: String) -> Self {
        Self {
            request_id,
            success: false,
            data: None,
            error: Some(error),
        }
    }
}
