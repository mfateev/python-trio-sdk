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

/// Result sent back to Python
#[derive(Debug, Serialize, Deserialize)]
pub struct RequestResult {
    /// Request ID this result is for
    pub request_id: RequestId,

    /// Success flag
    pub success: bool,

    /// Result data (if successful)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<Vec<u8>>,

    /// Error message (if failed)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
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
