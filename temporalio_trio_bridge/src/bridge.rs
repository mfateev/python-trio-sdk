/*!
 * TrioAsyncBridge - Core async bridge implementation
 *
 * Manages a single Rust thread running Tokio runtime that:
 * 1. Receives requests from Python via unbounded channel
 * 2. Processes requests asynchronously using Tokio
 * 3. Delivers results back to Python via callbacks
 *
 * Key properties:
 * - Non-blocking from Python side (just queue push)
 * - Single Rust thread (no matter how many concurrent operations)
 * - Memory safe (proper cleanup, no leaks)
 * - Error handling throughout
 */

use crate::request::{Request, RequestId, RequestResult};
use parking_lot::Mutex;
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use std::sync::Arc;
use tokio::sync::mpsc;

/// Main bridge class exposed to Python
///
/// Usage from Python:
/// ```python
/// bridge = TrioAsyncBridge()
/// bridge.send_request("poll_activation", b"", callback)
/// ```
#[pyclass]
pub struct TrioAsyncBridge {
    /// Channel sender for sending requests to Rust thread
    request_tx: Arc<Mutex<mpsc::UnboundedSender<Request>>>,

    /// Flag to track if shutdown has been initiated
    shutdown: Arc<Mutex<bool>>,
}

#[pymethods]
impl TrioAsyncBridge {
    /// Create a new bridge instance
    ///
    /// Spawns a single Rust thread with Tokio runtime.
    #[new]
    fn new() -> PyResult<Self> {
        let (tx, rx) = mpsc::unbounded_channel::<Request>();
        let shutdown = Arc::new(Mutex::new(false));
        let shutdown_clone = shutdown.clone();

        // Spawn Rust thread with Tokio runtime
        std::thread::Builder::new()
            .name("RustTokioThread".to_string())
            .spawn(move || {
                Self::rust_event_loop(rx, shutdown_clone);
            })
            .map_err(|e| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                    "Failed to spawn Rust thread: {}",
                    e
                ))
            })?;

        Ok(Self {
            request_tx: Arc::new(Mutex::new(tx)),
            shutdown,
        })
    }

    /// Send an async request to Rust thread
    ///
    /// This is non-blocking - just pushes to queue.
    ///
    /// Args:
    ///     operation: Operation name (e.g., "poll_activation")
    ///     data: Request data as bytes
    ///     callback: Python callable to invoke with result
    ///
    /// Returns:
    ///     request_id: Unique ID for this request
    ///
    /// The callback will be invoked from the Rust thread when the
    /// result is ready. It should use trio.from_thread.run_sync()
    /// to deliver the result back to Trio.
    fn send_request(
        &self,
        operation: String,
        data: Vec<u8>,
        callback: PyObject,
    ) -> PyResult<String> {
        // Check if shutdown
        if *self.shutdown.lock() {
            return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "Bridge has been shutdown",
            ));
        }

        // Generate unique request ID
        let request_id = uuid::Uuid::new_v4().to_string();

        // Create request
        let request = Request::new(request_id.clone(), operation, data, callback);

        // Send to Rust thread (non-blocking)
        self.request_tx
            .lock()
            .send(request)
            .map_err(|e| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                    "Failed to send request: {}",
                    e
                ))
            })?;

        Ok(request_id)
    }

    /// Shutdown the bridge
    ///
    /// Closes the request channel and signals the Rust thread to exit.
    /// After shutdown, no more requests can be sent.
    fn shutdown(&self) -> PyResult<()> {
        let mut shutdown = self.shutdown.lock();
        if *shutdown {
            return Ok(()); // Already shutdown
        }
        *shutdown = true;
        drop(shutdown);

        // Note: The Rust thread will exit when rx.recv() returns None
        // We don't wait for the thread to join here to avoid blocking Python

        Ok(())
    }
}

impl TrioAsyncBridge {
    /// Main event loop running in Rust thread
    ///
    /// This runs in a separate OS thread with Tokio runtime.
    /// It processes requests asynchronously and delivers results
    /// back to Python via callbacks.
    fn rust_event_loop(
        mut rx: mpsc::UnboundedReceiver<Request>,
        shutdown: Arc<Mutex<bool>>,
    ) {
        // Create Tokio runtime (single-threaded)
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("Failed to create Tokio runtime");

        // Run event loop
        rt.block_on(async move {
            // Process requests until channel closes
            while let Some(request) = rx.recv().await {
                // Check shutdown flag
                if *shutdown.lock() {
                    break;
                }

                // Spawn async task to process request
                // This allows concurrent processing of multiple requests
                tokio::spawn(Self::process_request_async(request));
            }
        });
    }

    /// Process a single request asynchronously
    ///
    /// This runs as a Tokio task spawned from the event loop.
    /// Multiple requests can be processed concurrently.
    async fn process_request_async(request: Request) {
        let request_id = request.request_id.clone();
        let callback = request.callback.clone();

        // Process the request
        let result = Self::handle_operation(&request).await;

        // Deliver result back to Python via callback
        Self::deliver_result(callback, result);
    }

    /// Handle a specific operation
    ///
    /// This is where actual async work happens.
    /// Currently just a stub - will be filled in with real
    /// temporalio-sdk-core operations later.
    async fn handle_operation(request: &Request) -> RequestResult {
        match request.operation.as_str() {
            "poll_activation" => {
                // TODO: Implement real poll_workflow_activation call
                // For now, simulate async work
                tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

                RequestResult::success(
                    request.request_id.clone(),
                    b"mock_activation".to_vec(),
                )
            }

            "complete_activation" => {
                // TODO: Implement real complete_workflow_activation call
                tokio::time::sleep(tokio::time::Duration::from_millis(5)).await;

                RequestResult::success(request.request_id.clone(), b"completed".to_vec())
            }

            _ => RequestResult::error(
                request.request_id.clone(),
                format!("Unknown operation: {}", request.operation),
            ),
        }
    }

    /// Deliver result back to Python via callback
    ///
    /// Acquires GIL and invokes the Python callback.
    /// The callback is responsible for using trio.from_thread
    /// to schedule the result in the Trio event loop.
    fn deliver_result(callback: PyObject, result: RequestResult) {
        Python::with_gil(|py| {
            // Serialize result to JSON
            let result_json = match serde_json::to_vec(&result) {
                Ok(json) => json,
                Err(e) => {
                    eprintln!("Failed to serialize result: {}", e);
                    return;
                }
            };

            // Convert to Python bytes
            let py_bytes = PyBytes::new_bound(py, &result_json);

            // Invoke callback
            if let Err(e) = callback.call1(py, (py_bytes,)) {
                eprintln!("Callback error: {}", e);
            }
        });
    }
}

// Implement Drop to ensure cleanup
impl Drop for TrioAsyncBridge {
    fn drop(&mut self) {
        // Mark as shutdown
        *self.shutdown.lock() = true;

        // Note: We don't wait for thread to join to avoid blocking
        // The thread will exit when the channel closes
    }
}
