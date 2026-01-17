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

use crate::core_worker::{CoreWorkerHandle, WorkerInitConfig};
use crate::request::{Request, RequestId, RequestResult};
use parking_lot::Mutex;
use pyo3::prelude::*;
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

        // Create Core Worker Handle
        let core_worker = Arc::new(CoreWorkerHandle::new());
        let core_worker_clone = core_worker.clone();

        // Spawn Rust thread with Tokio runtime
        std::thread::Builder::new()
            .name("RustTokioThread".to_string())
            .spawn(move || {
                Self::rust_event_loop(rx, shutdown_clone, core_worker_clone);
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
        core_worker: Arc<CoreWorkerHandle>,
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
                let core_worker_clone = core_worker.clone();
                tokio::spawn(Self::process_request_async(request, core_worker_clone));
            }
        });
    }

    /// Process a single request asynchronously
    ///
    /// This runs as a Tokio task spawned from the event loop.
    /// Multiple requests can be processed concurrently.
    async fn process_request_async(request: Request, core_worker: Arc<CoreWorkerHandle>) {
        // Process the request (before moving callback)
        let result = Self::handle_operation(&request, core_worker).await;

        // Move callback after processing
        let callback = request.callback;

        // Deliver result back to Python via callback
        Self::deliver_result(callback, result);
    }

    /// Handle a specific operation
    ///
    /// This is where actual async work happens.
    /// Routes operations to the appropriate CoreWorkerHandle methods.
    async fn handle_operation(
        request: &Request,
        core_worker: Arc<CoreWorkerHandle>,
    ) -> RequestResult {
        match request.operation.as_str() {
            "initialize" => {
                // Parse configuration from request data
                let config: WorkerInitConfig = match serde_json::from_slice(&request.data) {
                    Ok(cfg) => cfg,
                    Err(e) => {
                        return RequestResult::error(
                            request.request_id.clone(),
                            format!("Failed to parse config: {}", e),
                        );
                    }
                };

                // Initialize the worker
                match core_worker.initialize(config).await {
                    Ok(_) => RequestResult::success(request.request_id.clone(), vec![]),
                    Err(e) => RequestResult::error(
                        request.request_id.clone(),
                        format!("Initialize failed: {}", e),
                    ),
                }
            }

            "validate" => {
                // Validate that the worker is initialized and ready
                // This is a simple check - if we can get the worker guard, it's valid
                match core_worker.validate().await {
                    Ok(_) => RequestResult::success(request.request_id.clone(), vec![]),
                    Err(e) => RequestResult::error(
                        request.request_id.clone(),
                        format!("Validation failed: {}", e),
                    ),
                }
            }

            "poll_activation" => {
                // Poll for workflow activation
                match core_worker.poll_workflow_activation().await {
                    Ok(bytes) => RequestResult::success(request.request_id.clone(), bytes),
                    Err(e) => {
                        let error_msg = e.to_string();
                        // Check if this is a shutdown error
                        if error_msg.contains("Shutdown") || error_msg.contains("shutdown") {
                            RequestResult::error(
                                request.request_id.clone(),
                                "PollShutdownError".to_string(),
                            )
                        } else {
                            RequestResult::error(
                                request.request_id.clone(),
                                format!("Poll failed: {}", error_msg),
                            )
                        }
                    }
                }
            }

            "complete_activation" => {
                // Complete workflow activation
                match core_worker
                    .complete_workflow_activation(request.data.clone())
                    .await
                {
                    Ok(_) => RequestResult::success(request.request_id.clone(), vec![]),
                    Err(e) => RequestResult::error(
                        request.request_id.clone(),
                        format!("Complete failed: {}", e),
                    ),
                }
            }

            "initiate_shutdown" => {
                // Initiate graceful shutdown
                match core_worker.initiate_shutdown().await {
                    Ok(_) => RequestResult::success(request.request_id.clone(), vec![]),
                    Err(e) => RequestResult::error(
                        request.request_id.clone(),
                        format!("Shutdown initiation failed: {}", e),
                    ),
                }
            }

            "finalize_shutdown" => {
                // Finalize shutdown
                match core_worker.finalize_shutdown().await {
                    Ok(_) => RequestResult::success(request.request_id.clone(), vec![]),
                    Err(e) => RequestResult::error(
                        request.request_id.clone(),
                        format!("Shutdown failed: {}", e),
                    ),
                }
            }

            _ => RequestResult::error(
                request.request_id.clone(),
                format!("Unknown operation: {}", request.operation),
            ),
        }
    }

    /// Deliver result back to Python via callback
    ///
    /// Acquires GIL and invokes the Python callback with the RequestResult struct.
    /// The callback is responsible for using trio.from_thread
    /// to schedule the result in the Trio event loop.
    ///
    /// Note: The RequestResult is passed directly as a PyO3 class, eliminating
    /// JSON serialization overhead.
    fn deliver_result(callback: PyObject, result: RequestResult) {
        Python::with_gil(|py| {
            // Convert RequestResult to a Python object using Bound
            // Create a Bound instance which wraps the #[pyclass] for Python
            match pyo3::Bound::new(py, result) {
                Ok(bound_result) => {
                    // Pass the Bound object to the callback
                    if let Err(e) = callback.call1(py, (bound_result,)) {
                        eprintln!("Callback error: {}", e);
                    }
                }
                Err(e) => {
                    eprintln!("Failed to create Bound RequestResult: {}", e);
                }
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
