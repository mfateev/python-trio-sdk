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

use crate::core_client::{ClientInitConfig, CoreClientHandle};
use crate::core_worker::{CoreWorkerHandle, ReplayWorkerHandle, ReplayWorkerInitConfig, WorkerInitConfig};
use crate::request::{Request, RequestResult};
use futures_util::FutureExt;
use parking_lot::Mutex;
use pyo3::prelude::*;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::Mutex as AsyncMutex;

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

        // Create Core Worker, Client, and Replay Worker Handles
        let core_worker = Arc::new(CoreWorkerHandle::new());
        let core_worker_clone = core_worker.clone();
        // Use async mutex for core_client to allow concurrent client operations
        let core_client = Arc::new(AsyncMutex::new(CoreClientHandle::new()));
        let core_client_clone = core_client.clone();
        let replay_worker = Arc::new(ReplayWorkerHandle::new());
        let replay_worker_clone = replay_worker.clone();

        // Spawn Rust thread with Tokio runtime
        std::thread::Builder::new()
            .name("RustTokioThread".to_string())
            .spawn(move || {
                Self::rust_event_loop(rx, shutdown_clone, core_worker_clone, core_client_clone, replay_worker_clone);
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
        self.request_tx.lock().send(request).map_err(|e| {
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
        core_client: Arc<AsyncMutex<CoreClientHandle>>,
        replay_worker: Arc<ReplayWorkerHandle>,
    ) {
        // Create Tokio runtime (single-threaded)
        let rt = match tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
        {
            Ok(runtime) => runtime,
            Err(e) => {
                eprintln!(
                    "FATAL ERROR: Failed to create Tokio runtime\n\
                     Details: {}\n\
                     IMPACT: Bridge cannot function, all operations will fail.\n\
                     Possible causes:\n\
                     - System resource exhaustion (file descriptors, memory)\n\
                     - OS permissions issues\n\
                     - Incompatible system configuration\n\
                     RECOMMENDATION: Check system resources and logs.",
                    e
                );
                // Cannot proceed without runtime - exit thread cleanly
                return;
            }
        };

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
                let core_client_clone = core_client.clone();
                let replay_worker_clone = replay_worker.clone();
                tokio::spawn(Self::process_request_async(request, core_worker_clone, core_client_clone, replay_worker_clone));
            }
        });
    }

    /// Process a single request asynchronously
    ///
    /// This runs as a Tokio task spawned from the event loop.
    /// Multiple requests can be processed concurrently.
    ///
    /// CRITICAL: This function ensures callback is ALWAYS delivered, even on panic.
    async fn process_request_async(
        request: Request,
        core_worker: Arc<CoreWorkerHandle>,
        core_client: Arc<AsyncMutex<CoreClientHandle>>,
        replay_worker: Arc<ReplayWorkerHandle>,
    ) {
        let request_id = request.request_id.clone();

        // Wrap processing in catch_unwind to handle panics
        // Must do this before moving callback since we need request reference
        let panic_result = std::panic::AssertUnwindSafe(
            Self::handle_operation(&request, core_worker, core_client, replay_worker)
        ).catch_unwind().await;

        // Move callback after processing (can't do this before because handle_operation needs &request)
        let callback = request.callback;

        // Deliver result - either success/error from handle_operation or panic error
        let result = match panic_result {
            Ok(result) => result,
            Err(panic_err) => {
                let panic_msg = if let Some(s) = panic_err.downcast_ref::<&str>() {
                    s.to_string()
                } else if let Some(s) = panic_err.downcast_ref::<String>() {
                    s.clone()
                } else {
                    "Unknown panic".to_string()
                };
                eprintln!("ERROR: Request {} panicked during processing: {}", request_id, panic_msg);
                RequestResult::error(request_id, format!("Task panicked: {}", panic_msg))
            }
        };

        // Deliver result back to Python via callback (always called, even on panic)
        Self::deliver_result(callback, result);
    }

    /// Handle a specific operation
    ///
    /// This is where actual async work happens.
    /// Routes operations to the appropriate CoreWorkerHandle or CoreClientHandle methods.
    async fn handle_operation(
        request: &Request,
        core_worker: Arc<CoreWorkerHandle>,
        core_client: Arc<AsyncMutex<CoreClientHandle>>,
        replay_worker: Arc<ReplayWorkerHandle>,
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

            "poll_activity_task" => {
                // Poll for activity task
                match core_worker.poll_activity_task().await {
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
                                format!("Poll activity failed: {}", error_msg),
                            )
                        }
                    }
                }
            }

            "complete_activity_task" => {
                // Complete activity task
                match core_worker
                    .complete_activity_task(request.data.clone())
                    .await
                {
                    Ok(_) => RequestResult::success(request.request_id.clone(), vec![]),
                    Err(e) => RequestResult::error(
                        request.request_id.clone(),
                        format!("Complete activity failed: {}", e),
                    ),
                }
            }

            "record_activity_heartbeat" => {
                // Record activity heartbeat
                match core_worker
                    .record_activity_heartbeat(request.data.clone())
                    .await
                {
                    Ok(_) => RequestResult::success(request.request_id.clone(), vec![]),
                    Err(e) => RequestResult::error(
                        request.request_id.clone(),
                        format!("Record heartbeat failed: {}", e),
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

            // Client operations
            "initialize_client" => {
                let config: ClientInitConfig = match serde_json::from_slice(&request.data) {
                    Ok(cfg) => cfg,
                    Err(e) => {
                        return RequestResult::error(
                            request.request_id.clone(),
                            format!("Failed to parse client config: {}", e),
                        );
                    }
                };

                let mut client = core_client.lock().await;
                match client.initialize(config).await {
                    Ok(_) => RequestResult::success(request.request_id.clone(), vec![]),
                    Err(e) => RequestResult::error(
                        request.request_id.clone(),
                        format!("Client initialize failed: {}", e),
                    ),
                }
            }

            "start_workflow" => {
                let client = core_client.lock().await;
                match client.start_workflow_execution(request.data.clone()).await {
                    Ok(bytes) => RequestResult::success(request.request_id.clone(), bytes),
                    Err(e) => RequestResult::error(
                        request.request_id.clone(),
                        format!("Start workflow failed: {}", e),
                    ),
                }
            }

            "get_workflow_result" => {
                // Parse workflow_id and run_id from JSON
                #[derive(serde::Deserialize)]
                struct GetResultRequest {
                    workflow_id: String,
                    run_id: Option<String>,
                }

                let req: GetResultRequest = match serde_json::from_slice(&request.data) {
                    Ok(r) => r,
                    Err(e) => {
                        return RequestResult::error(
                            request.request_id.clone(),
                            format!("Failed to parse get result request: {}", e),
                        );
                    }
                };

                let client = core_client.lock().await;
                match client.get_workflow_result(req.workflow_id, req.run_id).await {
                    Ok(bytes) => RequestResult::success(request.request_id.clone(), bytes),
                    Err(e) => RequestResult::error(
                        request.request_id.clone(),
                        format!("Get workflow result failed: {}", e),
                    ),
                }
            }

            "get_workflow_execution_history" => {
                #[derive(serde::Deserialize)]
                struct GetHistoryRequest {
                    workflow_id: String,
                    run_id: Option<String>,
                    #[serde(default)]
                    next_page_token: Vec<u8>,
                }

                let req: GetHistoryRequest = match serde_json::from_slice(&request.data) {
                    Ok(r) => r,
                    Err(e) => {
                        return RequestResult::error(
                            request.request_id.clone(),
                            format!("Failed to parse get history request: {}", e),
                        );
                    }
                };

                let client = core_client.lock().await;
                match client
                    .get_workflow_execution_history(req.workflow_id, req.run_id, req.next_page_token)
                    .await
                {
                    Ok(bytes) => RequestResult::success(request.request_id.clone(), bytes),
                    Err(e) => RequestResult::error(
                        request.request_id.clone(),
                        format!("Get workflow execution history failed: {}", e),
                    ),
                }
            }

            "cancel_workflow" => {
                #[derive(serde::Deserialize)]
                struct CancelRequest {
                    workflow_id: String,
                    run_id: Option<String>,
                }

                let req: CancelRequest = match serde_json::from_slice(&request.data) {
                    Ok(r) => r,
                    Err(e) => {
                        return RequestResult::error(
                            request.request_id.clone(),
                            format!("Failed to parse cancel request: {}", e),
                        );
                    }
                };

                let client = core_client.lock().await;
                match client.cancel_workflow_execution(req.workflow_id, req.run_id).await {
                    Ok(_) => RequestResult::success(request.request_id.clone(), vec![]),
                    Err(e) => RequestResult::error(
                        request.request_id.clone(),
                        format!("Cancel workflow failed: {}", e),
                    ),
                }
            }

            "terminate_workflow" => {
                #[derive(serde::Deserialize)]
                struct TerminateRequest {
                    workflow_id: String,
                    run_id: Option<String>,
                    reason: String,
                }

                let req: TerminateRequest = match serde_json::from_slice(&request.data) {
                    Ok(r) => r,
                    Err(e) => {
                        return RequestResult::error(
                            request.request_id.clone(),
                            format!("Failed to parse terminate request: {}", e),
                        );
                    }
                };

                let client = core_client.lock().await;
                match client
                    .terminate_workflow_execution(req.workflow_id, req.run_id, req.reason)
                    .await
                {
                    Ok(_) => RequestResult::success(request.request_id.clone(), vec![]),
                    Err(e) => RequestResult::error(
                        request.request_id.clone(),
                        format!("Terminate workflow failed: {}", e),
                    ),
                }
            }

            "query_workflow" => {
                #[derive(serde::Deserialize)]
                struct QueryRequest {
                    workflow_id: String,
                    run_id: Option<String>,
                    query_type: String,
                    args_bytes: Vec<u8>,
                    #[serde(default)]
                    reject_condition: Option<i32>,
                }

                let req: QueryRequest = match serde_json::from_slice(&request.data) {
                    Ok(r) => r,
                    Err(e) => {
                        return RequestResult::error(
                            request.request_id.clone(),
                            format!("Failed to parse query request: {}", e),
                        );
                    }
                };

                let client = core_client.lock().await;
                match client
                    .query_workflow(req.workflow_id, req.run_id, req.query_type, req.args_bytes, req.reject_condition)
                    .await
                {
                    Ok(bytes) => RequestResult::success(request.request_id.clone(), bytes),
                    Err(e) => RequestResult::error(
                        request.request_id.clone(),
                        format!("Query workflow failed: {}", e),
                    ),
                }
            }

            "describe_workflow" => {
                #[derive(serde::Deserialize)]
                struct DescribeRequest {
                    workflow_id: String,
                    run_id: Option<String>,
                }

                let req: DescribeRequest = match serde_json::from_slice(&request.data) {
                    Ok(r) => r,
                    Err(e) => {
                        return RequestResult::error(
                            request.request_id.clone(),
                            format!("Failed to parse describe request: {}", e),
                        );
                    }
                };

                let client = core_client.lock().await;
                match client
                    .describe_workflow_execution(req.workflow_id, req.run_id)
                    .await
                {
                    Ok(bytes) => RequestResult::success(request.request_id.clone(), bytes),
                    Err(e) => RequestResult::error(
                        request.request_id.clone(),
                        format!("Describe workflow failed: {}", e),
                    ),
                }
            }

            "list_workflows" => {
                let client = core_client.lock().await;
                match client
                    .list_workflow_executions(request.data.clone())
                    .await
                {
                    Ok(bytes) => RequestResult::success(request.request_id.clone(), bytes),
                    Err(e) => RequestResult::error(
                        request.request_id.clone(),
                        format!("List workflows failed: {}", e),
                    ),
                }
            }

            "count_workflows" => {
                let client = core_client.lock().await;
                match client
                    .count_workflow_executions(request.data.clone())
                    .await
                {
                    Ok(bytes) => RequestResult::success(request.request_id.clone(), bytes),
                    Err(e) => RequestResult::error(
                        request.request_id.clone(),
                        format!("Count workflows failed: {}", e),
                    ),
                }
            }

            "signal_workflow" => {
                #[derive(serde::Deserialize)]
                struct SignalRequest {
                    workflow_id: String,
                    run_id: Option<String>,
                    signal_name: String,
                    args_bytes: Vec<u8>,
                }

                let req: SignalRequest = match serde_json::from_slice(&request.data) {
                    Ok(r) => r,
                    Err(e) => {
                        return RequestResult::error(
                            request.request_id.clone(),
                            format!("Failed to parse signal request: {}", e),
                        );
                    }
                };

                let client = core_client.lock().await;
                match client
                    .signal_workflow(req.workflow_id, req.run_id, req.signal_name, req.args_bytes)
                    .await
                {
                    Ok(_) => RequestResult::success(request.request_id.clone(), vec![]),
                    Err(e) => RequestResult::error(
                        request.request_id.clone(),
                        format!("Signal workflow failed: {}", e),
                    ),
                }
            }

            "update_workflow" => {
                let client = core_client.lock().await;
                match client.update_workflow(request.data.clone()).await {
                    Ok(bytes) => RequestResult::success(
                        request.request_id.clone(),
                        bytes,
                    ),
                    Err(e) => RequestResult::error(
                        request.request_id.clone(),
                        format!("Update workflow failed: {}", e),
                    ),
                }
            }

            "poll_workflow_execution_update" => {
                let client = core_client.lock().await;
                match client
                    .poll_workflow_execution_update(request.data.clone())
                    .await
                {
                    Ok(bytes) => RequestResult::success(
                        request.request_id.clone(),
                        bytes,
                    ),
                    Err(e) => RequestResult::error(
                        request.request_id.clone(),
                        format!("Poll workflow execution update failed: {}", e),
                    ),
                }
            }

            // Replay worker operations
            "initialize_replay_worker" => {
                let config: ReplayWorkerInitConfig = match serde_json::from_slice(&request.data) {
                    Ok(cfg) => cfg,
                    Err(e) => {
                        return RequestResult::error(
                            request.request_id.clone(),
                            format!("Failed to parse replay worker config: {}", e),
                        );
                    }
                };

                match replay_worker.initialize(config).await {
                    Ok(_) => RequestResult::success(request.request_id.clone(), vec![]),
                    Err(e) => RequestResult::error(
                        request.request_id.clone(),
                        format!("Initialize replay worker failed: {}", e),
                    ),
                }
            }

            "push_replay_history" => {
                // data is: JSON with workflow_id + history_bytes (base64-encoded or raw)
                // We use a simpler approach: the Python side sends a JSON object with
                // workflow_id (string) and the history protobuf bytes are sent separately.
                // Actually, let's use a length-prefixed format:
                // First 4 bytes = workflow_id length (big endian)
                // Next N bytes = workflow_id (UTF-8)
                // Remaining bytes = history protobuf
                if request.data.len() < 4 {
                    return RequestResult::error(
                        request.request_id.clone(),
                        "Push history data too short".to_string(),
                    );
                }
                let wf_id_len = u32::from_be_bytes([
                    request.data[0], request.data[1],
                    request.data[2], request.data[3],
                ]) as usize;
                if request.data.len() < 4 + wf_id_len {
                    return RequestResult::error(
                        request.request_id.clone(),
                        "Push history data too short for workflow_id".to_string(),
                    );
                }
                let workflow_id = match String::from_utf8(request.data[4..4+wf_id_len].to_vec()) {
                    Ok(s) => s,
                    Err(e) => {
                        return RequestResult::error(
                            request.request_id.clone(),
                            format!("Invalid workflow_id UTF-8: {}", e),
                        );
                    }
                };
                let history_bytes = request.data[4+wf_id_len..].to_vec();

                match replay_worker.push_history(workflow_id, history_bytes).await {
                    Ok(_) => RequestResult::success(request.request_id.clone(), vec![]),
                    Err(e) => RequestResult::error(
                        request.request_id.clone(),
                        format!("Push replay history failed: {}", e),
                    ),
                }
            }

            "close_replay_pusher" => {
                match replay_worker.close_feeder().await {
                    Ok(_) => RequestResult::success(request.request_id.clone(), vec![]),
                    Err(e) => RequestResult::error(
                        request.request_id.clone(),
                        format!("Close replay pusher failed: {}", e),
                    ),
                }
            }

            "poll_replay_activation" => {
                match replay_worker.poll_workflow_activation().await {
                    Ok(bytes) => RequestResult::success(request.request_id.clone(), bytes),
                    Err(e) => {
                        let error_msg = e.to_string();
                        if error_msg.contains("Shutdown") || error_msg.contains("shutdown") {
                            RequestResult::error(
                                request.request_id.clone(),
                                "PollShutdownError".to_string(),
                            )
                        } else {
                            RequestResult::error(
                                request.request_id.clone(),
                                format!("Replay poll failed: {}", error_msg),
                            )
                        }
                    }
                }
            }

            "complete_replay_activation" => {
                match replay_worker.complete_workflow_activation(request.data.clone()).await {
                    Ok(_) => RequestResult::success(request.request_id.clone(), vec![]),
                    Err(e) => RequestResult::error(
                        request.request_id.clone(),
                        format!("Complete replay activation failed: {}", e),
                    ),
                }
            }

            "initiate_replay_shutdown" => {
                match replay_worker.initiate_shutdown().await {
                    Ok(_) => RequestResult::success(request.request_id.clone(), vec![]),
                    Err(e) => RequestResult::error(
                        request.request_id.clone(),
                        format!("Replay shutdown initiation failed: {}", e),
                    ),
                }
            }

            "finalize_replay_shutdown" => {
                match replay_worker.finalize_shutdown().await {
                    Ok(_) => RequestResult::success(request.request_id.clone(), vec![]),
                    Err(e) => RequestResult::error(
                        request.request_id.clone(),
                        format!("Replay shutdown failed: {}", e),
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
            match pyo3::Bound::new(py, result.clone()) {
                Ok(bound_result) => {
                    // Pass the Bound object to the callback
                    if let Err(e) = callback.call1(py, (bound_result,)) {
                        eprintln!(
                            "ERROR: Failed to execute Python callback for request {}\n\
                             Details: {}\n\
                             IMPACT: Python Event will never fire, causing indefinite hang.\n\
                             RECOMMENDATION: Always use timeouts on bridge operations.\n\
                             Possible causes:\n\
                             - Trio runtime shutting down\n\
                             - Invalid trio_token (trio_token expired or from wrong context)\n\
                             - Python callback raised an exception\n\
                             - Python interpreter state corrupted",
                            result.request_id, e
                        );
                    }
                }
                Err(e) => {
                    eprintln!(
                        "ERROR: Failed to create Bound RequestResult for request {}\n\
                         Details: {}\n\
                         IMPACT: Callback cannot be invoked, Python will hang indefinitely.\n\
                         RECOMMENDATION: Always use timeouts on bridge operations.\n\
                         This indicates a serious PyO3 issue or memory corruption.",
                        result.request_id, e
                    );
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
