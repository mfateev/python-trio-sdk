use anyhow::{anyhow, Result};
use prost::Message;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use temporalio_client::ClientOptions;
use temporalio_common::errors::PollError;
use temporalio_common::protos::coresdk::workflow_completion::WorkflowActivationCompletion;
use temporalio_common::protos::coresdk::{ActivityHeartbeat, ActivityTaskCompletion};
use temporalio_common::worker::{WorkerTaskTypes, WorkerVersioningStrategy};
use temporalio_common::Worker as WorkerTrait;
use temporalio_sdk_core::{init_worker, CoreRuntime, RetryClient, Worker, WorkerConfig};
use tokio::sync::Mutex;
use url::Url;

/// Configuration for initializing the Core Worker
#[derive(serde::Deserialize)]
pub struct WorkerInitConfig {
    pub target_url: String,
    pub namespace: String,
    pub task_queue: String,
    #[serde(default)]
    pub identity: String,
    #[serde(default = "default_max_cached_workflows")]
    pub max_cached_workflows: usize,
    #[serde(default = "default_max_concurrent_polls")]
    pub max_concurrent_workflow_task_polls: usize,
    /// How long a workflow task is allowed to sit on the sticky queue before it is timed out
    /// and moved to the non-sticky queue. Value in milliseconds.
    #[serde(default = "default_sticky_queue_schedule_to_start_timeout_millis")]
    pub sticky_queue_schedule_to_start_timeout_millis: u64,
}

fn default_max_cached_workflows() -> usize {
    1000
}

fn default_max_concurrent_polls() -> usize {
    5
}

fn default_sticky_queue_schedule_to_start_timeout_millis() -> u64 {
    10_000 // 10 seconds, matches SDK-Core default
}

/// Wrapper around the Temporal SDK Core Worker that provides thread-safe access
///
/// The worker is stored in an Option<Arc<Worker>> so that:
/// 1. The Mutex only needs to be held briefly to clone the Arc
/// 2. The Arc<Worker> can be used concurrently without locking
/// 3. SDK-Core's Worker is internally thread-safe for concurrent operations
pub struct CoreWorkerHandle {
    worker: Arc<Mutex<Option<Arc<Worker>>>>,
    runtime: Arc<Mutex<Option<CoreRuntime>>>,
}

impl CoreWorkerHandle {
    /// Create a new uninitialized CoreWorkerHandle
    pub fn new() -> Self {
        Self {
            worker: Arc::new(Mutex::new(None)),
            runtime: Arc::new(Mutex::new(None)),
        }
    }

    /// Initialize the worker with the given configuration
    pub async fn initialize(&self, config: WorkerInitConfig) -> Result<()> {
        let mut worker_guard = self.worker.lock().await;
        if worker_guard.is_some() {
            return Err(anyhow!("Worker already initialized"));
        }

        // Create CoreRuntime with default options
        use temporalio_sdk_core::RuntimeOptions;
        let runtime_options = RuntimeOptions::default();
        let runtime = CoreRuntime::new_assume_tokio(runtime_options)?;

        // Parse target URL
        let target_url =
            Url::from_str(&config.target_url).map_err(|e| anyhow!("Invalid target URL: {}", e))?;

        // Create client options and connect (using bon builder)
        let client_options = ClientOptions::builder()
            .target_url(target_url)
            .client_name("temporal-trio-sdk".to_string())
            .client_version(env!("CARGO_PKG_VERSION").to_string())
            .identity(if config.identity.is_empty() {
                "trio-worker".to_string()
            } else {
                config.identity.clone()
            })
            .build();

        // Connect to Temporal server
        let client = client_options
            .connect_no_namespace(None)
            .await
            .map_err(|e| anyhow!("Failed to connect to Temporal server: {}", e))?;

        let retry_client = RetryClient::new(client, Default::default());

        // Build worker config (using bon builder)
        // Note: Using minimal configuration with required fields
        let worker_config = WorkerConfig::builder()
            .namespace(config.namespace.clone())
            .task_queue(config.task_queue.clone())
            .max_cached_workflows(config.max_cached_workflows)
            .sticky_queue_schedule_to_start_timeout(Duration::from_millis(
                config.sticky_queue_schedule_to_start_timeout_millis,
            ))
            .versioning_strategy(WorkerVersioningStrategy::None {
                build_id: "trio-worker".to_string(),
            })
            .task_types(WorkerTaskTypes {
                enable_workflows: true,
                enable_local_activities: false,
                enable_remote_activities: true,
                enable_nexus: false,
            })
            .build()
            .map_err(|e| anyhow!("Failed to build worker config: {}", e))?;

        // Initialize the worker
        let worker = init_worker(&runtime, worker_config, retry_client)?;

        // Store runtime
        let mut runtime_guard = self.runtime.lock().await;
        *runtime_guard = Some(runtime);
        drop(runtime_guard);

        // Store worker wrapped in Arc for concurrent access
        *worker_guard = Some(Arc::new(worker));

        Ok(())
    }

    /// Get a clone of the worker Arc
    ///
    /// This briefly locks the mutex to clone the Arc, then drops the lock.
    /// The caller can then use the Arc without holding any lock.
    async fn get_worker(&self) -> Result<Arc<Worker>> {
        let guard = self.worker.lock().await;
        guard
            .clone()
            .ok_or_else(|| anyhow!("Worker not initialized"))
    }

    /// Poll for a workflow activation
    pub async fn poll_workflow_activation(&self) -> Result<Vec<u8>> {
        // Get worker Arc and drop the lock before awaiting
        let worker = self.get_worker().await?;

        // Poll for activation
        let activation = match worker.poll_workflow_activation().await {
            Ok(act) => act,
            Err(PollError::ShutDown) => {
                return Err(anyhow!("PollShutdownError"));
            }
            Err(err) => {
                return Err(anyhow!("Poll failed: {}", err));
            }
        };

        // Encode to protobuf bytes
        let bytes = activation.encode_to_vec();
        Ok(bytes)
    }

    /// Complete a workflow activation
    pub async fn complete_workflow_activation(&self, completion_bytes: Vec<u8>) -> Result<()> {
        // Get worker Arc and drop the lock before awaiting
        let worker = self.get_worker().await?;

        // Decode completion from protobuf bytes
        let completion = WorkflowActivationCompletion::decode(&completion_bytes[..])
            .map_err(|e| anyhow!("Failed to decode completion: {}", e))?;

        // Complete the activation
        worker
            .complete_workflow_activation(completion)
            .await
            .map_err(|e| anyhow!("Complete failed: {}", e))?;

        Ok(())
    }

    /// Poll for an activity task
    pub async fn poll_activity_task(&self) -> Result<Vec<u8>> {
        // Get worker Arc and drop the lock before awaiting
        let worker = self.get_worker().await?;

        // Poll for activity task
        let task = match worker.poll_activity_task().await {
            Ok(task) => task,
            Err(PollError::ShutDown) => {
                return Err(anyhow!("PollShutdownError"));
            }
            Err(err) => {
                return Err(anyhow!("Poll activity failed: {}", err));
            }
        };

        // Encode to protobuf bytes
        let bytes = task.encode_to_vec();
        Ok(bytes)
    }

    /// Complete an activity task
    pub async fn complete_activity_task(&self, completion_bytes: Vec<u8>) -> Result<()> {
        // Get worker Arc and drop the lock before awaiting
        let worker = self.get_worker().await?;

        // Decode completion from protobuf bytes
        let completion = ActivityTaskCompletion::decode(&completion_bytes[..])
            .map_err(|e| anyhow!("Failed to decode activity completion: {}", e))?;

        // Complete the activity task
        worker
            .complete_activity_task(completion)
            .await
            .map_err(|e| anyhow!("Complete activity failed: {}", e))?;

        Ok(())
    }

    /// Record an activity heartbeat
    pub async fn record_activity_heartbeat(&self, heartbeat_bytes: Vec<u8>) -> Result<()> {
        // Get worker Arc and drop the lock before doing work
        let worker = self.get_worker().await?;

        // Decode heartbeat from protobuf bytes
        let heartbeat = ActivityHeartbeat::decode(&heartbeat_bytes[..])
            .map_err(|e| anyhow!("Failed to decode activity heartbeat: {}", e))?;

        // Record the heartbeat (fire and forget internally by SDK Core)
        worker.record_activity_heartbeat(heartbeat);

        Ok(())
    }

    /// Validate that the worker is initialized and ready
    pub async fn validate(&self) -> Result<()> {
        // Just try to get the worker - this validates it's initialized
        let _ = self.get_worker().await?;
        Ok(())
    }

    /// Initiate graceful shutdown of the worker
    pub async fn initiate_shutdown(&self) -> Result<()> {
        // Get worker Arc - no await after this so lock timing doesn't matter
        let worker = self.get_worker().await?;
        worker.initiate_shutdown();
        Ok(())
    }

    /// Finalize shutdown and wait for completion
    pub async fn finalize_shutdown(&self) -> Result<()> {
        // Need to take ownership for finalize_shutdown, so we take from the Option
        let mut worker_guard = self.worker.lock().await;
        let worker_opt = worker_guard.take();
        // Drop the lock before any awaiting
        drop(worker_guard);

        if let Some(worker) = worker_opt {
            // Now we need to get the inner Worker from the Arc
            // If there are other references, this will fail - that's expected during proper shutdown
            match Arc::try_unwrap(worker) {
                Ok(inner_worker) => {
                    inner_worker.finalize_shutdown().await;
                }
                Err(_arc) => {
                    // Other references exist - this shouldn't happen during proper shutdown
                    // but we can't finalize without ownership
                    return Err(anyhow!(
                        "Cannot finalize shutdown: other references to worker exist"
                    ));
                }
            }
        }

        // Drop the runtime
        let mut runtime_guard = self.runtime.lock().await;
        *runtime_guard = None;

        Ok(())
    }
}

impl Default for CoreWorkerHandle {
    fn default() -> Self {
        Self::new()
    }
}
