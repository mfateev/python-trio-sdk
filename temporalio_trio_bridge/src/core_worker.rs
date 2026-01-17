use anyhow::{anyhow, Result};
use prost::Message;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use temporalio_client::ClientOptions;
use temporalio_common::protos::coresdk::{
    workflow_activation::WorkflowActivation, workflow_completion::WorkflowActivationCompletion,
};
use temporalio_common::errors::PollError;
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
}

fn default_max_cached_workflows() -> usize {
    1000
}

fn default_max_concurrent_polls() -> usize {
    5
}

/// Wrapper around the Temporal SDK Core Worker that provides thread-safe access
pub struct CoreWorkerHandle {
    worker: Arc<Mutex<Option<Worker>>>,
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
        let target_url = Url::from_str(&config.target_url)
            .map_err(|e| anyhow!("Invalid target URL: {}", e))?;

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
            .versioning_strategy(WorkerVersioningStrategy::None {
                build_id: "trio-worker".to_string(),
            })
            .task_types(WorkerTaskTypes {
                enable_workflows: true,
                enable_local_activities: false,
                enable_remote_activities: false,
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

        // Store worker
        *worker_guard = Some(worker);

        Ok(())
    }

    /// Poll for a workflow activation
    pub async fn poll_workflow_activation(&self) -> Result<Vec<u8>> {
        let guard = self.worker.lock().await;
        let worker = guard
            .as_ref()
            .ok_or_else(|| anyhow!("Worker not initialized"))?;

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
        let guard = self.worker.lock().await;
        let worker = guard
            .as_ref()
            .ok_or_else(|| anyhow!("Worker not initialized"))?;

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

    /// Validate that the worker is initialized and ready
    pub async fn validate(&self) -> Result<()> {
        let guard = self.worker.lock().await;
        if guard.is_some() {
            Ok(())
        } else {
            Err(anyhow!("Worker not initialized"))
        }
    }

    /// Initiate graceful shutdown of the worker
    pub async fn initiate_shutdown(&self) -> Result<()> {
        let guard = self.worker.lock().await;
        if let Some(worker) = guard.as_ref() {
            worker.initiate_shutdown();
            Ok(())
        } else {
            Err(anyhow!("Worker not initialized"))
        }
    }

    /// Finalize shutdown and wait for completion
    pub async fn finalize_shutdown(&self) -> Result<()> {
        let mut worker_guard = self.worker.lock().await;
        if let Some(worker) = worker_guard.take() {
            worker.finalize_shutdown().await;
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
