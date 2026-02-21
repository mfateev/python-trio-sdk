use anyhow::{anyhow, Result};
use prost::Message;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use temporalio_client::ClientOptions;
use temporalio_common::errors::PollError;
use temporalio_common::protos::coresdk::workflow_completion::WorkflowActivationCompletion;
use temporalio_common::protos::coresdk::{ActivityHeartbeat, ActivityTaskCompletion};
use temporalio_common::telemetry::{
    MetricTemporality, OtelCollectorOptions, OtlpProtocol, PrometheusExporterOptions,
    TelemetryOptions,
};
use temporalio_common::worker::{WorkerTaskTypes, WorkerVersioningStrategy};
use temporalio_common::Worker as WorkerTrait;
use temporalio_sdk_core::telemetry::{build_otlp_metric_exporter, start_prometheus_metric_exporter};
use temporalio_sdk_core::{init_worker, CoreRuntime, RetryClient, RuntimeOptions, Worker, WorkerConfig};
use tokio::sync::Mutex;
use url::Url;

/// Telemetry configuration received from Python
#[derive(serde::Deserialize, Default)]
pub struct TelemetryInitConfig {
    pub prometheus: Option<PrometheusInitConfig>,
    pub opentelemetry: Option<OpenTelemetryInitConfig>,
    #[serde(default = "default_true")]
    pub attach_service_name: bool,
    pub metric_prefix: Option<String>,
    pub global_tags: Option<HashMap<String, String>>,
}

#[derive(serde::Deserialize)]
pub struct PrometheusInitConfig {
    pub bind_address: String,
    #[serde(default)]
    pub counters_total_suffix: bool,
    #[serde(default)]
    pub unit_suffix: bool,
    #[serde(default)]
    pub durations_as_seconds: bool,
}

#[derive(serde::Deserialize)]
pub struct OpenTelemetryInitConfig {
    pub url: String,
    #[serde(default)]
    pub headers: HashMap<String, String>,
    pub metric_periodicity_millis: Option<u64>,
    #[serde(default)]
    pub metric_temporality_delta: bool,
    #[serde(default)]
    pub durations_as_seconds: bool,
    #[serde(default)]
    pub http: bool,
}

fn default_true() -> bool {
    true
}

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
    #[serde(default)]
    pub telemetry: Option<TelemetryInitConfig>,
}

fn default_max_cached_workflows() -> usize {
    1000
}

fn default_max_concurrent_polls() -> usize {
    5
}

fn default_sticky_queue_schedule_to_start_timeout_millis() -> u64 {
    10_000 // 10 seconds
}

/// Wrapper around the Temporal SDK Core Worker.
///
/// The inner Worker is stored as `Option<Arc<Worker>>` — matching the pattern
/// used by the official C-bridge. SDK-Core's Worker handles its own internal
/// synchronization, so no external Mutex is needed for poll/complete operations.
/// A Mutex is only used during one-time initialization.
pub struct CoreWorkerHandle {
    worker: Mutex<Option<Arc<Worker>>>,
    runtime: Mutex<Option<CoreRuntime>>,
}

impl CoreWorkerHandle {
    /// Create a new uninitialized CoreWorkerHandle
    pub fn new() -> Self {
        Self {
            worker: Mutex::new(None),
            runtime: Mutex::new(None),
        }
    }

    /// Get the worker Arc, or error if not initialized.
    async fn worker(&self) -> Result<Arc<Worker>> {
        self.worker
            .lock()
            .await
            .clone()
            .ok_or_else(|| anyhow!("Worker not initialized"))
    }

    /// Initialize the worker with the given configuration
    pub async fn initialize(&self, config: WorkerInitConfig) -> Result<()> {
        let mut worker_guard = self.worker.lock().await;
        if worker_guard.is_some() {
            return Err(anyhow!("Worker already initialized"));
        }

        // Build TelemetryOptions from config
        let telem_config = config.telemetry.unwrap_or_default();
        let telemetry_options = TelemetryOptions::builder()
            .attach_service_name(telem_config.attach_service_name)
            .metric_prefix(
                telem_config
                    .metric_prefix
                    .unwrap_or_else(|| "temporal_".to_string()),
            )
            .build();

        // Build RuntimeOptions with telemetry
        let runtime_options = RuntimeOptions::builder()
            .telemetry_options(telemetry_options)
            .build()
            .map_err(|e| anyhow!("Failed to build runtime options: {}", e))?;

        // Create CoreRuntime (Tokio runtime already active)
        let mut runtime = CoreRuntime::new_assume_tokio(runtime_options)?;

        // Late-bind metrics exporter (requires active Tokio runtime)
        if let Some(ref prom) = telem_config.prometheus {
            let global_tags = telem_config.global_tags.clone().unwrap_or_default();
            let prom_opts = PrometheusExporterOptions::builder()
                .socket_addr(
                    prom.bind_address
                        .parse()
                        .map_err(|e| anyhow!("Invalid Prometheus bind address: {}", e))?,
                )
                .global_tags(global_tags)
                .counters_total_suffix(prom.counters_total_suffix)
                .unit_suffix(prom.unit_suffix)
                .use_seconds_for_durations(prom.durations_as_seconds)
                .build();
            let started = start_prometheus_metric_exporter(prom_opts)
                .map_err(|e| anyhow!("Failed to start Prometheus exporter: {}", e))?;
            runtime
                .telemetry_mut()
                .attach_late_init_metrics(started.meter);
        } else if let Some(ref otel) = telem_config.opentelemetry {
            let global_tags = telem_config.global_tags.clone().unwrap_or_default();
            let otel_opts = OtelCollectorOptions::builder()
                .url(
                    Url::from_str(&otel.url)
                        .map_err(|e| anyhow!("Invalid OTel collector URL: {}", e))?,
                )
                .headers(otel.headers.clone())
                .metric_periodicity(Duration::from_millis(
                    otel.metric_periodicity_millis.unwrap_or(1000),
                ))
                .metric_temporality(if otel.metric_temporality_delta {
                    MetricTemporality::Delta
                } else {
                    MetricTemporality::Cumulative
                })
                .use_seconds_for_durations(otel.durations_as_seconds)
                .global_tags(global_tags)
                .protocol(if otel.http {
                    OtlpProtocol::Http
                } else {
                    OtlpProtocol::Grpc
                })
                .build();
            let meter = build_otlp_metric_exporter(otel_opts)
                .map_err(|e| anyhow!("Failed to build OTel exporter: {}", e))?;
            runtime
                .telemetry_mut()
                .attach_late_init_metrics(Arc::new(meter));
        }

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
        let worker_config = WorkerConfig::builder()
            .namespace(config.namespace.clone())
            .task_queue(config.task_queue.clone())
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

        // Store worker as Arc (no lock needed for subsequent operations)
        *worker_guard = Some(Arc::new(worker));

        Ok(())
    }

    /// Poll for a workflow activation (lock-free — uses cloned Arc)
    pub async fn poll_workflow_activation(&self) -> Result<Vec<u8>> {
        let worker = self.worker().await?;

        let activation = match worker.poll_workflow_activation().await {
            Ok(act) => act,
            Err(PollError::ShutDown) => {
                return Err(anyhow!("PollShutdownError"));
            }
            Err(err) => {
                return Err(anyhow!("Poll failed: {}", err));
            }
        };

        let bytes = activation.encode_to_vec();
        Ok(bytes)
    }

    /// Complete a workflow activation (lock-free — uses cloned Arc)
    pub async fn complete_workflow_activation(&self, completion_bytes: Vec<u8>) -> Result<()> {
        let worker = self.worker().await?;

        let completion = WorkflowActivationCompletion::decode(&completion_bytes[..])
            .map_err(|e| anyhow!("Failed to decode completion: {}", e))?;

        worker
            .complete_workflow_activation(completion)
            .await
            .map_err(|e| anyhow!("Complete failed: {}", e))?;

        Ok(())
    }

    /// Poll for an activity task (lock-free)
    pub async fn poll_activity_task(&self) -> Result<Vec<u8>> {
        let worker = self.worker().await?;

        let task = match worker.poll_activity_task().await {
            Ok(task) => task,
            Err(PollError::ShutDown) => {
                return Err(anyhow!("PollShutdownError"));
            }
            Err(err) => {
                return Err(anyhow!("Poll activity failed: {}", err));
            }
        };

        let bytes = task.encode_to_vec();
        Ok(bytes)
    }

    /// Complete an activity task (lock-free)
    pub async fn complete_activity_task(&self, completion_bytes: Vec<u8>) -> Result<()> {
        let worker = self.worker().await?;

        let completion = ActivityTaskCompletion::decode(&completion_bytes[..])
            .map_err(|e| anyhow!("Failed to decode activity completion: {}", e))?;

        worker
            .complete_activity_task(completion)
            .await
            .map_err(|e| anyhow!("Complete activity failed: {}", e))?;

        Ok(())
    }

    /// Record an activity heartbeat (lock-free)
    pub async fn record_activity_heartbeat(&self, heartbeat_bytes: Vec<u8>) -> Result<()> {
        let worker = self.worker().await?;

        let heartbeat = ActivityHeartbeat::decode(&heartbeat_bytes[..])
            .map_err(|e| anyhow!("Failed to decode activity heartbeat: {}", e))?;

        worker.record_activity_heartbeat(heartbeat);

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

    /// Initiate graceful shutdown of the worker (lock-free)
    pub async fn initiate_shutdown(&self) -> Result<()> {
        let worker = self.worker().await?;
        worker.initiate_shutdown();
        Ok(())
    }

    /// Finalize shutdown and wait for completion
    pub async fn finalize_shutdown(&self) -> Result<()> {
        // Take the Arc out; try_unwrap to get owned Worker for finalize_shutdown
        let worker = {
            let mut guard = self.worker.lock().await;
            guard.take()
        };
        if let Some(worker_arc) = worker {
            match Arc::try_unwrap(worker_arc) {
                Ok(worker) => {
                    worker.finalize_shutdown().await;
                }
                Err(_arc) => {
                    // Other references still exist; just drop our reference.
                    // The worker will be cleaned up when all references are dropped.
                }
            }
        }

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
