use anyhow::{anyhow, Result};
use base64::Engine;
use prost::Message;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use temporalio_client::{ClientOptions, ClientTlsOptions, TlsOptions, WorkflowService};
use temporalio_sdk_core::CoreRuntime;
use tokio::sync::Mutex;
use tonic;
use url::Url;

/// TLS configuration received from Python (base64-encoded cert bytes).
#[derive(serde::Deserialize)]
pub struct TlsInitConfig {
    /// Base64-encoded root CA certificate (PEM bytes).
    pub server_root_ca_cert: Option<String>,
    /// TLS domain override.
    pub domain: Option<String>,
    /// Base64-encoded client certificate (PEM bytes) for mTLS.
    pub client_cert: Option<String>,
    /// Base64-encoded client private key (PEM bytes) for mTLS.
    pub client_private_key: Option<String>,
}

/// Configuration for initializing the Core Client
#[derive(serde::Deserialize)]
pub struct ClientInitConfig {
    pub target_url: String,
    pub namespace: String,
    #[serde(default)]
    pub identity: String,
    #[serde(default)]
    pub api_key: Option<String>,
    #[serde(default)]
    pub tls_config: Option<TlsInitConfig>,
    #[serde(default)]
    pub rpc_metadata: Option<HashMap<String, String>>,
}

type ClientType = temporalio_sdk_core::RetryClient<temporalio_client::ConfiguredClient<temporalio_client::TemporalServiceClient>>;

/// The inner configured client type, used when sharing the client's gRPC
/// connection with workers (same pattern as sdk-python's
/// `client.retry_client.clone().into_inner()`).
pub type InnerClientType = temporalio_client::ConfiguredClient<temporalio_client::TemporalServiceClient>;

/// Inner state behind the mutex — holds client, runtime, and namespace.
struct ClientState {
    client: Option<ClientType>,
    runtime: Option<Arc<CoreRuntime>>,
    namespace: String,
}

/// Wrapper around the Temporal SDK Core Client that provides thread-safe access.
///
/// All methods clone the underlying gRPC client (which is cheap — shares the
/// same connection channel) so that multiple concurrent RPCs can proceed without
/// blocking each other. The mutex is only held briefly to clone the client.
pub struct CoreClientHandle {
    state: Mutex<ClientState>,
}

impl CoreClientHandle {
    /// Create a new uninitialized CoreClientHandle
    pub fn new() -> Self {
        Self {
            state: Mutex::new(ClientState {
                client: None,
                runtime: None,
                namespace: String::new(),
            }),
        }
    }

    /// Get a clone of the client and namespace for concurrent use.
    ///
    /// The underlying tonic gRPC client and RetryClient are cheaply cloneable
    /// (they share the same connection/channel). Cloning allows multiple
    /// concurrent gRPC calls without holding the mutex for the entire duration
    /// of long-polling RPCs (e.g., update_workflow, get_workflow_result).
    async fn get_client(&self) -> Result<(ClientType, String)> {
        let guard = self.state.lock().await;
        let client = guard
            .client
            .as_ref()
            .cloned()
            .ok_or_else(|| anyhow!("Client not initialized"))?;
        let namespace = guard.namespace.clone();
        Ok((client, namespace))
    }

    /// Initialize the client with the given configuration
    pub async fn initialize(&self, config: ClientInitConfig) -> Result<()> {
        let mut guard = self.state.lock().await;
        if guard.client.is_some() {
            return Err(anyhow!("Client already initialized"));
        }

        // Create CoreRuntime with default options
        use temporalio_sdk_core::RuntimeOptions;
        let runtime_options = RuntimeOptions::default();
        let runtime = CoreRuntime::new_assume_tokio(runtime_options)?;

        // Parse target URL
        let target_url =
            Url::from_str(&config.target_url).map_err(|e| anyhow!("Invalid target URL: {}", e))?;

        // Build TLS options from config
        let tls_options = if let Some(tls_cfg) = &config.tls_config {
            let b64 = base64::engine::general_purpose::STANDARD;
            let server_root_ca_cert = tls_cfg
                .server_root_ca_cert
                .as_ref()
                .map(|s| b64.decode(s))
                .transpose()
                .map_err(|e| anyhow!("Invalid base64 for server_root_ca_cert: {}", e))?;

            let client_tls_options = match (&tls_cfg.client_cert, &tls_cfg.client_private_key) {
                (Some(cert_b64), Some(key_b64)) => {
                    let client_cert = b64
                        .decode(cert_b64)
                        .map_err(|e| anyhow!("Invalid base64 for client_cert: {}", e))?;
                    let client_private_key = b64
                        .decode(key_b64)
                        .map_err(|e| anyhow!("Invalid base64 for client_private_key: {}", e))?;
                    Some(ClientTlsOptions {
                        client_cert,
                        client_private_key,
                    })
                }
                _ => None,
            };

            Some(TlsOptions {
                server_root_ca_cert,
                domain: tls_cfg.domain.clone(),
                client_tls_options,
            })
        } else {
            None
        };

        // Create client options and connect (using bon builder)
        let client_options = ClientOptions::builder()
            .target_url(target_url)
            .client_name("temporal-trio-sdk".to_string())
            .client_version(env!("CARGO_PKG_VERSION").to_string())
            .identity(if config.identity.is_empty() {
                "trio-client".to_string()
            } else {
                config.identity.clone()
            })
            .maybe_tls_options(tls_options)
            .maybe_api_key(config.api_key.clone())
            .maybe_headers(config.rpc_metadata.clone())
            .build();

        // Connect to Temporal server
        let client = client_options
            .connect_no_namespace(None)
            .await
            .map_err(|e| anyhow!("Failed to connect to Temporal server: {}", e))?;

        // Store everything
        guard.runtime = Some(Arc::new(runtime));
        guard.client = Some(client);
        guard.namespace = config.namespace;

        Ok(())
    }

    /// Start a workflow execution
    pub async fn start_workflow_execution(&self, request_bytes: Vec<u8>) -> Result<Vec<u8>> {
        let (mut client, _ns) = self.get_client().await?;

        use temporalio_common::protos::temporal::api::workflowservice::v1::StartWorkflowExecutionRequest;
        let request = StartWorkflowExecutionRequest::decode(&request_bytes[..])
            .map_err(|e| anyhow!("Failed to decode start workflow request: {}", e))?;

        let response = client
            .start_workflow_execution(tonic::Request::new(request))
            .await
            .map_err(|e| anyhow!("Failed to start workflow: {}", e))?;

        Ok(response.into_inner().encode_to_vec())
    }

    /// Get workflow execution result (blocking until complete)
    pub async fn get_workflow_result(
        &self,
        workflow_id: String,
        run_id: Option<String>,
    ) -> Result<Vec<u8>> {
        let (mut client, ns) = self.get_client().await?;

        use temporalio_common::protos::temporal::api::workflowservice::v1::GetWorkflowExecutionHistoryRequest;
        use temporalio_common::protos::temporal::api::common::v1::WorkflowExecution;

        let request = GetWorkflowExecutionHistoryRequest {
            namespace: ns,
            execution: Some(WorkflowExecution {
                workflow_id,
                run_id: run_id.unwrap_or_default(),
            }),
            wait_new_event: true,
            history_event_filter_type: 2, // CLOSE_EVENT
            skip_archival: true,
            ..Default::default()
        };

        let response = client
            .get_workflow_execution_history(tonic::Request::new(request))
            .await
            .map_err(|e| anyhow!("Failed to get workflow result: {}", e))?;

        Ok(response.into_inner().encode_to_vec())
    }

    /// Get full workflow execution history (all events, no filtering)
    pub async fn get_workflow_execution_history(
        &self,
        workflow_id: String,
        run_id: Option<String>,
        next_page_token: Vec<u8>,
        event_filter_type: Option<i32>,
        skip_archival: Option<bool>,
    ) -> Result<Vec<u8>> {
        let (mut client, ns) = self.get_client().await?;

        use temporalio_common::protos::temporal::api::workflowservice::v1::GetWorkflowExecutionHistoryRequest;
        use temporalio_common::protos::temporal::api::common::v1::WorkflowExecution;

        let request = GetWorkflowExecutionHistoryRequest {
            namespace: ns,
            execution: Some(WorkflowExecution {
                workflow_id,
                run_id: run_id.unwrap_or_default(),
            }),
            wait_new_event: false,
            history_event_filter_type: event_filter_type.unwrap_or(0),
            skip_archival: skip_archival.unwrap_or(true),
            next_page_token,
            ..Default::default()
        };

        let response = client
            .get_workflow_execution_history(tonic::Request::new(request))
            .await
            .map_err(|e| anyhow!("Failed to get workflow execution history: {}", e))?;

        Ok(response.into_inner().encode_to_vec())
    }

    /// Cancel a workflow execution
    pub async fn cancel_workflow_execution(
        &self,
        workflow_id: String,
        run_id: Option<String>,
        first_execution_run_id: Option<String>,
    ) -> Result<()> {
        let (mut client, ns) = self.get_client().await?;

        use temporalio_common::protos::temporal::api::workflowservice::v1::RequestCancelWorkflowExecutionRequest;
        use temporalio_common::protos::temporal::api::common::v1::WorkflowExecution;

        let request = RequestCancelWorkflowExecutionRequest {
            namespace: ns,
            workflow_execution: Some(WorkflowExecution {
                workflow_id,
                run_id: run_id.unwrap_or_default(),
            }),
            first_execution_run_id: first_execution_run_id.unwrap_or_default(),
            ..Default::default()
        };

        client
            .request_cancel_workflow_execution(tonic::Request::new(request))
            .await
            .map_err(|e| anyhow!("Failed to cancel workflow: {}", e))?;

        Ok(())
    }

    /// Terminate a workflow execution
    pub async fn terminate_workflow_execution(
        &self,
        workflow_id: String,
        run_id: Option<String>,
        reason: String,
        first_execution_run_id: Option<String>,
        details_bytes: Vec<u8>,
    ) -> Result<()> {
        let (mut client, ns) = self.get_client().await?;

        use temporalio_common::protos::temporal::api::workflowservice::v1::TerminateWorkflowExecutionRequest;
        use temporalio_common::protos::temporal::api::common::v1::{WorkflowExecution, Payloads};

        let details = if details_bytes.is_empty() {
            None
        } else {
            Some(Payloads::decode(&details_bytes[..])
                .map_err(|e| anyhow!("Failed to decode terminate details: {}", e))?)
        };

        let request = TerminateWorkflowExecutionRequest {
            namespace: ns,
            workflow_execution: Some(WorkflowExecution {
                workflow_id,
                run_id: run_id.unwrap_or_default(),
            }),
            reason,
            first_execution_run_id: first_execution_run_id.unwrap_or_default(),
            details,
            ..Default::default()
        };

        client
            .terminate_workflow_execution(tonic::Request::new(request))
            .await
            .map_err(|e| anyhow!("Failed to terminate workflow: {}", e))?;

        Ok(())
    }

    /// Query a workflow execution
    pub async fn query_workflow(
        &self,
        workflow_id: String,
        run_id: Option<String>,
        query_type: String,
        args_bytes: Vec<u8>,
        reject_condition: Option<i32>,
    ) -> Result<Vec<u8>> {
        let (mut client, ns) = self.get_client().await?;

        use temporalio_common::protos::temporal::api::workflowservice::v1::QueryWorkflowRequest;
        use temporalio_common::protos::temporal::api::common::v1::{WorkflowExecution, Payloads};
        use temporalio_common::protos::temporal::api::query::v1::WorkflowQuery;

        let payloads = if args_bytes.is_empty() {
            None
        } else {
            Some(Payloads::decode(&args_bytes[..])
                .map_err(|e| anyhow!("Failed to decode query args: {}", e))?)
        };

        let request = QueryWorkflowRequest {
            namespace: ns,
            execution: Some(WorkflowExecution {
                workflow_id,
                run_id: run_id.unwrap_or_default(),
            }),
            query: Some(WorkflowQuery {
                query_type,
                query_args: payloads,
                ..Default::default()
            }),
            query_reject_condition: reject_condition.unwrap_or(0),
            ..Default::default()
        };

        let response = client
            .query_workflow(tonic::Request::new(request))
            .await
            .map_err(|e| anyhow!("Failed to query workflow: {}", e))?;

        Ok(response.into_inner().encode_to_vec())
    }

    /// Describe a workflow execution
    pub async fn describe_workflow_execution(
        &self,
        workflow_id: String,
        run_id: Option<String>,
    ) -> Result<Vec<u8>> {
        let (mut client, ns) = self.get_client().await?;

        use temporalio_common::protos::temporal::api::workflowservice::v1::DescribeWorkflowExecutionRequest;
        use temporalio_common::protos::temporal::api::common::v1::WorkflowExecution;

        let request = DescribeWorkflowExecutionRequest {
            namespace: ns,
            execution: Some(WorkflowExecution {
                workflow_id,
                run_id: run_id.unwrap_or_default(),
            }),
        };

        let response = client
            .describe_workflow_execution(tonic::Request::new(request))
            .await
            .map_err(|e| anyhow!("Failed to describe workflow: {}", e))?;

        Ok(response.into_inner().encode_to_vec())
    }

    /// Signal a workflow execution
    pub async fn signal_workflow(
        &self,
        workflow_id: String,
        run_id: Option<String>,
        signal_name: String,
        args_bytes: Vec<u8>,
    ) -> Result<()> {
        let (mut client, ns) = self.get_client().await?;

        use temporalio_common::protos::temporal::api::workflowservice::v1::SignalWorkflowExecutionRequest;
        use temporalio_common::protos::temporal::api::common::v1::{WorkflowExecution, Payloads};

        let payloads = if args_bytes.is_empty() {
            None
        } else {
            Some(Payloads::decode(&args_bytes[..])
                .map_err(|e| anyhow!("Failed to decode signal args: {}", e))?)
        };

        let request = SignalWorkflowExecutionRequest {
            namespace: ns,
            workflow_execution: Some(WorkflowExecution {
                workflow_id,
                run_id: run_id.unwrap_or_default(),
            }),
            signal_name,
            input: payloads,
            ..Default::default()
        };

        client
            .signal_workflow_execution(tonic::Request::new(request))
            .await
            .map_err(|e| anyhow!("Failed to signal workflow: {}", e))?;

        Ok(())
    }

    /// Signal-with-start a workflow execution (protobuf passthrough)
    pub async fn signal_with_start_workflow_execution(&self, request_bytes: Vec<u8>) -> Result<Vec<u8>> {
        let (mut client, _ns) = self.get_client().await?;

        use temporalio_common::protos::temporal::api::workflowservice::v1::SignalWithStartWorkflowExecutionRequest;
        let request = SignalWithStartWorkflowExecutionRequest::decode(&request_bytes[..])
            .map_err(|e| anyhow!("Failed to decode signal_with_start request: {}", e))?;

        let response = client
            .signal_with_start_workflow_execution(tonic::Request::new(request))
            .await
            .map_err(|e| anyhow!("Failed to signal_with_start workflow: {}", e))?;

        Ok(response.into_inner().encode_to_vec())
    }

    /// Update a workflow execution
    pub async fn update_workflow(
        &self,
        request_bytes: Vec<u8>,
    ) -> Result<Vec<u8>> {
        let (mut client, _ns) = self.get_client().await?;

        use temporalio_common::protos::temporal::api::workflowservice::v1::UpdateWorkflowExecutionRequest;
        let request = UpdateWorkflowExecutionRequest::decode(&request_bytes[..])
            .map_err(|e| anyhow!("Failed to decode update workflow request: {}", e))?;

        let response = client
            .update_workflow_execution(tonic::Request::new(request))
            .await
            .map_err(|e| anyhow!("Failed to update workflow: {}", e))?;

        Ok(response.into_inner().encode_to_vec())
    }

    /// Poll for a workflow execution update result
    pub async fn poll_workflow_execution_update(
        &self,
        request_bytes: Vec<u8>,
    ) -> Result<Vec<u8>> {
        let (mut client, _ns) = self.get_client().await?;

        use temporalio_common::protos::temporal::api::workflowservice::v1::PollWorkflowExecutionUpdateRequest;
        let request = PollWorkflowExecutionUpdateRequest::decode(&request_bytes[..])
            .map_err(|e| anyhow!("Failed to decode poll update request: {}", e))?;

        let response = client
            .poll_workflow_execution_update(tonic::Request::new(request))
            .await
            .map_err(|e| anyhow!("Failed to poll update: {}", e))?;

        Ok(response.into_inner().encode_to_vec())
    }

    /// List workflow executions
    pub async fn list_workflow_executions(&self, request_bytes: Vec<u8>) -> Result<Vec<u8>> {
        let (mut client, _ns) = self.get_client().await?;

        use temporalio_common::protos::temporal::api::workflowservice::v1::ListWorkflowExecutionsRequest;
        let request = ListWorkflowExecutionsRequest::decode(&request_bytes[..])
            .map_err(|e| anyhow!("Failed to decode list workflow executions request: {}", e))?;

        let response = client
            .list_workflow_executions(tonic::Request::new(request))
            .await
            .map_err(|e| anyhow!("Failed to list workflow executions: {}", e))?;

        Ok(response.into_inner().encode_to_vec())
    }

    /// Count workflow executions
    pub async fn count_workflow_executions(&self, request_bytes: Vec<u8>) -> Result<Vec<u8>> {
        let (mut client, _ns) = self.get_client().await?;

        use temporalio_common::protos::temporal::api::workflowservice::v1::CountWorkflowExecutionsRequest;
        let request = CountWorkflowExecutionsRequest::decode(&request_bytes[..])
            .map_err(|e| anyhow!("Failed to decode count workflow executions request: {}", e))?;

        let response = client
            .count_workflow_executions(tonic::Request::new(request))
            .await
            .map_err(|e| anyhow!("Failed to count workflow executions: {}", e))?;

        Ok(response.into_inner().encode_to_vec())
    }

    /// Get the inner configured client for use by a worker.
    ///
    /// This clones the `RetryClient` and extracts its inner `ConfiguredClient`.
    /// The clone is cheap — it shares the same tonic channel (gRPC connection).
    /// This matches sdk-python's `client.retry_client.clone().into_inner()`.
    pub async fn get_client_for_worker(&self) -> Result<InnerClientType> {
        let guard = self.state.lock().await;
        let client = guard
            .client
            .as_ref()
            .ok_or_else(|| anyhow!("Client not initialized"))?;
        Ok(client.clone().into_inner())
    }

    /// Get a reference to the CoreRuntime for sharing with workers.
    ///
    /// Workers need the runtime for telemetry and other core operations.
    /// Returns a clone of the Arc reference (cheap, shares the same runtime).
    pub async fn get_runtime(&self) -> Result<Arc<CoreRuntime>> {
        let guard = self.state.lock().await;
        guard
            .runtime
            .clone()
            .ok_or_else(|| anyhow!("Client not initialized (no runtime)"))
    }

    /// Validate that the client is initialized and ready
    pub async fn validate(&self) -> Result<()> {
        let guard = self.state.lock().await;
        if guard.client.is_some() {
            Ok(())
        } else {
            Err(anyhow!("Client not initialized"))
        }
    }
}

impl Default for CoreClientHandle {
    fn default() -> Self {
        Self::new()
    }
}
