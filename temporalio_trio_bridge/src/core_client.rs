use anyhow::{anyhow, Result};
use prost::Message;
use std::str::FromStr;
use std::sync::Arc;
use temporalio_client::{ClientOptions, WorkflowService};
use temporalio_sdk_core::CoreRuntime;
use tokio::sync::Mutex;
use tonic;
use url::Url;

/// Configuration for initializing the Core Client
#[derive(serde::Deserialize)]
pub struct ClientInitConfig {
    pub target_url: String,
    pub namespace: String,
    #[serde(default)]
    pub identity: String,
}

type ClientType = temporalio_sdk_core::RetryClient<temporalio_client::ConfiguredClient<temporalio_client::TemporalServiceClient>>;

/// Wrapper around the Temporal SDK Core Client that provides thread-safe access
pub struct CoreClientHandle {
    client: Arc<Mutex<Option<ClientType>>>,
    runtime: Arc<Mutex<Option<CoreRuntime>>>,
    namespace: String,
}

impl CoreClientHandle {
    /// Create a new uninitialized CoreClientHandle
    pub fn new() -> Self {
        Self {
            client: Arc::new(Mutex::new(None)),
            runtime: Arc::new(Mutex::new(None)),
            namespace: String::new(),
        }
    }

    /// Initialize the client with the given configuration
    pub async fn initialize(&mut self, config: ClientInitConfig) -> Result<()> {
        let mut client_guard = self.client.lock().await;
        if client_guard.is_some() {
            return Err(anyhow!("Client already initialized"));
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
                "trio-client".to_string()
            } else {
                config.identity.clone()
            })
            .build();

        // Connect to Temporal server
        let client = client_options
            .connect_no_namespace(None)
            .await
            .map_err(|e| anyhow!("Failed to connect to Temporal server: {}", e))?;

        // Store runtime
        let mut runtime_guard = self.runtime.lock().await;
        *runtime_guard = Some(runtime);
        drop(runtime_guard);

        // Store client and namespace
        *client_guard = Some(client);
        self.namespace = config.namespace;

        Ok(())
    }

    /// Start a workflow execution
    pub async fn start_workflow_execution(&self, request_bytes: Vec<u8>) -> Result<Vec<u8>> {
        let mut guard = self.client.lock().await;
        let client = guard
            .as_mut()
            .ok_or_else(|| anyhow!("Client not initialized"))?;

        // Decode request from protobuf
        use temporalio_common::protos::temporal::api::workflowservice::v1::StartWorkflowExecutionRequest;
        let request = StartWorkflowExecutionRequest::decode(&request_bytes[..])
            .map_err(|e| anyhow!("Failed to decode start workflow request: {}", e))?;

        // Call the client
        let response = client
            .start_workflow_execution(tonic::Request::new(request))
            .await
            .map_err(|e| anyhow!("Failed to start workflow: {}", e))?;

        // Encode response to protobuf bytes
        let bytes = response.into_inner().encode_to_vec();
        Ok(bytes)
    }

    /// Get workflow execution result (blocking until complete)
    pub async fn get_workflow_result(
        &self,
        workflow_id: String,
        run_id: Option<String>,
    ) -> Result<Vec<u8>> {
        let mut guard = self.client.lock().await;
        let client = guard
            .as_mut()
            .ok_or_else(|| anyhow!("Client not initialized"))?;

        // Build get_workflow_execution_history request
        // Uses server-side long polling like the Python SDK:
        // - wait_new_event=true: Server blocks until workflow closes or timeout
        // - history_event_filter_type=CLOSE_EVENT (2): Only return close events
        // - skip_archival=true: Don't search archived data
        use temporalio_common::protos::temporal::api::workflowservice::v1::GetWorkflowExecutionHistoryRequest;
        use temporalio_common::protos::temporal::api::common::v1::WorkflowExecution;

        let request = GetWorkflowExecutionHistoryRequest {
            namespace: self.namespace.clone(),
            execution: Some(WorkflowExecution {
                workflow_id,
                run_id: run_id.unwrap_or_default(),
            }),
            wait_new_event: true, // Server-side long polling - blocks until close event
            history_event_filter_type: 2, // CLOSE_EVENT - only return close events
            skip_archival: true,
            ..Default::default()
        };

        // Server will block until workflow closes (completed, failed, canceled, etc.)
        let response = client
            .get_workflow_execution_history(tonic::Request::new(request))
            .await
            .map_err(|e| anyhow!("Failed to get workflow result: {}", e))?;

        // Encode response to protobuf bytes
        let bytes = response.into_inner().encode_to_vec();
        Ok(bytes)
    }

    /// Get full workflow execution history (all events, no filtering)
    pub async fn get_workflow_execution_history(
        &self,
        workflow_id: String,
        run_id: Option<String>,
        next_page_token: Vec<u8>,
    ) -> Result<Vec<u8>> {
        let mut guard = self.client.lock().await;
        let client = guard
            .as_mut()
            .ok_or_else(|| anyhow!("Client not initialized"))?;

        use temporalio_common::protos::temporal::api::workflowservice::v1::GetWorkflowExecutionHistoryRequest;
        use temporalio_common::protos::temporal::api::common::v1::WorkflowExecution;

        let request = GetWorkflowExecutionHistoryRequest {
            namespace: self.namespace.clone(),
            execution: Some(WorkflowExecution {
                workflow_id,
                run_id: run_id.unwrap_or_default(),
            }),
            wait_new_event: false,
            history_event_filter_type: 0, // HISTORY_EVENT_FILTER_TYPE_ALL_EVENT
            skip_archival: true,
            next_page_token,
            ..Default::default()
        };

        let response = client
            .get_workflow_execution_history(tonic::Request::new(request))
            .await
            .map_err(|e| anyhow!("Failed to get workflow execution history: {}", e))?;

        let bytes = response.into_inner().encode_to_vec();
        Ok(bytes)
    }

    /// Cancel a workflow execution
    pub async fn cancel_workflow_execution(
        &self,
        workflow_id: String,
        run_id: Option<String>,
    ) -> Result<()> {
        let mut guard = self.client.lock().await;
        let client = guard
            .as_mut()
            .ok_or_else(|| anyhow!("Client not initialized"))?;

        use temporalio_common::protos::temporal::api::workflowservice::v1::RequestCancelWorkflowExecutionRequest;
        use temporalio_common::protos::temporal::api::common::v1::WorkflowExecution;

        let request = RequestCancelWorkflowExecutionRequest {
            namespace: self.namespace.clone(),
            workflow_execution: Some(WorkflowExecution {
                workflow_id,
                run_id: run_id.unwrap_or_default(),
            }),
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
    ) -> Result<()> {
        let mut guard = self.client.lock().await;
        let client = guard
            .as_mut()
            .ok_or_else(|| anyhow!("Client not initialized"))?;

        use temporalio_common::protos::temporal::api::workflowservice::v1::TerminateWorkflowExecutionRequest;
        use temporalio_common::protos::temporal::api::common::v1::WorkflowExecution;

        let request = TerminateWorkflowExecutionRequest {
            namespace: self.namespace.clone(),
            workflow_execution: Some(WorkflowExecution {
                workflow_id,
                run_id: run_id.unwrap_or_default(),
            }),
            reason,
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
        let mut guard = self.client.lock().await;
        let client = guard
            .as_mut()
            .ok_or_else(|| anyhow!("Client not initialized"))?;

        use temporalio_common::protos::temporal::api::workflowservice::v1::QueryWorkflowRequest;
        use temporalio_common::protos::temporal::api::common::v1::{WorkflowExecution, Payloads};
        use temporalio_common::protos::temporal::api::query::v1::WorkflowQuery;

        // Decode payloads from bytes
        let payloads = if args_bytes.is_empty() {
            None
        } else {
            Some(Payloads::decode(&args_bytes[..])
                .map_err(|e| anyhow!("Failed to decode query args: {}", e))?)
        };

        let request = QueryWorkflowRequest {
            namespace: self.namespace.clone(),
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

        // Encode response to protobuf bytes
        let bytes = response.into_inner().encode_to_vec();
        Ok(bytes)
    }

    /// Describe a workflow execution
    pub async fn describe_workflow_execution(
        &self,
        workflow_id: String,
        run_id: Option<String>,
    ) -> Result<Vec<u8>> {
        let mut guard = self.client.lock().await;
        let client = guard
            .as_mut()
            .ok_or_else(|| anyhow!("Client not initialized"))?;

        use temporalio_common::protos::temporal::api::workflowservice::v1::DescribeWorkflowExecutionRequest;
        use temporalio_common::protos::temporal::api::common::v1::WorkflowExecution;

        let request = DescribeWorkflowExecutionRequest {
            namespace: self.namespace.clone(),
            execution: Some(WorkflowExecution {
                workflow_id,
                run_id: run_id.unwrap_or_default(),
            }),
        };

        let response = client
            .describe_workflow_execution(tonic::Request::new(request))
            .await
            .map_err(|e| anyhow!("Failed to describe workflow: {}", e))?;

        // Encode response to protobuf bytes
        let bytes = response.into_inner().encode_to_vec();
        Ok(bytes)
    }

    /// Signal a workflow execution
    pub async fn signal_workflow(
        &self,
        workflow_id: String,
        run_id: Option<String>,
        signal_name: String,
        args_bytes: Vec<u8>,
    ) -> Result<()> {
        let mut guard = self.client.lock().await;
        let client = guard
            .as_mut()
            .ok_or_else(|| anyhow!("Client not initialized"))?;

        use temporalio_common::protos::temporal::api::workflowservice::v1::SignalWorkflowExecutionRequest;
        use temporalio_common::protos::temporal::api::common::v1::{WorkflowExecution, Payloads};

        // Decode payloads from bytes
        let payloads = if args_bytes.is_empty() {
            None
        } else {
            Some(Payloads::decode(&args_bytes[..])
                .map_err(|e| anyhow!("Failed to decode signal args: {}", e))?)
        };

        let request = SignalWorkflowExecutionRequest {
            namespace: self.namespace.clone(),
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

    /// Update a workflow execution
    pub async fn update_workflow(
        &self,
        request_bytes: Vec<u8>,
    ) -> Result<Vec<u8>> {
        let mut guard = self.client.lock().await;
        let client = guard
            .as_mut()
            .ok_or_else(|| anyhow!("Client not initialized"))?;

        use temporalio_common::protos::temporal::api::workflowservice::v1::UpdateWorkflowExecutionRequest;
        let request = UpdateWorkflowExecutionRequest::decode(&request_bytes[..])
            .map_err(|e| anyhow!("Failed to decode update workflow request: {}", e))?;

        let response = client
            .update_workflow_execution(tonic::Request::new(request))
            .await
            .map_err(|e| anyhow!("Failed to update workflow: {}", e))?;

        let bytes = response.into_inner().encode_to_vec();
        Ok(bytes)
    }

    /// Poll for a workflow execution update result
    pub async fn poll_workflow_execution_update(
        &self,
        request_bytes: Vec<u8>,
    ) -> Result<Vec<u8>> {
        let mut guard = self.client.lock().await;
        let client = guard
            .as_mut()
            .ok_or_else(|| anyhow!("Client not initialized"))?;

        use temporalio_common::protos::temporal::api::workflowservice::v1::PollWorkflowExecutionUpdateRequest;
        let request = PollWorkflowExecutionUpdateRequest::decode(&request_bytes[..])
            .map_err(|e| anyhow!("Failed to decode poll update request: {}", e))?;

        let response = client
            .poll_workflow_execution_update(tonic::Request::new(request))
            .await
            .map_err(|e| anyhow!("Failed to poll update: {}", e))?;

        let bytes = response.into_inner().encode_to_vec();
        Ok(bytes)
    }

    /// List workflow executions
    pub async fn list_workflow_executions(&self, request_bytes: Vec<u8>) -> Result<Vec<u8>> {
        let mut guard = self.client.lock().await;
        let client = guard
            .as_mut()
            .ok_or_else(|| anyhow!("Client not initialized"))?;

        use temporalio_common::protos::temporal::api::workflowservice::v1::ListWorkflowExecutionsRequest;
        let request = ListWorkflowExecutionsRequest::decode(&request_bytes[..])
            .map_err(|e| anyhow!("Failed to decode list workflow executions request: {}", e))?;

        let response = client
            .list_workflow_executions(tonic::Request::new(request))
            .await
            .map_err(|e| anyhow!("Failed to list workflow executions: {}", e))?;

        let bytes = response.into_inner().encode_to_vec();
        Ok(bytes)
    }

    /// Count workflow executions
    pub async fn count_workflow_executions(&self, request_bytes: Vec<u8>) -> Result<Vec<u8>> {
        let mut guard = self.client.lock().await;
        let client = guard
            .as_mut()
            .ok_or_else(|| anyhow!("Client not initialized"))?;

        use temporalio_common::protos::temporal::api::workflowservice::v1::CountWorkflowExecutionsRequest;
        let request = CountWorkflowExecutionsRequest::decode(&request_bytes[..])
            .map_err(|e| anyhow!("Failed to decode count workflow executions request: {}", e))?;

        let response = client
            .count_workflow_executions(tonic::Request::new(request))
            .await
            .map_err(|e| anyhow!("Failed to count workflow executions: {}", e))?;

        let bytes = response.into_inner().encode_to_vec();
        Ok(bytes)
    }

    /// Validate that the client is initialized and ready
    pub async fn validate(&self) -> Result<()> {
        let guard = self.client.lock().await;
        if guard.is_some() {
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
