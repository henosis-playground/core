use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use connectrpc::client::ClientConfig;
use connectrpc::client::HttpClient;
use henosis_db_queries::ConnectorCheckpointStore;
use henosis_proto::connect::henosis::v1::ConnectorServiceClient;
use henosis_proto::proto::henosis::v1 as pb;
use henosis_proto::reconcile_slice_request;
use henosis_proto::retire_slice_request;
use henosis_types::ConnectorKey;
use henosis_types::GraphHistory;
use henosis_types::GraphUuid;
use henosis_types::NewConnectorCheckpoint;
use scoped_futures::ScopedFutureExt;
use tokio::time::sleep;
use tracing::Span;
use tracing::field::Empty;
use tracing::instrument;

use crate::ConnectorConfig;
use crate::Orchestrator;
use crate::slice::compute_slice;
use crate::slice::connectors_for_sequence;
use crate::slice::superseded_components;
use crate::telemetry;

impl Orchestrator {
    pub(crate) async fn schedule_delivery(self: &Arc<Self>, graph_id: GraphUuid) {
        let orchestrator = Arc::clone(self);
        tokio::spawn(async move {
            let _ = orchestrator.deliver_graph(graph_id).await;
        });
    }

    #[instrument(
        name = "deliver graph",
        skip_all,
        fields(
            { telemetry::GRAPH_ID } = %graph_id,
            { telemetry::DELIVERY_OUTCOME } = Empty,
            { telemetry::ERROR_TYPE } = Empty,
        )
    )]
    async fn deliver_graph(self: &Arc<Self>, graph_id: GraphUuid) -> anyhow::Result<()> {
        let result = self.deliver_graph_inner(graph_id).await;
        let span = Span::current();
        match &result {
            Ok(()) => {
                span.record(telemetry::DELIVERY_OUTCOME, "completed");
            }
            Err(error) => {
                span.record(telemetry::DELIVERY_OUTCOME, "failed");
                span.record(telemetry::ERROR_TYPE, error_type(error));
            }
        }
        result
    }

    async fn deliver_graph_inner(self: &Arc<Self>, graph_id: GraphUuid) -> anyhow::Result<()> {
        let runtime = self.runtime(graph_id).await;
        let _delivery = runtime.delivery.lock().await;
        let history = {
            let mut cached = runtime.history.lock().await;
            self.ensure_loaded(graph_id, &mut cached)
                .await
                .map_err(anyhow::Error::new)?;
            cached.as_ref().expect("history was loaded").clone()
        };
        let specs = self.specs.read().await.clone();
        if history.is_retired() {
            return self.deliver_retirement(&history, &specs).await;
        }
        for state in history.states() {
            for connector in connectors_for_sequence(&history, &specs, state.sequence())? {
                let checkpoint = self
                    .metadata
                    .transaction(|connection| {
                        async {
                            connection
                                .connector_checkpoint_get(graph_id, &connector)
                                .await
                        }
                        .scope_boxed()
                    })
                    .await
                    .map_err(anyhow::Error::new)?;
                if checkpoint
                    .as_ref()
                    .is_some_and(|value| value.accepted_sequence() >= state.sequence())
                {
                    continue;
                }
                self.deliver_sequence(graph_id, &history, &specs, state.sequence(), &connector)
                    .await?;
            }
        }
        Ok(())
    }

    #[instrument(
        name = "deliver connector slice",
        skip_all,
        fields(
            { telemetry::GRAPH_ID } = %graph_id,
            { telemetry::CONNECTOR_NAME } = %connector,
            { telemetry::GRAPH_SEQUENCE } = sequence.to_string(),
            { telemetry::DELIVERY_RETRY_COUNT } = Empty,
            { telemetry::DELIVERY_OUTCOME } = Empty,
            { telemetry::ERROR_TYPE } = Empty,
        )
    )]
    async fn deliver_sequence(
        &self,
        graph_id: GraphUuid,
        history: &GraphHistory,
        specs: &henosis_types::SpecCatalog,
        sequence: u64,
        connector: &ConnectorKey,
    ) -> anyhow::Result<()> {
        let config = self
            .connectors
            .get(connector)
            .context("slice connector is not configured")?;
        let slice = compute_slice(history, specs, sequence, connector)?;
        let superseded = superseded_components(history, specs, sequence, connector)?;
        let request = reconcile_slice_request(&slice, &superseded);
        let mut delay = Duration::from_millis(250);
        let mut retries = 0_u64;
        loop {
            match reconcile(config, request.clone()).await {
                Ok(accepted) if accepted >= sequence => {
                    self.metadata
                        .transaction(|connection| {
                            async move {
                                connection
                                    .connector_checkpoint_upsert(NewConnectorCheckpoint {
                                        graph_id,
                                        connector: connector.clone(),
                                        accepted_sequence: accepted,
                                    })
                                    .await
                            }
                            .scope_boxed()
                        })
                        .await
                        .map_err(anyhow::Error::new)?;
                    let span = Span::current();
                    span.record(telemetry::DELIVERY_RETRY_COUNT, retries);
                    span.record(telemetry::DELIVERY_OUTCOME, "acknowledged");
                    return Ok(());
                }
                Ok(_) => {
                    Span::current().record(telemetry::ERROR_TYPE, "stale_acknowledgement");
                }
                Err(_) => {
                    Span::current().record(telemetry::ERROR_TYPE, "connector_rpc");
                }
            }
            retries = retries.saturating_add(1);
            Span::current().record(telemetry::DELIVERY_RETRY_COUNT, retries);
            sleep(delay).await;
            delay = (delay * 2).min(Duration::from_secs(10));
        }
    }

    async fn deliver_retirement(
        &self,
        history: &GraphHistory,
        specs: &henosis_types::SpecCatalog,
    ) -> anyhow::Result<()> {
        let mut connectors = std::collections::BTreeSet::new();
        for graph in history.generations() {
            for component in graph.components() {
                connectors.insert(
                    specs
                        .get(component.spec_hash())
                        .context("retired graph references an unknown spec")?
                        .spec()
                        .connector()
                        .clone(),
                );
            }
        }
        let sequence = history
            .head_sequence()
            .context("retired graph has no durable records")?;
        for connector in connectors {
            let slice = compute_slice(history, specs, sequence, &connector)?;
            self.deliver_connector_retirement(&connector, &slice)
                .await?;
        }
        Ok(())
    }

    #[instrument(
        name = "retire connector slice",
        skip_all,
        fields(
            { telemetry::GRAPH_ID } = %slice.graph_id(),
            { telemetry::CONNECTOR_NAME } = %connector,
            { telemetry::DELIVERY_RETRY_COUNT } = Empty,
            { telemetry::DELIVERY_OUTCOME } = Empty,
            { telemetry::ERROR_TYPE } = Empty,
        )
    )]
    async fn deliver_connector_retirement(
        &self,
        connector: &ConnectorKey,
        slice: &henosis_types::GraphSlice,
    ) -> anyhow::Result<()> {
        let config = self
            .connectors
            .get(connector)
            .context("retirement connector is not configured")?;
        let request = retire_slice_request(slice);
        let mut delay = Duration::from_millis(250);
        let mut retries = 0_u64;
        loop {
            match retire(config, request.clone()).await {
                Ok(retired) if retired >= slice.generation() => {
                    let span = Span::current();
                    span.record(telemetry::DELIVERY_RETRY_COUNT, retries);
                    span.record(telemetry::DELIVERY_OUTCOME, "acknowledged");
                    return Ok(());
                }
                Ok(_) => {
                    Span::current().record(telemetry::ERROR_TYPE, "stale_acknowledgement");
                }
                Err(_) => {
                    Span::current().record(telemetry::ERROR_TYPE, "connector_rpc");
                }
            }
            retries = retries.saturating_add(1);
            Span::current().record(telemetry::DELIVERY_RETRY_COUNT, retries);
            sleep(delay).await;
            delay = (delay * 2).min(Duration::from_secs(10));
        }
    }
}

async fn reconcile(
    config: &ConnectorConfig,
    request: pb::ReconcileSliceRequest,
) -> anyhow::Result<u64> {
    let response = connector_client(config)?
        .reconcile_slice(request)
        .await?
        .into_view();
    response
        .reborrow()
        .accepted_sequence
        .context("connector omitted accepted_sequence")
}

async fn retire(config: &ConnectorConfig, request: pb::RetireSliceRequest) -> anyhow::Result<u64> {
    let response = connector_client(config)?
        .retire_slice(request)
        .await?
        .into_view();
    Ok(response.reborrow().retired_generation.unwrap_or(0))
}

fn connector_client(
    config: &ConnectorConfig,
) -> anyhow::Result<ConnectorServiceClient<HttpClient>> {
    let uri = config.endpoint().parse::<http::Uri>()?;
    let client_config = ClientConfig::new(uri)
        .with_default_timeout(Duration::from_secs(30))
        .with_default_header("authorization", format!("Bearer {}", config.token()));
    Ok(ConnectorServiceClient::new(
        HttpClient::plaintext(),
        client_config,
    ))
}

fn error_type(error: &anyhow::Error) -> &'static str {
    if error.downcast_ref::<crate::slice::SliceError>().is_some() {
        "slice"
    } else {
        "delivery"
    }
}
