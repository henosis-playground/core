use std::sync::Arc;
use std::time::Duration;

use buffa::MessageField;
use connectrpc::ConnectError;
use connectrpc::RequestContext;
use connectrpc::Response;
use connectrpc::ServiceRequest;
use connectrpc::ServiceResult;
use connectrpc::ServiceStream;
use henosis_orchestrator::Orchestrator;
use henosis_orchestrator::WatchEvent;
use henosis_proto::connect::henosis::v1::ConnectorCallbackService;
use henosis_proto::connect::henosis::v1::GraphService;
use henosis_proto::proto::henosis::v1 as pb;
use tokio::sync::broadcast;
use tokio::time::interval;

use crate::error::ErrorSurface;
use crate::error::connect_error;
use crate::error::conversion_error;
use crate::watch_response::change;
use crate::watch_response::is_retired;
use crate::watch_response::progress;
use crate::watch_response::snapshot;
use crate::watch_response::volatile_status;

#[derive(Clone, Debug)]
pub(crate) struct Api {
    core: Arc<Orchestrator>,
    bearer_tokens: Arc<Vec<String>>,
}

impl Api {
    pub(crate) fn new(core: Arc<Orchestrator>, bearer_tokens: Vec<String>) -> Self {
        Self {
            core,
            bearer_tokens: Arc::new(bearer_tokens),
        }
    }

    fn authorize(&self, context: &RequestContext) -> Result<(), ConnectError> {
        let supplied = context
            .header("authorization")
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.strip_prefix("Bearer "));
        if supplied.is_some_and(|token| self.bearer_tokens.iter().any(|valid| valid == token)) {
            Ok(())
        } else {
            Err(ConnectError::unauthenticated("valid bearer token required"))
        }
    }
}

impl GraphService for Api {
    async fn register_component_spec<'a>(
        &'a self,
        context: RequestContext,
        request: ServiceRequest<'_, pb::RegisterComponentSpecRequest>,
    ) -> ServiceResult<impl connectrpc::Encodable<pb::RegisterComponentSpecResponse> + Send + use<'a>>
    {
        self.authorize(&context)?;
        let command =
            henosis_types::RegisterComponentSpec::try_from(&*request).map_err(conversion_error)?;
        let component = self
            .core
            .component_spec_register(command)
            .await
            .map_err(|error| connect_error(error, ErrorSurface::Edit))?;
        Ok(Response::new(pb::RegisterComponentSpecResponse {
            component: MessageField::some((&component).into()),
            ..Default::default()
        }))
    }

    async fn create_graph<'a>(
        &'a self,
        context: RequestContext,
        request: ServiceRequest<'_, pb::CreateGraphRequest>,
    ) -> ServiceResult<impl connectrpc::Encodable<pb::CreateGraphResponse> + Send + use<'a>> {
        self.authorize(&context)?;
        let command = henosis_types::CreateGraph::try_from(&*request).map_err(conversion_error)?;
        let graph = self
            .core
            .graph_create(command)
            .await
            .map_err(|error| connect_error(error, ErrorSurface::Edit))?;
        Ok(Response::new(pb::CreateGraphResponse {
            graph: MessageField::some((&graph).into()),
            ..Default::default()
        }))
    }

    async fn add_components<'a>(
        &'a self,
        context: RequestContext,
        request: ServiceRequest<'_, pb::AddComponentsRequest>,
    ) -> ServiceResult<impl connectrpc::Encodable<pb::AddComponentsResponse> + Send + use<'a>> {
        self.authorize(&context)?;
        let command =
            henosis_types::AddComponents::try_from(&*request).map_err(conversion_error)?;
        let graph = self
            .core
            .graph_add_components(command)
            .await
            .map_err(|error| connect_error(error, ErrorSurface::Edit))?;
        Ok(Response::new(pb::AddComponentsResponse {
            graph: MessageField::some((&graph).into()),
            ..Default::default()
        }))
    }

    async fn update_components<'a>(
        &'a self,
        context: RequestContext,
        request: ServiceRequest<'_, pb::UpdateComponentsRequest>,
    ) -> ServiceResult<impl connectrpc::Encodable<pb::UpdateComponentsResponse> + Send + use<'a>>
    {
        self.authorize(&context)?;
        let command =
            henosis_types::UpdateComponents::try_from(&*request).map_err(conversion_error)?;
        let graph = self
            .core
            .graph_update_components(command)
            .await
            .map_err(|error| connect_error(error, ErrorSurface::Edit))?;
        Ok(Response::new(pb::UpdateComponentsResponse {
            graph: MessageField::some((&graph).into()),
            ..Default::default()
        }))
    }

    async fn remove_components<'a>(
        &'a self,
        context: RequestContext,
        request: ServiceRequest<'_, pb::RemoveComponentsRequest>,
    ) -> ServiceResult<impl connectrpc::Encodable<pb::RemoveComponentsResponse> + Send + use<'a>>
    {
        self.authorize(&context)?;
        let command =
            henosis_types::RemoveComponents::try_from(&*request).map_err(conversion_error)?;
        let graph = self
            .core
            .graph_remove_components(command)
            .await
            .map_err(|error| connect_error(error, ErrorSurface::Edit))?;
        Ok(Response::new(pb::RemoveComponentsResponse {
            graph: MessageField::some((&graph).into()),
            ..Default::default()
        }))
    }

    async fn get_graph<'a>(
        &'a self,
        context: RequestContext,
        request: ServiceRequest<'_, pb::GetGraphRequest>,
    ) -> ServiceResult<impl connectrpc::Encodable<pb::GetGraphResponse> + Send + use<'a>> {
        self.authorize(&context)?;
        let command = henosis_types::GetGraph::try_from(&*request).map_err(conversion_error)?;
        let state = self
            .core
            .graph_get(command.graph_id())
            .await
            .map_err(|error| connect_error(error, ErrorSurface::Edit))?;
        Ok(Response::new(pb::GetGraphResponse {
            state: MessageField::some((&state).into()),
            ..Default::default()
        }))
    }

    async fn get_graph_generation<'a>(
        &'a self,
        context: RequestContext,
        request: ServiceRequest<'_, pb::GetGraphGenerationRequest>,
    ) -> ServiceResult<impl connectrpc::Encodable<pb::GetGraphGenerationResponse> + Send + use<'a>>
    {
        self.authorize(&context)?;
        let command =
            henosis_types::GetGraphGeneration::try_from(&*request).map_err(conversion_error)?;
        let generation = self
            .core
            .graph_generation_get(command)
            .await
            .map_err(|error| connect_error(error, ErrorSurface::Edit))?;
        let current_lifecycle = match generation.current_lifecycle() {
            henosis_types::GraphLifecycle::Active => pb::GraphLifecycle::Active,
            henosis_types::GraphLifecycle::Retired => pb::GraphLifecycle::Retired,
        };
        Ok(Response::new(pb::GetGraphGenerationResponse {
            state: MessageField::some(generation.state().into()),
            components: generation.components().iter().map(Into::into).collect(),
            current_lifecycle: Some(current_lifecycle.into()),
            last_published_generation: generation.last_published_generation(),
            ..Default::default()
        }))
    }

    async fn retire_graph<'a>(
        &'a self,
        context: RequestContext,
        request: ServiceRequest<'_, pb::RetireGraphRequest>,
    ) -> ServiceResult<impl connectrpc::Encodable<pb::RetireGraphResponse> + Send + use<'a>> {
        self.authorize(&context)?;
        let command = henosis_types::RetireGraph::try_from(&*request).map_err(conversion_error)?;
        let (graph_id, last_generation) = self
            .core
            .graph_retire(command)
            .await
            .map_err(|error| connect_error(error, ErrorSurface::Edit))?;
        Ok(Response::new(pb::RetireGraphResponse {
            graph_id: Some(graph_id.to_bytes().to_vec()),
            last_generation: Some(last_generation),
            ..Default::default()
        }))
    }

    async fn watch_graph(
        &self,
        context: RequestContext,
        request: ServiceRequest<'_, pb::WatchGraphRequest>,
    ) -> ServiceResult<
        ServiceStream<impl connectrpc::Encodable<pb::WatchGraphResponse> + Send + use<>>,
    > {
        self.authorize(&context)?;
        let command = henosis_types::WatchGraph::try_from(&*request).map_err(conversion_error)?;
        let watch = self
            .core
            .graph_watch(command)
            .await
            .map_err(|error| connect_error(error, ErrorSurface::Watch))?;
        let parts = watch.into_parts();
        let snapshot_sequence = parts.snapshot_sequence;
        let snapshot_state = parts.snapshot;
        let backlog = parts.backlog;
        let reports = parts.reports;
        let mut receiver = parts.receiver;
        let stream = async_stream::stream! {
            let mut delivered = snapshot_sequence;
            let snapshot_retired = is_retired(&snapshot_state);
            yield Ok(snapshot(delivered, &snapshot_state));
            for (sequence, state) in backlog {
                delivered = sequence;
                let retired = is_retired(&state);
                yield Ok(change(sequence, &state));
                if retired {
                    return;
                }
            }
            if snapshot_retired {
                return;
            }
            yield Ok(volatile_status(delivered, &reports));
            let mut heartbeat = interval(Duration::from_secs(25));
            heartbeat.tick().await;
            loop {
                tokio::select! {
                    _ = heartbeat.tick() => yield Ok(progress(delivered)),
                    event = receiver.recv() => match event {
                        Ok(WatchEvent::Durable { sequence, state }) if sequence > delivered => {
                            delivered = sequence;
                            let retired = is_retired(&state);
                            yield Ok(change(sequence, &state));
                            if retired {
                                return;
                            }
                        }
                        Ok(WatchEvent::Durable { .. }) => {}
                        Ok(WatchEvent::Volatile { reports }) => {
                            yield Ok(volatile_status(delivered, &reports));
                        }
                        Err(broadcast::error::RecvError::Lagged(_)) => {
                            yield Err(ConnectError::internal("watch consumer fell behind"));
                            return;
                        }
                        Err(broadcast::error::RecvError::Closed) => return,
                    }
                }
            }
        };
        Response::stream_ok(stream)
    }
}

impl ConnectorCallbackService for Api {
    async fn report_slice<'a>(
        &'a self,
        context: RequestContext,
        request: ServiceRequest<'_, pb::ReportSliceRequest>,
    ) -> ServiceResult<impl connectrpc::Encodable<pb::ReportSliceResponse> + Send + use<'a>> {
        self.authorize(&context)?;
        let command = henosis_types::ReportSlice::try_from(&*request).map_err(conversion_error)?;
        let publication_sequence = self
            .core
            .slice_report(command)
            .await
            .map_err(|error| connect_error(error, ErrorSurface::Report))?;
        Ok(Response::new(pb::ReportSliceResponse {
            publication_sequence,
            ..Default::default()
        }))
    }

    async fn fetch_slice<'a>(
        &'a self,
        context: RequestContext,
        request: ServiceRequest<'_, pb::FetchSliceRequest>,
    ) -> ServiceResult<impl connectrpc::Encodable<pb::FetchSliceResponse> + Send + use<'a>> {
        self.authorize(&context)?;
        let command = henosis_types::FetchSlice::try_from(&*request).map_err(conversion_error)?;
        let slice = self
            .core
            .slice_fetch(command)
            .await
            .map_err(|error| connect_error(error, ErrorSurface::Report))?;
        Ok(Response::new(pb::FetchSliceResponse {
            slice: MessageField::some((&slice).into()),
            ..Default::default()
        }))
    }
}
