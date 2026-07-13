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
use henosis_proto::protobuf;
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
    // === CreateComponent ===

    async fn create_component<'a>(
        &'a self,
        context: RequestContext,
        request: ServiceRequest<'_, protobuf::v1::CreateComponentRequest>,
    ) -> ServiceResult<
        impl connectrpc::Encodable<protobuf::v1::CreateComponentResponse> + Send + use<'a>,
    > {
        self.authorize(&context)?;
        let command = types::domain::NewComponent::try_from(&*request).map_err(conversion_error)?;
        let component = self
            .core
            .component_create(command)
            .await
            .map_err(|error| connect_error(error, ErrorSurface::Edit))?;
        Ok(Response::new(protobuf::v1::CreateComponentResponse {
            component: MessageField::some((&component).into()),
            ..Default::default()
        }))
    }

    // === CreateGraph ===

    async fn create_graph<'a>(
        &'a self,
        context: RequestContext,
        request: ServiceRequest<'_, protobuf::v1::CreateGraphRequest>,
    ) -> ServiceResult<impl connectrpc::Encodable<protobuf::v1::CreateGraphResponse> + Send + use<'a>>
    {
        self.authorize(&context)?;
        let command = types::domain::CreateGraph::try_from(&*request).map_err(conversion_error)?;
        let graph = self
            .core
            .graph_create(command)
            .await
            .map_err(|error| connect_error(error, ErrorSurface::Edit))?;
        Ok(Response::new(protobuf::v1::CreateGraphResponse {
            graph: MessageField::some((&graph).into()),
            ..Default::default()
        }))
    }

    // === AddComponents ===

    async fn add_components<'a>(
        &'a self,
        context: RequestContext,
        request: ServiceRequest<'_, protobuf::v1::AddComponentsRequest>,
    ) -> ServiceResult<
        impl connectrpc::Encodable<protobuf::v1::AddComponentsResponse> + Send + use<'a>,
    > {
        self.authorize(&context)?;
        let command =
            types::domain::AddComponents::try_from(&*request).map_err(conversion_error)?;
        let graph = self
            .core
            .graph_add_components(command)
            .await
            .map_err(|error| connect_error(error, ErrorSurface::Edit))?;
        Ok(Response::new(protobuf::v1::AddComponentsResponse {
            graph: MessageField::some((&graph).into()),
            ..Default::default()
        }))
    }

    // === UpdateComponents ===

    async fn update_components<'a>(
        &'a self,
        context: RequestContext,
        request: ServiceRequest<'_, protobuf::v1::UpdateComponentsRequest>,
    ) -> ServiceResult<
        impl connectrpc::Encodable<protobuf::v1::UpdateComponentsResponse> + Send + use<'a>,
    > {
        self.authorize(&context)?;
        let command =
            types::domain::UpdateComponents::try_from(&*request).map_err(conversion_error)?;
        let graph = self
            .core
            .graph_update_components(command)
            .await
            .map_err(|error| connect_error(error, ErrorSurface::Edit))?;
        Ok(Response::new(protobuf::v1::UpdateComponentsResponse {
            graph: MessageField::some((&graph).into()),
            ..Default::default()
        }))
    }

    // === RemoveComponents ===

    async fn remove_components<'a>(
        &'a self,
        context: RequestContext,
        request: ServiceRequest<'_, protobuf::v1::RemoveComponentsRequest>,
    ) -> ServiceResult<
        impl connectrpc::Encodable<protobuf::v1::RemoveComponentsResponse> + Send + use<'a>,
    > {
        self.authorize(&context)?;
        let command =
            types::domain::RemoveComponents::try_from(&*request).map_err(conversion_error)?;
        let graph = self
            .core
            .graph_remove_components(command)
            .await
            .map_err(|error| connect_error(error, ErrorSurface::Edit))?;
        Ok(Response::new(protobuf::v1::RemoveComponentsResponse {
            graph: MessageField::some((&graph).into()),
            ..Default::default()
        }))
    }

    // === GetGraph ===

    async fn get_graph<'a>(
        &'a self,
        context: RequestContext,
        request: ServiceRequest<'_, protobuf::v1::GetGraphRequest>,
    ) -> ServiceResult<impl connectrpc::Encodable<protobuf::v1::GetGraphResponse> + Send + use<'a>>
    {
        self.authorize(&context)?;
        let command = types::domain::GetGraph::try_from(&*request).map_err(conversion_error)?;
        let state = self
            .core
            .graph_get(command.graph_id())
            .await
            .map_err(|error| connect_error(error, ErrorSurface::Edit))?;
        Ok(Response::new(protobuf::v1::GetGraphResponse {
            state: MessageField::some((&state).into()),
            ..Default::default()
        }))
    }

    // === GetGraphGeneration ===

    async fn get_graph_generation<'a>(
        &'a self,
        context: RequestContext,
        request: ServiceRequest<'_, protobuf::v1::GetGraphGenerationRequest>,
    ) -> ServiceResult<
        impl connectrpc::Encodable<protobuf::v1::GetGraphGenerationResponse> + Send + use<'a>,
    > {
        self.authorize(&context)?;
        let command =
            types::domain::GetGraphGeneration::try_from(&*request).map_err(conversion_error)?;
        let generation = self
            .core
            .graph_generation_get(command)
            .await
            .map_err(|error| connect_error(error, ErrorSurface::Edit))?;
        let current_lifecycle = match generation.current_lifecycle() {
            types::domain::GraphLifecycle::Active => protobuf::v1::GraphLifecycle::Active,
            types::domain::GraphLifecycle::Retired => protobuf::v1::GraphLifecycle::Retired,
        };
        Ok(Response::new(protobuf::v1::GetGraphGenerationResponse {
            state: MessageField::some(generation.state().into()),
            components: generation.components().iter().map(Into::into).collect(),
            current_lifecycle: Some(current_lifecycle.into()),
            last_published_generation: generation.last_published_generation(),
            ..Default::default()
        }))
    }

    // === RetireGraph ===

    async fn retire_graph<'a>(
        &'a self,
        context: RequestContext,
        request: ServiceRequest<'_, protobuf::v1::RetireGraphRequest>,
    ) -> ServiceResult<impl connectrpc::Encodable<protobuf::v1::RetireGraphResponse> + Send + use<'a>>
    {
        self.authorize(&context)?;
        let command = types::domain::RetireGraph::try_from(&*request).map_err(conversion_error)?;
        let (graph_id, last_generation) = self
            .core
            .graph_retire(command)
            .await
            .map_err(|error| connect_error(error, ErrorSurface::Edit))?;
        Ok(Response::new(protobuf::v1::RetireGraphResponse {
            graph_id: Some(graph_id.into_bytes().to_vec()),
            last_generation: Some(last_generation),
            ..Default::default()
        }))
    }

    // === WatchGraph ===

    async fn watch_graph(
        &self,
        context: RequestContext,
        request: ServiceRequest<'_, protobuf::v1::WatchGraphRequest>,
    ) -> ServiceResult<
        ServiceStream<impl connectrpc::Encodable<protobuf::v1::WatchGraphResponse> + Send + use<>>,
    > {
        self.authorize(&context)?;
        let command = types::domain::WatchGraph::try_from(&*request).map_err(conversion_error)?;
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
    // === ReportSlice ===

    async fn report_slice<'a>(
        &'a self,
        context: RequestContext,
        request: ServiceRequest<'_, protobuf::v1::ReportSliceRequest>,
    ) -> ServiceResult<impl connectrpc::Encodable<protobuf::v1::ReportSliceResponse> + Send + use<'a>>
    {
        self.authorize(&context)?;
        let command = types::domain::ReportSlice::try_from(&*request).map_err(conversion_error)?;
        let publication_sequence = self
            .core
            .slice_report(command)
            .await
            .map_err(|error| connect_error(error, ErrorSurface::Report))?;
        Ok(Response::new(protobuf::v1::ReportSliceResponse {
            publication_sequence,
            ..Default::default()
        }))
    }

    // === FetchSlice ===

    async fn fetch_slice<'a>(
        &'a self,
        context: RequestContext,
        request: ServiceRequest<'_, protobuf::v1::FetchSliceRequest>,
    ) -> ServiceResult<impl connectrpc::Encodable<protobuf::v1::FetchSliceResponse> + Send + use<'a>>
    {
        self.authorize(&context)?;
        let command = types::domain::FetchSlice::try_from(&*request).map_err(conversion_error)?;
        let slice = self
            .core
            .slice_fetch(command)
            .await
            .map_err(|error| connect_error(error, ErrorSurface::Report))?;
        Ok(Response::new(protobuf::v1::FetchSliceResponse {
            slice: MessageField::some((&slice).into()),
            ..Default::default()
        }))
    }
}
