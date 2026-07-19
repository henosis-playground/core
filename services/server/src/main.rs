//! `ConnectRPC` service process for the Henosis graph orchestrator.

mod materialization;

use std::collections::BTreeMap;
use std::collections::VecDeque;
use std::path::PathBuf;
use std::str::FromStr as _;
use std::sync::Arc;
use std::time::Duration;

use async_stream::try_stream;
use connectrpc::ConnectError;
use connectrpc::ErrorCode;
use connectrpc::Router;
use connectrpc::ServiceRequest;
use connectrpc::ServiceResult;
use connectrpc::ServiceStream;
use faultline::Error as FaultlineError;
use henosis_app::verify_bundle_directory;
use henosis_evaluation_engine::EngineConfig;
use henosis_evaluation_engine::inspect_bundle;
use henosis_journal::Journal;
use henosis_journal::S2Storage;
use henosis_orchestrator::Command;
use henosis_orchestrator::CommandError;
use henosis_orchestrator::ControllerEffect;
use henosis_proto::connect::henosis::v1::GraphService;
use henosis_proto::connect::henosis::v1::GraphServiceExt;
use henosis_proto::proto::henosis::v1 as proto;
use henosis_types::BundleRef;
use henosis_types::ComponentInputBinding;
use henosis_types::ComponentInputSource;
use henosis_types::ComponentIntent;
use henosis_types::ContentDigest;
use henosis_types::Controller;
use henosis_types::ControllerName;
use henosis_types::Generation;
use henosis_types::GraphId;
use henosis_types::GraphSourcePolicy;
use henosis_types::InputName;
use henosis_types::NativeValue;
use henosis_types::NewGraphIntent;
use henosis_types::OutputAvailability;
use henosis_types::OutputSource;
use henosis_types::Resource;
use henosis_types::ResourceDispositionKind;
use henosis_types::SourceProvenance;
use materialization::MaterializedGraphs;
use tokio::sync::Mutex;
use tokio::sync::watch;
use tracing::error;
use tracing::info;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "henosis=info".into()),
        )
        .init();

    let assembly = henosis_server_assembly::from_environment()?;
    let bind = assembly.bind;
    let bundle_root = assembly.bundle_root;
    let config = assembly.engine_config;
    let evaluator = assembly.evaluator;
    let controllers = assembly.controllers;

    let s2 = S2Storage::connect(
        required_env("S2_ACCESS_TOKEN")?,
        &required_env("S2_ACCOUNT_ENDPOINT")?,
        &required_env("S2_BASIN_ENDPOINT")?,
        &required_env("S2_BASIN")?,
    )?;
    let (materialized, resume_effects) =
        MaterializedGraphs::boot(Arc::clone(&evaluator), Journal::new(Arc::new(s2))).await?;
    let service = Arc::new(CoreService {
        materialized,
        bundle_root,
        engine_config: config,
        controllers: Arc::new(controllers),
        watches: Arc::new(Mutex::new(BTreeMap::new())),
    });
    if !resume_effects.is_empty() {
        let service = Arc::clone(&service);
        tokio::spawn(async move {
            if let Err(error) = service.drive(resume_effects).await {
                error!(%error, "controller resume after journal replay failed");
            }
        });
    }
    let router = service.register(Router::new());
    info!(%bind, "Henosis core demo server listening");
    connectrpc::server::Server::new(router)
        .serve(bind.parse()?)
        .await
        .map_err(|error| anyhow::anyhow!(error.to_string()))?;
    Ok(())
}

#[derive(Clone)]
struct CoreService {
    materialized: MaterializedGraphs,
    bundle_root: PathBuf,
    engine_config: EngineConfig,
    controllers: Arc<BTreeMap<ControllerName, Arc<dyn Controller>>>,
    watches: Arc<Mutex<BTreeMap<GraphId, watch::Sender<proto::GraphStatus>>>>,
}

impl CoreService {
    async fn inspect_components(
        &self,
        components: Vec<proto::ComponentIntent>,
    ) -> Result<Vec<ComponentIntent>, ConnectError> {
        let mut inspected = Vec::with_capacity(components.len());
        for component in components {
            let name = component.name.clone().unwrap_or_default();
            let provenance = source_from_wire(component.source.into_option())?;
            let bindings = component
                .input_bindings
                .into_iter()
                .map(|binding| {
                    let binding_name = binding.name.unwrap_or_default();
                    let value =
                        serde_json::from_slice(binding.value_json.as_deref().unwrap_or_default())
                            .map_err(|error| {
                            invalid(format!(
                                "component {name:?} input {binding_name:?} has invalid JSON: \
                                 {error}"
                            ))
                        })?;
                    Ok(ComponentInputBinding::new(
                        InputName::new(binding_name).map_err(|error| invalid(error.to_string()))?,
                        NativeValue::new(value).map_err(|error| invalid(error.to_string()))?,
                    ))
                })
                .collect::<Result<Vec<_>, ConnectError>>()?;
            let digest = digest(component.bundle_digest.as_deref().unwrap_or_default())?;
            let bundle_id = hex(digest.as_bytes());
            let verified = verify_bundle_directory(&self.bundle_root.join(&bundle_id), &bundle_id)
                .map_err(|error| invalid(error.to_string()))?;
            let path = verified.module;
            let bundle_source = tokio::fs::read(&path).await.map_err(|error| {
                invalid(format!(
                    "cannot read bundle for {name:?} at {}: {error}",
                    path.display()
                ))
            })?;
            let intent =
                inspect_bundle(BundleRef::new(digest), &bundle_source, &self.engine_config)
                    .map_err(|error| invalid(error.to_string()))?;
            if intent.name().as_str() != name {
                return Err(invalid(format!(
                    "submitted component {name:?} contains bundle for {:?}",
                    intent.name().as_str()
                )));
            }
            inspected.push(
                intent
                    .with_source(provenance)
                    .with_input_bindings(bindings)
                    .map_err(|error| invalid(error.to_string()))?,
            );
        }
        Ok(inspected)
    }

    async fn apply(&self, command: Command) -> Result<proto::GraphStatus, ConnectError> {
        let applied = self
            .materialized
            .apply(command)
            .await
            .map_err(apply_error)?;
        let graph_id = applied.graph_id;
        let transition = applied.transition;
        let status = graph_status(&applied.state, graph_id)?;
        self.publish(graph_id, status.clone()).await;
        if !transition.effects().is_empty() {
            let service = self.clone();
            let effects = transition.effects().to_vec();
            tokio::spawn(async move {
                if let Err(error) = service.drive(effects).await {
                    error!(%error, "controller dispatch failed");
                }
            });
        }
        Ok(status)
    }

    async fn drive(&self, effects: Vec<ControllerEffect>) -> anyhow::Result<()> {
        let mut queue = VecDeque::from(effects);
        while let Some(effect) = queue.pop_front() {
            let controller = self
                .controllers
                .get(effect.controller())
                .ok_or_else(|| anyhow::anyhow!("no controller named {}", effect.controller()))?;
            if effect.controller().as_str() == "cloudflare" {
                tokio::time::sleep(Duration::from_millis(1_500)).await;
            }
            let Some(report) = controller.execute(effect.command()).await? else {
                continue;
            };
            let applied = self
                .materialized
                .apply(Command::ReportController(report))
                .await?;
            let graph_id = applied.graph_id;
            let transition = applied.transition;
            let status = graph_status(&applied.state, graph_id)
                .map_err(|error| anyhow::anyhow!(error.to_string()))?;
            self.publish(graph_id, status).await;
            if !transition.effects().is_empty() {
                queue = VecDeque::from(transition.effects().to_vec());
            }
        }
        Ok(())
    }

    async fn publish(&self, graph_id: GraphId, status: proto::GraphStatus) {
        let mut watches = self.watches.lock().await;
        if let Some(sender) = watches.get(&graph_id) {
            sender.send_replace(status);
        } else {
            let (sender, _) = watch::channel(status);
            watches.insert(graph_id, sender);
        }
    }

    async fn current(&self, graph_id: GraphId) -> Result<proto::GraphStatus, ConnectError> {
        let state = self
            .materialized
            .snapshot(graph_id)
            .await
            .ok_or_else(|| ConnectError::new(ErrorCode::NotFound, "graph does not exist"))?;
        graph_status(&state, graph_id)
    }
}

impl GraphService for CoreService {
    async fn create_graph<'a>(
        &'a self,
        _ctx: connectrpc::RequestContext,
        request: ServiceRequest<'_, proto::CreateGraphRequest>,
    ) -> ServiceResult<impl connectrpc::Encodable<proto::CreateGraphResponse> + Send + use<'a>>
    {
        let request = request.to_owned_message();
        let graph_id = parse_graph(request.graph_id.as_deref().unwrap_or_default())?;
        let components = self.inspect_components(request.components).await?;
        let status = self
            .apply(Command::CreateGraph(NewGraphIntent {
                id: graph_id,
                components,
                source_policy: source_policy(request.source_policy.as_ref())?,
            }))
            .await?;
        Ok(proto::CreateGraphResponse {
            status: status.into(),
            ..Default::default()
        }
        .into())
    }

    async fn update_graph<'a>(
        &'a self,
        _ctx: connectrpc::RequestContext,
        request: ServiceRequest<'_, proto::UpdateGraphRequest>,
    ) -> ServiceResult<impl connectrpc::Encodable<proto::UpdateGraphResponse> + Send + use<'a>>
    {
        let request = request.to_owned_message();
        let graph_id = parse_graph(request.graph_id.as_deref().unwrap_or_default())?;
        let expected = Generation::new(request.expected_generation.unwrap_or_default())
            .map_err(|error| invalid(error.to_string()))?;
        let components = self.inspect_components(request.components).await?;
        let status = self
            .apply(Command::UpdateGraph {
                graph_id,
                expected_generation: expected,
                components,
            })
            .await?;
        Ok(proto::UpdateGraphResponse {
            status: status.into(),
            ..Default::default()
        }
        .into())
    }

    async fn retire_graph<'a>(
        &'a self,
        _ctx: connectrpc::RequestContext,
        request: ServiceRequest<'_, proto::RetireGraphRequest>,
    ) -> ServiceResult<impl connectrpc::Encodable<proto::RetireGraphResponse> + Send + use<'a>>
    {
        let request = request.to_owned_message();
        let graph_id = parse_graph(request.graph_id.as_deref().unwrap_or_default())?;
        let expected = Generation::new(request.expected_generation.unwrap_or_default())
            .map_err(|error| invalid(error.to_string()))?;
        let status = self
            .apply(Command::RetireGraph {
                graph_id,
                expected_generation: expected,
            })
            .await?;
        Ok(proto::RetireGraphResponse {
            status: status.into(),
            ..Default::default()
        }
        .into())
    }

    async fn get_graph<'a>(
        &'a self,
        _ctx: connectrpc::RequestContext,
        request: ServiceRequest<'_, proto::GetGraphRequest>,
    ) -> ServiceResult<impl connectrpc::Encodable<proto::GetGraphResponse> + Send + use<'a>> {
        let request = request.to_owned_message();
        let status = self
            .current(parse_graph(
                request.graph_id.as_deref().unwrap_or_default(),
            )?)
            .await?;
        Ok(proto::GetGraphResponse {
            status: status.into(),
            ..Default::default()
        }
        .into())
    }

    async fn list_graphs<'a>(
        &'a self,
        _ctx: connectrpc::RequestContext,
        request: ServiceRequest<'_, proto::ListGraphsRequest>,
    ) -> ServiceResult<impl connectrpc::Encodable<proto::ListGraphsResponse> + Send + use<'a>> {
        let include_retired = request
            .to_owned_message()
            .include_retired
            .unwrap_or_default();
        let graphs = self
            .materialized
            .snapshots()
            .await
            .iter()
            .flat_map(|state| graph_summaries(state, include_retired))
            .collect();
        Ok(proto::ListGraphsResponse {
            graphs,
            ..Default::default()
        }
        .into())
    }

    async fn watch_graph(
        &self,
        _ctx: connectrpc::RequestContext,
        request: ServiceRequest<'_, proto::WatchGraphRequest>,
    ) -> ServiceResult<
        ServiceStream<impl connectrpc::Encodable<proto::WatchGraphResponse> + Send + use<>>,
    > {
        let request = request.to_owned_message();
        let graph_id = parse_graph(request.graph_id.as_deref().unwrap_or_default())?;
        let current = self.current(graph_id).await?;
        let mut receiver = {
            let mut watches = self.watches.lock().await;
            watches
                .entry(graph_id)
                .or_insert_with(|| watch::channel(current.clone()).0)
                .subscribe()
        };
        // Watch sequence numbers are connection-local cursors, not durable journal
        // offsets. A reconnect (including after a server crash) receives the current
        // replayed snapshot immediately, numbered after the caller's supplied cursor,
        // and then level-triggered replacements for changes observed on this process.
        let stream = try_stream! {
            let mut sequence = request.after_sequence.unwrap_or_default();
            loop {
                sequence += 1;
                let status = receiver.borrow_and_update().clone();
                let retired = status.retired.unwrap_or(false);
                yield proto::WatchGraphResponse {
                    sequence: Some(sequence),
                    status: status.into(),
                    heartbeat: Some(false),
                    ..Default::default()
                };
                if retired {
                    break;
                }
                receiver.changed().await.map_err(|_| {
                    ConnectError::new(ErrorCode::Unavailable, "graph watch closed")
                })?;
            }
        };
        let stream: ServiceStream<proto::WatchGraphResponse> = Box::pin(stream);
        Ok(stream.into())
    }
}

fn source_policy(
    value: Option<&buffa::EnumValue<proto::GraphSourcePolicy>>,
) -> Result<GraphSourcePolicy, ConnectError> {
    match value.and_then(buffa::EnumValue::as_known) {
        None
        | Some(proto::GraphSourcePolicy::Unspecified | proto::GraphSourcePolicy::AcceptLocal) => {
            Ok(GraphSourcePolicy::AcceptLocal)
        }
        Some(proto::GraphSourcePolicy::RequireVcs) => Ok(GraphSourcePolicy::RequireVcs),
    }
}

fn source_from_wire(
    source: Option<proto::SourceProvenance>,
) -> Result<Option<SourceProvenance>, ConnectError> {
    let Some(source) = source else {
        return Ok(None);
    };
    match source.source {
        Some(proto::__buffa::oneof::source_provenance::Source::Local(local)) => {
            Ok(Some(SourceProvenance::Local {
                repository: nonempty(local.repository),
                base_revision: nonempty(local.base_revision),
                dirty: local.dirty.unwrap_or(false),
            }))
        }
        Some(proto::__buffa::oneof::source_provenance::Source::Vcs(vcs)) => {
            let repository = vcs
                .repository
                .filter(|value| !value.is_empty())
                .ok_or_else(|| invalid("Vcs source provenance requires repository"))?;
            let revision = vcs
                .revision
                .filter(|value| !value.is_empty())
                .ok_or_else(|| invalid("Vcs source provenance requires revision"))?;
            Ok(Some(SourceProvenance::Vcs {
                repository,
                revision,
                reference: nonempty(vcs.reference),
            }))
        }
        None => Err(invalid("source provenance omitted its Local or Vcs value")),
    }
}

fn nonempty(value: Option<String>) -> Option<String> {
    value.filter(|value| !value.is_empty())
}

fn digest(bytes: &[u8]) -> Result<ContentDigest, ConnectError> {
    let bytes: [u8; 32] = bytes
        .try_into()
        .map_err(|_| invalid("bundle_digest must contain exactly 32 bytes"))?;
    Ok(ContentDigest::from_bytes(bytes))
}

fn parse_graph(value: &str) -> Result<GraphId, ConnectError> {
    GraphId::from_str(value).map_err(|error| invalid(format!("invalid graph id: {error}")))
}

fn invalid(message: impl Into<String>) -> ConnectError {
    ConnectError::new(ErrorCode::InvalidArgument, message)
}

fn apply_error(error: anyhow::Error) -> ConnectError {
    if let Some(FaultlineError::Domain(CommandError::InvalidIntent(diagnostic))) =
        error.downcast_ref::<FaultlineError<CommandError, faultline::Never, anyhow::Error>>()
    {
        invalid(diagnostic.clone())
    } else {
        invalid(error.to_string())
    }
}

fn required_env(name: &str) -> anyhow::Result<String> {
    std::env::var(name).map_err(|_| anyhow::anyhow!("{name} is required"))
}

fn graph_summaries(
    state: &henosis_orchestrator::MaterializedCore,
    include_retired: bool,
) -> Vec<proto::GraphSummary> {
    state
        .graphs()
        .filter(|graph| include_retired || !graph.is_retired())
        .map(|graph| proto::GraphSummary {
            graph_id: Some(graph.intent().id().to_string()),
            current_generation: Some(graph.intent().generation().ordinal()),
            phase: Some(graph_phase(graph).into()),
            created: Some(true),
            retired: Some(graph.is_retired()),
            ..Default::default()
        })
        .collect()
}

fn graph_phase(graph: &henosis_orchestrator::GraphState) -> proto::GraphPhase {
    if graph.is_retired() {
        return proto::GraphPhase::Retired;
    }
    if graph.stall().is_some() {
        return proto::GraphPhase::Failed;
    }
    let Some(plan) = graph.plan() else {
        return proto::GraphPhase::Planning;
    };
    if plan.blocked().next().is_some() {
        return proto::GraphPhase::Blocked;
    }
    let dispositions = graph
        .reports()
        .filter(|report| report.generation() == graph.intent().generation())
        .flat_map(|report| report.dispositions())
        .collect::<Vec<_>>();
    if dispositions
        .iter()
        .any(|disposition| matches!(disposition.kind(), ResourceDispositionKind::Failed { .. }))
    {
        return proto::GraphPhase::Failed;
    }
    let planned_resources = plan.resources().len();
    if planned_resources == 0
        || dispositions.len() >= planned_resources
            && dispositions
                .iter()
                .all(|disposition| disposition.kind() == &ResourceDispositionKind::Ready)
    {
        proto::GraphPhase::Ready
    } else {
        proto::GraphPhase::Reconciling
    }
}

fn graph_status(
    state: &henosis_orchestrator::MaterializedCore,
    graph_id: GraphId,
) -> Result<proto::GraphStatus, ConnectError> {
    let graph = state
        .graph(graph_id)
        .ok_or_else(|| ConnectError::new(ErrorCode::NotFound, "graph does not exist"))?;
    let plan = graph.plan().map(|plan| proto::Plan {
        generation: Some(plan.generation().ordinal()),
        digest: Some(plan.digest().as_bytes().to_vec()),
        resources: plan.resources().map(resource_wire).collect(),
        blocked: plan
            .blocked()
            .map(|blocked| proto::BlockedOn {
                component: Some(blocked.component().to_string()),
                inputs: blocked
                    .blocked_on()
                    .iter()
                    .map(ToString::to_string)
                    .collect(),
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    });
    let outputs = graph
        .outputs()
        .filter(|output| output.key_value().generation() == graph.intent().generation())
        .map(|output| proto::OutputRecord {
            generation: Some(output.key_value().generation().ordinal()),
            reference: Some(output.key_value().reference().to_string()),
            canonical_value_json: Some(output.value().canonical().as_bytes().to_vec()),
            source: Some(match output.source() {
                OutputSource::Static => "static".into(),
                OutputSource::Observed {
                    resource_id,
                    resource_output,
                } => format!("observed:{resource_id}.{resource_output}"),
            }),
            ..Default::default()
        })
        .collect();
    let dispositions = graph
        .reports()
        .filter(|report| report.generation() == graph.intent().generation())
        .flat_map(|report| report.dispositions())
        .map(disposition_wire)
        .collect();
    let diagnostic = graph.stall().map(|stall| {
        format!(
            "stall: {}",
            stall
                .cycle()
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join(" -> ")
        )
    });
    Ok(proto::GraphStatus {
        graph_id: Some(graph_id.to_string()),
        generation: Some(graph.intent().generation().ordinal()),
        plan: plan.into(),
        outputs,
        stall_cycle: graph
            .stall()
            .map(|stall| stall.cycle().iter().map(ToString::to_string).collect())
            .unwrap_or_default(),
        retired: Some(graph.is_retired()),
        components: graph.intent().components().map(component_wire).collect(),
        diagnostic,
        source_policy: Some(
            match graph.intent().source_policy() {
                GraphSourcePolicy::AcceptLocal => proto::GraphSourcePolicy::AcceptLocal,
                GraphSourcePolicy::RequireVcs => proto::GraphSourcePolicy::RequireVcs,
            }
            .into(),
        ),
        dispositions,
        ..Default::default()
    })
}

fn component_wire(component: &ComponentIntent) -> proto::ComponentIntent {
    proto::ComponentIntent {
        name: Some(component.name().to_string()),
        bundle_digest: Some(component.bundle().digest().as_bytes().to_vec()),
        inputs: component
            .inputs()
            .filter_map(|input| match input.source() {
                ComponentInputSource::Output { source, optional } => Some(proto::ComponentInput {
                    name: Some(input.name().to_string()),
                    source_component: Some(source.component().to_string()),
                    source_output: Some(source.output().to_string()),
                    optional: Some(*optional),
                    ..Default::default()
                }),
                ComponentInputSource::Config { .. } => None,
            })
            .collect(),
        outputs: component
            .outputs()
            .map(|output| proto::ComponentOutput {
                name: Some(output.name().to_string()),
                availability: Some(
                    match output.availability() {
                        OutputAvailability::Static => proto::OutputAvailability::Static,
                        OutputAvailability::Observed => proto::OutputAvailability::Observed,
                    }
                    .into(),
                ),
                optional: Some(output.is_optional()),
                ..Default::default()
            })
            .collect(),
        source: component.source().map(source_wire).into(),
        input_bindings: component
            .input_bindings()
            .map(|binding| proto::InputBinding {
                name: Some(binding.name().to_string()),
                value_json: Some(binding.value().canonical().as_bytes().to_vec()),
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    }
}

fn source_wire(source: &SourceProvenance) -> proto::SourceProvenance {
    use proto::__buffa::oneof::source_provenance::Source;

    let source = match source {
        SourceProvenance::Local {
            repository,
            base_revision,
            dirty,
        } => Source::Local(Box::new(proto::LocalSource {
            repository: repository.clone(),
            base_revision: base_revision.clone(),
            dirty: Some(*dirty),
            ..Default::default()
        })),
        SourceProvenance::Vcs {
            repository,
            revision,
            reference,
        } => Source::Vcs(Box::new(proto::VcsSource {
            repository: Some(repository.clone()),
            revision: Some(revision.clone()),
            reference: reference.clone(),
            ..Default::default()
        })),
    };
    proto::SourceProvenance {
        source: Some(source),
        ..Default::default()
    }
}

fn disposition_wire(
    disposition: &henosis_types::ResourceDisposition,
) -> proto::ResourceDisposition {
    let (state, message) = match disposition.kind() {
        ResourceDispositionKind::Ready => ("ready", None),
        ResourceDispositionKind::Reconciling { message } => ("reconciling", Some(message.clone())),
        ResourceDispositionKind::Failed { message } => ("failed", Some(message.clone())),
    };
    proto::ResourceDisposition {
        resource_id: Some(disposition.resource_id().to_string()),
        state: Some(state.to_owned()),
        message,
        ..Default::default()
    }
}

fn resource_wire(resource: &Resource) -> proto::Resource {
    proto::Resource {
        id: Some(resource.id().to_string()),
        path: Some(resource.path().to_string()),
        controller: Some(resource.controller().to_string()),
        kind: Some(resource.kind().to_string()),
        canonical_body_json: Some(resource.body().canonical().as_bytes().to_vec()),
        outputs: resource
            .outputs()
            .map(|output| proto::ResourceOutput {
                name: Some(output.name().to_string()),
                availability: Some(
                    match output.availability() {
                        OutputAvailability::Static => proto::OutputAvailability::Static,
                        OutputAvailability::Observed => proto::OutputAvailability::Observed,
                    }
                    .into(),
                ),
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    }
}

fn hex(bytes: &[u8]) -> String {
    const TABLE: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(TABLE[(byte >> 4) as usize] as char);
        encoded.push(TABLE[(byte & 0xf) as usize] as char);
    }
    encoded
}

#[cfg(test)]
mod tests {
    use super::*;
    use henosis_types::ComponentName;
    use henosis_types::CoreEvent;
    use henosis_types::NewComponentIntent;

    #[test]
    fn graph_intent_rejection_preserves_diagnostic_verbatim() {
        let diagnostic = "error[HENOSIS_CONTRACT_SKEW]: consumer -> producer.api";
        let error = anyhow::Error::new(FaultlineError::<
            CommandError,
            faultline::Never,
            anyhow::Error,
        >::Domain(CommandError::InvalidIntent(
            diagnostic.to_owned(),
        )));

        let rendered = apply_error(error);

        assert_eq!(rendered.message.as_deref(), Some(diagnostic));
    }

    #[test]
    fn list_graphs_filters_retired_graphs() {
        let live = graph(1);
        let retired = graph(2);
        let state = henosis_orchestrator::MaterializedCore::fold(&[
            CoreEvent::GraphCreated(live.clone()),
            CoreEvent::GraphCreated(retired.clone()),
            CoreEvent::GraphRetired {
                graph_id: retired.id(),
                last_generation: retired.generation(),
            },
        ]);

        let live_only = graph_summaries(&state, false);
        assert_eq!(live_only.len(), 1);
        assert_eq!(
            live_only[0].graph_id.as_deref(),
            Some(live.id().to_string().as_str())
        );
        assert_eq!(
            live_only[0]
                .phase
                .as_ref()
                .and_then(buffa::EnumValue::as_known),
            Some(proto::GraphPhase::Planning)
        );
        assert_eq!(live_only[0].created, Some(true));
        assert_eq!(live_only[0].retired, Some(false));

        let with_retired = graph_summaries(&state, true);
        assert_eq!(with_retired.len(), 2);
        let retired_summary = with_retired
            .iter()
            .find(|summary| summary.graph_id.as_deref() == Some(retired.id().to_string().as_str()))
            .expect("retired graph is included when requested");
        assert_eq!(
            retired_summary
                .phase
                .as_ref()
                .and_then(buffa::EnumValue::as_known),
            Some(proto::GraphPhase::Retired)
        );
        assert_eq!(retired_summary.created, Some(true));
        assert_eq!(retired_summary.retired, Some(true));
    }

    fn graph(last_byte: u8) -> henosis_types::GraphIntent {
        let component = ComponentIntent::new(NewComponentIntent {
            name: ComponentName::new("api").expect("test component name"),
            revision: henosis_types::ComponentRevision::new(format!("{last_byte:02x}").repeat(32))
                .expect("test component revision"),
            bundle: BundleRef::new(ContentDigest::from_bytes([last_byte; 32])),
            inputs: Vec::new(),
            outputs: Vec::new(),
            compiled_dependencies: Vec::new(),
            source: None,
        })
        .expect("test component intent");
        henosis_types::GraphIntent::new(NewGraphIntent {
            id: GraphId::from_bytes([last_byte; 16]),
            components: vec![component],
            source_policy: GraphSourcePolicy::AcceptLocal,
        })
        .expect("test graph intent")
    }
}
