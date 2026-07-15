//! `ConnectRPC` service process for the Henosis graph orchestrator.

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
use futures::future::BoxFuture;
use henosis_controller_cloudflare::CloudflareError;
use henosis_controller_cloudflare::CloudflareTransport;
use henosis_controller_cloudflare::RouteBody;
use henosis_controller_cloudflare::RouteObservation;
use henosis_controller_cloudflare::TunnelBody;
use henosis_controller_cloudflare::TunnelObservation;
use henosis_controller_cloudflare::WorkerBody;
use henosis_controller_cloudflare::WorkerObservation;
use henosis_controller_k8s::K8sController;
use henosis_controller_runtime::GitRepository;
use henosis_controller_runtime::controller_name;
use henosis_controller_runtime::output;
use henosis_controller_runtime::publication_id;
use henosis_controller_runtime::ready_report;
use henosis_evaluation_engine::BundleSource;
use henosis_evaluation_engine::EngineConfig;
use henosis_evaluation_engine::EvaluationEngine;
use henosis_evaluation_engine::ResourceContract;
use henosis_evaluation_engine::ResourceRegistry;
use henosis_evaluation_engine::inspect_bundle;
use henosis_orchestrator::Command;
use henosis_orchestrator::ControllerEffect;
use henosis_orchestrator::Core;
use henosis_proto::connect::henosis::v1::GraphService;
use henosis_proto::connect::henosis::v1::GraphServiceExt;
use henosis_proto::proto::henosis::v1 as proto;
use henosis_types::BundleRef;
use henosis_types::ComponentInputBinding;
use henosis_types::ComponentIntent;
use henosis_types::ContentDigest;
use henosis_types::Controller;
use henosis_types::ControllerCommand;
use henosis_types::ControllerError;
use henosis_types::ControllerName;
use henosis_types::ControllerReport;
use henosis_types::ControllerSlice;
use henosis_types::Generation;
use henosis_types::GraphId;
use henosis_types::GraphName;
use henosis_types::GraphSourcePolicy;
use henosis_types::InputName;
use henosis_types::KindVersion;
use henosis_types::NativeValue;
use henosis_types::NewGraphIntent;
use henosis_types::OutputAvailability;
use henosis_types::OutputName;
use henosis_types::OutputSource;
use henosis_types::Resource;
use henosis_types::ResourceDispositionKind;
use henosis_types::ResourceId;
use henosis_types::SourceProvenance;
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

    let bind = std::env::var("HENOSIS_BIND").unwrap_or_else(|_| "127.0.0.1:4481".into());
    let bundle_root = PathBuf::from(
        std::env::var("HENOSIS_BUNDLE_ROOT").unwrap_or_else(|_| ".henosis/bundles".into()),
    );
    let deploy_remote = PathBuf::from(
        std::env::var("HENOSIS_DEPLOY_REMOTE")
            .map_err(|_| anyhow::anyhow!("HENOSIS_DEPLOY_REMOTE is required"))?,
    );

    let source = Arc::new(FileBundleSource {
        root: bundle_root.clone(),
    });
    let config = EngineConfig::default();
    let evaluator = Arc::new(EvaluationEngine::new(
        source,
        Arc::new(DemoResourceRegistry),
        config.clone(),
    )?);
    let mut controllers: BTreeMap<ControllerName, Arc<dyn Controller>> = BTreeMap::new();
    let k8s: Arc<dyn Controller> = Arc::new(K8sController::new(GitRepository::new(deploy_remote)));
    controllers.insert(k8s.name().clone(), k8s);
    let cloudflare: Arc<dyn Controller> = Arc::new(
        henosis_controller_cloudflare::CloudflareController::new(RecordedCloudflareTransport),
    );
    controllers.insert(cloudflare.name().clone(), cloudflare);
    let supabase: Arc<dyn Controller> = Arc::new(DemoSupabaseController::new());
    controllers.insert(supabase.name().clone(), supabase);

    let service = Arc::new(CoreService {
        core: Arc::new(Mutex::new(Core::new(evaluator))),
        bundle_root,
        engine_config: config,
        controllers: Arc::new(controllers),
        watches: Arc::new(Mutex::new(BTreeMap::new())),
    });
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
    core: Arc<Mutex<Core>>,
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
            let path = self
                .bundle_root
                .join(hex(digest.as_bytes()))
                .join("module.js");
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
        let (graph_id, transition, status) = {
            let mut core = self.core.lock().await;
            let transition = core
                .handle(command)
                .await
                .map_err(|error| invalid(error.to_string()))?;
            let graph_id = transition
                .events()
                .iter()
                .find_map(event_graph_id)
                .ok_or_else(|| invalid("core transition omitted graph identity"))?;
            let status = graph_status(core.state(), graph_id)?;
            (graph_id, transition, status)
        };
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
            let graph_id = report.graph_id();
            let (transition, status) = {
                let mut core = self.core.lock().await;
                let transition = core.handle(Command::ReportController(report)).await?;
                let status = graph_status(core.state(), graph_id)
                    .map_err(|error| anyhow::anyhow!(error.to_string()))?;
                (transition, status)
            };
            self.publish(graph_id, status).await;
            queue.extend(transition.effects().iter().cloned());
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
        let core = self.core.lock().await;
        graph_status(core.state(), graph_id)
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
        let name = GraphName::new(request.name.unwrap_or_else(|| graph_id.to_string()))
            .map_err(|error| invalid(error.to_string()))?;
        let components = self.inspect_components(request.components).await?;
        let status = self
            .apply(Command::CreateGraph(NewGraphIntent {
                id: graph_id,
                name,
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

struct FileBundleSource {
    root: PathBuf,
}

impl BundleSource for FileBundleSource {
    fn load(
        &self,
        bundle: BundleRef,
    ) -> BoxFuture<'_, Result<Arc<[u8]>, henosis_types::EvaluationError>> {
        let path = self
            .root
            .join(bundle.digest().to_string())
            .join("module.js");
        Box::pin(async move {
            tokio::fs::read(&path)
                .await
                .map(Arc::<[u8]>::from)
                .map_err(|error| {
                    henosis_types::EvaluationError::new(format!(
                        "cannot load bundle {}: {error}",
                        path.display()
                    ))
                })
        })
    }
}

struct DemoResourceRegistry;

impl ResourceRegistry for DemoResourceRegistry {
    fn validate(
        &self,
        kind: &KindVersion,
        body: &serde_json::Value,
    ) -> Result<ResourceContract, String> {
        if !body.is_object() {
            return Err(format!("{kind} body must be an object"));
        }
        let (controller, outputs): (&str, &[&str]) = match kind.to_string().as_str() {
            "k8s/object@1" => ("k8s", &[]),
            "supabase/schema@1" => (
                "supabase",
                &[
                    "project",
                    "database",
                    "schema",
                    "apiUrl",
                    "restUrl",
                    "databaseUrlRef",
                    "anonKeyRef",
                ],
            ),
            "cloudflare/worker@1" => (
                "cloudflare",
                &["url", "workerName", "deploymentId", "versionId"],
            ),
            "cloudflare/tunnel@1" => (
                "cloudflare",
                &["tunnelId", "tunnelName", "privateHostname", "tokenRef"],
            ),
            "cloudflare/route@1" => ("cloudflare", &["hostname"]),
            other => return Err(format!("unsupported resource kind {other}")),
        };
        Ok(ResourceContract::new(
            controller_name(controller),
            outputs
                .iter()
                .map(|name| OutputName::new(*name).expect("built-in API output name"))
                .collect(),
        ))
    }
}

struct DemoSupabaseController {
    name: ControllerName,
}

impl DemoSupabaseController {
    fn new() -> Self {
        Self {
            name: controller_name("supabase"),
        }
    }

    fn reconcile(&self, slice: &ControllerSlice) -> Result<ControllerReport, ControllerError> {
        let mut outputs = Vec::new();
        for resource in slice.resources() {
            for (name, value) in [
                ("project", serde_json::json!("henosis-local")),
                ("database", serde_json::json!("postgres")),
                ("schema", serde_json::json!("catalog")),
                ("apiUrl", serde_json::json!("http://127.0.0.1:4484")),
                (
                    "restUrl",
                    serde_json::json!("http://127.0.0.1:4484/rest/v1"),
                ),
                (
                    "databaseUrlRef",
                    serde_json::json!("demo-fake://supabase/database"),
                ),
                (
                    "anonKeyRef",
                    serde_json::json!("demo-fake://supabase/anon-key"),
                ),
            ] {
                if resource
                    .outputs()
                    .any(|declaration| declaration.name().as_str() == name)
                {
                    outputs.push(
                        output(resource, name, value)
                            .map_err(|error| ControllerError::new(error.to_string()))?,
                    );
                }
            }
        }
        ready_report(slice, Some(publication_id(b"demo-supabase-fake")), outputs)
            .map_err(|error| ControllerError::new(error.to_string()))
    }
}

impl Controller for DemoSupabaseController {
    fn name(&self) -> &ControllerName {
        &self.name
    }

    fn execute<'a>(
        &'a self,
        command: &'a ControllerCommand,
    ) -> BoxFuture<'a, Result<Option<ControllerReport>, ControllerError>> {
        Box::pin(async move {
            match command {
                ControllerCommand::Reconcile(slice) => self.reconcile(slice).map(Some),
                ControllerCommand::Supersede(_) | ControllerCommand::Retire(_) => Ok(None),
            }
        })
    }
}

struct RecordedCloudflareTransport;

impl CloudflareTransport for RecordedCloudflareTransport {
    fn apply_worker(
        &self,
        _graph: GraphId,
        resource: &Resource,
        _body: &WorkerBody,
    ) -> Result<WorkerObservation, CloudflareError> {
        Ok(WorkerObservation {
            url: format!(
                "https://{}.workers.demo.invalid",
                resource.path().address().name()
            ),
            worker_name: resource.path().address().name().to_string(),
            deployment_id: format!("recorded-{}", resource.id()),
            version_id: "recorded-v1".into(),
        })
    }

    fn apply_tunnel(
        &self,
        _graph: GraphId,
        resource: &Resource,
        _body: &TunnelBody,
    ) -> Result<TunnelObservation, CloudflareError> {
        Ok(TunnelObservation {
            tunnel_id: format!("recorded-{}", resource.id()),
            tunnel_name: resource.path().address().name().to_string(),
            private_hostname: "supabase.internal.demo.invalid".into(),
            token_ref: "demo-fake://cloudflare/tunnel-token".into(),
        })
    }

    fn apply_route(
        &self,
        _graph: GraphId,
        _resource: &Resource,
        body: &RouteBody,
    ) -> Result<RouteObservation, CloudflareError> {
        Ok(RouteObservation {
            hostname: body.pattern.clone(),
        })
    }

    fn delete(&self, _graph: GraphId, _resource: ResourceId) -> Result<(), CloudflareError> {
        Ok(())
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

fn event_graph_id(event: &henosis_types::CoreEvent) -> Option<GraphId> {
    match event {
        henosis_types::CoreEvent::GraphCreated(intent)
        | henosis_types::CoreEvent::GraphUpdated(intent) => Some(intent.id()),
        henosis_types::CoreEvent::PlanAccepted { graph_id, .. }
        | henosis_types::CoreEvent::GraphRetired { graph_id, .. } => Some(*graph_id),
        henosis_types::CoreEvent::ControllerReported(report) => Some(report.graph_id()),
        henosis_types::CoreEvent::ComponentOutputsReplaced(outputs) => Some(outputs.graph_id()),
        henosis_types::CoreEvent::OutputsPublished(outputs) => Some(outputs.graph_id()),
        henosis_types::CoreEvent::StallDetected(stall) => Some(stall.graph_id()),
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
        name: Some(graph.intent().name().to_string()),
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
        source: component.source().map(source_wire).into(),
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
