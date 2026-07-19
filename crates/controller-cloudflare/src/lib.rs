//! Cloudflare resource controller with a replaceable API transport.
//!
//! Workers carry graph/resource/digest tags in Cloudflare's script metadata.
//! Tunnels use an opaque deterministic target identity derived from the graph
//! and resource `TypeID`s because Cloudflare exposes tunnel metadata read-only.
//! Routes are scoped by their exact zone/pattern and may only reference a
//! Worker whose ownership tags match the graph.

use std::collections::BTreeMap;

use futures::FutureExt as _;
use futures::future::BoxFuture;
use henosis_controller_runtime::PerResourceReconciler;
use henosis_controller_runtime::ReconcileDecision;
use henosis_controller_runtime::ResourceConvergence;
use henosis_controller_runtime::ResourceGoal;
use henosis_controller_runtime::SlicePass;
use henosis_controller_runtime::controller_name;
use henosis_controller_runtime::failed_report;
use henosis_controller_runtime::output;
use henosis_controller_runtime::publication_id;
use henosis_controller_runtime::ready_report;
use henosis_controller_runtime::reconcile_absent;
use henosis_controller_runtime::reconcile_slice;
use henosis_types::ArtifactDigest;
use henosis_types::ContentDigest;
use henosis_types::Controller;
use henosis_types::ControllerCommand;
use henosis_types::ControllerError;
use henosis_types::ControllerName;
use henosis_types::ControllerPass;
use henosis_types::ControllerSlice;
use henosis_types::GraphId;
use henosis_types::ObservedOutput;
use henosis_types::Resource;
use serde::Deserialize;
use serde::Serialize;
use thiserror::Error;

mod live;

pub use live::LiveCloudflareConfig;
pub use live::LiveCloudflareTransport;

const CONTROLLER_NAME: &str = "cloudflare";

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkerBody {
    pub source: SourceRef,
    pub compatibility_date: Option<String>,
    #[serde(default)]
    pub compatibility_flags: Vec<String>,
    #[serde(default)]
    pub vars: BTreeMap<String, serde_json::Value>,
    #[serde(default)]
    pub services: BTreeMap<String, String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SourceRef {
    pub entry: ArtifactReference,
    pub assets: Option<ArtifactReference>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ArtifactReference {
    pub kind: ArtifactKind,
    pub digest: ArtifactDigest,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum ArtifactKind {
    CloudflareWorker,
    StaticAssets,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct TunnelBody {
    pub origin: TunnelOrigin,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct TunnelOrigin {
    pub host: String,
    pub port: u16,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RouteBody {
    pub pattern: String,
    pub zone: String,
    pub worker_name: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkerObservation {
    pub url: String,
    pub worker_name: String,
    pub deployment_id: String,
    pub version_id: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TunnelObservation {
    pub tunnel_id: String,
    pub tunnel_name: String,
    pub private_hostname: String,
    pub token_ref: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RouteObservation {
    pub hostname: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CloudflareObservation {
    Missing,
    Foreign,
    Worker {
        digest: ContentDigest,
        subdomain_enabled: bool,
        observation: WorkerObservation,
    },
    Tunnel {
        configured: bool,
        observation: TunnelObservation,
    },
    Route {
        matches: bool,
        observation: RouteObservation,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CloudflareAction {
    UploadWorker(WorkerBody),
    EnableWorkerSubdomain,
    CreateTunnel,
    ConfigureTunnel(TunnelBody),
    WriteRoute(RouteBody),
    Delete,
}

pub trait CloudflareTransport: Send + Sync {
    fn observe<'a>(
        &'a self,
        graph: GraphId,
        resource: &'a Resource,
    ) -> BoxFuture<'a, Result<CloudflareObservation, CloudflareError>>;
    fn act<'a>(
        &'a self,
        graph: GraphId,
        resource: &'a Resource,
        action: CloudflareAction,
    ) -> BoxFuture<'a, Result<(), CloudflareError>>;
}

pub struct CloudflareController<T> {
    name: ControllerName,
    transport: T,
}

impl<T> CloudflareController<T>
where
    T: CloudflareTransport,
{
    #[must_use]
    pub fn new(transport: T) -> Self {
        Self {
            name: controller_name(CONTROLLER_NAME),
            transport,
        }
    }

    async fn reconcile(&self, slice: &ControllerSlice) -> Result<ControllerPass, ControllerError> {
        match reconcile_slice(self, slice).await {
            Ok(SlicePass::Acted) => Ok(ControllerPass::Acted),
            Ok(SlicePass::Converged(convergence)) => ready_report(
                slice,
                Some(publication_id(&convergence.evidence)),
                convergence.outputs,
            )
            .map(|report| ControllerPass::Converged(Some(report)))
            .map_err(|error| ControllerError::new(error.to_string())),
            Err(CloudflareError::Unavailable(message)) => Ok(ControllerPass::Retryable(message)),
            Err(error) => failed_report(slice, error.to_string())
                .map(ControllerPass::Failed)
                .map_err(|report_error| ControllerError::new(report_error.to_string())),
        }
    }
}

impl<T> PerResourceReconciler for CloudflareController<T>
where
    T: CloudflareTransport,
{
    type Action = CloudflareAction;
    type Error = CloudflareError;
    type Observation = CloudflareObservation;

    fn rank(&self, resource: &Resource, goal: ResourceGoal) -> u8 {
        cloudflare_rank(resource, goal)
    }

    fn observe<'a>(
        &'a self,
        graph_id: GraphId,
        resource: &'a Resource,
    ) -> BoxFuture<'a, Result<Self::Observation, Self::Error>> {
        self.transport.observe(graph_id, resource)
    }

    fn diff(
        &self,
        _graph_id: GraphId,
        resource: &Resource,
        goal: ResourceGoal,
        observed: &Self::Observation,
    ) -> Result<ReconcileDecision<Self::Action>, Self::Error> {
        if observed == &CloudflareObservation::Foreign {
            return Err(CloudflareError::Provider(format!(
                "refusing to mutate {} because target ownership metadata/identity does not match",
                resource.path()
            )));
        }
        if goal == ResourceGoal::Absent {
            return if observed == &CloudflareObservation::Missing {
                Ok(ReconcileDecision::Converged(ResourceConvergence::default()))
            } else {
                Ok(ReconcileDecision::Act(CloudflareAction::Delete))
            };
        }
        match (
            resource.kind().name().as_str(),
            resource.kind().version().get(),
            observed,
        ) {
            ("cloudflare/worker", 1, CloudflareObservation::Missing) => Ok(ReconcileDecision::Act(
                CloudflareAction::UploadWorker(decode(resource)?),
            )),
            ("cloudflare/worker", 1, CloudflareObservation::Worker { digest, .. })
                if *digest != resource.digest() =>
            {
                Ok(ReconcileDecision::Act(CloudflareAction::UploadWorker(
                    decode(resource)?,
                )))
            }
            (
                "cloudflare/worker",
                1,
                CloudflareObservation::Worker {
                    subdomain_enabled: false,
                    ..
                },
            ) => Ok(ReconcileDecision::Act(
                CloudflareAction::EnableWorkerSubdomain,
            )),
            ("cloudflare/worker", 1, CloudflareObservation::Worker { observation, .. }) => {
                converged_worker(resource, observation)
            }
            ("cloudflare/tunnel", 1, CloudflareObservation::Missing) => {
                Ok(ReconcileDecision::Act(CloudflareAction::CreateTunnel))
            }
            (
                "cloudflare/tunnel",
                1,
                CloudflareObservation::Tunnel {
                    configured: false, ..
                },
            ) => Ok(ReconcileDecision::Act(CloudflareAction::ConfigureTunnel(
                decode(resource)?,
            ))),
            ("cloudflare/tunnel", 1, CloudflareObservation::Tunnel { observation, .. }) => {
                converged_tunnel(resource, observation)
            }
            ("cloudflare/route", 1, CloudflareObservation::Missing) => Ok(ReconcileDecision::Act(
                CloudflareAction::WriteRoute(decode(resource)?),
            )),
            ("cloudflare/route", 1, CloudflareObservation::Route { matches: false, .. }) => Ok(
                ReconcileDecision::Act(CloudflareAction::WriteRoute(decode(resource)?)),
            ),
            ("cloudflare/route", 1, CloudflareObservation::Route { observation, .. }) => {
                converged_route(resource, observation)
            }
            (_, _, CloudflareObservation::Missing) => Err(unsupported(resource)),
            _ => Err(CloudflareError::Provider(format!(
                "Cloudflare returned an observation incompatible with {}",
                resource.path()
            ))),
        }
    }

    fn act<'a>(
        &'a self,
        graph_id: GraphId,
        resource: &'a Resource,
        action: Self::Action,
    ) -> BoxFuture<'a, Result<(), Self::Error>> {
        self.transport.act(graph_id, resource, action)
    }
}

impl<T> Controller for CloudflareController<T>
where
    T: CloudflareTransport,
{
    fn name(&self) -> &ControllerName {
        &self.name
    }

    fn execute<'a>(
        &'a self,
        command: &'a ControllerCommand,
    ) -> BoxFuture<'a, Result<ControllerPass, ControllerError>> {
        async move {
            match command {
                ControllerCommand::Reconcile(slice) => self.reconcile(slice).await,
                ControllerCommand::Supersede(supersession) => {
                    reconcile_absent(self, supersession.graph_id, &supersession.resources)
                        .await
                        .map(|pass| match pass {
                            SlicePass::Acted => ControllerPass::Acted,
                            SlicePass::Converged(_) => ControllerPass::Converged(None),
                        })
                        .map_err(|error| ControllerError::new(error.to_string()))
                }
                ControllerCommand::Retire(retirement) => {
                    reconcile_absent(self, retirement.graph_id, &retirement.resources)
                        .await
                        .map(|pass| match pass {
                            SlicePass::Acted => ControllerPass::Acted,
                            SlicePass::Converged(_) => ControllerPass::Converged(None),
                        })
                        .map_err(|error| ControllerError::new(error.to_string()))
                }
            }
        }
        .boxed()
    }
}

fn converged_worker(
    resource: &Resource,
    observed: &WorkerObservation,
) -> Result<ReconcileDecision<CloudflareAction>, CloudflareError> {
    let mut outputs = Vec::new();
    for (name, value) in [
        ("url", serde_json::json!(observed.url)),
        ("workerName", serde_json::json!(observed.worker_name)),
        ("deploymentId", serde_json::json!(observed.deployment_id)),
        ("versionId", serde_json::json!(observed.version_id)),
    ] {
        push_if_declared(resource, name, value, &mut outputs)?;
    }
    Ok(ReconcileDecision::Converged(ResourceConvergence {
        outputs,
        evidence: observed.deployment_id.as_bytes().to_vec(),
    }))
}

fn converged_tunnel(
    resource: &Resource,
    observed: &TunnelObservation,
) -> Result<ReconcileDecision<CloudflareAction>, CloudflareError> {
    let mut outputs = Vec::new();
    for (name, value) in [
        ("tunnelId", serde_json::json!(observed.tunnel_id)),
        ("tunnelName", serde_json::json!(observed.tunnel_name)),
        (
            "privateHostname",
            serde_json::json!(observed.private_hostname),
        ),
        ("tokenRef", serde_json::json!(observed.token_ref)),
    ] {
        push_if_declared(resource, name, value, &mut outputs)?;
    }
    Ok(ReconcileDecision::Converged(ResourceConvergence {
        outputs,
        evidence: observed.tunnel_id.as_bytes().to_vec(),
    }))
}

fn converged_route(
    resource: &Resource,
    observed: &RouteObservation,
) -> Result<ReconcileDecision<CloudflareAction>, CloudflareError> {
    let mut outputs = Vec::new();
    push_if_declared(
        resource,
        "hostname",
        serde_json::json!(observed.hostname),
        &mut outputs,
    )?;
    Ok(ReconcileDecision::Converged(ResourceConvergence {
        outputs,
        evidence: observed.hostname.as_bytes().to_vec(),
    }))
}

fn cloudflare_rank(resource: &Resource, goal: ResourceGoal) -> u8 {
    let worker_has_services = resource
        .body()
        .as_json()
        .get("services")
        .and_then(serde_json::Value::as_object)
        .is_some_and(|services| !services.is_empty());
    match (goal, resource.kind().name().as_str(), worker_has_services) {
        (ResourceGoal::Present, "cloudflare/worker", false) => 0,
        (ResourceGoal::Present, "cloudflare/worker", true) => 1,
        (ResourceGoal::Present, "cloudflare/tunnel", _) => 2,
        (ResourceGoal::Present, "cloudflare/route", _) => 3,
        (ResourceGoal::Absent, "cloudflare/route", _) => 0,
        (ResourceGoal::Absent, "cloudflare/worker", true) => 1,
        (ResourceGoal::Absent, "cloudflare/worker", false) => 2,
        (ResourceGoal::Absent, "cloudflare/tunnel", _) => 3,
        _ => 4,
    }
}

fn decode<T>(resource: &Resource) -> Result<T, CloudflareError>
where
    T: for<'de> Deserialize<'de>,
{
    serde_json::from_value(resource.body().as_json().clone()).map_err(|error| {
        CloudflareError::Contract(format!(
            "error[cloudflare.body.invalid]: {}: {error}",
            resource.path()
        ))
    })
}

fn unsupported(resource: &Resource) -> CloudflareError {
    CloudflareError::Contract(format!(
        "error[cloudflare.kind.unsupported]: {} has unsupported kind {}",
        resource.path(),
        resource.kind()
    ))
}

fn push_if_declared(
    resource: &Resource,
    name: &str,
    value: serde_json::Value,
    outputs: &mut Vec<ObservedOutput>,
) -> Result<(), CloudflareError> {
    if resource
        .outputs()
        .any(|declaration| declaration.name().as_str() == name)
    {
        outputs.push(
            output(resource, name, value)
                .map_err(|error| CloudflareError::Contract(error.to_string()))?,
        );
    }
    Ok(())
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum CloudflareError {
    #[error("Cloudflare configuration: {0}")]
    Config(String),
    #[error("{0}")]
    Contract(String),
    #[error("Cloudflare API unavailable: {0}")]
    Unavailable(String),
    #[error("Cloudflare API rejected desired state: {0}")]
    Provider(String),
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU32;
    use std::sync::Arc;
    use std::sync::Mutex;

    use henosis_types::ComponentName;
    use henosis_types::Generation;
    use henosis_types::KindName;
    use henosis_types::KindVersion;
    use henosis_types::NewResource;
    use henosis_types::OutputAvailability;
    use henosis_types::OutputDeclaration;
    use henosis_types::OutputName;
    use henosis_types::ResourceAddress;
    use henosis_types::ResourceId;
    use henosis_types::ResourceName;
    use henosis_types::ResourcePath;
    use henosis_types::Retirement;

    use super::*;

    #[derive(Default)]
    struct FakeState {
        resources: BTreeMap<(GraphId, ResourceId), CloudflareObservation>,
        foreign: BTreeMap<ResourceId, CloudflareObservation>,
    }

    struct RecordedTransport {
        state: Arc<Mutex<FakeState>>,
        actions: Arc<Mutex<Vec<CloudflareAction>>>,
    }

    struct Fixture {
        controller: CloudflareController<RecordedTransport>,
        actions: Arc<Mutex<Vec<CloudflareAction>>>,
        state: Arc<Mutex<FakeState>>,
    }

    impl CloudflareTransport for RecordedTransport {
        fn observe<'a>(
            &'a self,
            graph: GraphId,
            resource: &'a Resource,
        ) -> BoxFuture<'a, Result<CloudflareObservation, CloudflareError>> {
            async move {
                let state = self.state.lock().unwrap();
                Ok(state
                    .foreign
                    .get(&resource.id())
                    .or_else(|| state.resources.get(&(graph, resource.id())))
                    .cloned()
                    .unwrap_or(CloudflareObservation::Missing))
            }
            .boxed()
        }

        fn act<'a>(
            &'a self,
            graph: GraphId,
            resource: &'a Resource,
            action: CloudflareAction,
        ) -> BoxFuture<'a, Result<(), CloudflareError>> {
            async move {
                self.actions.lock().unwrap().push(action.clone());
                let mut state = self.state.lock().unwrap();
                match action {
                    CloudflareAction::UploadWorker(_) => {
                        state.resources.insert(
                            (graph, resource.id()),
                            CloudflareObservation::Worker {
                                digest: resource.digest(),
                                subdomain_enabled: false,
                                observation: WorkerObservation {
                                    url: "https://api.example.workers.dev".into(),
                                    worker_name: "api".into(),
                                    deployment_id: "deployment-1".into(),
                                    version_id: "version-1".into(),
                                },
                            },
                        );
                    }
                    CloudflareAction::EnableWorkerSubdomain => {
                        let CloudflareObservation::Worker {
                            subdomain_enabled, ..
                        } = state.resources.get_mut(&(graph, resource.id())).unwrap()
                        else {
                            unreachable!()
                        };
                        *subdomain_enabled = true;
                    }
                    CloudflareAction::Delete => {
                        state.resources.remove(&(graph, resource.id()));
                    }
                    _ => unreachable!(),
                }
                Ok(())
            }
            .boxed()
        }
    }

    #[tokio::test]
    async fn converges_one_action_per_pass_without_flapping() {
        let fixture = fixture();
        let slice = slice();
        assert_eq!(
            reconcile_slice(&fixture.controller, &slice).await.unwrap(),
            SlicePass::Acted
        );
        assert!(matches!(
            fixture.actions.lock().unwrap()[0],
            CloudflareAction::UploadWorker(_)
        ));
        assert_eq!(
            reconcile_slice(&fixture.controller, &slice).await.unwrap(),
            SlicePass::Acted
        );
        assert_eq!(
            fixture.actions.lock().unwrap()[1],
            CloudflareAction::EnableWorkerSubdomain
        );
        assert!(matches!(
            reconcile_slice(&fixture.controller, &slice).await.unwrap(),
            SlicePass::Converged(_)
        ));
        assert_eq!(fixture.actions.lock().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn fresh_controller_retires_from_target_observation() {
        let fixture = fixture();
        let slice = slice();
        let reconcile = ControllerCommand::Reconcile(slice.clone());
        assert_eq!(
            fixture.controller.execute(&reconcile).await.unwrap(),
            ControllerPass::Acted
        );
        assert_eq!(
            fixture.controller.execute(&reconcile).await.unwrap(),
            ControllerPass::Acted
        );
        assert!(matches!(
            fixture.controller.execute(&reconcile).await.unwrap(),
            ControllerPass::Converged(Some(_))
        ));
        let restarted = CloudflareController::new(RecordedTransport {
            state: Arc::clone(&fixture.state),
            actions: fixture.actions,
        });
        let retire = ControllerCommand::Retire(Retirement {
            graph_id: slice.graph_id(),
            last_generation: slice.generation(),
            controller: restarted.name().clone(),
            resources: slice.resources().to_vec(),
        });
        assert_eq!(
            restarted.execute(&retire).await.unwrap(),
            ControllerPass::Acted
        );
        assert_eq!(
            restarted.execute(&retire).await.unwrap(),
            ControllerPass::Converged(None)
        );
        assert!(fixture.state.lock().unwrap().resources.is_empty());
    }

    #[tokio::test]
    async fn refuses_right_identity_with_wrong_ownership_metadata() {
        let fixture = fixture();
        let slice = slice();
        fixture
            .state
            .lock()
            .unwrap()
            .foreign
            .insert(slice.resources()[0].id(), CloudflareObservation::Foreign);
        let error = reconcile_slice(&fixture.controller, &slice)
            .await
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("ownership metadata/identity does not match")
        );
        assert!(fixture.actions.lock().unwrap().is_empty());
    }

    fn fixture() -> Fixture {
        let actions = Arc::new(Mutex::new(Vec::new()));
        let state = Arc::new(Mutex::new(FakeState::default()));
        Fixture {
            controller: CloudflareController::new(RecordedTransport {
                state: Arc::clone(&state),
                actions: Arc::clone(&actions),
            }),
            actions,
            state,
        }
    }

    fn slice() -> ControllerSlice {
        let outputs = ["url", "workerName", "deploymentId", "versionId"]
            .into_iter()
            .map(|name| {
                OutputDeclaration::new(OutputName::new(name).unwrap(), OutputAvailability::Observed)
            })
            .collect();
        let resource = Resource::new(NewResource {
            id: ResourceId::from_bytes([4; 16]),
            path: ResourcePath::new(
                ComponentName::new("api").unwrap(),
                ResourceAddress::new(
                    KindVersion::new(
                        KindName::new("cloudflare/worker").unwrap(),
                        NonZeroU32::new(1).unwrap(),
                    ),
                    ResourceName::new("api").unwrap(),
                ),
            ),
            controller: controller_name(CONTROLLER_NAME),
            body: serde_json::json!({
                "source": {
                    "entry": {
                        "kind": "cloudflare-worker",
                        "digest": format!("sha256:{}", "11".repeat(32))
                    },
                    "assets": null
                },
                "vars": {}
            })
            .try_into()
            .unwrap(),
            outputs,
        })
        .unwrap();
        ControllerSlice::new(
            GraphId::from_bytes([3; 16]),
            Generation::new(1).unwrap(),
            ContentDigest::digest(b"plan"),
            controller_name(CONTROLLER_NAME),
            BTreeMap::new(),
            vec![resource],
            Vec::new(),
        )
    }
}
