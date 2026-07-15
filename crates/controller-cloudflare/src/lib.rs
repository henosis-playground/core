//! Cloudflare resource controller with a replaceable API transport.
//!
//! The transport seam is deliberately provider-shaped and is exercised with a
//! recorded/in-memory implementation. Worker source bytes are resolved inside
//! the transport boundary because the D26 `cloudflare/worker@1` body currently
//! carries a repository-relative source entry rather than the bundled bytes
//! themselves.

use std::collections::BTreeMap;
use std::sync::Mutex;

use futures::FutureExt as _;
use futures::future::BoxFuture;
use henosis_controller_runtime::controller_name;
use henosis_controller_runtime::failed_report;
use henosis_controller_runtime::output;
use henosis_controller_runtime::publication_id;
use henosis_controller_runtime::ready_report;
use henosis_types::Controller;
use henosis_types::ControllerCommand;
use henosis_types::ControllerError;
use henosis_types::ControllerName;
use henosis_types::ControllerReport;
use henosis_types::ControllerSlice;
use henosis_types::GraphId;
use henosis_types::Resource;
use henosis_types::ResourceId;
use serde::Deserialize;
use serde::Serialize;
use thiserror::Error;

mod live;

pub use live::ComponentBundleResolver;
pub use live::LiveCloudflareConfig;
pub use live::LiveCloudflareTransport;

const CONTROLLER_NAME: &str = "cloudflare";

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkerBody {
    pub source: SourceRef,
    pub compatibility_date: Option<String>,
    #[serde(default)]
    pub vars: BTreeMap<String, serde_json::Value>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SourceRef {
    pub entry: String,
    pub assets: Option<String>,
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

pub trait CloudflareTransport: Send + Sync {
    fn apply_worker<'a>(
        &'a self,
        graph: GraphId,
        resource: &'a Resource,
        body: &'a WorkerBody,
    ) -> BoxFuture<'a, Result<WorkerObservation, CloudflareError>>;
    fn apply_tunnel<'a>(
        &'a self,
        graph: GraphId,
        resource: &'a Resource,
        body: &'a TunnelBody,
    ) -> BoxFuture<'a, Result<TunnelObservation, CloudflareError>>;
    fn apply_route<'a>(
        &'a self,
        graph: GraphId,
        resource: &'a Resource,
        body: &'a RouteBody,
    ) -> BoxFuture<'a, Result<RouteObservation, CloudflareError>>;
    fn delete(
        &self,
        graph: GraphId,
        resource: ResourceId,
    ) -> BoxFuture<'_, Result<(), CloudflareError>>;
}

pub struct CloudflareController<T> {
    name: ControllerName,
    transport: T,
    state: Mutex<BTreeMap<GraphId, BTreeMap<ResourceId, Resource>>>,
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
            state: Mutex::new(BTreeMap::new()),
        }
    }

    async fn reconcile(
        &self,
        slice: &ControllerSlice,
    ) -> Result<ControllerReport, ControllerError> {
        let result = self.apply_slice(slice).await;
        let (outputs, evidence) = match result {
            Ok(value) => value,
            Err(error) => {
                return failed_report(slice, error.to_string())
                    .map_err(|report_error| ControllerError::new(report_error.to_string()));
            }
        };
        let desired = slice
            .resources()
            .iter()
            .cloned()
            .map(|resource| (resource.id(), resource))
            .collect();
        self.state
            .lock()
            .expect("cloudflare controller state lock is not poisoned")
            .insert(slice.graph_id(), desired);
        ready_report(slice, Some(publication_id(evidence.as_bytes())), outputs)
            .map_err(|error| ControllerError::new(error.to_string()))
    }

    async fn apply_slice(
        &self,
        slice: &ControllerSlice,
    ) -> Result<(Vec<henosis_types::ObservedOutput>, String), CloudflareError> {
        let mut outputs = Vec::new();
        let mut evidence = String::new();
        let mut resources = slice.resources().iter().collect::<Vec<_>>();
        resources.sort_by_key(|resource| cloudflare_rank(resource));
        for resource in resources {
            match (
                resource.kind().name().as_str(),
                resource.kind().version().get(),
            ) {
                ("cloudflare/worker", 1) => {
                    let body: WorkerBody = decode(resource)?;
                    let observed =
                        self.transport
                            .apply_worker(slice.graph_id(), resource, &body)
                            .await?;
                    push_if_declared(
                        resource,
                        "url",
                        serde_json::json!(observed.url),
                        &mut outputs,
                    )?;
                    push_if_declared(
                        resource,
                        "workerName",
                        serde_json::json!(observed.worker_name),
                        &mut outputs,
                    )?;
                    push_if_declared(
                        resource,
                        "deploymentId",
                        serde_json::json!(observed.deployment_id),
                        &mut outputs,
                    )?;
                    push_if_declared(
                        resource,
                        "versionId",
                        serde_json::json!(observed.version_id),
                        &mut outputs,
                    )?;
                    evidence.push_str(&format!("{}:{};", resource.id(), observed.deployment_id));
                }
                ("cloudflare/tunnel", 1) => {
                    let body: TunnelBody = decode(resource)?;
                    let observed =
                        self.transport
                            .apply_tunnel(slice.graph_id(), resource, &body)
                            .await?;
                    push_if_declared(
                        resource,
                        "tunnelId",
                        serde_json::json!(observed.tunnel_id),
                        &mut outputs,
                    )?;
                    push_if_declared(
                        resource,
                        "tunnelName",
                        serde_json::json!(observed.tunnel_name),
                        &mut outputs,
                    )?;
                    push_if_declared(
                        resource,
                        "privateHostname",
                        serde_json::json!(observed.private_hostname),
                        &mut outputs,
                    )?;
                    push_if_declared(
                        resource,
                        "tokenRef",
                        serde_json::json!(observed.token_ref),
                        &mut outputs,
                    )?;
                    evidence.push_str(&format!("{}:{};", resource.id(), observed.tunnel_id));
                }
                ("cloudflare/route", 1) => {
                    let body: RouteBody = decode(resource)?;
                    let observed = self
                        .transport
                        .apply_route(slice.graph_id(), resource, &body)
                        .await?;
                    push_if_declared(
                        resource,
                        "hostname",
                        serde_json::json!(observed.hostname),
                        &mut outputs,
                    )?;
                    evidence.push_str(&format!("{}:{};", resource.id(), observed.hostname));
                }
                _ => {
                    return Err(CloudflareError::Contract(format!(
                        "error[cloudflare.kind.unsupported]: {} has unsupported kind {}",
                        resource.path(),
                        resource.kind()
                    )));
                }
            }
        }
        Ok((outputs, evidence))
    }

    async fn remove(
        &self,
        graph: GraphId,
        resources: &[ResourceId],
    ) -> Result<(), ControllerError> {
        for resource in resources {
            self.transport
                .delete(graph, *resource)
                .await
                .map_err(|error| ControllerError::new(error.to_string()))?;
        }
        if let Some(current) = self
            .state
            .lock()
            .expect("cloudflare controller state lock is not poisoned")
            .get_mut(&graph)
        {
            for resource in resources {
                current.remove(resource);
            }
        }
        Ok(())
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
    ) -> BoxFuture<'a, Result<Option<ControllerReport>, ControllerError>> {
        async move {
            match command {
                ControllerCommand::Reconcile(slice) => self.reconcile(slice).await.map(Some),
                ControllerCommand::Supersede(supersession) => {
                    self.remove(supersession.graph_id, &supersession.resources)
                        .await?;
                    Ok(None)
                }
                ControllerCommand::Retire(retirement) => {
                    self.remove(retirement.graph_id, &retirement.resources)
                        .await?;
                    self.state
                        .lock()
                        .expect("cloudflare controller state lock is not poisoned")
                        .remove(&retirement.graph_id);
                    Ok(None)
                }
            }
        }
        .boxed()
    }
}

fn cloudflare_rank(resource: &Resource) -> u8 {
    match resource.kind().name().as_str() {
        "cloudflare/worker" => 0,
        "cloudflare/tunnel" => 1,
        "cloudflare/route" => 2,
        _ => 3,
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

fn push_if_declared(
    resource: &Resource,
    name: &str,
    value: serde_json::Value,
    outputs: &mut Vec<henosis_types::ObservedOutput>,
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
    use std::sync::Mutex;

    use henosis_types::ComponentName;
    use henosis_types::ContentDigest;
    use henosis_types::ControllerSlice;
    use henosis_types::Generation;
    use henosis_types::KindName;
    use henosis_types::KindVersion;
    use henosis_types::NewResource;
    use henosis_types::OutputAvailability;
    use henosis_types::OutputDeclaration;
    use henosis_types::OutputName;
    use henosis_types::ResourceAddress;
    use henosis_types::ResourceName;
    use henosis_types::ResourcePath;
    use henosis_types::Retirement;

    use super::*;

    #[derive(Default)]
    struct RecordedTransport {
        digests: Mutex<BTreeMap<ResourceId, ContentDigest>>,
        mutations: Mutex<usize>,
        deletions: Mutex<Vec<ResourceId>>,
    }

    impl CloudflareTransport for RecordedTransport {
        fn apply_worker<'a>(
            &'a self,
            _graph: GraphId,
            resource: &'a Resource,
            _body: &'a WorkerBody,
        ) -> BoxFuture<'a, Result<WorkerObservation, CloudflareError>> {
            async move {
                self.record(resource);
                Ok(WorkerObservation {
                    url: "https://api.example.workers.dev".into(),
                    worker_name: "api".into(),
                    deployment_id: "deployment-1".into(),
                    version_id: "version-1".into(),
                })
            }
            .boxed()
        }

        fn apply_tunnel<'a>(
            &'a self,
            _graph: GraphId,
            _resource: &'a Resource,
            _body: &'a TunnelBody,
        ) -> BoxFuture<'a, Result<TunnelObservation, CloudflareError>> {
            async { unreachable!() }.boxed()
        }

        fn apply_route<'a>(
            &'a self,
            _graph: GraphId,
            _resource: &'a Resource,
            _body: &'a RouteBody,
        ) -> BoxFuture<'a, Result<RouteObservation, CloudflareError>> {
            async { unreachable!() }.boxed()
        }

        fn delete(
            &self,
            _graph: GraphId,
            resource: ResourceId,
        ) -> BoxFuture<'_, Result<(), CloudflareError>> {
            async move {
                self.digests.lock().unwrap().remove(&resource);
                self.deletions.lock().unwrap().push(resource);
                Ok(())
            }
            .boxed()
        }
    }

    impl RecordedTransport {
        fn record(&self, resource: &Resource) {
            let mut digests = self.digests.lock().unwrap();
            if digests.get(&resource.id()) != Some(&resource.digest()) {
                *self.mutations.lock().unwrap() += 1;
                digests.insert(resource.id(), resource.digest());
            }
        }
    }

    #[tokio::test]
    async fn reports_outputs_atomically_without_recreate_flapping_and_retires() {
        let controller = CloudflareController::new(RecordedTransport::default());
        let slice = slice();
        let report = controller
            .execute(&ControllerCommand::Reconcile(slice.clone()))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(report.dispositions().len(), 1);
        assert_eq!(report.outputs().len(), 4);
        controller
            .execute(&ControllerCommand::Reconcile(slice.clone()))
            .await
            .unwrap();
        assert_eq!(*controller.transport.mutations.lock().unwrap(), 1);
        controller
            .execute(&ControllerCommand::Retire(Retirement {
                graph_id: slice.graph_id(),
                last_generation: slice.generation(),
                controller: controller.name().clone(),
                resources: vec![slice.resources()[0].id()],
            }))
            .await
            .unwrap();
        assert_eq!(
            controller.transport.deletions.lock().unwrap().as_slice(),
            &[slice.resources()[0].id()]
        );
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
            body: serde_json::json!({"source":{"entry":"workers/api.ts"},"vars":{}})
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
            vec![resource],
            Vec::new(),
        )
    }
}
