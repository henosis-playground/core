use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::Mutex;

use futures::FutureExt as _;
use futures::future::BoxFuture;
use henosis_controller_cloudflare::CloudflareAction;
use henosis_controller_cloudflare::CloudflareError;
use henosis_controller_cloudflare::CloudflareObservation;
use henosis_controller_cloudflare::CloudflareTransport;
use henosis_controller_cloudflare::RouteObservation;
use henosis_controller_cloudflare::TunnelObservation;
use henosis_controller_cloudflare::WorkerObservation;
use henosis_controller_k8s::K8sTarget;
use henosis_controller_supabase::SupabaseError;
use henosis_controller_supabase::SupabaseObservation;
use henosis_controller_supabase::SupabaseOperation;
use henosis_controller_supabase::SupabaseTarget;
use henosis_types::ControllerName;
use henosis_types::Generation;
use henosis_types::GraphId;
use henosis_types::NativeValue;
use henosis_types::OutputName;
use henosis_types::Resource;
use henosis_types::ResourceId;
use thiserror::Error;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TargetOperation {
    pub idempotency_key: String,
    pub generation: Generation,
    pub controller: ControllerName,
    pub resources: Vec<ResourceId>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OutputDelivery {
    pub generation: Generation,
    pub resource: ResourceId,
    pub output: OutputName,
    pub value: NativeValue,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TargetFault {
    Apply,
    FailBeforeApply,
    ApplyThenTimeout,
    PartialApply(usize),
    DelayOutput,
    DuplicateOutput,
    StaleOutput(Generation),
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
#[error("idempotency key {key} was reused for a different target operation")]
pub struct IdempotencyViolation {
    pub key: String,
}

#[derive(Clone, Debug, Default)]
pub struct FakeTarget {
    accepted: BTreeMap<String, TargetOperation>,
    applied: BTreeMap<ResourceId, Generation>,
    faults: VecDeque<TargetFault>,
    delayed: Vec<OutputDelivery>,
}

impl FakeTarget {
    pub fn script(&mut self, faults: impl IntoIterator<Item = TargetFault>) {
        self.faults.extend(faults);
    }

    pub fn apply(&mut self, operation: TargetOperation) -> Result<bool, IdempotencyViolation> {
        if let Some(previous) = self.accepted.get(&operation.idempotency_key) {
            if previous != &operation {
                return Err(IdempotencyViolation {
                    key: operation.idempotency_key,
                });
            }
            return Ok(false);
        }
        self.accepted
            .insert(operation.idempotency_key.clone(), operation.clone());
        let fault = self.faults.pop_front().unwrap_or(TargetFault::Apply);
        let apply_count = match fault {
            TargetFault::FailBeforeApply => 0,
            TargetFault::PartialApply(count) => count.min(operation.resources.len()),
            _ => operation.resources.len(),
        };
        for resource in operation.resources.into_iter().take(apply_count) {
            self.applied.insert(resource, operation.generation);
        }
        Ok(!matches!(
            fault,
            TargetFault::FailBeforeApply | TargetFault::ApplyThenTimeout
        ))
    }

    pub fn delay(&mut self, output: OutputDelivery) {
        self.delayed.push(output);
    }

    pub fn drain_delayed(&mut self) -> Vec<OutputDelivery> {
        std::mem::take(&mut self.delayed)
    }

    #[must_use]
    pub fn generation_of(&self, resource: ResourceId) -> Option<Generation> {
        self.applied.get(&resource).copied()
    }
}

// === REAL-CONTROLLER TRANSPORTS ===

fn scripted_fault(faults: &mut VecDeque<TargetFault>) -> TargetFault {
    faults.pop_front().unwrap_or(TargetFault::Apply)
}

fn applies(fault: &TargetFault) -> bool {
    !matches!(fault, TargetFault::FailBeforeApply)
}

fn reports_failure(fault: &TargetFault) -> bool {
    matches!(
        fault,
        TargetFault::FailBeforeApply
            | TargetFault::ApplyThenTimeout
            | TargetFault::PartialApply(_)
    )
}

#[derive(Debug, Default)]
struct FakeK8sState {
    resources: BTreeMap<(GraphId, ResourceId), BTreeMap<String, Vec<u8>>>,
    faults: VecDeque<TargetFault>,
    actions: BTreeMap<ResourceId, usize>,
}

#[derive(Clone, Debug, Default)]
pub struct FakeK8sTarget {
    state: Arc<Mutex<FakeK8sState>>,
}

impl FakeK8sTarget {
    pub fn script(&self, faults: impl IntoIterator<Item = TargetFault>) {
        self.state
            .lock()
            .expect("fake Kubernetes target lock is not poisoned")
            .faults
            .extend(faults);
    }

    #[must_use]
    pub fn action_count(&self, resource: ResourceId) -> usize {
        self.state
            .lock()
            .expect("fake Kubernetes target lock is not poisoned")
            .actions
            .get(&resource)
            .copied()
            .unwrap_or(0)
    }

    #[must_use]
    pub fn contains(&self, graph: GraphId, resource: ResourceId) -> bool {
        self.state
            .lock()
            .expect("fake Kubernetes target lock is not poisoned")
            .resources
            .contains_key(&(graph, resource))
    }
}

impl K8sTarget for FakeK8sTarget {
    fn read_resource(
        &self,
        graph_id: GraphId,
        resource: &Resource,
    ) -> Result<BTreeMap<String, Vec<u8>>, String> {
        Ok(self
            .state
            .lock()
            .map_err(|_| "fake Kubernetes target lock is poisoned".to_owned())?
            .resources
            .get(&(graph_id, resource.id()))
            .cloned()
            .unwrap_or_default())
    }

    fn write_resource(
        &self,
        graph_id: GraphId,
        resource: &Resource,
        files: &BTreeMap<String, Vec<u8>>,
    ) -> Result<(), String> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| "fake Kubernetes target lock is poisoned".to_owned())?;
        *state.actions.entry(resource.id()).or_default() += 1;
        let fault = scripted_fault(&mut state.faults);
        if applies(&fault) {
            if files.is_empty() {
                state.resources.remove(&(graph_id, resource.id()));
            } else {
                state
                    .resources
                    .insert((graph_id, resource.id()), files.clone());
            }
        }
        if reports_failure(&fault) {
            Err("scripted Kubernetes target timeout/failure".to_owned())
        } else {
            Ok(())
        }
    }

    fn remove_graph_if_empty(&self, _graph_id: GraphId) -> Result<(), String> {
        Ok(())
    }
}

#[derive(Debug, Default)]
struct FakeCloudflareState {
    resources: BTreeMap<(GraphId, ResourceId), CloudflareObservation>,
    faults: VecDeque<TargetFault>,
    actions: BTreeMap<ResourceId, usize>,
}

#[derive(Clone, Debug, Default)]
pub struct FakeCloudflareTransport {
    state: Arc<Mutex<FakeCloudflareState>>,
}

impl FakeCloudflareTransport {
    pub fn script(&self, faults: impl IntoIterator<Item = TargetFault>) {
        self.state
            .lock()
            .expect("fake Cloudflare target lock is not poisoned")
            .faults
            .extend(faults);
    }

    #[must_use]
    pub fn action_count(&self, resource: ResourceId) -> usize {
        self.state
            .lock()
            .expect("fake Cloudflare target lock is not poisoned")
            .actions
            .get(&resource)
            .copied()
            .unwrap_or(0)
    }

    #[must_use]
    pub fn contains(&self, graph: GraphId, resource: ResourceId) -> bool {
        self.state
            .lock()
            .expect("fake Cloudflare target lock is not poisoned")
            .resources
            .contains_key(&(graph, resource))
    }
}

impl CloudflareTransport for FakeCloudflareTransport {
    fn observe<'a>(
        &'a self,
        graph: GraphId,
        resource: &'a Resource,
    ) -> BoxFuture<'a, Result<CloudflareObservation, CloudflareError>> {
        async move {
            Ok(self
                .state
                .lock()
                .map_err(|_| CloudflareError::Unavailable("fake target lock poisoned".into()))?
                .resources
                .get(&(graph, resource.id()))
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
            let mut state = self
                .state
                .lock()
                .map_err(|_| CloudflareError::Unavailable("fake target lock poisoned".into()))?;
            *state.actions.entry(resource.id()).or_default() += 1;
            let fault = scripted_fault(&mut state.faults);
            if applies(&fault) {
                apply_cloudflare_action(&mut state.resources, graph, resource, action);
            }
            if reports_failure(&fault) {
                Err(CloudflareError::Unavailable(
                    "scripted apply-then-timeout/failure".into(),
                ))
            } else {
                Ok(())
            }
        }
        .boxed()
    }
}

fn apply_cloudflare_action(
    resources: &mut BTreeMap<(GraphId, ResourceId), CloudflareObservation>,
    graph: GraphId,
    resource: &Resource,
    action: CloudflareAction,
) {
    let key = (graph, resource.id());
    match action {
        CloudflareAction::UploadWorker(_) => {
            resources.insert(
                key,
                CloudflareObservation::Worker {
                    digest: resource.digest(),
                    subdomain_enabled: false,
                    observation: WorkerObservation {
                        url: format!("https://{}.workers.test", resource.id()),
                        worker_name: resource.id().to_string(),
                        deployment_id: format!("deployment-{}", resource.digest()),
                        version_id: format!("version-{}", resource.digest()),
                    },
                },
            );
        }
        CloudflareAction::EnableWorkerSubdomain => {
            if let Some(CloudflareObservation::Worker {
                subdomain_enabled, ..
            }) = resources.get_mut(&key)
            {
                *subdomain_enabled = true;
            }
        }
        CloudflareAction::CreateTunnel => {
            resources.insert(
                key,
                CloudflareObservation::Tunnel {
                    configured: false,
                    observation: TunnelObservation {
                        tunnel_id: resource.id().to_string(),
                        tunnel_name: format!("tunnel-{}", resource.id()),
                        private_hostname: format!("{}.internal", resource.id()),
                        token_ref: format!("secret://tunnel/{}", resource.id()),
                    },
                },
            );
        }
        CloudflareAction::ConfigureTunnel(_) => {
            if let Some(CloudflareObservation::Tunnel { configured, .. }) =
                resources.get_mut(&key)
            {
                *configured = true;
            }
        }
        CloudflareAction::WriteRoute(body) => {
            resources.insert(
                key,
                CloudflareObservation::Route {
                    matches: true,
                    observation: RouteObservation {
                        hostname: body.pattern.trim_start_matches("*.").to_owned(),
                    },
                },
            );
        }
        CloudflareAction::Delete => {
            resources.remove(&key);
        }
    }
}

#[derive(Debug, Default)]
struct FakeSupabaseState {
    resources: BTreeMap<(GraphId, ResourceId), SupabaseObservation>,
    faults: VecDeque<TargetFault>,
    actions: BTreeMap<ResourceId, usize>,
}

#[derive(Clone, Debug, Default)]
pub struct FakeSupabaseTarget {
    state: Arc<Mutex<FakeSupabaseState>>,
}

impl FakeSupabaseTarget {
    pub fn script(&self, faults: impl IntoIterator<Item = TargetFault>) {
        self.state
            .lock()
            .expect("fake Supabase target lock is not poisoned")
            .faults
            .extend(faults);
    }

    #[must_use]
    pub fn action_count(&self, resource: ResourceId) -> usize {
        self.state
            .lock()
            .expect("fake Supabase target lock is not poisoned")
            .actions
            .get(&resource)
            .copied()
            .unwrap_or(0)
    }

    #[must_use]
    pub fn contains(&self, graph: GraphId, resource: ResourceId) -> bool {
        self.state
            .lock()
            .expect("fake Supabase target lock is not poisoned")
            .resources
            .get(&(graph, resource))
            .is_some_and(|observation| observation.schema_exists)
    }
}

impl SupabaseTarget for FakeSupabaseTarget {
    fn observe(
        &self,
        graph: GraphId,
        resource: ResourceId,
        _schema: &str,
    ) -> Result<SupabaseObservation, SupabaseError> {
        Ok(self
            .state
            .lock()
            .map_err(|_| SupabaseError::Unavailable("fake target lock poisoned".into()))?
            .resources
            .get(&(graph, resource))
            .cloned()
            .unwrap_or_else(|| SupabaseObservation {
                exposed: BTreeSet::from(["public".to_owned()]),
                .. SupabaseObservation::default()
            }))
    }

    fn apply(
        &self,
        _observed_digest: &str,
        operation: &SupabaseOperation,
    ) -> Result<String, SupabaseError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| SupabaseError::Unavailable("fake target lock poisoned".into()))?;
        if let Some((_, resource)) = supabase_identity(operation) {
            *state.actions.entry(resource).or_default() += 1;
        }
        let fault = scripted_fault(&mut state.faults);
        if applies(&fault) {
            apply_supabase_operation(&mut state.resources, operation);
        }
        if reports_failure(&fault) {
            Err(SupabaseError::Unavailable(
                "scripted apply-then-timeout/failure".into(),
            ))
        } else {
            Ok("applied".to_owned())
        }
    }

    fn api_url(&self) -> &str {
        "https://supabase.test"
    }

    fn database_url_ref(&self) -> &str {
        "secret://supabase/database-url"
    }

    fn anon_key_ref(&self) -> &str {
        "secret://supabase/anon-key"
    }
}

fn supabase_identity(operation: &SupabaseOperation) -> Option<(GraphId, ResourceId)> {
    match operation {
        SupabaseOperation::EnsureSchema {
            graph, resource, ..
        }
        | SupabaseOperation::ApplyMigration {
            graph, resource, ..
        }
        | SupabaseOperation::DropSchema {
            graph, resource, ..
        } => Some((*graph, *resource)),
        SupabaseOperation::ConfigureApi { .. } => None,
    }
}

fn apply_supabase_operation(
    resources: &mut BTreeMap<(GraphId, ResourceId), SupabaseObservation>,
    operation: &SupabaseOperation,
) {
    match operation {
        SupabaseOperation::EnsureSchema {
            graph,
            resource,
            schema,
        } => {
            let observation = resources.entry((*graph, *resource)).or_default();
            observation.schema_exists = true;
            observation.owned_schema = Some(schema.clone());
            observation.exposed.insert("public".to_owned());
        }
        SupabaseOperation::ApplyMigration {
            graph,
            resource,
            id,
            checksum,
            ..
        } => {
            resources
                .entry((*graph, *resource))
                .or_default()
                .migrations
                .insert(id.clone(), checksum.clone());
        }
        SupabaseOperation::ConfigureApi { exposed, anon_read } => {
            for observation in resources.values_mut() {
                observation.exposed = exposed.clone();
                observation.anon_read = anon_read.clone();
            }
        }
        SupabaseOperation::DropSchema {
            graph, resource, ..
        } => {
            resources.remove(&(*graph, *resource));
        }
    }
}
